package blob

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/breezewish/gscache/internal/cache"
	"github.com/breezewish/gscache/internal/cache/backends/local"
	"github.com/breezewish/gscache/internal/log"
	"github.com/breezewish/gscache/internal/protocol"
	"github.com/breezewish/gscache/internal/stats"
	"github.com/breezewish/gscache/internal/util"
	"go.uber.org/zap"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
	"golang.org/x/sync/errgroup"

	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/memblob"
	_ "gocloud.dev/blob/s3blob"
)

const (
	InitialCheckTimeout = 5 * time.Second
	MaxDownloadTimeout  = 1 * time.Minute
	MaxUploadTimeout    = 1 * time.Minute
	MaxCloseTimeout     = 1 * time.Minute
)

type BlobBackend struct {
	config Config
	log    *zap.Logger

	closed         atomic.Bool // When true, new requests will be rejected.
	lifecycle      context.Context
	lifecycleClose context.CancelFunc
	bucket         *blob.Bucket
	diskStore      *local.LocalBackend
	archiveStore   *ArStore // Storing small files in BlobArchive format.
	uploadQueue    pond.Pool

	sfGet    *util.SingleFlightGroup
	sfUpload *util.SingleFlightGroup
}

var _ cache.BackendSupportCompaction = (*BlobBackend)(nil)

func NewBlobBackend(config Config) (*BlobBackend, error) {
	if config.URL == "" {
		return nil, fmt.Errorf("url must be set")
	}
	if config.WorkDir == "" {
		return nil, fmt.Errorf("workDir must be set")
	}
	return &BlobBackend{
		config:   config,
		log:      log.Named("cache.blob"),
		closed:   atomic.Bool{},
		sfGet:    util.NewSingleFlightGroup(),
		sfUpload: util.NewSingleFlightGroup(),
	}, nil
}

func (store *BlobBackend) Open(ctx context.Context) error {
	diskStore, err := local.NewLocalBackend(store.config.WorkDir)
	if err != nil {
		return fmt.Errorf("failed to create local disk store: %w", err)
	}
	store.diskStore = diskStore

	if err := store.diskStore.Open(ctx); err != nil {
		return fmt.Errorf("failed to open local disk store: %w", err)
	}

	b, err := blob.OpenBucket(ctx, store.config.URL)
	if err != nil {
		_ = store.diskStore.Close()
		return err
	}
	store.bucket = b
	store.lifecycle, store.lifecycleClose = context.WithCancel(context.Background())
	store.uploadQueue = pond.NewPool(store.config.UploadConcurrency, pond.WithNonBlocking(true))

	ctx, cancel := context.WithTimeout(store.lifecycle, InitialCheckTimeout)
	accessOk, err := b.IsAccessible(ctx)
	cancel()
	if err != nil || !accessOk {
		_ = store.diskStore.Close()
		_ = store.bucket.Close()
		if err != nil {
			return fmt.Errorf("cannot access blob store: %w", err)
		} else {
			return fmt.Errorf("blob store is not accessible")
		}
	}

	archiveStore, err := NewArStore(ArStoreOpts{
		WorkDir:              store.config.WorkDir,
		Remote:               store.bucket,
		AllPossibleKeyspaces: ArchiveKeyspaces,
		SkipInitialSync:      false,
	})
	if err != nil {
		_ = store.diskStore.Close()
		_ = store.bucket.Close()
		return fmt.Errorf("failed to create BlobArchive store: %w", err)
	}
	store.archiveStore = archiveStore

	go func() {
		// Run compact in parallel with the blob store open.
		// Compact will be cancelled if the store is closed.
		store.Compact()
	}()

	store.log.Info("Blob store opened", zap.Any("config", store.config))
	return nil
}

func (store *BlobBackend) Compact() error {
	if store.closed.Load() {
		return fmt.Errorf("blob store is closed")
	}
	store.log.Info("Start parallel compaction")
	var g errgroup.Group
	for _, keyspacex := range ArchiveKeyspaces {
		keyspace := keyspacex
		g.Go(func() error {
			job := NewCompactionJob(CompactionJobOpts{
				Keyspace:    keyspace,
				BlobArStore: store.archiveStore,
				BlobCache:   store,
				Remote:      store.bucket,
				Ctx:         store.lifecycle,
			})
			job.Work()
			return nil
		})
	}
	_ = g.Wait()
	store.log.Info("Parallel compaction finished")
	return nil
}

func (store *BlobBackend) Get(opts cache.GetOpts) (*protocol.GetResponse, error) {
	if store.closed.Load() {
		return nil, fmt.Errorf("blob store is closed")
	}

	resp, err, _ := store.sfGet.Do(string(opts.Req.ActionID), func() (any, error) {
		return store.get(opts)
	})

	if err != nil {
		store.log.Warn("Get cache entry from blob store failed",
			zap.String("actionID", fmt.Sprintf("%x", opts.Req.ActionID)),
			zap.String("object", CacheEntityKey(opts.Req.ActionID)),
			zap.Error(err))
		return &protocol.GetResponse{Miss: true}, nil
	}
	return resp.(*protocol.GetResponse), nil
}

func (store *BlobBackend) get(opts cache.GetOpts) (*protocol.GetResponse, error) {
	if len(opts.Req.ActionID) == 0 {
		return nil, fmt.Errorf("actionID must be specified in GetRequest")
	}

	defer stats.Default.Persist()

	arEntry := store.archiveStore.GetBlob(CacheEntityKeyspace(opts.Req.ActionID), opts.Req.ActionID)
	if arEntry != nil && arEntry.Size == 0 {
		// Fast path: We can serve from archive store in-memory directly.
		outputPath, err := store.diskStore.EnsureEmptyOutputFile()
		if err != nil {
			return nil, fmt.Errorf("failed to prepare empty output file: %w", err)
		}
		stats.Default.GetBlobMetrics(opts.IsInCompaction).GetByArchive.Inc()
		return &protocol.GetResponse{
			Miss:     false,
			OutputID: arEntry.OutputID,
			Size:     arEntry.Size,
			Time:     &arEntry.Time,
			DiskPath: outputPath,
		}, nil
	}

	diskResp, err := store.diskStore.Get(opts)
	if err != nil {
		return nil, err
	}
	if !diskResp.Miss {
		stats.Default.GetBlobMetrics(opts.IsInCompaction).GetByLocal.Inc()
		return diskResp, nil
	}

	// Before looking up in blob store, let's check if we have the entry in archive store.
	if arEntry != nil {
		zipFileHandle, err := arEntry.Open()
		if err != nil {
			return nil, fmt.Errorf("failed to open archive entry for keyspace %s: %w", CacheEntityKeyspace(opts.Req.ActionID), err)
		}
		putResp, err := store.diskStore.Put(cache.PutOpts{
			Req: protocol.PutRequest{
				ActionID: arEntry.ActionID,
				OutputID: arEntry.OutputID,
				BodySize: arEntry.Size,
			},
			Body:           zipFileHandle,
			OverrideTime:   &arEntry.Time,
			IsInCompaction: opts.IsInCompaction,
		})
		_ = zipFileHandle.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to put archive entry in disk store: %w", err)
		}
		stats.Default.GetBlobMetrics(opts.IsInCompaction).GetByArchive.Inc()
		stats.Default.GetBlobMetrics(opts.IsInCompaction).ArchiveToLocalFiles.Inc() // Later GET will be served from local disk store.
		stats.Default.GetBlobMetrics(opts.IsInCompaction).ArchiveToLocalBytes.Add(uint64(arEntry.Size))
		return &protocol.GetResponse{
			Miss:     false,
			OutputID: arEntry.OutputID,
			Size:     arEntry.Size,
			Time:     &arEntry.Time,
			DiskPath: putResp.DiskPath,
		}, nil
	}

	t := time.Now()

	ctx, cancel := context.WithTimeout(store.lifecycle, MaxDownloadTimeout)
	defer cancel()

	r, err := store.bucket.NewReader(ctx, CacheEntityKey(opts.Req.ActionID), nil)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			store.log.Debug("Miss in blob store",
				zap.String("actionID", fmt.Sprintf("%x", opts.Req.ActionID)))
			return &protocol.GetResponse{Miss: true}, nil
		}
		return nil, err
	}
	defer r.Close()

	// the header part of r is our entry metadata
	// the remaining part is the cache data

	stats.Default.GetBlobMetrics(opts.IsInCompaction).GetByDownload.Inc()
	meta, err := cache.ReadEntryMeta(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read entry metadata: %w", err)
	}
	if !bytes.Equal(meta.ActionID, opts.Req.ActionID) {
		return nil, fmt.Errorf("actionID mismatch: got %x, want %x", meta.ActionID, opts.Req.ActionID)
	}

	diskPutResp, err := store.diskStore.Put(cache.PutOpts{
		Req: protocol.PutRequest{
			ActionID: meta.ActionID,
			OutputID: meta.OutputID,
			BodySize: meta.Size,
		},
		Body:           r,
		OverrideTime:   &meta.Time,
		IsInCompaction: opts.IsInCompaction,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to put entry in disk store: %w", err)
	}

	stats.Default.GetBlobMetrics(opts.IsInCompaction).DownloadBytes.Add(uint64(meta.Size))

	store.log.Debug("Hit and downloaded file from blob store",
		zap.String("cost", time.Since(t).String()),
		zap.String("actionID", fmt.Sprintf("%x", opts.Req.ActionID)),
		zap.String("object", CacheEntityKey(opts.Req.ActionID)),
		zap.String("dataPath", diskPutResp.DiskPath),
		zap.Int64("size", meta.Size))

	return &protocol.GetResponse{
		Miss:     false,
		OutputID: meta.OutputID,
		Size:     meta.Size,
		Time:     &meta.Time,
		DiskPath: diskPutResp.DiskPath,
	}, nil
}

func (store *BlobBackend) Put(opts cache.PutOpts) (*protocol.PutResponse, error) {
	if store.closed.Load() {
		return nil, fmt.Errorf("blob store is closed")
	}

	// First make the file available locally, then we can do upload in background and return immediately.
	diskPutResp, err := store.diskStore.Put(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to put entry in disk store: %w", err)
	}

	// Do dedup until the upload is finished in background.
	_ = store.sfUpload.DoChan(string(opts.Req.ActionID), func() (any, error) {
		task := store.uploadQueue.Submit(func() {
			store.doBgUpload(opts, diskPutResp.DiskPath)
		})
		task.Wait()
		return nil, nil
	})

	return &protocol.PutResponse{
		DiskPath: diskPutResp.DiskPath,
	}, nil
}

func (store *BlobBackend) doBgUpload(putOpts cache.PutOpts, payloadPathOnDisk string) {
	objName := CacheEntityKey(putOpts.Req.ActionID)
	t := time.Now()

	logError := func(msg string, err error) {
		store.log.Error(msg,
			zap.String("actionID", fmt.Sprintf("%x", putOpts.Req.ActionID)),
			zap.String("object", objName),
			zap.String("dataPath", payloadPathOnDisk),
			zap.Error(err))
	}

	// Note that the real upload file should first contain the metadata header,
	// and then the payload data (bodyPathOnDisk).

	ctx, cancel := context.WithTimeout(store.lifecycle, MaxUploadTimeout)
	defer cancel()

	meta := cache.EntryMeta{
		ActionID: putOpts.Req.ActionID,
		OutputID: putOpts.Req.OutputID,
		Size:     putOpts.Req.BodySize,
		Time:     time.Now(),
	}
	if putOpts.OverrideTime != nil {
		meta.Time = *putOpts.OverrideTime
	}

	metadataBuf := bytes.NewBuffer(nil)
	if _, err := meta.WriteTo(metadataBuf); err != nil {
		logError("Failed to write entry metadata", err)
		return
	}

	var bodyReader io.Reader = metadataBuf
	if putOpts.Req.BodySize > 0 {
		payloadReader, err := os.Open(payloadPathOnDisk)
		if err != nil {
			logError("Failed to open file for upload", err)
			return
		}
		defer payloadReader.Close()
		bodyReader = io.MultiReader(metadataBuf, payloadReader)
	}

	err := store.bucket.Upload(
		ctx,
		objName,
		bodyReader,
		&blob.WriterOptions{
			ContentType: "application/octet-stream",
		})
	if err != nil {
		logError("Failed to upload file to blob store", err)
		return
	}

	stats.Default.GetBlobMetrics(putOpts.IsInCompaction).UploadedFiles.Inc()
	stats.Default.GetBlobMetrics(putOpts.IsInCompaction).UploadedBytes.Add(uint64(putOpts.Req.BodySize + int64(metadataBuf.Len())))
	stats.Default.Persist()

	store.log.Debug("Uploaded file to blob store",
		zap.String("cost", time.Since(t).String()),
		zap.String("actionID", fmt.Sprintf("%x", putOpts.Req.ActionID)),
		zap.String("object", objName))
}

func (store *BlobBackend) Close() error {
	defer func() {
		_ = store.diskStore.Close()
		_ = store.bucket.Close()
		store.log.Info("Blob store closed")
	}()

	store.closed.Store(true)

	store.log.Info("Closing blobStore, wait for ongoing uploads to finish",
		zap.Int("remaining", int(store.uploadQueue.RunningWorkers())))

	fullyStopped := make(chan struct{}, 1)
	go func() {
		store.uploadQueue.StopAndWait()
		fullyStopped <- struct{}{}
	}()

	cancelTimeout := time.After(MaxCloseTimeout)
	quitTimeout := time.After(MaxCloseTimeout + 3*time.Second)

	for {
		select {
		case <-cancelTimeout:
			store.log.Warn("Timeout while waiting for ongoing uploads to finish, cancelling upload tasks",
				zap.Int("remaining", int(store.uploadQueue.RunningWorkers())))
			store.lifecycleClose()
		case <-quitTimeout:
			store.log.Warn("Failed to cancel ongoing uploads, forcefully quitting",
				zap.Int("remaining", int(store.uploadQueue.RunningWorkers())))
			return nil
		case <-fullyStopped:
			return nil
		}
	}
}

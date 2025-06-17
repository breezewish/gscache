package blob

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/breezewish/gscache/internal/log"
	"github.com/breezewish/gscache/internal/stats"
	"go.uber.org/zap"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
	"golang.org/x/sync/errgroup"
)

const (
	ArStoreMinSyncInterval = 5 * time.Second
	ArStoreDownloadTimeout = 10 * time.Second
	ArStoreUploadTimeout   = 10 * time.Second
)

// ArStore is the major access point for BlobArchive content.
// It connects a local BlobArchive store with a remote bucket.
// ArStore is shared for both cache read and compaction process
// so that BlobArchive files can be reused in the two processes.
type ArStore struct {
	opts  ArStoreOpts
	local *ArLocalStore

	muLastSync sync.RWMutex
	lastSyncAt map[string]time.Time
}

type ArStoreOpts struct {
	WorkDir              string
	Remote               *blob.Bucket
	AllPossibleKeyspaces []string
	SkipInitialSync      bool // If true, skip initial sync from remote to local.
}

func NewArStore(opts ArStoreOpts) (*ArStore, error) {
	local, err := NewArLocalStore(opts.WorkDir)
	if err != nil {
		return nil, err
	}
	if opts.Remote == nil {
		return nil, fmt.Errorf("remote bucket must not be nil")
	}
	arStore := &ArStore{
		opts:       opts,
		local:      local,
		lastSyncAt: make(map[string]time.Time),
	}
	_ = arStore.ForAllKeyspaces(func(keyspace string) error {
		defer stats.Default.Persist()
		stats.Default.BlobArchiveStore.LoadTotal.Inc()
		if err := local.LoadLocal(keyspace); err != nil {
			stats.Default.BlobArchiveStore.LoadFail.Inc()
			log.Warn("Failed to load local BlobArchive",
				zap.String("keyspace", keyspace),
				zap.Error(err))
		}
		return nil
	})
	if !opts.SkipInitialSync {
		_ = arStore.ForAllKeyspaces(func(keyspace string) error {
			if err := arStore.SyncFromRemote(keyspace); err != nil {
				log.Warn("failed to sync BlobArchive for keyspace",
					zap.String("keyspace", keyspace),
					zap.Error(err),
					zap.Stack("stack"))
			}
			return nil
		})
	}

	return arStore, nil
}

func (s *ArStore) ForAllKeyspaces(fn func(keyspace string) error) error {
	g := errgroup.Group{}
	for _, keyspace := range s.opts.AllPossibleKeyspaces {
		k := keyspace
		g.Go(func() error {
			return fn(k)
		})
	}
	return g.Wait()
}

// SyncFromRemote downloads the latest BlobArchive file from remote storage to local.
func (s *ArStore) SyncFromRemote(keyspace string) error {
	{
		// Skip syncing this keyspace if it has been synced recently.
		shouldSkipSync := false
		s.muLastSync.RLock()
		lastSync, ok := s.lastSyncAt[keyspace]
		if ok && time.Since(lastSync) < ArStoreMinSyncInterval {
			shouldSkipSync = true
		}
		s.muLastSync.RUnlock()
		if shouldSkipSync {
			return nil
		}
	}

	defer stats.Default.Persist()
	stats.Default.BlobArchiveStore.DownloadTotal.Inc()

	ctx, cancel := context.WithTimeout(context.Background(), ArStoreDownloadTimeout)
	defer cancel()
	blobReader, err := s.opts.Remote.NewReader(ctx, ArchiveKey(keyspace), nil)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			stats.Default.BlobArchiveStore.DownloadSkip.Inc()
			return nil
		}
		stats.Default.BlobArchiveStore.DownloadFail.Inc()
		return fmt.Errorf("failed to read %s: %w", ArchiveKey(keyspace), err)
	}
	err = s.local.Put(keyspace, blobReader)
	_ = blobReader.Close()
	if err != nil {
		stats.Default.BlobArchiveStore.DownloadFail.Inc()
		return err
	}

	stats.Default.BlobArchiveStore.DownloadSuccessBytes.Add(uint64(blobReader.Size()))
	{
		s.muLastSync.Lock()
		s.lastSyncAt[keyspace] = time.Now()
		s.muLastSync.Unlock()
	}
	return nil
}

// IngestNewArchive ingests an external BlobArchive file to both local and remote storage.
func (s *ArStore) IngestNewArchive(keyspace string, localFilePath string) error {
	file, err := os.Open(localFilePath)
	if err != nil {
		return fmt.Errorf("failed to open %s: %w", localFilePath, err)
	}
	defer file.Close()
	// First ingest locally to make sure the file is fine.
	err = s.local.Put(keyspace, file)
	if err != nil {
		return err
	}

	file2, _ := os.Open(localFilePath)
	defer file2.Close()
	ctx, cancel := context.WithTimeout(context.Background(), ArStoreUploadTimeout)
	defer cancel()
	err = s.opts.Remote.Upload(
		ctx,
		ArchiveKey(keyspace),
		file2,
		&blob.WriterOptions{
			ContentType: "application/octet-stream",
		})
	if err != nil {
		return fmt.Errorf("failed to upload %s to %s: %w", localFilePath, ArchiveKey(keyspace), err)
	}
	{
		s.muLastSync.Lock()
		s.lastSyncAt[keyspace] = time.Now()
		s.muLastSync.Unlock()
	}
	return nil
}

func (s *ArStore) GetArchive(keyspace string) *ArReader {
	return s.local.Get(keyspace)
}

func (s *ArStore) GetBlob(keyspace string, actionID []byte) *ArEntry {
	r := s.local.Get(keyspace)
	if r == nil {
		return nil
	}
	entry := r.Get(CacheEntityNameInArchive(actionID))
	if entry == nil {
		return nil
	}
	if !bytes.Equal(entry.ActionID, actionID) {
		log.Error("Meet corrupted BlobArchive entry",
			zap.String("keyspace", keyspace),
			zap.String("actionID", fmt.Sprintf("%x", actionID)),
			zap.String("actionIDFromAr", fmt.Sprintf("%x", entry.ActionID)))
		return nil
	}
	return entry
}

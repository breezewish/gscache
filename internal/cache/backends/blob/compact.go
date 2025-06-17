package blob

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/breezewish/gscache/internal/cache"
	"github.com/breezewish/gscache/internal/log"
	"github.com/breezewish/gscache/internal/protocol"
	"github.com/breezewish/gscache/internal/stats"
	"go.uber.org/zap"
	"gocloud.dev/blob"
)

const (
	// Only compact small blob files that are smaller than this size.
	// Note that all blob files contain a EntryMeta header, which is counted
	// in this size.
	CompactionSmallBlobSize = 1 * 1024 * 1024 // 1 MiB
	// Minimum number of new small blob files to trigger compaction
	CompactionAtLeastAddFiles = 10

	CompactionListFilesTimeout = 20 * time.Second
)

type compactItem struct {
	ActionID   []byte
	ObjectKey  string
	ObjectSize int64 // Size in the bucket, always includes the EntryMeta header
}

// CompactionJob compacts small blob files into larger ones in BlobArchive format.
// See `ar.go` for details about BlobArchive format.
//
// Compaction workflow:
// 1. Scan the prefix for all small blob files.
// 2. Download files that existing BlobArchive does not contain.
// 3. Generate a new BlobArchive file and upload.
// Compaction will not be triggered if new small blobs are few (<10).
//
// To avoid BlobArchive file grow infinitely, when source blob no longer exists,
// the entry in the BlobArchive will be also removed. This allows it to work
// with bucket lifecycle rules such as "delete after 30 days".
//
// Multiple compaction is allowed to run concurrently. Later upload of the
// new BlobArchive file will overwrite the existing one, so only the latest
// compaction will take effect.
//
// Each compactor only works for a single keyspace ('0' to 'f') to enable
// better parallelism (like LIST and GET).
type CompactionJob struct {
	opts CompactionJobOpts
	log  *zap.Logger

	// Fields below are filled during the compaction process.
	isSkipped              bool
	plannedList            []compactItem
	newArFile              *os.File  // Temporary file to store the new BlobArchive file
	newArFileWriter        *ArWriter // Writer to the new BlobArchive file
	nIncludedFiles         int
	nNewlyAddedFiles       int
	nNewlyAddedBytes       int
	nNewlyRemovedFiles     int // How many files are removed in the new archive
	elapsedFindBlobs       time.Duration
	elapsedDownload        time.Duration
	elapsedDownloadAndFill time.Duration
	elapsedIngest          time.Duration
}

type CompactionJobOpts struct {
	Keyspace    string   // Keyspace must be '0' to 'f'
	BlobArStore *ArStore // To figure out which small blob files are newly included
	BlobCache   *BlobBackend
	Remote      *blob.Bucket // Must not contain keyspace as the prefix
	Ctx         context.Context
}

func NewCompactionJob(opts CompactionJobOpts) *CompactionJob {
	return &CompactionJob{
		opts: opts,
		log: log.
			Named("blob.compactionJob").
			With(zap.String("keyspace", opts.Keyspace)),
	}
}

func (c *CompactionJob) cleanUp() {
	if c.newArFileWriter != nil {
		_ = c.newArFileWriter.Close()
		c.newArFileWriter = nil
	}
	if c.newArFile != nil {
		_ = c.newArFile.Close()
		_ = os.Remove(c.newArFile.Name())
		c.newArFile = nil
	}
}

func (c *CompactionJob) step1FindBlobsToCompact() (bool /* needCompact */, error) {
	t := time.Now()
	defer func() {
		c.elapsedFindBlobs = time.Since(t)
	}()

	iter := c.opts.Remote.List(&blob.ListOptions{
		Prefix:    ArchiveListPrefixKey(c.opts.Keyspace),
		Delimiter: "",
	})

	c.plannedList = make([]compactItem, 0)
	plannedTotalSize := int64(0)

	for {
		ctxList, cancel := context.WithTimeout(c.opts.Ctx, CompactionListFilesTimeout)
		obj, err := iter.Next(ctxList)
		cancel()
		if err == io.EOF {
			break
		}
		if err != nil {
			return false, fmt.Errorf("failed to list objects using prefix %s: %w",
				ArchiveListPrefixKey(c.opts.Keyspace),
				err)
		}
		if obj.IsDir {
			continue
		}
		if obj.Size >= CompactionSmallBlobSize {
			continue
		}
		actionID, err := DecodeCacheEntityKey(obj.Key)
		if err != nil {
			c.log.Warn("Skip object which does not seems to be a cache entry",
				zap.String("object", obj.Key))
			continue
		}
		c.log.Debug("Adding small blob to compact list",
			zap.String("object", obj.Key),
			zap.Int64("size", obj.Size),
			zap.String("actionID", fmt.Sprintf("%x", actionID)))
		c.plannedList = append(c.plannedList, compactItem{
			ActionID:   actionID,
			ObjectKey:  obj.Key,
			ObjectSize: obj.Size,
		})
		plannedTotalSize += obj.Size
	}
	if len(c.plannedList) == 0 {
		return false, nil
	}

	ar := c.opts.BlobArStore.GetArchive(c.opts.Keyspace)
	c.nNewlyAddedFiles = 0
	if ar != nil {
		for _, item := range c.plannedList {
			if ar.Get(CacheEntityNameInArchive(item.ActionID)) == nil {
				c.nNewlyAddedFiles++
				c.nNewlyAddedBytes += int(item.ObjectSize)
			}
		}
		// Also count how many files are removed in the new archive for statistics.
		finalArchiveNames := make(map[string]struct{})
		for _, item := range c.plannedList {
			finalArchiveNames[CacheEntityNameInArchive(item.ActionID)] = struct{}{}
		}
		for _, name := range ar.List() {
			if _, ok := finalArchiveNames[name]; !ok {
				c.nNewlyRemovedFiles++
			}
		}
	} else {
		c.nNewlyAddedFiles = len(c.plannedList)
		c.nNewlyAddedBytes = int(plannedTotalSize)
		c.nNewlyRemovedFiles = 0
	}

	if c.nNewlyAddedFiles < CompactionAtLeastAddFiles {
		return false, nil
	}

	stats.Default.BlobCompactor.BlobAddTotal.Add(uint32(c.nNewlyAddedFiles))
	stats.Default.BlobCompactor.BlobAddTotalBytes.Add(uint64(c.nNewlyAddedBytes))
	stats.Default.BlobCompactor.BlobRemoveTotal.Add(uint32(c.nNewlyRemovedFiles))
	stats.Default.Persist()

	c.log.Info("Finish listing small blob files",
		zap.Int("planned", len(c.plannedList)),
		zap.Int("newlyAdded", c.nNewlyAddedFiles),
		zap.Int("newlyAddedBytes", c.nNewlyAddedBytes),
		zap.Int("newlyRemoved", c.nNewlyRemovedFiles),
		zap.Int64("totalSize", plannedTotalSize))
	return true, nil
}

func (c *CompactionJob) step2DownloadAndFill() error {
	t := time.Now()
	defer func() {
		c.elapsedDownloadAndFill = time.Since(t)
	}()

	newArFile, err := os.CreateTemp("", "gscache_compact.*.zip")
	if err != nil {
		return fmt.Errorf("failed to create file for new BlobArchive: %w", err)
	}
	c.newArFile = newArFile
	c.newArFileWriter = NewArWriter(newArFile)

	// for an ActionID, it may be available in local cache, or in BlobArchive store,
	// or only in the remote bucket. In any case, we will always retrieve it
	// via BlobBackend because BlobBackend covers all these cases. Additionally,
	// when ActionID is downloaded in the BlobBackend, it can be immediately
	// available to GET requests.

	// In this step, we concurrently trigger GET requests to BlobBackend (result
	// in a LocalPath) and collect them in a channel. The channel is then
	// processed by a single goroutine to fill the new BlobArchive file.
	// This is because we could only have a single writer to the new
	// BlobArchive file.

	type result struct {
		compactItem
		resp *protocol.GetResponse
	}

	resultQueue := make(chan result, len(c.plannedList))
	getQueue := pond.NewPool(32, pond.WithContext(c.opts.Ctx))

	arWriteFinish := make(chan struct{})
	go func() {
		defer close(arWriteFinish)
		// This goroutine will fill the new BlobArchive file
		for {
			r, ok := <-resultQueue
			if !ok {
				// No result any more
				return
			}
			objLogger := c.log.With(
				zap.String("actionID", fmt.Sprintf("%x", r.ActionID)),
				zap.String("object", r.ObjectKey),
				zap.String("diskPath", r.resp.DiskPath))
			data, err := os.ReadFile(r.resp.DiskPath)
			if err != nil {
				objLogger.Warn("Failed to open local cache file for adding to new BlobArchive",
					zap.Error(err))
				stats.Default.BlobCompactor.BlobSkipForIOFailure.Inc()
				stats.Default.Persist()
				continue
			}
			{
				// Do some verification for the local cache file. This is going to be uploaded
				// to the remote bucket so we want to make sure it is valid.
				if len(data) != int(r.resp.Size) {
					objLogger.Warn("Corrupted local cache file",
						zap.Int64("sizeInMeta", r.resp.Size),
						zap.Int("actualSize", len(data)))
					stats.Default.BlobCompactor.BlobSkipForCorrupted.Inc()
					stats.Default.Persist()
					continue
				}
				meta := cache.EntryMeta{
					ActionID: r.ActionID,
					OutputID: r.resp.OutputID,
					Size:     r.resp.Size,
					Time:     *r.resp.Time,
				}
				metaSize := meta.SerializedSize()
				localObjSize := metaSize + len(data)
				if localObjSize != int(r.ObjectSize) {
					objLogger.Warn("Corrupted local cache file",
						zap.Int64("sizeInRemote", r.ObjectSize),
						zap.Int("actualSize", localObjSize))
					stats.Default.BlobCompactor.BlobSkipForCorrupted.Inc()
					stats.Default.Persist()
					continue
				}
			}
			err = c.newArFileWriter.Add(
				CacheEntityNameInArchive(r.ActionID),
				cache.EntryMeta{
					ActionID: r.ActionID,
					OutputID: r.resp.OutputID,
					Size:     r.resp.Size,
					Time:     *r.resp.Time,
				}, data)
			if err != nil {
				objLogger.Warn("Failed to add blob file to new BlobArchive", zap.Error(err))
				stats.Default.BlobCompactor.BlobSkipForIOFailure.Inc()
				stats.Default.Persist()
			}
			c.nIncludedFiles++
		}
	}()

	tDownload := time.Now()

	for _, item2 := range c.plannedList {
		item := item2
		_ = getQueue.Go(func() {
			resp, err := c.opts.BlobCache.Get(cache.GetOpts{
				Req: protocol.GetRequest{
					ActionID: item.ActionID,
				},
				IsInCompaction: true,
			})
			objLogger := c.log.With(
				zap.String("actionID", fmt.Sprintf("%x", item.ActionID)),
				zap.String("object", item.ObjectKey))
			if err != nil {
				objLogger.Warn("Failed to get blob file", zap.Error(err))
				stats.Default.BlobCompactor.BlobSkipForOther.Inc()
				stats.Default.Persist()
				return
			}
			if resp.Miss {
				// Maybe deleted after LIST
				objLogger.Warn("Blob file in list but not found, skip")
				stats.Default.BlobCompactor.BlobSkipForMissing.Inc()
				stats.Default.Persist()
				return
			}
			resultQueue <- result{item, resp}
		})
	}

	getQueue.StopAndWait()
	close(resultQueue)

	c.elapsedDownload = time.Since(tDownload)

	<-arWriteFinish

	c.log.Info("Finish writing new BlobArchive file",
		zap.Int("nPlannedFiles", len(c.plannedList)),
		zap.Int("nIncludedFiles", c.nIncludedFiles),
		zap.String("downloadCost", c.elapsedDownload.String()))

	return nil
}

func (c *CompactionJob) step3IngestNewArFile() error {
	if err := c.newArFileWriter.Close(); err != nil {
		return err
	}
	if err := c.newArFile.Close(); err != nil {
		return err
	}
	t := time.Now()
	if err := c.opts.BlobArStore.IngestNewArchive(c.opts.Keyspace, c.newArFile.Name()); err != nil {
		return err
	}
	c.elapsedIngest = time.Since(t)
	c.log.Info("Finish ingesting new BlobArchive file",
		zap.String("cost", c.elapsedIngest.String()))
	return nil
}

func (c *CompactionJob) work() error {
	defer c.cleanUp()
	c.log.Debug("Starting compaction")
	if err := c.opts.BlobArStore.SyncFromRemote(c.opts.Keyspace); err != nil {
		c.log.Warn("Failed to sync BlobArchive", zap.Error(err))
	}
	needCompact, err := c.step1FindBlobsToCompact()
	if err != nil {
		return fmt.Errorf("failed to find blobs to compact: %w", err)
	}
	if !needCompact {
		c.log.Info("Not enough new small blob files to compact, skip compaction",
			zap.Int("planned", len(c.plannedList)),
			zap.Int("newlyAdded", c.nNewlyAddedFiles),
			zap.Int("newlyAddedBytes", c.nNewlyAddedBytes),
			zap.Int("minRequired", CompactionAtLeastAddFiles))
		c.isSkipped = true
		return nil
	}
	err = c.step2DownloadAndFill()
	if err != nil {
		return fmt.Errorf("failed to download and fill new BlobArchive file: %w", err)
	}
	err = c.step3IngestNewArFile()
	if err != nil {
		return fmt.Errorf("failed to ingest new BlobArchive file: %w", err)
	}
	return nil
}

func (c *CompactionJob) Work() {
	defer stats.Default.Persist()
	stats.Default.BlobCompactor.Total.Inc()

	t := time.Now()
	if err := c.work(); err != nil {
		stats.Default.BlobCompactor.Fail.Inc()
		c.log.Error("Compaction job failed",
			zap.Int("nPlannedFiles", len(c.plannedList)),
			zap.String("costJob", time.Since(t).String()),
			zap.Error(err))
	} else {
		if c.isSkipped {
			stats.Default.BlobCompactor.Skip.Inc()
		} else {
			stats.Default.BlobCompactor.Success.Inc()
		}
		c.log.Info("Compaction job finished",
			zap.Bool("isSkipped", c.isSkipped),
			zap.Int("nPlannedFiles", len(c.plannedList)),
			zap.Int("nIncludedFiles", c.nIncludedFiles),
			zap.String("costJob", time.Since(t).String()),
			zap.String("costFindBlobs", c.elapsedFindBlobs.String()),
			zap.String("costDownloadAndFill", c.elapsedDownloadAndFill.String()),
			zap.String("costDownload", c.elapsedDownload.String()),
			zap.String("costIngest", c.elapsedIngest.String()))
	}
}

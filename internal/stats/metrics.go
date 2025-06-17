package stats

import (
	"sync"
	"time"

	"go.uber.org/atomic"
)

type BlobMetrics struct {
	GetByLocal          atomic.Uint32 `json:"Get.ByLocal"`
	GetByArchive        atomic.Uint32 `json:"Get.ByArchive"`
	GetByDownload       atomic.Uint32 `json:"Get.ByDownload"`
	DownloadBytes       atomic.Uint64 `json:"Download.Bytes"`
	UploadedFiles       atomic.Uint32 `json:"Uploaded.Files"`
	UploadedBytes       atomic.Uint64 `json:"Uploaded.Bytes"`
	ArchiveToLocalFiles atomic.Uint32 `json:"Archive.ToLocal.Files"` // How many small blobs are copied from archive to local store.
	ArchiveToLocalBytes atomic.Uint64 `json:"Archive.ToLocal.Bytes"`
}

func (m *BlobMetrics) Clear() {
	m.GetByLocal.Store(0)
	m.GetByArchive.Store(0)
	m.GetByDownload.Store(0)
	m.DownloadBytes.Store(0)
	m.UploadedFiles.Store(0)
	m.UploadedBytes.Store(0)
	m.ArchiveToLocalFiles.Store(0)
	m.ArchiveToLocalBytes.Store(0)
}

type BlobCompactorMetrics struct {
	Total                atomic.Uint32 `json:"Total"` // Note: Each namespace compact will be counted as 1.
	Success              atomic.Uint32 `json:"Success"`
	Skip                 atomic.Uint32 `json:"Skip"`
	Fail                 atomic.Uint32 `json:"Fail"`
	BlobAddTotal         atomic.Uint32 `json:"SmallBlob.Add.Total"` // How many small blobs files are newly added to the archive.
	BlobAddTotalBytes    atomic.Uint64 `json:"SmallBlob.Add.TotalBytes"`
	BlobRemoveTotal      atomic.Uint32 `json:"SmallBlob.Remove.Total"`      // How many small blobs files are removed from the archive due to remote removal.
	BlobSkipForIOFailure atomic.Uint32 `json:"SmallBlob.SkipFor.IOFailure"` // How many small blobs files are planned but skipped due to IO failure.
	BlobSkipForCorrupted atomic.Uint32 `json:"SmallBlob.SkipFor.Corrupted"` // How many small blobs files are planned but skipped due to corrupted.
	BlobSkipForMissing   atomic.Uint32 `json:"SmallBlob.SkipFor.Missing"`   // How many small blobs files are planned but skipped due to missing after LIST.
	BlobSkipForOther     atomic.Uint32 `json:"SmallBlob.SkipFor.Other"`     // How many small blobs files are planned but skipped for other reasons.
}

func (m *BlobCompactorMetrics) Clear() {
	m.Total.Store(0)
	m.Success.Store(0)
	m.Skip.Store(0)
	m.Fail.Store(0)
	m.BlobAddTotal.Store(0)
	m.BlobAddTotalBytes.Store(0)
	m.BlobRemoveTotal.Store(0)
	m.BlobSkipForIOFailure.Store(0)
	m.BlobSkipForCorrupted.Store(0)
	m.BlobSkipForMissing.Store(0)
	m.BlobSkipForOther.Store(0)
}

type BlobArchiveStoreMetrics struct {
	DownloadTotal        atomic.Uint32 `json:"Download.Total"` // How many archives are downloaded from remote.
	DownloadFail         atomic.Uint32 `json:"Download.Fail"`
	DownloadSkip         atomic.Uint32 `json:"Download.Skip"`
	DownloadSuccessBytes atomic.Uint64 `json:"Download.Success.Bytes"`
	LoadTotal            atomic.Uint32 `json:"Load.Total"` // How many archives are loaded from local store.
	LoadFail             atomic.Uint32 `json:"Load.Fail"`
}

func (m *BlobArchiveStoreMetrics) Clear() {
	m.DownloadTotal.Store(0)
	m.DownloadFail.Store(0)
	m.DownloadSkip.Store(0)
	m.DownloadSuccessBytes.Store(0)
	m.LoadTotal.Store(0)
	m.LoadFail.Store(0)
}

type Metrics struct {
	GetTotal         atomic.Uint32           `json:"Get.Total"`
	GetHit           atomic.Uint32           `json:"Get.Hit"`
	GetMiss          atomic.Uint32           `json:"Get.Miss"`
	GetError         atomic.Uint32           `json:"Get.Error"`
	PutTotal         atomic.Uint32           `json:"Put.Total"`
	PutError         atomic.Uint32           `json:"Put.Error"`
	BlobOrganic      BlobMetrics             `json:"Blob.FromOrganic"`
	BlobCompaction   BlobMetrics             `json:"Blob.FromCompaction"`
	BlobCompactor    BlobCompactorMetrics    `json:"Blob.Compactor"`
	BlobArchiveStore BlobArchiveStoreMetrics `json:"Blob.ArchiveStore"`

	// =================================================================================
	// Fields below are only for flushing stats to disk.
	// Stats can be either "inmemory" or "ondisk"
	// - inmemory: stats are only in memory, changes are not persisted to disk.
	// - ondisk:   stats are load from a disk file and changes are persisted to disk.
	diskPath       string // If set, stats are ondisk.
	mu             sync.Mutex
	lastPersistAt  time.Time
	pendingPersist *time.Timer
}

func (m *Metrics) Clear() {
	m.GetTotal.Store(0)
	m.GetHit.Store(0)
	m.GetMiss.Store(0)
	m.GetError.Store(0)
	m.PutTotal.Store(0)
	m.PutError.Store(0)
	m.BlobOrganic.Clear()
	m.BlobCompaction.Clear()
	m.BlobCompactor.Clear()
	m.BlobArchiveStore.Clear()
}

var Default = NewMetrics()

func (m *Metrics) GetBlobMetrics(isCompaction bool) *BlobMetrics {
	if isCompaction {
		return &m.BlobCompaction
	}
	return &m.BlobOrganic
}

func NewMetrics() *Metrics {
	m := &Metrics{}
	m.Clear()
	return m
}

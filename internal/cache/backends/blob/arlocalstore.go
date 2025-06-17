package blob

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	gonanoid "github.com/matoous/go-nanoid/v2"
)

// ArLocalStore manages multiple BlobArchive readers for different keyspaces locally.
// It is concurrent-safe.
// Load = Load local archive file from workDir and make it available for reading.
// Put = Write local archive file in workDir and make it available for reading.
// Get = Read a BlobArchive
type ArLocalStore struct {
	workDir string

	mu      sync.RWMutex
	readers map[string]*ArReader // key=keyspace
}

func NewArLocalStore(workDir string) (*ArLocalStore, error) {
	if workDir == "" {
		return nil, fmt.Errorf("workDir must be set")
	}
	arDir := filepath.Dir(ArchiveFilePath(workDir, "a"))
	err := os.MkdirAll(arDir, 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", arDir, err)
	}
	return &ArLocalStore{
		workDir: workDir,
		readers: make(map[string]*ArReader),
	}, nil
}

func (s *ArLocalStore) LoadLocal(keyspace string) error {
	filePath := ArchiveFilePath(s.workDir, keyspace)
	if _, err := os.Stat(filePath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	arReader, err := NewArReader(filePath)
	if err != nil {
		// file is broken for some reason
		return fmt.Errorf("failed to read as BlobArchive %s: %w", filePath, err)
	}
	s.set(keyspace, arReader)
	return nil
}

func (s *ArLocalStore) Put(keyspace string, r io.Reader) error {
	// The step is designed to make sure write and read operations are not contending with each other.
	// Note: only works in UNIX-like systems.
	// 1. Write to a temporary file.
	// 2. Open the file.
	// 3. Rename the file to the final name (this keep original readers working).

	// 1
	uniqueId := gonanoid.Must(8)
	newFilePath := ArchiveFilePath(s.workDir, keyspace)
	newFilePathTmp := newFilePath + ".tmp." + uniqueId

	_ = os.MkdirAll(filepath.Dir(newFilePathTmp), 0755)
	newFile, err := os.Create(newFilePathTmp)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", newFilePathTmp, err)
	}
	_, err = io.Copy(newFile, r)
	if err != nil {
		_ = newFile.Close()
		_ = os.Remove(newFilePathTmp)
		return fmt.Errorf("failed to write to file %s: %w", newFilePathTmp, err)
	}
	_ = newFile.Close()

	// 2
	arReader, err := NewArReader(newFilePathTmp)
	if err != nil {
		_ = os.Remove(newFilePathTmp)
		return fmt.Errorf("failed to read as BlobArchive: %w", err)
	}

	// 3
	err = os.Rename(newFilePathTmp, newFilePath)
	if err != nil {
		_ = arReader.Close()
		_ = os.Remove(newFilePathTmp)
		return fmt.Errorf("failed to rename file %s to %s: %w", newFilePathTmp, newFilePath, err)
	}

	// 4. Make new reader available for further reading.
	s.set(keyspace, arReader)

	return nil
}

func (s *ArLocalStore) set(keyspace string, arReader *ArReader) {
	// We will use finalizer to close the reader when reader is no longer used.
	runtime.SetFinalizer(arReader, func(r *ArReader) {
		_ = r.Close()
	})
	s.mu.Lock()
	// Old reader is not closed now, because it may still be in use.
	// Close will be called by the finalizer when the reader is no longer used.
	s.readers[keyspace] = arReader
	s.mu.Unlock()
}

func (s *ArLocalStore) Get(keyspace string) *ArReader {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if r, ok := s.readers[keyspace]; ok {
		return r
	}
	return nil
}

package blob

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"

	"github.com/breezewish/gscache/internal/cache"
)

type ArEntry struct {
	cache.EntryMeta
	f *zip.File
}

func (e *ArEntry) Open() (io.ReadCloser, error) {
	r, err := e.f.Open()
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s in BlobArchive: %w", e.f.Name, err)
	}
	return r, nil
}

// ArReader reads a BlobArchive file, and is concurrent-safe.
// BlobArchive file is a collection of small blob files stored in a zip archive.
// The zip format is only used for convenience. Compression is not the main purpose.
type ArReader struct {
	z     *zip.ReadCloser
	files map[string]ArEntry // Map of file names to cache entries.
}

func NewArReader(path string) (*ArReader, error) {
	z, err := zip.OpenReader(path)
	if err != nil {
		return nil, err
	}
	files := make(map[string]ArEntry)
	for _, f := range z.File {
		var meta cache.EntryMeta
		if err := json.Unmarshal([]byte(f.Comment), &meta); err != nil {
			return nil, fmt.Errorf("failed to unmarshal entry meta from file comment %s: %w", f.Name, err)
		}
		// For compatibility, we use JSON format instead
		// of binary format to store EntryMeta in the comment.
		files[f.Name] = ArEntry{meta, f}
	}
	return &ArReader{z, files}, nil
}

func (r *ArReader) Get(name string) *ArEntry {
	if entry, ok := r.files[name]; ok {
		return &entry
	}
	return nil
}

func (r *ArReader) List() []string {
	names := make([]string, 0, len(r.files))
	for name := range r.files {
		names = append(names, name)
	}
	return names
}

func (r *ArReader) Close() error {
	r.files = nil
	return r.z.Close()
}

type ArWriter struct {
	z *zip.Writer
}

func NewArWriter(w io.Writer) *ArWriter {
	zW := zip.NewWriter(w)
	return &ArWriter{zW}
}

func (w *ArWriter) Add(name string, meta cache.EntryMeta, data []byte) error {
	// Intentionally accept a data buffer instead of a reader, so that we can
	// verify data size matching meta before writing to the zip archive.
	// The archive is supposed to contain small blob files, so it should be fine.
	if len(data) != int(meta.Size) {
		return fmt.Errorf("size mismatch for file %s: expected %d according to meta, got %d", name, meta.Size, len(data))
	}

	comment, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("failed to marshal entry meta for file %s: %w", name, err)
	}
	f, err := w.z.CreateHeader(&zip.FileHeader{
		Name:    name,
		Method:  zip.Deflate,
		Comment: string(comment),
	})
	if err != nil {
		return fmt.Errorf("failed to create file %s in BlobArchive: %w", name, err)
	}
	n, err := f.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write data for file %s: %w", name, err)
	}
	if n != len(data) {
		return fmt.Errorf("failed to write all data for file %s: expected %d bytes, wrote %d bytes", name, len(data), n)
	}
	return nil
}

func (w *ArWriter) Close() error {
	return w.z.Close()
}

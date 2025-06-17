package blob

import (
	"bytes"
	"io"
	"os"
	"testing"
	"time"

	"github.com/breezewish/gscache/internal/cache"
	"github.com/stretchr/testify/require"
)

func createBlobar(entries map[string][]byte) *bytes.Reader {
	var buf bytes.Buffer
	writer := NewArWriter(&buf)
	for name, data := range entries {
		meta := cache.EntryMeta{
			ActionID: []byte("action_" + name),
			OutputID: []byte("output_" + name),
			Size:     int64(len(data)),
			Time:     time.Now(),
		}
		err := writer.Add(name, meta, data)
		if err != nil {
			panic(err)
		}
	}
	err := writer.Close()
	if err != nil {
		panic(err)
	}
	return bytes.NewReader(buf.Bytes())
}

func TestArLocalStore_Put_And_Get(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "arlocalstore_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store, err := NewArLocalStore(tmpDir)
	require.NoError(t, err)

	keyspace := "a"
	testEntries := map[string][]byte{
		"file1.txt": []byte("content1"),
		"file2.bin": []byte("binary content"),
		"empty":     []byte{},
	}

	archiveReader := createBlobar(testEntries)
	err = store.Put(keyspace, archiveReader)
	require.NoError(t, err)

	arReader := store.Get(keyspace)
	require.NotNil(t, arReader)

	// Verify file1.txt
	entry := arReader.Get("file1.txt")
	require.NotNil(t, entry)
	require.Equal(t, []byte("action_file1.txt"), entry.ActionID)
	require.Equal(t, []byte("output_file1.txt"), entry.OutputID)
	require.Equal(t, int64(8), entry.Size)
	rc, err := entry.Open()
	require.NoError(t, err)
	actualData, err := io.ReadAll(rc)
	require.NoError(t, err)
	rc.Close()
	require.Equal(t, []byte("content1"), actualData)

	// Verify file2.bin
	entry = arReader.Get("file2.bin")
	require.NotNil(t, entry)
	require.Equal(t, []byte("action_file2.bin"), entry.ActionID)
	require.Equal(t, []byte("output_file2.bin"), entry.OutputID)
	require.Equal(t, int64(14), entry.Size)
	rc, err = entry.Open()
	require.NoError(t, err)
	actualData, err = io.ReadAll(rc)
	require.NoError(t, err)
	rc.Close()
	require.Equal(t, []byte("binary content"), actualData)

	// Verify empty file
	entry = arReader.Get("empty")
	require.NotNil(t, entry)
	require.Equal(t, []byte("action_empty"), entry.ActionID)
	require.Equal(t, []byte("output_empty"), entry.OutputID)
	require.Equal(t, int64(0), entry.Size)
	rc, err = entry.Open()
	require.NoError(t, err)
	actualData, err = io.ReadAll(rc)
	require.NoError(t, err)
	rc.Close()
	require.Equal(t, []byte{}, actualData)
}

func TestArLocalStore_LoadLocal_After_Put(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "arlocalstore_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// First instance: Put an archive
	store1, err := NewArLocalStore(tmpDir)
	require.NoError(t, err)

	keyspace := "b"
	testEntries := map[string][]byte{
		"persistent.txt": []byte("this should persist"),
		"data.bin":       bytes.Repeat([]byte("x"), 100),
	}

	archiveReader := createBlobar(testEntries)
	err = store1.Put(keyspace, archiveReader)
	require.NoError(t, err)

	// Create a second instance in the same workDir
	store2, err := NewArLocalStore(tmpDir)
	require.NoError(t, err)

	// Initially, Get should return nil since LoadLocal hasn't been called
	arReader := store2.Get(keyspace)
	require.Nil(t, arReader)

	// Load the archive from disk
	err = store2.LoadLocal(keyspace)
	require.NoError(t, err)

	// Now Get should return the archive
	arReader = store2.Get(keyspace)
	require.NotNil(t, arReader)

	// Verify persistent.txt
	entry := arReader.Get("persistent.txt")
	require.NotNil(t, entry)
	rc, err := entry.Open()
	require.NoError(t, err)
	actualData, err := io.ReadAll(rc)
	require.NoError(t, err)
	rc.Close()
	require.Equal(t, []byte("this should persist"), actualData)

	// Verify data.bin
	entry = arReader.Get("data.bin")
	require.NotNil(t, entry)
	rc, err = entry.Open()
	require.NoError(t, err)
	actualData, err = io.ReadAll(rc)
	require.NoError(t, err)
	rc.Close()
	require.Equal(t, bytes.Repeat([]byte("x"), 100), actualData)
}

func TestArLocalStore_Get_NonExistentKeyspace(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "arlocalstore_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store, err := NewArLocalStore(tmpDir)
	require.NoError(t, err)

	// Get a keyspace that doesn't exist
	arReader := store.Get("nonexistent")
	require.Nil(t, arReader)

	// Try LoadLocal on a keyspace that doesn't have a file
	err = store.LoadLocal("nonexistent")
	require.NoError(t, err) // Should not error, just do nothing

	// Get should still return nil
	arReader = store.Get("nonexistent")
	require.Nil(t, arReader)
}

func TestArLocalStore_Override_Keyspace(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "arlocalstore_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store, err := NewArLocalStore(tmpDir)
	require.NoError(t, err)

	keyspace := "c"

	// First archive
	firstEntries := map[string][]byte{
		"original.txt": []byte("original content"),
	}
	archiveReader1 := createBlobar(firstEntries)
	err = store.Put(keyspace, archiveReader1)
	require.NoError(t, err)

	// Get the first archive reader
	originalReader := store.Get(keyspace)
	require.NotNil(t, originalReader)

	// Verify original content is accessible
	entry := originalReader.Get("original.txt")
	require.NotNil(t, entry)
	rc, err := entry.Open()
	require.NoError(t, err)
	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	rc.Close()
	require.Equal(t, []byte("original content"), data)

	// Second archive (override)
	secondEntries := map[string][]byte{
		"new.txt":     []byte("new content"),
		"another.bin": []byte("another file"),
	}
	archiveReader2 := createBlobar(secondEntries)
	err = store.Put(keyspace, archiveReader2)
	require.NoError(t, err)

	// Get the new archive reader
	newReader := store.Get(keyspace)
	require.NotNil(t, newReader)
	require.NotEqual(t, originalReader, newReader) // Should be different instances

	// Verify new content is accessible via new reader
	entry = newReader.Get("new.txt")
	require.NotNil(t, entry)
	rc, err = entry.Open()
	require.NoError(t, err)
	data, err = io.ReadAll(rc)
	require.NoError(t, err)
	rc.Close()
	require.Equal(t, []byte("new content"), data)

	// Verify old file doesn't exist in new reader
	entry = newReader.Get("original.txt")
	require.Nil(t, entry)

	// Verify original reader is STILL working and can read original content
	entry = originalReader.Get("original.txt")
	require.NotNil(t, entry, "Original reader should still work after override")
	rc, err = entry.Open()
	require.NoError(t, err)
	data, err = io.ReadAll(rc)
	require.NoError(t, err)
	rc.Close()
	require.Equal(t, []byte("original content"), data, "Original reader should still return original content")

	// Verify original reader can't see new files
	entry = originalReader.Get("new.txt")
	require.Nil(t, entry, "Original reader should not see new files")
}

func TestArLocalStore_LoadLocal_NonExistentFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "arlocalstore_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store, err := NewArLocalStore(tmpDir)
	require.NoError(t, err)

	// Try to load a keyspace that doesn't have a file on disk
	err = store.LoadLocal("nonexistent")
	require.NoError(t, err) // Should not error

	// Get should return nil
	arReader := store.Get("nonexistent")
	require.Nil(t, arReader)
}

func TestArLocalStore_MultipleKeyspaces(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "arlocalstore_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store, err := NewArLocalStore(tmpDir)
	require.NoError(t, err)

	// Put archives in different keyspaces
	// Keyspace "0"
	archiveReader0 := createBlobar(map[string][]byte{
		"file.txt": []byte("content for keyspace 0"),
	})
	err = store.Put("0", archiveReader0)
	require.NoError(t, err)

	arReader0 := store.Get("0")
	require.NotNil(t, arReader0)
	entry := arReader0.Get("file.txt")
	require.NotNil(t, entry)
	rc, err := entry.Open()
	require.NoError(t, err)
	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	rc.Close()
	require.Equal(t, []byte("content for keyspace 0"), data)

	// Keyspace "1"
	archiveReader1 := createBlobar(map[string][]byte{
		"file.txt": []byte("content for keyspace 1"),
	})
	err = store.Put("1", archiveReader1)
	require.NoError(t, err)

	arReader1 := store.Get("1")
	require.NotNil(t, arReader1)
	entry = arReader1.Get("file.txt")
	require.NotNil(t, entry)
	rc, err = entry.Open()
	require.NoError(t, err)
	data, err = io.ReadAll(rc)
	require.NoError(t, err)
	rc.Close()
	require.Equal(t, []byte("content for keyspace 1"), data)

	// Verify keyspace "0" is still accessible
	arReader0 = store.Get("0")
	require.NotNil(t, arReader0)

	// Keyspace "a"
	archiveReaderA := createBlobar(map[string][]byte{
		"file.txt": []byte("content for keyspace a"),
	})
	err = store.Put("a", archiveReaderA)
	require.NoError(t, err)

	arReaderA := store.Get("a")
	require.NotNil(t, arReaderA)
	entry = arReaderA.Get("file.txt")
	require.NotNil(t, entry)
	rc, err = entry.Open()
	require.NoError(t, err)
	data, err = io.ReadAll(rc)
	require.NoError(t, err)
	rc.Close()
	require.Equal(t, []byte("content for keyspace a"), data)

	// Verify previous keyspaces are still accessible
	arReader0 = store.Get("0")
	require.NotNil(t, arReader0)
	arReader1 = store.Get("1")
	require.NotNil(t, arReader1)

	// Keyspace "f"
	archiveReaderF := createBlobar(map[string][]byte{
		"file.txt": []byte("content for keyspace f"),
	})
	err = store.Put("f", archiveReaderF)
	require.NoError(t, err)

	arReaderF := store.Get("f")
	require.NotNil(t, arReaderF)
	entry = arReaderF.Get("file.txt")
	require.NotNil(t, entry)
	rc, err = entry.Open()
	require.NoError(t, err)
	data, err = io.ReadAll(rc)
	require.NoError(t, err)
	rc.Close()
	require.Equal(t, []byte("content for keyspace f"), data)

	// Verify all previous keyspaces are still accessible
	arReader0 = store.Get("0")
	require.NotNil(t, arReader0)
	arReader1 = store.Get("1")
	require.NotNil(t, arReader1)
	arReaderA = store.Get("a")
	require.NotNil(t, arReaderA)
}

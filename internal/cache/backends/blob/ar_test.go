package blob

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/breezewish/gscache/internal/cache"
	"github.com/stretchr/testify/require"
)

func TestArWriter_ArReader_RoundTrip(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "ar_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	archivePath := filepath.Join(tmpDir, "test.ar")

	func() {
		file, err := os.Create(archivePath)
		require.NoError(t, err)
		defer file.Close()

		writer := NewArWriter(file)
		defer writer.Close()

		err = writer.Add("small.txt", cache.EntryMeta{
			ActionID: []byte("action1"),
			OutputID: []byte("output1"),
			Size:     5,
			Time:     time.Unix(1640995200, 0),
		}, []byte("small"))
		require.NoError(t, err)

		err = writer.Add("medium.bin", cache.EntryMeta{
			ActionID: []byte("action2"),
			OutputID: []byte("output2"),
			Size:     1024,
			Time:     time.Unix(1640995260, 0),
		}, bytes.Repeat([]byte("x"), 1024))
		require.NoError(t, err)

		err = writer.Add("empty.dat", cache.EntryMeta{
			ActionID: []byte("action3"),
			OutputID: []byte("output3"),
			Size:     0,
			Time:     time.Unix(1640995320, 0),
		}, []byte{})
		require.NoError(t, err)
	}()

	reader, err := NewArReader(archivePath)
	require.NoError(t, err)
	defer reader.Close()

	listedNames := reader.List()
	require.Len(t, listedNames, 3)
	require.Contains(t, listedNames, "small.txt")
	require.Contains(t, listedNames, "medium.bin")
	require.Contains(t, listedNames, "empty.dat")

	// Check small.txt
	entry := reader.Get("small.txt")
	require.NotNil(t, entry)
	require.Equal(t, []byte("action1"), entry.ActionID)
	require.Equal(t, []byte("output1"), entry.OutputID)
	require.Equal(t, int64(5), entry.Size)
	require.True(t, time.Unix(1640995200, 0).Equal(entry.Time))
	rc, err := entry.Open()
	require.NoError(t, err)
	data, err := bytes.NewBuffer(nil), error(nil)
	_, err = data.ReadFrom(rc)
	require.NoError(t, err)
	rc.Close()
	require.Equal(t, []byte("small"), data.Bytes())

	// Check medium.bin
	entry = reader.Get("medium.bin")
	require.NotNil(t, entry)
	require.Equal(t, []byte("action2"), entry.ActionID)
	require.Equal(t, []byte("output2"), entry.OutputID)
	require.Equal(t, int64(1024), entry.Size)
	require.True(t, time.Unix(1640995260, 0).Equal(entry.Time))
	rc, err = entry.Open()
	require.NoError(t, err)
	data, err = bytes.NewBuffer(nil), error(nil)
	_, err = data.ReadFrom(rc)
	require.NoError(t, err)
	rc.Close()
	require.Equal(t, bytes.Repeat([]byte("x"), 1024), data.Bytes())

	// Check empty.dat
	entry = reader.Get("empty.dat")
	require.NotNil(t, entry)
	require.Equal(t, []byte("action3"), entry.ActionID)
	require.Equal(t, []byte("output3"), entry.OutputID)
	require.Equal(t, int64(0), entry.Size)
	require.True(t, time.Unix(1640995320, 0).Equal(entry.Time))
	rc, err = entry.Open()
	require.NoError(t, err)
	data, err = bytes.NewBuffer(nil), error(nil)
	_, err = data.ReadFrom(rc)
	require.NoError(t, err)
	rc.Close()
	require.Equal(t, []byte{}, data.Bytes())

	// Test non-existent entry
	nonExistent := reader.Get("non_existent_file.txt")
	require.Nil(t, nonExistent)
}

func TestArWriter_SizeMismatch(t *testing.T) {
	var buf bytes.Buffer
	writer := NewArWriter(&buf)
	defer writer.Close()

	meta := cache.EntryMeta{
		ActionID: []byte("action"),
		OutputID: []byte("output"),
		Size:     10, // Expect 10 bytes
		Time:     time.Now(),
	}
	data := []byte("hello") // Only 5 bytes

	err := writer.Add("test.txt", meta, data)
	require.Error(t, err)
	require.Contains(t, err.Error(), "size mismatch")
}

func TestArReader_InvalidPath(t *testing.T) {
	reader, err := NewArReader("/non/existent/path.ar")
	require.Error(t, err)
	require.Nil(t, reader)
}

func TestArReader_InvalidZipFormat(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "ar_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	invalidPath := filepath.Join(tmpDir, "invalid.ar")
	err = os.WriteFile(invalidPath, []byte("not a zip file"), 0644)
	require.NoError(t, err)

	reader, err := NewArReader(invalidPath)
	require.Error(t, err)
	require.Nil(t, reader)
}

func TestArWriter_EmptyArchive(t *testing.T) {
	var buf bytes.Buffer
	writer := NewArWriter(&buf)
	err := writer.Close()
	require.NoError(t, err)

	tmpDir, err := os.MkdirTemp("", "ar_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	emptyArchivePath := filepath.Join(tmpDir, "empty.ar")
	err = os.WriteFile(emptyArchivePath, buf.Bytes(), 0644)
	require.NoError(t, err)

	reader, err := NewArReader(emptyArchivePath)
	require.NoError(t, err)
	defer reader.Close()

	names := reader.List()
	require.Empty(t, names)

	entry := reader.Get("any_file")
	require.Nil(t, entry)
}

package util

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLineChunkedReader_Empty0(t *testing.T) {
	reader := NewLineChunkedReader(strings.NewReader(""))
	_, _, err := reader.NextValidLine()
	require.Equal(t, io.EOF, err)
}

func TestLineChunkedReader_Empty1(t *testing.T) {
	reader := NewLineChunkedReader(strings.NewReader("\n"))
	_, _, err := reader.NextValidLine()
	require.Equal(t, io.EOF, err)
}

func TestLineChunkedReader_Empty2(t *testing.T) {
	reader := NewLineChunkedReader(strings.NewReader("\n\n"))
	_, _, err := reader.NextValidLine()
	require.Equal(t, io.EOF, err)
}

func TestLineChunkedReader_BeginWithNewLine(t *testing.T) {
	reader := NewLineChunkedReader(strings.NewReader("\nline1\nline2\nline3"))

	line, isPrefix, err := reader.NextValidLine()
	require.NoError(t, err)
	require.Equal(t, "line1", string(line))
	require.False(t, isPrefix)

	line, isPrefix, err = reader.NextValidLine()
	require.NoError(t, err)
	require.Equal(t, "line2", string(line))
	require.False(t, isPrefix)

	line, isPrefix, err = reader.NextValidLine()
	require.NoError(t, err)
	require.Equal(t, "line3", string(line))
	require.False(t, isPrefix)

	_, _, err = reader.NextValidLine()
	require.Equal(t, io.EOF, err)
}

func TestLineChunkedReader_BeginWithNewLine2(t *testing.T) {
	reader := NewLineChunkedReader(strings.NewReader("\n\nline1\nline2"))

	line, isPrefix, err := reader.NextValidLine()
	require.NoError(t, err)
	require.Equal(t, "line1", string(line))
	require.False(t, isPrefix)

	line, isPrefix, err = reader.NextValidLine()
	require.NoError(t, err)
	require.Equal(t, "line2", string(line))
	require.False(t, isPrefix)

	_, _, err = reader.NextValidLine()
	require.Equal(t, io.EOF, err)
}

func TestLineChunkedReader_EndWithNewLine(t *testing.T) {
	reader := NewLineChunkedReader(strings.NewReader("line1\nline2\n"))

	line, isPrefix, err := reader.NextValidLine()
	require.NoError(t, err)
	require.Equal(t, "line1", string(line))
	require.False(t, isPrefix)

	line, isPrefix, err = reader.NextValidLine()
	require.NoError(t, err)
	require.Equal(t, "line2", string(line))
	require.False(t, isPrefix)

	_, _, err = reader.NextValidLine()
	require.Equal(t, io.EOF, err)
}

func TestLineChunkedReader_EndWithNewLine2(t *testing.T) {
	reader := NewLineChunkedReader(strings.NewReader("line1\nline2\n\n"))

	line, isPrefix, err := reader.NextValidLine()
	require.NoError(t, err)
	require.Equal(t, "line1", string(line))
	require.False(t, isPrefix)

	line, isPrefix, err = reader.NextValidLine()
	require.NoError(t, err)
	require.Equal(t, "line2", string(line))
	require.False(t, isPrefix)

	_, _, err = reader.NextValidLine()
	require.Equal(t, io.EOF, err)
}

func TestLineChunkedReader_MultiLineInMiddle(t *testing.T) {
	reader := NewLineChunkedReader(strings.NewReader("\n\nline1\n\n\nline2\nline3\n\n"))

	line, isPrefix, err := reader.NextValidLine()
	require.NoError(t, err)
	require.Equal(t, "line1", string(line))
	require.False(t, isPrefix)

	line, isPrefix, err = reader.NextValidLine()
	require.NoError(t, err)
	require.Equal(t, "line2", string(line))
	require.False(t, isPrefix)

	line, isPrefix, err = reader.NextValidLine()
	require.NoError(t, err)
	require.Equal(t, "line3", string(line))
	require.False(t, isPrefix)

	_, _, err = reader.NextValidLine()
	require.Equal(t, io.EOF, err)
}

func TestLineChunkedReader_ManyEmpty0(t *testing.T) {
	reader := NewLineChunkedReader(strings.NewReader(strings.Repeat("\n", 9)))
	_, _, err := reader.NextValidLine()
	require.Equal(t, io.EOF, err)
}

func TestLineChunkedReader_ManyEmpty1(t *testing.T) {
	reader := NewLineChunkedReader(strings.NewReader(strings.Repeat("\n", 12)))
	_, _, err := reader.NextValidLine()
	require.Equal(t, io.ErrNoProgress, err)
}

func TestLineChunkedReader_Small(t *testing.T) {
	// 16 is the minimal buffer size
	reader := NewLineChunkedReaderSize(strings.NewReader("\n\nlonglineexceeding16bytes1\n\n\nexactly 16 bytes\n\n"), 16)

	line, isPrefix, err := reader.NextValidLine()
	require.NoError(t, err)
	require.Equal(t, "longlineexceedin", string(line))
	require.True(t, isPrefix)

	line, isPrefix, err = reader.NextValidLine()
	require.NoError(t, err)
	require.Equal(t, "g16bytes1", string(line))
	require.False(t, isPrefix)

	line, isPrefix, err = reader.NextValidLine()
	require.NoError(t, err)
	require.Equal(t, "exactly 16 bytes", string(line))
	require.True(t, isPrefix)

	_, _, err = reader.NextValidLine()
	require.Equal(t, io.EOF, err)
}

package util

import (
	"bufio"
	"io"
)

// LineChunkedReader supports reading lines in a streaming way while skipping empty lines.
type LineChunkedReader struct {
	r *bufio.Reader
}

func NewLineChunkedReaderSize(r io.Reader, size int) *LineChunkedReader {
	return &LineChunkedReader{
		r: bufio.NewReaderSize(r, size),
	}
}

func NewLineChunkedReader(r io.Reader) *LineChunkedReader {
	return NewLineChunkedReaderSize(r, 4096)
}

func (r *LineChunkedReader) NextValidLine() (line []byte, isPrefix bool, err error) {
	attempts := 0
	for {
		attempts++
		if attempts > 10 {
			return nil, false, io.ErrNoProgress
		}
		line, isPrefix, err = r.r.ReadLine()
		if len(line) == 0 && err == nil {
			continue
		}
		return line, isPrefix, err
	}
}

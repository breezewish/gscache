package cache

import (
	"encoding/binary"
	"io"
	"time"
)

type EntryMeta struct {
	ActionID []byte
	OutputID []byte
	Size     int64
	Time     time.Time
}

// WriteTo writes the EntryMeta to an io.Writer in binary format
// Format: [ActionID length][OutputID length][ActionID][OutputID][Size][Time unix nano]
func (em EntryMeta) WriteTo(w io.Writer) (int64, error) {
	bufSize := em.SerializedSize()
	buf := make([]byte, bufSize)

	offset := 0

	// ActionID Length
	actionIDLen := uint32(len(em.ActionID))
	binary.LittleEndian.PutUint32(buf[offset:], actionIDLen)
	offset += 4
	// OutputID Length
	outputIDLen := uint32(len(em.OutputID))
	binary.LittleEndian.PutUint32(buf[offset:], outputIDLen)
	offset += 4
	// ActionID
	copy(buf[offset:], em.ActionID)
	offset += len(em.ActionID)
	// OutputID
	copy(buf[offset:], em.OutputID)
	offset += len(em.OutputID)
	// Size
	binary.LittleEndian.PutUint64(buf[offset:], uint64(em.Size))
	offset += 8
	// Time
	binary.LittleEndian.PutUint64(buf[offset:], uint64(em.Time.UnixNano()))
	offset += 8

	n, err := w.Write(buf)
	return int64(n), err
}

func ReadEntryMeta(r io.Reader) (EntryMeta, error) {
	var em EntryMeta

	var lengthHeader [8]byte
	if _, err := io.ReadFull(r, lengthHeader[:]); err != nil {
		return EntryMeta{}, err
	}

	actionIDLen := int(binary.LittleEndian.Uint32(lengthHeader[0:4]))
	outputIDLen := int(binary.LittleEndian.Uint32(lengthHeader[4:8]))

	remainingBuf := make([]byte, actionIDLen+outputIDLen+16)
	if _, err := io.ReadFull(r, remainingBuf); err != nil {
		return em, err
	}

	offset := 0
	em.ActionID = remainingBuf[offset:actionIDLen]
	offset += actionIDLen
	em.OutputID = remainingBuf[offset : offset+outputIDLen]
	offset += outputIDLen
	em.Size = int64(binary.LittleEndian.Uint64(remainingBuf[offset : offset+8]))
	offset += 8
	timeNano := int64(binary.LittleEndian.Uint64(remainingBuf[offset : offset+8]))
	if timeNano == (time.Time{}).UnixNano() {
		em.Time = time.Time{}
	} else {
		em.Time = time.Unix(0, timeNano)
	}

	return em, nil
}

func (em *EntryMeta) SerializedSize() int {
	size := 4 + 4 // length
	size += len(em.ActionID)
	size += len(em.OutputID)
	size += 8 // size
	size += 8 // time
	return size
}

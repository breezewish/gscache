package cache

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEntryMeta_WriteTo_ReadEntryMeta_RoundTrip(t *testing.T) {
	tests := []struct {
		name string
		meta EntryMeta
	}{
		{
			name: "Basic entry",
			meta: EntryMeta{
				ActionID: []byte("action123"),
				OutputID: []byte("output456"),
				Size:     1024,
				Time:     time.Unix(1640995200, 123456789), // 2022-01-01 00:00:00.123456789 UTC
			},
		},
		{
			name: "Empty IDs",
			meta: EntryMeta{
				ActionID: []byte{},
				OutputID: []byte{},
				Size:     0,
				Time:     time.Unix(0, 0),
			},
		},
		{
			name: "Large size",
			meta: EntryMeta{
				ActionID: []byte("large_action_id_with_many_characters"),
				OutputID: []byte("large_output_id_with_many_characters"),
				Size:     9223372036854775807,              // max int64
				Time:     time.Unix(2147483647, 999999999), // max time that fits in 32-bit unix timestamp
			},
		},
		{
			name: "Binary data in IDs",
			meta: EntryMeta{
				ActionID: []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD},
				OutputID: []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF},
				Size:     512,
				Time:     time.Now(),
			},
		},
		{
			name: "Very long IDs",
			meta: EntryMeta{
				ActionID: bytes.Repeat([]byte("A"), 1000),
				OutputID: bytes.Repeat([]byte("B"), 1000),
				Size:     2048,
				Time:     time.Unix(1234567890, 987654321),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			n, err := tt.meta.WriteTo(&buf)
			require.NoError(t, err)
			require.Equal(t, int64(tt.meta.SerializedSize()), n)
			readMeta, err := ReadEntryMeta(&buf)
			require.NoError(t, err)
			require.Equal(t, tt.meta.ActionID, readMeta.ActionID)
			require.Equal(t, tt.meta.OutputID, readMeta.OutputID)
			require.Equal(t, tt.meta.Size, readMeta.Size)
			require.True(t, tt.meta.Time.Equal(readMeta.Time))
			require.Equal(t, 0, buf.Len())
		})
	}
}

func TestReadEntryMeta_IncompleteData(t *testing.T) {
	tests := []struct {
		name        string
		data        []byte
		expectError bool
	}{
		{
			name:        "Empty buffer",
			data:        []byte{},
			expectError: true,
		},
		{
			name:        "Only partial header",
			data:        []byte{0x05, 0x00, 0x00, 0x00}, // Only ActionID length
			expectError: true,
		},
		{
			name:        "Header only",
			data:        []byte{0x05, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00}, // Both lengths
			expectError: true,
		},
		{
			name: "Partial ActionID",
			data: []byte{
				0x05, 0x00, 0x00, 0x00, // ActionID length = 5
				0x06, 0x00, 0x00, 0x00, // OutputID length = 6
				0x61, 0x62, 0x63, // Only 3 bytes of ActionID (need 5)
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewReader(tt.data)
			_, err := ReadEntryMeta(buf)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestEntryMeta_ZeroValues(t *testing.T) {
	// Test with zero/nil values
	meta := EntryMeta{
		ActionID: nil,
		OutputID: nil,
		Size:     0,
		Time:     time.Time{}, // zero time
	}

	var buf bytes.Buffer
	_, err := meta.WriteTo(&buf)
	require.NoError(t, err)

	readMeta, err := ReadEntryMeta(&buf)
	require.NoError(t, err)

	require.Equal(t, []byte{}, readMeta.ActionID)
	require.Equal(t, []byte{}, readMeta.OutputID)
	require.Equal(t, int64(0), readMeta.Size)
	require.True(t, meta.Time.Equal(readMeta.Time))
}

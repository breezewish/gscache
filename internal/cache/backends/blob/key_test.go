package blob

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeCacheEntityKey_RoundTrip(t *testing.T) {
	testActionIDs := [][]byte{
		{0x00},
		{0xff},
		{0xab, 0xcd},
		{0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef},
		{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff},
	}
	for _, originalActionID := range testActionIDs {
		t.Run("", func(t *testing.T) {
			key := CacheEntityKey(originalActionID)
			decodedActionID, err := DecodeCacheEntityKey(key)
			require.NoError(t, err)
			require.Equal(t, originalActionID, decodedActionID)
		})
	}
}

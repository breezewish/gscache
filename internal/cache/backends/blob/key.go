package blob

import (
	"encoding/hex"
	"fmt"
)

// Key is for Object Store
// Path is for Local File System

func CacheEntityKey(actionID []byte) string {
	return fmt.Sprintf("b/%02x/%x", actionID[0], actionID)
}

func DecodeCacheEntityKey(key string) (actionID []byte, err error) {
	if len(key) < 5 || key[0] != 'b' || key[1] != '/' || key[4] != '/' {
		return nil, fmt.Errorf("invalid cache entity key %s", key)
	}
	actionIDInHex := key[5:]
	actionIdInBytes, err := hex.DecodeString(actionIDInHex)
	if err != nil {
		return nil, fmt.Errorf("invalid cache entity key %s", key)
	}
	if CacheEntityKey(actionIdInBytes) != key {
		// This also compares the b/%02x part
		return nil, fmt.Errorf("invalid cache entity key %s", key)
	}
	return actionIdInBytes, nil
}

func CacheEntityNameInArchive(actionID []byte) string {
	return fmt.Sprintf("%x", actionID)
}

func ArchiveListPrefixKey(keyspace string) string {
	return fmt.Sprintf("b/%s", keyspace)
}

func ArchiveKey(keyspace string) string {
	return fmt.Sprintf("blobar/%s.zip", keyspace)
}

func ArchiveFilePath(workDir, keyspace string) string {
	return fmt.Sprintf("%s/blobar/%s.zip", workDir, keyspace)
}

var ArchiveKeyspaces = []string{
	"0", "1", "2", "3", "4", "5", "6", "7",
	"8", "9", "a", "b", "c", "d", "e", "f",
}

func CacheEntityKeyspace(actionID []byte) string {
	return fmt.Sprintf("%02x", actionID[0])[0:1]
}

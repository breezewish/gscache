package stats

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/breezewish/gscache/internal/log"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"go.uber.org/zap"
)

func FileName(workDir string) string {
	return filepath.Join(workDir, "stats.json")
}

// LoadFromFile loads stats data from a JSON file.
// If file does not exist, it clears the current stats.
// It does not switch stats from inmemory to ondisk.
func (m *Metrics) LoadFromFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			m.Clear()
			return nil
		}
		return err
	}
	if err := json.Unmarshal(data, m); err != nil {
		return err
	}
	return nil
}

// LoadFromFileAndAttach makes the stats ondisk,
// attached to the given file path.
// It also loads existing stats from the file.
// Existing stats in memory will be overwritten.
func (m *Metrics) LoadFromFileAndAttach(path string) {
	if path == "" {
		return
	}
	m.diskPath = path
	if err := m.LoadFromFile(path); err != nil {
		log.Warn("Failed to load stats from file",
			zap.String("path", path),
			zap.Error(err))
	}
}

const MinPersistInterval time.Duration = 1 * time.Second

// saveToFile saves the current stats data to a JSON file.
func (m *Metrics) saveToFile(path string) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	uniqueId := gonanoid.Must(8)
	tmpPath := path + ".tmp." + uniqueId
	err = os.WriteFile(tmpPath, data, 0644)
	if err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func (m *Metrics) ForcePersist() {
	if m.diskPath == "" {
		return
	}
	if err := m.saveToFile(m.diskPath); err != nil {
		log.Warn("Failed to persist stats",
			zap.Error(err))
	}
}

// Persist saves the current stats to the attached file path at a minimum rate limit.
// If current stats are inmemory, nothing happens.
func (m *Metrics) Persist() {
	if m.diskPath == "" {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	elapsed := time.Since(m.lastPersistAt)

	if elapsed < MinPersistInterval {
		if m.pendingPersist == nil {
			after := MinPersistInterval - elapsed + 50*time.Millisecond
			m.pendingPersist = time.AfterFunc(after, func() {
				m.Persist()
			})
		}
		return
	}

	// Here: Elapsed >= MinPersistInterval, so we can proceed with saving.

	if m.pendingPersist != nil {
		m.pendingPersist.Stop()
		m.pendingPersist = nil
	}
	m.lastPersistAt = time.Now()
	m.ForcePersist()
}

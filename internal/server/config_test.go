package server

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

func TestLoadEmptyConfigPathReturnsDefault(t *testing.T) {
	config, err := LoadConfig("", nil)
	require.NoError(t, err)

	expected := DefaultConfig()
	require.Equal(t, expected, config)
}

func TestLoadValidTomlConfigFile(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.toml")

	tomlContent := `
port = 9000
dir = "/custom/dir"
shutdown_after_inactivity = "5m"

[log]
level = "debug"
file = "test.log"
`

	err := os.WriteFile(configPath, []byte(tomlContent), 0644)
	require.NoError(t, err)

	config, err := LoadConfig(configPath, nil)
	require.NoError(t, err)

	require.Equal(t, 9000, config.Port)
	require.Equal(t, "/custom/dir", config.Dir)
	require.Equal(t, 5*time.Minute, config.ShutdownAfterInactivity)
	require.Equal(t, "debug", config.Log.Level)
	require.Equal(t, "test.log", config.Log.File)
	require.Equal(t, "", config.Blob.URL)
}

func TestLoadPartialConfigFileMergesWithDefaults(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.toml")

	tomlContent := `port = 7500`

	err := os.WriteFile(configPath, []byte(tomlContent), 0644)
	require.NoError(t, err)

	config, err := LoadConfig(configPath, nil)
	require.NoError(t, err)

	defaultConfig := DefaultConfig()
	require.Equal(t, 7500, config.Port)
	require.Equal(t, defaultConfig.Dir, config.Dir)
	require.Equal(t, defaultConfig.ShutdownAfterInactivity, config.ShutdownAfterInactivity)
}

func TestLoadInvalidTomlSyntaxReturnsError(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.toml")

	invalidToml := `port = [invalid toml`

	err := os.WriteFile(configPath, []byte(invalidToml), 0644)
	require.NoError(t, err)

	_, err = LoadConfig(configPath, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to load config file")
}

func TestLoadInvalidFieldTypeReturnsError(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.toml")

	invalidContent := `shutdown_after_inactivity = "invalid duration"`

	err := os.WriteFile(configPath, []byte(invalidContent), 0644)
	require.NoError(t, err)

	_, err = LoadConfig(configPath, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "error decoding 'shutdown_after_inactivity'")
}

func TestLoadConfigOverride(t *testing.T) {
	// priority order: defaults < config file < env vars < flags

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.toml")

	tomlContent := `
port = 8080
dir = "/config/work"

[log]
level = "info"
file = "config.log"

[blob]
url = "file:///config/storage"
`

	err := os.WriteFile(configPath, []byte(tomlContent), 0644)
	require.NoError(t, err)

	t.Setenv("GSCACHE_PORT", "9000")
	t.Setenv("GSCACHE_LOG_LEVEL", "error")

	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flags.Int("port", 0, "port")
	flags.String("log.file", "", "log file")

	err = flags.Parse([]string{
		"--port=7000",          // Should override env var (9000) and config (8080)
		"--log.file=/flag.log", // Should override config file value
	})
	require.NoError(t, err)

	config, err := LoadConfig(configPath, flags)
	require.NoError(t, err)

	require.Equal(t, 7000, config.Port)                         // Flag wins over env var and config
	require.Equal(t, "error", config.Log.Level)                 // Env var wins over config (no flag set)
	require.Equal(t, "/flag.log", config.Log.File)              // Flag wins over config
	require.Equal(t, "/config/work", config.Dir)                // Config file wins over default (no env or flag)
	require.Equal(t, "file:///config/storage", config.Blob.URL) // Config file wins over default
	require.Equal(t, DefaultConfig().Blob.UploadConcurrency, config.Blob.UploadConcurrency)
}

func TestEmptyEnvVarsUseDefault(t *testing.T) {
	// If env var is set to an empty string, it falls back to the default value instead of being empty.
	t.Setenv("GSCACHE_PORT", "")
	t.Setenv("GSCACHE_LOG_LEVEL", "")
	t.Setenv("GSCACHE_DIR", "")

	config, err := LoadConfig("", nil)
	require.NoError(t, err)

	defaultConfig := DefaultConfig()
	require.Equal(t, defaultConfig.Port, config.Port)
	require.Equal(t, defaultConfig.Log.Level, config.Log.Level)
	require.Equal(t, defaultConfig.Dir, config.Dir)
}

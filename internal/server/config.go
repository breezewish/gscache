package server

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/breezewish/gscache/internal/cache/backends/blob"
	"github.com/breezewish/gscache/internal/log"
	"github.com/knadh/koanf/parsers/toml/v2"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/posflag"
	"github.com/knadh/koanf/providers/structs"
	"github.com/knadh/koanf/v2"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

var (
	DefaultWorkDir    = defaultWorkDir()
	DefaultConfigPath = defaultConfigPath()
)

type Config struct {
	Port                    int           `json:"port"`
	Log                     log.Config    `json:"log"`
	Dir                     string        `json:"dir"`
	ShutdownAfterInactivity time.Duration `json:"shutdown_after_inactivity"` // Note: This cannot be overridden by env variable due to its name
	Blob                    blob.Config   `json:"blob"`
}

func defaultWorkDir() string {
	baseDir, err := os.UserHomeDir()
	if err == nil {
		return filepath.Join(baseDir, ".gscache")
	}
	wd, err := os.Getwd()
	if err == nil {
		return filepath.Join(wd, ".gscache")
	}
	return filepath.Join(os.TempDir(), ".gscache")
}

func defaultConfigPath() string {
	baseDir, err := os.UserHomeDir()
	if err == nil {
		return filepath.Join(baseDir, ".config", "gscache", "config.toml")
	}
	wd, err := os.Getwd()
	if err == nil {
		return filepath.Join(wd, "gscache_config.toml")
	}
	return "./gscache_config.toml"
}

func DefaultConfig() Config {
	return Config{
		Port:                    8511,
		Log:                     log.DefaultConfig(DefaultWorkDir),
		Dir:                     DefaultWorkDir,
		ShutdownAfterInactivity: 10 * time.Minute,
		Blob:                    blob.DefaultConfig(),
	}
}

func LoadConfig(configPath string, flags *pflag.FlagSet) (Config, error) {
	k := koanf.New(".")
	// 1. Load from default
	if err := k.Load(structs.Provider(DefaultConfig(), "json"), nil); err != nil {
		return Config{}, err
	}
	displayLoadFileFailure := true
	if configPath == "" {
		configPath = DefaultConfigPath
		displayLoadFileFailure = false
	}

	// 2. Load from config file
	if err := k.Load(file.Provider(configPath), toml.Parser()); err != nil {
		// If user has specified a config path that does not exist, we return an error
		if os.IsNotExist(err) {
			if displayLoadFileFailure {
				log.Warn("Config file does not exist, skip loading", zap.String("file", configPath))
			}
		} else {
			return Config{}, fmt.Errorf("failed to load config file %s: %w", configPath, err)
		}
	}

	// 3. Load from environment variables
	// Example: GSCACHE_LOG_LEVEL=debug -> log.level=debug
	if err := k.Load(env.ProviderWithValue("GSCACHE_", ".", func(key string, value string) (string, any) {
		if len(value) == 0 {
			return "", nil
		}
		key = strings.Replace(strings.ToLower(strings.TrimPrefix(key, "GSCACHE_")), "_", ".", -1)
		return key, value
	}), nil); err != nil {
		log.Warn("Failed to load environment variables", zap.Error(err))
	}

	// 4. Load from command-line flags
	// Example: --log.level=debug -> log.level=debug
	if flags != nil {
		if err := k.Load(posflag.Provider(flags, ".", k), nil); err != nil {
			log.Warn("Failed to load command-line flags", zap.Error(err))
		}
	}

	var instance Config
	if err := k.UnmarshalWithConf("", &instance, koanf.UnmarshalConf{Tag: "json"}); err != nil {
		return Config{}, err
	}
	return instance, nil
}

func AddFlags(f *pflag.FlagSet) {
	defServerCfg := DefaultConfig()
	f.IntP("port", "p", defServerCfg.Port,
		"(env: GSCACHE_PORT)  Listen port of gscache server (or the gscache server port to connect to if running as client)")
	f.String("log.file", defServerCfg.Log.File,
		"(env: GSCACHE_LOG_FILE)  Server only: Log file path")
	f.String("log.level", defServerCfg.Log.Level,
		"(env: GSCACHE_LOG_LEVEL)  Server only: Log level (info, debug, warn, error)")
	f.String("dir", defServerCfg.Dir,
		"(env: GSCACHE_DIR)  Server only: Working directory for the server, where local cache files will be stored")
	f.String("blob.url", defServerCfg.Blob.URL,
		"(env: GSCACHE_BLOB_URL)  Server only: If set, remote blob cache will be used. If not set, by default a local cache is used. Example: s3://my-bucket")
}

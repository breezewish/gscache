package log

import (
	"path/filepath"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Config struct {
	File  string `json:"file"`
	Level string `json:"level"`
}

func DefaultConfig(workDir string) Config {
	return Config{
		File:  filepath.Join(workDir, "gscache.log"),
		Level: "info",
	}
}

func SetupJSONLogging(cfg Config) error {
	zapConfig := zap.NewProductionConfig()
	parsedLevel, err := zapcore.ParseLevel(cfg.Level)
	if err != nil {
		return err
	}
	zapConfig.Level = zap.NewAtomicLevelAt(parsedLevel)
	zapConfig.Encoding = "json"
	l, err := zapConfig.Build()
	if err != nil {
		return err
	}
	logger = l
	return nil
}

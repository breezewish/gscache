package log

import (
	"os"
	"time"

	prettyconsole "github.com/thessem/zap-prettyconsole"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logger *zap.Logger

func SetupReadableLogging(level zapcore.Level) {
	ec := prettyconsole.NewEncoderConfig()
	ec.EncodeTime = prettyconsole.DefaultTimeEncoder(time.DateTime)
	enc := prettyconsole.NewEncoder(ec)
	logger = zap.New(zapcore.NewCore(enc, os.Stderr, level))
}

func init() {
	SetupReadableLogging(zap.InfoLevel)
}

func Named(name string) *zap.Logger {
	return logger.Named(name)
}

func Info(msg string, fields ...zap.Field) {
	logger.Info(msg, fields...)
}

func Error(msg string, fields ...zap.Field) {
	logger.Error(msg, fields...)
}

func Debug(msg string, fields ...zap.Field) {
	logger.Debug(msg, fields...)
}

func Warn(msg string, fields ...zap.Field) {
	logger.Warn(msg, fields...)
}

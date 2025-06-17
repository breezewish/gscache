package local

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/breezewish/gscache/internal/cache"
	"github.com/breezewish/gscache/internal/log"
	"github.com/breezewish/gscache/internal/protocol"
	"github.com/breezewish/gscache/internal/util"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"go.uber.org/zap"
)

type LocalBackend struct {
	dir    string
	log    *zap.Logger
	closed atomic.Bool // When true, new requests will be rejected.

	sfGet *util.SingleFlightGroup
	sfPut *util.SingleFlightGroup
}

var _ cache.Backend = (*LocalBackend)(nil)

func NewLocalBackend(workDir string) (*LocalBackend, error) {
	if workDir == "" {
		return nil, fmt.Errorf("workDir must be specified")
	}
	return &LocalBackend{
		dir:    filepath.Join(workDir, "data"),
		log:    log.Named("cache.local"),
		closed: atomic.Bool{},
		sfGet:  util.NewSingleFlightGroup(),
		sfPut:  util.NewSingleFlightGroup(),
	}, nil
}

func (store *LocalBackend) EnsureEmptyOutputFile() (string, error) {
	path := filepath.Join(store.dir, "_empty.output")
	info, err := os.Stat(path)
	if err == nil && !info.IsDir() && info.Size() == 0 {
		return path, nil
	}
	err = os.WriteFile(path, []byte{}, 0644)
	if err != nil {
		return "", fmt.Errorf("failed to prepare empty output file %s: %w", path, err)
	}
	return path, nil
}

func (store *LocalBackend) Open(_ context.Context) error {
	for i := 0; i < 256; i++ {
		subdir := filepath.Join(store.dir, fmt.Sprintf("%02x", i))
		if err := os.MkdirAll(subdir, 0755); err != nil {
			return fmt.Errorf("failed to create subdirectory %s: %w", subdir, err)
		}
	}
	if _, err := store.EnsureEmptyOutputFile(); err != nil {
		return fmt.Errorf("failed to prepare empty output file: %w", err)
	}

	store.log.Info("Local cache store opened", zap.Any("dir", store.dir))
	return nil
}

func (store *LocalBackend) Close() error {
	store.closed.Store(true)
	store.log.Info("Local cache store closed")
	return nil
}

func (store *LocalBackend) actionPath(actionID []byte) string {
	return filepath.Join(store.dir, fmt.Sprintf("%02x", actionID[0]), fmt.Sprintf("%x.action", actionID))
}

func (store *LocalBackend) outputPath(outputID []byte) string {
	return filepath.Join(store.dir, fmt.Sprintf("%02x", outputID[0]), fmt.Sprintf("%x.output", outputID))
}

func (store *LocalBackend) Get(opts cache.GetOpts) (*protocol.GetResponse, error) {
	if store.closed.Load() {
		return nil, fmt.Errorf("local cache store is closed")
	}
	resp, err, _ := store.sfGet.Do(string(opts.Req.ActionID), func() (any, error) {
		return store.get(opts)
	})
	if err != nil {
		store.log.Warn("Failed to get from local cache",
			zap.String("actionID", fmt.Sprintf("%x", opts.Req.ActionID)),
			zap.String("metaPath", store.actionPath(opts.Req.ActionID)),
			zap.Error(err))
		return &protocol.GetResponse{
			Miss: true,
		}, nil
	}
	return resp.(*protocol.GetResponse), nil
}

func (store *LocalBackend) Put(opts cache.PutOpts) (*protocol.PutResponse, error) {
	if store.closed.Load() {
		return nil, fmt.Errorf("local cache store is closed")
	}
	resp, err, _ := store.sfPut.Do(string(opts.Req.ActionID), func() (any, error) {
		return store.put(opts)
	})
	if err != nil {
		store.log.Warn("Failed to put in local cache",
			zap.String("actionID", fmt.Sprintf("%x", opts.Req.ActionID)),
			zap.String("metaPath", store.actionPath(opts.Req.ActionID)),
			zap.Error(err))
	}
	store.log.Debug("Put in local cache",
		zap.String("actionID", fmt.Sprintf("%x", opts.Req.ActionID)),
		zap.String("metaPath", store.actionPath(opts.Req.ActionID)),
		zap.String("dataPath", store.outputPath(opts.Req.OutputID)))

	return resp.(*protocol.PutResponse), err
}

func (store *LocalBackend) markRecentlyUsed(actionPath string) bool {
	// We follow a similar strategy as Golang:
	// https://github.com/golang/go/blob/go1.24.3/src/cmd/go/internal/cache/cache.go#L349
	info, err := os.Stat(actionPath)
	if err != nil {
		return false
	}
	if now := time.Now(); now.Sub(info.ModTime()) >= 1*time.Hour {
		os.Chtimes(actionPath, now, now)
	}
	return true
}

func (store *LocalBackend) get(opts cache.GetOpts) (*protocol.GetResponse, error) {
	actionPath := store.actionPath(opts.Req.ActionID)
	actionFile, err := os.Open(actionPath)
	if err != nil {
		if os.IsNotExist(err) {
			store.log.Debug("Miss in local cache",
				zap.String("actionID", fmt.Sprintf("%x", opts.Req.ActionID)))
			return &protocol.GetResponse{
				Miss: true,
			}, nil
		}
		return nil, err
	}
	meta, err := cache.ReadEntryMeta(actionFile)
	_ = actionFile.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to read entry metadata: %w", err)
	}
	if !bytes.Equal(meta.ActionID, opts.Req.ActionID) {
		return nil, fmt.Errorf("action ID mismatch: expected %x, got %x", opts.Req.ActionID, meta.ActionID)
	}

	outputPath := store.outputPath(meta.OutputID)
	if meta.Size == 0 {
		emptyPath, err := store.EnsureEmptyOutputFile()
		if err != nil {
			return nil, fmt.Errorf("failed to prepare empty output file: %w", err)
		}
		outputPath = emptyPath
	} else {
		info, err := os.Stat(outputPath)
		if err != nil {
			_ = os.Remove(actionPath)
			return nil, fmt.Errorf("failed to stat output file: %w", err)
		}
		if info.IsDir() {
			_ = os.Remove(actionPath)
			_ = os.Remove(outputPath)
			return nil, fmt.Errorf("output path is a directory, expected a file: %s", outputPath)
		}
		if info.Size() != meta.Size {
			_ = os.Remove(actionPath)
			_ = os.Remove(outputPath)
			return nil, fmt.Errorf("output file size mismatch: expected %d, got %d", meta.Size, info.Size())
		}
	}

	_ = store.markRecentlyUsed(actionPath)
	_ = store.markRecentlyUsed(outputPath)

	store.log.Debug("Hit in local cache",
		zap.String("actionID", fmt.Sprintf("%x", opts.Req.ActionID)))

	return &protocol.GetResponse{
		Miss:     false,
		OutputID: meta.OutputID,
		Size:     meta.Size,
		Time:     &meta.Time,
		DiskPath: outputPath,
	}, nil
}

func (store *LocalBackend) put(opts cache.PutOpts) (*protocol.PutResponse, error) {
	actionPath := store.actionPath(opts.Req.ActionID)
	outputPath := store.outputPath(opts.Req.OutputID)
	uniqueId := gonanoid.Must(8)

	// Write object first to ensure atomicity
	if opts.Req.BodySize > 0 {
		if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
			return nil, fmt.Errorf("failed to create output directory: %w", err)
		}
		outputPathTmp := outputPath + ".tmp." + uniqueId
		outputFile, err := os.Create(outputPathTmp)
		if err != nil {
			return nil, fmt.Errorf("failed to create output file: %w", err)
		}
		defer outputFile.Close()
		n, err := io.Copy(outputFile, opts.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to write output body: %w", err)
		}
		if n != opts.Req.BodySize {
			return nil, fmt.Errorf("body size mismatch: expected %d according to meta, got %d", opts.Req.BodySize, n)
		}
		_ = outputFile.Close()
		if err := os.Rename(outputPathTmp, outputPath); err != nil {
			return nil, fmt.Errorf("failed to rename output file: %w", err)
		}
	} else {
		emptyPath, err := store.EnsureEmptyOutputFile()
		if err != nil {
			return nil, fmt.Errorf("failed to prepare empty output file: %w", err)
		}
		outputPath = emptyPath
	}
	{
		if err := os.MkdirAll(filepath.Dir(actionPath), 0755); err != nil {
			return nil, fmt.Errorf("failed to create action directory: %w", err)
		}
		actionPathTmp := actionPath + ".tmp." + uniqueId
		actionFile, err := os.Create(actionPathTmp)
		if err != nil {
			return nil, fmt.Errorf("failed to create action file: %w", err)
		}
		defer actionFile.Close()
		meta := cache.EntryMeta{
			ActionID: opts.Req.ActionID,
			OutputID: opts.Req.OutputID,
			Size:     opts.Req.BodySize,
			Time:     time.Now(),
		}
		if opts.OverrideTime != nil {
			meta.Time = *opts.OverrideTime
		}
		if _, err := meta.WriteTo(actionFile); err != nil {
			return nil, fmt.Errorf("failed to write entry metadata: %w", err)
		}
		_ = actionFile.Close()
		if err := os.Rename(actionPathTmp, actionPath); err != nil {
			return nil, fmt.Errorf("failed to rename action file: %w", err)
		}
	}

	// Note: No sync() is called because we are a cache anyway.

	return &protocol.PutResponse{
		DiskPath: outputPath,
	}, nil
}

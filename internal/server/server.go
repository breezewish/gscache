package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/breezewish/gscache/internal/cache"
	"github.com/breezewish/gscache/internal/cache/backends/blob"
	"github.com/breezewish/gscache/internal/cache/backends/local"
	"github.com/breezewish/gscache/internal/log"
	"github.com/breezewish/gscache/internal/stats"
	"github.com/nightlyone/lockfile"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	shutdownTimeout = 5 * time.Second
)

// Server is the gscache daemon server. All cacheprog simply talks to this server via HTTP REST API.
type Server struct {
	config  Config
	backend cache.Backend

	activityCh chan struct{} // Channel to track server activity

	lifecycle      context.Context    // Can be used to track server's stop. Only available after Run is called
	lifecycleClose context.CancelFunc // Only available after Run is called
}

func NewServer(config Config) (*Server, error) {
	if err := os.MkdirAll(config.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %w", err)
	}
	var backend cache.Backend
	var err error
	if config.Blob.URL == "" {
		backend, err = local.NewLocalBackend(config.Dir)
	} else {
		config.Blob.WorkDir = config.Dir
		backend, err = blob.NewBlobBackend(config.Blob)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create backend: %w", err)
	}
	return &Server{
		config:     config,
		backend:    backend,
		activityCh: make(chan struct{}, 1),
	}, nil
}

// lockWorkDir ensures local cache dir is not reused by multiple daemons.
func (s *Server) lockWorkDir() (lockfile.Lockfile, error) {
	lockfilePath := filepath.Join(s.config.Dir, ".gscache_daemon.lock")
	log.Info("Acquiring lock for work dir",
		zap.String("lockfile", lockfilePath))

	absLockFilePath, err := filepath.Abs(lockfilePath)
	if err != nil {
		return lockfile.Lockfile(""), fmt.Errorf("failed to resolve lock file path: %w", err)
	}
	lock, err := lockfile.New(absLockFilePath)
	if err != nil {
		// Must not happen
		return lockfile.Lockfile(""), err
	}
	if err := lock.TryLock(); err != nil {
		return lockfile.Lockfile(""), fmt.Errorf("work dir '%s' is in use by another daemon: %w", s.config.Dir, err)
	}
	return lock, nil
}

func (s *Server) startInactivityMonitor() {
	if s.config.ShutdownAfterInactivity <= 0 {
		return
	}

	log.Info("Server is configured to shutdown after inactivity",
		zap.String("inactivityTimeout", s.config.ShutdownAfterInactivity.String()))

	lastActive := time.Now()
	shutdownTimer := time.NewTimer(s.config.ShutdownAfterInactivity)

	// Worker routine
	go func() {
		for {
			select {
			case <-s.activityCh:
				lastActive = time.Now()
				shutdownTimer.Reset(s.config.ShutdownAfterInactivity)
			case <-shutdownTimer.C:
				log.Warn("Server idle, shutting down", zap.Time("lastActive", lastActive))
				s.Shutdown()
			case <-s.lifecycle.Done():
				// Server is shutting down
				return
			}
		}
	}()
}

// Run starts the gscache server, returns error if start failed.
// Blocks until the server is stopped (by signal or as request).
func (s *Server) Run() error {
	dirLock, err := s.lockWorkDir()
	if err != nil {
		return err
	}
	defer dirLock.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = s.backend.Open(ctx)
	if err != nil {
		return err
	}

	// Start the listener
	listenAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	log.Info("Starting gscache server", zap.Any("config", s.config))

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}

	ctx, cancel = context.WithCancel(context.Background())
	s.lifecycle = ctx
	s.lifecycleClose = cancel
	defer cancel()

	router := s.newRouter()
	server := &http.Server{
		Addr:    listenAddr,
		Handler: router.Handler(),
	}

	sigtermCh := make(chan os.Signal, 1)
	signal.Notify(sigtermCh, syscall.SIGINT, syscall.SIGTERM)

	shutdownWg := errgroup.Group{}
	shutdownWg.Go(func() error {
		select {
		case <-s.lifecycle.Done():
		case sig := <-sigtermCh:
			log.Info("Received shutdown signal", zap.String("signal", sig.String()))
			s.lifecycleClose() // Some routines rely on lifecycle context, so we close the lifecycle context as well
		}

		log.Info("Gracefully stopping server")

		// Shutdown the server and close the cache store in parallel,
		// because server requests may be waiting for cache store, while
		// we also want to reject new requests.

		shutdownWg.Go(func() error {
			ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
			defer cancel()
			_ = server.Shutdown(ctx)
			_ = server.Close()
			return nil
		})
		s.backend.Close()

		return nil
	})

	s.startInactivityMonitor()

	log.Info("Server is started")

	var retErr error = nil
	if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
		s.lifecycleClose()
		retErr = err
	}

	_ = shutdownWg.Wait()

	// Stats persisting by default has a delay, so we force it to persist now
	// before we exit.
	stats.Default.ForcePersist()

	log.Info("Server stopped")

	return retErr
}

func (s *Server) Shutdown() {
	s.lifecycleClose()
}

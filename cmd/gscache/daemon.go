package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	zappretty "github.com/maoueh/zap-pretty"
	"github.com/sevlyar/go-daemon"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/zap"

	"github.com/breezewish/gscache/internal/log"
	"github.com/breezewish/gscache/internal/util"
)

func rebuildCliArgs() []string {
	flags := rootCmd.PersistentFlags()
	args := []string{}
	flags.VisitAll(func(f *pflag.Flag) {
		if f.Changed {
			// Only include flags that are set.
			args = append(args, "--"+f.Name, f.Value.String())
		}
	})
	return args
}

func tailPrintLogFile(logFile string, n int) error {
	f, err := os.Open(logFile)
	if err != nil {
		return err
	}
	defer f.Close()

	// Seek last 1MB of the file
	_, _ = f.Seek(-1024*1024, io.SeekEnd)
	lastBuf, err := io.ReadAll(f)
	if err != nil {
		return err
	}

	// Tail last N lines
	outputBuf := []string{}
	lines := bytes.Split(lastBuf, []byte{'\n'})
	for i := len(lines) - 1; i >= 0; i-- {
		line := lines[i]
		if len(line) == 0 {
			continue
		}
		// Try to parse the line as JSON, and stop if it is a valid log line with too old timestamp.
		var logEntry map[string]interface{}
		if err := json.Unmarshal([]byte(line), &logEntry); err == nil {
			if ts, ok := logEntry["ts"].(float64); ok {
				if time.Since(time.Unix(int64(ts), 0)) > 1*time.Minute {
					break
				}
			}
		}
		outputBuf = append(outputBuf, string(line))
		if len(outputBuf) >= n {
			break // Stop if we have enough lines
		}
	}

	// Pretty print
	printDone := make(chan struct{})
	pipeR, pipeW := io.Pipe()
	go func() {
		scanner := bufio.NewScanner(pipeR)
		processor := zappretty.NewProcessor(scanner, os.Stderr)
		processor.Process()
		close(printDone)
	}()
	for i := len(outputBuf) - 1; i >= 0; i-- {
		pipeW.Write([]byte(outputBuf[i] + "\n"))
	}
	pipeW.Close()
	<-printDone

	return nil
}

// ensureDaemonRunning starts a daemon process if it is not running.
// The daemon process will be started like `gscache server <args...>`.
func ensureDaemonRunning(isExplicitStart bool) error {
	client := newClient()
	ping, _ := client.CallPing()
	if ping != nil {
		if isExplicitStart {
			log.Info("Server daemon is already running", zap.Int("pid", ping.Pid))
		}
		return nil
	}

	args := []string{os.Args[0], "server"}
	args = append(args, rebuildCliArgs()...)

	_ = os.MkdirAll(filepath.Dir(getServerConfig().Log.File), 0755)
	cntxt := &daemon.Context{
		LogFileName: getServerConfig().Log.File,
		LogFilePerm: 0640,
		Args:        args,
	}
	d, err := cntxt.Reborn()
	if err != nil {
		return fmt.Errorf("failed to start server daemon: %w", err)
	}
	if d == nil {
		// Should not enter here.
		return fmt.Errorf("failed to reborn daemon process")
	}

	if isExplicitStart {
		log.Info("Starting server daemon", zap.Int("pid", d.Pid))
	}

	ctx, ctxCancel := context.WithCancelCause(context.Background())
	go func() {
		// In case of daemon process exits unexpectedly.
		_, _ = d.Wait()
		ctxCancel(fmt.Errorf("daemon process was exited unexpectedly"))
	}()

	ping, err = client.WaitServerAlive(ctx, 10*time.Second)
	if err != nil {
		log.Error("Start daemon failed, printing Last 10 lines of the server log")
		err2 := tailPrintLogFile(getServerConfig().Log.File, 10)
		if err2 != nil {
			log.Error("Failed to tail server log file", zap.Error(err2))
		}
		return fmt.Errorf("failed to wait for server ready: %w", err)
	}

	if isExplicitStart {
		log.Info("Server daemon is ready", zap.Int("pid", ping.Pid))
	}
	return nil
}

func init() {
	daemonCmd := &cobra.Command{
		Use:   "daemon",
		Short: "Manage the gscache daemon",
	}

	startCmd := &cobra.Command{
		Use:   "start",
		Short: "Start the gscache server daemon in the background using current environment variables, flags and configs",
		Run: func(cmd *cobra.Command, args []string) {
			if err := ensureDaemonRunning( /* isExplicitStart */ true); err != nil {
				log.Error("Failed to start gscache server daemon", zap.Error(err))
				os.Exit(1)
			}
		},
	}

	stopCmd := &cobra.Command{
		Use:   "stop",
		Short: "Stop the gscache server daemon if it is running",
		Run: func(cmd *cobra.Command, args []string) {
			wasRunning, err := newClient().ShutdownAndWait(30 * time.Second)
			if err != nil {
				log.Error("Failed to shutdown server", zap.Error(err))
				os.Exit(1)
			}
			if wasRunning {
				log.Info("Server daemon stopped")
			} else {
				log.Info("Server daemon is not running")
			}
		},
	}

	restartCmd := &cobra.Command{
		Use:   "restart",
		Short: "Restart the gscache server daemon in the background using current environment variables, flags and configs",
		Run: func(cmd *cobra.Command, args []string) {
			client := newClient()
			wasRunning, err := client.ShutdownAndWait(30 * time.Second)
			if err != nil {
				log.Error("Failed to shutdown server", zap.Error(err))
				os.Exit(1)
			}
			if wasRunning {
				log.Info("Server daemon stopped")
			}
			if err := ensureDaemonRunning( /* isExplicitStart */ true); err != nil {
				log.Error("Failed to start gscache server daemon", zap.Error(err))
				os.Exit(1)
			}
		},
	}

	statusCmd := &cobra.Command{
		Use:   "status",
		Short: "Check the status of the gscache server daemon",
		Run: func(cmd *cobra.Command, args []string) {
			ping, err := newClient().CallPing()
			if err != nil {
				if errors.Is(err, syscall.ECONNREFUSED) {
					log.Error("Server daemon is not running")
				} else {
					log.Error("Failed to ping server", zap.Error(err))
				}
				os.Exit(1)
			}
			util.PrettyPrintJSON(ping)
		},
	}

	rootCmd.AddCommand(daemonCmd)
	daemonCmd.AddCommand(startCmd)
	daemonCmd.AddCommand(stopCmd)
	daemonCmd.AddCommand(restartCmd)
	daemonCmd.AddCommand(statusCmd)
}

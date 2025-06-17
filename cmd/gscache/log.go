package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"syscall"

	"github.com/breezewish/gscache/internal/log"
	"github.com/breezewish/gscache/internal/server"
	zappretty "github.com/maoueh/zap-pretty"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var logCmd = &cobra.Command{
	Use:   "log",
	Short: "Tail the daemon log file",
	Run: func(cmd *cobra.Command, args []string) {
		logFile := getServerConfig().Log.File
		pid := -1

		ping, err := newClient().CallPing()
		if err != nil {
			if errors.Is(err, syscall.ECONNREFUSED) {
				log.Info("Server daemon is not running, tail default log file")
			} else {
				log.Warn("Failed to ping server, tail default log file", zap.Error(err))
			}
		} else {
			j, _ := json.Marshal(ping.Config)
			var serverCfg server.Config
			if err := json.Unmarshal(j, &serverCfg); err != nil {
				log.Error("Failed to read server config", zap.Error(err))
			}
			logFile = serverCfg.Log.File
			pid = ping.Pid
		}

		if logFile == "" {
			log.Error("Server did not provide log file path")
			os.Exit(1)
		}

		if _, err := os.Stat(logFile); os.IsNotExist(err) {
			log.Error("Log file does not exist", zap.String("logFile", logFile))
			os.Exit(1)
		}

		log.Info("Tailing log file", zap.String("logFile", logFile), zap.Int("pid", pid))
		log.Info("Press Ctrl+C to stop")

		if err := runTailCommand(logFile); err != nil {
			log.Error("Failed to tail log file", zap.Error(err))
			os.Exit(1)
		}
	},
}

func runTailCommand(logFile string) error {
	cmd := exec.Command("tail", "-f", logFile)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	cmd.Stderr = os.Stderr

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start tail command: %w", err)
	}

	done := make(chan error, 1)
	go func() {
		defer stdout.Close()

		scanner := bufio.NewScanner(stdout)
		processor := zappretty.NewProcessor(scanner, os.Stdout)
		processor.Process()

		done <- cmd.Wait()
	}()

	select {
	case <-c:
		if err := cmd.Process.Kill(); err != nil {
			return fmt.Errorf("failed to kill tail process: %w", err)
		}
		<-done
		return nil
	case err := <-done:
		return err
	}
}

func init() {
	rootCmd.AddCommand(logCmd)
}

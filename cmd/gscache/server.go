package main

import (
	"fmt"
	"os"

	"github.com/breezewish/gscache/internal/log"
	"github.com/breezewish/gscache/internal/server"
	"github.com/breezewish/gscache/internal/stats"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func runAsServer() error {
	cfg := getServerConfig()

	// Actually as a daemon we write to stdout / stderr. The stdout and stderr
	// are pointed to the log file specified in the config when bring up
	// the daemon.
	err := log.SetupJSONLogging(cfg.Log)
	if err != nil {
		return fmt.Errorf("failed to setup logging: %w", err)
	}

	stats.Default.LoadFromFileAndAttach(stats.FileName(cfg.Dir))

	s, err := server.NewServer(*cfg)
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}
	if err := s.Run(); err != nil {
		return fmt.Errorf("failed to run server: %w", err)
	}
	return nil
}

func init() {
	serverCmd := &cobra.Command{
		Hidden: true,
		Use:    "server",
		Short:  "Start the gscache server",
		Run: func(cmd *cobra.Command, args []string) {
			if err := runAsServer(); err != nil {
				log.Error("Failed to run as server", zap.Error(err))
				os.Exit(1)
			}
		},
	}
	rootCmd.AddCommand(serverCmd)
}

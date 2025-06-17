package main

import (
	"os"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/breezewish/gscache/internal/cacheprog"
	"github.com/breezewish/gscache/internal/client"
	"github.com/breezewish/gscache/internal/log"
)

func init() {
	progCmd := &cobra.Command{
		Use:   "prog",
		Short: "Run as a cacheprog for go/cmd",
		Run: func(cmd *cobra.Command, args []string) {
			// Only log errors when it is a cacheprog
			log.SetupReadableLogging(zap.ErrorLevel)

			ensureDaemonRunning( /* isExplicitStart */ false)
			if err := cacheprog.New(cacheprog.Opts{
				CacheHandler: cacheprog.NewHandlerViaServer(client.Config{
					DaemonPort: getServerConfig().Port,
				}),
				In:  os.Stdin,
				Out: os.Stdout,
			}).Run(); err != nil {
				log.Error("Failed to run cacheprog", zap.Error(err))
				os.Exit(1)
			}
		},
	}

	rootCmd.AddCommand(progCmd)
}

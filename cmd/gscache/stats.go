package main

import (
	"os"

	"github.com/breezewish/gscache/internal/log"
	"github.com/breezewish/gscache/internal/stats"
	"github.com/breezewish/gscache/internal/util"
	"github.com/knadh/koanf/maps"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func init() {
	statsCmd := &cobra.Command{
		Use:   "stats",
		Short: "Show statistics",
		Run: func(cmd *cobra.Command, args []string) {
			_ = stats.Default.LoadFromFile(stats.FileName(getServerConfig().Dir))
			jsonMap, _ := util.ObjectToMapViaJSONSerde(stats.Default)
			imapFlat, _ := maps.Flatten(jsonMap, nil, ".")
			util.PrettyPrintJSON(imapFlat)
		},
	}

	clearCmd := &cobra.Command{
		Use:   "clear",
		Short: "Clear statistics",
		Run: func(cmd *cobra.Command, args []string) {
			client := newClient()
			alive, err := client.IsDaemonAlive()
			if err != nil {
				log.Error("Failed to check if server is alive", zap.Error(err))
				os.Exit(1)
			}
			if alive {
				_, err = client.CallStatsClear()
				if err != nil {
					log.Error("Failed to clear statistics", zap.Error(err))
					os.Exit(1)
				}
			} else {
				// Server is not running, let's just reset the local stats file
				statsFileName := stats.FileName(getServerConfig().Dir)
				if _, err = os.Stat(statsFileName); !os.IsNotExist(err) {
					err = os.Remove(statsFileName)
					if err != nil {
						log.Error("Failed to clear statistics", zap.Error(err))
						os.Exit(1)
					}
				}
			}
			log.Info("Statistics cleared")
		},
	}

	rootCmd.AddCommand(statsCmd)
	statsCmd.AddCommand(clearCmd)
}

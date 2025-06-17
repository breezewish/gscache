package main

import (
	"os"

	"github.com/breezewish/gscache/internal/client"
	"github.com/breezewish/gscache/internal/log"
	"github.com/breezewish/gscache/internal/server"
	"go.uber.org/zap"
)

func init() {
	rootCmd.PersistentFlags().StringP("config", "c", "",
		"(env: GSCACHE_CONFIG)  Config file path, if not specified, will try to load from "+server.DefaultConfigPath)
	server.AddFlags(rootCmd.PersistentFlags())
}

// newClient must be called in a command execute. Otherwise flags are not initialized yet.
func newClient() *client.Client {
	return client.NewClient(client.Config{
		DaemonPort: getServerConfig().Port,
	})
}

var serverConfig *server.Config = nil

// getServerConfig must be called in a command execute. Otherwise flags are not initialized yet.
func getServerConfig() *server.Config {
	if serverConfig != nil {
		return serverConfig
	}
	configFile := os.Getenv("GSCACHE_CONFIG")
	if rootCmd.PersistentFlags().Lookup("config").Value.String() != "" {
		configFile = rootCmd.PersistentFlags().Lookup("config").Value.String()
	}
	cfg, err := server.LoadConfig(configFile, rootCmd.PersistentFlags())
	if err != nil {
		log.Error("Failed to load server config", zap.Error(err))
		os.Exit(1)
	}
	serverConfig = &cfg
	return serverConfig
}

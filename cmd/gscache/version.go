package main

import (
	"fmt"
	"runtime/debug"

	"github.com/spf13/cobra"
)

var (
	version = "nightly"
	commit  = "unknown"
	date    = "unknown"
)

func init() {
	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(`
                                                      _/
     _/_/_/    _/_/_/    _/_/_/    _/_/_/    _/_/_/  _/_/_/      _/_/
  _/    _/  _/_/      _/        _/    _/  _/        _/    _/  _/_/_/_/
 _/    _/      _/_/  _/        _/    _/  _/        _/    _/  _/
  _/_/_/  _/_/_/      _/_/_/    _/_/_/    _/_/_/  _/    _/    _/_/_/
     _/
_/_/
			`)
			fmt.Printf("Version:      %s\n", version)
			fmt.Printf("Commit:       %s\n", commit)
			fmt.Printf("Build at:     %s\n", date)

			info, ok := debug.ReadBuildInfo()
			if !ok {
				return
			}

			fmt.Printf("Go version:   %s\n", info.GoVersion)
		},
	}

	rootCmd.AddCommand(versionCmd)
}

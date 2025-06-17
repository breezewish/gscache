package main

import (
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "gscache",
	Short: "gscache is a shared cache for Go",
}

func main() {
	rootCmd.Execute()
}

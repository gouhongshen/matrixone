package main

import (
	"github.com/spf13/cobra"
	"os"
)

func main() {
	migrate := migrateArg{}
	cmd := migrate.PrepareCommand()

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}

type InspectCmd interface {
	FromCommand(cmd *cobra.Command) error
	String() string
	Run() error
}

func RunFactory[T InspectCmd](c T) func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		if err := c.FromCommand(cmd); err != nil {
			os.Exit(1)
		}
		if err := c.Run(); err != nil {
			os.Exit(1)
		}
	}
}

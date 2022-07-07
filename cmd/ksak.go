package main

import (
	"drio/ksak"
	"errors"
	"fmt"
	"os"
)

func help(msg string) {
	fmt.Printf("ksak: Kafka swiss army knife: %s\n", msg)
	os.Exit(0)
}

type Runner interface {
	Init([]string) error
	Run() error
	Name() string
}

func root(args []string) error {
	if len(args) < 1 {
		return errors.New("You must pass a sub-command.\nUse subcommand help for details.")
	}

	cmds := []Runner{
		ksak.NewProduceCommand(),
		ksak.NewConsumeCommand(),
		ksak.NewListGroupsCommand(),
		ksak.NewLagCommand(),
		ksak.NewHelpCommand(),
		ksak.NewExporterCommand(),
	}

	subcommand := os.Args[1]

	for _, cmd := range cmds {
		if cmd.Name() == subcommand {
			cmd.Init(os.Args[2:])
			return cmd.Run()
		}
	}

	return fmt.Errorf("Unknown subcommand: %s", subcommand)
}

func main() {
	if err := root(os.Args[1:]); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

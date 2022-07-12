package main

import (
	"drio/ksak"
	"errors"
	"fmt"
	"log"
	"os"
)

func help(msg string) {
	fmt.Printf("ksak: Kafka swiss army knife: %s\n", msg)
	os.Exit(0)
}

type Runner interface {
	Init([]string) error
	Run(*ksak.KafkaDetails) error
	Name() string
}

func root(args []string) error {
	if len(args) < 1 {
		return errors.New("You must pass a sub-command.\nUse subcommand help for details.")
	}

	cmds := []Runner{
		ksak.NewProduceCommand(),
		ksak.NewConsumeCommand(),
		ksak.NewPartitionsCommand(),
		ksak.NewLagCommand(),
		ksak.NewHelpCommand(),
		ksak.NewExporterCommand(),
	}

	ksak.SetupCloseHandler()
	kd := &ksak.KafkaDetails{
		Url:      os.Getenv("KASK_URL"),
		Username: os.Getenv("KASK_USERNAME"),
		Password: os.Getenv("KASK_PASSWORD"),
	}
	log.Printf("url: [%s] user: [%s] ", kd.Url, kd.Username)

	kd.Init()
	defer func() {
		log.Printf("Closing kafka connection\n")
		if kd.Conn != nil {
			kd.Conn.Close()
		}
		if kd.Reader != nil {
			kd.Reader.Close()
		}
		// TODO: shutdown Client
	}()

	subcommand := os.Args[1]

	for _, cmd := range cmds {
		if cmd.Name() == subcommand {
			err := cmd.Init(os.Args[2:])
			if err != nil {
				return fmt.Errorf("%s", err)
			}
			return cmd.Run(kd)
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

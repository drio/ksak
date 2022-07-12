package ksak

import (
	"flag"
	"fmt"
)

const version = "0.0.1"

type HelpCommand struct {
	fs *flag.FlagSet

	name string
}

func NewHelpCommand() *HelpCommand {
	gc := &HelpCommand{
		fs: flag.NewFlagSet("help", flag.ContinueOnError),
	}

	return gc
}

func (l *HelpCommand) Name() string {
	return l.fs.Name()
}

func (l *HelpCommand) Init(args []string) error {
	return l.fs.Parse(args)
}

func (l *HelpCommand) Run(kd *KafkaDetails) error {
	PrintHelp()
	return nil
}

func PrintHelp() {
	fmt.Printf(`🔪 ksak (Kafka Swiss Army Knife) v%s
  Usage:
    $ ksak <command> [flags]

  [help]: show help.

  [produce]: produce random integers to topic.
    $ ksak produce --topic=test-topic

  [consume]: consume from topic and group-id.
    $ ksak consume --topic=test-topic -group-id=a-group-id

  [lag]: show lag for topic and group-id.
    $ ksak lag --topic=foo-bar --group-id=drio1

  [partitions]: show partitions with where replication != ISR
    $ ksak partitions
    $ ksak partitions --verbose

  [exporter]: starts a prometheus exporter that exposes lag related metrics.
              csv input format: kafka topic, kafka group id
    $ echo "foo-bar-topic, group-id1" | ksak exporter

  Environment variables: (kafka server credentials)
    - KASK_URL
    - KASK_USERNAME
    - KASK_PASSWORD

`, version)
}

package ksak

import (
	"flag"
	"fmt"
)

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

func (l *HelpCommand) Run() error {
	PrintHelp()
	return nil
}

func PrintHelp() {
	fmt.Printf(`🔪 ksak (Kafka Swiss Army Knife)

  Usage:
    ksak <command> [flags]

  [help]: show help.

  [produce]: produce random integers to topic.
    $ ksak produce --topic=test-topic

  [consume]: consume from topic and group-id.
    $ ksak consume --topic=test-topic -group-id=a-group-id

  [lag]: show lag for topic and group-id.
    $ ksak lag --topic=foo-bar --url=localhost:9092 --group-id=drio1

  [list-groups]: list groups in kafka server.
    $ ksak list-groups

  [exporter]: start a prometheus exporter that exposes lag related metrics.
    $ ksak exporter --input=%s --port=%d --sleep=%d
    csv input format:
    kafka broker url, kafka topic,  kafka group id
    example:
    localhost:9092, foo-bar-topic, group-id1

`, exporterDefaultInput, exporterDefaultPort, exporterDefaultSleep)
}

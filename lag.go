package ksak

import (
	"flag"
	"fmt"
	//"github.com/segmentio/kafka-go"
)

type LagCommand struct {
	fs *flag.FlagSet

	name    string
	url     string
	topic   string
	groupId string
}

func NewLagCommand() *LagCommand {
	c := &LagCommand{
		fs: flag.NewFlagSet("lag", flag.ContinueOnError),
	}

	c.fs.StringVar(&c.url, "url", "", "broker url")
	c.fs.StringVar(&c.topic, "topic", "", "kafka topic")
	c.fs.StringVar(&c.groupId, "group-id", "", "group id")

	return c
}

func (l *LagCommand) Name() string {
	return l.fs.Name()
}

func (l *LagCommand) Init(args []string) error {
	return l.fs.Parse(args)
}

func (l *LagCommand) Run() error {
	fmt.Println("lag time!")
	listOffsets(l.url, l.topic, l.groupId)
	return nil
}

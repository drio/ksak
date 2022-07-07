package ksak

import (
	"flag"
	"fmt"
	"os"
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
	if l.topic == "" {
		fmt.Println("No kafka topic name provided.")
		os.Exit(2)
	}

	if l.url == "" {
		fmt.Println("No kafka broker url")
		os.Exit(2)
	}

	if l.groupId == "" {
		fmt.Println("No kafka topic group id provided.")
		os.Exit(2)
	}

	les, err := getLag(l.url, l.topic, l.groupId)
	if err != nil {
		return err
	}
	fmt.Printf("%+v", les[0])
	return nil
}

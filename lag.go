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
	topic   string
	groupId string
}

func NewLagCommand() *LagCommand {
	c := &LagCommand{
		fs: flag.NewFlagSet("lag", flag.ContinueOnError),
	}

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

func (l *LagCommand) Run(kd *KafkaDetails) error {
	if l.topic == "" {
		fmt.Println("No kafka topic name provided.")
		os.Exit(2)
	}

	if l.groupId == "" {
		fmt.Println("No kafka topic group id provided.")
		os.Exit(2)
	}

	// FIXME: param partition
	kd.SetConn(l.topic, 0).SetClient()
	les, err := getLag(l.topic, l.groupId, kd.Client, kd.Conn)
	if err != nil {
		return err
	}
	fmt.Printf("%+v\n", les[0])
	return nil
}

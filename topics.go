package ksak

import (
	"flag"
	"fmt"
)

type ListTopicsCommand struct {
	fs *flag.FlagSet

	name string
}

func NewListTopicsCommand() *ListTopicsCommand {
	gc := &ListTopicsCommand{
		fs: flag.NewFlagSet("list-topics", flag.ContinueOnError),
	}

	return gc
}

func (l *ListTopicsCommand) Name() string {
	return l.fs.Name()
}

func (l *ListTopicsCommand) Init(args []string) error {
	return l.fs.Parse(args)
}

func (l *ListTopicsCommand) Run(kd *KafkaDetails) error {
	kd.SetPlainConn()
	partitions, err := kd.Conn.ReadPartitions()
	if err != nil {
		panic(err.Error())
	}

	m := map[string]struct{}{}

	for _, p := range partitions {
		m[p.Topic] = struct{}{}
	}
	for k := range m {
		fmt.Println(k)
	}

	return nil
}

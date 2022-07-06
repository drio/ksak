package ksak

import (
	"flag"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type ListGroupsCommand struct {
	fs *flag.FlagSet

	name string
	url  string
}

func NewListGroupsCommand() *ListGroupsCommand {
	gc := &ListGroupsCommand{
		fs: flag.NewFlagSet("list-groups", flag.ContinueOnError),
	}

	gc.fs.StringVar(&gc.url, "url", "", "broker url")

	return gc
}

func (l *ListGroupsCommand) Name() string {
	return l.fs.Name()
}

func (l *ListGroupsCommand) Init(args []string) error {
	return l.fs.Parse(args)
}

func (l *ListGroupsCommand) Run() error {
	conn, err := kafka.Dial("tcp", l.url)
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
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

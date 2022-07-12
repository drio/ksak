package ksak

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/segmentio/kafka-go"
)

const consumeDefaultsPartition = 0

type ConsumeCommand struct {
	fs *flag.FlagSet

	name      string
	topic     string
	partition int
	groupId   string
}

func NewConsumeCommand() *ConsumeCommand {
	gc := &ConsumeCommand{
		fs: flag.NewFlagSet("consume", flag.ContinueOnError),
	}

	gc.fs.StringVar(&gc.topic, "topic", "", "kafka topic to consume from")
	gc.fs.StringVar(&gc.groupId, "group-id", "", "kafka group-id to use")
	gc.fs.IntVar(&gc.partition, "partition", consumeDefaultsPartition, "kafka partition")

	return gc
}

func (c *ConsumeCommand) Name() string {
	return c.fs.Name()
}

func (c *ConsumeCommand) Init(args []string) error {
	return c.fs.Parse(args)
}

func (c *ConsumeCommand) Run(kd *KafkaDetails) error {
	if c.topic == "" {
		fmt.Println("No kafka <topic> name provided.")
		os.Exit(2)
	}

	if c.groupId == "" {
		fmt.Println("No <group-id> provided.")
		os.Exit(2)
	}

	kd.SetReader(c.topic, c.groupId, c.partition)
	c.loop(kd.Reader)
	return nil
}

func (c *ConsumeCommand) loop(reader *kafka.Reader) {
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			break
		}
		log.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}
}

package ksak

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/segmentio/kafka-go"
)

type ConsumeCommand struct {
	fs *flag.FlagSet

	name      string
	topic     string
	url       string
	partition int
	groupId   string
	reader    *kafka.Reader
}

func NewConsumeCommand() *ConsumeCommand {
	gc := &ConsumeCommand{
		fs: flag.NewFlagSet("consume", flag.ContinueOnError),
	}

	gc.fs.StringVar(&gc.topic, "topic", "", "kafka topic to consume from")
	gc.fs.StringVar(&gc.url, "url", "", "kafka broker url")
	gc.fs.StringVar(&gc.groupId, "group-id", "", "kafka topic group-id to use")

	return gc
}

func (c *ConsumeCommand) Name() string {
	return c.fs.Name()
}

func (c *ConsumeCommand) Init(args []string) error {
	return c.fs.Parse(args)
}

func (c *ConsumeCommand) Run() error {
	if c.topic == "" {
		fmt.Println("No kafka topic name provided.")
		os.Exit(2)
	}

	if c.url == "" {
		fmt.Println("No broker url")
		os.Exit(2)
	}

	if c.groupId == "" {
		fmt.Println("No group id provided.")
		os.Exit(2)
	}

	c.startReader()

	defer func() {
		if err := c.reader.Close(); err != nil {
			log.Fatal("failed to close reader:", err)
		}
	}()

	c.loop()
	return nil
}

func (c *ConsumeCommand) startReader() {
	c.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{c.url},
		Topic:     c.topic,
		GroupID:   c.groupId,
		Partition: c.partition,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	//r.SetOffset(0)
	log.Println("Consumer started ...")
}

func (c *ConsumeCommand) loop() {
	for {
		m, err := c.reader.ReadMessage(context.Background())
		if err != nil {
			break
		}
		log.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}
}

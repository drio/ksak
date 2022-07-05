package main

import (
	"context"
	"flag"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	topic := flag.String("topic", "", "kafka topic")
	groupId := flag.String("group-id", "", "kafka group id")
	partition := flag.Int("partition", 0, "kafka partition")
	url := flag.String("url", "localhost:9092", "kafka broker url")

	flag.Parse()

	if *topic == "" {
		log.Fatal("please, provide a <topic>")
	}

	if *groupId == "" {
		log.Fatal("please, provide a <group-id>")
	}

	app := &App{
		*topic,
		*url,
		*partition,
		*groupId,
		nil,
	}
	app.StartReader()

	defer func() {
		if err := app.reader.Close(); err != nil {
			log.Fatal("failed to close reader:", err)
		}
	}()

	app.Loop()

}

type App struct {
	topic     string
	url       string
	partition int
	groupId   string
	reader    *kafka.Reader
}

func (a *App) StartReader() {
	a.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{a.url},
		Topic:     a.topic,
		GroupID:   a.groupId,
		Partition: a.partition,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	//r.SetOffset(0)
	log.Println("Consumer started ...")
}

func (a *App) Loop() {
	for {
		m, err := a.reader.ReadMessage(context.Background())
		if err != nil {
			break
		}
		log.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}
}

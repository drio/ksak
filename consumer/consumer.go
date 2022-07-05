package main

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	app := &App{
		topic:     "drio_test_go",
		url:       "localhost:9092",
		partition: 0,
		groupId:   "drio-group-1",
		reader:    nil,
	}
	app.StartReader()

	if err := app.reader.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
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

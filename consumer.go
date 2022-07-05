package ksak

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

func (a *App) Consume() {
	a.startReader()

	defer func() {
		if err := a.Reader.Close(); err != nil {
			log.Fatal("failed to close reader:", err)
		}
	}()

	a.loop()
}

func (a *App) startReader() {
	a.Reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{a.Url},
		Topic:     a.Topic,
		GroupID:   a.GroupId,
		Partition: a.Partition,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	//r.SetOffset(0)
	log.Println("Consumer started ...")
}

func (a *App) loop() {
	for {
		m, err := a.Reader.ReadMessage(context.Background())
		if err != nil {
			break
		}
		log.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}
}

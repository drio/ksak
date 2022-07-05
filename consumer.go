package ksak

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

type SubCmdConsume struct {
	Topic     string
	Url       string
	Partition int
	GroupId   string
	Reader    *kafka.Reader
}

func (s *SubCmdConsume) Consume() {
	s.startReader()

	defer func() {
		if err := s.Reader.Close(); err != nil {
			log.Fatal("failed to close reader:", err)
		}
	}()

	s.loop()
}

func (s *SubCmdConsume) startReader() {
	s.Reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{s.Url},
		Topic:     s.Topic,
		GroupID:   s.GroupId,
		Partition: s.Partition,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	//r.SetOffset(0)
	log.Println("Consumer started ...")
}

func (s *SubCmdConsume) loop() {
	for {
		m, err := s.Reader.ReadMessage(context.Background())
		if err != nil {
			break
		}
		log.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}
}

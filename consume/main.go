package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	// to consume messages
	topic := "drio_test_go"
	url := "localhost:9092"
	partition := 0

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{url},
		Topic:     topic,
		GroupID:   "drio-group-1",
		Partition: partition,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	//r.SetOffset(0)
	fmt.Println("Consumer started. Consuming ...")

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}

}

package ksak

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type SubCmdProduce struct {
	Topic     string
	Url       string
	Partition int
	Conn      *kafka.Conn
}

func (s *SubCmdProduce) Produce() {
	SetupCloseHandler()
	s.Connect()
	defer func() {
		if err := s.Conn.Close(); err != nil {
			log.Fatal("failed to close writer:", err)
		}
	}()

	var sleepBy time.Duration
	// FIXME
	sleepBy = 2
	s.SendLoop(sleepBy)
}

func (s *SubCmdProduce) Connect() {
	conn, err := kafka.DialLeader(context.Background(), "tcp", s.Url, s.Topic, s.Partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}
	//conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	s.Conn = conn
}

func (s *SubCmdProduce) SendLoop(sleepSecs time.Duration) {
	for {
		r := fmt.Sprintf("%d", GenRandomInt(1000))
		log.Printf("Sending %s", r)
		_, err := s.Conn.WriteMessages(
			kafka.Message{Value: []byte(r)},
		)
		if err != nil {
			log.Fatal("failed to write messages:", err)
		}
		time.Sleep(sleepSecs * time.Second)
	}
}

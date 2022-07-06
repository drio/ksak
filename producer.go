package ksak

import (
	"context"
	"flag"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

type ProduceCommand struct {
	fs *flag.FlagSet

	name      string
	topic     string
	url       string
	partition int
	conn      *kafka.Conn
}

func NewProduceCommand() *ProduceCommand {
	gc := &ProduceCommand{
		fs: flag.NewFlagSet("produce", flag.ContinueOnError),
	}

	gc.fs.StringVar(&gc.topic, "topic", "", "kafka topic")

	return gc
}

func (g *ProduceCommand) Name() string {
	return g.fs.Name()
}

func (p *ProduceCommand) Init(args []string) error {
	return p.fs.Parse(args)
}

func (p *ProduceCommand) Run() error {
	p.Produce()
	return nil
}

func (s *ProduceCommand) Produce() {
	SetupCloseHandler()
	s.Connect()
	defer func() {
		if err := s.conn.Close(); err != nil {
			log.Fatal("failed to close writer:", err)
		}
	}()

	var sleepBy time.Duration
	// FIXME: cmd option
	sleepBy = 2
	s.SendLoop(sleepBy)
}

func (s *ProduceCommand) Connect() {
	conn, err := kafka.DialLeader(context.Background(), "tcp", s.url, s.topic, s.partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}
	//conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	s.conn = conn
}

func (s *ProduceCommand) SendLoop(sleepSecs time.Duration) {
	for {
		// FIXME: command option
		r := fmt.Sprintf("%d", GenRandomInt(1000))
		log.Printf("Sending %s", r)
		_, err := s.conn.WriteMessages(
			kafka.Message{Value: []byte(r)},
		)
		if err != nil {
			log.Fatal("failed to write messages:", err)
		}
		time.Sleep(sleepSecs * time.Second)
	}
}

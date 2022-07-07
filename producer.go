package ksak

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

const produceDefaultsPartition = 0

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
	gc.fs.StringVar(&gc.url, "url", "", "kafka broker url")
	gc.fs.IntVar(&gc.partition, "partition", produceDefaultsPartition, "kafka broker url")

	return gc
}

func (g *ProduceCommand) Name() string {
	return g.fs.Name()
}

func (p *ProduceCommand) Init(args []string) error {
	return p.fs.Parse(args)
}

func (p *ProduceCommand) Run() error {
	if p.topic == "" {
		fmt.Println("No kafka topic name provided.")
		os.Exit(2)
	}

	if p.url == "" {
		fmt.Println("No kafka broker url provided")
		os.Exit(2)
	}

	SetupCloseHandler()
	p.Connect()
	defer func() {
		if err := p.conn.Close(); err != nil {
			log.Fatal("failed to close writer:", err)
		}
	}()

	var sleepBy time.Duration
	// FIXME: cmd option
	sleepBy = 2
	p.SendLoop(sleepBy)
	return nil
}

func (p *ProduceCommand) Connect() {
	conn, err := kafka.DialLeader(context.Background(), "tcp", p.url, p.topic, p.partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}
	//conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	p.conn = conn
}

func (p *ProduceCommand) SendLoop(sleepSecs time.Duration) {
	for {
		// FIXME: command option
		r := fmt.Sprintf("%d", GenRandomInt(1000))
		log.Printf("Sending %s", r)
		_, err := p.conn.WriteMessages(
			kafka.Message{Value: []byte(r)},
		)
		if err != nil {
			log.Fatal("failed to write messages:", err)
		}
		time.Sleep(sleepSecs * time.Second)
	}
}

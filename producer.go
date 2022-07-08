package ksak

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

const produceDefaultsPartition = 0
const produceDefaultsSleepBy = 5

type ProduceCommand struct {
	fs *flag.FlagSet

	name      string
	topic     string
	partition int
	sleepBy   int
}

func NewProduceCommand() *ProduceCommand {
	gc := &ProduceCommand{
		fs: flag.NewFlagSet("produce", flag.ContinueOnError),
	}

	gc.fs.StringVar(&gc.topic, "topic", "", "kafka topic")
	gc.fs.IntVar(&gc.partition, "partition", produceDefaultsPartition, "kafka broker url")
	gc.fs.IntVar(&gc.sleepBy, "sleep", produceDefaultsPartition, "Number of seconds to sleep between production")

	return gc
}

func (g *ProduceCommand) Name() string {
	return g.fs.Name()
}

func (p *ProduceCommand) Init(args []string) error {
	return p.fs.Parse(args)
}

func (p *ProduceCommand) Run(kd *KafkaDetails) error {
	if p.topic == "" {
		fmt.Println("No kafka topic name provided.")
		os.Exit(2)
	}

	if p.sleepBy == 0 {
		log.Printf("No sleep provided, defaulting to %d", produceDefaultsSleepBy)
		p.sleepBy = produceDefaultsSleepBy
	}

	kd.SetConn(p.topic, p.partition)

	p.SendLoop(kd.Conn, time.Duration(p.sleepBy))
	return nil
}

func (p *ProduceCommand) SendLoop(conn *kafka.Conn, sleepSecs time.Duration) {
	for {
		// FIXME: command option
		r := fmt.Sprintf("%d", GenRandomInt(1000))
		log.Printf("Sending %s", r)
		_, err := conn.WriteMessages(
			kafka.Message{Value: []byte(r)},
		)
		if err != nil {
			log.Fatal("failed to write messages:", err)
		}
		time.Sleep(sleepSecs * time.Second)
	}
}

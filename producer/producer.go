package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	topic := flag.String("topic", "", "kafka topic")
	partition := flag.Int("partition", 0, "kafka partition")
	url := flag.String("url", "localhost:9092", "kafka broker url")

	flag.Parse()

	if *topic == "" {
		log.Fatal("please, provide a <topic>")
	}

	app := &App{
		*topic,
		*url,
		*partition,
		nil,
	}
	SetupCloseHandler()
	app.Connect()
	defer func() {
		if err := app.conn.Close(); err != nil {
			log.Fatal("failed to close writer:", err)
		}
	}()

	var sleepBy time.Duration
	sleepBy = 2
	app.SendLoop(sleepBy)
}

type App struct {
	topic     string
	url       string
	partition int
	conn      *kafka.Conn
}

func (a *App) Connect() {
	conn, err := kafka.DialLeader(context.Background(), "tcp", a.url, a.topic, a.partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}
	//conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	a.conn = conn
}

func (a *App) SendLoop(sleepSecs time.Duration) {
	for {
		r := fmt.Sprintf("%d", GenRandomInt(1000))
		log.Printf("Sending %s", r)
		_, err := a.conn.WriteMessages(
			kafka.Message{Value: []byte(r)},
		)
		if err != nil {
			log.Fatal("failed to write messages:", err)
		}
		time.Sleep(sleepSecs * time.Second)
	}
}

func SetupCloseHandler() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("\r- Ctrl+C pressed. Bye")
		os.Exit(0)
	}()
}

func GenRandomInt(max int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(max)
}

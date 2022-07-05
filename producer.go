package ksak

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func (a *App) RunProduce() {
	SetupCloseHandler()
	a.Connect()
	defer func() {
		if err := a.Conn.Close(); err != nil {
			log.Fatal("failed to close writer:", err)
		}
	}()

	var sleepBy time.Duration
	// FIXME
	sleepBy = 2
	a.SendLoop(sleepBy)
}

func (a *App) Connect() {
	conn, err := kafka.DialLeader(context.Background(), "tcp", a.Url, a.Topic, a.Partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}
	//conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	a.Conn = conn
}

func (a *App) SendLoop(sleepSecs time.Duration) {
	for {
		r := fmt.Sprintf("%d", GenRandomInt(1000))
		log.Printf("Sending %s", r)
		_, err := a.Conn.WriteMessages(
			kafka.Message{Value: []byte(r)},
		)
		if err != nil {
			log.Fatal("failed to write messages:", err)
		}
		time.Sleep(sleepSecs * time.Second)
	}
}

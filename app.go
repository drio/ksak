package ksak

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

type App struct {
	Topic     string
	Url       string
	Partition int
	GroupId   string
	Conn      *kafka.Conn
	Reader    *kafka.Reader
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

package ksak

import (
	"context"
	"crypto/tls"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

type KafkaDetails struct {
	Url       string // flag
	Username  string // env
	Password  string // env
	Topic     string // flag
	GroupId   string // flag
	Partition int    // flag

	Dialer *kafka.Dialer
	Reader *kafka.Reader
	Conn   *kafka.Conn
	Client *kafka.Client
}

func (a *KafkaDetails) Init() *KafkaDetails {
	a.Dialer = &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	if a.Username != "" && a.Password != "" {
		a.Dialer.TLS = &tls.Config{
			InsecureSkipVerify: true,
		}

		a.Dialer.SASLMechanism = plain.Mechanism{
			Username: a.Username,
			Password: a.Password,
		}
	}

	return a
}

func (a *KafkaDetails) SetConn(topic string, partition int) *KafkaDetails {
	conn, err := a.Dialer.DialLeader(context.Background(), "tcp", a.Url, topic, partition)
	if err != nil {
		log.Fatal(err)
	}
	a.Conn = conn
	return a
}

func (a *KafkaDetails) SetPlainConn() *KafkaDetails {
	conn, err := a.Dialer.Dial("tcp", a.Url)
	if err != nil {
		log.Fatal(err)
	}
	a.Conn = conn
	return a
}

func (a *KafkaDetails) SetClient() *KafkaDetails {
	transport := &kafka.Transport{
		Dial:     a.Dialer.DialFunc,
		Resolver: kafka.NewBrokerResolver(nil),
	}
	a.Client = &kafka.Client{
		Addr:      kafka.TCP(a.Url),
		Timeout:   5 * time.Second,
		Transport: transport,
	}
	return a
}

func (a *KafkaDetails) SetReader(topic, groupId string, partition int) *KafkaDetails {
	a.Reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{a.Url},
		Topic:     topic,
		Partition: partition,
		GroupID:   groupId,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
		Dialer:    a.Dialer,
	})
	return a
}

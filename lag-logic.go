package ksak

import (
	"context"
	"log"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type lagEntry struct {
	partition int
	committed int
	last      int
	lag       int
	topic     string
	groupId   string
}

func getPartitionsForTopic(url string, topic string) ([]int, error) {
	conn, err := kafka.DialLeader(context.Background(), "tcp", url, topic, 0)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	if err != nil {
		log.Printf("error connecting to Kafka url: %v, error is: %v\n", url, err)
		return nil, err
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		log.Printf("error reading partitions. error is: %v\n", err)
		return nil, err
	}
	var topicPartitions []int
	for _, p := range partitions {
		if p.Topic == topic {
			topicPartitions = append(topicPartitions, p.ID)
		}
	}
	return topicPartitions, nil
}

func newClient(addr net.Addr) (*kafka.Client, func()) {
	transport := &kafka.Transport{
		Dial:     (&net.Dialer{}).DialContext,
		Resolver: kafka.NewBrokerResolver(nil),
	}
	client := &kafka.Client{
		Addr:      addr,
		Timeout:   5 * time.Second,
		Transport: transport,
	}
	type ConnWaitGroup struct {
		sync.WaitGroup
	}
	conns := &ConnWaitGroup{}
	return client, func() { transport.CloseIdleConnections(); conns.Wait() }
}

func getLag(url, topic, groupid string) []lagEntry {
	partitions, err := getPartitionsForTopic(url, topic)
	if err != nil {
		log.Printf("error getting partitions for topic: %v, error is: %v\n", topic, err)
		return nil
	}
	client, shutdown := newClient(kafka.TCP(url))
	defer shutdown()

	// first get "Committed"
	offsets, err := client.OffsetFetch(context.Background(), &kafka.OffsetFetchRequest{
		GroupID: groupid,
		Topics: map[string][]int{
			topic: partitions,
		},
	})
	if err != nil {
		log.Printf("error fetching offsets for topic: %v, error is: %v\n", topic, err)
		return nil
	}
	type offsetInfo = struct {
		committed int
		last      int
	}
	final := map[int]offsetInfo{}

	for _, offsetFetchPartition := range offsets.Topics[topic] {
		final[offsetFetchPartition.Partition] = offsetInfo{
			committed: int(offsetFetchPartition.CommittedOffset),
		}
	}

	// now get "Last"
	var offsetRequests []kafka.OffsetRequest
	for _, partition := range partitions {
		offsetRequests = append(offsetRequests, kafka.LastOffsetOf(partition))
	}
	res, err := client.ListOffsets(context.Background(), &kafka.ListOffsetsRequest{
		Topics: map[string][]kafka.OffsetRequest{
			topic: offsetRequests,
		},
	})
	if err != nil {
		log.Printf("error listing offsets for topic: %v, error is: %v\n", url, err)
		return nil
	}
	partitionOffsets, ok := res.Topics[topic]
	if !ok {
		log.Printf("error getting partition offsets for topic: %v, error is: %v\n", url, err)
		return nil
	}

	// combine committed and last into final
	for _, partitionOffset := range partitionOffsets {
		// assume we will find it
		f := final[partitionOffset.Partition]
		f.last = int(partitionOffset.LastOffset)
		final[partitionOffset.Partition] = f
	}

	// sort final on partition ID asc and display
	keys := make([]int, 0, len(final))
	for k := range final {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	les := []lagEntry{}
	for _, k := range keys {
		les = append(les, lagEntry{
			partition: k,
			committed: final[k].committed,
			last:      final[k].last,
			lag:       final[k].last - final[k].committed,
			topic:     topic,
			groupId:   groupid,
		})
	}
	return les
}

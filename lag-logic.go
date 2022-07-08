package ksak

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"

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

func getPartitionsForTopic(topic string, conn *kafka.Conn) ([]int, error) {
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

func getLag(topic, groupid string, client *kafka.Client, conn *kafka.Conn) ([]lagEntry, error) {
	partitions, err := getPartitionsForTopic(topic, conn)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("error getting partitions for topic: %v, error is: %v\n", topic, err))
	}

	// first get "Committed"
	offsets, err := client.OffsetFetch(context.Background(), &kafka.OffsetFetchRequest{
		GroupID: groupid,
		Topics: map[string][]int{
			topic: partitions,
		},
	})
	if err != nil {
		return nil, errors.New(fmt.Sprintf("error fetching offsets for topic: %v, error is: %v\n", topic, err))
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
		return nil, errors.New(fmt.Sprintf("error listing offsets for topic: %v, error is: %v\n", topic, err))
	}
	partitionOffsets, ok := res.Topics[topic]
	if !ok {
		return nil, errors.New(fmt.Sprintf("error getting partition offsets for topic: %v, error is: %v\n", topic, err))
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
	return les, nil
}

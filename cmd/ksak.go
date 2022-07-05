package main

import (
	//"drio/ksak"
	"drio/ksak"
	"flag"
	"fmt"
	"os"
)

func help(msg string) {
	fmt.Printf("Help here: %s", msg)
	os.Exit(0)
}

func main() {
	produceCmd := flag.NewFlagSet("produce", flag.ExitOnError)
	produceTopic := produceCmd.String("topic", "", "kafka topic")
	producePartition := produceCmd.Int("partition", 0, "kafka partition")
	produceUrl := produceCmd.String("url", "localhost:9092", "kafka broker url")

	consumeCmd := flag.NewFlagSet("consume", flag.ExitOnError)
	consumeTopic := consumeCmd.String("topic", "", "kafka topic")
	consumePartition := consumeCmd.Int("partition", 0, "kafka partition")
	consumeUrl := consumeCmd.String("url", "localhost:9092", "kafka broker url")
	consumeGroupId := consumeCmd.String("group-id", "", "kafka topic group id")

	if len(os.Args) == 1 {
		help("")
	}

	switch os.Args[1] {
	case "produce":
		produceCmd.Parse(os.Args[2:])
	case "consume":
		consumeCmd.Parse(os.Args[2:])
	default:
		fmt.Printf("%q is not valid command.\n", os.Args[1])
		os.Exit(2)
	}

	if produceCmd.Parsed() {
		if *produceTopic == "" {
			// TODO
			help("")
		}

		app := &ksak.SubCmdProduce{
			Topic:     *produceTopic,
			Url:       *produceUrl,
			Partition: *producePartition,
			Conn:      nil,
		}
		app.Produce()
	}

	if consumeCmd.Parsed() {
		if *consumeTopic == "" {
			// TODO
			help("Empty topic")
		}

		if *consumeGroupId == "" {
			// TODO
			help("Empty group-id")
		}

		app := &ksak.SubCmdConsume{
			Topic:     *consumeTopic,
			Url:       *consumeUrl,
			Partition: *consumePartition,
			GroupId:   *consumeGroupId,
			Reader:    nil,
		}
		app.Consume()

	}

	/*
		if *topic == "" {
			log.Fatal("please, provide a <topic>")
		}


	*/
}

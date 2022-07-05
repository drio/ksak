package main

import (
	//"drio/ksak"
	"drio/ksak"
	"flag"
	"fmt"
	"os"
)

/*
func produce(app *ksak.App) {
	topic := flag.String("topic", "", "kafka topic")
	partition := flag.Int("partition", 0, "kafka partition")
	url := flag.String("url", "localhost:9092", "kafka broker url")
	groupId := flag.String("group-id", "", "kafka topic group id")

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
*/

func help() {
	fmt.Println("Help here")
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
		help()
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
		// TODO: checks
		fmt.Printf("Produce!: %s %d %s \n", *produceTopic, *producePartition, *produceUrl)
		app := &ksak.App{
			Topic:     *produceTopic,
			Url:       *produceUrl,
			Partition: *producePartition,
			GroupId:   "",
			Conn:      nil,
			Reader:    nil,
		}
		app.RunProduce()
	}

	if consumeCmd.Parsed() {
		// TODO: checks
		fmt.Printf("consume!: %s %d %s %s\n", *consumeTopic, *consumePartition, *consumeUrl, *consumeGroupId)
	}

	/*
		if *topic == "" {
			log.Fatal("please, provide a <topic>")
		}


	*/
}

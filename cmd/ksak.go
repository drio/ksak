package main

import (
	"drio/ksak"
	//"flag"
	"errors"
	"fmt"
	"os"
)

func help(msg string) {
	fmt.Printf("ksak: Kafka swiss army knife: %s\n", msg)
	os.Exit(0)
}

type Runner interface {
	Init([]string) error
	Run() error
	Name() string
}

func root(args []string) error {
	if len(args) < 1 {
		return errors.New("You must pass a sub-command")
	}

	cmds := []Runner{
		ksak.NewProduceCommand(),
		ksak.NewConsumeCommand(),
		ksak.NewListGroupsCommand(),
		ksak.NewLagCommand(),
	}

	subcommand := os.Args[1]

	for _, cmd := range cmds {
		if cmd.Name() == subcommand {
			cmd.Init(os.Args[2:])
			return cmd.Run()
		}
	}

	return fmt.Errorf("Unknown subcommand: %s", subcommand)
}

func main() {
	if err := root(os.Args[1:]); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

/*
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

	if len(os.Args) < 2 {
		help("You did not provide a subcommand")
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
			help("Need topic to be able to produce")
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
			help("Need topic to consume")
		}

		if *consumeGroupId == "" {
			// TODO
			help("Need group-id to consume")
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

}
*/

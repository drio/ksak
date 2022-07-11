package ksak

import (
	"flag"
	"fmt"
)

type PartitionsCommand struct {
	fs *flag.FlagSet

	name    string
	verbose bool
}

func NewPartitionsCommand() *PartitionsCommand {
	gc := &PartitionsCommand{
		fs: flag.NewFlagSet("partitions", flag.ContinueOnError),
	}

	gc.fs.BoolVar(&gc.verbose, "verbose", false, "Show partition details")
	return gc
}

func (l *PartitionsCommand) Name() string {
	return l.fs.Name()
}

func (l *PartitionsCommand) Init(args []string) error {
	return l.fs.Parse(args)
}

func (l *PartitionsCommand) Run(kd *KafkaDetails) error {
	kd.SetPlainConn()
	partitions, err := kd.Conn.ReadPartitions()
	if err != nil {
		panic(err.Error())
	}

	if l.verbose {
		//m := map[string]kafka.Partition
		for _, p := range partitions {
			fmt.Printf("%d:%s %d replicas=[ ", p.ID, p.Topic, p.Leader.ID)
			for _, r := range p.Replicas {
				fmt.Printf("%d ", r.ID)
			}
			fmt.Printf("] ISR=[ ")
			for _, r := range p.Isr {
				fmt.Printf("%d ", r.ID)
			}
			fmt.Printf("]\n")

		}
	} else {
		for _, p := range partitions {
			if len(p.Replicas) != len(p.Isr) {
				fmt.Printf("%d:%s \n ", p.ID, p.Topic)
			}
		}
	}

	return nil
}

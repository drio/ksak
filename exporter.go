package ksak

import (
	"flag"
	"fmt"
)

type ExporterCommand struct {
	fs *flag.FlagSet

	name string
}

func NewExporterCommand() *ExporterCommand {
	gc := &ExporterCommand{
		fs: flag.NewFlagSet("exporter", flag.ContinueOnError),
	}

	return gc
}

func (l *ExporterCommand) Name() string {
	return l.fs.Name()
}

func (l *ExporterCommand) Init(args []string) error {
	return l.fs.Parse(args)
}

func (l *ExporterCommand) Run() error {
	fmt.Println("exporter")

	return nil
}

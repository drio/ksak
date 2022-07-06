package ksak

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
	/*
				  TODO:
		        - Read list of csv: (url, topic, groupid) from stdin
		        - Every x seconds:
		          per each entry:
		            - get the latest lag and update the gauge (use labels)
	*/

	gaugeLag := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "namesp_Foo",
			Subsystem: "sub_Bar",
			Name:      "ksak_kafka_alg",
			Help:      "lag metrics on kafka topics.",
		},
		[]string{
			"topic",
			"groupid",
			"host",
		},
	)
	prometheus.MustRegister(gaugeLag)

	gaugeLag.WithLabelValues("foo_bar", "gid_12", "localhost_9092").Add(123)
	//gaugeLag.With(prometheus.Labels{"type": "delete", "user": "alice"}).Inc()

	http.Handle("/metrics", promhttp.Handler())
	log.Println("Listening on port 8080")
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", 8080), nil))
	return nil
}

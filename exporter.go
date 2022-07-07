package ksak

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

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

	listEntries, err := readCsv()
	if err != nil {
		log.Fatalf("Could not load csv: %s", err)
	}
	fmt.Printf("%v ", listEntries)

	go func() {
		for {
			time.Sleep(2 * time.Second)
		}
	}()

	http.Handle("/metrics", promhttp.Handler())
	log.Println("Listening on port 8080")
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", 8080), nil))
	return nil
}

type csvEntry struct {
	url     string
	topic   string
	groupId string
}

func readCsv() ([]csvEntry, error) {
	file, err := os.Open("/dev/stdin")
	if err != nil {
		return nil, err
	}

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	listEntries := []csvEntry{}
	for _, entry := range records {
		listEntries = append(listEntries, csvEntry{
			url:     entry[0],
			topic:   entry[1],
			groupId: entry[2],
		})
	}

	return listEntries, nil
}

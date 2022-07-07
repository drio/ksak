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
	gaugeLag := registerGauge()

	listCsvEntries, err := readCsv()
	if err != nil {
		log.Fatalf("Could not load csv: %s", err)
	}

	go func() {
		for {
			log.Printf("Updating gauge")
			listLags, err := getAllLags(listCsvEntries)
			if err != nil {
				log.Printf("Error getting lags: %s", err)
			} else {
				updateGauge(gaugeLag, listCsvEntries, listLags)
			}
			// TODO: dynamic
			time.Sleep(10 * time.Second)
		}
	}()

	http.Handle("/metrics", promhttp.Handler())
	log.Println("Listening on port 8080")
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", 8080), nil))
	return nil
}

func registerGauge() *prometheus.GaugeVec {
	gaugeLag := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ksak_space",
			Subsystem: "ksa_sub",
			Name:      "ksak_kafka_lag",
			Help:      "lag metrics on kafka topics.",
		},
		[]string{
			"topic",
			"groupid",
			"host",
			"partition",
		},
	)
	prometheus.MustRegister(gaugeLag)
	return gaugeLag
}

type csvEntry struct {
	url     string
	topic   string
	groupId string
}

// Example on how to update gauge
// gaugeLag.WithLabelValues("foo_bar", "gid_12", "localhost_9092").Set(123)
// gaugeLag.With(prometheus.Labels{"type": "delete", "user": "alice"}).Set(344)
func updateGauge(gauge *prometheus.GaugeVec, csvEntries []csvEntry, lagEntries [][]lagEntry) error {
	for i, ce := range csvEntries {
		for _, le := range lagEntries[i] {
			gauge.With(prometheus.Labels{
				"topic":     ce.topic,
				"groupid":   ce.groupId,
				"host":      ce.url,
				"partition": fmt.Sprint(le.partition),
			}).Set(float64(le.lag))
		}
	}
	return nil
}

func getAllLags(listCsvEntries []csvEntry) ([][]lagEntry, error) {
	lagEntriesPerCsvRow := [][]lagEntry{}
	for _, ce := range listCsvEntries {
		listLags, err := getLag(ce.url, ce.topic, ce.groupId)
		if err != nil {
			log.Printf("Error processing csv entry %v. Continuing with next one", ce)
			return nil, err
		}
		lagEntriesPerCsvRow = append(lagEntriesPerCsvRow, listLags)
	}
	return lagEntriesPerCsvRow, nil
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

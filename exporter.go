package ksak

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const exporterDefaultInput = "/dev/stdin"
const exporterDefaultSleep = 30
const exporterDefaultPort = 8080

type exporterCmdDefaults struct {
	fName     string
	sleepTime int
	port      int
}

type ExporterCommand struct {
	fs *flag.FlagSet

	name      string
	fName     string
	sleepTime int
	port      int
}

func NewExporterCommand() *ExporterCommand {
	gc := &ExporterCommand{
		fs: flag.NewFlagSet("exporter", flag.ContinueOnError),
	}

	gc.fs.StringVar(&gc.fName, "input", exporterDefaultInput, "csv file name to proces")
	gc.fs.IntVar(&gc.sleepTime, "sleep", exporterDefaultSleep, "Sleep time (in seconds) between lag updates")
	gc.fs.IntVar(&gc.port, "port", exporterDefaultPort, "Server port number")
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

	if l.fName == "" {
		fmt.Println("No input file name provided.")
		os.Exit(2)
	}
	log.Printf("Reading from %s", l.fName)
	log.Printf("Sleeping for %d seconds between lag updates", l.sleepTime)

	listCsvEntries, err := readCsv(l.fName)
	if err != nil {
		log.Fatalf("Could not load csv: %s", err)
	}
	log.Printf("Loaded %d rows from csv", len(listCsvEntries))

	go func() {
		for {
			log.Printf("Updating gauge")
			listLags, err := getAllLags(listCsvEntries)
			if err != nil {
				log.Printf("Error getting lags: %s", err)
			} else {
				updateGauge(gaugeLag, listCsvEntries, listLags)
			}
			log.Printf("Sleeping update gauge goroutine for %d seconds", l.sleepTime)
			time.Sleep(time.Duration(l.sleepTime) * time.Second)
		}
	}()

	http.Handle("/metrics", promhttp.Handler())
	log.Printf("Listening on port %d", l.port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", l.port), nil))
	return nil
}

func registerGauge() *prometheus.GaugeVec {
	gaugeLag := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "ksak_kafka_lag",
			Help: "lag metrics on kafka topics.",
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

func readCsv(fName string) ([]csvEntry, error) {
	file, err := os.Open(fName)
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
			url:     strings.TrimSpace(entry[0]),
			topic:   strings.TrimSpace(entry[1]),
			groupId: strings.TrimSpace(entry[2]),
		})
	}

	return listEntries, nil
}

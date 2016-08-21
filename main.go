package main

import (
	"flag"
	"fmt"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/prometheus/client_golang/api/prometheus"
	"github.com/prometheus/common/model"
	"golang.org/x/net/context"
	"strings"
	"sync"
	"time"
)

type QueryType int

const (
	Gauge QueryType = iota
	Counter
	Histogram
	Set
	Milliseconds
)

type Query struct {
	Type  QueryType
	Query string
}

type Queries []Query

func (flags *Queries) String() string {
	return "Queries"
}

func (flags *Queries) Set(value string) error {
	parts := strings.SplitN(value, ":", 2)
	metric_type := parts[0]
	prometheus_query := parts[1]

	var query_type QueryType
	switch metric_type {
	case "gauge":
		query_type = Gauge
	case "counter":
		query_type = Counter
	case "histogram":
		query_type = Histogram
	case "set":
		return fmt.Errorf("Don't know how to handle sets (yet)")
	case "milliseconds":
		query_type = Milliseconds
	default:
		return fmt.Errorf("Can't handle query type %v (%v)", metric_type, value)
	}

	*flags = append(*flags, Query{Type: query_type, Query: prometheus_query})
	return nil
}

var (
	dogstatsd_addr  = flag.String("dogstatsd-address", "127.0.0.1:8125", "The address to send dogstatsd metrics to.")
	prometheus_addr = flag.String("prometheus-address", "127.0.0.1:9090", "The prometheus address")
	interval        = flag.Int("interval", 10, "Frequency to query Prometheus (in seconds)")
	queries         Queries
)

func run_query(query Query, query_api prometheus.QueryAPI, when time.Time, statsd_client *statsd.Client) {
	results, err := query_api.Query(context.Background(), query.Query, when)
	if err != nil {
		panic(err)
	}

	for _, sample := range results.(model.Vector) {
		var tags []string
		var name string
		for label, val := range sample.Metric {
			switch label {
			case "__name__":
				name = string(val)
			default:
				tags = append(tags, fmt.Sprintf("%s:%s", label, val))
			}
		}
		switch query.Type {
		case Gauge:
			statsd_client.Gauge(name, float64(sample.Value), tags, 1)
		case Counter:
			statsd_client.Count(name, int64(sample.Value), tags, 1)
		case Histogram:
			statsd_client.Histogram(name, float64(sample.Value), tags, 1)
		case Milliseconds:
			statsd_client.TimeInMilliseconds(name, float64(sample.Value), tags, 1)
		default:
			panic(fmt.Errorf("Can't handle %g", query.Type))
		}
	}
}

func start_querying(wg *sync.WaitGroup, interval time.Duration, queries Queries, query_api prometheus.QueryAPI, statsd_client *statsd.Client) {
	ticker := time.NewTicker(interval)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for now := range ticker.C {
			for _, query := range queries {
				run_query(query, query_api, now, statsd_client)
			}
		}
	}()
}

func main() {
	flag.Var(&queries, "query", "Prometheus query (in form gauge:myquery{}). Can be specified multiple times.")
	flag.Parse()

	statsd_client, err := statsd.New(*dogstatsd_addr)
	if err != nil {
		panic(err)
	}
	defer statsd_client.Close()

	statsd_client.Namespace = "prometheus."

	prometheus_config := prometheus.Config{Address: *prometheus_addr}
	prometheus_client, err := prometheus.New(prometheus_config)
	if err != nil {
		panic(err)
	}

	prometheus_query_api := prometheus.NewQueryAPI(prometheus_client)

	duration := time.Duration(*interval) * time.Second
	var wg sync.WaitGroup
	start_querying(&wg, duration, queries, prometheus_query_api, statsd_client)
	wg.Wait()
}

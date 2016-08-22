package main

import (
	"flag"
	"fmt"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/prometheus/client_golang/api/prometheus"
	prometheus_metrics "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"golang.org/x/net/context"
	"net/http"
	"strings"
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
	Name  string
	Query string
}

type Queries []Query

func (flags *Queries) String() string {
	return "Queries"
}

func (flags *Queries) Set(value string) error {
	parts := strings.SplitN(value, ":", 3)
	metric_type := parts[0]
	metric_name := parts[1]
	prometheus_query := parts[2]

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

	*flags = append(*flags, Query{Type: query_type, Name: metric_name, Query: prometheus_query})
	return nil
}

var (
	dogstatsd_addr  = flag.String("dogstatsd-address", "127.0.0.1:8125", "The address to send dogstatsd metrics to.")
	prometheus_addr = flag.String("prometheus-address", "127.0.0.1:9090", "The prometheus address")
	listen_addr     = flag.String("listen-address", ":9132", "HTTP address to listen on to publish internal metrics.")
	interval        = flag.Int("interval", 10, "Frequency to query Prometheus (in seconds)")
	queries         Queries
)

var (
	pushedMetrics = prometheus_metrics.NewCounterVec(
		prometheus_metrics.CounterOpts{
			Namespace: "prometheus_to_datadog",
			Name:      "pushed_metrics_total",
			Help:      "Number of metrics pushed",
		},
		[]string{"prometheus_name", "dogstatsd_name", "metric_type"},
	)
	failedQueries = prometheus_metrics.NewCounterVec(
		prometheus_metrics.CounterOpts{
			Namespace: "prometheus_to_datadog",
			Name:      "failed_queries_total",
			Help:      "Number of failed queries to Prometheus",
		},
		[]string{"query"},
	)
	failedPushedMetrics = prometheus_metrics.NewCounterVec(
		prometheus_metrics.CounterOpts{
			Namespace: "prometheus_to_datadog",
			Name:      "failed_pushed_metrics_total",
			Help:      "Number of failed failed metric pushes.",
		},
		[]string{"reason"},
	)
)

func run_query(query Query, query_api prometheus.QueryAPI, when time.Time, statsd_client *statsd.Client) error {
	var err error
	results, err := query_api.Query(context.Background(), query.Query, when)
	if err != nil {
		failedQueries.WithLabelValues(query.Query).Inc()
		return err
	}

	for _, sample := range results.(model.Vector) {
		var tags []string
		name := query.Name
		for label, val := range sample.Metric {
			switch label {
			case "__name__":
				name = string(val)
			default:
				tags = append(tags, fmt.Sprintf("%s:%s", label, val))
			}
		}

		name = strings.TrimSpace(name)

		if name == "" {
			failedPushedMetrics.WithLabelValues("invalid-name").Inc()
			return fmt.Errorf("Invalid metric name from %v", query)
		}

		post_pushed_metric := func(metric_type string) {
			pushedMetrics.WithLabelValues(name, name, metric_type).Inc()
		}

		switch query.Type {
		case Gauge:
			err = statsd_client.Gauge(name, float64(sample.Value), tags, 1)
			post_pushed_metric("gauge")
		case Counter:
			err = statsd_client.Count(name, int64(sample.Value), tags, 1)
			post_pushed_metric("counter")
		case Histogram:
			err = statsd_client.Histogram(name, float64(sample.Value), tags, 1)
			post_pushed_metric("histogram")
		case Milliseconds:
			err = statsd_client.TimeInMilliseconds(name, float64(sample.Value), tags, 1)
			post_pushed_metric("milliseconds")
		default:
			return fmt.Errorf("Can't handle %g", query.Type)
		}
		if err != nil {
			failedPushedMetrics.WithLabelValues("failed-push").Inc()
			return err
		}
	}
	return err
}

func start_querying(ticker *time.Ticker, queries Queries, query_api prometheus.QueryAPI, statsd_client *statsd.Client) {

	go func() {
		for now := range ticker.C {
			for _, query := range queries {
				run_query(query, query_api, now, statsd_client)
			}
		}
	}()
}

func init() {
	prometheus_metrics.MustRegister(pushedMetrics)
	prometheus_metrics.MustRegister(failedQueries)
	prometheus_metrics.MustRegister(failedPushedMetrics)
}

func main() {
	flag.Var(&queries, "query", "Prometheus query (in form type:datadog_metric_name:prometheus_query). Can be specified multiple times.")
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
	ticker := time.NewTicker(duration)
	start_querying(ticker, queries, prometheus_query_api, statsd_client)

	http.Handle("/metrics", prometheus_metrics.Handler())
	http.ListenAndServe(*listen_addr, nil)
}

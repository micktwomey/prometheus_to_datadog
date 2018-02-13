package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"golang.org/x/net/context"

	"gopkg.in/yaml.v2"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/prometheus/client_golang/api/prometheus"
	prometheusMetrics "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/model"
	"github.com/spf13/viper"
)

type QueryType string

const (
	Gauge        QueryType = "gauge"
	Counter                = "counter"
	Histogram              = "histogram"
	Set                    = "set"
	Milliseconds           = "milliseconds"
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

var (
	pushedMetrics = prometheusMetrics.NewCounterVec(
		prometheusMetrics.CounterOpts{
			Namespace: "prometheus_to_datadog",
			Name:      "pushed_metrics_total",
			Help:      "Number of metrics pushed",
		},
		[]string{"prometheus_name", "dogstatsd_name", "metric_type"},
	)
	failedQueries = prometheusMetrics.NewCounterVec(
		prometheusMetrics.CounterOpts{
			Namespace: "prometheus_to_datadog",
			Name:      "failed_queries_total",
			Help:      "Number of failed queries to Prometheus",
		},
		[]string{"query"},
	)
	failedPushedMetrics = prometheusMetrics.NewCounterVec(
		prometheusMetrics.CounterOpts{
			Namespace: "prometheus_to_datadog",
			Name:      "failed_pushed_metrics_total",
			Help:      "Number of failed failed metric pushes.",
		},
		[]string{"reason"},
	)
)

func runQuery(query Query, queryAPI prometheus.QueryAPI, when time.Time, statsdClient *statsd.Client) error {
	var err error
	results, err := queryAPI.Query(context.Background(), query.Query, when)
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
			return fmt.Errorf("invalid metric name from %v", query)
		}

		postPushedMetric := func(metricType string) {
			pushedMetrics.WithLabelValues(name, name, metricType).Inc()
		}

		switch query.Type {
		case Gauge:
			err = statsdClient.Gauge(name, float64(sample.Value), tags, 1)
			postPushedMetric("gauge")
		case Counter:
			err = statsdClient.Count(name, int64(sample.Value), tags, 1)
			postPushedMetric("counter")
		case Histogram:
			err = statsdClient.Histogram(name, float64(sample.Value), tags, 1)
			postPushedMetric("histogram")
		case Milliseconds:
			err = statsdClient.TimeInMilliseconds(name, float64(sample.Value), tags, 1)
			postPushedMetric("milliseconds")
		case Set:
			return fmt.Errorf("cannot handle query type 'set' (yet)")
		default:
			return fmt.Errorf("unknown query type %g", query.Type)
		}
		if err != nil {
			failedPushedMetrics.WithLabelValues("failed-push").Inc()
			return err
		}
	}
	return err
}

func startQuerying(ticker *time.Ticker, queries Queries, queryAPI prometheus.QueryAPI, statsdClient *statsd.Client) {

	go func() {
		for now := range ticker.C {
			for _, query := range queries {
				runQuery(query, queryAPI, now, statsdClient)
			}
		}
	}()
}

func init() {
	prometheusMetrics.MustRegister(pushedMetrics)
	prometheusMetrics.MustRegister(failedQueries)
	prometheusMetrics.MustRegister(failedPushedMetrics)
}

func main() {
	// Load config
	viper.SetConfigName("prometheus_to_datadog")
	viper.AddConfigPath("config/")
	viper.AddConfigPath("etc/prometheus_to_datadog")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}

	statsdClient, err := statsd.New(viper.GetString("dogstatsd.address"))
	if err != nil {
		panic(err)
	}
	defer statsdClient.Close()

	statsdClient.Namespace = viper.GetString("prometheus_to_datadog.namespace") + "."

	prometheusConfig := prometheus.Config{Address: viper.GetString("prometheus.address")}
	prometheusClient, err := prometheus.New(prometheusConfig)
	if err != nil {
		panic(err)
	}

	prometheusQueryAPI := prometheus.NewQueryAPI(prometheusClient)

	queryInterval := time.Duration(viper.GetInt("prometheus_to_datadog.query_interval")) * time.Second
	ticker := time.NewTicker(queryInterval)

	// Load queries
	queryFile, err := ioutil.ReadFile(viper.GetString("prometheus_to_datadog.query_file_path"))
	if err != nil {
		panic(fmt.Errorf("Fatal error queries file: %s \n", err))
	}
	var queries []Query
	err = yaml.Unmarshal(queryFile, &queries)
	if err != nil {
		panic(fmt.Errorf("Fatal error parsing queries file: %s \n", err))
	}

	startQuerying(ticker, queries, prometheusQueryAPI, statsdClient)

	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(viper.GetString("prometheus_to_datadog.listen_address"), nil)
}

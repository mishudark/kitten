package metrics

import (
	"log"

	"contrib.go.opencensus.io/exporter/prometheus"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

var (
	// LatencyMs is the latency in milliseconds
	LatencyMs = stats.Float64("grpc/latency", "The latency in milliseconds", "ms")

	// Hits counts the number of hits to  the endpoint
	Hits = stats.Int64("grpc/lines_in", "The number of hits to the endpoint", "1")

	// Errors encounters the number of errors
	Errors = stats.Int64("grpc/errors", "The number of errors encountered", "1")
)

// KeyMethod has a content "method"
var KeyMethod, _ = tag.NewKey("method") // nolint: errcheck

var (
	// LatencyView is the latency in ms
	LatencyView = &view.View{
		Name:        "service/latency",
		Measure:     LatencyMs,
		Description: "The distribution of the latencies",

		// Latency in buckets:
		// [>=0ms, >=25ms, >=50ms, >=75ms, >=100ms, >=200ms, >=400ms, >=600ms, >=800ms, >=1s, >=2s, >=4s, >=6s]
		Aggregation: view.Distribution(0, 25, 50, 75, 100, 200, 400, 600, 800, 1000, 2000, 4000, 6000),
		TagKeys:     []tag.Key{KeyMethod}}

	// HitsCountView count the hits to a method
	HitsCountView = &view.View{
		Name:        "service/hits",
		Measure:     Hits,
		Description: "The number of hits for a given method",
		Aggregation: view.Count(),
	}

	// ErrorCountView the number of errors encountered
	ErrorCountView = &view.View{
		Name:        "service/errors",
		Measure:     Errors,
		Description: "The number of errors encountered",
		Aggregation: view.Count(),
	}
)

// NewPrometheusExporter returns a prometheus exporter based on the previous
// defined metrics
func NewPrometheusExporter(namespace string) *prometheus.Exporter {
	// Register the views, it is imperative that this step exists
	// lest recorded metrics will be dropped and never exported.
	if err := view.Register(LatencyView, HitsCountView, ErrorCountView); err != nil {
		log.Fatalf("Failed to register the views: %v", err)
	}

	// Create the Prometheus exporter.
	pe, err := prometheus.NewExporter(prometheus.Options{
		Namespace: namespace,
	})
	if err != nil {
		log.Fatalf("Failed to create the Prometheus stats exporter: %v", err)
	}

	// Register the Prometheus exporter.
	// This step is needed so that metrics can be exported.
	view.RegisterExporter(pe)

	return pe
}

package middleware

import (
	"context"
	"time"

	"github.com/go-kit/kit/endpoint"
	"github.com/mishudark/kitten/metrics"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

// Metrics add prometheus instrumentation to endpoints, it records
// Latency, Hits and Errors
func Metrics(e endpoint.Endpoint, method string) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		ctx, err = tag.New(ctx, tag.Insert(metrics.KeyMethod, method))
		if err != nil {
			return nil, err
		}

		defer func(since time.Time) {
			ms := float64(time.Since(since).Nanoseconds()) / 1e6
			stats.Record(ctx, metrics.Hits.M(1), metrics.LatencyMs.M(ms))

			if err != nil {
				stats.Record(ctx, metrics.Errors.M(1))
			}
		}(time.Now())

		return e(ctx, request)
	}
}

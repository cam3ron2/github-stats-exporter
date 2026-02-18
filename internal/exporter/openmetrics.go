package exporter

import (
	"net/http"
	"sort"

	"github.com/cam3ron2/github-stats/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// SnapshotReader reads metric snapshots.
type SnapshotReader interface {
	Snapshot() []store.MetricPoint
}

type cacheMetricsProvider interface {
	CacheStats() CacheStats
}

// NewOpenMetricsHandler returns a handler that renders store snapshots through the Prometheus OpenMetrics encoder.
func NewOpenMetricsHandler(reader SnapshotReader) http.Handler {
	registry := prometheus.NewRegistry()
	registry.MustRegister(&snapshotCollector{reader: reader})

	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}

type snapshotCollector struct {
	reader SnapshotReader
}

func (c *snapshotCollector) Describe(_ chan<- *prometheus.Desc) {}

func (c *snapshotCollector) Collect(ch chan<- prometheus.Metric) {
	if c == nil || c.reader == nil {
		return
	}

	points := c.reader.Snapshot()
	seriesLoaded := make(map[string]float64)
	for _, point := range points {
		if point.Name == "" {
			continue
		}
		seriesLoaded[point.Name]++

		labelKeys := make([]string, 0, len(point.Labels))
		for key := range point.Labels {
			labelKeys = append(labelKeys, key)
		}
		sort.Strings(labelKeys)

		labelValues := make([]string, 0, len(labelKeys))
		for _, key := range labelKeys {
			labelValues = append(labelValues, point.Labels[key])
		}

		desc := prometheus.NewDesc(point.Name, point.Name, labelKeys, nil)
		metric, err := prometheus.NewConstMetric(desc, prometheus.GaugeValue, point.Value, labelValues...)
		if err != nil {
			continue
		}
		ch <- metric
	}

	for metricName, loaded := range seriesLoaded {
		desc := prometheus.NewDesc(
			"gh_exporter_metrics_series_loaded",
			"number of metric series loaded in exporter cache by metric name",
			[]string{"metric"},
			nil,
		)
		metric, err := prometheus.NewConstMetric(desc, prometheus.GaugeValue, loaded, metricName)
		if err != nil {
			continue
		}
		ch <- metric
	}

	provider, ok := c.reader.(cacheMetricsProvider)
	if !ok {
		return
	}
	stats := provider.CacheStats()
	desc := prometheus.NewDesc(
		"gh_exporter_metrics_cache_refresh_duration_seconds",
		"duration in seconds of the latest exporter cache refresh",
		nil,
		nil,
	)
	metric, err := prometheus.NewConstMetric(desc, prometheus.GaugeValue, stats.RefreshDuration.Seconds())
	if err != nil {
		return
	}
	ch <- metric
}

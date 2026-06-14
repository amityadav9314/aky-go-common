package metric

import "context"

// MetricsConf is a placeholder for future external metrics wiring.
type MetricsConf struct {
	Enable bool
}

func NewMetricsConf() MetricsConf {
	return MetricsConf{Enable: false}
}

// SetupMetricLogging is a no-op until a metrics backend is added.
func SetupMetricLogging() {}

func IncrementMetricCounter(_ context.Context, _ string, _ []Tag, _ MetricsConf) {}

func LogLatencyMetric(_ string, _ int64, _ int, _ string) {}

// Tag is a simple key/value metric tag (reserved for future use).
type Tag struct {
	Key   string
	Value string
}

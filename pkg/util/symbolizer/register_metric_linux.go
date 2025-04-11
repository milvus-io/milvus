package symbolizer

import (
	"github.com/ianlancetaylor/cgosymbolizer"
	"github.com/prometheus/client_golang/prometheus"
)

func RegisterJemallocStatsMetrics(namespace string, r prometheus.Registerer) {
	cgosymbolizer.RegisterJemallocStatsMetrics(namespace, r)
}

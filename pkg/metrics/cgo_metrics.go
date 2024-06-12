package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	subsystemCGO = "cgo"
	cgoLabelName = "name"
	once         sync.Once

	ActiveFutureTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: subsystemCGO,
			Name:      "active_future_total",
			Help:      "Total number of active futures.",
		}, []string{
			nodeIDLabelName,
		},
	)

	RunningCgoCallTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: subsystemCGO,
			Name:      "running_cgo_call_total",
			Help:      "Total number of running cgo calls.",
		}, []string{
			nodeIDLabelName,
		})

	CGODuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: subsystemCGO,
			Name:      "cgo_duration_seconds",
			Help:      "Histogram of cgo call duration in seconds.",
		}, []string{
			nodeIDLabelName,
			cgoLabelName,
		},
	)

	CGOQueueDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: subsystemCGO,
			Name:      "cgo_queue_duration_seconds",
			Help:      "Duration of cgo call in queue.",
		}, []string{
			nodeIDLabelName,
		},
	)
)

// RegisterCGOMetrics registers the cgo metrics.
func RegisterCGOMetrics(registry *prometheus.Registry) {
	once.Do(func() {
		prometheus.MustRegister(RunningCgoCallTotal)
		prometheus.MustRegister(CGODuration)
		prometheus.MustRegister(CGOQueueDuration)
	})
}

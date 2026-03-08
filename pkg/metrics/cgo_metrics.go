package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	subsystemCGO      = "cgo"
	cgoLabelName      = "name"
	once              sync.Once
	bucketsForCGOCall = []float64{
		10 * time.Nanosecond.Seconds(),
		100 * time.Nanosecond.Seconds(),
		250 * time.Nanosecond.Seconds(),
		500 * time.Nanosecond.Seconds(),
		time.Microsecond.Seconds(),
		10 * time.Microsecond.Seconds(),
		20 * time.Microsecond.Seconds(),
		50 * time.Microsecond.Seconds(),
		100 * time.Microsecond.Seconds(),
		250 * time.Microsecond.Seconds(),
		500 * time.Microsecond.Seconds(),
		time.Millisecond.Seconds(),
		2 * time.Millisecond.Seconds(),
		10 * time.Millisecond.Seconds(),
	}

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
			Buckets:   bucketsForCGOCall,
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
			Buckets:   bucketsForCGOCall,
		}, []string{
			nodeIDLabelName,
		},
	)
)

// RegisterCGOMetrics registers the cgo metrics.
func RegisterCGOMetrics(registry *prometheus.Registry) {
	once.Do(func() {
		registry.MustRegister(ActiveFutureTotal)
		registry.MustRegister(RunningCgoCallTotal)
		registry.MustRegister(CGODuration)
		registry.MustRegister(CGOQueueDuration)
	})
}

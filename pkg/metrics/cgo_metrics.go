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

	// UnmappedSegcoreCodeTotal counts segcore (C++ ErrorCode) values that arrive
	// over the cgo boundary but are not registered in merr's segcore code table,
	// so they fall back to a generic non-retriable error. A non-zero, growing
	// value means the C++ side added an ErrorCode the Go classifier has not been
	// taught about yet (classification drift) -- the precise retry/ownership
	// policy is degraded until it is registered.
	UnmappedSegcoreCodeTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subsystemCGO,
			Name:      "unmapped_segcore_code_total",
			Help:      "Total number of unregistered segcore error codes seen at the cgo boundary, by code.",
		}, []string{
			"code",
		},
	)

	// UnexpectedSegcoreOriginTotal counts UnexpectedError(2001) errors crossing
	// the cgo boundary, by the C++ source location that raised them.
	//
	// 2001 is the bucket for a failure the C++ core could not classify: an
	// invariant violation (AssertInfo) or an unclassified exception. Every other
	// segcore code names what went wrong, so 2001 alone is not actionable -- the
	// location is. Sites are only labeled once they actually fire, so the
	// series count is bounded by real failures, not by the number of asserts.
	//
	// Reading it: a site that appears here is, by construction, a Milvus bug or
	// a misclassification (a condition driven by external input -- corrupt file,
	// full disk, OOM -- that should carry a specific retriable code instead).
	// Either way the location names the code to look at.
	UnexpectedSegcoreOriginTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subsystemCGO,
			Name:      "unexpected_segcore_origin_total",
			Help:      "Total number of segcore UnexpectedError(2001) results at the cgo boundary, by C++ source location.",
		}, []string{
			"origin",
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
		registry.MustRegister(UnmappedSegcoreCodeTotal)
		registry.MustRegister(UnexpectedSegcoreOriginTotal)
	})
}

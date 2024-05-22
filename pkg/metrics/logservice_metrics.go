package metrics

import (
	"sync"

	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	subsystemLogServiceClient           = "logservice"
	LogServiceClientProducerAvailable   = "available"
	LogServiceClientProducerUnAvailable = "unavailable"
)

var (
	logServiceClientRegisterOnce sync.Once

	// from 64 bytes to 5MB
	bytesBuckets = prometheus.ExponentialBucketsRange(64, 5242880, 10)
	// from 1ms to 5s
	secondsBuckets = prometheus.ExponentialBucketsRange(0.001, 5, 10)

	// Client side metrics
	LogServiceClientProducerTotal = newLogServiceClientGaugeVec(prometheus.GaugeOpts{
		Name: "producer_total",
		Help: "Total of producers",
	}, statusLabelName)

	LogServiceClientConsumerTotal = newLogServiceClientGaugeVec(prometheus.GaugeOpts{
		Name: "consumer_total",
		Help: "Total of consumers",
	}, statusLabelName)

	LogServiceClientProduceBytes = newLogServiceClientHistogramVec(prometheus.HistogramOpts{
		Name:    "produce_bytes",
		Help:    "Bytes of produced message",
		Buckets: bytesBuckets,
	})

	LogServiceClientConsumeBytes = newLogServiceClientHistogramVec(prometheus.HistogramOpts{
		Name:    "consume_bytes",
		Help:    "Bytes of consumed message",
		Buckets: bytesBuckets,
	})

	LogServiceClientProduceDurationSeconds = newLogServiceClientHistogramVec(
		prometheus.HistogramOpts{
			Name:    "produce_duration_seconds",
			Help:    "Duration of client produce",
			Buckets: secondsBuckets,
		},
		statusLabelName,
	)

	// LogCoord metrics
	LogCoordPChannelTotal = newLogCoordGaugeVec(prometheus.GaugeOpts{
		Name: "pchannel_total",
		Help: "Total of pchannels",
	})

	// LogCoordVChannelTotal = newLogCoordGaugeVec(prometheus.GaugeOpts{
	// 	Name: "vchannel_total",
	// 	Help: "Total of vchannels",
	// })

	LogCoordAssignmentListenerTotal = newLogCoordGaugeVec(prometheus.GaugeOpts{
		Name: "assignment_listener_total",
		Help: "Total of assignment listener",
	})

	LogCoordAssignmentInfo = newLogCoordGaugeVec(prometheus.GaugeOpts{
		Name: "assignment_info",
		Help: "Info of assignment",
	}, "global_version", "local_version")

	// LogNode metrics
	LogNodeWALTotal = newLogNodeGaugeVec(prometheus.GaugeOpts{
		Name: "wal_total",
		Help: "Total of wal",
	})

	LogNodeProducerTotal = newLogNodeGaugeVec(prometheus.GaugeOpts{
		Name: "producer_total",
		Help: "Total of producers",
	})

	LogNodeConsumerTotal = newLogNodeGaugeVec(prometheus.GaugeOpts{
		Name: "consumer_total",
		Help: "Total of consumers",
	})

	LogNodeProduceBytes = newLogNodeHistogramVec(prometheus.HistogramOpts{
		Name:    "produce_bytes",
		Help:    "Bytes of produced message",
		Buckets: bytesBuckets,
	})

	LogNodeConsumeBytes = newLogNodeHistogramVec(prometheus.HistogramOpts{
		Name:    "consume_bytes",
		Help:    "Bytes of consumed message",
		Buckets: bytesBuckets,
	})

	LogNodeProduceDurationSeconds = newLogNodeHistogramVec(prometheus.HistogramOpts{
		Name:    "produce_duration_seconds",
		Help:    "Duration of producing message",
		Buckets: secondsBuckets,
	}, statusLabelName)
)

func RegisterLogServiceClient(registry *prometheus.Registry) {
	logServiceClientRegisterOnce.Do(func() {
		registry.MustRegister(LogServiceClientProducerTotal)
		registry.MustRegister(LogServiceClientConsumerTotal)
		registry.MustRegister(LogServiceClientProduceBytes)
		registry.MustRegister(LogServiceClientConsumeBytes)
		registry.MustRegister(LogServiceClientProduceDurationSeconds)
	})
}

// RegisterLogCoord registers log service metrics
func RegisterLogCoord(registry *prometheus.Registry) {
	registry.MustRegister(LogCoordPChannelTotal)
	registry.MustRegister(LogCoordAssignmentListenerTotal)
	registry.MustRegister(LogCoordAssignmentInfo)
}

// RegisterLogNode registers log service metrics
func RegisterLogNode(registry *prometheus.Registry) {
	registry.MustRegister(LogNodeWALTotal)
	registry.MustRegister(LogNodeProducerTotal)
	registry.MustRegister(LogNodeConsumerTotal)
	registry.MustRegister(LogNodeProduceBytes)
	registry.MustRegister(LogNodeConsumeBytes)
	registry.MustRegister(LogNodeProduceDurationSeconds)
}

func newLogCoordGaugeVec(opts prometheus.GaugeOpts, extra ...string) *prometheus.GaugeVec {
	opts.Namespace = milvusNamespace
	opts.Subsystem = typeutil.LogCoordRole
	labels := mergeLabel(extra...)
	return prometheus.NewGaugeVec(opts, labels)
}

func newLogServiceClientGaugeVec(opts prometheus.GaugeOpts, extra ...string) *prometheus.GaugeVec {
	opts.Namespace = milvusNamespace
	opts.Subsystem = subsystemLogServiceClient
	labels := mergeLabel(extra...)
	return prometheus.NewGaugeVec(opts, labels)
}

func newLogServiceClientHistogramVec(opts prometheus.HistogramOpts, extra ...string) *prometheus.HistogramVec {
	opts.Namespace = milvusNamespace
	opts.Subsystem = subsystemLogServiceClient
	labels := mergeLabel(extra...)
	return prometheus.NewHistogramVec(opts, labels)
}

func newLogNodeGaugeVec(opts prometheus.GaugeOpts, extra ...string) *prometheus.GaugeVec {
	opts.Namespace = milvusNamespace
	opts.Subsystem = typeutil.LogNodeRole
	labels := mergeLabel(extra...)
	return prometheus.NewGaugeVec(opts, labels)
}

func newLogNodeHistogramVec(opts prometheus.HistogramOpts, extra ...string) *prometheus.HistogramVec {
	opts.Namespace = milvusNamespace
	opts.Subsystem = typeutil.LogNodeRole
	labels := mergeLabel(extra...)
	return prometheus.NewHistogramVec(opts, labels)
}

func mergeLabel(extra ...string) []string {
	labels := make([]string, 0, 1+len(extra))
	labels = append(labels, nodeIDLabelName)
	labels = append(labels, extra...)
	return labels
}

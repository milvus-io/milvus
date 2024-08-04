package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	subsystemStreamingServiceClient           = "streaming"
	StreamingServiceClientProducerAvailable   = "available"
	StreamingServiceClientProducerUnAvailable = "unavailable"
)

var (
	logServiceClientRegisterOnce sync.Once

	// from 64 bytes to 5MB
	bytesBuckets = prometheus.ExponentialBucketsRange(64, 5242880, 10)
	// from 1ms to 5s
	secondsBuckets = prometheus.ExponentialBucketsRange(0.001, 5, 10)

	// Client side metrics
	StreamingServiceClientProducerTotal = newStreamingServiceClientGaugeVec(prometheus.GaugeOpts{
		Name: "producer_total",
		Help: "Total of producers",
	}, statusLabelName)

	StreamingServiceClientConsumerTotal = newStreamingServiceClientGaugeVec(prometheus.GaugeOpts{
		Name: "consumer_total",
		Help: "Total of consumers",
	}, statusLabelName)

	StreamingServiceClientProduceBytes = newStreamingServiceClientHistogramVec(prometheus.HistogramOpts{
		Name:    "produce_bytes",
		Help:    "Bytes of produced message",
		Buckets: bytesBuckets,
	}, statusLabelName)

	StreamingServiceClientConsumeBytes = newStreamingServiceClientHistogramVec(prometheus.HistogramOpts{
		Name:    "consume_bytes",
		Help:    "Bytes of consumed message",
		Buckets: bytesBuckets,
	})

	StreamingServiceClientProduceDurationSeconds = newStreamingServiceClientHistogramVec(
		prometheus.HistogramOpts{
			Name:    "produce_duration_seconds",
			Help:    "Duration of client produce",
			Buckets: secondsBuckets,
		},
		statusLabelName,
	)

	// StreamingCoord metrics
	StreamingCoordPChannelTotal = newStreamingCoordGaugeVec(prometheus.GaugeOpts{
		Name: "pchannel_total",
		Help: "Total of pchannels",
	})

	StreamingCoordAssignmentListenerTotal = newStreamingCoordGaugeVec(prometheus.GaugeOpts{
		Name: "assignment_listener_total",
		Help: "Total of assignment listener",
	})

	StreamingCoordAssignmentVersion = newStreamingCoordGaugeVec(prometheus.GaugeOpts{
		Name: "assignment_info",
		Help: "Info of assignment",
	})

	// StreamingNode metrics
	StreamingNodeWALTotal = newStreamingNodeGaugeVec(prometheus.GaugeOpts{
		Name: "wal_total",
		Help: "Total of wal",
	})

	StreamingNodeProducerTotal = newStreamingNodeGaugeVec(prometheus.GaugeOpts{
		Name: "producer_total",
		Help: "Total of producers",
	})

	StreamingNodeConsumerTotal = newStreamingNodeGaugeVec(prometheus.GaugeOpts{
		Name: "consumer_total",
		Help: "Total of consumers",
	})

	StreamingNodeProduceBytes = newStreamingNodeHistogramVec(prometheus.HistogramOpts{
		Name:    "produce_bytes",
		Help:    "Bytes of produced message",
		Buckets: bytesBuckets,
	}, channelNameLabelName, channelTermLabelName, statusLabelName)

	StreamingNodeConsumeBytes = newStreamingNodeHistogramVec(prometheus.HistogramOpts{
		Name:    "consume_bytes",
		Help:    "Bytes of consumed message",
		Buckets: bytesBuckets,
	}, channelNameLabelName, channelTermLabelName)

	StreamingNodeProduceDurationSeconds = newStreamingNodeHistogramVec(prometheus.HistogramOpts{
		Name:    "produce_duration_seconds",
		Help:    "Duration of producing message",
		Buckets: secondsBuckets,
	}, channelNameLabelName, channelTermLabelName, statusLabelName)
)

func RegisterStreamingServiceClient(registry *prometheus.Registry) {
	logServiceClientRegisterOnce.Do(func() {
		registry.MustRegister(StreamingServiceClientProducerTotal)
		registry.MustRegister(StreamingServiceClientConsumerTotal)
		registry.MustRegister(StreamingServiceClientProduceBytes)
		registry.MustRegister(StreamingServiceClientConsumeBytes)
		registry.MustRegister(StreamingServiceClientProduceDurationSeconds)
	})
}

// RegisterStreamingCoord registers log service metrics
func RegisterStreamingCoord(registry *prometheus.Registry) {
	registry.MustRegister(StreamingCoordPChannelTotal)
	registry.MustRegister(StreamingCoordAssignmentListenerTotal)
	registry.MustRegister(StreamingCoordAssignmentVersion)
}

// RegisterStreamingNode registers log service metrics
func RegisterStreamingNode(registry *prometheus.Registry) {
	registry.MustRegister(StreamingNodeWALTotal)
	registry.MustRegister(StreamingNodeProducerTotal)
	registry.MustRegister(StreamingNodeConsumerTotal)
	registry.MustRegister(StreamingNodeProduceBytes)
	registry.MustRegister(StreamingNodeConsumeBytes)
	registry.MustRegister(StreamingNodeProduceDurationSeconds)
}

func newStreamingCoordGaugeVec(opts prometheus.GaugeOpts, extra ...string) *prometheus.GaugeVec {
	opts.Namespace = milvusNamespace
	opts.Subsystem = typeutil.StreamingCoordRole
	labels := mergeLabel(extra...)
	return prometheus.NewGaugeVec(opts, labels)
}

func newStreamingServiceClientGaugeVec(opts prometheus.GaugeOpts, extra ...string) *prometheus.GaugeVec {
	opts.Namespace = milvusNamespace
	opts.Subsystem = subsystemStreamingServiceClient
	labels := mergeLabel(extra...)
	return prometheus.NewGaugeVec(opts, labels)
}

func newStreamingServiceClientHistogramVec(opts prometheus.HistogramOpts, extra ...string) *prometheus.HistogramVec {
	opts.Namespace = milvusNamespace
	opts.Subsystem = subsystemStreamingServiceClient
	labels := mergeLabel(extra...)
	return prometheus.NewHistogramVec(opts, labels)
}

func newStreamingNodeGaugeVec(opts prometheus.GaugeOpts, extra ...string) *prometheus.GaugeVec {
	opts.Namespace = milvusNamespace
	opts.Subsystem = typeutil.StreamingNodeRole
	labels := mergeLabel(extra...)
	return prometheus.NewGaugeVec(opts, labels)
}

func newStreamingNodeHistogramVec(opts prometheus.HistogramOpts, extra ...string) *prometheus.HistogramVec {
	opts.Namespace = milvusNamespace
	opts.Subsystem = typeutil.StreamingNodeRole
	labels := mergeLabel(extra...)
	return prometheus.NewHistogramVec(opts, labels)
}

func mergeLabel(extra ...string) []string {
	labels := make([]string, 0, 1+len(extra))
	labels = append(labels, nodeIDLabelName)
	labels = append(labels, extra...)
	return labels
}

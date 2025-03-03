package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	subsystemStreamingServiceClient         = "streaming"
	subsystemWAL                            = "wal"
	StreamingServiceClientStatusAvailable   = "available"
	StreamingServiceClientStatusUnavailable = "unavailable"
	StreamingServiceClientStatusOK          = "ok"
	StreamingServiceClientStatusCancel      = "cancel"
	StreamignServiceClientStatusError       = "error"

	BroadcasterTaskStateLabelName     = "state"
	ResourceKeyDomainLabelName        = "domain"
	TimeTickSyncTypeLabelName         = "type"
	TimeTickAckTypeLabelName          = "type"
	WALTxnStateLabelName              = "state"
	WALChannelLabelName               = channelNameLabelName
	WALSegmentSealPolicyNameLabelName = "policy"
	WALSegmentAllocStateLabelName     = "state"
	WALMessageTypeLabelName           = "message_type"
	WALChannelTermLabelName           = "term"
	WALNameLabelName                  = "wal_name"
	WALTxnTypeLabelName               = "txn_type"
	StatusLabelName                   = statusLabelName
	StreamingNodeLabelName            = "streaming_node"
	NodeIDLabelName                   = nodeIDLabelName
)

var (
	StreamingServiceClientRegisterOnce sync.Once

	// from 64 bytes to 8MB
	messageBytesBuckets = prometheus.ExponentialBucketsRange(64, 8388608, 10)
	// from 1ms to 5s
	secondsBuckets = prometheus.ExponentialBucketsRange(0.001, 5, 10)

	// Streaming Service Client Producer Metrics.
	StreamingServiceClientProducerTotal = newStreamingServiceClientGaugeVec(prometheus.GaugeOpts{
		Name: "producer_total",
		Help: "Total of producers",
	}, WALChannelLabelName, StatusLabelName)

	StreamingServiceClientProduceInflightTotal = newStreamingServiceClientGaugeVec(prometheus.GaugeOpts{
		Name: "produce_inflight_total",
		Help: "Total of inflight produce request",
	}, WALChannelLabelName)

	StreamingServiceClientProduceBytes = newStreamingServiceClientHistogramVec(prometheus.HistogramOpts{
		Name:    "produce_bytes",
		Help:    "Bytes of produced message",
		Buckets: messageBytesBuckets,
	}, WALChannelLabelName, StatusLabelName)

	StreamingServiceClientProduceDurationSeconds = newStreamingServiceClientHistogramVec(
		prometheus.HistogramOpts{
			Name:    "produce_duration_seconds",
			Help:    "Duration of client produce",
			Buckets: secondsBuckets,
		}, WALChannelLabelName, StatusLabelName)

	// Streaming Service Client Consumer Metrics.
	StreamingServiceClientConsumerTotal = newStreamingServiceClientGaugeVec(prometheus.GaugeOpts{
		Name: "consumer_total",
		Help: "Total of consumers",
	}, WALChannelLabelName, StatusLabelName)

	StreamingServiceClientConsumeBytes = newStreamingServiceClientHistogramVec(prometheus.HistogramOpts{
		Name:    "consume_bytes",
		Help:    "Bytes of consumed message",
		Buckets: messageBytesBuckets,
	}, WALChannelLabelName)

	StreamingServiceClientConsumeInflightTotal = newStreamingServiceClientGaugeVec(prometheus.GaugeOpts{
		Name: "consume_inflight_total",
		Help: "Total of inflight consume body",
	}, WALChannelLabelName)

	// StreamingCoord metrics
	StreamingCoordPChannelInfo = newStreamingCoordGaugeVec(prometheus.GaugeOpts{
		Name: "pchannel_info",
		Help: "Term of pchannels",
	}, WALChannelLabelName, WALChannelTermLabelName, StreamingNodeLabelName)

	StreamingCoordAssignmentVersion = newStreamingCoordGaugeVec(prometheus.GaugeOpts{
		Name: "assignment_info",
		Help: "Info of assignment",
	})

	StreamingCoordAssignmentListenerTotal = newStreamingCoordGaugeVec(prometheus.GaugeOpts{
		Name: "assignment_listener_total",
		Help: "Total of assignment listener",
	})

	StreamingCoordBroadcasterTaskTotal = newStreamingCoordGaugeVec(prometheus.GaugeOpts{
		Name: "broadcaster_task_total",
		Help: "Total of broadcaster task",
	}, BroadcasterTaskStateLabelName)

	StreamingCoordBroadcastDurationSeconds = newStreamingCoordHistogramVec(prometheus.HistogramOpts{
		Name:    "broadcaster_broadcast_duration_seconds",
		Help:    "Duration of broadcast",
		Buckets: secondsBuckets,
	})

	StreamingCoordBroadcasterAckAllDurationSeconds = newStreamingCoordHistogramVec(prometheus.HistogramOpts{
		Name:    "broadcaster_ack_all_duration_seconds",
		Help:    "Duration of acknowledge all message",
		Buckets: secondsBuckets,
	})

	StreamingCoordResourceKeyTotal = newStreamingCoordGaugeVec(prometheus.GaugeOpts{
		Name: "resource_key_total",
		Help: "Total of resource key hold at streaming coord",
	}, ResourceKeyDomainLabelName)

	// StreamingNode Producer Server Metrics.
	StreamingNodeProducerTotal = newStreamingNodeGaugeVec(prometheus.GaugeOpts{
		Name: "producer_total",
		Help: "Total of producers on current streaming node",
	}, WALChannelLabelName)

	StreamingNodeProduceInflightTotal = newStreamingNodeGaugeVec(prometheus.GaugeOpts{
		Name: "produce_inflight_total",
		Help: "Total of inflight produce request",
	}, WALChannelLabelName)

	// StreamingNode Consumer Server Metrics.
	StreamingNodeConsumerTotal = newStreamingNodeGaugeVec(prometheus.GaugeOpts{
		Name: "consumer_total",
		Help: "Total of consumers on current streaming node",
	}, WALChannelLabelName)

	StreamingNodeConsumeInflightTotal = newStreamingNodeGaugeVec(prometheus.GaugeOpts{
		Name: "consume_inflight_total",
		Help: "Total of inflight consume body",
	}, WALChannelLabelName)

	StreamingNodeConsumeBytes = newStreamingNodeHistogramVec(prometheus.HistogramOpts{
		Name:    "consume_bytes",
		Help:    "Bytes of consumed message",
		Buckets: messageBytesBuckets,
	}, WALChannelLabelName)

	// WAL WAL metrics
	WALInfo = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "info",
		Help: "current info of wal on current streaming node",
	}, WALChannelLabelName, WALChannelTermLabelName, WALNameLabelName)

	// TimeTick related metrics
	WALLastAllocatedTimeTick = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "last_allocated_time_tick",
		Help: "Current max allocated time tick of wal",
	}, WALChannelLabelName)

	WALAllocateTimeTickTotal = newWALCounterVec(prometheus.CounterOpts{
		Name: "allocate_time_tick_total",
		Help: "Total of allocated time tick on wal",
	}, WALChannelLabelName)

	WALLastConfirmedTimeTick = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "last_confirmed_time_tick",
		Help: "Current max confirmed time tick of wal",
	}, WALChannelLabelName)

	WALAcknowledgeTimeTickTotal = newWALCounterVec(prometheus.CounterOpts{
		Name: "acknowledge_time_tick_total",
		Help: "Total of acknowledge time tick on wal",
	}, WALChannelLabelName, TimeTickAckTypeLabelName)

	WALSyncTimeTickTotal = newWALCounterVec(prometheus.CounterOpts{
		Name: "sync_time_tick_total",
		Help: "Total of sync time tick on wal",
	}, WALChannelLabelName, TimeTickAckTypeLabelName)

	WALTimeTickSyncTotal = newWALCounterVec(prometheus.CounterOpts{
		Name: "sync_total",
		Help: "Total of time tick sync sent",
	}, WALChannelLabelName, TimeTickSyncTypeLabelName)

	WALTimeTickSyncTimeTick = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "sync_time_tick",
		Help: "Max time tick of time tick sync sent",
	}, WALChannelLabelName, TimeTickSyncTypeLabelName)

	// Txn Related Metrics
	WALInflightTxn = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "inflight_txn",
		Help: "Total of inflight txn on wal",
	}, WALChannelLabelName)

	WALFinishTxn = newWALCounterVec(prometheus.CounterOpts{
		Name: "finish_txn",
		Help: "Total of finish txn on wal",
	}, WALChannelLabelName, WALTxnStateLabelName)

	// Segment related metrics
	WALSegmentAllocTotal = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "segment_assign_segment_alloc_total",
		Help: "Total of segment alloc on wal",
	}, WALChannelLabelName, WALSegmentAllocStateLabelName)

	WALSegmentFlushedTotal = newWALCounterVec(prometheus.CounterOpts{
		Name: "segment_assign_flushed_segment_total",
		Help: "Total of segment sealed on wal",
	}, WALChannelLabelName, WALSegmentSealPolicyNameLabelName)

	WALSegmentBytes = newWALHistogramVec(prometheus.HistogramOpts{
		Name:    "segment_assign_segment_bytes",
		Help:    "Bytes of segment alloc on wal",
		Buckets: prometheus.ExponentialBucketsRange(5242880, 1073741824, 10), // 5MB -> 1024MB
	}, WALChannelLabelName)

	WALPartitionTotal = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "segment_assign_partition_total",
		Help: "Total of partition on wal",
	}, WALChannelLabelName)

	WALCollectionTotal = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "segment_assign_collection_total",
		Help: "Total of collection on wal",
	}, WALChannelLabelName)

	// Append Related Metrics
	WALAppendMessageBytes = newWALHistogramVec(prometheus.HistogramOpts{
		Name:    "append_message_bytes",
		Help:    "Bytes of append message to wal",
		Buckets: messageBytesBuckets,
	}, WALChannelLabelName, StatusLabelName)

	WALAppendMessageTotal = newWALCounterVec(prometheus.CounterOpts{
		Name: "append_message_total",
		Help: "Total of append message to wal",
	}, WALChannelLabelName, WALMessageTypeLabelName, StatusLabelName)

	WALAppendMessageDurationSeconds = newWALHistogramVec(prometheus.HistogramOpts{
		Name:    "append_message_duration_seconds",
		Help:    "Duration of wal append message",
		Buckets: secondsBuckets,
	}, WALChannelLabelName, StatusLabelName)

	WALImplsAppendMessageDurationSeconds = newWALHistogramVec(prometheus.HistogramOpts{
		Name:    "impls_append_message_duration_seconds",
		Help:    "Duration of wal impls append message",
		Buckets: secondsBuckets,
	}, WALChannelLabelName, StatusLabelName)

	// Scanner Related Metrics
	WALScannerTotal = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "scanner_total",
		Help: "Total of wal scanner on current streaming node",
	}, WALChannelLabelName)

	WALScanMessageBytes = newWALHistogramVec(prometheus.HistogramOpts{
		Name:    "scan_message_bytes",
		Help:    "Bytes of scanned message from wal",
		Buckets: messageBytesBuckets,
	}, WALChannelLabelName)

	WALScanMessageTotal = newWALCounterVec(prometheus.CounterOpts{
		Name: "scan_message_total",
		Help: "Total of scanned message from wal",
	}, WALChannelLabelName, WALMessageTypeLabelName)

	WALScanPassMessageBytes = newWALHistogramVec(prometheus.HistogramOpts{
		Name:    "scan_pass_message_bytes",
		Help:    "Bytes of pass (not filtered) scanned message from wal",
		Buckets: messageBytesBuckets,
	}, WALChannelLabelName)

	WALScanPassMessageTotal = newWALCounterVec(prometheus.CounterOpts{
		Name: "scan_pass_message_total",
		Help: "Total of pass (not filtered) scanned message from wal",
	}, WALChannelLabelName, WALMessageTypeLabelName)

	WALScanTimeTickViolationMessageTotal = newWALCounterVec(prometheus.CounterOpts{
		Name: "scan_time_tick_violation_message_total",
		Help: "Total of time tick violation message (dropped) from wal",
	}, WALChannelLabelName, WALMessageTypeLabelName)

	WALScanTxnTotal = newWALCounterVec(prometheus.CounterOpts{
		Name: "scan_txn_total",
		Help: "Total of scanned txn from wal",
	}, WALChannelLabelName, WALTxnStateLabelName)

	WALScannerPendingQueueBytes = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "scanner_pending_queue_bytes",
		Help: "Size of pending queue in wal scanner",
	}, WALChannelLabelName)

	WALScannerTimeTickBufBytes = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "scanner_time_tick_buf_bytes",
		Help: "Size of time tick buffer in wal scanner",
	}, WALChannelLabelName)

	WALScannerTxnBufBytes = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "scanner_txn_buf_bytes",
		Help: "Size of txn buffer in wal scanner",
	}, WALChannelLabelName)
)

// RegisterStreamingServiceClient registers streaming service client metrics
func RegisterStreamingServiceClient(registry *prometheus.Registry) {
	StreamingServiceClientRegisterOnce.Do(func() {
		registry.MustRegister(StreamingServiceClientProducerTotal)
		registry.MustRegister(StreamingServiceClientProduceInflightTotal)
		registry.MustRegister(StreamingServiceClientProduceBytes)
		registry.MustRegister(StreamingServiceClientProduceDurationSeconds)

		registry.MustRegister(StreamingServiceClientConsumerTotal)
		registry.MustRegister(StreamingServiceClientConsumeBytes)
		registry.MustRegister(StreamingServiceClientConsumeInflightTotal)
	})
}

// registerStreamingCoord registers streaming coord metrics
func registerStreamingCoord(registry *prometheus.Registry) {
	registry.MustRegister(StreamingCoordPChannelInfo)
	registry.MustRegister(StreamingCoordAssignmentVersion)
	registry.MustRegister(StreamingCoordAssignmentListenerTotal)
	registry.MustRegister(StreamingCoordBroadcasterTaskTotal)
	registry.MustRegister(StreamingCoordBroadcastDurationSeconds)
	registry.MustRegister(StreamingCoordBroadcasterAckAllDurationSeconds)
	registry.MustRegister(StreamingCoordResourceKeyTotal)
}

// RegisterStreamingNode registers streaming node metrics
func RegisterStreamingNode(registry *prometheus.Registry) {
	registry.MustRegister(StreamingNodeProducerTotal)
	registry.MustRegister(StreamingNodeProduceInflightTotal)

	registry.MustRegister(StreamingNodeConsumerTotal)
	registry.MustRegister(StreamingNodeConsumeInflightTotal)
	registry.MustRegister(StreamingNodeConsumeBytes)

	registerWAL(registry)
}

// registerWAL registers wal metrics
func registerWAL(registry *prometheus.Registry) {
	registry.MustRegister(WALInfo)
	registry.MustRegister(WALLastAllocatedTimeTick)
	registry.MustRegister(WALAllocateTimeTickTotal)
	registry.MustRegister(WALLastConfirmedTimeTick)
	registry.MustRegister(WALAcknowledgeTimeTickTotal)
	registry.MustRegister(WALSyncTimeTickTotal)
	registry.MustRegister(WALTimeTickSyncTotal)
	registry.MustRegister(WALTimeTickSyncTimeTick)
	registry.MustRegister(WALInflightTxn)
	registry.MustRegister(WALFinishTxn)
	registry.MustRegister(WALSegmentAllocTotal)
	registry.MustRegister(WALSegmentFlushedTotal)
	registry.MustRegister(WALSegmentBytes)
	registry.MustRegister(WALPartitionTotal)
	registry.MustRegister(WALCollectionTotal)
	registry.MustRegister(WALAppendMessageBytes)
	registry.MustRegister(WALAppendMessageTotal)
	registry.MustRegister(WALAppendMessageDurationSeconds)
	registry.MustRegister(WALImplsAppendMessageDurationSeconds)
	registry.MustRegister(WALScannerTotal)
	registry.MustRegister(WALScanMessageBytes)
	registry.MustRegister(WALScanMessageTotal)
	registry.MustRegister(WALScanPassMessageBytes)
	registry.MustRegister(WALScanPassMessageTotal)
	registry.MustRegister(WALScanTimeTickViolationMessageTotal)
	registry.MustRegister(WALScanTxnTotal)
	registry.MustRegister(WALScannerPendingQueueBytes)
	registry.MustRegister(WALScannerTimeTickBufBytes)
	registry.MustRegister(WALScannerTxnBufBytes)
}

func newStreamingCoordGaugeVec(opts prometheus.GaugeOpts, extra ...string) *prometheus.GaugeVec {
	opts.Namespace = milvusNamespace
	opts.Subsystem = typeutil.StreamingCoordRole
	labels := mergeLabel(extra...)
	return prometheus.NewGaugeVec(opts, labels)
}

func newStreamingCoordHistogramVec(opts prometheus.HistogramOpts, extra ...string) *prometheus.HistogramVec {
	opts.Namespace = milvusNamespace
	opts.Subsystem = typeutil.StreamingCoordRole
	labels := mergeLabel(extra...)
	return prometheus.NewHistogramVec(opts, labels)
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

func newWALGaugeVec(opts prometheus.GaugeOpts, extra ...string) *prometheus.GaugeVec {
	opts.Namespace = milvusNamespace
	opts.Subsystem = subsystemWAL
	labels := mergeLabel(extra...)
	return prometheus.NewGaugeVec(opts, labels)
}

func newWALCounterVec(opts prometheus.CounterOpts, extra ...string) *prometheus.CounterVec {
	opts.Namespace = milvusNamespace
	opts.Subsystem = subsystemWAL
	labels := mergeLabel(extra...)
	return prometheus.NewCounterVec(opts, labels)
}

func newWALHistogramVec(opts prometheus.HistogramOpts, extra ...string) *prometheus.HistogramVec {
	opts.Namespace = milvusNamespace
	opts.Subsystem = subsystemWAL
	labels := mergeLabel(extra...)
	return prometheus.NewHistogramVec(opts, labels)
}

func mergeLabel(extra ...string) []string {
	labels := make([]string, 0, 1+len(extra))
	labels = append(labels, NodeIDLabelName)
	labels = append(labels, extra...)
	return labels
}

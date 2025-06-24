package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	subsystemStreamingServiceClient         = "streaming"
	subsystemWAL                            = "wal"
	WALAccessModelRemote                    = "remote"
	WALAccessModelLocal                     = "local"
	WALScannerModelCatchup                  = "catchup"
	WALScannerModelTailing                  = "tailing"
	StreamingServiceClientStatusAvailable   = "available"
	StreamingServiceClientStatusUnavailable = "unavailable"
	WALStatusOK                             = "ok"
	WALStatusCancel                         = "cancel"
	WALStatusError                          = "error"

	BroadcasterTaskStateLabelName     = "state"
	ResourceKeyDomainLabelName        = "domain"
	WALAccessModelLabelName           = "access_model"
	WALScannerModelLabelName          = "scanner_model"
	TimeTickSyncTypeLabelName         = "type"
	TimeTickAckTypeLabelName          = "type"
	WALInterceptorLabelName           = "interceptor_name"
	WALTxnStateLabelName              = "state"
	WALFlusherStateLabelName          = "state"
	WALRecoveryStorageStateLabelName  = "state"
	WALStateLabelName                 = "state"
	WALChannelLabelName               = channelNameLabelName
	WALSegmentSealPolicyNameLabelName = "policy"
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
	StreamingServiceClientResumingProducerTotal = newStreamingServiceClientGaugeVec(prometheus.GaugeOpts{
		Name: "resuming_producer_total",
		Help: "Total of resuming producers",
	}, WALChannelLabelName, StatusLabelName)

	StreamingServiceClientProducerTotal = newStreamingServiceClientGaugeVec(prometheus.GaugeOpts{
		Name: "producer_total",
		Help: "Total of producers",
	}, WALChannelLabelName, WALAccessModelLabelName)

	StreamingServiceClientProduceTotal = newStreamingServiceClientCounterVec(prometheus.CounterOpts{
		Name: "produce_total",
		Help: "Total of produce message",
	}, WALChannelLabelName, WALAccessModelLabelName, StatusLabelName)

	StreamingServiceClientProduceBytes = newStreamingServiceClientCounterVec(prometheus.CounterOpts{
		Name: "produce_bytes",
		Help: "Total of produce message",
	}, WALChannelLabelName, WALAccessModelLabelName, StatusLabelName)

	StreamingServiceClientSuccessProduceBytes = newStreamingServiceClientHistogramVec(prometheus.HistogramOpts{
		Name:    "produce_success_bytes",
		Help:    "Bytes of produced message",
		Buckets: messageBytesBuckets,
	}, WALChannelLabelName, WALAccessModelLabelName)

	StreamingServiceClientSuccessProduceDurationSeconds = newStreamingServiceClientHistogramVec(prometheus.HistogramOpts{
		Name:    "produce_success_duration_seconds",
		Help:    "Duration of produced message",
		Buckets: secondsBuckets,
	}, WALChannelLabelName, WALAccessModelLabelName)

	// Streaming Service Client Consumer Metrics.
	StreamingServiceClientResumingConsumerTotal = newStreamingServiceClientGaugeVec(prometheus.GaugeOpts{
		Name: "resuming_consumer_total",
		Help: "Total of resuming consumers",
	}, WALChannelLabelName, StatusLabelName)

	StreamingServiceClientConsumerTotal = newStreamingServiceClientGaugeVec(prometheus.GaugeOpts{
		Name: "consumer_total",
		Help: "Total of consumers",
	}, WALChannelLabelName, WALAccessModelLabelName)

	StreamingServiceClientConsumeBytes = newStreamingServiceClientHistogramVec(prometheus.HistogramOpts{
		Name:    "consume_bytes",
		Help:    "Bytes of consumed message",
		Buckets: messageBytesBuckets,
	}, WALChannelLabelName)

	// StreamingCoord metrics
	StreamingCoordPChannelInfo = newStreamingCoordGaugeVec(prometheus.GaugeOpts{
		Name: "pchannel_info",
		Help: "Term of pchannels",
	}, WALChannelLabelName, WALChannelTermLabelName, StreamingNodeLabelName, WALStateLabelName)

	StreamingCoordVChannelTotal = newStreamingCoordGaugeVec(prometheus.GaugeOpts{
		Name: "vchannel_total",
		Help: "Total of vchannels",
	}, WALChannelLabelName, StreamingNodeLabelName)

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
	}, WALChannelLabelName, StatusLabelName)

	WALTimeTickAllocateDurationSeconds = newWALHistogramVec(prometheus.HistogramOpts{
		Name: "allocate_time_tick_duration_seconds",
		Help: "Duration of wal allocate time tick",
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

	WALTxnDurationSeconds = newWALHistogramVec(prometheus.HistogramOpts{
		Name:    "txn_duration_seconds",
		Help:    "Duration of wal txn",
		Buckets: secondsBuckets,
	}, WALChannelLabelName, WALTxnStateLabelName)

	// Rows level counter.
	WALInsertRowsTotal = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "insert_rows_total",
		Help: "Rows of growing insert on wal",
	}, WALChannelLabelName)

	WALInsertBytes = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "insert_bytes",
		Help: "Bytes of growing insert on wal",
	}, WALChannelLabelName)

	WALDeleteRowsTotal = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "delete_rows_total",
		Help: "Rows of growing delete on wal",
	}, WALChannelLabelName)

	// Segment related metrics
	WALGrowingSegmentRowsTotal = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "growing_segment_rows_total",
		Help: "Rows of segment growing on wal",
	}, WALChannelLabelName)

	WALGrowingSegmentBytes = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "growing_segment_bytes",
		Help: "Bytes of segment growing on wal",
	}, WALChannelLabelName)

	WALGrowingSegmentHWMBytes = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "growing_segment_hwm_bytes",
		Help: "HWM of segment growing bytes on node",
	})

	WALGrowingSegmentLWMBytes = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "growing_segment_lwm_bytes",
		Help: "LWM of segment growing bytes on node",
	})

	WALSegmentAllocTotal = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "segment_assign_segment_alloc_total",
		Help: "Total of segment alloc on wal",
	}, WALChannelLabelName)

	WALSegmentFlushedTotal = newWALCounterVec(prometheus.CounterOpts{
		Name: "segment_assign_flushed_segment_total",
		Help: "Total of segment sealed on wal",
	}, WALChannelLabelName, WALSegmentSealPolicyNameLabelName)

	WALSegmentRowsTotal = newWALHistogramVec(prometheus.HistogramOpts{
		Name:    "segment_assign_segment_rows_total",
		Help:    "Total rows of segment alloc on wal",
		Buckets: prometheus.ExponentialBucketsRange(128, 1048576, 10), // 5MB -> 1024MB
	}, WALChannelLabelName)

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

	WALAppendMessageBeforeInterceptorDurationSeconds = newWALHistogramVec(prometheus.HistogramOpts{
		Name:    "interceptor_before_append_duration_seconds",
		Help:    "Intercept duration before wal append message",
		Buckets: secondsBuckets,
	}, WALChannelLabelName, WALInterceptorLabelName)

	WALAppendMessageAfterInterceptorDurationSeconds = newWALHistogramVec(prometheus.HistogramOpts{
		Name:    "interceptor_after_append_duration_seconds",
		Help:    "Intercept duration after wal append message",
		Buckets: secondsBuckets,
	}, WALChannelLabelName, WALInterceptorLabelName)

	WALImplsAppendRetryTotal = newWALCounterVec(prometheus.CounterOpts{
		Name: "impls_append_message_retry_total",
		Help: "Total of append message retry",
	}, WALChannelLabelName)

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

	WALWriteAheadBufferEntryTotal = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "write_ahead_buffer_entry_total",
		Help: "Total of write ahead buffer entry in wal",
	}, WALChannelLabelName)

	WALWriteAheadBufferSizeBytes = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "write_ahead_buffer_size_bytes",
		Help: "Size of write ahead buffer in wal",
	}, WALChannelLabelName)

	WALWriteAheadBufferCapacityBytes = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "write_ahead_buffer_capacity_bytes",
		Help: "Capacity of write ahead buffer in wal",
	}, WALChannelLabelName)

	WALWriteAheadBufferEarliestTimeTick = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "write_ahead_buffer_earliest_time_tick",
		Help: "Earliest time tick of write ahead buffer in wal",
	}, WALChannelLabelName)

	WALWriteAheadBufferLatestTimeTick = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "write_ahead_buffer_latest_time_tick",
		Help: "Latest time tick of write ahead buffer in wal",
	}, WALChannelLabelName)

	// Scanner Related Metrics
	WALScannerTotal = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "scanner_total",
		Help: "Total of wal scanner on current streaming node",
	}, WALChannelLabelName, WALScannerModelLabelName)

	WALScanMessageBytes = newWALHistogramVec(prometheus.HistogramOpts{
		Name:    "scan_message_bytes",
		Help:    "Bytes of scanned message from wal",
		Buckets: messageBytesBuckets,
	}, WALChannelLabelName, WALScannerModelLabelName)

	WALScanMessageTotal = newWALCounterVec(prometheus.CounterOpts{
		Name: "scan_message_total",
		Help: "Total of scanned message from wal",
	}, WALChannelLabelName, WALMessageTypeLabelName, WALScannerModelLabelName)

	WALScanPassMessageBytes = newWALHistogramVec(prometheus.HistogramOpts{
		Name:    "scan_pass_message_bytes",
		Help:    "Bytes of pass (not filtered) scanned message from wal",
		Buckets: messageBytesBuckets,
	}, WALChannelLabelName, WALScannerModelLabelName)

	WALScanPassMessageTotal = newWALCounterVec(prometheus.CounterOpts{
		Name: "scan_pass_message_total",
		Help: "Total of pass (not filtered) scanned message from wal",
	}, WALChannelLabelName, WALMessageTypeLabelName, WALScannerModelLabelName)

	WALScanTimeTickViolationMessageTotal = newWALCounterVec(prometheus.CounterOpts{
		Name: "scan_time_tick_violation_message_total",
		Help: "Total of time tick violation message (dropped) from wal",
	}, WALChannelLabelName, WALMessageTypeLabelName, WALScannerModelLabelName)

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

	WALFlusherInfo = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "flusher_info",
		Help: "Current info of flusher on current wal",
	}, WALChannelLabelName, WALChannelTermLabelName, WALFlusherStateLabelName)

	WALFlusherTimeTick = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "flusher_time_tick",
		Help: "the final timetick tick of flusher seen",
	}, WALChannelLabelName, WALChannelTermLabelName)

	WALRecoveryInfo = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "recovery_info",
		Help: "Current info of recovery storage on current wal",
	}, WALChannelLabelName, WALChannelTermLabelName, WALRecoveryStorageStateLabelName)

	WALRecoveryInMemTimeTick = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "recovery_in_mem_time_tick",
		Help: "the final timetick tick of recovery storage seen",
	}, WALChannelLabelName, WALChannelTermLabelName)

	WALRecoveryPersistedTimeTick = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "recovery_persisted_time_tick",
		Help: "the final persisted timetick tick of recovery storage seen",
	}, WALChannelLabelName, WALChannelTermLabelName)

	WALRecoveryInconsistentEventTotal = newWALCounterVec(prometheus.CounterOpts{
		Name: "recovery_inconsistent_event_total",
		Help: "Total of recovery inconsistent event",
	}, WALChannelLabelName, WALChannelTermLabelName)

	WALRecoveryIsOnPersisting = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "recovery_is_on_persisting",
		Help: "Is recovery storage on persisting",
	}, WALChannelLabelName, WALChannelTermLabelName)

	WALTruncateTimeTick = newWALGaugeVec(prometheus.GaugeOpts{
		Name: "truncate_time_tick",
		Help: "the final timetick tick of truncator seen",
	}, WALChannelLabelName, WALChannelTermLabelName)
)

// RegisterStreamingServiceClient registers streaming service client metrics
func RegisterStreamingServiceClient(registry *prometheus.Registry) {
	StreamingServiceClientRegisterOnce.Do(func() {
		registry.MustRegister(StreamingServiceClientResumingProducerTotal)
		registry.MustRegister(StreamingServiceClientProducerTotal)
		registry.MustRegister(StreamingServiceClientProduceTotal)
		registry.MustRegister(StreamingServiceClientProduceBytes)
		registry.MustRegister(StreamingServiceClientSuccessProduceBytes)
		registry.MustRegister(StreamingServiceClientSuccessProduceDurationSeconds)
		registry.MustRegister(StreamingServiceClientResumingConsumerTotal)
		registry.MustRegister(StreamingServiceClientConsumerTotal)
		registry.MustRegister(StreamingServiceClientConsumeBytes)
	})
}

// registerStreamingCoord registers streaming coord metrics
func registerStreamingCoord(registry *prometheus.Registry) {
	registry.MustRegister(StreamingCoordPChannelInfo)
	registry.MustRegister(StreamingCoordVChannelTotal)
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

	// TODO: after remove the implementation of old data node
	// Such as flowgraph and writebuffer, we can remove these metrics from streaming node.
	RegisterDataNode(registry)
}

// registerWAL registers wal metrics
func registerWAL(registry *prometheus.Registry) {
	registry.MustRegister(WALInfo)
	registry.MustRegister(WALLastAllocatedTimeTick)
	registry.MustRegister(WALAllocateTimeTickTotal)
	registry.MustRegister(WALTimeTickAllocateDurationSeconds)
	registry.MustRegister(WALLastConfirmedTimeTick)
	registry.MustRegister(WALAcknowledgeTimeTickTotal)
	registry.MustRegister(WALSyncTimeTickTotal)
	registry.MustRegister(WALTimeTickSyncTotal)
	registry.MustRegister(WALTimeTickSyncTimeTick)
	registry.MustRegister(WALInflightTxn)
	registry.MustRegister(WALTxnDurationSeconds)
	registry.MustRegister(WALInsertRowsTotal)
	registry.MustRegister(WALInsertBytes)
	registry.MustRegister(WALDeleteRowsTotal)
	registry.MustRegister(WALGrowingSegmentBytes)
	registry.MustRegister(WALGrowingSegmentRowsTotal)
	registry.MustRegister(WALGrowingSegmentHWMBytes)
	registry.MustRegister(WALGrowingSegmentLWMBytes)
	registry.MustRegister(WALSegmentAllocTotal)
	registry.MustRegister(WALSegmentFlushedTotal)
	registry.MustRegister(WALSegmentRowsTotal)
	registry.MustRegister(WALSegmentBytes)
	registry.MustRegister(WALPartitionTotal)
	registry.MustRegister(WALCollectionTotal)
	registry.MustRegister(WALAppendMessageBytes)
	registry.MustRegister(WALAppendMessageTotal)
	registry.MustRegister(WALAppendMessageBeforeInterceptorDurationSeconds)
	registry.MustRegister(WALAppendMessageAfterInterceptorDurationSeconds)
	registry.MustRegister(WALImplsAppendRetryTotal)
	registry.MustRegister(WALAppendMessageDurationSeconds)
	registry.MustRegister(WALImplsAppendMessageDurationSeconds)
	registry.MustRegister(WALWriteAheadBufferEntryTotal)
	registry.MustRegister(WALWriteAheadBufferSizeBytes)
	registry.MustRegister(WALWriteAheadBufferCapacityBytes)
	registry.MustRegister(WALWriteAheadBufferEarliestTimeTick)
	registry.MustRegister(WALWriteAheadBufferLatestTimeTick)
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
	registry.MustRegister(WALFlusherInfo)
	registry.MustRegister(WALFlusherTimeTick)
	registry.MustRegister(WALRecoveryInfo)
	registry.MustRegister(WALRecoveryInMemTimeTick)
	registry.MustRegister(WALRecoveryPersistedTimeTick)
	registry.MustRegister(WALRecoveryInconsistentEventTotal)
	registry.MustRegister(WALRecoveryIsOnPersisting)
	registry.MustRegister(WALTruncateTimeTick)
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

func newStreamingServiceClientCounterVec(opts prometheus.CounterOpts, extra ...string) *prometheus.CounterVec {
	opts.Namespace = milvusNamespace
	opts.Subsystem = subsystemStreamingServiceClient
	labels := mergeLabel(extra...)
	return prometheus.NewCounterVec(opts, labels)
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

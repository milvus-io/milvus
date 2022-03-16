package metrics

import (
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// RootCoordProxyLister counts the num of registered proxy nodes
	RootCoordProxyLister = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "list_of_proxy",
			Help:      "List of proxy nodes which have registered with etcd",
		}, []string{nodeIDLabelName})

	////////////////////////////////////////////////////////////////////////////
	// for grpc

	// RootCoordCreateCollectionCounter counts the num of calls of CreateCollection
	RootCoordCreateCollectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "create_collection_total",
			Help:      "Counter of create collection",
		}, []string{statusLabelName})

	// RootCoordDropCollectionCounter counts the num of calls of DropCollection
	RootCoordDropCollectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "drop_collection_total",
			Help:      "Counter of drop collection",
		}, []string{statusLabelName})

	// RootCoordHasCollectionCounter counts the num of calls of HasCollection
	RootCoordHasCollectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "has_collection_total",
			Help:      "Counter of has collection",
		}, []string{statusLabelName})

	// RootCoordDescribeCollectionCounter counts the num of calls of DescribeCollection
	RootCoordDescribeCollectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "describe_collection_total",
			Help:      "Counter of describe collection",
		}, []string{statusLabelName})

	// RootCoordShowCollectionsCounter counts the num of calls of ShowCollections
	RootCoordShowCollectionsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "show_collections_total",
			Help:      "Counter of show collections",
		}, []string{statusLabelName})

	// RootCoordCreatePartitionCounter counts the num of calls of CreatePartition
	RootCoordCreatePartitionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "create_partition_total",
			Help:      "Counter of create partition",
		}, []string{statusLabelName})

	// RootCoordDropPartitionCounter counts the num of calls of DropPartition
	RootCoordDropPartitionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "drop_partition_total",
			Help:      "Counter of drop partition",
		}, []string{statusLabelName})

	// RootCoordHasPartitionCounter counts the num of calls of HasPartition
	RootCoordHasPartitionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "has_partition_total",
			Help:      "Counter of has partition",
		}, []string{statusLabelName})

	// RootCoordShowPartitionsCounter counts the num of calls of ShowPartitions
	RootCoordShowPartitionsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "show_partitions_total",
			Help:      "Counter of show partitions",
		}, []string{statusLabelName})

	// RootCoordCreateIndexCounter counts the num of calls of CreateIndex
	RootCoordCreateIndexCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "create_index_total",
			Help:      "Counter of create index",
		}, []string{statusLabelName})

	// RootCoordDropIndexCounter counts the num of calls of DropIndex
	RootCoordDropIndexCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "drop_index_total",
			Help:      "Counter of drop index",
		}, []string{statusLabelName})

	// RootCoordDescribeIndexCounter counts the num of calls of DescribeIndex
	RootCoordDescribeIndexCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "describe_index_total",
			Help:      "Counter of describe index",
		}, []string{statusLabelName})

	// RootCoordDescribeSegmentCounter counts the num of calls of DescribeSegment
	RootCoordDescribeSegmentCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "describe_segment_total",
			Help:      "Counter of describe segment",
		}, []string{statusLabelName})

	// RootCoordShowSegmentsCounter counts the num of calls of ShowSegments
	RootCoordShowSegmentsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "show_segments_total",
			Help:      "Counter of show segments",
		}, []string{statusLabelName})

	////////////////////////////////////////////////////////////////////////////
	// for time tick

	// RootCoordInsertChannelTimeTick counts the time tick num of insert channel in 24H
	RootCoordInsertChannelTimeTick = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "insert_channel_time_tick",
			Help:      "Time tick of insert Channel in 24H",
		}, []string{"PChannel"})

	// RootCoordDDLReadTypeLatency records the latency for read type of DDL operations.
	RootCoordDDLReadTypeLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "ddl_read_type_latency",
			Help:      "The latency for read type of DDL operations",
		}, []string{functionLabelName})

	// RootCoordDDLWriteTypeLatency records the latency for write type of DDL operations.
	RootCoordDDLWriteTypeLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "ddl_write_type_latency",
			Help:      "The latency for write type of DDL operations",
		}, []string{functionLabelName})

	// RootCoordSyncTimeTickLatency records the latency of sync time tick.
	RootCoordSyncTimeTickLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "sync_time_tick_latency",
			Help:      "The latency of sync time tick",
		})

	// RootCoordIDAllocCounter records the number of global ID allocations.
	RootCoordIDAllocCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "id_alloc_count",
			Help:      "The number of global ID allocations",
		})

	// RootCoordLocalTimestampAllocCounter records the number of timestamp allocations in RootCoord.
	RootCoordTimestampAllocCounter = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "timestamp_alloc_count",
			Help:      "The number of timestamp allocations in RootCoord",
		})

	// RootCoordETCDTimestampAllocCounter records the number of timestamp allocations in ETCD.
	RootCoordETCDTimestampAllocCounter = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "etcd_timestamp_alloc_count",
			Help:      "The number of timestamp allocations in ETCD",
		})

	// RootCoordNumOfCollections counts the number of collections.
	RootCoordNumOfCollections = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "num_of_collections",
			Help:      "The number of collections",
		})

	// RootCoordNumOfPartitions counts the number of partitions per collection.
	RootCoordNumOfPartitions = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "num_of_partitions",
			Help:      "The number of partitions per collection",
		}, []string{collectionIDLabelName})

	// RootCoordNumOfSegments counts the number of segments per collections.
	RootCoordNumOfSegments = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "num_of_segments",
			Help:      "The number of segments per collection",
		}, []string{collectionIDLabelName})

	// RootCoordNumOfIndexedSegments counts the number of indexed segments per collection.
	RootCoordNumOfIndexedSegments = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "num_of_indexed_segments",
			Help:      "The number of indexed segments per collection",
		}, []string{collectionIDLabelName})

	// RootCoordNumOfDMLChannel counts the number of DML channels.
	RootCoordNumOfDMLChannel = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "num_of_dml_channel",
			Help:      "The number of DML channels",
		})

	// RootCoordNumOfMsgStream counts the number of message streams.
	RootCoordNumOfMsgStream = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "num_of_msg_stream",
			Help:      "The number of message streams",
		})
)

//RegisterRootCoord registers RootCoord metrics
func RegisterRootCoord() {
	prometheus.MustRegister(RootCoordProxyLister)

	// for grpc
	prometheus.MustRegister(RootCoordCreateCollectionCounter)
	prometheus.MustRegister(RootCoordDropCollectionCounter)
	prometheus.MustRegister(RootCoordHasCollectionCounter)
	prometheus.MustRegister(RootCoordDescribeCollectionCounter)
	prometheus.MustRegister(RootCoordShowCollectionsCounter)
	prometheus.MustRegister(RootCoordCreatePartitionCounter)
	prometheus.MustRegister(RootCoordDropPartitionCounter)
	prometheus.MustRegister(RootCoordHasPartitionCounter)
	prometheus.MustRegister(RootCoordShowPartitionsCounter)
	prometheus.MustRegister(RootCoordCreateIndexCounter)
	prometheus.MustRegister(RootCoordDropIndexCounter)
	prometheus.MustRegister(RootCoordDescribeIndexCounter)
	prometheus.MustRegister(RootCoordDescribeSegmentCounter)
	prometheus.MustRegister(RootCoordShowSegmentsCounter)

	// for time tick
	prometheus.MustRegister(RootCoordInsertChannelTimeTick)
	//prometheus.MustRegister(PanicCounter)
	prometheus.MustRegister(RootCoordSyncTimeTickLatency)

	// for DDL latency
	prometheus.MustRegister(RootCoordDDLReadTypeLatency)
	prometheus.MustRegister(RootCoordDDLWriteTypeLatency)

	// for allocator
	prometheus.MustRegister(RootCoordIDAllocCounter)
	prometheus.MustRegister(RootCoordTimestampAllocCounter)
	prometheus.MustRegister(RootCoordETCDTimestampAllocCounter)

	// for collection
	prometheus.MustRegister(RootCoordNumOfCollections)
	prometheus.MustRegister(RootCoordNumOfPartitions)
	prometheus.MustRegister(RootCoordNumOfSegments)
	prometheus.MustRegister(RootCoordNumOfIndexedSegments)

	prometheus.MustRegister(RootCoordNumOfDMLChannel)
	prometheus.MustRegister(RootCoordNumOfMsgStream)
}

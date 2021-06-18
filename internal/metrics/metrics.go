package metrics

import (
	"net/http"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"go.uber.org/zap"
)

const (
	milvusNamespace    = "milvus"
	subSystemRootCoord = "rootcoord"
	subSystemDataCoord = "dataCoord"
)

/*
var (
	PanicCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milvus",
			Subsystem: "server",
			Name:      "panic_total",
			Help:      "Counter of panic.",
		}, []string{"type"})
)
*/

var (
	// RootCoordProxyNodeLister used to count the num of registered proxy nodes
	RootCoordProxyNodeLister = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "list_of_proxy_node",
			Help:      "List of proxy nodes which has register with etcd",
		}, []string{"client_id"})

	////////////////////////////////////////////////////////////////////////////
	// for grpc

	// RootCoordCreateCollectionCounter used to count the num of calls of CreateCollection
	RootCoordCreateCollectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "create_collection_total",
			Help:      "Counter of create collection",
		}, []string{"client_id", "type"})

	// RootCoordDropCollectionCounter used to count the num of calls of DropCollection
	RootCoordDropCollectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "drop_collection_total",
			Help:      "Counter of drop collection",
		}, []string{"client_id", "type"})

	// RootCoordHasCollectionCounter used to count the num of calls of HasCollection
	RootCoordHasCollectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "has_collection_total",
			Help:      "Counter of has collection",
		}, []string{"client_id", "type"})

	// RootCoordDescribeCollectionCounter used to count the num of calls of DescribeCollection
	RootCoordDescribeCollectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "describe_collection_total",
			Help:      "Counter of describe collection",
		}, []string{"client_id", "type"})

	// RootCoordShowCollectionsCounter used to count the num of calls of ShowCollections
	RootCoordShowCollectionsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "show_collections_total",
			Help:      "Counter of show collections",
		}, []string{"client_id", "type"})

	// RootCoordCreatePartitionCounter used to count the num of calls of CreatePartition
	RootCoordCreatePartitionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "create_partition_total",
			Help:      "Counter of create partition",
		}, []string{"client_id", "type"})

	// RootCoordDropPartitionCounter used to count the num of calls of DropPartition
	RootCoordDropPartitionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "drop_partition_total",
			Help:      "Counter of drop partition",
		}, []string{"client_id", "type"})

	// RootCoordHasPartitionCounter used to count the num of calls of HasPartition
	RootCoordHasPartitionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "has_partition_total",
			Help:      "Counter of has partition",
		}, []string{"client_id", "type"})

	// RootCoordShowPartitionsCounter used to count the num of calls of ShowPartitions
	RootCoordShowPartitionsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "show_partitions_total",
			Help:      "Counter of show partitions",
		}, []string{"client_id", "type"})

	// RootCoordCreateIndexCounter used to count the num of calls of CreateIndex
	RootCoordCreateIndexCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "create_index_total",
			Help:      "Counter of create index",
		}, []string{"client_id", "type"})

	// RootCoordDropIndexCounter used to count the num of calls of DropIndex
	RootCoordDropIndexCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "drop_index_total",
			Help:      "Counter of drop index",
		}, []string{"client_id", "type"})

	// RootCoordDescribeIndexCounter used to count the num of calls of DescribeIndex
	RootCoordDescribeIndexCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "describe_index_total",
			Help:      "Counter of describe index",
		}, []string{"client_id", "type"})

	// RootCoordDescribeSegmentCounter used to count the num of calls of DescribeSegment
	RootCoordDescribeSegmentCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "describe_segment_total",
			Help:      "Counter of describe segment",
		}, []string{"client_id", "type"})

	// RootCoordShowSegmentsCounter used to count the num of calls of ShowSegments
	RootCoordShowSegmentsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "show_segments_total",
			Help:      "Counter of show segments",
		}, []string{"client_id", "type"})

	////////////////////////////////////////////////////////////////////////////
	// for time tick

	// RootCoordInsertChannelTimeTick used to count the time tick num of insert channel in 24H
	RootCoordInsertChannelTimeTick = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "insert_channel_time_tick",
			Help:      "Time tick of insert Channel in 24H",
		}, []string{"vchannel"})

	// RootCoordDDChannelTimeTick used to count the time tick num of dd channel in 24H
	RootCoordDDChannelTimeTick = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "dd_channel_time_tick",
			Help:      "Time tick of dd Channel in 24H",
		})
)

//RegisterRootCoord register RootCoord metrics
func RegisterRootCoord() {
	prometheus.MustRegister(RootCoordProxyNodeLister)

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
	prometheus.MustRegister(RootCoordDDChannelTimeTick)
	//prometheus.MustRegister(PanicCounter)
}

//RegisterProxyNode register ProxyNode metrics
func RegisterProxyNode() {

}

//RegisterQueryCoord register QueryCoord metrics
func RegisterQueryCoord() {

}

//RegisterQueryNode register QueryNode metrics
func RegisterQueryNode() {

}

var (
	//DataCoordDataNodeList records the num of regsitered data nodes
	DataCoordDataNodeList = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemDataCoord,
			Name:      "list_of_data_node",
			Help:      "List of data nodes regsitered within etcd",
		}, []string{"status"},
	)
)

//RegisterDataCoord register DataService metrics
func RegisterDataCoord() {
	prometheus.Register(DataCoordDataNodeList)
}

//RegisterDataNode register DataNode metrics
func RegisterDataNode() {

}

//RegisterIndexCoord register IndexCoord metrics
func RegisterIndexCoord() {

}

//RegisterIndexNode register IndexNode metrics
func RegisterIndexNode() {

}

//RegisterMsgStreamCoord register MsgStreamCoord metrics
func RegisterMsgStreamCoord() {

}

//ServeHTTP serve prometheus http service
func ServeHTTP() {
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(":9091", nil); err != nil {
			log.Error("handle metrics failed", zap.Error(err))
		}
	}()
}

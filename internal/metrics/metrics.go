package metrics

import (
	"net/http"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"go.uber.org/zap"
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
	MasterProxyNodeLister = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "milvus",
			Subsystem: "master",
			Name:      "list_of_proxy_node",
			Help:      "List of proxy nodes which has register with etcd",
		}, []string{"client_id"})

	// for grpc
	MasterCreateCollectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milvus",
			Subsystem: "master",
			Name:      "create_collection_total",
			Help:      "Counter of create collection",
		}, []string{"client_id", "type"})
	MasterDropCollectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milvus",
			Subsystem: "master",
			Name:      "drop_collection_total",
			Help:      "Counter of drop collection",
		}, []string{"client_id", "type"})
	MasterHasCollectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milvus",
			Subsystem: "master",
			Name:      "has_collection_total",
			Help:      "Counter of has collection",
		}, []string{"client_id", "type"})
	MasterDescribeCollectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milvus",
			Subsystem: "master",
			Name:      "describe_collection_total",
			Help:      "Counter of describe collection",
		}, []string{"client_id", "type"})
	MasterShowCollectionsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milvus",
			Subsystem: "master",
			Name:      "show_collections_total",
			Help:      "Counter of show collections",
		}, []string{"client_id", "type"})
	MasterCreatePartitionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milvus",
			Subsystem: "master",
			Name:      "create_partition_total",
			Help:      "Counter of create partition",
		}, []string{"client_id", "type"})
	MasterDropPartitionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milvus",
			Subsystem: "master",
			Name:      "drop_partition_total",
			Help:      "Counter of drop partition",
		}, []string{"client_id", "type"})
	MasterHasPartitionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milvus",
			Subsystem: "master",
			Name:      "has_partition_total",
			Help:      "Counter of has partition",
		}, []string{"client_id", "type"})
	MasterShowPartitionsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milvus",
			Subsystem: "master",
			Name:      "show_partitions_total",
			Help:      "Counter of show partitions",
		}, []string{"client_id", "type"})
	MasterCreateIndexCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milvus",
			Subsystem: "master",
			Name:      "create_index_total",
			Help:      "Counter of create index",
		}, []string{"client_id", "type"})
	MasterDropIndexCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milvus",
			Subsystem: "master",
			Name:      "drop_index_total",
			Help:      "Counter of drop index",
		}, []string{"client_id", "type"})
	MasterDescribeIndexCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milvus",
			Subsystem: "master",
			Name:      "describe_index_total",
			Help:      "Counter of describe index",
		}, []string{"client_id", "type"})
	MasterDescribeSegmentCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milvus",
			Subsystem: "master",
			Name:      "describe_segment_total",
			Help:      "Counter of describe segment",
		}, []string{"client_id", "type"})
	MasterShowSegmentsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milvus",
			Subsystem: "master",
			Name:      "show_segments_total",
			Help:      "Counter of show segments",
		}, []string{"client_id", "type"})

	// for time tick
	MasterInsertChannelTimeTick = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "milvus",
			Subsystem: "master",
			Name:      "insert_channel_time_tick",
			Help:      "Time tick of insert Channel in 24H",
		}, []string{"vchannel"})
	MasterDDChannelTimeTick = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "milvus",
			Subsystem: "master",
			Name:      "dd_channel_time_tick",
			Help:      "Time tick of dd Channel in 24H",
		})
)

//RegisterMaster register Master metrics
func RegisterMaster() {
	prometheus.MustRegister(MasterProxyNodeLister)

	// for grpc
	prometheus.MustRegister(MasterCreateCollectionCounter)
	prometheus.MustRegister(MasterDropCollectionCounter)
	prometheus.MustRegister(MasterHasCollectionCounter)
	prometheus.MustRegister(MasterDescribeCollectionCounter)
	prometheus.MustRegister(MasterShowCollectionsCounter)
	prometheus.MustRegister(MasterCreatePartitionCounter)
	prometheus.MustRegister(MasterDropPartitionCounter)
	prometheus.MustRegister(MasterHasPartitionCounter)
	prometheus.MustRegister(MasterShowPartitionsCounter)
	prometheus.MustRegister(MasterCreateIndexCounter)
	prometheus.MustRegister(MasterDropIndexCounter)
	prometheus.MustRegister(MasterDescribeIndexCounter)
	prometheus.MustRegister(MasterDescribeSegmentCounter)
	prometheus.MustRegister(MasterShowSegmentsCounter)

	// for time tick
	prometheus.MustRegister(MasterInsertChannelTimeTick)
	prometheus.MustRegister(MasterDDChannelTimeTick)
	//prometheus.MustRegister(PanicCounter)
}

//RegisterProxyService register ProxyService metrics
func RegisterProxyService() {

}

//RegisterProxyNode register ProxyNode metrics
func RegisterProxyNode() {

}

//RegisterQueryService register QueryService metrics
func RegisterQueryService() {

}

//RegisterQueryNode register QueryNode metrics
func RegisterQueryNode() {

}

//RegisterDataService register DataService metrics
func RegisterDataService() {

}

//RegisterDataNode register DataNode metrics
func RegisterDataNode() {

}

//RegisterIndexService register IndexService metrics
func RegisterIndexService() {

}

//RegisterIndexNode register IndexNode metrics
func RegisterIndexNode() {

}

//RegisterMsgStreamService register MsgStreamService metrics
func RegisterMsgStreamService() {

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

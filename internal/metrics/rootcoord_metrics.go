package metrics

import (
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// RootCoordProxyNum records the num of registered proxy nodes
	RootCoordProxyNum = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "proxy_num",
			Help:      "number of proxy nodes managered by rootcoord",
		}, []string{})

	////////////////////////////////////////////////////////////////////////////

	////////////////////////////////////////////////////////////////////////////
	// for time tick

	// RootCoordInsertChannelTimeTick counts the time tick num of insert channel in 24H
	RootCoordInsertChannelTimeTick = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "sync_epoch_time",
			Help:      "synchronized unix epoch per physical channel",
		}, []string{channelNameLabelName})

	RootCoordDDLReqCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "ddl_req_count",
			Help:      "count of DDL operations",
		}, []string{functionLabelName, statusLabelName})

	//RootCoordDDLReqLatency records the latency for read type of DDL operations.
	RootCoordDDLReqLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "ddl_req_latency",
			Help:      "latency of each DDL operations",
		}, []string{functionLabelName})

	// RootCoordSyncTimeTickLatency records the latency of sync time tick.
	RootCoordSyncTimeTickLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "sync_timetick_latency",
			Help:      "latency of synchronizing timetick message",
		})

	// RootCoordIDAllocCounter records the number of global ID allocations.
	RootCoordIDAllocCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "id_alloc_count",
			Help:      "count of ID allocated",
		})

	//RootCoordTimestamp records the number of timestamp allocations in RootCoord.
	RootCoordTimestamp = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "timestamp",
			Help:      "lateste timestamp allocated in memory",
		})

	// RootCoordTimestampSaved records the number of timestamp allocations in ETCD.
	RootCoordTimestampSaved = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "timestamp_saved",
			Help:      "timestamp saved in meta storage",
		})

	// RootCoordNumOfCollections counts the number of collections.
	RootCoordNumOfCollections = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "collection_num",
			Help:      "number of collections",
		})

	// RootCoordNumOfPartitions counts the number of partitions per collection.
	RootCoordNumOfPartitions = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "partition_num",
			Help:      "number of partitions",
		}, []string{})

	// RootCoordNumOfDMLChannel counts the number of DML channels.
	RootCoordNumOfDMLChannel = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "dml_channel_num",
			Help:      "number of DML channels",
		})

	// RootCoordNumOfMsgStream counts the number of message streams.
	RootCoordNumOfMsgStream = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "msgstream_obj_num",
			Help:      "number of message streams",
		})

	// RootCoordNumOfCredentials counts the number of credentials.
	RootCoordNumOfCredentials = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "credential_num",
			Help:      "number of credentials",
		})
)

//RegisterRootCoord registers RootCoord metrics
func RegisterRootCoord(registry *prometheus.Registry) {
	registry.Register(RootCoordProxyNum)

	// for time tick
	registry.MustRegister(RootCoordInsertChannelTimeTick)
	registry.MustRegister(RootCoordSyncTimeTickLatency)

	// for DDL
	registry.MustRegister(RootCoordDDLReqCounter)
	registry.MustRegister(RootCoordDDLReqLatency)

	// for allocator
	registry.MustRegister(RootCoordIDAllocCounter)
	registry.MustRegister(RootCoordTimestamp)
	registry.MustRegister(RootCoordTimestampSaved)

	// for collection
	registry.MustRegister(RootCoordNumOfCollections)
	registry.MustRegister(RootCoordNumOfPartitions)

	registry.MustRegister(RootCoordNumOfDMLChannel)
	registry.MustRegister(RootCoordNumOfMsgStream)

	// for credential
	registry.MustRegister(RootCoordNumOfCredentials)
}

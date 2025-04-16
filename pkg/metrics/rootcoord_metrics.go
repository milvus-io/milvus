package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var (
	// RootCoordProxyCounter counts the num of registered proxy nodes
	RootCoordProxyCounter = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "proxy_num",
			Help:      "number of proxy nodes managered by rootcoord",
		}, []string{})

	////////////////////////////////////////////////////////////////////////////
	// for time tick

	// RootCoordInsertChannelTimeTick counts the time tick num of insert channel in 24H
	RootCoordInsertChannelTimeTick = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "produce_tt_lag_ms",
			Help:      "now time minus tt per physical channel",
		}, []string{channelNameLabelName})

	RootCoordDDLReqCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "ddl_req_count",
			Help:      "count of DDL operations",
		}, []string{functionLabelName, statusLabelName})

	// RootCoordDDLReqLatency records the latency for read type of DDL operations.
	RootCoordDDLReqLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "ddl_req_latency",
			Help:      "latency of each DDL operations",
			Buckets:   buckets,
		}, []string{functionLabelName})

	// RootCoordSyncTimeTickLatency records the latency of sync time tick.
	RootCoordSyncTimeTickLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "sync_timetick_latency",
			Help:      "latency of synchronizing timetick message",
			Buckets:   buckets,
		})

	// RootCoordIDAllocCounter records the number of global ID allocations.
	RootCoordIDAllocCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "id_alloc_count",
			Help:      "count of ID allocated",
		})

	// RootCoordTimestamp records the number of timestamp allocations in RootCoord.
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

	// RootCoordNumOfDatabases counts the number of database.
	RootCoordNumOfDatabases = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "database_num",
			Help:      "number of database",
		})

	// RootCoordNumOfCollections counts the number of collections.
	RootCoordNumOfCollections = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "collection_num",
			Help:      "number of collections",
		}, []string{databaseLabelName})

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

	// RootCoordNumOfRoles counts the number of credentials.
	RootCoordNumOfRoles = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "num_of_roles",
			Help:      "The number of roles",
		})

	// RootCoordNumOfPrivilegeGroups counts the number of credentials.
	RootCoordNumOfPrivilegeGroups = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "num_of_privilege_groups",
			Help:      "The number of privilege groups",
		})

	// RootCoordTtDelay records the max time tick delay of flow graphs in DataNodes and QueryNodes.
	RootCoordTtDelay = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "time_tick_delay",
			Help:      "The max time tick delay of flow graphs",
		}, []string{
			roleNameLabelName,
			nodeIDLabelName,
		})

	// RootCoordQuotaStates records the quota states of cluster.
	RootCoordQuotaStates = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "quota_states",
			Help:      "The quota states of cluster",
		}, []string{
			"quota_states",
			"name",
		})

	// RootCoordForceDenyWritingCounter records the number of times that milvus turns into force-deny-writing states.
	RootCoordForceDenyWritingCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "force_deny_writing_counter",
			Help:      "The number of times milvus turns into force-deny-writing states",
		})

	// RootCoordRateLimitRatio reflects the ratio of rate limit.
	RootCoordRateLimitRatio = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "rate_limit_ratio",
			Help:      "",
		}, []string{collectionIDLabelName})

	RootCoordDDLReqLatencyInQueue = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "ddl_req_latency_in_queue",
			Help:      "latency of each DDL operations in queue",
		}, []string{functionLabelName})

	RootCoordNumEntities = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "entity_num",
			Help:      "number of entities, clustered by collection and their status(loaded/total)",
		}, []string{
			collectionName,
			statusLabelName,
		})

	RootCoordIndexedNumEntities = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "indexed_entity_num",
			Help:      "indexed number of entities, clustered by collection, index name and whether it's a vector index",
		}, []string{
			collectionName,
			indexName,
			isVectorIndex,
		})

	QueryNodeMemoryHighWaterLevel = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "qn_mem_high_water_level",
			Help:      "querynode memory high water level",
		})

	DiskQuota = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "disk_quota",
			Help:      "disk quota",
		}, []string{"node_id", "scope"})
)

// RegisterRootCoord registers RootCoord metrics
func RegisterMixCoord(registry *prometheus.Registry) {
	registry.Register(RootCoordProxyCounter)

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

	registry.MustRegister(RootCoordNumOfRoles)
	registry.MustRegister(RootCoordTtDelay)
	registry.MustRegister(RootCoordQuotaStates)
	registry.MustRegister(RootCoordForceDenyWritingCounter)
	registry.MustRegister(RootCoordRateLimitRatio)
	registry.MustRegister(RootCoordDDLReqLatencyInQueue)

	registry.MustRegister(RootCoordNumEntities)
	registry.MustRegister(RootCoordIndexedNumEntities)

	registry.MustRegister(QueryNodeMemoryHighWaterLevel)
	registry.MustRegister(DiskQuota)

	RegisterStreamingServiceClient(registry)
	RegisterQueryCoord(registry)
	RegisterDataCoord(registry)
}

func CleanupRootCoordDBMetrics(dbName string) {
	RootCoordNumOfCollections.Delete(prometheus.Labels{
		databaseLabelName: dbName,
	})
}

// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"net/http"

	// nolint:gosec
	_ "net/http/pprof"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

const (
	milvusNamespace = "milvus"
)

var (
	// RootCoordProxyLister counts the num of registered proxy nodes
	RootCoordProxyLister = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "list_of_proxy",
			Help:      "List of proxy nodes which have registered with etcd",
		}, []string{"client_id"})

	////////////////////////////////////////////////////////////////////////////
	// for grpc

	// RootCoordCreateCollectionCounter counts the num of calls of CreateCollection
	RootCoordCreateCollectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "create_collection_total",
			Help:      "Counter of create collection",
		}, []string{"client_id", "type"})

	// RootCoordDropCollectionCounter counts the num of calls of DropCollection
	RootCoordDropCollectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "drop_collection_total",
			Help:      "Counter of drop collection",
		}, []string{"client_id", "type"})

	// RootCoordHasCollectionCounter counts the num of calls of HasCollection
	RootCoordHasCollectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "has_collection_total",
			Help:      "Counter of has collection",
		}, []string{"client_id", "type"})

	// RootCoordDescribeCollectionCounter counts the num of calls of DescribeCollection
	RootCoordDescribeCollectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "describe_collection_total",
			Help:      "Counter of describe collection",
		}, []string{"client_id", "type"})

	// RootCoordShowCollectionsCounter counts the num of calls of ShowCollections
	RootCoordShowCollectionsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "show_collections_total",
			Help:      "Counter of show collections",
		}, []string{"client_id", "type"})

	// RootCoordCreatePartitionCounter counts the num of calls of CreatePartition
	RootCoordCreatePartitionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "create_partition_total",
			Help:      "Counter of create partition",
		}, []string{"client_id", "type"})

	// RootCoordDropPartitionCounter counts the num of calls of DropPartition
	RootCoordDropPartitionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "drop_partition_total",
			Help:      "Counter of drop partition",
		}, []string{"client_id", "type"})

	// RootCoordHasPartitionCounter counts the num of calls of HasPartition
	RootCoordHasPartitionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "has_partition_total",
			Help:      "Counter of has partition",
		}, []string{"client_id", "type"})

	// RootCoordShowPartitionsCounter counts the num of calls of ShowPartitions
	RootCoordShowPartitionsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "show_partitions_total",
			Help:      "Counter of show partitions",
		}, []string{"client_id", "type"})

	// RootCoordCreateIndexCounter counts the num of calls of CreateIndex
	RootCoordCreateIndexCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "create_index_total",
			Help:      "Counter of create index",
		}, []string{"client_id", "type"})

	// RootCoordDropIndexCounter counts the num of calls of DropIndex
	RootCoordDropIndexCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "drop_index_total",
			Help:      "Counter of drop index",
		}, []string{"client_id", "type"})

	// RootCoordDescribeIndexCounter counts the num of calls of DescribeIndex
	RootCoordDescribeIndexCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "describe_index_total",
			Help:      "Counter of describe index",
		}, []string{"client_id", "type"})

	// RootCoordDescribeSegmentCounter counts the num of calls of DescribeSegment
	RootCoordDescribeSegmentCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "describe_segment_total",
			Help:      "Counter of describe segment",
		}, []string{"client_id", "type"})

	// RootCoordShowSegmentsCounter counts the num of calls of ShowSegments
	RootCoordShowSegmentsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "show_segments_total",
			Help:      "Counter of show segments",
		}, []string{"client_id", "type"})

	////////////////////////////////////////////////////////////////////////////
	// for time tick

	// RootCoordInsertChannelTimeTick counts the time tick num of insert channel in 24H
	RootCoordInsertChannelTimeTick = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "insert_channel_time_tick",
			Help:      "Time tick of insert Channel in 24H",
		}, []string{"vchannel"})

	// RootCoordDDChannelTimeTick counts the time tick num of dd channel in 24H
	RootCoordDDChannelTimeTick = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.RootCoordRole,
			Name:      "dd_channel_time_tick",
			Help:      "Time tick of dd Channel in 24H",
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
	prometheus.MustRegister(RootCoordDDChannelTimeTick)
	//prometheus.MustRegister(PanicCounter)
}

var (
	// ProxyCreateCollectionCounter counts the num of calls of CreateCollection
	ProxyCreateCollectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "create_collection_total",
			Help:      "Counter of create collection",
		}, []string{"status"})

	// ProxyDropCollectionCounter counts the num of calls of DropCollection
	ProxyDropCollectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "drop_collection_total",
			Help:      "Counter of drop collection",
		}, []string{"status"})

	// ProxyHasCollectionCounter counts the num of calls of HasCollection
	ProxyHasCollectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "has_collection_total",
			Help:      "Counter of has collection",
		}, []string{"status"})

	// ProxyLoadCollectionCounter counts the num of calls of LoadCollection
	ProxyLoadCollectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "load_collection_total",
			Help:      "Counter of load collection",
		}, []string{"status"})

	// ProxyReleaseCollectionCounter counts the num of calls of ReleaseCollection
	ProxyReleaseCollectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "release_collection_total",
			Help:      "Counter of release collection",
		}, []string{"status"})

	// ProxyDescribeCollectionCounter counts the num of calls of DescribeCollection
	ProxyDescribeCollectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "describe_collection_total",
			Help:      "Counter of describe collection",
		}, []string{"status"})

	// ProxyGetCollectionStatisticsCounter counts the num of calls of GetCollectionStatistics
	ProxyGetCollectionStatisticsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "get_collection_statistics_total",
			Help:      "Counter of get collection statistics",
		}, []string{"status"})

	// ProxyShowCollectionsCounter counts the num of calls of ShowCollections
	ProxyShowCollectionsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "show_collections_total",
			Help:      "Counter of show collections",
		}, []string{"status"})

	// ProxyCreatePartitionCounter counts the num of calls of CreatePartition
	ProxyCreatePartitionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "create_partition_total",
			Help:      "Counter of create partition",
		}, []string{"status"})

	// ProxyDropPartitionCounter counts the num of calls of DropPartition
	ProxyDropPartitionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "drop_partition_total",
			Help:      "Counter of drop partition",
		}, []string{"status"})

	// ProxyHasPartitionCounter counts the num of calls of HasPartition
	ProxyHasPartitionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "has_partition_total",
			Help:      "Counter of has partition",
		}, []string{"status"})

	// ProxyLoadPartitionsCounter counts the num of calls of LoadPartitions
	ProxyLoadPartitionsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "load_partitions_total",
			Help:      "Counter of load partitions",
		}, []string{"status"})

	// ProxyReleasePartitionsCounter counts the num of calls of ReleasePartitions
	ProxyReleasePartitionsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "release_partitions_total",
			Help:      "Counter of release partitions",
		}, []string{"status"})

	// ProxyGetPartitionStatisticsCounter counts the num of calls of GetPartitionStatistics
	ProxyGetPartitionStatisticsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "get_partition_statistics_total",
			Help:      "Counter of get partition statistics",
		}, []string{"status"})

	// ProxyShowPartitionsCounter counts the num of calls of ShowPartitions
	ProxyShowPartitionsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "show_partitions_total",
			Help:      "Counter of show partitions",
		}, []string{"status"})

	// ProxyCreateIndexCounter counts the num of calls of CreateIndex
	ProxyCreateIndexCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "create_index_counter",
			Help:      "Counter of create index",
		}, []string{"status"})

	// ProxyDescribeIndexCounter counts the num of calls of DescribeIndex
	ProxyDescribeIndexCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "describe_index_counter",
			Help:      "Counter of describe index",
		}, []string{"status"})

	// ProxyGetIndexStateCounter counts the num of calls of GetIndexState
	ProxyGetIndexStateCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "get_index_state_counter",
			Help:      "Counter of get index state",
		}, []string{"status"})

	// ProxyGetIndexBuildProgressCounter counts the num of calls of GetIndexBuildProgress
	ProxyGetIndexBuildProgressCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "get_index_build_progress_total",
			Help:      "Counter of get index build progress",
		}, []string{"status"})

	// ProxyDropIndexCounter counts the num of calls of DropIndex
	ProxyDropIndexCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "drop_index_total",
			Help:      "Counter of drop index",
		}, []string{"status"})

	// ProxyInsertCounter counts the num of calls of Insert
	ProxyInsertCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "insert_total",
			Help:      "Counter of insert",
		}, []string{"status"})

	// ProxySearchCounter counts the num of calls of Search
	ProxySearchCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "search_total",
			Help:      "Counter of search",
		}, []string{"status"})

	// ProxyRetrieveCounter counts the num of calls of Retrieve
	ProxyRetrieveCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "retrieve_total",
			Help:      "Counter of retrieve",
		}, []string{"status"})

	// ProxyFlushCounter counts the num of calls of Flush
	ProxyFlushCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "flush_total",
			Help:      "Counter of flush",
		}, []string{"status"})

	// ProxyQueryCounter counts the num of calls of Query
	ProxyQueryCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "query_total",
			Help:      "Counter of query",
		}, []string{"status"})

	// ProxyGetPersistentSegmentInfoCounter counts the num of calls of GetPersistentSegmentInfo
	ProxyGetPersistentSegmentInfoCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "get_persistent_segment_info_total",
			Help:      "Counter of get persistent segment info",
		}, []string{"status"})

	// ProxyGetQuerySegmentInfoCounter counts the num of calls of GetQuerySegmentInfo
	ProxyGetQuerySegmentInfoCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "get_query_segment_info_total",
			Help:      "Counter of get query segment info",
		}, []string{"status"})

	// ProxyDummyCounter counts the num of calls of Dummy
	ProxyDummyCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "dummy_total",
			Help:      "Counter of dummy",
		}, []string{"status"})

	// ProxyRegisterLinkCounter counts the num of calls of RegisterLink
	ProxyRegisterLinkCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "register_link_total",
			Help:      "Counter of register link",
		}, []string{"status"})

	// ProxyGetComponentStatesCounter counts the num of calls of GetComponentStates
	ProxyGetComponentStatesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "get_component_states_total",
			Help:      "Counter of get component states",
		}, []string{"status"})

	// ProxyGetStatisticsChannelCounter counts the num of calls of GetStatisticsChannel
	ProxyGetStatisticsChannelCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "get_statistics_channel_total",
			Help:      "Counter of get statistics channel",
		}, []string{"status"})

	// ProxyInvalidateCollectionMetaCacheCounter counts the num of calls of InvalidateCollectionMetaCache
	ProxyInvalidateCollectionMetaCacheCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "invalidate_collection_meta_cache_total",
			Help:      "Counter of invalidate collection meta cache",
		}, []string{"status"})

	// ProxyGetDdChannelCounter counts the num of calls of GetDdChannel
	ProxyGetDdChannelCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "get_dd_channel_total",
			Help:      "Counter of get dd channel",
		}, []string{"status"})

	// ProxyReleaseDQLMessageStreamCounter counts the num of calls of ReleaseDQLMessageStream
	ProxyReleaseDQLMessageStreamCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "release_dql_message_stream_total",
			Help:      "Counter of release dql message stream",
		}, []string{"status"})

	// ProxyDmlChannelTimeTick counts the time tick value of dml channels
	ProxyDmlChannelTimeTick = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "dml_channels_time_tick",
			Help:      "Time tick of dml channels",
		}, []string{"pchan"})
)

//RegisterProxy registers Proxy metrics
func RegisterProxy() {
	prometheus.MustRegister(ProxyCreateCollectionCounter)
	prometheus.MustRegister(ProxyDropCollectionCounter)
	prometheus.MustRegister(ProxyHasCollectionCounter)
	prometheus.MustRegister(ProxyLoadCollectionCounter)
	prometheus.MustRegister(ProxyReleaseCollectionCounter)
	prometheus.MustRegister(ProxyDescribeCollectionCounter)
	prometheus.MustRegister(ProxyGetCollectionStatisticsCounter)
	prometheus.MustRegister(ProxyShowCollectionsCounter)

	prometheus.MustRegister(ProxyCreatePartitionCounter)
	prometheus.MustRegister(ProxyDropPartitionCounter)
	prometheus.MustRegister(ProxyHasPartitionCounter)
	prometheus.MustRegister(ProxyLoadPartitionsCounter)
	prometheus.MustRegister(ProxyReleasePartitionsCounter)
	prometheus.MustRegister(ProxyGetPartitionStatisticsCounter)
	prometheus.MustRegister(ProxyShowPartitionsCounter)

	prometheus.MustRegister(ProxyCreateIndexCounter)
	prometheus.MustRegister(ProxyDescribeIndexCounter)
	prometheus.MustRegister(ProxyGetIndexStateCounter)
	prometheus.MustRegister(ProxyGetIndexBuildProgressCounter)
	prometheus.MustRegister(ProxyDropIndexCounter)

	prometheus.MustRegister(ProxyInsertCounter)
	prometheus.MustRegister(ProxySearchCounter)
	prometheus.MustRegister(ProxyRetrieveCounter)
	prometheus.MustRegister(ProxyFlushCounter)
	prometheus.MustRegister(ProxyQueryCounter)

	prometheus.MustRegister(ProxyGetPersistentSegmentInfoCounter)
	prometheus.MustRegister(ProxyGetQuerySegmentInfoCounter)

	prometheus.MustRegister(ProxyDummyCounter)

	prometheus.MustRegister(ProxyRegisterLinkCounter)

	prometheus.MustRegister(ProxyGetComponentStatesCounter)
	prometheus.MustRegister(ProxyGetStatisticsChannelCounter)

	prometheus.MustRegister(ProxyInvalidateCollectionMetaCacheCounter)
	prometheus.MustRegister(ProxyGetDdChannelCounter)

	prometheus.MustRegister(ProxyReleaseDQLMessageStreamCounter)

	prometheus.MustRegister(ProxyDmlChannelTimeTick)
}

//RegisterQueryCoord registers QueryCoord metrics
func RegisterQueryCoord() {

}

//RegisterQueryNode registers QueryNode metrics
func RegisterQueryNode() {

}

var (
	//DataCoordDataNodeList records the num of regsitered data nodes
	DataCoordDataNodeList = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "list_of_data_node",
			Help:      "List of data nodes registered within etcd",
		}, []string{"status"},
	)
)

//RegisterDataCoord registers DataCoord metrics
func RegisterDataCoord() {
	prometheus.MustRegister(DataCoordDataNodeList)
}

var (
	// DataNodeFlushSegmentsCounter counts the num of calls of FlushSegments
	DataNodeFlushSegmentsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "flush_segments_total",
			Help:      "Counter of flush segments",
		}, []string{"type"})

	// DataNodeWatchDmChannelsCounter counts the num of calls of WatchDmChannels
	DataNodeWatchDmChannelsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "watch_dm_channels_total",
			Help:      "Counter of watch dm channel",
		}, []string{"type"})
)

//RegisterDataNode registers DataNode metrics
func RegisterDataNode() {
	prometheus.MustRegister(DataNodeFlushSegmentsCounter)
	prometheus.MustRegister(DataNodeWatchDmChannelsCounter)
}

//RegisterIndexNode registers IndexNode metrics
func RegisterIndexNode() {

}

//ServeHTTP serves prometheus http service
func ServeHTTP() {
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(":9091", nil); err != nil {
			log.Error("handle metrics failed", zap.Error(err))
		}
	}()
}

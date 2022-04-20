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
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// ProxyDmlChannelTimeTick counts the time tick value of dml channels
	ProxyDmlChannelTimeTick = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "dml_channels_time_tick",
			Help:      "Time tick of dml channels",
		}, []string{nodeIDLabelName, "pchan"})

	// ProxySearchCount record the number of times search succeeded or failed.
	ProxySearchCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "search_counter",
			Help:      "The number of times search succeeded or failed",
		}, []string{nodeIDLabelName, collectionIDLabelName, queryTypeLabelName, statusLabelName})

	// ProxyInsertCount record the number of times insert succeeded or failed.
	ProxyInsertCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "insert_counter",
			Help:      "The number of times insert succeeded or failed",
		}, []string{nodeIDLabelName, collectionIDLabelName, statusLabelName})

	// ProxySearchVectors record the number of vectors search successfully.
	ProxySearchVectors = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "search_vectors",
			Help:      "The number of vectors search successfully",
		}, []string{nodeIDLabelName, collectionIDLabelName, queryTypeLabelName})

	// ProxyInsertVectors record the number of vectors insert successfully.
	ProxyInsertVectors = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "insert_vectors",
			Help:      "The number of vectors insert successfully",
		}, []string{nodeIDLabelName, collectionIDLabelName})

	// ProxyLinkedSDKs record The number of SDK linked proxy.
	// TODO: how to know when sdk disconnect?
	ProxyLinkedSDKs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "linked_sdk_numbers",
			Help:      "The number of SDK linked proxy",
		}, []string{nodeIDLabelName})

	// ProxySearchLatency record the latency of search successfully.
	ProxySearchLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "search_latency",
			Help:      "The latency of search successfully",
			Buckets:   buckets,
		}, []string{nodeIDLabelName, collectionIDLabelName, queryTypeLabelName})

	// ProxySendMessageLatency record the latency that the proxy sent the search request to the message stream.
	ProxySendMessageLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "send_search_msg_time",
			Help:      "The latency that the proxy sent the search request to the message stream",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName, collectionIDLabelName, queryTypeLabelName})

	// ProxyWaitForSearchResultLatency record the time that the proxy waits for the search result.
	ProxyWaitForSearchResultLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "wait_for_search_result_time",
			Help:      "The time that the proxy waits for the search result",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName, collectionIDLabelName, queryTypeLabelName})

	// ProxyReduceSearchResultLatency record the time that the proxy reduces search result.
	ProxyReduceSearchResultLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "reduce_search_result_time",
			Help:      "The time that the proxy reduces search result",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName, collectionIDLabelName, queryTypeLabelName})

	// ProxyDecodeSearchResultLatency record the time that the proxy decodes the search result.
	ProxyDecodeSearchResultLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "decode_search_result_time",
			Help:      "The time that the proxy decodes the search result",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName, collectionIDLabelName, queryTypeLabelName})

	// ProxyMsgStreamObjectsForPChan record the number of MsgStream objects per PChannel on each collection_id on Proxy.
	ProxyMsgStreamObjectsForPChan = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "msg_stream_obj_for_PChan",
			Help:      "The number of MsgStream objects per PChannel on each collection on Proxy",
		}, []string{nodeIDLabelName, collectionIDLabelName})

	// ProxyMsgStreamObjectsForSearch record the number of MsgStream objects for search per collection_id.
	ProxyMsgStreamObjectsForSearch = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "msg_stream_obj_for_search",
			Help:      "The number of MsgStream objects for search per collection",
		}, []string{nodeIDLabelName, collectionIDLabelName, queryTypeLabelName})

	// ProxyInsertLatency record the latency that insert successfully.
	ProxyInsertLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "insert_latency",
			Help:      "The latency that insert successfully.",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName, collectionIDLabelName})

	// ProxyInsertColToRowLatency record the latency that column to row for inserting in Proxy.
	ProxyInsertColToRowLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "col_to_row_latency",
			Help:      "The time that column to row for inserting in Proxy",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName, collectionIDLabelName})

	// ProxySendInsertReqLatency record the latency that Proxy send insert request to MsgStream.
	ProxySendInsertReqLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "send_insert_req_latency",
			Help:      "The latency that Proxy send insert request to MsgStream",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName, collectionIDLabelName})

	// ProxyCacheHitCounter record the number of Proxy cache hits or miss.
	// TODO: @xiaocai2333 add more cache type
	ProxyCacheHitCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "cache_hits",
			Help:      "Proxy cache hits",
		}, []string{nodeIDLabelName, "cache_type", "hit_type"})

	// ProxyUpdateCacheLatency record the time that proxy update cache when cache miss.
	ProxyUpdateCacheLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "update_cache_latency",
			Help:      "The time that proxy update cache when cache miss",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName})

	// ProxySyncTimeTick record Proxy synchronization timestamp statistics, differentiated by Channel.
	ProxySyncTimeTick = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "sync_time_tick",
			Help:      "Proxy synchronization timestamp statistics, differentiated by Channel",
		}, []string{nodeIDLabelName, channelNameLabelName})

	// ProxyApplyPrimaryKeyLatency record the latency that apply primary key.
	ProxyApplyPrimaryKeyLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "apply_pk_latency",
			Help:      "The latency that apply primary key",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName})

	// ProxyApplyTimestampLatency record the latency that proxy apply timestamp.
	ProxyApplyTimestampLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "apply_timestamp_latency",
			Help:      "The latency that proxy apply timestamp",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName})

	// ProxyDDLFunctionCall records the number of times the function of the DDL operation was executed, like `CreateCollection`.
	ProxyDDLFunctionCall = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "DDL_call_counter",
			Help:      "the number of times the function of the DDL operation was executed",
		}, []string{nodeIDLabelName, functionLabelName, statusLabelName})

	// ProxyDQLFunctionCall records the number of times the function of the DQL operation was executed, like `HasCollection`.
	ProxyDQLFunctionCall = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "DQL_call_counter",
			Help:      "",
		}, []string{nodeIDLabelName, functionLabelName, collectionIDLabelName, statusLabelName})

	// ProxyDMLFunctionCall records the number of times the function of the DML operation was executed, like `LoadCollection`.
	ProxyDMLFunctionCall = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "DML_call_counter",
			Help:      "",
		}, []string{nodeIDLabelName, functionLabelName, collectionIDLabelName, statusLabelName})

	// ProxyDDLReqLatency records the latency that for DML request, like "CreateCollection".
	ProxyDDLReqLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "DDL_call_latency",
			Help:      "The latency that for DDL request",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName, functionLabelName})

	// ProxyDMLReqLatency records the latency that for DML request.
	ProxyDMLReqLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "DML_call_latency",
			Help:      "The latency that for DML request",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName, functionLabelName, collectionIDLabelName})

	// ProxyDQLReqLatency record the latency that for DQL request, like "HasCollection".
	ProxyDQLReqLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "DQL_call_latency",
			Help:      "The latency that for DQL request",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName, functionLabelName, collectionIDLabelName})

	// ProxySearchLatencyPerNQ records the latency for searching.
	ProxySearchLatencyPerNQ = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "proxy_search_latency_count",
			Help:      "The latency for searching",
			Buckets:   buckets,
		}, []string{nodeIDLabelName, collectionIDLabelName})

	ProxySendReqsNum = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "send_reqs",
			Help:      "The number for proxy send reqs",
		}, []string{nodeIDLabelName, collectionIDLabelName})

	ProxyReceiveReqsNum = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "receive_search_req_num",
			Help:      "The number for proxy receive search reqs",
		}, []string{nodeIDLabelName, collectionIDLabelName})
	ProxyServerSearch = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "server_search",
		}, []string{nodeIDLabelName})

	ProxyInQueue = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "search_in_queue",
		}, []string{nodeIDLabelName})

	ProxySearchValidateAndShowCollection = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "search_validate_show_collection",
		}, []string{nodeIDLabelName})

	ProxySearchPreExecute = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "search_pre_execute",
		}, []string{nodeIDLabelName})

	ProxySearchExecute = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "search_execute",
		}, []string{nodeIDLabelName})

	ProxySearchPostExecute = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "search_post_execute",
		}, []string{nodeIDLabelName})
)

//RegisterProxy registers Proxy metrics
func RegisterProxy() {
	prometheus.MustRegister(ProxyDmlChannelTimeTick)

	prometheus.MustRegister(ProxySearchCount)
	prometheus.MustRegister(ProxyInsertCount)
	prometheus.MustRegister(ProxySearchVectors)
	prometheus.MustRegister(ProxyInsertVectors)

	prometheus.MustRegister(ProxyLinkedSDKs)

	prometheus.MustRegister(ProxySearchLatency)
	prometheus.MustRegister(ProxySearchLatencyPerNQ)
	prometheus.MustRegister(ProxySendMessageLatency)
	prometheus.MustRegister(ProxyWaitForSearchResultLatency)
	prometheus.MustRegister(ProxyReduceSearchResultLatency)
	prometheus.MustRegister(ProxyDecodeSearchResultLatency)

	prometheus.MustRegister(ProxyMsgStreamObjectsForPChan)
	prometheus.MustRegister(ProxyMsgStreamObjectsForSearch)

	prometheus.MustRegister(ProxyInsertLatency)
	prometheus.MustRegister(ProxyInsertColToRowLatency)
	prometheus.MustRegister(ProxySendInsertReqLatency)

	prometheus.MustRegister(ProxyCacheHitCounter)
	prometheus.MustRegister(ProxyUpdateCacheLatency)

	prometheus.MustRegister(ProxySyncTimeTick)
	prometheus.MustRegister(ProxyApplyPrimaryKeyLatency)
	prometheus.MustRegister(ProxyApplyTimestampLatency)

	prometheus.MustRegister(ProxyDDLFunctionCall)
	prometheus.MustRegister(ProxyDQLFunctionCall)
	prometheus.MustRegister(ProxyDMLFunctionCall)
	prometheus.MustRegister(ProxyDDLReqLatency)
	prometheus.MustRegister(ProxyDMLReqLatency)
	prometheus.MustRegister(ProxyDQLReqLatency)

	prometheus.MustRegister(ProxySendReqsNum)
	prometheus.MustRegister(ProxyReceiveReqsNum)
	prometheus.MustRegister(ProxyServerSearch)
	prometheus.MustRegister(ProxyInQueue)
	prometheus.MustRegister(ProxySearchValidateAndShowCollection)
	prometheus.MustRegister(ProxySearchPreExecute)
	prometheus.MustRegister(ProxySearchExecute)
	prometheus.MustRegister(ProxySearchPostExecute)
}

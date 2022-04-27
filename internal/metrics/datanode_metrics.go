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
	DataNodeNumFlowGraphs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "flowgraph_num",
			Help:      "number of flowgraphs",
		}, []string{
			nodeIDLabelName,
		})

	DataNodeConsumeMsgRowsCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "msg_rows_count",
			Help:      "count of rows consumed from msgStream",
		}, []string{
			nodeIDLabelName,
			msgTypeLabelName,
		})

	DataNodeFlushedSize = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "flushed_data_size",
			Help:      "byte size of data flushed to storage",
		}, []string{
			nodeIDLabelName,
			msgTypeLabelName,
		})

	DataNodeNumConsumers = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "consumer_num",
			Help:      "number of consumers",
		}, []string{
			nodeIDLabelName,
		})

	DataNodeNumProducers = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "producer_num",
			Help:      "number of producers",
		}, []string{
			nodeIDLabelName,
		})

	DataNodeTimeSync = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "sync_epoch_time",
			Help:      "synchronized unix epoch per physical channel",
		}, []string{
			nodeIDLabelName,
			channelNameLabelName,
		})

	DataNodeNumUnflushedSegments = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "unflushed_segment_num",
			Help:      "number of unflushed segments",
		}, []string{
			nodeIDLabelName,
		})

	DataNodeEncodeBufferLatency = prometheus.NewHistogramVec( // TODO: arguably
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "encode_buffer_latency",
			Help:      "latency of encode buffer data",
			Buckets:   buckets,
		}, []string{
			nodeIDLabelName,
		})

	DataNodeSave2StorageLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "save_latency",
			Help:      "latency of saving flush data to storage",
			Buckets:   []float64{0, 10, 100, 200, 400, 1000, 10000},
		}, []string{
			nodeIDLabelName,
			msgTypeLabelName,
		})

	DataNodeFlushBufferCount = prometheus.NewCounterVec( // TODO: arguably
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "flush_buffer_op_count",
			Help:      "count of flush buffer operations",
		}, []string{
			nodeIDLabelName,
			statusLabelName,
		})

	DataNodeAutoFlushBufferCount = prometheus.NewCounterVec( // TODO: arguably
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "autoflush_buffer_op_count",
			Help:      "count of auto flush buffer operations",
		}, []string{
			nodeIDLabelName,
			statusLabelName,
		})

	DataNodeCompactionLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "compaction_latency",
			Help:      "latency of compaction operation",
			Buckets:   buckets,
		}, []string{
			nodeIDLabelName,
		})

	// DataNodeFlushReqCounter counts the num of calls of FlushSegments
	DataNodeFlushReqCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "flush_req_count",
			Help:      "count of flush request",
		}, []string{
			nodeIDLabelName,
			statusLabelName,
		})
)

//RegisterDataNode registers DataNode metrics
func RegisterDataNode(registry *prometheus.Registry) {
	registry.MustRegister(DataNodeNumFlowGraphs)
	registry.MustRegister(DataNodeConsumeMsgRowsCount)
	registry.MustRegister(DataNodeFlushedSize)
	registry.MustRegister(DataNodeNumConsumers)
	registry.MustRegister(DataNodeNumProducers)
	registry.MustRegister(DataNodeTimeSync)
	registry.MustRegister(DataNodeNumUnflushedSegments)
	registry.MustRegister(DataNodeEncodeBufferLatency)
	registry.MustRegister(DataNodeSave2StorageLatency)
	registry.MustRegister(DataNodeFlushBufferCount)
	registry.MustRegister(DataNodeAutoFlushBufferCount)
	registry.MustRegister(DataNodeCompactionLatency)
	registry.MustRegister(DataNodeFlushReqCounter)
}

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
			Name:      "num_flow_graphs",
			Help:      "Number of flow graphs in DataNode.",
		}, []string{
			nodeIDLabelName,
		})

	DataNodeConsumeMsgRowsCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "message_rows_count",
			Help:      "Messages rows size count consumed from msgStream in DataNode.",
		}, []string{
			msgTypeLabelName,
			nodeIDLabelName,
		})

	DataNodeFlushedSize = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "flushed_size",
			Help:      "Data size flushed to storage in DataNode.",
		}, []string{
			msgTypeLabelName,
			nodeIDLabelName,
		})

	DataNodeNumDmlChannels = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "num_dml_channels",
			Help:      "Number of dmlChannels per collection in DataNode.",
		}, []string{
			collectionIDLabelName,
			nodeIDLabelName,
		})

	DataNodeNumDeltaChannels = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "num_delta_channels",
			Help:      "Number of deltaChannels per collection in DataNode.",
		}, []string{
			collectionIDLabelName,
			nodeIDLabelName,
		})

	DataNodeNumConsumers = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "num_consumers",
			Help:      "Number of consumers per collection in DataNode.",
		}, []string{
			collectionIDLabelName,
			nodeIDLabelName,
		})

	DataNodeNumProducers = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "num_producers",
			Help:      "Number of producers per collection in DataNode.",
		}, []string{
			collectionIDLabelName,
			nodeIDLabelName,
		})

	DataNodeTimeSync = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "time_sync",
			Help:      "Synchronized timestamps per channel in DataNode.",
		}, []string{
			channelNameLabelName,
			nodeIDLabelName,
		})

	DataNodeSegmentRowsCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "seg_rows_count",
			Help:      "Rows count of segments which sent to DataCoord from DataNode.",
		}, []string{
			collectionIDLabelName,
			nodeIDLabelName,
		})

	DataNodeNumUnflushedSegments = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "num_unflushed_segments",
			Help:      "Number of unflushed segments in DataNode.",
		}, []string{
			collectionIDLabelName,
			nodeIDLabelName,
		})

	DataNodeFlushSegmentLatency = prometheus.NewHistogramVec( // TODO: arguably
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "flush_segment_latency",
			Help:      "The flush segment latency in DataNode.",
			Buckets:   buckets,
		}, []string{
			collectionIDLabelName,
			nodeIDLabelName,
		})

	DataNodeSave2StorageLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "save_latency",
			Help:      "The latency saving flush data to storage in DataNode.",
			Buckets:   []float64{0, 10, 100, 200, 400, 1000, 10000},
		}, []string{
			msgTypeLabelName,
			nodeIDLabelName,
		})

	DataNodeFlushSegmentCount = prometheus.NewCounterVec( // TODO: arguably
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "flush_segment_count",
			Help:      "Flush segment statistics in DataNode.",
		}, []string{
			statusLabelName,
			nodeIDLabelName,
		})

	DataNodeAutoFlushSegmentCount = prometheus.NewCounterVec( // TODO: arguably
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "auto_flush_segment_count",
			Help:      "Auto flush segment statistics in DataNode.",
		}, []string{
			channelNameLabelName,
			nodeIDLabelName,
		})

	DataNodeCompactionLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "compaction_latency",
			Help:      "Compaction latency in DataNode.",
			Buckets:   buckets,
		}, []string{
			collectionIDLabelName,
			nodeIDLabelName,
		})
)

//RegisterDataNode registers DataNode metrics
func RegisterDataNode() {
	prometheus.MustRegister(DataNodeNumFlowGraphs)
	prometheus.MustRegister(DataNodeConsumeMsgRowsCount)
	prometheus.MustRegister(DataNodeFlushedSize)
	prometheus.MustRegister(DataNodeNumDmlChannels)
	prometheus.MustRegister(DataNodeNumDeltaChannels)
	prometheus.MustRegister(DataNodeNumConsumers)
	prometheus.MustRegister(DataNodeNumProducers)
	prometheus.MustRegister(DataNodeTimeSync)
	prometheus.MustRegister(DataNodeSegmentRowsCount)
	prometheus.MustRegister(DataNodeNumUnflushedSegments)
	prometheus.MustRegister(DataNodeFlushSegmentLatency)
	prometheus.MustRegister(DataNodeSave2StorageLatency)
	prometheus.MustRegister(DataNodeFlushSegmentCount)
	prometheus.MustRegister(DataNodeAutoFlushSegmentCount)
	prometheus.MustRegister(DataNodeCompactionLatency)
}

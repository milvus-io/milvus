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

const (
	SuccessLabel = "success"
	FailLabel    = "fail"
	TotalLabel   = "total"
)

var (
	IndexNodeBuildIndexTaskCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.IndexNodeRole,
			Name:      "index_task_counter",
			Help:      "The number of tasks that index node received",
		}, []string{"node_id", "status"})

	IndexNodeLoadBinlogLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.IndexNodeRole,
			Name:      "load_segment_latency",
			Help:      "The latency of loading the segment",
		}, []string{"node_id", "segment_id"})

	IndexNodeDecodeBinlogLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.IndexNodeRole,
			Name:      "decode_binlog_latency",
			Help:      "The latency of decode the binlog",
		}, []string{"node_id", "segment_id"})

	IndexNodeKnowhereBuildIndexLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.IndexNodeRole,
			Name:      "knowhere_build_index_latency",
			Help:      "The latency of knowhere building the index",
		}, []string{"node_id", "segment_id"})

	IndexNodeEncodeIndexFileLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.IndexNodeRole,
			Name:      "encode_index_file_latency",
			Help:      "The latency of encoding the index file",
		}, []string{"node_id", "segment_id"})

	IndexNodeSaveIndexFileLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.IndexNodeRole,
			Name:      "save_index_file_latency",
			Help:      "The latency of saving the index file",
		}, []string{"node_id", "segment_id"})
)

//RegisterIndexNode registers IndexNode metrics
func RegisterIndexNode() {
	prometheus.MustRegister(IndexNodeBuildIndexTaskCounter)
	prometheus.MustRegister(IndexNodeLoadBinlogLatency)
	prometheus.MustRegister(IndexNodeDecodeBinlogLatency)
	prometheus.MustRegister(IndexNodeKnowhereBuildIndexLatency)
	prometheus.MustRegister(IndexNodeEncodeIndexFileLatency)
	prometheus.MustRegister(IndexNodeSaveIndexFileLatency)
}

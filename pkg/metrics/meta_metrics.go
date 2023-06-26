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
	"github.com/prometheus/client_golang/prometheus"
)

const (
	MetaGetLabel    = "get"
	MetaPutLabel    = "put"
	MetaRemoveLabel = "remove"
	MetaTxnLabel    = "txn"

	metaOpType = "meta_op_type"
)

var (
	MetaKvSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: "meta",
			Name:      "kv_size",
			Help:      "kv size stats",
			Buckets:   buckets,
		}, []string{metaOpType})

	MetaRequestLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: "meta",
			Name:      "request_latency",
			Help:      "request latency on the client side ",
			Buckets:   buckets,
		}, []string{metaOpType})

	MetaOpCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: "meta",
			Name:      "op_count",
			Help:      "count of meta operation",
		}, []string{metaOpType, statusLabelName})
)

// RegisterMetaMetrics registers meta metrics
func RegisterMetaMetrics(registry *prometheus.Registry) {
	registry.MustRegister(MetaKvSize)
	registry.MustRegister(MetaRequestLatency)
	registry.MustRegister(MetaOpCounter)
}

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

	"github.com/milvus-io/milvus/internal/util/typeutil"
)

var (
	QueryCoordNumCollections = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "collection_num",
			Help:      "number of collections",
		}, []string{})

	QueryCoordNumEntities = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "entity_num",
			Help:      "number of entities",
		}, []string{})

	QueryCoordLoadCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "load_req_count",
			Help:      "count of load request",
		}, []string{
			statusLabelName,
		})

	QueryCoordReleaseCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "release_req_count",
			Help:      "count of release request",
		}, []string{
			statusLabelName,
		})

	QueryCoordLoadLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "load_latency",
			Help:      "latency of load request",
			Buckets:   buckets,
		}, []string{})

	QueryCoordReleaseLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "release_latency",
			Help:      "latency of release request",
			Buckets:   []float64{0, 5, 10, 20, 40, 100, 200, 400, 1000, 10000},
		}, []string{})

	QueryCoordNumChildTasks = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "child_task_num",
			Help:      "number of child tasks in QueryCoord's queue",
		}, []string{})

	QueryCoordNumParentTasks = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "parent_task_num",
			Help:      "number of parent tasks in QueryCoord's queue",
		}, []string{})

	QueryCoordChildTaskLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "child_task_latency",
			Help:      "latency of child tasks",
			Buckets:   buckets,
		}, []string{})

	QueryCoordNumQueryNodes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "querynode_num",
			Help:      "number of QueryNodes managered by QueryCoord",
		}, []string{})
)

//RegisterQueryCoord registers QueryCoord metrics
func RegisterQueryCoord(registry *prometheus.Registry) {
	registry.MustRegister(QueryCoordNumCollections)
	registry.MustRegister(QueryCoordNumEntities)
	registry.MustRegister(QueryCoordLoadCount)
	registry.MustRegister(QueryCoordReleaseCount)
	registry.MustRegister(QueryCoordLoadLatency)
	registry.MustRegister(QueryCoordReleaseLatency)
	registry.MustRegister(QueryCoordNumChildTasks)
	registry.MustRegister(QueryCoordNumParentTasks)
	registry.MustRegister(QueryCoordChildTaskLatency)
	registry.MustRegister(QueryCoordNumQueryNodes)
}

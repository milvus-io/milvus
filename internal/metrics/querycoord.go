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
			Name:      "num_collections",
			Help:      "Number of collections in QueryCoord.",
		}, []string{})

	QueryCoordNumEntities = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "num_entities",
			Help:      "Number of entities in collection.",
		}, []string{
			collectionIDLabelName,
		})

	QueryCoordLoadCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "load_count",
			Help:      "Load request statistic in QueryCoord.",
		}, []string{
			statusLabelName,
		})

	QueryCoordReleaseCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "release_count",
			Help:      "Release request statistic in QueryCoord.",
		}, []string{
			statusLabelName,
		})

	QueryCoordLoadLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "load_latency",
			Help:      "Load request latency in QueryCoord",
			Buckets:   buckets,
		}, []string{})

	QueryCoordReleaseLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "release_latency",
			Help:      "Release request latency in QueryCoord",
			Buckets:   []float64{0, 5, 10, 20, 40, 100, 200, 400, 1000, 10000},
		}, []string{})

	QueryCoordNumChildTasks = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "num_child_tasks",
			Help:      "Number of child tasks in QueryCoord.",
		}, []string{})

	QueryCoordNumParentTasks = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "num_parent_tasks",
			Help:      "Number of parent tasks in QueryCoord.",
		}, []string{})

	QueryCoordChildTaskLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "child_task_latency",
			Help:      "Child tasks latency in QueryCoord.",
			Buckets:   buckets,
		}, []string{})

	QueryCoordNumQueryNodes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "num_querynodes",
			Help:      "Number of QueryNodes in QueryCoord.",
		}, []string{})
)

//RegisterQueryCoord registers QueryCoord metrics
func RegisterQueryCoord() {
	prometheus.MustRegister(QueryCoordNumCollections)
	prometheus.MustRegister(QueryCoordNumEntities)
	prometheus.MustRegister(QueryCoordLoadCount)
	prometheus.MustRegister(QueryCoordReleaseCount)
	prometheus.MustRegister(QueryCoordLoadLatency)
	prometheus.MustRegister(QueryCoordReleaseLatency)
	prometheus.MustRegister(QueryCoordNumChildTasks)
	prometheus.MustRegister(QueryCoordNumParentTasks)
	prometheus.MustRegister(QueryCoordChildTaskLatency)
	prometheus.MustRegister(QueryCoordNumQueryNodes)
}

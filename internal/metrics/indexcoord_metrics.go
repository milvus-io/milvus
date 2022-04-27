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
	// IndexCoordIndexRequestCounter records the number of the index requests.
	IndexCoordIndexRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.IndexCoordRole,
			Name:      "indexreq_count",
			Help:      "number of building index requests ",
		}, []string{statusLabelName})

	// IndexCoordIndexTaskCounter records the number of index tasks of each type.
	IndexCoordIndexTaskCounter = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.IndexCoordRole,
			Name:      "indextask_count",
			Help:      "number of index tasks of each type",
		}, []string{indexTaskStatusLabelName})

	// IndexCoordIndexNodeNum records the number of IndexNodes managed by IndexCoord.
	IndexCoordIndexNodeNum = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.IndexCoordRole,
			Name:      "indexnode_num",
			Help:      "number of IndexNodes managed by IndexCoord",
		}, []string{})
)

//RegisterIndexCoord registers IndexCoord metrics
func RegisterIndexCoord(registry *prometheus.Registry) {
	registry.MustRegister(IndexCoordIndexRequestCounter)
	registry.MustRegister(IndexCoordIndexTaskCounter)
	registry.MustRegister(IndexCoordIndexNodeNum)
}

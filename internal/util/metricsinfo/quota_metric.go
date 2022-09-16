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

package metricsinfo

import (
	"time"

	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// RateMetricLabel defines the metric label collected from nodes.
type RateMetricLabel = string

const (
	NQPerSecond             RateMetricLabel = "NQPerSecond"
	SearchThroughput        RateMetricLabel = "SearchThroughput"
	InsertConsumeThroughput RateMetricLabel = "InsertConsumeThroughput"
	DeleteConsumeThroughput RateMetricLabel = "DeleteConsumeThroughput"
)

// RateMetric contains a RateMetricLabel and a float rate.
type RateMetric struct {
	Label RateMetricLabel
	Rate  float64
}

// FlowGraphMetric contains a minimal timestamp of flow graph and the number of flow graphs.
type FlowGraphMetric struct {
	MinFlowGraphTt typeutil.Timestamp
	NumFlowGraph   int
}

// ReadInfoInQueue contains NQ num or task num in QueryNode's task queue.
type ReadInfoInQueue struct {
	UnsolvedQueue    int64
	ReadyQueue       int64
	ReceiveChan      int64
	ExecuteChan      int64
	AvgQueueDuration time.Duration
}

// QueryNodeQuotaMetrics are metrics of QueryNode.
type QueryNodeQuotaMetrics struct {
	Hms         HardwareMetrics
	Rms         []RateMetric
	Fgm         FlowGraphMetric
	SearchQueue ReadInfoInQueue
	QueryQueue  ReadInfoInQueue
}

// DataNodeQuotaMetrics are metrics of DataNode.
type DataNodeQuotaMetrics struct {
	Hms HardwareMetrics
	Rms []RateMetric
	Fgm FlowGraphMetric
}

// ProxyQuotaMetrics are metrics of Proxy.
type ProxyQuotaMetrics struct {
	Hms HardwareMetrics
	Rms []RateMetric
}

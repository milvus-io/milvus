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
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// RateMetricLabel defines the metric label collected from nodes.
type RateMetricLabel = string

const (
	ReadResultThroughput    RateMetricLabel = "ReadResultThroughput"
	InsertConsumeThroughput RateMetricLabel = "InsertConsumeThroughput"
	DeleteConsumeThroughput RateMetricLabel = "DeleteConsumeThroughput"
)

const (
	UnsolvedQueueType string = "Unsolved"
	ReadyQueueType    string = "Ready"
	ReceiveQueueType  string = "Receive"
	ExecuteQueueType  string = "Execute"
)

// RateMetric contains a RateMetricLabel and a float rate.
type RateMetric struct {
	Label RateMetricLabel
	Rate  float64
}

// FlowGraphMetric contains a minimal timestamp of flow graph and the number of flow graphs.
type FlowGraphMetric struct {
	MinFlowGraphChannel string
	MinFlowGraphTt      typeutil.Timestamp
	NumFlowGraph        int
}

// NodeEffect contains the a node and its effected collection info.
type NodeEffect struct {
	NodeID        int64
	CollectionIDs []int64
}

// QueryNodeQuotaMetrics are metrics of QueryNode.
type QueryNodeQuotaMetrics struct {
	Hms                 HardwareMetrics
	Rms                 []RateMetric
	Fgm                 FlowGraphMetric
	GrowingSegmentsSize int64
	Effect              NodeEffect
	DeleteBufferInfo    DeleteBufferInfo
	StreamingQuota      *StreamingQuotaMetrics
}

// StreamingQuotaMetrics contains the metrics of streaming node.
// Only used in queryNode embedded in streaming node.
type StreamingQuotaMetrics struct {
	WALs []WALMetrics
}

// WALMetrics contains the metrics of wal.
type WALMetrics struct {
	Channel          types.ChannelID    // ChannelID is the unique identifier of a pchannel.
	RecoveryTimeTick typeutil.Timestamp // current recovery
}

type DeleteBufferInfo struct {
	CollectionDeleteBufferNum  map[int64]int64
	CollectionDeleteBufferSize map[int64]int64
}

type DataCoordQuotaMetrics struct {
	TotalBinlogSize      int64
	CollectionBinlogSize map[int64]int64
	PartitionsBinlogSize map[int64]map[int64]int64
	// l0 segments
	CollectionL0RowCount map[int64]int64
}

// DataNodeQuotaMetrics are metrics of DataNode.
type DataNodeQuotaMetrics struct {
	Hms    HardwareMetrics
	Rms    []RateMetric
	Fgm    FlowGraphMetric
	Effect NodeEffect
}

// ProxyQuotaMetrics are metrics of Proxy.
type ProxyQuotaMetrics struct {
	Hms          HardwareMetrics
	Rms          []RateMetric
	QueueMetrics []TaskQueueMetrics
}

type QuotaCenterMetrics struct {
	QueryNodeMetrics map[int64]*QueryNodeQuotaMetrics
	DataNodeMetrics  map[int64]*DataNodeQuotaMetrics
	ProxyMetrics     map[int64]*ProxyQuotaMetrics
	DataCoordMetrics *DataCoordQuotaMetrics
}

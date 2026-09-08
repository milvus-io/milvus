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

package datacoord

import (
	"strconv"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func normalizeCompactionMetricNodeID(nodeID int64) int64 {
	if nodeID == 0 {
		return NullNodeID
	}
	return nodeID
}

func addCompactionTaskNum(nodeID int64, compactionType datapb.CompactionType, status string, delta float64) {
	metrics.DataCoordCompactionTaskNum.
		WithLabelValues(strconv.FormatInt(normalizeCompactionMetricNodeID(nodeID), 10), compactionType.String(), status).
		Add(delta)
}

func incCompactionTaskNum(nodeID int64, compactionType datapb.CompactionType, status string) {
	addCompactionTaskNum(nodeID, compactionType, status, 1)
}

func decCompactionTaskNum(nodeID int64, compactionType datapb.CompactionType, status string) {
	addCompactionTaskNum(nodeID, compactionType, status, -1)
}

func incNodeDoneCompactionTaskNum(nodeID int64, compactionType datapb.CompactionType) {
	incCompactionTaskNum(nodeID, compactionType, metrics.Done)
}

func incNodeExecutingCompactionTaskNum(nodeID int64, compactionType datapb.CompactionType) {
	incCompactionTaskNum(nodeID, compactionType, metrics.Executing)
}

func decNodeExecutingCompactionTaskNum(nodeID int64, compactionType datapb.CompactionType) {
	decCompactionTaskNum(nodeID, compactionType, metrics.Executing)
}

func incCoordPendingCompactionTaskNum(compactionType datapb.CompactionType) {
	incCompactionTaskNum(NullNodeID, compactionType, metrics.Pending)
}

func decCoordPendingCompactionTaskNum(compactionType datapb.CompactionType) {
	decCompactionTaskNum(NullNodeID, compactionType, metrics.Pending)
}

func incCoordExecutingCompactionTaskNum(compactionType datapb.CompactionType) {
	incCompactionTaskNum(NullNodeID, compactionType, metrics.Executing)
}

func decCoordExecutingCompactionTaskNum(compactionType datapb.CompactionType) {
	decCompactionTaskNum(NullNodeID, compactionType, metrics.Executing)
}

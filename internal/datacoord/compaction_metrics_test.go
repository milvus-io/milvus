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
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestNormalizeCompactionMetricNodeID(t *testing.T) {
	require.Equal(t, NullNodeID, normalizeCompactionMetricNodeID(0))
	require.Equal(t, NullNodeID, normalizeCompactionMetricNodeID(NullNodeID))
	require.Equal(t, int64(1), normalizeCompactionMetricNodeID(1))
}

func TestCompactionTaskNumRetryAndCompletionTransitions(t *testing.T) {
	metrics.DataCoordCompactionTaskNum.Reset()
	t.Cleanup(func() {
		metrics.DataCoordCompactionTaskNum.Reset()
	})

	compactionType := datapb.CompactionType_MixCompaction
	nodeID := int64(101)

	// Repeated create/query failures must not accumulate gauges for the same
	// logical transition from pending to coord-executing and back.
	for i := 0; i < 5; i++ {
		incCoordPendingCompactionTaskNum(compactionType)
		decCoordPendingCompactionTaskNum(compactionType)
		incCoordExecutingCompactionTaskNum(compactionType)
		decNodeExecutingCompactionTaskNum(NullNodeID, compactionType)
		incCoordPendingCompactionTaskNum(compactionType)
	}
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.DataCoordCompactionTaskNum.WithLabelValues(
		strconvNullNodeID(), compactionType.String(), metrics.Pending)))
	require.Equal(t, float64(0), testutil.ToFloat64(metrics.DataCoordCompactionTaskNum.WithLabelValues(
		strconvNullNodeID(), compactionType.String(), metrics.Executing)))

	// A final accepted worker and terminal state complete the lifecycle.
	decCoordPendingCompactionTaskNum(compactionType)
	incCoordExecutingCompactionTaskNum(compactionType)
	decCoordExecutingCompactionTaskNum(compactionType)
	incNodeExecutingCompactionTaskNum(nodeID, compactionType)
	decNodeExecutingCompactionTaskNum(nodeID, compactionType)
	incNodeDoneCompactionTaskNum(nodeID, compactionType)

	require.Equal(t, float64(0), testutil.ToFloat64(metrics.DataCoordCompactionTaskNum.WithLabelValues(
		strconvNullNodeID(), compactionType.String(), metrics.Pending)))
	require.Equal(t, float64(0), testutil.ToFloat64(metrics.DataCoordCompactionTaskNum.WithLabelValues(
		strconvNullNodeID(), compactionType.String(), metrics.Executing)))
	require.Equal(t, float64(0), testutil.ToFloat64(metrics.DataCoordCompactionTaskNum.WithLabelValues(
		"101", compactionType.String(), metrics.Executing)))
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.DataCoordCompactionTaskNum.WithLabelValues(
		"101", compactionType.String(), metrics.Done)))
}

func TestCompactionTaskNumNormalizesZeroNodeID(t *testing.T) {
	metrics.DataCoordCompactionTaskNum.Reset()
	t.Cleanup(func() {
		metrics.DataCoordCompactionTaskNum.Reset()
	})

	compactionType := datapb.CompactionType_SortCompaction
	incNodeExecutingCompactionTaskNum(0, compactionType)
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.DataCoordCompactionTaskNum.WithLabelValues(
		strconvNullNodeID(), compactionType.String(), metrics.Executing)))
}

func strconvNullNodeID() string {
	return strconv.FormatInt(NullNodeID, 10)
}

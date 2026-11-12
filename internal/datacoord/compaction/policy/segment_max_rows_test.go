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

package policy

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datacoord/compaction/view"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestGetSegmentMaxRows(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	require.NoError(t, params.Save(params.DataCoordCfg.SegmentMaxRows.Key, "4000000"))
	t.Cleanup(func() { params.Reset(params.DataCoordCfg.SegmentMaxRows.Key) })

	// The cap does not depend on the schema: it is the configured value
	// unless the collection property overrides it.
	require.Equal(t, int64(4000000), GetSegmentMaxRows(nil))
	require.Equal(t, int64(4000000), GetSegmentMaxRows(map[string]string{common.CollectionTTLConfigKey: "10"}))
	require.Equal(t, int64(1000000), GetSegmentMaxRows(map[string]string{common.CollectionSegmentMaxRowsKey: "1000000"}))
	require.Equal(t, int64(9000000), GetSegmentMaxRows(map[string]string{common.CollectionSegmentMaxRowsKey: "9000000"}))
	// 0 turns the cap off for the collection
	require.Equal(t, int64(0), GetSegmentMaxRows(map[string]string{common.CollectionSegmentMaxRowsKey: "0"}))
	// a property that does not parse is ignored
	require.Equal(t, int64(4000000), GetSegmentMaxRows(map[string]string{common.CollectionSegmentMaxRowsKey: "-1"}))
	require.Equal(t, int64(4000000), GetSegmentMaxRows(map[string]string{common.CollectionSegmentMaxRowsKey: "many"}))

	require.NoError(t, params.Save(params.DataCoordCfg.SegmentMaxRows.Key, "0"))
	require.Equal(t, int64(0), GetSegmentMaxRows(nil))
	require.Equal(t, int64(1000000), GetSegmentMaxRows(map[string]string{common.CollectionSegmentMaxRowsKey: "1000000"}))
	require.NoError(t, params.Save(params.DataCoordCfg.SegmentMaxRows.Key, "-1"))
	require.Equal(t, int64(0), GetSegmentMaxRows(nil))
}

func TestRowCapSizing(t *testing.T) {
	const (
		gb      = int64(1024 * 1024 * 1024)
		maxRows = int64(4_000_000)
	)
	t.Run("MinRowSize", func(t *testing.T) {
		require.InDelta(t, 268.4, MinRowSize(gb, maxRows), 0.1)
		require.Zero(t, MinRowSize(gb, 0))
		require.Zero(t, MinRowSize(0, maxRows))
	})
	t.Run("RowCappedSize", func(t *testing.T) {
		// maxRows narrow rows are charged the whole expected size
		require.InDelta(t, float64(gb), RowCappedSize(48e6, maxRows, gb, maxRows), 1)
		// rows wider than expectedSize / maxRows keep their bytes
		require.Equal(t, 2e9, RowCappedSize(2e9, 1_000_000, gb, maxRows))
		// no cap
		require.Equal(t, 48e6, RowCappedSize(48e6, maxRows, gb, 0))
	})
	t.Run("PlanMaxSegmentRows", func(t *testing.T) {
		require.Equal(t, maxRows, PlanMaxSegmentRows(gb, gb, maxRows))
		// a plan with a smaller byte target cuts at the same fraction of the rows
		require.Equal(t, maxRows/4, PlanMaxSegmentRows(gb/4, gb, maxRows))
		require.Equal(t, maxRows, PlanMaxSegmentRows(2*gb, gb, maxRows))
		require.Zero(t, PlanMaxSegmentRows(gb, gb, 0))
		require.Zero(t, PlanMaxSegmentRows(0, gb, maxRows))
	})
}

// Clustering derives its row limits from the expected size and the measured
// row size of the inputs; the row cap bounds them.
func TestCalculateClusteringCompactionConfigRowCap(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	require.NoError(t, params.Save(params.DataCoordCfg.ClusteringCompactionMaxSegmentSizeRatio.Key, "1.0"))
	require.NoError(t, params.Save(params.DataCoordCfg.ClusteringCompactionPreferSegmentSizeRatio.Key, "0.5"))
	t.Cleanup(func() {
		params.Reset(params.DataCoordCfg.ClusteringCompactionMaxSegmentSizeRatio.Key)
		params.Reset(params.DataCoordCfg.ClusteringCompactionPreferSegmentSizeRatio.Key)
	})
	const gb = int64(1024 * 1024 * 1024)
	// 10 M rows of 12 B: 1 GB would be 89 M rows per segment
	cv := &ClusteringSegmentsView{Segments: []*view.SegmentView{{ID: 1, NumOfRows: 10_000_000, Size: 120e6}}}

	_, maxSegmentRows, preferSegmentRows, err := CalculateClusteringCompactionConfig(nil, cv, gb, 0)
	require.NoError(t, err)
	require.Greater(t, maxSegmentRows, int64(80_000_000))
	require.Greater(t, preferSegmentRows, int64(40_000_000))

	totalRows, maxSegmentRows, preferSegmentRows, err := CalculateClusteringCompactionConfig(nil, cv, gb, 4_000_000)
	require.NoError(t, err)
	require.Equal(t, int64(10_000_000), totalRows)
	require.Equal(t, int64(4_000_000), maxSegmentRows)
	require.Equal(t, int64(2_000_000), preferSegmentRows)
}

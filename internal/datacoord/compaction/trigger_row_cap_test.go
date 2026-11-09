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

package compaction

import (
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/view"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// The mix planners pack on bytes. Under a row cap every segment is charged at
// least expectedSize / maxRows bytes per row, which makes both planners treat
// a segment holding maxRows rows as full and stop a bucket at maxRows rows,
// for rows far narrower than expectedSize / maxRows.
func TestGeneratePlansRowCap(t *testing.T) {
	paramtable.Init()
	const (
		expectedSize = int64(1024 * 1024 * 1024)
		maxRows      = int64(4_000_000)
		rowBytes     = int64(12) // int64 + int32, no vector
	)
	newSegments := func(count int, rows int64) []*meta.SegmentInfo {
		return lo.Times(count, func(i int) *meta.SegmentInfo {
			return meta.NewSegmentInfo(&datapb.SegmentInfo{
				ID:            int64(i + 1),
				CollectionID:  2,
				PartitionID:   1,
				InsertChannel: "ch1",
				State:         commonpb.SegmentState_Flushed,
				Level:         datapb.SegmentLevel_L1,
				NumOfRows:     rows,
				Stats:         &datapb.Statistics{InsertBinlogSize: rows * rowBytes, InsertBinlogCount: 1},
			})
		})
	}
	bucketRows := func(buckets []*compactionBucket) []int64 {
		return lo.Map(buckets, func(b *compactionBucket, _ int) int64 { return b.totalRows })
	}
	signal := &CompactionSignal{CollectionID: 2, partitionID: 1, channel: "ch1"}
	compactTime := &view.CompactTime{StartTime: 1000, CollectionTTL: time.Hour}

	for _, twoTier := range []string{"true", "false"} {
		t.Run("twoTier="+twoTier, func(t *testing.T) {
			params := paramtable.Get()
			require.NoError(t, params.Save(params.DataCoordCfg.TwoTierCompaction.Key, twoTier))
			defer params.Reset(params.DataCoordCfg.TwoTierCompaction.Key)
			tr := &compactionTrigger{closeCh: lifetime.NewSafeChan(), testingOnly: true}

			// Twenty segments of maxRows rows each: 48 MB apiece, 960 MB in all.
			t.Run("segments holding maxRows rows are full", func(t *testing.T) {
				// On bytes alone they are small and get merged into one
				// 80 M row bucket.
				uncapped := tr.generatePlans(newSegments(20, maxRows), signal, compactTime, expectedSize, 0)
				require.NotEmpty(t, uncapped)
				require.Equal(t, 20*maxRows, lo.Sum(bucketRows(uncapped)))
				// Under the cap each one is full and none is selected: an
				// output the writer cut at maxRows rows is not compacted again.
				require.Empty(t, tr.generatePlans(newSegments(20, maxRows), signal, compactTime, expectedSize, maxRows))
			})

			// Twenty segments of maxRows / 8 rows each.
			t.Run("smaller segments are packed up to maxRows rows", func(t *testing.T) {
				buckets := tr.generatePlans(newSegments(20, maxRows/8), signal, compactTime, expectedSize, maxRows)
				require.NotEmpty(t, buckets)
				expansion := paramtable.Get().DataCoordCfg.SegmentExpansionRate.GetAsFloat()
				for _, bucket := range buckets {
					require.Equal(t, expectedSize, bucket.maxSize)
					require.Greater(t, len(bucket.segments), 1)
					require.LessOrEqual(t, float64(bucket.totalRows), float64(maxRows)*expansion)
				}
				// The copies in a bucket carry the per-row minimum, so the
				// size used to pre-allocate output segment IDs counts rows.
				charged := lo.SumBy(buckets[0].segments, func(s *meta.SegmentInfo) int64 { return s.GetSegmentSize() })
				require.InDelta(t, float64(buckets[0].totalRows)*float64(expectedSize)/float64(maxRows), float64(charged), float64(len(buckets[0].segments)))
			})
		})
	}
}

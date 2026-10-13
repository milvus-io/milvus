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

package segcore

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func TestFlattenClusteringSearchHints(t *testing.T) {
	h := &querypb.SegmentClusterSearchHints{SnapshotVersion: 10, NumRows: 1 << 34, Queries: []*querypb.QueryClusterSearchHints{
		{Hints: &querypb.ClusterSearchHintList{Ranges: []*querypb.ClusterSearchHint{{RowOffset: 1 << 33, RowCount: 2, CentroidDistance: 1.25}}}}, {}, {Hints: &querypb.ClusterSearchHintList{}},
	}}
	v := flattenClusteringSearchHints(h, 3)
	require.Equal(t, []byte{1, 0, 1}, v.presence)
	require.Equal(t, []int64{0, 1, 1, 1}, v.boundaries)
	require.Equal(t, []int64{1 << 33}, v.offsets)
	require.Equal(t, []float32{1.25}, v.distances)
	require.Empty(t, flattenClusteringSearchHints(h, 2).presence)
	for _, mutate := range []func(*querypb.SegmentClusterSearchHints){
		func(x *querypb.SegmentClusterSearchHints) { x.SnapshotVersion = 0 },
		func(x *querypb.SegmentClusterSearchHints) { x.NumRows = 1 },
		func(x *querypb.SegmentClusterSearchHints) { x.Queries[0].Hints.Ranges[0].RowCount = math.MaxInt64 },
		func(x *querypb.SegmentClusterSearchHints) {
			x.Queries[0].Hints.Ranges[0].CentroidDistance = float32(math.NaN())
		},
		func(x *querypb.SegmentClusterSearchHints) { x.Queries[0].Hints.Ranges[0].RowOffset = -1 },
		func(x *querypb.SegmentClusterSearchHints) {
			x.Queries[0].Hints.Ranges = append(x.Queries[0].Hints.Ranges, nil)
		},
	} {
		bad := proto.Clone(h).(*querypb.SegmentClusterSearchHints)
		mutate(bad)
		require.Empty(t, flattenClusteringSearchHints(bad, 3).presence)
	}
}

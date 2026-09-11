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

package cmek

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

func TestSelectFlushSegmentsDoesNotSubstituteCompactionChildren(t *testing.T) {
	original := &datapb.SegmentInfo{
		ID: 11, State: commonpb.SegmentState_Dropped, NumOfRows: 512, Compacted: true,
	}
	child := &datapb.SegmentInfo{
		ID: 22, State: commonpb.SegmentState_Flushed, NumOfRows: 512, CompactionFrom: []int64{11},
	}
	second := &datapb.SegmentInfo{
		ID: 12, State: commonpb.SegmentState_Flushed, NumOfRows: 256,
	}

	require.Equal(t, []*datapb.SegmentInfo{original}, selectFlushSegments([]int64{11}, []*datapb.SegmentInfo{original, child}))
	require.Empty(t, selectFlushSegments([]int64{11}, []*datapb.SegmentInfo{child}))
	require.Empty(t, selectFlushSegments([]int64{11, 12}, []*datapb.SegmentInfo{original, child}))
	require.Equal(t, []*datapb.SegmentInfo{original, second}, selectFlushSegments([]int64{11, 12}, []*datapb.SegmentInfo{original, second, child}))
}

func TestCompleteVectorIndexMetadataRequiresEveryField(t *testing.T) {
	segments := []*datapb.SegmentInfo{{ID: 31}}
	vectorFieldIDs := map[int64]struct{}{101: {}, 102: {}}
	response := &indexpb.GetIndexInfoResponse{SegmentInfo: map[int64]*indexpb.SegmentInfo{
		31: {SegmentID: 31},
	}}

	require.False(t, completeVectorIndexMetadata(response, segments, vectorFieldIDs))
	response.SegmentInfo[31].IndexInfos = []*indexpb.IndexFilePathInfo{{SegmentID: 31, FieldID: 101}}
	require.False(t, completeVectorIndexMetadata(response, segments, vectorFieldIDs))
	response.SegmentInfo[31].IndexInfos = append(response.SegmentInfo[31].IndexInfos,
		&indexpb.IndexFilePathInfo{SegmentID: 31, FieldID: 102})
	require.True(t, completeVectorIndexMetadata(response, segments, vectorFieldIDs))
	response.SegmentInfo[31].IndexInfos = append(response.SegmentInfo[31].IndexInfos,
		&indexpb.IndexFilePathInfo{SegmentID: 31, FieldID: 102})
	require.False(t, completeVectorIndexMetadata(response, segments, vectorFieldIDs))
}

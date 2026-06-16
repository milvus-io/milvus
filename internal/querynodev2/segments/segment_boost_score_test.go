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

package segments

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/querynodev2/segments/state"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func TestComputeScorerScoresOnChunkedOffsetsNilSegment(t *testing.T) {
	scores, err := ComputeScorerScoresOnChunkedOffsets(context.Background(), nil, nil, nil, nil)
	require.Error(t, err)
	require.Nil(t, scores)
	require.Contains(t, err.Error(), "segment is nil")

	scores, err = AsyncComputeScorerScoresOnChunkedOffsets(context.Background(), nil, nil, nil, nil)
	require.Error(t, err)
	require.Nil(t, scores)
	require.Contains(t, err.Error(), "segment is nil")
}

func TestComputeScorerScoresOnChunkedOffsetsNonLocalSegment(t *testing.T) {
	segment := NewMockSegment(t)
	segment.EXPECT().ID().Return(int64(1001))

	scores, err := ComputeScorerScoresOnChunkedOffsets(context.Background(), segment, nil, nil, nil)
	require.Error(t, err)
	require.Nil(t, scores)
	require.Contains(t, err.Error(), "does not support boost score")
}

func TestComputeScorerScoresOnChunkedOffsetsNilCSegment(t *testing.T) {
	segment := &LocalSegment{
		baseSegment: baseSegment{
			loadInfo: atomic.NewPointer(&querypb.SegmentLoadInfo{SegmentID: 1002}),
		},
	}

	scores, err := ComputeScorerScoresOnChunkedOffsets(context.Background(), segment, nil, nil, nil)
	require.Error(t, err)
	require.Nil(t, scores)
	require.Contains(t, err.Error(), "has nil CSegment")
}

func TestComputeScorerScoresOnChunkedOffsetsReleasedSegment(t *testing.T) {
	segment := &LocalSegment{
		baseSegment: baseSegment{
			loadInfo: atomic.NewPointer(&querypb.SegmentLoadInfo{SegmentID: 1003}),
		},
		ptrLock:  state.NewLoadStateLock(state.LoadStateOnlyMeta),
		csegment: mock_segcore.NewMockCSegment(t),
	}
	guard := segment.ptrLock.StartReleaseAll()
	require.NotNil(t, guard)

	scores, err := AsyncComputeScorerScoresOnChunkedOffsets(context.Background(), segment, nil, nil, nil)
	require.Error(t, err)
	require.Nil(t, scores)
	require.Contains(t, err.Error(), "segment released")
}

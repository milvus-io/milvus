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

package querynodev2

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestDropIndexReportsFailureAndPartialProgress(t *testing.T) {
	ctx := context.Background()
	segment := segments.NewMockSegment(t)
	manager := segments.NewMockSegmentManager(t)
	manager.EXPECT().GetAndPinBy(mock.Anything, mock.Anything).Return([]segments.Segment{segment}, nil)
	manager.EXPECT().Unpin([]segments.Segment{segment}).Return()
	segment.EXPECT().DropIndex(ctx, int64(1000)).Return(nil).Once()
	dropErr := merr.WrapErrServiceUnavailable("field data reload failed")
	segment.EXPECT().DropIndex(ctx, int64(1001)).Return(dropErr).Once()
	node := &QueryNode{
		manager:          &segments.Manager{Segment: manager},
		distDeltaTracker: newDataDistributionDeltaTracker(),
	}
	status, err := node.DropIndex(ctx, &querypb.DropIndexRequest{
		SegmentID: 2, IndexIDs: []int64{1000, 1001},
	})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(status), merr.ErrServiceUnavailable)
	require.Contains(t, node.distDeltaTracker.dirtySegments, int64(2), "successful partial deletion must be reported")
	require.Positive(t, node.getDistributionModifyTS())
}

func TestDropIndexMissingSegmentIsIdempotent(t *testing.T) {
	manager := segments.NewMockSegmentManager(t)
	manager.EXPECT().GetAndPinBy(mock.Anything, mock.Anything).Return(nil, nil)
	node := &QueryNode{manager: &segments.Manager{Segment: manager}}
	status, err := node.DropIndex(context.Background(), &querypb.DropIndexRequest{SegmentID: 2, IndexIDs: []int64{1000}})
	require.NoError(t, err)
	require.True(t, merr.Ok(status))
}

func TestDropIndexSelectsSealedCopy(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	manager := segments.NewSegmentManager()
	var sealed *segments.MockSegment
	for _, typ := range []segments.SegmentType{segments.SegmentTypeGrowing, segments.SegmentTypeSealed} {
		segment := segments.NewMockSegment(t)
		segment.EXPECT().ID().Return(int64(2))
		segment.EXPECT().Type().Return(typ)
		segment.EXPECT().Collection().Return(int64(1))
		segment.EXPECT().Shard().Return(metautil.Channel{})
		segment.EXPECT().Level().Return(datapb.SegmentLevel_L1)
		segment.EXPECT().PinIfNotReleased().Return(nil).Maybe()
		segment.EXPECT().Unpin().Return().Maybe()
		segment.EXPECT().DropIndex(ctx, int64(1000)).Return(nil).Maybe()
		manager.Put(ctx, typ, segment)
		if typ == segments.SegmentTypeSealed {
			sealed = segment
		}
	}
	node := &QueryNode{
		manager:          &segments.Manager{Segment: manager},
		distDeltaTracker: newDataDistributionDeltaTracker(),
	}
	status, err := node.DropIndex(ctx, &querypb.DropIndexRequest{SegmentID: 2, IndexIDs: []int64{1000}})
	require.NoError(t, err)
	require.True(t, merr.Ok(status))
	sealed.AssertCalled(t, "DropIndex", ctx, int64(1000))
}

// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/querynodev2/segments/state"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestRetrySegmentReadGate(t *testing.T) {
	t.Run("sealed retries gate busy", func(t *testing.T) {
		calls := 0
		waits := 0
		result, err := retrySegmentReadGate(
			context.Background(),
			SegmentTypeSealed,
			func() (int, error) {
				calls++
				if calls == 1 {
					return 0, merr.SegcoreError(
						2037, "segment read gate busy for segment 1")
				}
				return 42, nil
			},
			func(context.Context, int) error {
				waits++
				return nil
			},
		)
		require.NoError(t, err)
		assert.Equal(t, 42, result)
		assert.Equal(t, 2, calls)
		assert.Equal(t, 1, waits)
	})

	t.Run("growing does not retry", func(t *testing.T) {
		calls := 0
		waits := 0
		_, err := retrySegmentReadGate(
			context.Background(),
			SegmentTypeGrowing,
			func() (int, error) {
				calls++
				return 0, merr.SegcoreError(
					2037, "segment read gate busy for segment 2")
			},
			func(context.Context, int) error {
				waits++
				return nil
			},
		)
		require.Error(t, err)
		assert.Equal(t, 1, calls)
		assert.Zero(t, waits)
	})

	t.Run("unrelated folly error does not retry", func(t *testing.T) {
		calls := 0
		waits := 0
		_, err := retrySegmentReadGate(
			context.Background(),
			SegmentTypeSealed,
			func() (int, error) {
				calls++
				return 0, merr.SegcoreError(
					2037, "unrelated folly failure")
			},
			func(context.Context, int) error {
				waits++
				return nil
			},
		)
		require.Error(t, err)
		assert.Equal(t, 1, calls)
		assert.Zero(t, waits)
	})

	t.Run("retry obeys context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		calls := 0
		result, err := retrySegmentReadGate(
			ctx,
			SegmentTypeSealed,
			func() (int, error) {
				calls++
				return 0, merr.SegcoreError(
					2037, "segment read gate busy for segment 3")
			},
			func(ctx context.Context, _ int) error {
				cancel()
				return ctx.Err()
			},
		)
		assert.ErrorIs(t, err, context.Canceled)
		assert.Zero(t, result)
		assert.Equal(t, 1, calls)
	})
}

func TestLocalSegmentRetrieveRetriesReadGateBusy(t *testing.T) {
	paramtable.Init()

	t.Run("retrieve", func(t *testing.T) {
		ctx := context.Background()
		plan := new(segcore.RetrievePlan)
		csegment := mock_segcore.NewMockCSegment(t)
		csegment.EXPECT().
			Retrieve(ctx, plan).
			Return(nil, merr.SegcoreError(
				2037, "segment read gate busy for segment 10")).
			Once()
		csegment.EXPECT().Retrieve(ctx, plan).Return(nil, nil).Once()

		base := newTestBaseSegment(10, 20)
		base.segmentType = SegmentTypeSealed
		segment := &LocalSegment{
			baseSegment: base,
			ptrLock:     state.NewLoadStateLock(state.LoadStateDataLoaded),
			csegment:    csegment,
		}

		result, err := segment.retrieve(ctx, plan, mlog.With())
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("retrieve by offsets", func(t *testing.T) {
		ctx := context.Background()
		plan := &segcore.RetrievePlanWithOffsets{
			Offsets: []int64{1},
		}
		csegment := mock_segcore.NewMockCSegment(t)
		csegment.EXPECT().
			RetrieveByOffsets(ctx, plan).
			Return(nil, merr.SegcoreError(
				2037, "segment read gate busy for segment 11")).
			Once()
		csegment.EXPECT().
			RetrieveByOffsets(ctx, plan).
			Return(nil, nil).
			Once()

		base := newTestBaseSegment(11, 20)
		base.segmentType = SegmentTypeSealed
		segment := &LocalSegment{
			baseSegment: base,
			ptrLock:     state.NewLoadStateLock(state.LoadStateDataLoaded),
			csegment:    csegment,
		}

		result, err := segment.retrieveByOffsets(ctx, plan, mlog.With())
		require.NoError(t, err)
		assert.Nil(t, result)
	})
}

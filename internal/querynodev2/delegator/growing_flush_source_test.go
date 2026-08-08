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

package delegator

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestDelegatorGrowingFlushSourcePassesTaskSchema(t *testing.T) {
	ctx := context.Background()
	schema := &schemapb.CollectionSchema{Name: "task-schema"}
	segment := segments.NewMockSegment(t)
	segment.EXPECT().
		FlushData(ctx, uint64(3), uint64(7), mock.Anything).
		RunAndReturn(func(_ context.Context, startTs uint64, endTs uint64, config *segments.FlushConfig) (*segments.FlushResult, error) {
			require.EqualValues(t, 3, startTs)
			require.EqualValues(t, 7, endTs)
			require.True(t, config.Schema == schema)
			return &segments.FlushResult{
				ManifestPath:           "manifest",
				NumRows:                4,
				TimestampFrom:          100,
				TimestampTo:            200,
				ColumnGroupMemorySizes: map[int64]int64{100: 64},
				FieldNullCounts:        map[int64]int64{100: 1},
			}, nil
		})

	source := &delegatorGrowingFlushSource{segment: segment}
	result, err := source.FlushGrowingData(ctx, 3, 7, &syncmgr.GrowingFlushConfig{
		Schema: schema,
	})
	require.NoError(t, err)
	require.Equal(t, "manifest", result.ManifestPath)
	require.EqualValues(t, 4, result.NumRows)
	require.EqualValues(t, 100, result.TimestampFrom)
	require.EqualValues(t, 200, result.TimestampTo)
	require.EqualValues(t, 64, result.ColumnGroupMemorySizes[100])
	require.EqualValues(t, 1, result.FieldNullCounts[100])
}

func TestDelegatorGrowingSourceProviderCloseWaitsForSourceRelease(t *testing.T) {
	segmentManager := segments.NewMockSegmentManager(t)
	segment := segments.NewMockSegment(t)
	provider := newDelegatorGrowingSourceProvider(segmentManager, nil)

	segmentManager.EXPECT().GetGrowing(int64(1001)).Return(segment).Once()
	segment.EXPECT().PinIfNotReleased().Return(nil).Once()
	segment.EXPECT().Unpin().Once()

	source, state := provider.GetGrowingFlushSource(1001, nil)
	require.Equal(t, syncmgr.GrowingSourceUsable, state)
	require.NotNil(t, source)

	closeDone := make(chan struct{})
	go func() {
		provider.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
		t.Fatal("provider close should wait for active growing source lease")
	case <-time.After(50 * time.Millisecond):
	}

	source.Release()
	require.Eventually(t, func() bool {
		select {
		case <-closeDone:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	source, state = provider.GetGrowingFlushSource(1001, nil)
	require.Equal(t, syncmgr.GrowingSourceUnavailable, state)
	require.Nil(t, source)
}

func TestDelegatorGrowingSourceProviderRetainedSource(t *testing.T) {
	segmentManager := segments.NewMockSegmentManager(t)
	segment := segments.NewMockSegment(t)
	provider := newDelegatorGrowingSourceProvider(segmentManager, nil)

	segmentManager.EXPECT().GetGrowing(int64(1001)).Return(segment).Once()
	segment.EXPECT().PinIfNotReleased().Return(nil).Once()
	segment.EXPECT().MemSize().Return(int64(1024)).Once()

	err := provider.PrepareGrowingSourceReleaseHandoff(context.Background(), 0, []syncmgr.GrowingSourceReleaseHandoffSegment{
		{SegmentID: 1001, FlushThroughTs: 10},
	})
	require.NoError(t, err)

	segmentManager.EXPECT().GetGrowing(int64(1001)).Return(nil).Twice()
	segment.EXPECT().PinIfNotReleased().Return(nil).Twice()
	segment.EXPECT().Unpin().Times(3)
	segmentManager.EXPECT().ReleaseDetached(context.Background(), segment).Once()

	source, state := provider.GetGrowingFlushSource(1001, nil)
	require.Equal(t, syncmgr.GrowingSourceUsable, state)
	require.NotNil(t, source)
	source.Release()

	source, state = provider.GetGrowingFlushSource(1001, nil)
	require.Equal(t, syncmgr.GrowingSourceUsable, state)
	require.NotNil(t, source)
	source.Release()
	source.CommitGrowingFlush(10)
	segmentManager.AssertNotCalled(t, "ReleaseDetached", context.Background(), segment)
	provider.MarkReleaseDetached(1001)

	segmentManager.EXPECT().GetGrowing(int64(1001)).Return(nil).Once()
	source, state = provider.GetGrowingFlushSource(1001, nil)
	require.Equal(t, syncmgr.GrowingSourceUnavailable, state)
	require.Nil(t, source)
}

// The final flush's CommitGrowingFlush can land BEFORE the release handoff
// registers its retained entry: the caller reads the flush progress, drops the
// lock, and only then registers. A commit lost in that window used to leave
// the entry's committedFlushedTs at zero forever — the segment pin, provider
// registration and on-releasing state never released. The provider now records
// the fence independently of the retained map and registration inherits it.
func TestDelegatorGrowingSourceCommitBeforeRegisterReleases(t *testing.T) {
	segmentManager := segments.NewMockSegmentManager(t)
	segment := segments.NewMockSegment(t)
	provider := newDelegatorGrowingSourceProvider(segmentManager, nil)

	// Commit arrives first: no retained entry exists yet.
	provider.releaseRetainedIfComplete(1001, 10)

	segmentManager.EXPECT().GetGrowing(int64(1001)).Return(segment).Once()
	segment.EXPECT().PinIfNotReleased().Return(nil).Once()
	segment.EXPECT().MemSize().Return(int64(1024)).Once()
	err := provider.PrepareGrowingSourceReleaseHandoff(context.Background(), 0, []syncmgr.GrowingSourceReleaseHandoffSegment{
		{SegmentID: 1001, FlushThroughTs: 10},
	})
	require.NoError(t, err)

	// Detach must release immediately: the fence was already committed, and no
	// later commit will ever arrive to unlock this entry.
	segment.EXPECT().Unpin().Once()
	segmentManager.EXPECT().ReleaseDetached(context.Background(), segment).Once()
	provider.MarkReleaseDetached(1001)

	segmentManager.EXPECT().GetGrowing(int64(1001)).Return(nil).Once()
	source, state := provider.GetGrowingFlushSource(1001, nil)
	require.Equal(t, syncmgr.GrowingSourceUnavailable, state)
	require.Nil(t, source)
}

// The provider no longer decides readiness: resolving only pins the segment,
// and how far it may be flushed is a timestamp question the caller asks through
// TSafe. This is a raw read of the delegator's watermark, never
// shardDelegator.waitTSafe, whose external-table and DowngradeTsafe branches
// report success without the watermark having advanced.
func TestDelegatorGrowingFlushSourceReportsRawTSafe(t *testing.T) {
	segmentManager := segments.NewMockSegmentManager(t)
	segment := segments.NewMockSegment(t)
	currentTSafe := uint64(4242)
	provider := newDelegatorGrowingSourceProvider(segmentManager, nil, func() uint64 {
		return currentTSafe
	})

	segmentManager.EXPECT().GetGrowing(int64(1001)).Return(segment).Once()
	segment.EXPECT().PinIfNotReleased().Return(nil).Once()
	segment.EXPECT().Unpin().Once()

	source, state := provider.GetGrowingFlushSource(1001, nil)
	require.Equal(t, syncmgr.GrowingSourceUsable, state)
	require.NotNil(t, source)
	require.EqualValues(t, 4242, source.TSafe())

	currentTSafe = 9999
	require.EqualValues(t, 9999, source.TSafe())
	source.Release()
}

func TestDelegatorGrowingSourceProviderMissingSegmentPendingUntilTSafeCaughtUp(t *testing.T) {
	segmentManager := segments.NewMockSegmentManager(t)
	currentTSafe := uint64(100)
	provider := newDelegatorGrowingSourceProvider(segmentManager, nil, func() uint64 {
		return currentTSafe
	})

	segmentManager.EXPECT().GetGrowing(int64(1001)).Return(nil).Once()
	source, state := provider.GetGrowingFlushSource(1001, &msgpb.MsgPosition{Timestamp: 200})
	require.Equal(t, syncmgr.GrowingSourcePending, state)
	require.Nil(t, source)

	currentTSafe = 200
	segmentManager.EXPECT().GetGrowing(int64(1001)).Return(nil).Once()
	source, state = provider.GetGrowingFlushSource(1001, &msgpb.MsgPosition{Timestamp: 200})
	require.Equal(t, syncmgr.GrowingSourceUnavailable, state)
	require.Nil(t, source)

	currentTSafe = 100
	provider.Deactivate()
	segmentManager.EXPECT().GetGrowing(int64(1001)).Return(nil).Once()
	source, state = provider.GetGrowingFlushSource(1001, &msgpb.MsgPosition{Timestamp: 200})
	require.Equal(t, syncmgr.GrowingSourceUnavailable, state)
	require.Nil(t, source)
}

func TestDelegatorGrowingSourceProviderPrepareWaitsFence(t *testing.T) {
	segmentManager := segments.NewMockSegmentManager(t)
	segment := segments.NewMockSegment(t)
	waitedFence := uint64(0)
	provider := newDelegatorGrowingSourceProvider(segmentManager, func(ctx context.Context, fenceTs uint64) error {
		waitedFence = fenceTs
		return nil
	})

	segmentManager.EXPECT().GetGrowing(int64(1001)).Return(segment).Once()
	segment.EXPECT().PinIfNotReleased().Return(nil).Once()
	segment.EXPECT().MemSize().Return(int64(1024)).Once()

	err := provider.PrepareGrowingSourceReleaseHandoff(context.Background(), 200, []syncmgr.GrowingSourceReleaseHandoffSegment{
		{SegmentID: 1001, FlushThroughTs: 10},
		{SegmentID: 1002},
	})
	require.NoError(t, err)
	require.EqualValues(t, 200, waitedFence)
	require.True(t, provider.IsReleasePrepared(1001, 0))
	require.True(t, provider.IsReleaseAllowed(1001, 0))
	require.False(t, provider.IsReleasePrepared(1002, 200))
	require.True(t, provider.IsReleaseAllowed(1002, 200))
	require.False(t, provider.IsReleaseAllowed(1002, 201))
	require.False(t, provider.IsReleasePrepared(1003, 200))
	require.False(t, provider.IsReleaseAllowed(1003, 200))

	segment.EXPECT().Unpin().Once()
	segmentManager.EXPECT().ReleaseDetached(context.Background(), segment).Once()
	provider.releaseRetainedIfComplete(1001, 10)
	segmentManager.AssertNotCalled(t, "ReleaseDetached", context.Background(), segment)
	provider.MarkReleaseDetached(1001)
}

func TestDelegatorGrowingSourceProviderHandoffOnlyRejectsNewSegmentsBeforeFence(t *testing.T) {
	segmentManager := segments.NewMockSegmentManager(t)
	segment := segments.NewMockSegment(t)
	waitFenceEntered := make(chan struct{})
	releaseFence := make(chan struct{})
	provider := newDelegatorGrowingSourceProvider(segmentManager, func(ctx context.Context, fenceTs uint64) error {
		close(waitFenceEntered)
		<-releaseFence
		return nil
	})

	prepareDone := make(chan error, 1)
	go func() {
		prepareDone <- provider.PrepareGrowingSourceReleaseHandoff(context.Background(), 200, []syncmgr.GrowingSourceReleaseHandoffSegment{
			{SegmentID: 1001, FlushThroughTs: 10},
		})
	}()

	select {
	case <-waitFenceEntered:
	case <-time.After(time.Second):
		t.Fatal("prepare did not enter fence wait")
	}

	source, state := provider.GetGrowingFlushSource(1002, nil)
	require.Equal(t, syncmgr.GrowingSourceUnavailable, state)
	require.Nil(t, source)

	segmentManager.EXPECT().GetGrowing(int64(1001)).Return(segment).Once()
	segment.EXPECT().PinIfNotReleased().Return(nil).Once()
	segment.EXPECT().MemSize().Return(int64(1024)).Once()
	close(releaseFence)
	require.NoError(t, <-prepareDone)

	segment.EXPECT().Unpin().Once()
	provider.Close()
}

func TestDelegatorGrowingSourceProviderDeactivatedOnlyServesRetainedSources(t *testing.T) {
	segmentManager := segments.NewMockSegmentManager(t)
	segment := segments.NewMockSegment(t)
	waitCount := 0
	provider := newDelegatorGrowingSourceProvider(segmentManager, func(ctx context.Context, fenceTs uint64) error {
		waitCount++
		return nil
	})

	segmentManager.EXPECT().GetGrowing(int64(1001)).Return(segment).Once()
	segment.EXPECT().PinIfNotReleased().Return(nil).Once()
	segment.EXPECT().MemSize().Return(int64(1024)).Once()

	err := provider.PrepareGrowingSourceReleaseHandoff(context.Background(), 100, []syncmgr.GrowingSourceReleaseHandoffSegment{
		{SegmentID: 1001, FlushThroughTs: 10},
	})
	require.NoError(t, err)
	require.Equal(t, 1, waitCount)

	provider.Deactivate()

	err = provider.PrepareGrowingSourceReleaseHandoff(context.Background(), 200, []syncmgr.GrowingSourceReleaseHandoffSegment{
		{SegmentID: 1002, FlushThroughTs: 5},
	})
	require.NoError(t, err)
	require.Equal(t, 1, waitCount)
	require.False(t, provider.IsReleaseAllowed(1002, 200))
	require.False(t, provider.IsReleasePrepared(1002, 200))
	require.True(t, provider.IsReleaseAllowed(1001, 100))
	require.True(t, provider.IsReleasePrepared(1001, 100))

	err = provider.PrepareGrowingSourceReleaseHandoff(context.Background(), 200, []syncmgr.GrowingSourceReleaseHandoffSegment{
		{SegmentID: 1001, FlushThroughTs: 11},
	})
	require.NoError(t, err)
	require.False(t, provider.IsReleasePrepared(1001, 200))

	segment.EXPECT().Unpin().Once()
	segmentManager.EXPECT().ReleaseDetached(context.Background(), segment).Once()
	provider.releaseRetainedIfComplete(1001, 10)
	segmentManager.AssertNotCalled(t, "ReleaseDetached", context.Background(), segment)
	provider.MarkReleaseDetached(1001)
}

func TestDelegatorGrowingSourceProviderReleasesWhenDetachedBeforeCommit(t *testing.T) {
	segmentManager := segments.NewMockSegmentManager(t)
	segment := segments.NewMockSegment(t)
	provider := newDelegatorGrowingSourceProvider(segmentManager, nil)

	segmentManager.EXPECT().GetGrowing(int64(1001)).Return(segment).Once()
	segment.EXPECT().PinIfNotReleased().Return(nil).Once()
	segment.EXPECT().MemSize().Return(int64(1024)).Once()

	err := provider.PrepareGrowingSourceReleaseHandoff(context.Background(), 0, []syncmgr.GrowingSourceReleaseHandoffSegment{
		{SegmentID: 1001, FlushThroughTs: 10},
	})
	require.NoError(t, err)

	provider.MarkReleaseDetached(1001)
	segmentManager.AssertNotCalled(t, "ReleaseDetached", context.Background(), segment)

	segment.EXPECT().Unpin().Once()
	segmentManager.EXPECT().ReleaseDetached(context.Background(), segment).Once()
	provider.releaseRetainedIfComplete(1001, 10)
}

func TestDelegatorGrowingSourceProviderPrepareRollbackOnFailure(t *testing.T) {
	segmentManager := segments.NewMockSegmentManager(t)
	segment := segments.NewMockSegment(t)
	failingSegment := segments.NewMockSegment(t)
	provider := newDelegatorGrowingSourceProvider(segmentManager, nil)

	segmentManager.EXPECT().GetGrowing(int64(1001)).Return(segment).Once()
	segment.EXPECT().PinIfNotReleased().Return(nil).Once()
	segment.EXPECT().MemSize().Return(int64(1024)).Once()
	// The second segment cannot be pinned, so the whole prepare rolls back and
	// the first segment's retention is undone too.
	segmentManager.EXPECT().GetGrowing(int64(1002)).Return(failingSegment).Once()
	failingSegment.EXPECT().PinIfNotReleased().Return(merr.WrapErrSegmentNotLoaded(1002, "released")).Once()
	segment.EXPECT().Unpin().Once()

	err := provider.PrepareGrowingSourceReleaseHandoff(context.Background(), 200, []syncmgr.GrowingSourceReleaseHandoffSegment{
		{SegmentID: 1001, FlushThroughTs: 10},
		{SegmentID: 1002, FlushThroughTs: 10},
	})
	require.Error(t, err)

	segmentManager.EXPECT().GetGrowing(int64(1001)).Return(nil).Once()
	source, state := provider.GetGrowingFlushSource(1001, nil)
	require.Equal(t, syncmgr.GrowingSourceUnavailable, state)
	require.Nil(t, source)
}

func TestDelegatorGrowingSourceProviderReleaseAllowedWhenSegmentNotFound(t *testing.T) {
	segmentManager := segments.NewMockSegmentManager(t)
	provider := newDelegatorGrowingSourceProvider(segmentManager, nil)

	segmentManager.EXPECT().GetGrowing(int64(1001)).Return(nil).Once()

	err := provider.PrepareGrowingSourceReleaseHandoff(context.Background(), 0, []syncmgr.GrowingSourceReleaseHandoffSegment{
		{SegmentID: 1001, FlushThroughTs: 10},
	})
	require.NoError(t, err)
	require.True(t, provider.IsReleaseAllowed(1001, 0))
	require.False(t, provider.IsReleasePrepared(1001, 0))

	segmentManager.EXPECT().GetGrowing(int64(1001)).Return(nil).Once()
	source, state := provider.GetGrowingFlushSource(1001, nil)
	require.Equal(t, syncmgr.GrowingSourceUnavailable, state)
	require.Nil(t, source)
}

func TestDelegatorGrowingSourceProviderPrepareMixedRetainedAndMissing(t *testing.T) {
	segmentManager := segments.NewMockSegmentManager(t)
	segment := segments.NewMockSegment(t)
	provider := newDelegatorGrowingSourceProvider(segmentManager, nil)

	segmentManager.EXPECT().GetGrowing(int64(1001)).Return(segment).Once()
	segment.EXPECT().PinIfNotReleased().Return(nil).Once()
	segment.EXPECT().MemSize().Return(int64(1024)).Once()
	segmentManager.EXPECT().GetGrowing(int64(1002)).Return(nil).Once()

	err := provider.PrepareGrowingSourceReleaseHandoff(context.Background(), 200, []syncmgr.GrowingSourceReleaseHandoffSegment{
		{SegmentID: 1001, FlushThroughTs: 10},
		{SegmentID: 1002, FlushThroughTs: 10},
	})
	require.NoError(t, err)
	require.True(t, provider.IsReleaseAllowed(1001, 200))
	require.True(t, provider.IsReleasePrepared(1001, 200))
	require.True(t, provider.IsReleaseAllowed(1002, 200))
	require.False(t, provider.IsReleasePrepared(1002, 200))

	segment.EXPECT().Unpin().Once()
	segmentManager.EXPECT().ReleaseDetached(context.Background(), segment).Once()
	provider.MarkReleaseDetached(1001)
	provider.releaseRetainedIfComplete(1001, 10)
}

// Retention is refused when the delegator has not consumed the fence: retaining
// then could not serve the flush the fence was issued for. The watermark is per
// channel, so this is a property of the provider, not of one segment.
func TestDelegatorGrowingSourceProviderRegisterRetainedBehindTarget(t *testing.T) {
	segmentManager := segments.NewMockSegmentManager(t)
	segment := segments.NewMockSegment(t)
	provider := newDelegatorGrowingSourceProvider(segmentManager, nil, func() uint64 {
		return 5 // behind the fence of 10 below
	})

	segmentManager.EXPECT().GetGrowing(int64(1001)).Return(segment).Once()
	segment.EXPECT().PinIfNotReleased().Return(nil).Once()
	segment.EXPECT().Unpin().Once()

	err := provider.PrepareGrowingSourceReleaseHandoff(context.Background(), 0, []syncmgr.GrowingSourceReleaseHandoffSegment{
		{SegmentID: 1001, FlushThroughTs: 10},
	})
	require.Error(t, err)
}

func TestDelegatorGrowingSourceProviderRetainedMetrics(t *testing.T) {
	metrics.QueryNodeGrowingSourceRetainedBytes.Reset()
	metrics.QueryNodeGrowingSourceRetainedSegments.Reset()
	t.Cleanup(func() {
		metrics.QueryNodeGrowingSourceRetainedBytes.Reset()
		metrics.QueryNodeGrowingSourceRetainedSegments.Reset()
	})
	paramtable.SetNodeID(1)

	const channel = "by-dev-rootcoord-dml_0_100v0"
	segmentManager := segments.NewMockSegmentManager(t)
	segment := segments.NewMockSegment(t)
	provider := newDelegatorGrowingSourceProvider(segmentManager, nil)
	provider.SetChannelName(channel)
	requireRetainedMetricCount(t, 0)

	segmentManager.EXPECT().GetGrowing(int64(1001)).Return(segment).Once()
	segment.EXPECT().PinIfNotReleased().Return(nil).Once()
	segment.EXPECT().MemSize().Return(int64(4096)).Once()

	err := provider.PrepareGrowingSourceReleaseHandoff(context.Background(), 0, []syncmgr.GrowingSourceReleaseHandoffSegment{
		{SegmentID: 1001, FlushThroughTs: 10},
	})
	require.NoError(t, err)
	requireRetainedMetricCount(t, 1)
	require.Equal(t, float64(4096), testutil.ToFloat64(metrics.QueryNodeGrowingSourceRetainedBytes.WithLabelValues("1", channel)))
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.QueryNodeGrowingSourceRetainedSegments.WithLabelValues("1", channel)))

	provider.releaseRetainedIfComplete(1001, 10)
	require.Equal(t, float64(4096), testutil.ToFloat64(metrics.QueryNodeGrowingSourceRetainedBytes.WithLabelValues("1", channel)))
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.QueryNodeGrowingSourceRetainedSegments.WithLabelValues("1", channel)))
	requireRetainedMetricCount(t, 1)

	provider.Deactivate()
	requireRetainedMetricCount(t, 1)

	segment.EXPECT().Unpin().Once()
	segmentManager.EXPECT().ReleaseDetached(context.Background(), segment).Once()
	provider.MarkReleaseDetached(1001)
	requireRetainedMetricCount(t, 0)
}

func TestDelegatorGrowingSourceProviderRetainedMetricsDeletedOnDeactivateWithoutRetained(t *testing.T) {
	metrics.QueryNodeGrowingSourceRetainedBytes.Reset()
	metrics.QueryNodeGrowingSourceRetainedSegments.Reset()
	t.Cleanup(func() {
		metrics.QueryNodeGrowingSourceRetainedBytes.Reset()
		metrics.QueryNodeGrowingSourceRetainedSegments.Reset()
	})
	paramtable.SetNodeID(1)

	provider := newDelegatorGrowingSourceProvider(segments.NewMockSegmentManager(t), nil)
	provider.SetChannelName("by-dev-rootcoord-dml_0_101v0")
	requireRetainedMetricCount(t, 0)

	provider.Deactivate()
	requireRetainedMetricCount(t, 0)
}

func TestDelegatorGrowingSourceProviderRetainedMetricsDeletedWhenRetainedDrains(t *testing.T) {
	metrics.QueryNodeGrowingSourceRetainedBytes.Reset()
	metrics.QueryNodeGrowingSourceRetainedSegments.Reset()
	t.Cleanup(func() {
		metrics.QueryNodeGrowingSourceRetainedBytes.Reset()
		metrics.QueryNodeGrowingSourceRetainedSegments.Reset()
	})
	paramtable.SetNodeID(1)

	const channel = "by-dev-rootcoord-dml_0_102v0"
	segmentManager := segments.NewMockSegmentManager(t)
	segment := segments.NewMockSegment(t)
	provider := newDelegatorGrowingSourceProvider(segmentManager, nil)
	provider.SetChannelName(channel)
	requireRetainedMetricCount(t, 0)

	segmentManager.EXPECT().GetGrowing(int64(1001)).Return(segment).Once()
	segment.EXPECT().PinIfNotReleased().Return(nil).Once()
	segment.EXPECT().MemSize().Return(int64(2048)).Once()

	err := provider.PrepareGrowingSourceReleaseHandoff(context.Background(), 0, []syncmgr.GrowingSourceReleaseHandoffSegment{
		{SegmentID: 1001, FlushThroughTs: 10},
	})
	require.NoError(t, err)
	requireRetainedMetricCount(t, 1)

	segment.EXPECT().Unpin().Once()
	segmentManager.EXPECT().ReleaseDetached(context.Background(), segment).Once()
	provider.releaseRetainedIfComplete(1001, 10)
	requireRetainedMetricCount(t, 1)
	provider.MarkReleaseDetached(1001)
	requireRetainedMetricCount(t, 0)
}

func requireRetainedMetricCount(t *testing.T, expected int) {
	t.Helper()
	require.Equal(t, expected, testutil.CollectAndCount(metrics.QueryNodeGrowingSourceRetainedBytes))
	require.Equal(t, expected, testutil.CollectAndCount(metrics.QueryNodeGrowingSourceRetainedSegments))
}

// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package querynodev2

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/registry"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// prepareFn mirrors WALAccesser.Local().PrepareReleaseManualFlushIfLocal.
type prepareFn func(ctx context.Context, collectionID int64, vchannel string, segmentIDs []int64) error

// newStopDrainWAL builds the same WAL mock shape the service suite uses, with a
// caller-supplied PrepareReleaseManualFlushIfLocal body so a test can observe
// the node state at the moment the drain runs.
func newStopDrainWAL(t *testing.T, prepare prepareFn) *mock_streaming.MockWALAccesser {
	wal := mock_streaming.NewMockWALAccesser(t)
	local := mock_streaming.NewMockLocal(t)
	local.EXPECT().GetLatestMVCCTimestampIfLocal(mock.Anything, mock.Anything).Return(0, nil).Maybe()
	local.EXPECT().PrepareReleaseManualFlushIfLocal(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(prepare).Maybe()
	local.EXPECT().GetMetricsIfLocal(mock.Anything).Return(&types.StreamingNodeMetrics{}, nil).Maybe()
	wal.EXPECT().Local().Return(local).Maybe()
	scanner := mock_streaming.NewMockScanner(t)
	scanner.EXPECT().Done().Return(make(chan struct{})).Maybe()
	scanner.EXPECT().Error().Return(nil).Maybe()
	scanner.EXPECT().Close().Return().Maybe()
	wal.EXPECT().Read(mock.Anything, mock.Anything).Return(scanner).Maybe()
	return wal
}

// stopDrainStubProvider marks the suite channel as growing-source enabled in
// the process-global registry, the way an activated delegator provider would.
type stopDrainStubProvider struct{}

func (stopDrainStubProvider) GetGrowingFlushSource(int64, *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
	return nil, syncmgr.GrowingSourcePending
}

// registerGrowingSourceProvider publishes a stub provider for the suite channel.
// A registered provider is what makes growing-source debt POSSIBLE on the
// channel — the Stop drain keys off it, not off the local segment snapshot.
func (suite *ServiceSuite) registerGrowingSourceProvider() {
	registration := syncmgr.DefaultGrowingSourceRegistry().Register(suite.vchannel, stopDrainStubProvider{})
	suite.T().Cleanup(func() { syncmgr.DefaultGrowingSourceRegistry().Unregister(registration) })
}

// putGrowingSegment installs a real growing segment on the suite channel.
func (suite *ServiceSuite) putGrowingSegment(ctx context.Context, segmentID int64) {
	collection := suite.node.manager.Collection.Get(suite.collectionID)
	suite.Require().NotNil(collection)
	segment, err := segments.NewSegment(ctx, collection, suite.node.manager.Segment,
		segments.SegmentTypeGrowing, 0, &querypb.SegmentLoadInfo{
			SegmentID:     segmentID,
			PartitionID:   suite.partitionIDs[0],
			CollectionID:  suite.collectionID,
			InsertChannel: suite.vchannel,
			Level:         datapb.SegmentLevel_Legacy,
		})
	suite.Require().NoError(err)
	suite.node.manager.Segment.Put(ctx, segments.SegmentTypeGrowing, segment)
}

// shortenGracefulStop makes Stop's redundant internal migrate budget expire
// immediately, which is exactly the fallback shape this drain exists for.
func (suite *ServiceSuite) shortenGracefulStop() {
	key := paramtable.Get().QueryNodeCfg.GracefulStopTimeout.Key
	paramtable.Get().Save(key, "1")
	suite.T().Cleanup(func() { paramtable.Get().Reset(key) })
}

// The drain must run while the pipeline is still consuming and the delegator
// (and with it the growing-source provider) is still registered. Once the
// pipeline closes, TSafe freezes and the growing source answers Pending
// forever, so a drain placed after it could never succeed.
func (suite *ServiceSuite) TestStopDrainsGrowingSourceFlushBeforeDestroy() {
	ctx := context.Background()
	suite.TestWatchDmChannelsInt64()
	suite.registerGrowingSourceProvider()
	suite.putGrowingSegment(ctx, 990001)
	suite.shortenGracefulStop()

	var (
		calls          int
		hadDelegator   bool
		hadPipeline    bool
		gotChannel     string
		gotCollection  int64
		gotSegmentIDs  []int64
		hadSegment     bool
		hadDeadlineSet bool
	)
	wal := newStopDrainWAL(suite.T(), func(callCtx context.Context, collectionID int64, vchannel string, segmentIDs []int64) error {
		calls++
		_, hadDelegator = suite.node.delegators.Get(vchannel)
		hadPipeline = suite.node.pipelineManager.Get(vchannel) != nil
		hadSegment = suite.node.manager.Segment.GetGrowing(990001) != nil
		_, hadDeadlineSet = callCtx.Deadline()
		gotChannel, gotCollection, gotSegmentIDs = vchannel, collectionID, segmentIDs
		return nil
	})
	streaming.SetWALForTest(wal)
	defer streaming.SetWALForTest(newServiceSuiteWAL(suite.T(), nil))

	suite.NoError(suite.node.Stop())

	suite.Equal(1, calls, "the drain must run exactly once for the channel")
	suite.True(hadDelegator, "the drain must run before the delegators are closed (delegator.Close is what unregisters the growing-source provider)")
	// pipelineManager.Close() closes the pipelines but leaves them in its map,
	// so Num() cannot witness closure; the delegator and the growing segment
	// still being there is the observable form of "nothing destroyed yet".
	suite.True(hadPipeline, "the channel pipeline must still be present")
	suite.True(hadSegment, "the drain must run before the segment manager is cleared")
	suite.True(hadDeadlineSet, "the drain must be bounded")
	suite.Equal(suite.vchannel, gotChannel)
	suite.Equal(suite.collectionID, gotCollection)
	suite.ElementsMatch([]int64{990001}, gotSegmentIDs)

	// destroy still happened
	_, ok := suite.node.delegators.Get(suite.vchannel)
	suite.False(ok)
	suite.Nil(suite.node.manager.Segment.GetGrowing(990001))
}

// A drain that never returns must not hang Stop: it is bounded, and the destroy
// path runs unchanged afterwards.
func (suite *ServiceSuite) TestStopDrainGrowingSourceFlushTimeout() {
	ctx := context.Background()
	suite.TestWatchDmChannelsInt64()
	suite.registerGrowingSourceProvider()
	suite.putGrowingSegment(ctx, 990002)
	suite.shortenGracefulStop()

	var mu sync.Mutex
	calls := 0
	wal := newStopDrainWAL(suite.T(), func(callCtx context.Context, _ int64, _ string, _ []int64) error {
		mu.Lock()
		calls++
		mu.Unlock()
		<-callCtx.Done()
		return callCtx.Err()
	})
	streaming.SetWALForTest(wal)
	defer streaming.SetWALForTest(newServiceSuiteWAL(suite.T(), nil))

	start := time.Now()
	suite.NoError(suite.node.Stop(), "a failed drain must never fail Stop")
	elapsed := time.Since(start)

	suite.Equal(1, calls)
	suite.GreaterOrEqual(elapsed, minStopGrowingFlushDrainTimeout)
	suite.Less(elapsed, minStopGrowingFlushDrainTimeout+maxStopGrowingFlushDrainTimeout)

	_, ok := suite.node.delegators.Get(suite.vchannel)
	suite.False(ok)
	suite.Nil(suite.node.manager.Segment.GetGrowing(990002))
}

// No local streaming node: nothing local can owe a growing-source flush, so the
// drain is skipped without spending budget and without a loud warning.
func (suite *ServiceSuite) TestStopDrainGrowingSourceFlushStructurallyUnavailable() {
	ctx := context.Background()
	suite.TestWatchDmChannelsInt64()
	suite.registerGrowingSourceProvider()
	suite.putGrowingSegment(ctx, 990003)
	suite.shortenGracefulStop()

	calls := 0
	wal := newStopDrainWAL(suite.T(), func(_ context.Context, _ int64, _ string, _ []int64) error {
		calls++
		return registry.ErrNoStreamingNodeDeployed
	})
	streaming.SetWALForTest(wal)
	defer streaming.SetWALForTest(newServiceSuiteWAL(suite.T(), nil))

	start := time.Now()
	suite.NoError(suite.node.Stop())
	suite.Equal(1, calls)
	suite.Less(time.Since(start), minStopGrowingFlushDrainTimeout, "a structural unavailability must not spend the drain budget")

	_, ok := suite.node.delegators.Get(suite.vchannel)
	suite.False(ok)
	suite.Nil(suite.node.manager.Segment.GetGrowing(990003))
}

// A channel with a registered growing-source provider must be drained even when
// this node has ZERO materialized growing segments. The provider answers
// GrowingSourcePending for a segment the node has not materialized yet, and the
// write buffer treats Pending as "choose growing source" — stickily — so the
// DataNode may already own ref-payload debt the local snapshot cannot see.
// Skipping the drain here would close the pipeline, freeze TSafe, and leave
// that debt waiting for a restart.
func (suite *ServiceSuite) TestStopDrainsPendingOnlyChannelWithoutGrowingSegments() {
	suite.TestWatchDmChannelsInt64()
	suite.registerGrowingSourceProvider()
	suite.shortenGracefulStop()

	calls := 0
	var gotSegmentIDs []int64
	gotChannel := ""
	wal := newStopDrainWAL(suite.T(), func(_ context.Context, _ int64, vchannel string, segmentIDs []int64) error {
		calls++
		gotChannel, gotSegmentIDs = vchannel, segmentIDs
		return nil
	})
	streaming.SetWALForTest(wal)
	defer streaming.SetWALForTest(newServiceSuiteWAL(suite.T(), nil))

	suite.NoError(suite.node.Stop())
	suite.Equal(1, calls, "a provider-bearing channel must be drained even with no materialized growing segment")
	suite.Equal(suite.vchannel, gotChannel)
	suite.Empty(gotSegmentIDs, "an empty snapshot is fine — the preparer's drain unions all tracked ref payloads of the channel")
	_, ok := suite.node.delegators.Get(suite.vchannel)
	suite.False(ok)
}

// Without a registered provider no local write buffer ever chose growing source
// on the channel, so nothing can be owed: no drain call, no budget spent — even
// when local growing segments exist.
func (suite *ServiceSuite) TestStopSkipsDrainWithoutProvider() {
	ctx := context.Background()
	suite.TestWatchDmChannelsInt64()
	suite.putGrowingSegment(ctx, 990004)
	suite.shortenGracefulStop()

	calls := 0
	wal := newStopDrainWAL(suite.T(), func(_ context.Context, _ int64, _ string, _ []int64) error {
		calls++
		return nil
	})
	streaming.SetWALForTest(wal)
	defer streaming.SetWALForTest(newServiceSuiteWAL(suite.T(), nil))

	suite.NoError(suite.node.Stop())
	suite.Zero(calls, "no growing-source provider means no drain and no budget spent")
	_, ok := suite.node.delegators.Get(suite.vchannel)
	suite.False(ok)
}

func TestStopGrowingSourceFlushDrainTimeout(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().QueryNodeCfg.GracefulStopTimeout.Key

	tests := []struct {
		name        string
		gracefulSec string
		elapsed     time.Duration
		expect      time.Duration
	}{
		{
			name:        "budget exhausted falls back to the floor",
			gracefulSec: "10",
			elapsed:     10 * time.Second,
			expect:      minStopGrowingFlushDrainTimeout,
		},
		{
			name:        "huge default is capped",
			gracefulSec: "1800",
			elapsed:     0,
			expect:      maxStopGrowingFlushDrainTimeout,
		},
		{
			name:        "remaining budget is used when it fits",
			gracefulSec: "40",
			elapsed:     10 * time.Second,
			expect:      30 * time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			paramtable.Get().Save(key, tt.gracefulSec)
			defer paramtable.Get().Reset(key)

			got := stopGrowingSourceFlushDrainTimeout(time.Now().Add(-tt.elapsed))
			// time.Since drift: allow a small slack on the un-clamped case.
			if got > tt.expect || tt.expect-got > time.Second {
				t.Fatalf("expect ~%v, got %v", tt.expect, got)
			}
		})
	}
}

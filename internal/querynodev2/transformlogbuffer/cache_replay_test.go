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

package transformlogbuffer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

type (
	boundedSummaryStreams struct{ m *walsummary.Manager }
	boundedSummaryStream  struct{ *walsummary.DispatchStream }
)

func (m boundedSummaryStreams) AcquireStream(context.Context, string) (wal.TransformLogStream, error) {
	return boundedSummaryStream{walsummary.NewDispatchStream(m.m)}, nil
}

func (s boundedSummaryStream) Subscribe(ctx context.Context, opt wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
	if opt.EndTimeTick == 0 {
		ctx = context.WithoutCancel(ctx)
	}
	return s.DispatchStream.Subscribe(ctx, opt)
}

func observeCacheDelete(t *testing.T, m *walsummary.Manager, tick uint64) {
	t.Helper()
	msg := message.NewDeleteMessageBuilderV1().WithVChannel("p_1v0").WithHeader(&message.DeleteMessageHeader{CollectionId: 1, Rows: 1}).WithBody(&msgpb.DeleteRequest{CollectionID: 1, PartitionID: 1, PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{int64(tick)}}}}, Timestamps: []uint64{tick}}).MustBuildMutable().WithTimeTick(tick).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(int64(tick)))
	m.ObserveMessage(t.Context(), msg)
}

func TestBoundedCacheReplaysEvictedHistoryAndLiveSuffix(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	m := walsummary.NewManager(walsummary.ManagerConfig{})
	buffer := New(boundedSummaryStreams{m}, 1)
	buffer.SetCacheLimits(2048, 1024)
	defer buffer.Close()
	guard, err := buffer.Acquire(ctx, newTestQueryView("p_1v0", 0))
	require.NoError(t, err)
	defer guard.Release()
	for tick := uint64(1); tick <= 600; tick++ {
		observeCacheDelete(t, m, tick)
	}
	m.ObserveQueryBarrier("p_1v0", 601)
	require.NoError(t, guard.WaitTransformVisible(ctx, 601))
	require.LessOrEqual(t, buffer.CachedBytes(), int64(2048))
	segment := &fakeSegment{id: 1, vchannel: "p_1v0"}
	reg, err := buffer.RegisterSegment(ctx, segment)
	require.NoError(t, err)
	defer reg.Unregister()
	require.NoError(t, reg.WaitCatchup(ctx))
	require.NoError(t, reg.WaitTransformVisible(ctx, 601))
	require.Len(t, segment.applied, 600)
	observeCacheDelete(t, m, 602)
	require.NoError(t, reg.WaitTransformVisible(ctx, 602))
	require.Equal(t, uint64(602), segment.AppliedTransformTimeTick())
}

func TestBoundedCacheProgressOnlyGapDoesNotLoop(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	m := walsummary.NewManager(walsummary.ManagerConfig{})
	m.ObserveQueryBarrier("p_1v0", 100)
	b := New(boundedSummaryStreams{m}, 1)
	defer b.Close()
	guard, err := b.Acquire(ctx, newTestQueryView("p_1v0", 0))
	require.NoError(t, err)
	defer guard.Release()
	require.NoError(t, guard.WaitTransformVisible(ctx, 100))
	b.channels["p_1v0"].mu.Lock()
	b.channels["p_1v0"].cacheStart = 100
	b.channels["p_1v0"].mu.Unlock()
	reg, err := b.RegisterSegment(ctx, &fakeSegment{id: 2, vchannel: "p_1v0"})
	require.NoError(t, err)
	defer reg.Unregister()
	require.NoError(t, reg.WaitCatchup(ctx))
	require.NoError(t, reg.WaitTransformVisible(ctx, 100))
}

func TestBoundedCacheOlderBaselineUsesSummaryInsteadOfRAMFloor(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	m := walsummary.NewManager(walsummary.ManagerConfig{})
	for tick := uint64(1); tick <= 20; tick++ {
		observeCacheDelete(t, m, tick)
	}
	b := New(boundedSummaryStreams{m}, 1)
	defer b.Close()
	newer, err := b.Acquire(ctx, newTestQueryView("p_1v0", 15))
	require.NoError(t, err)
	defer newer.Release()
	require.NoError(t, newer.WaitTransformVisible(ctx, 20))
	older, err := b.Bind(newTestQueryView("p_1v0", 0))
	require.NoError(t, err)
	defer older.Release()
	segment := &fakeSegment{id: 3, vchannel: "p_1v0"}
	reg, err := b.RegisterSegment(ctx, segment)
	require.NoError(t, err)
	defer reg.Unregister()
	require.NoError(t, reg.WaitCatchup(ctx))
	require.Len(t, segment.applied, 20)
}

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

package walsummary

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestTransformStatisticsAcrossStorageAndGC(t *testing.T) {
	ctx := context.Background()
	m, store := newTransformTestManagerWithStore(t)
	m.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 100, 10, 1, 2))
	require.NoError(t, persistSummary(ctx, m))
	m.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 200, 10, 3, 4, 5))
	sealed := m.seal()
	m.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 300, 10, 6))
	observeReadBarrier(m, 400)
	want := m.TransformStats("v1", 0, math.MaxUint64)
	require.Equal(t, want.Bytes, want.UpperBytes)
	require.Equal(t, uint64(100), want.FirstTimeTick)
	require.Equal(t, uint64(300), want.LastTimeTick)
	batch, err := m.ReadTransform(ctx, "v1", 0, 400, ReadLimits{})
	require.NoError(t, err)
	var bytes uint64
	for _, entry := range batch.Entries {
		bytes += uint64(proto.Size(entry))
	}
	require.Equal(t, bytes, want.Bytes)
	require.Equal(t, uint64(proto.Size(batch.Entries[1])), m.TransformStats("v1", 100, 250).Bytes)
	require.Equal(t, TransformStats{}, m.TransformStats("v1", 200, 100))
	require.Equal(t, TransformStats{}, m.TransformStats("absent", 0, 400))
	require.NoError(t, m.writeChunk(ctx, sealed))
	require.Equal(t, want, m.TransformStats("v1", 0, 400))
	require.NoError(t, persistSummary(ctx, m))
	restored := newTestManager(t, store, 1<<30)
	require.NoError(t, restored.Restore(ctx))
	require.Equal(t, want, restored.TransformStats("v1", 0, 400))
	// Restored records are skipped on replay, including their statistics.
	restored.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 300, 10, 6))
	require.Equal(t, want, restored.TransformStats("v1", 0, 400))
	restored.cfg.RetentionMaxBytes = 1
	restored.AdvanceGCTimeTick("v1", 100)
	require.NoError(t, gcSummary(ctx, restored))
	require.Equal(t, want.Bytes-uint64(proto.Size(batch.Entries[0])), restored.TransformStats("v1", 100, 400).Bytes)
	restored.AdvanceGCTimeTick("v1", 400)
	require.NoError(t, gcSummary(ctx, restored))
	require.Equal(t, TransformStats{}, restored.TransformStats("v1", 400, 500))
	restored.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 500, 10, 7))
	require.Positive(t, restored.TransformStats("v1", 400, 500).Bytes)
}

func TestMaterializationBacklogSurvivesPersistenceAndRestart(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	first := tsoutil.ComposeTSByTime(now.Add(-2 * time.Minute))
	last := tsoutil.ComposeTSByTime(now.Add(-time.Second))
	m, store := newTransformTestManagerWithStore(t)
	requests := map[string]uint64{}
	request := func(vc string, tt uint64) { requests[vc] = tt }
	m.cfg.RequestMaterialization = request
	m.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", first, 10, 1))
	m.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", last, 10, 2))
	m.requestMaterializationBacklog(context.Background(), now, 3*time.Minute)
	require.Empty(t, requests)
	m.requestMaterializationBacklog(context.Background(), now, time.Minute)
	require.Equal(t, last, requests["v1"])
	clear(requests)
	require.NoError(t, persistSummary(ctx, m))
	require.Empty(t, requests, "upload does not itself request materialization")
	require.Empty(t, m.pending)
	restored := newTestManager(t, store, 1<<30)
	restored.cfg.RequestMaterialization = request
	require.NoError(t, restored.Restore(ctx))
	restored.requestMaterializationBacklog(context.Background(), now, time.Minute)
	require.Equal(t, last, requests["v1"], "durable backlog retains its original age")
	clear(requests)
	// Output completion suppresses new requests, but does not authorize GC.
	restored.ReportMaterialized("v1", last)
	restored.ReportMaterialized("v1", first)
	restored.requestMaterializationBacklog(context.Background(), now, time.Minute)
	require.Empty(t, requests)
	restored.cfg.RetentionMaxBytes = 1
	require.NoError(t, gcSummary(ctx, restored))
	require.Len(t, restored.Manifest().GetChunks(), 1)
	restored.RestoreTransformGCTimeTicks(map[string]*streamingpb.VChannelMeta{"v1": {TransformMaterializedTimeTick: last}})
	require.NoError(t, gcSummary(ctx, restored))
	require.Empty(t, restored.Manifest().GetChunks())
}

func TestRetentionBacklogRequestsOnlyOldestBlockingChunk(t *testing.T) {
	m, _ := newTransformTestManagerWithStore(t)
	var unused bool
	flushTransform(t, m, "v1", 100, &unused)
	flushTransform(t, m, "v1", 200, &unused)
	requests := map[string]uint64{}
	m.cfg.RequestMaterialization = func(vc string, tt uint64) {
		// Callback may re-enter Summary through VChannel capacity checks.
		require.Positive(t, m.TransformStats(vc, 0, tt).Bytes)
		requests[vc] = tt
	}
	m.cfg.RetentionMaxBytes = 1
	m.requestMaterializationBacklog(context.Background(), time.Now(), 0)
	require.Equal(t, uint64(100), requests["v1"])
	clear(requests)
	m.ReportMaterialized("v1", 100)
	m.requestMaterializationBacklog(context.Background(), time.Now(), 0)
	require.Empty(t, requests, "wait for metadata/GC instead of draining later chunks")
	m.AdvanceGCTimeTick("v1", 100)
	require.NoError(t, gcSummary(context.Background(), m))
	m.requestMaterializationBacklog(context.Background(), time.Now(), 0)
	require.Equal(t, uint64(200), requests["v1"])
	clear(requests)
	m.terminalErr = ErrStoreCorrupted
	m.requestMaterializationBacklog(context.Background(), time.Now(), time.Nanosecond)
	require.Empty(t, requests)
}

func TestValidateTransformStatistics(t *testing.T) {
	m, _ := newTransformTestManagerWithStore(t)
	m.ObserveMessage(context.Background(), newTestDeleteMessage(t, "v1", 100, 10, 1))
	m.ObserveMessage(context.Background(), newTestDeleteMessage(t, "v1", 200, 10, 2))
	require.NoError(t, persistSummary(context.Background(), m))
	valid := m.Manifest().GetChunks()[0].GetVchannels()[0]
	require.NoError(t, validateTransformIndex(valid))
	for _, damage := range []func(*streamingpb.VChannelSummaryChunkIndex){
		func(i *streamingpb.VChannelSummaryChunkIndex) { i.Transform.Ref = nil },
		func(i *streamingpb.VChannelSummaryChunkIndex) { i.Transform.TotalSize = 0 },
		func(i *streamingpb.VChannelSummaryChunkIndex) { i.Transform.StartTimeTick = 201 },
		func(i *streamingpb.VChannelSummaryChunkIndex) { i.Transform.EndTimeTick = 0 },
		func(i *streamingpb.VChannelSummaryChunkIndex) { i.Transform.EndTimeTick = 201 },
	} {
		index := proto.Clone(valid).(*streamingpb.VChannelSummaryChunkIndex)
		damage(index)
		require.ErrorIs(t, validateTransformIndex(index), ErrStoreCorrupted)
	}
}

func TestRestoredLifecycleStillPinsUnfinishedMaterialization(t *testing.T) {
	for _, state := range []streamingpb.VChannelState{streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED} {
		t.Run(state.String(), func(t *testing.T) {
			m, store := newTransformTestManagerWithStore(t)
			var unused bool
			flushTransform(t, m, "v1", 100, &unused)
			recovered := newTestManager(t, store, 1)
			require.NoError(t, recovered.Restore(context.Background()))
			recovered.RestoreTransformGCTimeTicks(map[string]*streamingpb.VChannelMeta{"v1": {State: state, CheckpointTimeTick: 200, TransformMaterializedTimeTick: 50}})
			require.NoError(t, gcSummary(context.Background(), recovered))
			require.Len(t, recovered.Manifest().GetChunks(), 1, "lifecycle state cannot discard outstanding Delete@100")
			var requested uint64
			recovered.cfg.RequestMaterialization = func(_ string, tt uint64) { requested = tt }
			recovered.requestMaterializationBacklog(context.Background(), time.Now(), time.Minute)
			require.Equal(t, uint64(100), requested)
			batch, err := recovered.ReadTransform(context.Background(), "v1", 50, 200, ReadLimits{})
			require.NoError(t, err)
			require.Len(t, batch.Entries, 1)
			recovered.AdvanceGCTimeTick("v1", 200)
			require.NoError(t, gcSummary(context.Background(), recovered))
			require.Empty(t, recovered.Manifest().GetChunks())
		})
	}
}

func TestBacklogWorkerRequestsAlreadyPersistedDeletes(t *testing.T) {
	m, _ := newTransformTestManagerWithStore(t)
	var unused bool
	flushTransform(t, m, "v1", 100, &unused)
	require.Empty(t, m.pending)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var requested uint64
	m.cfg.RequestMaterialization = func(vc string, tt uint64) {
		require.Equal(t, "v1", vc)
		requested = tt
		cancel()
	}
	m.Run(ctx, time.Millisecond, nil)
	require.Equal(t, uint64(100), requested, "idle cold backlog is driven by the existing Summary worker")
}

func TestPartialTransformSectionBoundsAndBacklogAge(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	old := tsoutil.ComposeTSByTime(now.Add(-2 * time.Minute))
	recent := tsoutil.ComposeTSByTime(now.Add(-time.Second))
	m, _ := newTransformTestManagerWithStore(t)
	var requested uint64
	m.cfg.RequestMaterialization = func(_ string, through uint64) { requested = through }
	m.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", old, 10, 1))
	m.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", recent, 10, 2))
	require.NoError(t, persistSummary(ctx, m))
	full := m.TransformStats("v1", 0, recent)
	partial := m.TransformStats("v1", old, recent)
	require.Positive(t, full.Bytes)
	require.Equal(t, full.Bytes, full.UpperBytes)
	require.Zero(t, partial.Bytes)
	require.Equal(t, full.Bytes, partial.UpperBytes)
	m.ReportMaterialized("v1", old)
	m.requestMaterializationBacklog(ctx, now, time.Minute)
	require.Zero(t, requested, "consumed old records must not age the remaining section")
	m.requestMaterializationBacklog(ctx, now.Add(2*time.Minute), time.Minute)
	require.Equal(t, recent, requested)
}

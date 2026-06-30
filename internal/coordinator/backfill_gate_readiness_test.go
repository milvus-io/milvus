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

package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func init() {
	// Register/IsRoundReady read the promote-barrier param; make the package tests
	// independent of which test file runs first.
	paramtable.Init()
}

type fakeDataView struct {
	stale        map[int64][]int64
	checkpointOK map[int64]bool // consulted only for rounds with SchemaChangeTimeTick > 0
}

func (f *fakeDataView) StaleBackfillSegments(_ context.Context, collectionID int64, _ int32) []int64 {
	return f.stale[collectionID]
}

func (f *fakeDataView) ChannelCheckpointsAtLeast(_ context.Context, collectionID int64, _ uint64) bool {
	return f.checkpointOK[collectionID]
}

type fakeTargetVersion struct {
	version map[int64]int64
}

func (f *fakeTargetVersion) CurrentTargetVersion(_ context.Context, collectionID int64) int64 {
	return f.version[collectionID]
}

func futureNanos() int64 { return time.Now().Add(time.Hour).UnixNano() }

func scalarRoundWM(coll, round int64, wm int32, fields ...int64) *BackfillRound {
	// SchemaChangeTimeTick zero disables the checkpoint condition (legacy-round shape).
	return &BackfillRound{CollectionID: coll, RoundID: round, Fields: fields, Scope: NewWatermarkScope(wm, 0)}
}

func externalRound(coll, round int64, fields ...int64) *BackfillRound {
	return &BackfillRound{
		CollectionID: coll, RoundID: round, Fields: fields,
		Scope: NewExternalScope(),
	}
}

func TestReadiness_Watermark(t *testing.T) {
	ctx := context.Background()
	round := scalarRoundWM(1, 1, 3 /*V*/, 10)

	// Write side not done (a stale eligible segment remains in datacoord) -> not ready,
	// no matter how fresh the current target is.
	staleDV := &fakeDataView{stale: map[int64][]int64{1: {102}}}
	fresh := &fakeTargetVersion{version: map[int64]int64{1: futureNanos()}}
	assert.False(t, NewReadinessProvider(staleDV, fresh).IsRoundReady(ctx, round))
	assert.Zero(t, round.writeDoneObservedAtNanos, "T_c must not be recorded while the write side is dirty")

	// Write side clear, but the current target was built BEFORE T_c (or does not exist,
	// version 0) -> not ready.
	clearDV := &fakeDataView{}
	stale := &fakeTargetVersion{version: map[int64]int64{1: 1}}
	assert.False(t, NewReadinessProvider(clearDV, stale).IsRoundReady(ctx, round))
	assert.NotZero(t, round.writeDoneObservedAtNanos, "first clear observation records T_c")
	tc := round.writeDoneObservedAtNanos

	// A target built after T_c has been promoted -> ready. T_c stays at the first
	// observation (monotone write side).
	assert.True(t, NewReadinessProvider(clearDV, fresh).IsRoundReady(ctx, round))
	assert.Equal(t, tc, round.writeDoneObservedAtNanos)
}

func TestReadiness_External(t *testing.T) {
	ctx := context.Background()

	// The write side holds by construction, so T_c is stamped at the round's first
	// observation; a current target built before that -> not ready.
	round := externalRound(1, 1, 10)
	older := &fakeTargetVersion{version: map[int64]int64{1: 1}}
	assert.False(t, NewReadinessProvider(&fakeDataView{}, older).IsRoundReady(ctx, round))
	assert.NotZero(t, round.writeDoneObservedAtNanos, "first observation stamps T_c")
	tc := round.writeDoneObservedAtNanos

	// A target built after T_c has been promoted -> ready (the promote barrier
	// certified every copy loaded + reopened against the post-apply snapshot).
	newer := &fakeTargetVersion{version: map[int64]int64{1: futureNanos()}}
	assert.True(t, NewReadinessProvider(&fakeDataView{}, newer).IsRoundReady(ctx, round))
	assert.Equal(t, tc, round.writeDoneObservedAtNanos)
}

func TestReadiness_BarrierDisabledStandsDown(t *testing.T) {
	ctx := context.Background()
	key := paramtable.Get().QueryCoordCfg.UpdateTargetNeedSegmentDataReady.Key
	paramtable.Get().Save(key, "false")
	defer paramtable.Get().Reset(key)

	// With the promote barrier disabled nothing can ever certify the round -- holding
	// it would block the fields forever. The gate stands down: the round reads ready
	// (revoked with a warning) even though no fresh target exists.
	round := externalRound(1, 1, 10)
	stale := &fakeTargetVersion{version: map[int64]int64{1: 1}}
	assert.True(t, NewReadinessProvider(&fakeDataView{}, stale).IsRoundReady(ctx, round))
}

func TestReadiness_EmptyRoundIsSatisfied(t *testing.T) {
	paramtable.Init()
	provider := NewReadinessProvider(&fakeDataView{}, &fakeTargetVersion{})
	assert.True(t, provider.IsRoundReady(context.Background(), &BackfillRound{CollectionID: 1}))
	assert.True(t, provider.IsRoundReady(context.Background(), nil))
}

func TestReadiness_Watermark_CheckpointBlocks(t *testing.T) {
	ctx := context.Background()
	round := &BackfillRound{
		CollectionID: 1, RoundID: 1, Fields: []int64{10},
		Scope: NewWatermarkScope(3, 42 /* DDL timetick */),
	}
	fresh := &fakeTargetVersion{version: map[int64]int64{1: futureNanos()}}

	// Sealed view is clear, but the vchannel checkpoints have not passed the DDL
	// timetick -> pre-V data may still hide in growing segments -> not ready, and no
	// T_c is recorded.
	lagging := &fakeDataView{checkpointOK: map[int64]bool{}}
	assert.False(t, NewReadinessProvider(lagging, fresh).IsRoundReady(ctx, round))
	assert.Zero(t, round.writeDoneObservedAtNanos)

	// Checkpoints passed the tick -> write side complete -> ready once a later-built
	// target is promoted.
	passed := &fakeDataView{checkpointOK: map[int64]bool{1: true}}
	assert.True(t, NewReadinessProvider(passed, fresh).IsRoundReady(ctx, round))
}

func TestReadiness_Watermark_TcRestampOnDirty(t *testing.T) {
	ctx := context.Background()
	round := scalarRoundWM(1, 1, 3, 10)
	fresh := &fakeTargetVersion{version: map[int64]int64{1: futureNanos()}}
	stale := &fakeTargetVersion{version: map[int64]int64{1: 1}}

	// First clear observation stamps T_c.
	dv := &fakeDataView{stale: map[int64][]int64{}}
	assert.False(t, NewReadinessProvider(dv, stale).IsRoundReady(ctx, round))
	first := round.writeDoneObservedAtNanos
	assert.NotZero(t, first)

	// A late-flushed stale segment re-dirties the write side -> T_c must RESET, so the
	// round now requires a target built after the NEXT clear, not the first one.
	dv.stale[1] = []int64{102}
	assert.False(t, NewReadinessProvider(dv, fresh).IsRoundReady(ctx, round))
	assert.Zero(t, round.writeDoneObservedAtNanos)

	// Clear again -> new T_c -> a later-built target releases the round.
	dv.stale[1] = nil
	assert.True(t, NewReadinessProvider(dv, fresh).IsRoundReady(ctx, round))
	assert.Greater(t, round.writeDoneObservedAtNanos, first)
}

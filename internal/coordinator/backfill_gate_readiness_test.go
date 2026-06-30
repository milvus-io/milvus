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

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
)

type fakeDist struct {
	segs map[int64][]SegmentDistInfo
}

func (f *fakeDist) SegmentDist(collectionID int64) []SegmentDistInfo { return f.segs[collectionID] }

type fakeDataView struct {
	stale   map[int64][]int64
	dropped map[int64]bool
}

func (f *fakeDataView) StaleBackfillSegments(_ context.Context, collectionID int64, _ int32) []int64 {
	return f.stale[collectionID]
}

func (f *fakeDataView) BackfillSegmentDropped(_ context.Context, _, segmentID int64) bool {
	return f.dropped[segmentID]
}

func scalarRoundWM(coll, round int64, wm int32, fields ...int64) *BackfillRound {
	return &BackfillRound{CollectionID: coll, RoundID: round, Fields: fields, Scope: NewWatermarkScope(wm)}
}

func TestReadiness_WatermarkScalar(t *testing.T) {
	ctx := context.Background()
	round := scalarRoundWM(1, 1, 3 /*V*/, 10)

	// all segments reopened to schema_version >= 3 -> satisfied
	ready := &fakeDist{segs: map[int64][]SegmentDistInfo{1: {
		{SegmentID: 100, SchemaVersion: 3},
		{SegmentID: 101, SchemaVersion: 4},
	}}}
	assert.True(t, NewReadinessProvider(ready, &fakeDataView{}).IsRoundReady(ctx, round))

	// one segment still lags (schema_version < V) -> not satisfied
	lag := &fakeDist{segs: map[int64][]SegmentDistInfo{1: {
		{SegmentID: 100, SchemaVersion: 3},
		{SegmentID: 101, SchemaVersion: 2},
	}}}
	assert.False(t, NewReadinessProvider(lag, &fakeDataView{}).IsRoundReady(ctx, round))
}

func TestReadiness_Watermark_DataViewBlocks(t *testing.T) {
	ctx := context.Background()
	round := scalarRoundWM(1, 1, 3, 10)
	// Layer 1: dist is fully reopened, but datacoord still knows a stale segment (102)
	// that has not been loaded/reported anywhere (e.g. loading in flight) -> not satisfied.
	dist := &fakeDist{segs: map[int64][]SegmentDistInfo{1: {{SegmentID: 100, SchemaVersion: 3}}}}
	staleDV := &fakeDataView{stale: map[int64][]int64{1: {102}}}
	assert.False(t, NewReadinessProvider(dist, staleDV).IsRoundReady(ctx, round))

	// DataView clear -> the dist layer decides.
	assert.True(t, NewReadinessProvider(dist, &fakeDataView{}).IsRoundReady(ctx, round))
}

func TestReadiness_Watermark_IndexNotConsulted(t *testing.T) {
	ctx := context.Background()
	// E4: with global index-meta sync a field is ready once its data is loaded
	// (schema_version >= V); the index is never consulted. These segments carry NO index,
	// yet the round is judged ready purely on schema_version.
	round := vectorRound(1, 1, 10) // watermark scope (V=2)

	ready := &fakeDist{segs: map[int64][]SegmentDistInfo{1: {
		{SegmentID: 100, SchemaVersion: 2},
		{SegmentID: 101, SchemaVersion: 3},
	}}}
	assert.True(t, NewReadinessProvider(ready, &fakeDataView{}).IsRoundReady(ctx, round))

	// one segment below the watermark -> not ready (no index involved either way)
	lag := &fakeDist{segs: map[int64][]SegmentDistInfo{1: {
		{SegmentID: 100, SchemaVersion: 2},
		{SegmentID: 101, SchemaVersion: 1},
	}}}
	assert.False(t, NewReadinessProvider(lag, &fakeDataView{}).IsRoundReady(ctx, round))
}

func TestReadiness_SparkScalar(t *testing.T) {
	ctx := context.Background()
	round := &BackfillRound{
		CollectionID: 1, RoundID: 1,
		Fields: []int64{10},
		Scope:  NewSegmentListScope(map[int64]int64{100: 5, 101: 7}), // per-seg V_commit
	}

	// each committed segment reached its own V_commit -> satisfied
	ready := &fakeDist{segs: map[int64][]SegmentDistInfo{1: {
		{SegmentID: 100, Version: 5},
		{SegmentID: 101, Version: 9},
	}}}
	assert.True(t, NewReadinessProvider(ready, &fakeDataView{}).IsRoundReady(ctx, round))

	// one committed segment below its V_commit -> not satisfied
	below := &fakeDist{segs: map[int64][]SegmentDistInfo{1: {
		{SegmentID: 100, Version: 5},
		{SegmentID: 101, Version: 6},
	}}}
	assert.False(t, NewReadinessProvider(below, &fakeDataView{}).IsRoundReady(ctx, round))

	// a committed segment missing from dist -> not satisfied
	missing := &fakeDist{segs: map[int64][]SegmentDistInfo{1: {{SegmentID: 100, Version: 5}}}}
	assert.False(t, NewReadinessProvider(missing, &fakeDataView{}).IsRoundReady(ctx, round))
}

func TestReadiness_SparkScalar_V3Manifest(t *testing.T) {
	ctx := context.Background()
	// storage-v3 segment-list round: each per-segment V_commit is the committed manifest
	// version; the loaded manifest path (which encodes the loaded version) must reach it.
	round := &BackfillRound{
		CollectionID: 1, RoundID: 1,
		Fields: []int64{10},
		Scope:  NewSegmentListScope(map[int64]int64{100: 5, 101: 7}),
	}

	// each committed segment loaded a manifest version >= its V_commit -> satisfied
	ready := &fakeDist{segs: map[int64][]SegmentDistInfo{1: {
		{SegmentID: 100, ManifestPath: packed.MarshalManifestPath("b", 5)},
		{SegmentID: 101, ManifestPath: packed.MarshalManifestPath("b", 9)},
	}}}
	assert.True(t, NewReadinessProvider(ready, &fakeDataView{}).IsRoundReady(ctx, round))

	// one segment's loaded manifest version is below its V_commit -> not satisfied
	below := &fakeDist{segs: map[int64][]SegmentDistInfo{1: {
		{SegmentID: 100, ManifestPath: packed.MarshalManifestPath("b", 5)},
		{SegmentID: 101, ManifestPath: packed.MarshalManifestPath("b", 6)},
	}}}
	assert.False(t, NewReadinessProvider(below, &fakeDataView{}).IsRoundReady(ctx, round))

	// a malformed manifest path is treated as not-ready (conservative, never premature)
	bad := &fakeDist{segs: map[int64][]SegmentDistInfo{1: {
		{SegmentID: 100, ManifestPath: packed.MarshalManifestPath("b", 5)},
		{SegmentID: 101, ManifestPath: "not-a-manifest"},
	}}}
	assert.False(t, NewReadinessProvider(bad, &fakeDataView{}).IsRoundReady(ctx, round))
}

func TestReadiness_Spark_MultiCopyAllMustReachVCommit(t *testing.T) {
	ctx := context.Background()
	round := &BackfillRound{
		CollectionID: 1, RoundID: 1,
		Fields: []int64{10},
		Scope:  NewSegmentListScope(map[int64]int64{100: 5}),
	}
	// Segment 100 is loaded on two nodes/replicas: one copy reopened to V_commit, the
	// other still stale. The fresh copy must NOT mask the stale one -> not satisfied.
	mixed := &fakeDist{segs: map[int64][]SegmentDistInfo{1: {
		{SegmentID: 100, Version: 5},
		{SegmentID: 100, Version: 4},
	}}}
	assert.False(t, NewReadinessProvider(mixed, &fakeDataView{}).IsRoundReady(ctx, round))

	// Every copy reached V_commit -> satisfied.
	allFresh := &fakeDist{segs: map[int64][]SegmentDistInfo{1: {
		{SegmentID: 100, Version: 5},
		{SegmentID: 100, Version: 6},
	}}}
	assert.True(t, NewReadinessProvider(allFresh, &fakeDataView{}).IsRoundReady(ctx, round))
}

func TestReadiness_Spark_DroppedSegmentPruned(t *testing.T) {
	ctx := context.Background()
	round := &BackfillRound{
		CollectionID: 1, RoundID: 1,
		Fields: []int64{10},
		Scope:  NewSegmentListScope(map[int64]int64{100: 5, 101: 7}),
	}
	// 101 is missing from dist. While it is still a healthy datacoord segment the round
	// must keep blocking (presence-required: it may be reloading stale content).
	dist := &fakeDist{segs: map[int64][]SegmentDistInfo{1: {{SegmentID: 100, Version: 5}}}}
	assert.False(t, NewReadinessProvider(dist, &fakeDataView{}).IsRoundReady(ctx, round))

	// Once datacoord dropped it (compacted away / GC) it can never report again -> pruned,
	// the round is judged on the surviving committed segments only.
	droppedDV := &fakeDataView{dropped: map[int64]bool{101: true}}
	assert.True(t, NewReadinessProvider(dist, droppedDV).IsRoundReady(ctx, round))
}

func TestReadiness_RoundAtomicity(t *testing.T) {
	ctx := context.Background()
	// A round reveals atomically: every in-scope segment must reach the watermark; one
	// lagging segment holds back the whole (multi-field) round.
	round := vectorRound(1, 1, 10, 11) // watermark scope (V=2), two fields
	dist := &fakeDist{segs: map[int64][]SegmentDistInfo{1: {
		{SegmentID: 100, SchemaVersion: 2},
		{SegmentID: 101, SchemaVersion: 1}, // below V=2
	}}}
	assert.False(t, NewReadinessProvider(dist, &fakeDataView{}).IsRoundReady(ctx, round))
}

func TestReadiness_EmptyRoundIsSatisfied(t *testing.T) {
	dist := &fakeDist{segs: map[int64][]SegmentDistInfo{}}
	assert.True(t, NewReadinessProvider(dist, &fakeDataView{}).IsRoundReady(context.Background(),
		&BackfillRound{CollectionID: 1}))
}

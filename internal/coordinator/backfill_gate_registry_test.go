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
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeGateKV is an in-memory gateKV for tests.
type fakeGateKV struct {
	mu   sync.Mutex
	data map[string]string
}

func newFakeGateKV() *fakeGateKV { return &fakeGateKV{data: map[string]string{}} }

func (f *fakeGateKV) Save(_ context.Context, key, value string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[key] = value
	return nil
}

func (f *fakeGateKV) Remove(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.data, key)
	return nil
}

func (f *fakeGateKV) LoadWithPrefix(_ context.Context, prefix string) ([]string, []string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var keys, vals []string
	for k, v := range f.data {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
			vals = append(vals, v)
		}
	}
	return keys, vals, nil
}

func (f *fakeGateKV) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.data)
}

// recordingPusher records the order and content of pushes.
type recordingPusher struct {
	mu    sync.Mutex
	last  map[int64][]int64
	calls int
}

func newRecordingPusher() *recordingPusher {
	return &recordingPusher{last: map[int64][]int64{}}
}

func (p *recordingPusher) PushGatedFields(_ context.Context, collectionID int64, fields []int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	cp := append([]int64(nil), fields...)
	p.last[collectionID] = cp
	return nil
}

func (p *recordingPusher) lastFor(collectionID int64) []int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.last[collectionID]
}

func vectorRound(coll, round int64, fields ...int64) *BackfillRound {
	return &BackfillRound{CollectionID: coll, RoundID: round, Fields: fields, Scope: NewWatermarkScope(2)}
}

func TestBackfillAtomicGate_RegisterPersistsAndPushesUnion(t *testing.T) {
	ctx := context.Background()
	kv := newFakeGateKV()
	pusher := newRecordingPusher()
	reg := NewBackfillAtomicGate(kv, pusher)

	require.NoError(t, reg.Register(ctx, vectorRound(100, 1, 10, 11)))
	require.NoError(t, reg.Register(ctx, vectorRound(100, 2, 11, 12)))

	// two etcd entries written (write-once per round)
	assert.Equal(t, 2, kv.count())
	// proxy holds the union across rounds, sorted + deduped
	assert.Equal(t, []int64{10, 11, 12}, reg.GatedFields(100))
	assert.Equal(t, []int64{10, 11, 12}, pusher.lastFor(100))
	assert.Len(t, reg.List(), 2)
}

func TestBackfillAtomicGate_RegisterRejectsInvalid(t *testing.T) {
	reg := NewBackfillAtomicGate(newFakeGateKV(), newRecordingPusher())
	assert.Error(t, reg.Register(context.Background(), &BackfillRound{CollectionID: 0}))
	assert.Error(t, reg.Register(context.Background(), &BackfillRound{CollectionID: 1, RoundID: 1}))
}

func TestBackfillAtomicGate_RevokeReleasesThenDeletes(t *testing.T) {
	ctx := context.Background()
	kv := newFakeGateKV()
	pusher := newRecordingPusher()
	reg := NewBackfillAtomicGate(kv, pusher)

	require.NoError(t, reg.Register(ctx, vectorRound(100, 1, 10, 11)))
	require.NoError(t, reg.Register(ctx, vectorRound(100, 2, 12)))

	// revoke round 1 -> union shrinks to round 2's field, etcd entry removed
	require.NoError(t, reg.Revoke(ctx, 100, 1))
	assert.Equal(t, []int64{12}, reg.GatedFields(100))
	assert.Equal(t, []int64{12}, pusher.lastFor(100))
	assert.Equal(t, 1, kv.count())

	// revoke the last round -> empty union pushed (release), no entries left
	require.NoError(t, reg.Revoke(ctx, 100, 2))
	assert.Empty(t, reg.GatedFields(100))
	assert.Empty(t, pusher.lastFor(100))
	assert.Equal(t, 0, kv.count())

	// revoke is idempotent
	assert.NoError(t, reg.Revoke(ctx, 100, 2))
}

func TestBackfillAtomicGate_ReloadRestoresAndRepushes(t *testing.T) {
	ctx := context.Background()
	kv := newFakeGateKV()

	// seed etcd via a first registry, then reload into a fresh one (restart).
	seed := NewBackfillAtomicGate(kv, newRecordingPusher())
	require.NoError(t, seed.Register(ctx, vectorRound(100, 1, 10)))
	require.NoError(t, seed.Register(ctx, vectorRound(200, 7, 20, 21)))

	pusher := newRecordingPusher()
	reg := NewBackfillAtomicGate(kv, pusher)
	require.NoError(t, reg.Reload(ctx))

	assert.Len(t, reg.List(), 2)
	assert.Equal(t, []int64{10}, reg.GatedFields(100))
	assert.Equal(t, []int64{20, 21}, reg.GatedFields(200))
	// reload re-pushes each collection's union
	assert.Equal(t, []int64{10}, pusher.lastFor(100))
	assert.Equal(t, []int64{20, 21}, pusher.lastFor(200))
}

func TestBackfillAtomicGate_NilPusherIsSafe(t *testing.T) {
	reg := NewBackfillAtomicGate(newFakeGateKV(), nil)
	assert.NoError(t, reg.Register(context.Background(), vectorRound(1, 1, 1)))
	assert.NoError(t, reg.Revoke(context.Background(), 1, 1))
}

func sourceRound(coll, round int64, source string, field int64) *BackfillRound {
	return &BackfillRound{
		CollectionID: coll, RoundID: round, Source: source,
		Fields: []int64{field},
		Scope:  NewSegmentListScope(map[int64]int64{1: 5}),
	}
}

func TestBackfillAtomicGate_SourceDedupIdempotent(t *testing.T) {
	ctx := context.Background()
	kv := newFakeGateKV()
	reg := NewBackfillAtomicGate(kv, newRecordingPusher())

	// register a round identified by a source (e.g. a backfill result path)
	require.NoError(t, reg.Register(ctx, sourceRound(100, 1, "s3://result-A", 10)))
	// a RETRY with a different (freshly-allocated) roundID but the SAME source must reuse
	// the existing roundID -> no duplicate gate (idempotent).
	require.NoError(t, reg.Register(ctx, sourceRound(100, 2, "s3://result-A", 10)))
	require.Len(t, reg.List(), 1)
	assert.Equal(t, int64(1), reg.List()[0].RoundID) // reused, not 2
	assert.Equal(t, 1, kv.count())                   // single etcd entry

	// a different source is a distinct round -> separate entry
	require.NoError(t, reg.Register(ctx, sourceRound(100, 3, "s3://result-B", 11)))
	assert.Len(t, reg.List(), 2)
	assert.Equal(t, 2, kv.count())
}

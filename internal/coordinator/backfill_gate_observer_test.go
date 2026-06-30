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
)

// roundKey identifies a (coll, round) pair in tests.
type roundKey struct{ coll, round int64 }

type fakeStore struct {
	rounds   []*BackfillRound
	revoked  []roundKey
	repushed int
}

func (s *fakeStore) List() []*BackfillRound { return s.rounds }

func (s *fakeStore) Revoke(_ context.Context, collectionID, roundID int64) error {
	s.revoked = append(s.revoked, roundKey{collectionID, roundID})
	return nil
}

func (s *fakeStore) RepushAll(_ context.Context) { s.repushed++ }

// satisfiedSet marks specific rounds as ready.
type satisfiedSet struct{ ready map[roundKey]bool }

func (r satisfiedSet) IsRoundReady(_ context.Context, round *BackfillRound) bool {
	return r.ready[roundKey{round.CollectionID, round.RoundID}]
}

func TestBackfillAtomicGateObserver_SweepRevokesOnlySatisfied(t *testing.T) {
	store := &fakeStore{rounds: []*BackfillRound{
		{CollectionID: 1, RoundID: 1, Fields: []int64{10}},
		{CollectionID: 1, RoundID: 2, Fields: []int64{11}},
		{CollectionID: 2, RoundID: 5, Fields: []int64{20}},
	}}
	readiness := satisfiedSet{ready: map[roundKey]bool{
		{1, 1}: true,  // satisfied -> revoke
		{1, 2}: false, // not yet -> keep
		{2, 5}: true,  // satisfied -> revoke
	}}

	o := NewBackfillAtomicGateObserver(context.Background(), store, readiness, nil, 0)
	o.sweep()

	assert.ElementsMatch(t, []roundKey{{1, 1}, {2, 5}}, store.revoked)
}

func TestBackfillAtomicGateObserver_SweepNoneReady(t *testing.T) {
	store := &fakeStore{rounds: []*BackfillRound{
		{CollectionID: 1, RoundID: 1, Fields: []int64{10}},
	}}
	o := NewBackfillAtomicGateObserver(context.Background(), store, satisfiedSet{ready: map[roundKey]bool{}}, nil, 0)
	o.sweep()
	assert.Empty(t, store.revoked)
}

func TestBackfillAtomicGateObserver_SweepRevokesDroppedCollection(t *testing.T) {
	store := &fakeStore{rounds: []*BackfillRound{
		{CollectionID: 1, RoundID: 1, Fields: []int64{10}}, // dropped -> force-revoked even though not ready
		{CollectionID: 2, RoundID: 5, Fields: []int64{20}}, // alive + not ready -> kept
	}}
	dropped := func(_ context.Context, collectionID int64) bool { return collectionID == 1 }
	o := NewBackfillAtomicGateObserver(context.Background(), store, satisfiedSet{ready: map[roundKey]bool{}}, dropped, 0)
	o.sweep()
	assert.Equal(t, []roundKey{{1, 1}}, store.revoked)
}

func TestBackfillAtomicGateObserver_StartStopIdempotent(t *testing.T) {
	o := NewBackfillAtomicGateObserver(context.Background(), &fakeStore{}, satisfiedSet{ready: map[roundKey]bool{}}, nil, 0)
	o.Start()
	o.Start() // idempotent
	o.Stop()
	o.Stop() // idempotent
	assert.Equal(t, defaultBackfillGateSweepInterval, o.interval)
}

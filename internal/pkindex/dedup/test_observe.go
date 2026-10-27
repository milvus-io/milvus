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

//go:build test
// +build test

package dedup

import (
	"context"

	"github.com/milvus-io/milvus/internal/pkindex/codec"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
)

// ObserveForTest stands in for the observe of the WAL recovery: it applies one
// appended write with time tick tt to the engine, then removes its entries
// from the in-flight set (rule A). For a transaction that this index upgraded,
// puts are the keys of the insert and deletes the keys of the companion
// delete. Deletes are applied first, because both take effect at the same
// time tick and the insert survives the delete.
func ObserveForTest(ctx context.Context, d Decider, tt uint64, segmentID int64, puts []PK, deletes []PK) error {
	a := d.(*decider)
	muts := make([]engine.Mutation, 0, len(puts)+len(deletes))
	for _, pk := range deletes {
		muts = append(muts, engine.Mutation{Key: pk.Encode(), Delete: true})
	}
	value := codec.EncodePKEntry(codec.PKEntry{SegmentID: segmentID})
	for _, pk := range puts {
		muts = append(muts, engine.Mutation{Key: pk.Encode(), Value: value})
	}
	if len(muts) > 0 {
		if err := a.eng.Write(ctx, muts); err != nil {
			return err
		}
	}
	a.Observe(tt, append(append([]PK(nil), deletes...), puts...))
	return nil
}

// PendingTxnIDsForTest returns the transactions that have pending writes.
func PendingTxnIDsForTest(d Decider) []int64 {
	a := d.(*decider)
	a.pendingMu.Lock()
	defer a.pendingMu.Unlock()
	ids := make([]int64, 0, len(a.pending))
	for id := range a.pending {
		ids = append(ids, id)
	}
	return ids
}

// ObserveTimeTickForTest is Observe of a TimeTick, for the tests of other
// packages.
func ObserveTimeTickForTest(d Decider, tt uint64) {
	d.Observe(tt, nil)
}

// WaitUntilForTest is WaitUntil, for the tests of other packages.
func WaitUntilForTest(d Decider, tt uint64) {
	d.(*decider).WaitUntil(tt)
}

// InflightLenForTest returns the number of entries of the in-flight set.
func InflightLenForTest(d Decider) int {
	return d.(*decider).inflight.len()
}

// PresentForTest reports whether pk is in the in-flight set or in the engine.
func PresentForTest(ctx context.Context, d Decider, pk PK) (bool, error) {
	a := d.(*decider)
	key := pk.lockKey()
	a.inflight.mu.Lock()
	inSet := len(a.inflight.keys[key]) > 0
	a.inflight.mu.Unlock()
	if inSet {
		return true, nil
	}
	entries, err := lookup(ctx, a.eng, []PK{pk})
	if err != nil {
		return false, err
	}
	return entries[0] != nil, nil
}

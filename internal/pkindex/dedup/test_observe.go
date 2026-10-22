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

import "context"

// ObserveForTest stands in for observe until the recovery step adds it: it
// applies one appended write with time tick tt to the engine, then removes its
// entries from the in-flight set (rule A). For a transaction that this index
// upgraded, puts are the keys of the insert and deletes the keys of the
// companion delete. Deletes are applied first, because both take effect at the
// same time tick and the insert survives the delete.
func ObserveForTest(ctx context.Context, dd Decider, tt uint64, segmentID int64, puts []PK, deletes []PK) error {
	d := dd.(*decider)
	batch := d.store.NewBatch()
	for _, pk := range deletes {
		batch.Delete(pk)
	}
	for _, pk := range puts {
		batch.Put(pk, Entry{SegmentID: segmentID})
	}
	if err := d.store.Write(ctx, batch); err != nil {
		return err
	}
	d.inflight.observe(tt, append(append([]PK(nil), deletes...), puts...))
	return nil
}

// ObserveTimeTickForTest stands in for observe of a message that writes no key
// of this vchannel, a TimeTick for example.
func ObserveTimeTickForTest(d Decider, tt uint64) {
	d.(*decider).inflight.observeTimeTick(tt)
}

// InflightLenForTest returns the number of key entries in the in-flight set.
func InflightLenForTest(d Decider) int {
	return d.(*decider).inflight.len()
}

// PresentForTest reports whether pk exists for the write path: it has any write
// in the in-flight set, or the engine holds it.
func PresentForTest(ctx context.Context, dd Decider, pk PK) (bool, error) {
	d := dd.(*decider)
	key := pk.lockKey()
	d.inflight.mu.Lock()
	inSet := len(d.inflight.keys[key]) > 0
	d.inflight.mu.Unlock()
	if inSet {
		return true, nil
	}
	entry, err := d.store.Get(ctx, pk)
	return entry != nil, err
}

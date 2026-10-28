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

package index

import (
	"context"

	"github.com/milvus-io/milvus/internal/pkindex/codec"
	"github.com/milvus-io/milvus/internal/pkindex/dedup"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
)

// Put is one key that a message inserts, with the segment of the row.
type Put struct {
	PK        dedup.PK
	SegmentID int64
}

// Write is the index change of one WAL message: the keys it deletes and the
// keys it puts, at the time tick the message takes effect. The recovery of
// the pchannel builds it from a message, so this package reads no message.
// An autocommit write takes effect at its own time tick, a transaction at
// the time tick of its commit, with every body in one Write: its puts can go
// to more than one segment.
type Write struct {
	// Puts are the keys the message inserts, each with its segment.
	Puts []Put
	// Deletes are the keys the message deletes. They are written before Puts,
	// so a key in both ends up present: a companion delete must not hide the
	// insert of the same message.
	Deletes  []dedup.PK
	TimeTick uint64
}

// Apply writes w to the engine as one batch, tombstones first, then puts,
// and then tells the Decider that the keys are applied, which retires
// their entries of the in-flight set (rule A: the engine first, then the
// set). It reports whether the Index went from clean to dirty, so that the
// caller adds it to the LastAcked heap of the pchannel. A write at or below
// LastCommittedTimeTick is in the committed tables already and is skipped. A
// write before EnableTimeTick is before the index was enabled and is skipped.
// It does not decide: every delete writes a tombstone, whether the key is in
// the index or not. A failed engine write returns the error, it does not
// panic: the caller, the observe of the WAL recovery, classifies it.
//
// Only the observe of the WAL recovery calls it, in time tick order, one
// message at a time. The whole batch goes to one generation: engine.Write
// holds the generation for the whole call, so a rotate can not split it.
func (idx *Index) Apply(ctx context.Context, w Write) (dirtied bool, err error) {
	if w.TimeTick <= idx.LastCommittedTimeTick() || w.TimeTick < idx.cfg.EnableTimeTick {
		return false, nil
	}
	muts := make([]engine.Mutation, 0, len(w.Deletes)+len(w.Puts))
	for _, pk := range w.Deletes {
		muts = append(muts, engine.Mutation{Key: pk.Encode(), Delete: true})
	}
	var segmentID int64
	var value []byte
	for _, p := range w.Puts {
		if value == nil || p.SegmentID != segmentID {
			segmentID = p.SegmentID
			value = codec.EncodePKEntry(codec.PKEntry{SegmentID: segmentID})
		}
		muts = append(muts, engine.Mutation{Key: p.PK.Encode(), Value: value})
	}
	if len(muts) == 0 {
		return false, nil
	}
	if err := idx.write(ctx, muts); err != nil {
		return false, err
	}
	keys := make([]dedup.PK, 0, len(w.Deletes)+len(w.Puts))
	keys = append(keys, w.Deletes...)
	for _, p := range w.Puts {
		keys = append(keys, p.PK)
	}
	idx.auth.Observe(w.TimeTick, keys)
	return idx.markDirty(), nil
}

// write writes one batch to the engine, through the test hooks.
func (idx *Index) write(ctx context.Context, muts []engine.Mutation) error {
	if fail := idx.failWrites.Load(); fail != nil {
		return *fail
	}
	if hook := idx.onWrite.Load(); hook != nil {
		(*hook)(muts)
	}
	return idx.eng.Write(ctx, muts)
}

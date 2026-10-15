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

package pkindex

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/pkindex/codec"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
)

// errValueCountMismatch reports an engine whose MultiGet result does not align with the keys.
var errValueCountMismatch = errors.New("primary key index engine returned a wrong number of values")

// Entry is what the index stores for one primary key.
type Entry struct {
	SegmentID int64
}

func (e Entry) encode() []byte {
	return codec.EncodePKEntry(codec.PKEntry{SegmentID: e.SegmentID})
}

func decodeEntry(value []byte) (*Entry, error) {
	entry, err := codec.DecodePKEntry(value)
	if err != nil {
		return nil, err
	}
	return &Entry{SegmentID: entry.SegmentID}, nil
}

// pkStore reads and writes the engine of one vchannel in terms of primary
// keys and entries. It does not own the engine: the Index closes it.
type pkStore struct {
	eng engine.RW
}

// get returns the entry of pk, nil if pk is absent.
func (s pkStore) get(ctx context.Context, pk PK) (*Entry, error) {
	entries, err := s.multiGet(ctx, []PK{pk})
	if err != nil {
		return nil, err
	}
	return entries[0], nil
}

// multiGet returns the entries aligned with pks, nil for absent keys.
func (s pkStore) multiGet(ctx context.Context, pks []PK) ([]*Entry, error) {
	if len(pks) == 0 {
		return nil, nil
	}
	keys := make([][]byte, len(pks))
	for i, pk := range pks {
		keys[i] = pk.Encode()
	}
	values, err := s.eng.MultiGet(ctx, keys)
	if err != nil {
		return nil, err
	}
	if len(values) != len(keys) {
		return nil, errors.Wrapf(errValueCountMismatch, "%d values for %d keys", len(values), len(keys))
	}
	entries := make([]*Entry, len(pks))
	for i, value := range values {
		if value == nil {
			continue
		}
		if entries[i], err = decodeEntry(value); err != nil {
			return nil, err
		}
	}
	return entries, nil
}

// write applies the whole batch atomically. It can block on disk. The caller
// ends the process on an error, because the WAL already accepted the write
// that the batch describes.
func (s pkStore) write(ctx context.Context, b *entryBatch) error {
	if b == nil || len(b.muts) == 0 {
		return nil
	}
	return s.eng.Write(ctx, b.muts)
}

// entryBatch collects changes in memory. Nothing reaches the engine before
// pkStore.write. Changes are applied in the order of the put and delete calls
// that collected them.
type entryBatch struct {
	muts []engine.Mutation
}

// put maps pk to e.
func (b *entryBatch) put(pk PK, e Entry) {
	b.muts = append(b.muts, engine.Mutation{Key: pk.Encode(), Value: e.encode()})
}

// delete makes pk absent.
func (b *entryBatch) delete(pk PK) {
	b.muts = append(b.muts, engine.Mutation{Key: pk.Encode(), Delete: true})
}

// len returns the number of collected changes.
func (b *entryBatch) len() int {
	return len(b.muts)
}

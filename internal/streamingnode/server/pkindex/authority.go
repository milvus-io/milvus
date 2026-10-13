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

var (
	// errNilEngine reports a caller that passes no engine to New.
	errNilEngine = errors.New("primary key index engine is nil")
	// errValueCountMismatch reports an engine whose MultiGet result does not align with the keys.
	errValueCountMismatch = errors.New("primary key index engine returned a wrong number of values")
)

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

// Authority is the primary key index of one vchannel. It has key-value semantics
// only: it knows nothing about the WAL or about deduplication.
//
// The Authority does not own its engine. The owner closes the engine through
// engine.Lifecycle after it stops using the Authority.
//
// TODO: the index starts empty on every WAL open. Recovery and lazy loading are not designed yet.
type Authority interface {
	// Get returns the entry of pk, nil if pk is absent.
	Get(ctx context.Context, pk PK) (*Entry, error)
	// MultiGet returns the entries aligned with pks, nil for absent keys.
	MultiGet(ctx context.Context, pks []PK) ([]*Entry, error)
	NewBatch() *Batch
	// Write applies the whole batch atomically. It can block on disk.
	// The caller ends the process on an error, because the WAL already accepted
	// the write that the batch describes.
	Write(ctx context.Context, b *Batch) error
	// Ready is closed once the index of this vchannel can be used.
	// It gates this vchannel only, never the whole pchannel.
	Ready() <-chan struct{}
}

// NewAuthority creates the Authority of a vchannel on top of eng.
func NewAuthority(vchannel string, eng engine.RW) (Authority, error) {
	if eng == nil {
		return nil, errors.Wrapf(errNilEngine, "create primary key index authority of vchannel %s", vchannel)
	}
	return newAuthority(vchannel, eng), nil
}

func newAuthority(vchannel string, eng engine.RW) *authorityImpl {
	ready := make(chan struct{})
	close(ready)
	return &authorityImpl{vchannel: vchannel, engine: eng, ready: ready}
}

type authorityImpl struct {
	vchannel string
	engine   engine.RW
	ready    chan struct{}
}

func (a *authorityImpl) Get(ctx context.Context, pk PK) (*Entry, error) {
	entries, err := a.MultiGet(ctx, []PK{pk})
	if err != nil {
		return nil, err
	}
	return entries[0], nil
}

func (a *authorityImpl) MultiGet(ctx context.Context, pks []PK) ([]*Entry, error) {
	if len(pks) == 0 {
		return nil, nil
	}
	keys := make([][]byte, len(pks))
	for i, pk := range pks {
		keys[i] = pk.Encode()
	}
	values, err := a.engine.MultiGet(ctx, keys)
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

func (a *authorityImpl) NewBatch() *Batch {
	return &Batch{}
}

func (a *authorityImpl) Write(ctx context.Context, b *Batch) error {
	if b == nil || len(b.muts) == 0 {
		return nil
	}
	return a.engine.Write(ctx, b.muts)
}

func (a *authorityImpl) Ready() <-chan struct{} {
	return a.ready
}

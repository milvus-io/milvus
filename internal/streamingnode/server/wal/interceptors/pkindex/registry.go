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
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	snpkindex "github.com/milvus-io/milvus/internal/streamingnode/server/pkindex"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// target is what the interceptor needs to translate the writes of one vchannel.
type target struct {
	collectionID int64
	indexID      int64
	pkFieldID    int64
	pkDataType   schemapb.DataType
	// cipher is the cipher config last seen on a write of this vchannel, nil if
	// the collection is not encrypted. Companion deletes are encrypted with it.
	cipher atomic.Pointer[message.CipherConfig]
}

// registry maps the vchannels whose primary key index is enabled to their
// targets, and opens the Index of a vchannel at the enabling CreateIndex.
type registry struct {
	ch *snpkindex.PChannelIndexManager

	mu      sync.RWMutex
	targets map[string]*target
	// pending maps a vchannel to the time tick of its enabling CreateIndex
	// whose append result is unknown: the append failed after it got a time
	// tick, so the message may be in the WAL.
	pending map[string]uint64
	// dropped maps a vchannel to the index ID of its Index that a DropIndex
	// or a DropCollection appended after dropped. The Index stays open until
	// the observe reaches the drop, and lookup does not adopt it again.
	dropped map[string]int64
}

func newRegistry(ch *snpkindex.PChannelIndexManager) *registry {
	return &registry{ch: ch, targets: make(map[string]*target), pending: make(map[string]uint64), dropped: make(map[string]int64)}
}

func (r *registry) get(vchannel string) *target {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.targets[vchannel]
}

// forEach calls f for every target. f must not call back into the registry.
func (r *registry) forEach(f func(vchannel string, t *target)) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for vchannel, t := range r.targets {
		f(vchannel, t)
	}
}

// newTarget returns the target of an Index of the index indexID on the field
// fieldID of schema. It returns an error when the field is not a primary key
// of a supported data type.
func newTarget(collectionID, indexID, fieldID int64, schema *schemapb.CollectionSchema) (*target, error) {
	field := typeutil.GetField(schema, fieldID)
	if field == nil || !field.GetIsPrimaryKey() {
		return nil, merr.WrapErrServiceInternalMsg("field %d of collection %d is not the primary key", fieldID, collectionID)
	}
	if dt := field.GetDataType(); dt != schemapb.DataType_Int64 && dt != schemapb.DataType_VarChar {
		return nil, merr.WrapErrServiceInternalMsg("primary key of collection %d has the unsupported data type %s", collectionID, dt)
	}
	// The primary key field is cached for the life of the target. It stays valid
	// because a schema change can neither drop the primary key field nor alter
	// its id or its data type.
	return &target{collectionID: collectionID, indexID: indexID, pkFieldID: fieldID, pkDataType: field.GetDataType()}, nil
}

// enable opens the Index of spec and makes the vchannel a target. The append
// of the enabling CreateIndex calls it after the append succeeded. The
// CreateIndex holds the exclusive lock of its vchannel, so no write of the
// vchannel runs at the same time.
func (r *registry) enable(ctx context.Context, spec snpkindex.IndexSpec, fieldID int64, schema *schemapb.CollectionSchema) error {
	t, err := newTarget(spec.CollectionID, spec.IndexID, fieldID, schema)
	if err != nil {
		return err
	}
	// The engine open does disk IO, so it runs outside mu. Writes of other
	// vchannels keep reading the registry meanwhile.
	if _, _, err := r.ch.OpenIndex(ctx, spec); err != nil {
		return errors.Wrapf(err, "open the primary key index of vchannel %s", spec.VChannel)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.targets[spec.VChannel] = t
	delete(r.pending, spec.VChannel)
	delete(r.dropped, spec.VChannel)
	mlog.Info(ctx, "primary key index of vchannel enabled",
		mlog.FieldVChannel(spec.VChannel), mlog.FieldCollectionID(spec.CollectionID), mlog.Int64("indexID", spec.IndexID))
	return nil
}

// adopt makes a vchannel whose Index is open a target. The WAL open calls it
// for the collections that exist: the WAL recovery opened the Index of every
// vchannel with an enabled index. A vchannel without an Index gets no target.
func (r *registry) adopt(ctx context.Context, vchannel string, collectionID int64, schema *schemapb.CollectionSchema) error {
	idx := r.ch.IndexOf(vchannel)
	if idx == nil {
		return nil
	}
	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return err
	}
	t, err := newTarget(collectionID, idx.IndexID(), pkField.GetFieldID(), schema)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.targets[vchannel] = t
	mlog.Info(ctx, "primary key index of vchannel adopted at wal open",
		mlog.FieldVChannel(vchannel), mlog.FieldCollectionID(collectionID), mlog.Int64("indexID", idx.IndexID()))
	return nil
}

// markPending records that the decision of the enabling CreateIndex of a
// vchannel at tt is left to the observe: its append result is unknown, or
// its append read a write history that was not NEVER and that can lag the
// observe.
func (r *registry) markPending(vchannel string, tt uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pending[vchannel] = tt
}

// errEnablePending reports a write of a vchannel whose enabling CreateIndex
// waits for the decision of the observe, see markPending.
var errEnablePending = errors.New("the primary key index of the vchannel is being enabled")

// lookup returns the target of a vchannel, nil when its writes pass through.
//
// A vchannel without a target whose Index the observe of the WAL recovery
// opened becomes a target: the append of its enabling CreateIndex read a
// write history that was not NEVER, the CreateIndex was replicated, or its
// append result was unknown. An Index that a drop appended after is not
// adopted again.
//
// When the enabling CreateIndex of the vchannel waits for the observe (see
// markPending) and the observe opened no Index, an observe past the time
// tick of the CreateIndex means that the observe decided not to enable the
// index, or that the message is not in the WAL. Until then lookup returns an
// error that wraps errEnablePending.
func (r *registry) lookup(ctx context.Context, vchannel string) (*target, error) {
	// The observe opens the Index at the CreateIndex before it moves past
	// the time tick of the CreateIndex. Read the observe position before the
	// Index, so that a position past the CreateIndex implies that the Index
	// read below is the one that the observe opened, if any.
	lastObserved := r.ch.LastObserved()
	r.mu.RLock()
	t := r.targets[vchannel]
	tt, pending := r.pending[vchannel]
	droppedID, dropped := r.dropped[vchannel]
	r.mu.RUnlock()
	if t != nil {
		return t, nil
	}
	if idx := r.ch.IndexOf(vchannel); idx != nil && (!dropped || droppedID != idx.IndexID()) {
		if t, ok := r.adoptOpened(ctx, vchannel, idx.CollectionID(), idx.IndexID()); ok {
			return t, nil
		}
	}
	if !pending {
		return nil, nil
	}
	if lastObserved > tt {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.pending[vchannel] == tt {
			delete(r.pending, vchannel)
		}
		return nil, nil
	}
	return nil, errors.Wrapf(errEnablePending, "vchannel %s, create index at time tick %d", vchannel, tt)
}

// adoptOpened makes a vchannel a target of the Index indexID that the observe
// of the WAL recovery opened. It reports false when the vchannel state does
// not name that index, or does not name a usable primary key field.
func (r *registry) adoptOpened(ctx context.Context, vchannel string, collectionID, indexID int64) (*target, bool) {
	state, ok := r.ch.VChannelState(vchannel)
	if !ok || state.PKIndex.GetIndexId() != indexID {
		return nil, false
	}
	t, err := newTarget(collectionID, indexID, state.PKIndex.GetFieldId(), state.Schema)
	if err != nil {
		mlog.Warn(ctx, "the open primary key index of vchannel has no usable primary key field, its writes pass through",
			mlog.FieldVChannel(vchannel), mlog.Int64("indexID", indexID), mlog.Err(err))
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if cur := r.targets[vchannel]; cur != nil {
		return cur, true
	}
	r.targets[vchannel] = t
	delete(r.pending, vchannel)
	mlog.Info(ctx, "primary key index of vchannel adopted from the observe",
		mlog.FieldVChannel(vchannel), mlog.Int64("indexID", indexID))
	return t, true
}

// drop forgets the target and the pending enable of a vchannel after the
// append of a drop of its index, and remembers the index ID of its open
// Index, so that lookup does not adopt it again. The index stays open: the
// observe of the WAL recovery closes it when it reaches the drop, after it
// applied the writes before the drop, which must still find the engine open.
func (r *registry) drop(vchannel string) {
	idx := r.ch.IndexOf(vchannel)
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.targets, vchannel)
	delete(r.pending, vchannel)
	if idx != nil {
		r.dropped[vchannel] = idx.IndexID()
	}
}

// forget removes the target and the pending enable of a vchannel. A write
// that finds the Index of its target closed by the observe calls it.
func (r *registry) forget(vchannel string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.targets, vchannel)
	delete(r.pending, vchannel)
}

// close forgets the targets. It does not close the index of the pchannel: the
// WAL recovery owns it.
func (r *registry) close() {
	r.mu.Lock()
	r.targets = make(map[string]*target)
	r.pending = make(map[string]uint64)
	r.dropped = make(map[string]int64)
	r.mu.Unlock()
}

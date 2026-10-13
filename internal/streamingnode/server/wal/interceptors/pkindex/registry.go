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
	"github.com/milvus-io/milvus/internal/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// errOpenIndex marks an error of the index open. The open does local disk IO,
// so the error does not show a broken schema.
var errOpenIndex = errors.New("open the primary key index")

// target is what the interceptor needs to translate the writes of one vchannel.
type target struct {
	idx          *index.Index
	collectionID int64
	pkFieldID    int64
	pkDataType   schemapb.DataType
	// cipher is the cipher config last seen on a write of this vchannel, nil if
	// the collection is not encrypted. Companion deletes are encrypted with it.
	cipher atomic.Pointer[message.CipherConfig]
}

// registry turns the collection events of the WAL into calls on the index
// Channel, and maps vchannels to their targets.
type registry struct {
	ch *index.Channel

	mu      sync.RWMutex
	targets map[string]*target
}

func newRegistry(ch *index.Channel) *registry {
	return &registry{ch: ch, targets: make(map[string]*target)}
}

func (r *registry) get(vchannel string) *target {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.targets[vchannel]
}

// forEach calls f for every target. f must not call back into the registry.
func (r *registry) forEach(f func(t *target)) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, t := range r.targets {
		f(t)
	}
}

// add creates the target of a vchannel and reports whether this call created
// it. It creates none, and returns false and nil, when the collection has an
// autoID primary key or the target already exists. It returns an error when the
// schema has no usable primary key or the index can not be created. The caller
// decides what that error means for it.
//
// The caller serializes add and remove of one vchannel: the WAL open builds
// the targets before any append, and a collection DDL holds the exclusive
// lock of its vchannel.
//
// TODO: replace the global switch with the PRIMARY_KEY index enablement once it is delivered to the streaming node.
// TODO: transactions left uncommitted by a previous WAL lifetime are recovered by TxnManager and can
// still be committed, but their pending writes are not rebuilt here.
func (r *registry) add(ctx context.Context, vchannel string, collectionID int64, schema *schemapb.CollectionSchema) (bool, error) {
	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return false, err
	}
	if pkField.GetAutoID() {
		mlog.Info(ctx, "collection has an autoID primary key, it gets no primary key index",
			mlog.FieldVChannel(vchannel), mlog.FieldCollectionID(collectionID))
		return false, nil
	}
	if dt := pkField.GetDataType(); dt != schemapb.DataType_Int64 && dt != schemapb.DataType_VarChar {
		return false, merr.WrapErrServiceInternalMsg("primary key of collection %d has the unsupported data type %s", collectionID, dt)
	}
	// A vchannel name embeds the id of its collection, so an existing target of
	// this name can not belong to another collection and is kept as it is.
	if r.get(vchannel) != nil {
		return false, nil
	}
	// The engine open does disk IO, so it runs outside mu. Writes of other
	// vchannels keep reading the registry meanwhile.
	created, err := r.ch.Add(ctx, vchannel, collectionID, pkField.GetDataType())
	if err != nil {
		return false, errors.Mark(errors.Wrapf(err, "create primary key index engine of vchannel %s", vchannel), errOpenIndex)
	}
	idx := r.ch.Get(vchannel)
	if idx == nil {
		return false, merr.WrapErrServiceInternalMsg("primary key index of vchannel %s is removed while it is created", vchannel)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.targets[vchannel]; ok {
		return false, nil
	}
	// The primary key field is cached for the life of the target. It stays valid
	// because a schema change can neither drop the primary key field nor alter
	// its id or its data type.
	r.targets[vchannel] = &target{
		idx:          idx,
		collectionID: collectionID,
		pkFieldID:    pkField.GetFieldID(),
		pkDataType:   pkField.GetDataType(),
	}
	mlog.Info(ctx, "primary key index of vchannel created",
		mlog.FieldVChannel(vchannel), mlog.FieldCollectionID(collectionID))
	return created, nil
}

// drop removes the target of a vchannel, closes its index and removes the
// local files of the index. A failed removal leaves the files on disk and is
// only logged, because the caller can not undo its append.
//
// TODO: files that a failed removal or a crash leaves stay on disk. A sweep at
// the WAL open removes the directories of the vchannels that it does not open.
func (r *registry) drop(ctx context.Context, vchannel string) {
	r.mu.Lock()
	_, ok := r.targets[vchannel]
	delete(r.targets, vchannel)
	r.mu.Unlock()
	if !ok {
		return
	}
	if err := r.ch.Drop(ctx, vchannel); err != nil {
		mlog.Warn(ctx, "failed to remove the local files of the primary key index",
			mlog.FieldVChannel(vchannel), mlog.Err(err))
	}
}

// close drops every target and closes the Channel.
func (r *registry) close() {
	r.mu.Lock()
	r.targets = make(map[string]*target)
	r.mu.Unlock()
	r.ch.Close()
}

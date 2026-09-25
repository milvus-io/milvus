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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/pkindex/authority"
	"github.com/milvus-io/milvus/internal/pkindex/dedup"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// target is what the interceptor needs to handle the writes of one vchannel.
type target struct {
	decider      *dedup.Decider
	collectionID int64
	pkFieldID    int64
	pkDataType   schemapb.DataType
	// cipher is the cipher config last seen on a write of this vchannel, nil if
	// the collection is not encrypted. Companion deletes are encrypted with it.
	cipher atomic.Pointer[message.CipherConfig]
}

// registry maps vchannels to their targets.
type registry struct {
	stripeCount int

	mu      sync.RWMutex
	targets map[string]*target
}

func newRegistry(stripeCount int) *registry {
	return &registry{stripeCount: stripeCount, targets: make(map[string]*target)}
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

// add creates the target of a vchannel. It creates none, and returns nil, when
// the collection has an autoID primary key or the target already exists. It
// returns an error when the schema has no usable primary key or the index can
// not be created. The caller decides what that error means for it.
//
// TODO: replace the global switch with the PRIMARY_KEY index enablement once it is delivered to the streaming node.
// TODO: transactions left uncommitted by a previous WAL lifetime are recovered by TxnManager and can
// still be committed, but their pending writes are not rebuilt here.
func (r *registry) add(ctx context.Context, vchannel string, collectionID int64, schema *schemapb.CollectionSchema) error {
	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return err
	}
	if pkField.GetAutoID() {
		mlog.Info(ctx, "collection has an autoID primary key, it gets no primary key index",
			mlog.FieldVChannel(vchannel), mlog.FieldCollectionID(collectionID))
		return nil
	}
	if dt := pkField.GetDataType(); dt != schemapb.DataType_Int64 && dt != schemapb.DataType_VarChar {
		return merr.WrapErrServiceInternalMsg("primary key of collection %d has the unsupported data type %s", collectionID, dt)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// A vchannel name embeds the id of its collection, so an existing target of
	// this name can not belong to another collection and is kept as it is.
	if _, ok := r.targets[vchannel]; ok {
		return nil
	}
	a, err := authority.New(vchannel)
	if err != nil {
		return err
	}
	// The primary key field is cached for the life of the target. It stays valid
	// because a schema change can neither drop the primary key field nor alter
	// its id or its data type.
	r.targets[vchannel] = &target{
		decider:      dedup.New(a, r.stripeCount),
		collectionID: collectionID,
		pkFieldID:    pkField.GetFieldID(),
		pkDataType:   pkField.GetDataType(),
	}
	mlog.Info(ctx, "primary key index of vchannel created",
		mlog.FieldVChannel(vchannel), mlog.FieldCollectionID(collectionID))
	return nil
}

func (r *registry) remove(ctx context.Context, vchannel string) {
	r.mu.Lock()
	t, ok := r.targets[vchannel]
	delete(r.targets, vchannel)
	r.mu.Unlock()
	if !ok {
		return
	}
	closeTarget(ctx, vchannel, t)
}

func (r *registry) close(ctx context.Context) {
	r.mu.Lock()
	targets := r.targets
	r.targets = make(map[string]*target)
	r.mu.Unlock()
	for vchannel, t := range targets {
		closeTarget(ctx, vchannel, t)
	}
}

func closeTarget(ctx context.Context, vchannel string, t *target) {
	if err := t.decider.Close(); err != nil {
		mlog.Warn(ctx, "failed to close the primary key index of a vchannel",
			mlog.FieldVChannel(vchannel), mlog.Err(err))
	}
}

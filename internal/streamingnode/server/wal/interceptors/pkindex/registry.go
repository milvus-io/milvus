package pkindex

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/pkindex/authority"
	"github.com/milvus-io/milvus/internal/pkindex/authority/decider"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// target is what the interceptor needs to handle the writes of one vchannel.
type target struct {
	decider      *decider.Decider
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

// add creates the target of a vchannel if its collection has a user-provided primary key.
//
// TODO: replace the global switch with the PRIMARY_KEY index enablement once it is delivered to the streaming node.
// TODO: transactions left uncommitted by a previous WAL lifetime are recovered by TxnManager and can
// still be committed, but their pending writes are not rebuilt here.
func (r *registry) add(ctx context.Context, vchannel string, collectionID int64, schema *schemapb.CollectionSchema) {
	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil || pkField.GetAutoID() {
		return
	}
	if pkField.GetDataType() != schemapb.DataType_Int64 && pkField.GetDataType() != schemapb.DataType_VarChar {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// A vchannel name embeds the id of its collection, so an existing target of
	// this name can not belong to another collection and is kept as it is.
	if _, ok := r.targets[vchannel]; ok {
		return
	}
	a, err := authority.New(vchannel)
	if err != nil {
		mlog.Warn(ctx, "failed to create the primary key index of a vchannel, its writes are not deduplicated",
			mlog.FieldVChannel(vchannel), mlog.FieldCollectionID(collectionID), mlog.Err(err))
		return
	}
	// The primary key field is cached for the life of the target. It stays valid
	// because a schema change can neither drop the primary key field nor alter
	// its id or its data type.
	r.targets[vchannel] = &target{
		decider:      decider.New(a, r.stripeCount),
		collectionID: collectionID,
		pkFieldID:    pkField.GetFieldID(),
		pkDataType:   pkField.GetDataType(),
	}
	mlog.Info(ctx, "primary key index of vchannel created",
		mlog.FieldVChannel(vchannel), mlog.FieldCollectionID(collectionID))
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

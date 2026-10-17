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
	"time"

	"github.com/cockroachdb/errors"

	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/authority"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	snpkindex "github.com/milvus-io/milvus/internal/streamingnode/server/pkindex"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility/primarykey"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

var _ interceptors.InterceptorWithMetrics = (*appendInterceptor)(nil)

// txnSessions is the part of *txn.TxnManager the interceptor needs.
type txnSessions interface {
	GetSessionOfTxn(id message.TxnID) (*txn.TxnSession, error)
}

// appendInterceptor translates WAL messages into primary key index requests and
// appends what the index asks for. It holds no index logic of its own.
type appendInterceptor struct {
	ch       *snpkindex.PChannelIndexManager
	registry *registry
	sessions txnSessions
	metrics  *pkindexMetrics
	onFatal  func(error) // marks the WAL unavailable
}

func (i *appendInterceptor) Name() string {
	return interceptorName
}

func (i *appendInterceptor) DoAppend(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	switch msg.MessageType() {
	case message.MessageTypeCreateCollection:
		return i.appendCreateCollection(ctx, msg, appendOp)
	case message.MessageTypeDropCollection:
		return i.appendDropCollection(ctx, msg, appendOp)
	case message.MessageTypeTimeTick:
		// The timetick interceptor below removes the ended transactions from the
		// manager on every TimeTick. Their pending writes are dropped right after.
		// A TimeTick message carries its time tick before the append, and a
		// failed append does not tick.
		id, err := appendOp(ctx, msg)
		if err == nil {
			i.ch.ObserveTimeTick(msg.TimeTick())
			i.dropPendingOfEndedTxns()
		}
		return id, err
	case message.MessageTypeManualFlush:
		// A flush of a collection is broadcast to every vchannel of the
		// collection, so the message names the vchannel it flushes. The
		// timetick interceptor sets the time tick of the message in place.
		id, err := appendOp(ctx, msg)
		if err == nil {
			i.ch.RequestHandover(msg.VChannel(), msg.TimeTick())
		}
		return id, err
	case message.MessageTypeFlushAll:
		// A flush of the cluster is broadcast to the control channel of every
		// pchannel, so it flushes every vchannel of this pchannel.
		id, err := appendOp(ctx, msg)
		if err == nil {
			i.ch.RequestHandoverAll(msg.TimeTick())
		}
		return id, err
	case message.MessageTypeInsert, message.MessageTypeDelete, message.MessageTypeCommitTxn, message.MessageTypeRollbackTxn:
	default:
		return appendOp(ctx, msg)
	}

	// TODO: replicated messages bypass the index entirely, so a secondary cluster has an empty index after promotion.
	if msg.ReplicateHeader() != nil {
		return appendOp(ctx, msg)
	}
	t := i.registry.get(msg.VChannel())
	if t == nil {
		return appendOp(ctx, msg)
	}

	switch msg.MessageType() {
	case message.MessageTypeInsert:
		return i.appendInsert(ctx, t, msg, appendOp)
	case message.MessageTypeDelete:
		return i.appendDelete(ctx, t, msg, appendOp)
	case message.MessageTypeCommitTxn:
		return i.appendCommit(ctx, t, msg, appendOp)
	default:
		return i.appendRollback(ctx, t, msg, appendOp)
	}
}

func (i *appendInterceptor) Close() {
	i.registry.close()
}

func (i *appendInterceptor) appendCreateCollection(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	// The control channel copy of a DDL message only orders the DDL. It carries
	// no write of a data vchannel, so the registry never sees it.
	if funcutil.IsControlChannel(msg.VChannel()) {
		return appendOp(ctx, msg)
	}
	// The shard interceptor below reads the message the same way and rejects a
	// message without a schema, so a message this interceptor can not read is a
	// message the WAL can not take.
	create := message.MustAsMutableCreateCollectionMessageV1(msg)
	schema := messageutil.MustGetSchemaFromCreateCollectionMessageBody(create.MustBody())
	// The index is created before the append, so a collection whose index can not
	// be created is not created either. CreateCollection holds the exclusive lock
	// of the vchannel, no write of it is in flight.
	created, err := i.registry.add(ctx, msg.VChannel(), create.Header().GetCollectionId(), schema)
	if err != nil {
		return nil, status.NewUnrecoverableError("create the primary key index of the collection: %v", err)
	}
	id, err := appendOp(ctx, msg)
	if err != nil {
		// A duplicate CreateCollection finds the target of the existing collection
		// in use, so only a target that this call created is removed.
		if created {
			i.registry.drop(ctx, msg.VChannel())
		}
		return id, err
	}
	return id, nil
}

func (i *appendInterceptor) appendDropCollection(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	id, err := appendOp(ctx, msg)
	if err != nil {
		return id, err
	}
	if funcutil.IsControlChannel(msg.VChannel()) {
		return id, nil
	}
	i.registry.drop(ctx, msg.VChannel())
	return id, nil
}

func (i *appendInterceptor) appendInsert(ctx context.Context, t *target, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	body, err := utility.DecodeInsertBody(ctx, msg)
	if err != nil {
		return nil, err
	}
	// Legacy inserts may leave the declared field type unset, so only the payload is checked.
	keys, err := primarykey.KeysOfInsert(body, t.pkFieldID, t.pkDataType)
	if err != nil {
		return nil, err
	}
	i.rememberCipher(t, msg)

	guard, err := i.decide(ctx, t, authority.DecideRequest{
		Kind:     authority.KindInsert,
		VChannel: msg.VChannel(),
		PKs:      snpkindex.PKsOf(keys),
		TxnID:    txnIDOf(msg),
	})
	if err != nil {
		return nil, err
	}

	companionPKs := guard.Decision().CompanionDeletePKs
	if len(companionPKs) == 0 {
		id, err := appendOp(ctx, msg)
		if err != nil {
			guard.Discard(ctx, err)
			return id, err
		}
		i.applyInsert(ctx, guard, msg.IntoImmutableMessage(id), msg.TimeTick())
		return id, nil
	}

	companion, err := newCompanionDelete(msg.VChannel(), t, companionPKs)
	if err != nil {
		guard.Discard(ctx, err)
		return nil, status.NewInner("build the companion delete of the primary key index: %v", err)
	}
	result, err := txn.AppendInTxn(ctx, appendOp, msg, []message.MutableMessage{companion})
	if err != nil {
		// A redo runs the whole chain again and makes the decision anew.
		guard.Discard(ctx, err)
		return nil, err
	}
	i.metrics.observeCompanionDelete(len(companionPKs))
	i.applyInsert(ctx, guard, result.Main, result.Commit.TimeTick())
	return result.Commit.MessageID(), nil
}

// applyInsert reads the assigned segment back from the appended insert. The shard
// interceptor rewrites the header downstream, so it must be decoded again here.
func (i *appendInterceptor) applyInsert(ctx context.Context, guard *authority.WriteGuard, appended message.ImmutableMessage, timeTick uint64) {
	header := message.MustAsImmutableInsertMessageV1(appended).Header()
	var segmentID int64
	if partitions := header.GetPartitions(); len(partitions) > 0 {
		segmentID = partitions[0].GetSegmentAssignment().GetSegmentId()
	}
	guard.Apply(ctx, authority.Applied{SegmentID: segmentID, TimeTick: timeTick})
}

func (i *appendInterceptor) appendDelete(ctx context.Context, t *target, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	body, err := utility.DecodeDeleteBody(ctx, msg)
	if err != nil {
		return nil, err
	}
	// A delete by expression carries a predicate instead of primary keys. The
	// index can not tell which keys it removes, so the index does not accept it.
	if len(body.GetSerializedExprPlan()) > 0 {
		panic("a delete by expression reached the primary key index, the index does not support it")
	}
	keys, err := primarykey.KeysOfDelete(body)
	if err != nil {
		return nil, err
	}
	i.rememberCipher(t, msg)

	guard, err := i.decide(ctx, t, authority.DecideRequest{
		Kind:     authority.KindDelete,
		VChannel: msg.VChannel(),
		PKs:      snpkindex.PKsOf(keys),
		TxnID:    txnIDOf(msg),
	})
	if err != nil {
		return nil, err
	}

	// The delete reaches the WAL unchanged. The index only ever adds deletes, it never
	// removes one: it is a subset of the data, because rows written by bulk import or
	// while the switch was off are not in it, and because it starts empty on every WAL
	// open. A key that the index does not know may still exist in the data.
	// The decision (PresentPKs) is used for the index update and the metrics only.
	// TODO: narrow or skip a delete once the index is proven complete for the collection.
	id, err := appendOp(ctx, msg)
	if err != nil {
		guard.Discard(ctx, err)
		return id, err
	}
	guard.Apply(ctx, authority.Applied{TimeTick: msg.TimeTick()})
	return id, nil
}

func (i *appendInterceptor) appendCommit(ctx context.Context, t *target, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	guard, err := i.decide(ctx, t, authority.DecideRequest{
		Kind:     authority.KindCommitTxn,
		VChannel: msg.VChannel(),
		TxnID:    txnIDOf(msg),
	})
	if err != nil {
		return nil, err
	}

	companionPKs := guard.Decision().CompanionDeletePKs
	// With no companion delete, AppendInTxn appends only the CommitTxn, which is
	// the plain append. The companion delete is encrypted with the cipher config
	// remembered for this vchannel, and this handler never remembers one itself.
	// It does not need to: a pending set exists only if a body of the same
	// transaction passed through appendInsert or appendDelete in this wal
	// lifetime, and that call remembered the config. The config is remembered
	// once per wal lifetime, the first body of the vchannel that carries one wins.
	var extras []message.MutableMessage
	if len(companionPKs) > 0 {
		companion, err := newCompanionDelete(msg.VChannel(), t, companionPKs)
		if err != nil {
			guard.Discard(ctx, err)
			return nil, status.NewInner("build the companion delete of the primary key index: %v", err)
		}
		extras = append(extras, companion)
	}
	result, err := txn.AppendInTxn(ctx, appendOp, msg, extras)
	if err != nil {
		guard.Discard(ctx, err)
		return nil, err
	}
	i.metrics.observeCompanionDelete(len(companionPKs))
	guard.Apply(ctx, authority.Applied{TimeTick: result.Commit.TimeTick()})
	return result.Commit.MessageID(), nil
}

func (i *appendInterceptor) appendRollback(ctx context.Context, t *target, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	guard, err := i.decide(ctx, t, authority.DecideRequest{
		Kind:     authority.KindRollbackTxn,
		VChannel: msg.VChannel(),
		TxnID:    txnIDOf(msg),
	})
	if err != nil {
		return nil, err
	}

	id, err := appendOp(ctx, msg)
	if err != nil {
		guard.Discard(ctx, err)
		return id, err
	}
	guard.Apply(ctx, authority.Applied{TimeTick: msg.TimeTick()})
	return id, nil
}

// decide calls the index and classifies its error for the WAL.
func (i *appendInterceptor) decide(ctx context.Context, t *target, req authority.DecideRequest) (*authority.WriteGuard, error) {
	start := time.Now()
	a := i.ch.Authority(req.VChannel)
	if a == nil {
		// The index closed between the lookup of the target and this call:
		// the WAL closes, or a DropCollection raced with the write.
		return nil, decideError(errors.Wrapf(engine.ErrClosed, "primary key index of vchannel %s is closed", req.VChannel))
	}
	guard, err := a.Decide(ctx, req)
	if err != nil {
		if errors.Is(err, pkroot.ErrCorrupted) {
			// The index of this vchannel is wrong, and every later decision
			// would be too. The WAL becomes unavailable, like on a fenced claim.
			mlog.Error(ctx, "primary key index lookup found corrupted data, the WAL becomes unavailable",
				mlog.FieldVChannel(req.VChannel), mlog.Err(err))
			i.onFatal(err)
		}
		return nil, decideError(err)
	}
	switch {
	case req.Kind == authority.KindCommitTxn:
		// A commit probes the pending writes of its transaction, and the decider
		// does not expose how many keys that is. Without that number the hit and
		// miss counters can not both be fed, and feeding only the hits would make
		// the ratio of the two wrong. Only the duration and the lock wait are
		// observed until the decider reports the size of the pending snapshot.
		i.metrics.observeCommitDecide(time.Since(start), guard.LockWait())
	case req.TxnID == 0 && (req.Kind == authority.KindInsert || req.Kind == authority.KindDelete):
		d := guard.Decision()
		hit := len(d.CompanionDeletePKs)
		if req.Kind == authority.KindDelete {
			hit = len(d.PresentPKs)
		}
		i.metrics.observeDecide(time.Since(start), guard.LockWait(), len(req.PKs), hit)
	}
	return guard, nil
}

// decideError classifies an error of Authority.Decide for the WAL.
func decideError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	// The index is closed. That happens when the WAL closes, and the client
	// retries on the next owner of the channel.
	if errors.Is(err, engine.ErrClosed) {
		return status.NewOnShutdownError("primary key index is closed: %v", err)
	}
	// The index failed, not the request, so the append stays retriable for the client.
	return status.NewInner("primary key index lookup failed: %v", err)
}

// dropPendingOfEndedTxns drops the pending writes of every transaction whose
// session is gone. A pending set exists only for a transaction whose session
// existed when a body of it was applied, and every end of a session removes it
// from the manager. So a failed lookup means the transaction is over and no
// CommitTxn or RollbackTxn will drop the set.
func (i *appendInterceptor) dropPendingOfEndedTxns() {
	if i.sessions == nil {
		return
	}
	i.registry.forEach(func(vchannel string, _ *target) {
		a := i.ch.Authority(vchannel)
		if a == nil {
			return
		}
		for _, txnID := range a.PendingTxnIDs() {
			if _, err := i.sessions.GetSessionOfTxn(message.TxnID(txnID)); err != nil {
				a.DropTxn(txnID)
			}
		}
	})
}

func (i *appendInterceptor) rememberCipher(t *target, msg message.MutableMessage) {
	if t.cipher.Load() != nil {
		return
	}
	if cfg := message.CipherConfigOf(msg); cfg != nil {
		t.cipher.Store(cfg)
	}
}

func txnIDOf(msg message.MutableMessage) int64 {
	if txnCtx := msg.TxnContext(); txnCtx != nil {
		return int64(txnCtx.TxnID)
	}
	return 0
}

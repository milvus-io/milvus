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

	"github.com/milvus-io/milvus/internal/pkindex/dedup"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/redo"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility/primarykey"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
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
// appends what the decider asks for. It holds no index logic of its own.
type appendInterceptor struct {
	registry *registry
	sessions txnSessions
	metrics  *pkindexMetrics
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
		id, err := appendOp(ctx, msg)
		if err == nil {
			i.dropPendingOfEndedTxns()
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
	i.registry.close(context.TODO())
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
			i.registry.remove(ctx, msg.VChannel())
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
	i.registry.remove(ctx, msg.VChannel())
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

	intent, err := i.decide(ctx, t, dedup.DecideRequest{
		Kind:     dedup.KindInsert,
		VChannel: msg.VChannel(),
		PKs:      toPKs(keys),
		TxnID:    txnIDOf(msg),
	})
	if err != nil {
		return nil, err
	}

	companionPKs := intent.Decision().CompanionDeletePKs
	if len(companionPKs) == 0 {
		id, tt, err := utility.AppendAndGetTimeTick(ctx, appendOp, msg)
		if err != nil {
			discard(ctx, intent, err, tt)
			return id, err
		}
		intent.Committed(ctx, msg.TimeTick())
		return id, nil
	}

	companion, err := newCompanionDelete(msg.VChannel(), t, companionPKs)
	if err != nil {
		intent.Aborted(ctx, err)
		return nil, status.NewInner("build the companion delete of the primary key index: %v", err)
	}
	// TODO: confirm with a test that the messages of the upgraded transaction go
	// down the chain through appendOp and never enter the lock interceptor again.
	// TestAppendInTxnContractConcurrentWithExclusive only shows that no deadlock
	// occurs with an exclusive DDL.
	result, err := txn.AppendInTxn(ctx, appendOp, msg, []message.MutableMessage{companion})
	if err != nil {
		// A redo runs the whole chain again and makes the decision anew.
		discard(ctx, intent, err, txn.AllocatedTimeTick(err))
		return nil, err
	}
	i.metrics.observeCompanionDelete(len(companionPKs))
	// The insert and the companion delete take effect at the time tick of the CommitTxn.
	intent.Committed(ctx, result.Commit.TimeTick())
	return result.Commit.MessageID(), nil
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

	intent, err := i.decide(ctx, t, dedup.DecideRequest{
		Kind:     dedup.KindDelete,
		VChannel: msg.VChannel(),
		PKs:      toPKs(keys),
		TxnID:    txnIDOf(msg),
	})
	if err != nil {
		return nil, err
	}

	// The delete reaches the WAL unchanged. The index only ever adds deletes, it never
	// removes one: it is a subset of the data, because rows written by bulk import or
	// while the switch was off are not in it, and because it starts empty on every WAL
	// open. A key that the index does not know may still exist in the data.
	// The decision (PresentPKs) is used for the metrics only.
	// TODO: narrow or skip a delete once the index is proven complete for the collection.
	id, tt, err := utility.AppendAndGetTimeTick(ctx, appendOp, msg)
	if err != nil {
		discard(ctx, intent, err, tt)
		return id, err
	}
	intent.Committed(ctx, msg.TimeTick())
	return id, nil
}

func (i *appendInterceptor) appendCommit(ctx context.Context, t *target, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	intent, err := i.decide(ctx, t, dedup.DecideRequest{
		Kind:     dedup.KindCommitTxn,
		VChannel: msg.VChannel(),
		TxnID:    txnIDOf(msg),
	})
	if err != nil {
		return nil, err
	}

	companionPKs := intent.Decision().CompanionDeletePKs
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
			intent.Aborted(ctx, err)
			return nil, status.NewInner("build the companion delete of the primary key index: %v", err)
		}
		extras = append(extras, companion)
	}
	result, err := txn.AppendInTxn(ctx, appendOp, msg, extras)
	if err != nil {
		discard(ctx, intent, err, txn.AllocatedTimeTick(err))
		return nil, err
	}
	i.metrics.observeCompanionDelete(len(companionPKs))
	intent.Committed(ctx, result.Commit.TimeTick())
	return result.Commit.MessageID(), nil
}

func (i *appendInterceptor) appendRollback(ctx context.Context, t *target, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	intent, err := i.decide(ctx, t, dedup.DecideRequest{
		Kind:     dedup.KindRollbackTxn,
		VChannel: msg.VChannel(),
		TxnID:    txnIDOf(msg),
	})
	if err != nil {
		return nil, err
	}

	id, err := appendOp(ctx, msg)
	if err != nil {
		// A rollback registers no key, so there is nothing to keep.
		intent.Aborted(ctx, err)
		return id, err
	}
	intent.Committed(ctx, msg.TimeTick())
	return id, nil
}

// discard ends intent after a failed append. tt is the largest time tick that
// the failed append assigned, 0 when it assigned none. A write that is known not
// to be in the WAL leaves the in-flight set. Any other failed write may be in
// the WAL at a time tick up to tt, for example when the backend persisted it and
// then returned an error, so its keys stay present until observe passes tt.
func discard(ctx context.Context, intent *dedup.WriteIntent, err error, tt uint64) {
	// Without a time tick nothing reached the timetick interceptor, so nothing
	// was written. The shard interceptor returns ErrRedo before anything is
	// written, and AppendInTxn reports when its transaction can not have committed.
	if tt == 0 || errors.Is(err, redo.ErrRedo) || txn.NotCommitted(err) {
		intent.Aborted(ctx, err)
		return
	}
	intent.InDoubt(ctx, err, tt)
}

// decide calls the decider and classifies its error for the WAL.
func (i *appendInterceptor) decide(ctx context.Context, t *target, req dedup.DecideRequest) (*dedup.WriteIntent, error) {
	start := time.Now()
	intent, err := t.decider.Decide(ctx, req)
	if err != nil {
		// The wait needs observe to advance, which needs the WAL lock that this
		// interceptor runs under, so the redo interceptor waits outside the lock.
		var wait *dedup.WaitError
		if errors.As(err, &wait) {
			return nil, &interceptors.WaitRequiredError{Wait: func(ctx context.Context) error {
				if err := wait.Wait(ctx); err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						return err
					}
					return status.NewInner("primary key index wait failed: %v", err)
				}
				return nil
			}}
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		// The index failed, not the request, so the append stays retriable for the client.
		return nil, status.NewInner("primary key index lookup failed: %v", err)
	}
	switch {
	case req.Kind == dedup.KindCommitTxn:
		// A commit probes the pending writes of its transaction, and the decider
		// does not expose how many keys that is. Without that number the hit and
		// miss counters can not both be fed, and feeding only the hits would make
		// the ratio of the two wrong. Only the duration and the lock wait are
		// observed until the decider reports the size of the pending snapshot.
		i.metrics.observeCommitDecide(time.Since(start), intent.LockWait())
	case req.TxnID == 0 && (req.Kind == dedup.KindInsert || req.Kind == dedup.KindDelete):
		d := intent.Decision()
		hit := len(d.CompanionDeletePKs)
		if req.Kind == dedup.KindDelete {
			hit = len(d.PresentPKs)
		}
		i.metrics.observeDecide(time.Since(start), intent.LockWait(), len(req.PKs), hit)
	}
	return intent, nil
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
	i.registry.forEach(func(t *target) {
		for _, txnID := range t.decider.PendingTxnIDs() {
			if _, err := i.sessions.GetSessionOfTxn(message.TxnID(txnID)); err != nil {
				t.decider.DropTxn(txnID)
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

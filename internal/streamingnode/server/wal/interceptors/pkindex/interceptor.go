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
	"slices"
	"time"

	"github.com/cockroachdb/errors"

	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/dedup"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	snpkindex "github.com/milvus-io/milvus/internal/streamingnode/server/pkindex"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/redo"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility/primarykey"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

var _ interceptors.InterceptorWithMetrics = (*appendInterceptor)(nil)

// txnSessions is the part of *txn.TxnManager the interceptor needs.
type txnSessions interface {
	GetSessionOfTxn(id message.TxnID) (*txn.TxnSession, error)
}

// appendInterceptor translates WAL messages into primary key index requests and
// appends what the Decider asks for. It holds no index logic of its own.
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
	case message.MessageTypeCreateIndex:
		return i.appendCreateIndex(ctx, msg, appendOp)
	case message.MessageTypeDropIndex:
		return i.appendDropIndex(ctx, msg, appendOp)
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

	// A replicated message carries the decision of the source cluster, so it
	// skips the decision here. The observe applies it to the engine.
	// TODO: at promotion, call WaitUntil with the time tick of the role
	// switch. Replicated writes that the observe has not applied yet are not
	// in the in-flight set.
	if msg.ReplicateHeader() != nil {
		return appendOp(ctx, msg)
	}
	t, err := i.registry.lookup(ctx, msg.VChannel())
	if err != nil {
		if errors.Is(err, errEnablePending) {
			// The client retries, and the observe decides meanwhile.
			return nil, status.NewInner("primary key index is being enabled: %v", err)
		}
		return nil, status.NewInner("look up the primary key index: %v", err)
	}
	if t == nil {
		return appendOp(ctx, msg)
	}
	if i.ch.Decider(msg.VChannel()) == nil && !i.ch.Closed() {
		// The observe of the WAL recovery dropped the Index: the vchannel was
		// not empty at the enabling CreateIndex, or a drop whose append
		// failed is in the WAL. The writes pass through from now on.
		i.registry.forget(msg.VChannel())
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

// appendCreateIndex appends a CreateIndex and enables the primary key index
// of a data vchannel after a successful append, when no row was ever written
// to the vchannel. The CreateIndex holds the exclusive lock of the vchannel,
// so no write of the vchannel is appended until this call returns. The
// observe of the WAL recovery may still lag the writes appended before it by
// about one tick, so the write history read here can still say NEVER for a
// vchannel that is not empty. The observe decides again at the position of
// the CreateIndex and drops a wrong Index.
//
// The lag also works in the other direction: the write history read here is
// UNKNOWN when the observe has not built the state of the vchannel yet, for
// example right after its CreateCollection. So when the write history is not
// NEVER, the writes of the vchannel wait for the observe, as after an append
// with an unknown result: they adopt the Index that the observe opens at the
// CreateIndex, or pass through when the observe passes the CreateIndex
// without one, see registry.lookup.
//
// An append that fails after it got a time tick may be in the WAL. The
// writes of the vchannel then wait for the observe the same way. A
// replicated CreateIndex passes through: the observe opens its Index.
func (i *appendInterceptor) appendCreateIndex(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	header := message.MustAsMutableCreateIndexMessageV2(msg).Header()
	if funcutil.IsControlChannel(msg.VChannel()) || header.GetIndexType() != common.PrimaryKeyIndexType || msg.ReplicateHeader() != nil {
		return appendOp(ctx, msg)
	}
	state, _ := i.ch.VChannelState(msg.VChannel())
	id, tt, err := utility.AppendAndGetTimeTick(ctx, appendOp, msg)
	if err != nil {
		if tt != 0 && !errors.Is(err, redo.ErrRedo) {
			i.registry.markPending(msg.VChannel(), tt)
		}
		return id, err
	}
	if state.EverWritten != streamingpb.VChannelWriteHistory_VCHANNEL_WRITE_HISTORY_NEVER {
		i.registry.markPending(msg.VChannel(), msg.TimeTick())
		return id, nil
	}
	spec := snpkindex.IndexSpec{
		VChannel:       msg.VChannel(),
		CollectionID:   header.GetCollectionId(),
		IndexID:        header.GetIndexId(),
		EnableTimeTick: msg.TimeTick(),
	}
	if err := i.registry.enable(ctx, spec, header.GetFieldId(), state.Schema); err != nil {
		// The message is in the WAL. The reopen of the WAL enables the index
		// through the observe.
		mlog.Error(ctx, "failed to enable the primary key index after its CreateIndex, the WAL becomes unavailable",
			mlog.FieldVChannel(msg.VChannel()), mlog.Int64("indexID", header.GetIndexId()), mlog.Err(err))
		i.onFatal(errors.Wrap(err, "enable the primary key index after its CreateIndex"))
	}
	return id, nil
}

// appendDropIndex appends a DropIndex and drops the target of the vchannel
// when the DropIndex names its open index, or names no index. The index stays
// open: the observe of the WAL recovery closes it when it reaches the drop.
func (i *appendInterceptor) appendDropIndex(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	id, err := appendOp(ctx, msg)
	if err != nil {
		return id, err
	}
	header := message.MustAsMutableDropIndexMessageV2(msg).Header()
	if funcutil.IsControlChannel(msg.VChannel()) || header.GetIndexType() != common.PrimaryKeyIndexType {
		return id, nil
	}
	idx := i.ch.IndexOf(msg.VChannel())
	if idx != nil && (len(header.GetIndexIds()) == 0 || slices.Contains(header.GetIndexIds(), idx.IndexID())) {
		i.registry.drop(msg.VChannel())
	}
	return id, nil
}

func (i *appendInterceptor) appendDropCollection(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	id, err := appendOp(ctx, msg)
	if err != nil {
		// A DropCollection whose append fails but is persisted is in the WAL:
		// the observe closes the index, and the next write forgets the target.
		return id, err
	}
	if funcutil.IsControlChannel(msg.VChannel()) {
		return id, nil
	}
	// The index stays open: the observe of the WAL recovery closes it when
	// it reaches the drop, see registry.drop.
	i.registry.drop(msg.VChannel())
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
	t.rememberCipher(msg)

	intent, err := i.decide(ctx, t, msg.VChannel(), dedup.DecideRequest{
		Kind:  dedup.KindInsert,
		PKs:   snpkindex.PKsOf(keys),
		TxnID: txnIDOf(msg),
	})
	if err != nil {
		return nil, err
	}

	companionPKs := intent.CompanionDeletePKs()
	if len(companionPKs) == 0 {
		id, tt, err := utility.AppendAndGetTimeTick(ctx, appendOp, msg)
		if err != nil {
			discard(intent, err, tt)
			return id, err
		}
		intent.Appended(msg.TimeTick())
		return id, nil
	}

	companion, err := newCompanionDelete(msg.VChannel(), t, companionPKs)
	if err != nil {
		intent.NotAppended()
		return nil, status.NewInner("build the companion delete of the primary key index: %v", err)
	}
	// TODO: confirm with a test that the messages of the transaction built for the insert go
	// down the chain through appendOp and never enter the lock interceptor again.
	// TestAppendInTxnContractConcurrentWithExclusive only shows that no deadlock
	// occurs with an exclusive DDL.
	commit, err := txn.AppendInTxn(ctx, appendOp, msg, []message.MutableMessage{companion})
	if err != nil {
		// A redo runs the whole chain again and makes the decision anew.
		discard(intent, err, txn.AllocatedTimeTick(err))
		return nil, err
	}
	i.metrics.observeCompanionDelete(len(companionPKs))
	// The insert and the companion delete take effect at the time tick of the CommitTxn.
	intent.Appended(commit.TimeTick())
	return commit.MessageID(), nil
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
	t.rememberCipher(msg)

	// The delete reaches the WAL unchanged. The index only ever adds deletes, it never
	// removes one: it is a subset of the data, because rows written by bulk import or
	// while the switch was off are not in it, and because it starts empty on every WAL
	// open. A key that the index does not know may still exist in the data.
	// So an autocommit delete needs no decision and is not registered (see
	// Decider.Decide). A delete in a transaction is recorded as a pending write, so
	// that its keys enter the in-flight set at commit. It does not remove a key from
	// the companion delete decision, because it can cover one partition only.
	// TODO: narrow or skip a delete once the index is proven complete for the collection.
	txnID := txnIDOf(msg)
	if txnID == 0 {
		return appendOp(ctx, msg)
	}
	keys, err := primarykey.KeysOfDelete(body)
	if err != nil {
		return nil, err
	}
	intent, err := i.decide(ctx, t, msg.VChannel(), dedup.DecideRequest{
		Kind:  dedup.KindDelete,
		PKs:   snpkindex.PKsOf(keys),
		TxnID: txnID,
	})
	if err != nil {
		return nil, err
	}
	id, tt, err := utility.AppendAndGetTimeTick(ctx, appendOp, msg)
	if err != nil {
		discard(intent, err, tt)
		return id, err
	}
	intent.Appended(msg.TimeTick())
	return id, nil
}

func (i *appendInterceptor) appendCommit(ctx context.Context, t *target, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	intent, err := i.decide(ctx, t, msg.VChannel(), dedup.DecideRequest{
		Kind:  dedup.KindCommitTxn,
		TxnID: txnIDOf(msg),
	})
	if err != nil {
		return nil, err
	}

	companionPKs := intent.CompanionDeletePKs()
	// With no companion delete, AppendInTxn appends only the CommitTxn, which is
	// the plain append. The companion delete is encrypted with the cipher config
	// remembered for this vchannel, and this handler never remembers one itself.
	// It does not need to: a pending set exists only if a body of the same
	// transaction passed through appendInsert or appendDelete in this wal
	// lifetime, or was restored from the transaction buffer when the wal
	// opened (restorePending), and both remember the config. The config is
	// remembered once per wal lifetime, the first body of the vchannel that
	// carries one wins.
	var extras []message.MutableMessage
	if len(companionPKs) > 0 {
		companion, err := newCompanionDelete(msg.VChannel(), t, companionPKs)
		if err != nil {
			intent.NotAppended()
			return nil, status.NewInner("build the companion delete of the primary key index: %v", err)
		}
		extras = append(extras, companion)
	}
	commit, err := txn.AppendInTxn(ctx, appendOp, msg, extras)
	if err != nil {
		discard(intent, err, txn.AllocatedTimeTick(err))
		return nil, err
	}
	i.metrics.observeCompanionDelete(len(companionPKs))
	intent.Appended(commit.TimeTick())
	return commit.MessageID(), nil
}

func (i *appendInterceptor) appendRollback(ctx context.Context, t *target, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	intent, err := i.decide(ctx, t, msg.VChannel(), dedup.DecideRequest{
		Kind:  dedup.KindRollbackTxn,
		TxnID: txnIDOf(msg),
	})
	if err != nil {
		return nil, err
	}

	id, err := appendOp(ctx, msg)
	if err != nil {
		// A rollback registers no key, so there is nothing to keep.
		intent.NotAppended()
		return id, err
	}
	intent.Appended(msg.TimeTick())
	return id, nil
}

// discard ends intent after a failed append. tt is the largest time tick that
// the failed append assigned, 0 when it assigned none. A write that is known not
// to be in the WAL leaves the in-flight set. Any other failed write may be in
// the WAL at a time tick up to tt, for example when the backend persisted it and
// then returned an error, so its keys stay present until observe passes tt.
func discard(intent *dedup.WriteIntent, err error, tt uint64) {
	// Without a time tick nothing reached the timetick interceptor, so nothing
	// was written. The shard interceptor returns ErrRedo before anything is
	// written, and AppendInTxn reports when its transaction can not have committed.
	if tt == 0 || errors.Is(err, redo.ErrRedo) || txn.NotCommitted(err) {
		intent.NotAppended()
		return
	}
	intent.InDoubt(tt)
}

// decide asks the Decider of the vchannel and classifies its error for the WAL.
func (i *appendInterceptor) decide(ctx context.Context, t *target, vchannel string, req dedup.DecideRequest) (*dedup.WriteIntent, error) {
	start := time.Now()
	a := i.ch.Decider(vchannel)
	if a == nil {
		// The index closed between the lookup of the target and this call:
		// the WAL closes, or a DropCollection raced with the write.
		return nil, decideError(errors.Wrapf(engine.ErrClosed, "primary key index of vchannel %s is closed", vchannel))
	}
	intent, err := a.Decide(ctx, req)
	if err != nil {
		// The wait needs observe to advance, which needs the WAL lock that this
		// interceptor runs under, so the redo interceptor waits outside the lock.
		var wait *dedup.WaitError
		if errors.As(err, &wait) {
			return nil, &interceptors.WaitRequiredError{Wait: func(ctx context.Context) error {
				if err := wait.Wait(ctx); err != nil {
					return decideError(err)
				}
				return nil
			}}
		}
		if errors.Is(err, pkroot.ErrCorrupted) {
			// The index of this vchannel is wrong, and every later decision
			// would be too. The WAL becomes unavailable, like on a fenced flush.
			mlog.Error(ctx, "primary key index lookup found corrupted data, the WAL becomes unavailable",
				mlog.FieldVChannel(vchannel), mlog.Err(err))
			i.onFatal(err)
		}
		return nil, decideError(err)
	}
	switch {
	case req.Kind == dedup.KindCommitTxn:
		// A commit probes the pending writes of its transaction, and the Decider
		// does not expose how many keys that is. Without that number the hit and
		// miss counters can not both be fed, and feeding only the hits would make
		// the ratio of the two wrong. Only the duration and the lock wait are
		// observed until the Decider reports the size of the pending snapshot.
		i.metrics.observeCommitDecide(time.Since(start), intent.LockWait())
	case req.TxnID == 0 && req.Kind == dedup.KindInsert:
		hit := len(intent.CompanionDeletePKs())
		i.metrics.observeDecide(time.Since(start), intent.LockWait(), len(req.PKs), hit)
	}
	return intent, nil
}

// dropPendingOfEndedTxns drops the pending writes of every transaction whose
// session is gone. A pending set exists only for a transaction whose session
// existed when a body of it was applied, and every end of a session removes it
// from the manager. So a failed lookup means the transaction is over and no
// CommitTxn or RollbackTxn will drop the set.
// decideError translates the error of a decision for the client.
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

func (i *appendInterceptor) dropPendingOfEndedTxns() {
	if i.sessions == nil {
		return
	}
	i.registry.forEach(func(vchannel string, _ *target) {
		a := i.ch.Decider(vchannel)
		if a == nil {
			return
		}
		a.DropEndedTxns(func(txnID int64) bool {
			_, err := i.sessions.GetSessionOfTxn(message.TxnID(txnID))
			return err != nil
		})
	})
}

// rememberCipher keeps the cipher config of msg for the companion deletes of
// the vchannel. The first message of the vchannel that carries one wins.
func (t *target) rememberCipher(msg message.BasicMessage) {
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

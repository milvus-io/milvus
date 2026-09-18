package pkindex

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/pkindex/authority/decider"
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
	id, err := appendOp(ctx, msg)
	if err != nil {
		return id, err
	}
	// The control channel copy of a DDL message only orders the DDL. It carries
	// no write of a data vchannel, so the registry never sees it.
	if funcutil.IsControlChannel(msg.VChannel()) {
		return id, nil
	}
	// CreateCollection holds the exclusive lock of the vchannel, no write of it is in flight.
	create, err := message.AsMutableCreateCollectionMessageV1(msg)
	if err != nil {
		mlog.Warn(ctx, "a create collection message is not a v1 message, the collection gets no primary key index",
			mlog.FieldVChannel(msg.VChannel()), mlog.Err(err))
		return id, nil
	}
	// The message is already appended, so a canceled append context must not skip the registration.
	body, err := create.Body(context.Background())
	if err != nil {
		mlog.Warn(ctx, "failed to decode a create collection message, the collection gets no primary key index",
			mlog.FieldVChannel(msg.VChannel()), mlog.Err(err))
		return id, nil
	}
	if body.GetCollectionSchema() == nil && len(body.GetSchema()) == 0 {
		mlog.Warn(ctx, "a create collection message carries no schema, the collection gets no primary key index",
			mlog.FieldVChannel(msg.VChannel()))
		return id, nil
	}
	schema := messageutil.MustGetSchemaFromCreateCollectionMessageBody(body)
	i.registry.add(ctx, msg.VChannel(), create.Header().GetCollectionId(), schema)
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

	intent, err := i.decide(ctx, t, decider.Request{
		Kind:     decider.KindInsert,
		VChannel: msg.VChannel(),
		PKs:      toPKs(keys),
		TxnID:    txnIDOf(msg),
	})
	if err != nil {
		return nil, err
	}
	resolved := false
	defer unresolvedIntentGuard(ctx, intent, &resolved)()

	companionPKs := intent.Decision().CompanionDeletePKs
	if len(companionPKs) == 0 {
		id, err := appendOp(ctx, msg)
		if err != nil {
			resolved = true
			intent.Discard(ctx, err)
			return id, err
		}
		resolved = true
		i.applyInsert(ctx, t, intent, msg, msg.IntoImmutableMessage(id), msg.TimeTick())
		return id, nil
	}

	companion, err := newCompanionDelete(msg.VChannel(), t, companionPKs)
	if err != nil {
		resolved = true
		intent.Discard(ctx, err)
		return nil, status.NewInner("build the companion delete of the primary key index: %v", err)
	}
	result, err := txn.AppendInTxn(ctx, appendOp, msg, []message.MutableMessage{companion})
	if err != nil {
		// A redo runs the whole chain again and makes the decision anew.
		resolved = true
		intent.Discard(ctx, err)
		return nil, err
	}
	i.metrics.observeCompanionDelete(len(companionPKs))
	resolved = true
	i.applyInsert(ctx, t, intent, msg, result.Main, result.Commit.TimeTick())
	return result.Commit.MessageID(), nil
}

// unresolvedIntentGuard returns the deferred guard of a handler that holds an
// intent. resolved must be set right before Apply or Discard. Without the guard
// a panic between Decide and Apply would keep the stripes of the decision locked
// for the whole life of the wal, and every later write of the same keys would
// block. The panic itself is not recovered, it keeps unwinding.
func unresolvedIntentGuard(ctx context.Context, intent *decider.Intent, resolved *bool) func() {
	return func() {
		if !*resolved {
			intent.Discard(ctx, status.NewInner("the primary key index append did not finish"))
		}
	}
}

// applyInsert reads the assigned segment back from the appended insert. The shard
// interceptor rewrites the header downstream, so it must be decoded again here.
func (i *appendInterceptor) applyInsert(ctx context.Context, t *target, intent *decider.Intent, origin message.MutableMessage, appended message.ImmutableMessage, timeTick uint64) {
	header := message.MustAsImmutableInsertMessageV1(appended).Header()
	var segmentID int64
	if partitions := header.GetPartitions(); len(partitions) > 0 {
		segmentID = partitions[0].GetSegmentAssignment().GetSegmentId()
	}
	intent.Apply(ctx, decider.Applied{SegmentID: segmentID, TimeTick: timeTick})
	// origin is the message the client appended. On the companion path it is the
	// autocommit main of the transaction built inside the wal and carries no txn
	// context, so this call does nothing there. It only does work on the plain
	// path, where origin can be a body of a client transaction.
	i.registerTxnCleanup(t, origin)
}

func (i *appendInterceptor) appendDelete(ctx context.Context, t *target, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	body, err := utility.DecodeDeleteBody(ctx, msg)
	if err != nil {
		return nil, err
	}
	// TODO: a delete by expression carries no primary keys. The field exists in the proto,
	// but no producer sets it yet. Such a delete passes through and leaves stale entries in the index.
	if len(body.GetSerializedExprPlan()) > 0 {
		return appendOp(ctx, msg)
	}
	keys, err := primarykey.KeysOfDelete(body)
	if err != nil {
		return nil, err
	}
	i.rememberCipher(t, msg)

	intent, err := i.decide(ctx, t, decider.Request{
		Kind:     decider.KindDelete,
		VChannel: msg.VChannel(),
		PKs:      toPKs(keys),
		TxnID:    txnIDOf(msg),
	})
	if err != nil {
		return nil, err
	}
	resolved := false
	defer unresolvedIntentGuard(ctx, intent, &resolved)()

	// The delete reaches the WAL unchanged. The index only ever adds deletes, it never
	// removes one: it is a subset of the data, because rows written by bulk import or
	// while the switch was off are not in it, and because it starts empty on every WAL
	// open. A key that the index does not know may still exist in the data.
	// The decision (KeepPKs, Skip) is used for the index update and the metrics only.
	// TODO: narrow or skip a delete once the index is proven complete for the collection.
	id, err := appendOp(ctx, msg)
	if err != nil {
		resolved = true
		intent.Discard(ctx, err)
		return id, err
	}
	resolved = true
	intent.Apply(ctx, decider.Applied{TimeTick: msg.TimeTick()})
	i.registerTxnCleanup(t, msg)
	return id, nil
}

func (i *appendInterceptor) appendCommit(ctx context.Context, t *target, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	intent, err := i.decide(ctx, t, decider.Request{
		Kind:     decider.KindCommitTxn,
		VChannel: msg.VChannel(),
		TxnID:    txnIDOf(msg),
	})
	if err != nil {
		return nil, err
	}
	resolved := false
	defer unresolvedIntentGuard(ctx, intent, &resolved)()

	companionPKs := intent.Decision().CompanionDeletePKs
	if len(companionPKs) == 0 {
		id, err := appendOp(ctx, msg)
		if err != nil {
			resolved = true
			intent.Discard(ctx, err)
			i.dropTxnOfEndedSession(t, msg)
			return id, err
		}
		resolved = true
		intent.Apply(ctx, decider.Applied{TimeTick: msg.TimeTick()})
		return id, nil
	}

	// The companion delete is encrypted with the cipher config remembered for this
	// vchannel, and this handler never remembers one itself. It does not need to:
	// a pending set exists only if a body of the same transaction passed through
	// appendInsert or appendDelete in this wal lifetime, and that call remembered
	// the config. The config is remembered once per wal lifetime, the first body
	// of the vchannel that carries one wins.
	companion, err := newCompanionDelete(msg.VChannel(), t, companionPKs)
	if err != nil {
		resolved = true
		intent.Discard(ctx, err)
		return nil, status.NewInner("build the companion delete of the primary key index: %v", err)
	}
	result, err := txn.AppendInTxn(ctx, appendOp, msg, []message.MutableMessage{companion})
	if err != nil {
		resolved = true
		intent.Discard(ctx, err)
		i.dropTxnOfEndedSession(t, msg)
		return nil, err
	}
	i.metrics.observeCompanionDelete(len(companionPKs))
	resolved = true
	intent.Apply(ctx, decider.Applied{TimeTick: result.Commit.TimeTick()})
	return result.Commit.MessageID(), nil
}

func (i *appendInterceptor) appendRollback(ctx context.Context, t *target, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	intent, err := i.decide(ctx, t, decider.Request{
		Kind:     decider.KindRollbackTxn,
		VChannel: msg.VChannel(),
		TxnID:    txnIDOf(msg),
	})
	if err != nil {
		return nil, err
	}
	resolved := false
	defer unresolvedIntentGuard(ctx, intent, &resolved)()

	id, err := appendOp(ctx, msg)
	if err != nil {
		resolved = true
		intent.Discard(ctx, err)
		i.dropTxnOfEndedSession(t, msg)
		return id, err
	}
	resolved = true
	intent.Apply(ctx, decider.Applied{TimeTick: msg.TimeTick()})
	return id, nil
}

// decide calls the decider and classifies its error for the WAL.
func (i *appendInterceptor) decide(ctx context.Context, t *target, req decider.Request) (*decider.Intent, error) {
	start := time.Now()
	intent, err := t.decider.Decide(ctx, req)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		// The index failed, not the request, so the append stays retriable for the client.
		return nil, status.NewInner("primary key index lookup failed: %v", err)
	}
	switch {
	case req.Kind == decider.KindCommitTxn:
		// A commit probes the pending writes of its transaction, and the decider
		// does not expose how many keys that is. Without that number the hit and
		// miss counters can not both be fed, and feeding only the hits would make
		// the ratio of the two wrong. Only the duration and the lock wait are
		// observed until the decider reports the size of the pending snapshot.
		i.metrics.observeCommitDecide(time.Since(start), intent.LockWait())
	case req.TxnID == 0 && (req.Kind == decider.KindInsert || req.Kind == decider.KindDelete):
		d := intent.Decision()
		hit := len(d.CompanionDeletePKs)
		if req.Kind == decider.KindDelete {
			switch {
			case d.Skip:
				hit = 0
			case d.KeepPKs != nil:
				hit = len(d.KeepPKs)
			default:
				hit = len(req.PKs)
			}
		}
		i.metrics.observeDecide(time.Since(start), intent.LockWait(), len(req.PKs), hit)
	}
	return intent, nil
}

// registerTxnCleanup makes the pending writes of a transaction go away when the
// transaction ends without a CommitTxn or RollbackTxn.
//
// Three cleaners end a session: FailTxnAtVChannel, CleanupTxnUntil and
// RollbackAllInFlightTransactions. The vchannel lock of this append excludes only
// FailTxnAtVChannel, which needs the exclusive lock of the same vchannel. The
// other two take no vchannel lock, so a session can end between the check here
// and RegisterCleanup, and the callback then never runs. Every path below
// therefore looks the session up again and drops the pending set itself.
//
// The panic of RegisterCleanup on an expired or done session is not reachable
// here. AddNewMessageDoneAndKeepalive raised lastTimetick to at least the time
// tick of this body before the append returned, so the session is not expired at
// that time tick, and only the client's own commit or rollback moves the session
// into a done state.
func (i *appendInterceptor) registerTxnCleanup(t *target, body message.MutableMessage) {
	txnID := txnIDOf(body)
	if txnID == 0 || i.sessions == nil {
		return
	}
	timeTick := body.TimeTick()
	if !t.decider.ClaimTxnCleanup(txnID) {
		// An earlier body of the same transaction registered the cleanup. The
		// session may have ended since then without running it, so this body
		// checks the session too.
		i.dropTxnIfSessionEnded(t, txnID, timeTick)
		return
	}
	session, err := i.sessions.GetSessionOfTxn(message.TxnID(txnID))
	if err != nil {
		// The transaction already ended, nobody will commit it.
		t.decider.DropTxn(txnID)
		return
	}
	if session.IsExpiredOrDone(timeTick) {
		t.decider.DropTxn(txnID)
		return
	}
	session.RegisterCleanup(func() { t.decider.DropTxn(txnID) }, timeTick)
	i.dropTxnIfSessionEnded(t, txnID, timeTick)
}

// dropTxnOfEndedSession drops the pending writes of the transaction of msg when
// its session is already gone. It is called after a failed commit or rollback:
// the pending set survives that failure, and nothing else would remove it once
// the session that carries the cleanup callback has ended.
func (i *appendInterceptor) dropTxnOfEndedSession(t *target, msg message.MutableMessage) {
	txnID := txnIDOf(msg)
	if txnID == 0 || i.sessions == nil {
		return
	}
	i.dropTxnIfSessionEnded(t, txnID, msg.TimeTick())
}

// dropTxnIfSessionEnded drops the pending writes of a transaction whose session
// is gone, expired or done.
func (i *appendInterceptor) dropTxnIfSessionEnded(t *target, txnID int64, timeTick uint64) {
	session, err := i.sessions.GetSessionOfTxn(message.TxnID(txnID))
	if err != nil || session.IsExpiredOrDone(timeTick) {
		t.decider.DropTxn(txnID)
	}
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

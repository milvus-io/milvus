package idempotency

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	idempotencyutils "github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/idempotency/utils"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const interceptorName = "idempotency"

var (
	_ interceptors.Interceptor            = (*idempotencyInterceptor)(nil)
	_ interceptors.InterceptorWithMetrics = (*idempotencyInterceptor)(nil)
)

type idempotencyInterceptor struct {
	config WindowConfig

	windows                *typeutil.ConcurrentMap[string, *Window]
	txnInsertResultBuffers *idempotencyutils.TxnInsertResultBuffers
	// txnActive reports whether a txn session is still tracked by the txn
	// manager; a duplicate commit only synthesizes a rollback for a txn that is
	// positively known to be still open. nil means "unknown" and skips the
	// rollback, leaving the txn to keepalive expiry.
	txnActive idempotencyutils.TxnActiveChecker
	// replicateRole is intentionally dynamic: AlterReplicateConfig can switch the
	// WAL role while this interceptor instance stays alive. On SECONDARY, native
	// client writes must reach the inner replicate interceptor so it can reject
	// them; a duplicate short-circuit here would otherwise acknowledge data that
	// is neither persisted nor replicated.
	replicateRole func() replicateutil.Role
}

func (impl *idempotencyInterceptor) Name() string {
	return interceptorName
}

func newIdempotencyInterceptor(config WindowConfig) *idempotencyInterceptor {
	return newIdempotencyInterceptorWithParam(config, nil)
}

func newIdempotencyInterceptorWithParam(config WindowConfig, param *interceptors.InterceptorBuildParam) *idempotencyInterceptor {
	var currentTimeTick idempotencyutils.CurrentTimeTickProvider
	var txnActive idempotencyutils.TxnActiveChecker
	if param != nil && param.MVCCManager != nil {
		currentTimeTick = func(vchannel string) (uint64, bool) {
			return param.MVCCManager.GetMVCCOfVChannel(vchannel).Timetick, true
		}
	}
	if param != nil && param.TxnManager != nil {
		txnManager := param.TxnManager
		// A txn whose session is no longer tracked by the txn manager has been
		// committed, rolled back, expired or failed over. Its buffered insert
		// results can be dropped even if no commit/rollback message reached this
		// interceptor (e.g. RollbackAllInFlightTransactions on failover).
		txnActive = func(txnID message.TxnID) bool {
			_, err := txnManager.GetSessionOfTxn(txnID)
			return err == nil
		}
	}
	var replicateRole func() replicateutil.Role
	if param != nil && param.ReplicateManager != nil {
		replicateManager := param.ReplicateManager
		replicateRole = replicateManager.Role
	}
	return &idempotencyInterceptor{
		windows:                typeutil.NewConcurrentMap[string, *Window](),
		txnInsertResultBuffers: idempotencyutils.NewTxnInsertResultBuffers(currentTimeTick, txnActive),
		txnActive:              txnActive,
		replicateRole:          replicateRole,
		config:                 config,
	}
}

func newIdempotencyInterceptorWithSnapshots(config WindowConfig, snapshots map[string]*recovery.VChannelSummarySnapshot, param *interceptors.InterceptorBuildParam) *idempotencyInterceptor {
	interceptor := newIdempotencyInterceptorWithParam(config, param)
	for vchannel, snapshot := range snapshots {
		interceptor.windows.Insert(vchannel, NewWindowFromSnapshot(config, snapshot))
	}
	return interceptor
}

func (impl *idempotencyInterceptor) DoAppend(ctx context.Context, msg message.MutableMessage, append interceptors.Append) (message.MessageID, error) {
	if !impl.config.Enabled {
		return append(ctx, msg)
	}

	// Replicated messages bypass the idempotency window entirely: the replicate
	// stream has its own exactly-once delivery (source-timetick checkpoints),
	// and the idempotency key inside a replicated header belongs to the SOURCE
	// cluster's window history. Deduplicating against the local window would
	// silently drop replicated writes (divergence) whenever the key happens to
	// sit in this cluster's window — e.g. after a demotion, or after the source
	// evicted the key by TTL and a client legally re-issued it.
	if msg.ReplicateHeader() != nil {
		msgID, err := append(ctx, msg)
		if err == nil && recovery.InvalidatesIdempotencyWindow(msg.MessageType()) {
			// A replicated drop or truncate reclaims the vchannel just like a native one.
			impl.removeWindow(msg.VChannel())
		}
		return msgID, err
	}

	if impl.shouldLetReplicateGateHandle(msg) {
		return append(ctx, msg)
	}

	if recovery.InvalidatesIdempotencyWindow(msg.MessageType()) {
		msgID, err := append(ctx, msg)
		if err == nil {
			impl.removeWindow(msg.VChannel())
		}
		return msgID, err
	}

	if isTxnMessage(msg) {
		return impl.appendTxnMessage(ctx, msg, append)
	}
	return impl.appendSingleMessage(ctx, msg, append)
}

func (impl *idempotencyInterceptor) shouldLetReplicateGateHandle(msg message.MutableMessage) bool {
	if impl.replicateRole == nil || impl.replicateRole() != replicateutil.RoleSecondary {
		return false
	}
	if msg.ReplicateHeader() != nil || msg.MessageType().IsSelfControlled() {
		return false
	}
	return true
}

// removeWindow drops the in-memory window, its metric series, and any buffered
// txn insert results for a reclaimed vchannel, mirroring the recovery-side
// removeSummary. Without this, dropped vchannels pin retained PKs,
// Prometheus series, or abandoned txn builders for the WAL's lifetime under
// collection create/drop churn.
func (impl *idempotencyInterceptor) removeWindow(vchannel string) {
	if vchannel == "" {
		return
	}
	impl.txnInsertResultBuffers.RemoveVChannel(vchannel)
	if _, loaded := impl.windows.GetAndRemove(vchannel); loaded {
		deleteWindowMetrics(vchannel)
	}
}

func logIdempotencyDuplicateHit(ctx context.Context, vchannel string, key IdempotencyKey) {
	if !mlog.LevelEnabled(mlog.DebugLevel) {
		return
	}
	rawKey := string(key)
	mlog.RatedDebug(ctx, 1, "idempotency duplicate hit",
		mlog.FieldVChannel(vchannel),
		mlog.String("idempotencyKeyHash", message.IdempotencyKeyFingerprint(rawKey)),
		mlog.Int("idempotencyKeyLength", len(rawKey)))
}

func (impl *idempotencyInterceptor) appendSingleMessage(ctx context.Context, msg message.MutableMessage, append interceptors.Append) (message.MessageID, error) {
	key, hasIdempotencyKey, err := getIdempotencyKey(msg, impl.config)
	if err != nil {
		return nil, err
	}
	if !hasIdempotencyKey {
		// Append pass-through messages without idempotency handling.
		return append(ctx, msg)
	}
	return impl.appendIdempotentMessage(ctx, msg, key, append)
}

func (impl *idempotencyInterceptor) appendIdempotentMessage(ctx context.Context, msg message.MutableMessage, key IdempotencyKey, append interceptors.Append) (message.MessageID, error) {
	window := impl.window(msg.VChannel())
	begin := window.Begin(key, msg)
	switch begin.Decision {
	case BeginDecisionOwner:
		insertResult, err := getInsertResultFromInsertMessage(msg)
		if err != nil {
			window.Fail(begin.Pending, err, msg)
			return nil, err
		}
		msgID, err := append(ctx, msg)
		if err != nil {
			// KNOWN LIMITATION (documented; live reconciliation is a follow-up):
			// releasing the key assumes an append error means nothing was
			// written, but some WAL impls may land the write despite returning an
			// error (pulsar walimpls documents exactly this). In that ambiguous
			// window a same-key retry re-owns the key and appends again under a
			// fresh timetick, producing duplicate rows — the same outcome a retry
			// without idempotency would produce. Crash recovery is unaffected
			// (the persisted window re-materializes landed keys at WAL open);
			// closing the live-process gap requires the interceptor window to
			// reconcile from the recovery-side observer. The txn commit path
			// below shares this limitation.
			window.Fail(begin.Pending, err, msg)
			return nil, err
		}
		window.Complete(begin.Pending, commitResultFromAppendContext(ctx, msgID, insertResult), msg)
		return msgID, nil
	case BeginDecisionWait:
		result := begin.Pending.Wait(ctx, msg)
		if result.Err != nil {
			return nil, result.Err
		}
		logIdempotencyDuplicateHit(ctx, msg.VChannel(), key)
		return fillDuplicateResult(ctx, result.Entry)
	case BeginDecisionDuplicate:
		logIdempotencyDuplicateHit(ctx, msg.VChannel(), key)
		return fillDuplicateResult(ctx, begin.Entry)
	default:
		return nil, status.NewInner("unknown idempotency begin decision: %d", begin.Decision)
	}
}

func (impl *idempotencyInterceptor) appendIdempotentTxnCommitMessage(ctx context.Context, msg message.MutableMessage, key IdempotencyKey, append interceptors.Append) (message.MessageID, error) {
	window := impl.window(msg.VChannel())
	begin := window.Begin(key, msg)
	switch begin.Decision {
	case BeginDecisionOwner:
		insertResult := impl.txnInsertResultBuffers.Build(msg)
		// The buffered per-body results are consumed by Build above and are no
		// longer needed whether or not the commit append succeeds. Drop them on
		// every Owner exit so a failed commit does not leak the txn buffer.
		defer impl.txnInsertResultBuffers.Remove(msg)
		if insertResult == nil {
			// A keyed commit is only synthesized from the insert path, and the
			// proxy stamps an IdempotentInsertResult onto every insert header when
			// idempotency is enabled — so a nil Build means the txn buffer expired
			// early (its MVCC-driven expiry clock can run ahead of the txn
			// session's timetick-driven one under concurrent non-txn traffic).
			// Completing with a nil result would permanently persist an entry
			// whose duplicates return the retry's own unpersisted IDs; fail the
			// commit with TransactionExpired instead. The classification matters:
			// this failure is deterministic on a bare commit retry (the buffer is
			// gone and Fail below reopens the Owner slot), so a recoverable code
			// would make the streaming producer hot-retry the commit forever.
			// TransactionExpired is unrecoverable for the resumable producer and
			// makes produceTxn rebuild the whole transaction — re-appending the
			// bodies repopulates this buffer, so the rebuilt commit converges.
			err := status.NewTransactionExpired("idempotent txn commit lost its buffered insert results; rebuild the transaction")
			window.Fail(begin.Pending, err, msg)
			return nil, err
		}
		msgID, err := append(ctx, msg)
		if err != nil {
			window.Fail(begin.Pending, err, msg)
			return nil, err
		}
		window.Complete(begin.Pending, commitResultFromAppendContext(ctx, msgID, insertResult), msg)
		return msgID, nil
	case BeginDecisionWait:
		result := begin.Pending.Wait(ctx, msg)
		// Reclaim this commit's txn buffer only when the OWNER resolved the
		// pending entry (Complete or Fail — both happen after the owner's Build
		// consumed its buffer, so a same-txnID Remove is a no-op and a
		// retried-txnID Remove reclaims the abandoned buffer). When Wait exited
		// on the waiter's own context instead, the owner may still sit between
		// Begin and Build, and removing the (vchannel, txnID) buffer here would
		// destroy the owner's un-built insert results — a committed entry would
		// then permanently carry no IdempotentResult and later duplicates would
		// silently return the retry's own unpersisted IDs. Leave that buffer to
		// the txnActive/keepalive reclamation.
		if result.OwnerResolved {
			defer impl.txnInsertResultBuffers.Remove(msg)
		}
		if result.Err != nil {
			// The owner failed, so there is no duplicate result to serve. No
			// rollback is synthesized here: a same-txnID concurrent commit may
			// still be legitimately retried by the client after the owner's
			// failure, and rolling its session back would turn that recoverable
			// commit retry into a whole-txn retry. A retried txn under its own
			// txnID is left to keepalive expiry, like any abandoned txn.
			return nil, result.Err
		}
		logIdempotencyDuplicateHit(ctx, msg.VChannel(), key)
		impl.resolveRetriedTxnAfterDuplicate(ctx, msg, append)
		return fillDuplicateResult(ctx, result.Entry)
	case BeginDecisionDuplicate:
		logIdempotencyDuplicateHit(ctx, msg.VChannel(), key)
		defer impl.txnInsertResultBuffers.Remove(msg)
		impl.resolveRetriedTxnAfterDuplicate(ctx, msg, append)
		return fillDuplicateResult(ctx, begin.Entry)
	default:
		return nil, status.NewInner("unknown idempotency begin decision: %d", begin.Decision)
	}
}

// resolveRetriedTxnAfterDuplicate closes the transaction whose commit was
// short-circuited by a duplicate hit. The retried txn's BeginTxn and body
// messages have already been appended under a new txnID, so without an explicit
// resolution the txn session lingers until keepalive expiry, stalling
// last-confirmed / checkpoint advancement and accumulating WAL garbage per
// retry. A RollbackTxn is appended through the inner chain (the timetick
// interceptor closes the session; the scanner discards the uncommitted bodies).
// The rollback is synthesized only for a txn positively known to be still open
// — a concurrent duplicate commit sharing the owner's txnID was already closed
// by the owner's commit and must not be rolled back. Failure is non-fatal: the
// txn then falls back to keepalive expiry.
func (impl *idempotencyInterceptor) resolveRetriedTxnAfterDuplicate(ctx context.Context, msg message.MutableMessage, append interceptors.Append) {
	txnCtx := msg.TxnContext()
	if txnCtx == nil {
		return
	}
	if impl.txnActive == nil || !impl.txnActive(txnCtx.TxnID) {
		return
	}
	rollback := message.NewRollbackTxnMessageBuilderV2().
		WithVChannel(msg.VChannel()).
		WithHeader(&message.RollbackTxnMessageHeader{}).
		WithBody(&message.RollbackTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(*txnCtx)
	if _, err := append(ctx, rollback); err != nil {
		mlog.Warn(ctx, "failed to rollback retried txn after idempotency duplicate hit; txn falls back to keepalive expiry",
			mlog.String("vchannel", msg.VChannel()),
			mlog.Int64("txnID", int64(txnCtx.TxnID)),
			mlog.Err(err))
	}
}

func (impl *idempotencyInterceptor) appendTxnMessage(ctx context.Context, msg message.MutableMessage, append interceptors.Append) (message.MessageID, error) {
	switch msg.MessageType() {
	case message.MessageTypeBeginTxn:
		return impl.appendTxnBegin(ctx, msg, append)
	case message.MessageTypeCommitTxn:
		return impl.appendTxnCommit(ctx, msg, append)
	case message.MessageTypeRollbackTxn:
		return impl.appendTxnRollback(ctx, msg, append)
	default:
		return impl.appendTxnBody(ctx, msg, append)
	}
}

func (impl *idempotencyInterceptor) appendTxnBegin(ctx context.Context, msg message.MutableMessage, append interceptors.Append) (message.MessageID, error) {
	msgID, err := append(ctx, msg)
	if err != nil {
		return nil, err
	}
	impl.txnInsertResultBuffers.Remove(msg)
	return msgID, nil
}

func (impl *idempotencyInterceptor) appendTxnCommit(ctx context.Context, msg message.MutableMessage, append interceptors.Append) (message.MessageID, error) {
	key, hasIdempotencyKey, err := getIdempotencyKey(msg, impl.config)
	if err != nil {
		return nil, err
	}
	if !hasIdempotencyKey {
		// Append pass-through messages without idempotency handling.
		return append(ctx, msg)
	}
	return impl.appendIdempotentTxnCommitMessage(ctx, msg, key, append)
}

func (impl *idempotencyInterceptor) appendTxnRollback(ctx context.Context, msg message.MutableMessage, append interceptors.Append) (message.MessageID, error) {
	msgID, err := append(ctx, msg)
	if err != nil {
		return nil, err
	}
	impl.txnInsertResultBuffers.Remove(msg)
	return msgID, nil
}

func (impl *idempotencyInterceptor) appendTxnBody(ctx context.Context, msg message.MutableMessage, append interceptors.Append) (message.MessageID, error) {
	insertResult, err := getInsertResultFromInsertMessage(msg)
	if err != nil {
		return nil, err
	}
	if insertResult == nil {
		// Append pass-through txn body without idempotent insert result handoff.
		return append(ctx, msg)
	}

	msgID, err := append(ctx, msg)
	if err != nil {
		return nil, err
	}
	timeTick := uint64(0)
	if extra := utility.GetExtraAppendResult(ctx); extra != nil {
		timeTick = extra.TimeTick
	}
	impl.txnInsertResultBuffers.Add(msg, insertResult, timeTick)
	return msgID, nil
}

func (impl *idempotencyInterceptor) Close() {
	// Drop the per-vchannel window metric series so they do not linger in the
	// registry after this interceptor (one per pchannel WAL) tears down.
	impl.windows.Range(func(vchannel string, _ *Window) bool {
		deleteWindowMetrics(vchannel)
		return true
	})
}

func (impl *idempotencyInterceptor) window(vchannel string) *Window {
	if window, found := impl.windows.Get(vchannel); found {
		return window
	}
	window, _ := impl.windows.GetOrInsert(vchannel, NewWindow(impl.config))
	return window
}

// getIdempotencyKey reads the idempotency key from the message property. The
// property is readable on any message type, so the window is deliberately gated
// here to the two types it is designed to deduplicate; a key carried by any
// other type is ignored rather than silently creating a window entry.
func getIdempotencyKey(msg message.MutableMessage, config WindowConfig) (key IdempotencyKey, hasIdempotencyKey bool, err error) {
	switch msg.MessageType() {
	case message.MessageTypeInsert, message.MessageTypeCommitTxn:
	default:
		return "", false, nil
	}
	rawKey := message.IdempotencyKeyOf(msg)
	if rawKey == "" {
		if err := rejectInsertResultWithoutKey(msg); err != nil {
			return "", false, err
		}
		return "", false, nil
	}
	if config.MaxKeyLength > 0 && len(rawKey) > config.MaxKeyLength {
		return "", false, status.NewInvalidArgument("idempotency key length %d exceeds limit %d", len(rawKey), config.MaxKeyLength)
	}
	return IdempotencyKey(rawKey), true, nil
}

// rejectInsertResultWithoutKey enforces the producer-side invariant that the two
// halves of an idempotent insert travel together: an insert carrying a duplicate
// insert result but no key would be appended outside the window, so its result
// could never be served and a later retry would duplicate the rows.
func rejectInsertResultWithoutKey(msg message.MutableMessage) error {
	if msg.MessageType() != message.MessageTypeInsert {
		return nil
	}
	insertMsg, err := message.AsMutableInsertMessageV1(msg)
	if err != nil {
		return status.NewInvalidArgument("malformed insert message header")
	}
	if _, hasInsertResult := message.IdempotentInsertResultFromInsertHeader(insertMsg.Header()); hasInsertResult {
		return status.NewInvalidArgument("idempotency insert result header requires idempotency key")
	}
	return nil
}

func isTxnMessage(msg message.MutableMessage) bool {
	switch msg.MessageType() {
	case message.MessageTypeBeginTxn, message.MessageTypeCommitTxn, message.MessageTypeRollbackTxn:
		return true
	default:
		return msg.TxnContext() != nil
	}
}

func getInsertResultFromInsertMessage(msg message.MutableMessage) (*messagespb.IdempotentInsertResult, error) {
	if msg.MessageType() != message.MessageTypeInsert {
		return nil, nil
	}
	insertMsg, err := message.AsMutableInsertMessageV1(msg)
	if err != nil {
		return nil, status.NewInvalidArgument("malformed insert message header")
	}
	result, hasInsertResult := message.IdempotentInsertResultFromInsertHeader(insertMsg.Header())
	if !hasInsertResult {
		return nil, nil
	}
	if err := message.ValidateIdempotentInsertResult(result); err != nil {
		return nil, status.NewInvalidArgument("malformed idempotency insert result header")
	}
	return result, nil
}

func commitResultFromAppendContext(ctx context.Context, msgID message.MessageID, insertResult *messagespb.IdempotentInsertResult) CommitResult {
	extra := utility.GetExtraAppendResult(ctx)
	result := CommitResult{
		MessageID: message.MustMarshalMessageID(msgID),
	}
	if extra != nil {
		result.CommitTimeTick = extra.TimeTick
		result.LastConfirmedMessageID = message.MustMarshalMessageID(extra.LastConfirmedMessageID)
	}
	if insertResult != nil {
		result.IdempotentResult = insertResult
	}
	return result
}

func fillDuplicateResult(ctx context.Context, entry *recovery.SummaryRecord) (message.MessageID, error) {
	if entry == nil || entry.SourceMessageID == nil {
		// Typed so the streamingnode->proxy status converter carries a real code
		// instead of the untyped catch-all.
		return nil, status.NewInner("missing duplicate idempotency entry result")
	}
	msgID := message.MustUnmarshalMessageID(entry.SourceMessageID)
	// The response must carry a last-confirmed position: the producer client
	// treats it as mandatory and fails the whole stream on an empty one. The
	// record keeps the original, so a duplicate answers exactly what the first
	// append answered. The fallback covers a record written before the store
	// carried the field; the message's own id is at or after the true
	// last-confirmed, which makes it usable as an identifier but not as a read
	// position.
	lastConfirmed := message.MustUnmarshalMessageID(entry.LastConfirmedMessageID)
	if lastConfirmed == nil {
		lastConfirmed = msgID
	}
	if extra := utility.GetExtraAppendResult(ctx); extra != nil {
		extra.TimeTick = entry.SourceTimeTick
		extra.LastConfirmedMessageID = lastConfirmed
		// A duplicate response never carries a txn context; clear whatever an
		// intervening inner append (e.g. the synthesized retried-txn rollback)
		// left behind.
		extra.TxnCtx = nil
		// Always overwrite Extra so a duplicate without an insert result does not
		// leak whatever value the ExtraAppendResult already carried into this
		// append's result.
		if entry.InsertResult != nil && entry.InsertResult.GetIds() != nil {
			extra.Extra = entry.InsertResult
		} else {
			extra.Extra = nil
		}
	}
	return msgID, nil
}

package idempotency

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	idempotencyutils "github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/idempotency/utils"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
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
	return &idempotencyInterceptor{
		windows:                typeutil.NewConcurrentMap[string, *Window](),
		txnInsertResultBuffers: idempotencyutils.NewTxnInsertResultBuffers(currentTimeTick, txnActive),
		config:                 config,
	}
}

func newIdempotencyInterceptorWithSnapshots(config WindowConfig, snapshots map[string]*streamingpb.WindowSnapshot, param *interceptors.InterceptorBuildParam) *idempotencyInterceptor {
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

	if isTxnMessage(msg) {
		return impl.appendTxnMessage(ctx, msg, append)
	}
	return impl.appendSingleMessage(ctx, msg, append)
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
		mlog.Debug(ctx, "idempotency duplicate hit",
			mlog.String("vchannel", msg.VChannel()),
			mlog.String("idempotency_key", string(key)))
		return fillDuplicateResult(ctx, result.Entry)
	case BeginDecisionDuplicate:
		mlog.Debug(ctx, "idempotency duplicate hit",
			mlog.String("vchannel", msg.VChannel()),
			mlog.String("idempotency_key", string(key)))
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
		msgID, err := append(ctx, msg)
		if err != nil {
			window.Fail(begin.Pending, err, msg)
			return nil, err
		}
		window.Complete(begin.Pending, commitResultFromAppendContext(ctx, msgID, insertResult), msg)
		return msgID, nil
	case BeginDecisionWait:
		// A duplicate commit must NOT touch the txn buffer here: it is keyed by
		// (vchannel, txnID), identical to the in-flight owner's, and the owner
		// consumes it via Build then drops it via the defer above. Removing it
		// from a duplicate races the owner's Build and can blank out the committed
		// entry's insert IDs.
		result := begin.Pending.Wait(ctx, msg)
		if result.Err != nil {
			return nil, result.Err
		}
		mlog.Debug(ctx, "idempotency duplicate hit",
			mlog.String("vchannel", msg.VChannel()),
			mlog.String("idempotency_key", string(key)))
		return fillDuplicateResult(ctx, result.Entry)
	case BeginDecisionDuplicate:
		mlog.Debug(ctx, "idempotency duplicate hit",
			mlog.String("vchannel", msg.VChannel()),
			mlog.String("idempotency_key", string(key)))
		return fillDuplicateResult(ctx, begin.Entry)
	default:
		return nil, status.NewInner("unknown idempotency begin decision: %d", begin.Decision)
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

func getIdempotencyKey(msg message.MutableMessage, config WindowConfig) (key IdempotencyKey, hasIdempotencyKey bool, err error) {
	var rawKey string
	var hasInsertResult bool
	switch msg.MessageType() {
	case message.MessageTypeInsert:
		insertMsg, err := message.AsMutableInsertMessageV1(msg)
		if err != nil {
			return "", false, status.NewInvalidArgument("malformed insert message header")
		}
		rawKey = insertMsg.Header().GetIdempotencyKey()
		_, hasInsertResult = message.IdempotentInsertResultFromInsertHeader(insertMsg.Header())
	case message.MessageTypeCommitTxn:
		commitMsg, err := message.AsMutableCommitTxnMessageV2(msg)
		if err != nil {
			return "", false, status.NewInvalidArgument("malformed commit txn message header")
		}
		rawKey = commitMsg.Header().GetIdempotencyKey()
	default:
		return "", false, nil
	}
	if rawKey == "" {
		if hasInsertResult {
			return "", false, status.NewInvalidArgument("idempotency insert result header requires idempotency key")
		}
		return "", false, nil
	}
	if config.MaxKeyLength > 0 && len(rawKey) > config.MaxKeyLength {
		return "", false, status.NewInvalidArgument("idempotency key length %d exceeds limit %d", len(rawKey), config.MaxKeyLength)
	}
	return IdempotencyKey(rawKey), true, nil
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

func fillDuplicateResult(ctx context.Context, entry *streamingpb.WindowEntry) (message.MessageID, error) {
	if entry == nil || entry.GetMessageId() == nil {
		return nil, errors.New("missing duplicate idempotency entry result")
	}
	msgID := message.MustUnmarshalMessageID(entry.GetMessageId())
	lastConfirmed := message.MustUnmarshalMessageID(entry.GetLastConfirmedMessageId())
	if lastConfirmed == nil {
		lastConfirmed = msgID
	}
	if extra := utility.GetExtraAppendResult(ctx); extra != nil {
		extra.TimeTick = entry.GetCommitTimetick()
		extra.LastConfirmedMessageID = lastConfirmed
		// Always overwrite Extra so a duplicate without an insert result does not
		// leak whatever value the ExtraAppendResult already carried into this
		// append's result.
		if result := entry.GetIdempotentResult(); result != nil && result.GetIds() != nil {
			extra.Extra = result
		} else {
			extra.Extra = nil
		}
	}
	return msgID, nil
}

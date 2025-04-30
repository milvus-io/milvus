package txn

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// NewTxnManager creates a new transaction manager.
// incoming buffer is used to recover the uncommitted messages for txn manager.
func NewTxnManager(pchannel types.PChannelInfo, uncommittedTxnBuilders map[message.TxnID]*message.ImmutableTxnMessageBuilder) *TxnManager {
	m := metricsutil.NewTxnMetrics(pchannel.Name)
	sessions := make(map[message.TxnID]*TxnSession, len(uncommittedTxnBuilders))
	recoveredSessions := make(map[message.TxnID]struct{}, len(uncommittedTxnBuilders))
	sessionIDs := make([]int64, 0, len(uncommittedTxnBuilders))
	for _, builder := range uncommittedTxnBuilders {
		beginMessages, body := builder.Messages()
		session := newTxnSession(
			beginMessages.VChannel(),
			*beginMessages.TxnContext(), // must be the txn message.
			beginMessages.TimeTick(),
			m.BeginTxn(),
		)
		for _, msg := range body {
			session.AddNewMessage(context.Background(), msg.TimeTick())
			session.AddNewMessageDoneAndKeepalive(msg.TimeTick())
		}
		sessions[session.TxnContext().TxnID] = session
		recoveredSessions[session.TxnContext().TxnID] = struct{}{}
		sessionIDs = append(sessionIDs, int64(session.TxnContext().TxnID))
	}
	txnManager := &TxnManager{
		mu:                        sync.Mutex{},
		recoveredSessions:         recoveredSessions,
		recoveredSessionsDoneChan: make(chan struct{}),
		sessions:                  sessions,
		closed:                    nil,
		metrics:                   m,
	}
	txnManager.notifyRecoverDone()
	txnManager.SetLogger(resource.Resource().Logger().With(log.FieldComponent("txn-manager")))
	txnManager.Logger().Info("txn manager recovered with txn", zap.Int64s("txnIDs", sessionIDs))
	return txnManager
}

// TxnManager is the manager of transactions.
// We don't support cross wal transaction by now and
// We don't support the transaction lives after the wal transferred to another streaming node.
type TxnManager struct {
	log.Binder

	mu                        sync.Mutex
	recoveredSessions         map[message.TxnID]struct{}
	recoveredSessionsDoneChan chan struct{}
	sessions                  map[message.TxnID]*TxnSession
	closed                    lifetime.SafeChan
	metrics                   *metricsutil.TxnMetrics
}

// RecoverDone returns a channel that is closed when all transactions are cleaned up.
func (m *TxnManager) RecoverDone() <-chan struct{} {
	return m.recoveredSessionsDoneChan
}

// BeginNewTxn starts a new transaction with a session.
// We only support a transaction work on a streaming node, once the wal is transferred to another node,
// the transaction is treated as expired (rollback), and user will got a expired error, then perform a retry.
func (m *TxnManager) BeginNewTxn(ctx context.Context, msg message.MutableBeginTxnMessageV2) (*TxnSession, error) {
	timetick := msg.TimeTick()
	vchannel := msg.VChannel()
	keepalive := time.Duration(msg.Header().KeepaliveMilliseconds) * time.Millisecond

	if keepalive == 0 {
		// If keepalive is 0, the txn set the keepalive with default keepalive.
		keepalive = paramtable.Get().StreamingCfg.TxnDefaultKeepaliveTimeout.GetAsDurationByParse()
	}
	if keepalive < 1*time.Millisecond {
		return nil, status.NewInvaildArgument("keepalive must be greater than 1ms")
	}
	id, err := resource.Resource().IDAllocator().Allocate(ctx)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	// The manager is on graceful shutdown.
	// Avoid creating new transactions.
	if m.closed != nil {
		return nil, status.NewTransactionExpired("manager closed")
	}
	txnCtx := message.TxnContext{
		TxnID:     message.TxnID(id),
		Keepalive: keepalive,
	}
	session := newTxnSession(vchannel, txnCtx, timetick, m.metrics.BeginTxn())
	m.sessions[session.TxnContext().TxnID] = session
	return session, nil
}

// FailTxnAtVChannel fails all transactions at the specified vchannel.
func (m *TxnManager) FailTxnAtVChannel(vchannel string) {
	// avoid the txn to be committed.
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]int64, 0, len(m.sessions))
	for id, session := range m.sessions {
		if session.VChannel() == vchannel {
			session.Cleanup()
			delete(m.sessions, id)
			delete(m.recoveredSessions, id)
			ids = append(ids, int64(id))
		}
	}
	if len(ids) > 0 {
		m.Logger().Info("transaction interrupted", zap.String("vchannel", vchannel), zap.Int64s("txnIDs", ids))
	}
	m.notifyRecoverDone()
}

// CleanupTxnUntil cleans up the transactions until the specified timestamp.
func (m *TxnManager) CleanupTxnUntil(ts uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for id, session := range m.sessions {
		if session.IsExpiredOrDone(ts) {
			session.Cleanup()
			delete(m.sessions, id)
			delete(m.recoveredSessions, id)
		}
	}

	// If the manager is on graceful shutdown and all transactions are cleaned up.
	if len(m.sessions) == 0 && m.closed != nil {
		m.closed.Close()
	}

	m.notifyRecoverDone()
}

// notifyRecoverDone notifies the recover done channel if all transactions from recover info is done.
func (m *TxnManager) notifyRecoverDone() {
	if len(m.recoveredSessions) == 0 && m.recoveredSessions != nil {
		close(m.recoveredSessionsDoneChan)
		m.recoveredSessions = nil
	}
}

// GetSessionOfTxn returns the session of the transaction.
func (m *TxnManager) GetSessionOfTxn(id message.TxnID) (*TxnSession, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	session, ok := m.sessions[id]
	if !ok {
		return nil, status.NewTransactionExpired("not found in manager")
	}
	return session, nil
}

// GracefulClose waits for all transactions to be cleaned up.
func (m *TxnManager) GracefulClose(ctx context.Context) error {
	defer m.metrics.Close()

	m.mu.Lock()
	if m.closed == nil {
		m.closed = lifetime.NewSafeChan()
		if len(m.sessions) == 0 {
			m.closed.Close()
		}
	}
	m.Logger().Info("graceful close txn manager", zap.Int("activeTxnCount", len(m.sessions)))
	m.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.closed.CloseCh():
		return nil
	}
}

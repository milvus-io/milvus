package txn

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var (
	ErrInvalidTxnState = errors.New("invalid txn state")
	ErrSessionExpired  = errors.New("txn session expired")
	ErrManagerClosed   = errors.New("txn manager closed")
)

// NewTxnManager creates a new transaction manager.
func NewTxnManager() *TxnManager {
	return &TxnManager{
		mu:          sync.Mutex{},
		sessionHeap: typeutil.NewHeap[*TxnSession](&txnSessionHeapArrayOrderByEndTSO{}),
		sessions:    make(map[message.TxnID]*TxnSession),
		closed:      nil,
	}
}

// TxnManager is the manager of transactions.
// We don't support cross wal transaction by now and
// We don't support the transaction lives after the wal transferred to another streaming node.
type TxnManager struct {
	mu sync.Mutex

	sessionHeap typeutil.Heap[*TxnSession]
	sessions    map[message.TxnID]*TxnSession
	closed      chan struct{}
}

// BeginNewTxn starts a new transaction with a session.
// We only support a transaction work on a streaming node, once the wal is transfered to another node,
// the transaction is treated as expired (rollback), and user will got a expired error, then perform a retry.
func (m *TxnManager) BeginNewTxn(ctx context.Context, beginTSO uint64, ttl time.Duration) (*TxnSession, error) {
	id, err := resource.Resource().IDAllocator().Allocate(ctx)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	// The manager is on graceful shutdown.
	// Avoid creating new transactions.
	if m.closed != nil {
		return nil, ErrManagerClosed
	}
	session := &TxnSession{
		mu: sync.Mutex{},
		txnContext: message.TxnContext{
			TxnID:    message.TxnID(id),
			BeginTSO: beginTSO,
			TTL:      ttl,
		},
		inFlightCount: 0,
		state:         message.TxnStateBegin,
		doneWait:      nil,
		rollback:      false,
	}
	session.expiredTimeTick = session.txnContext.ExpiredTimeTick()

	m.sessions[session.TxnContext().TxnID] = session
	m.sessionHeap.Push(session)
	return session, nil
}

// CleanupTxnUntil cleans up the transactions until the specified timestamp.
func (m *TxnManager) CleanupTxnUntil(ts uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for m.sessionHeap.Len() > 0 && m.sessionHeap.Peek().IsExpiredOrDone(ts) {
		session := m.sessionHeap.Pop()
		// Cleanup the session from the manager,
		// the expired session's cleanup will be called by the manager.
		session.Cleanup()
		delete(m.sessions, session.TxnContext().TxnID)
	}

	// If the manager is on graceful shutdown and all transactions are cleaned up.
	if m.sessionHeap.Len() == 0 && m.closed != nil {
		close(m.closed)
	}
}

// GetSessionOfTxn returns the session of the transaction.
func (m *TxnManager) GetSessionOfTxn(id message.TxnID) (*TxnSession, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	session, ok := m.sessions[id]
	if !ok {
		return nil, ErrSessionExpired
	}
	return session, nil
}

// GracefulClose waits for all transactions to be cleaned up.
func (m *TxnManager) GracefulClose() {
	m.mu.Lock()
	if m.closed == nil {
		m.closed = make(chan struct{})
		if len(m.sessions) == 0 {
			close(m.closed)
		}
	}
	m.mu.Unlock()

	<-m.closed
}

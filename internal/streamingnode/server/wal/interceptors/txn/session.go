package txn

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

type txnSessionKeyType int

var txnSessionKeyValue txnSessionKeyType = 1

// TxnSession is a session for a transaction.
type TxnSession struct {
	mu sync.Mutex

	txnContext       message.TxnContext // transaction id of the session
	expiredTimeTick  uint64             // The expired time tick of the session.
	inFlightCount    int                // The message is in flight count of the session.
	state            message.TxnState   // The state of the session.
	doneWait         chan struct{}      // The channel for waiting the transaction commited.
	rollback         bool               // The flag indicates the transaction is rollbacked.
	cleanupCallbacks []func()           // The cleanup callbacks function for the session.
}

// TxnContext returns the txn context of the session.
func (s *TxnSession) TxnContext() message.TxnContext {
	return s.txnContext
}

// EndTso returns the end tso of the session.
func (s *TxnSession) EndTSO() uint64 {
	return s.expiredTimeTick // we don't support renewing lease by now, so it's a constant.
}

// BeginDone marks the transaction as in flight.
func (s *TxnSession) BeginDone() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state != message.TxnStateBegin {
		// unreachable code here.
		panic("invalid state for in flight")
	}
	s.state = message.TxnStateInFlight
}

// BeginRollback marks the transaction as rollbacked at begin state.
func (s *TxnSession) BeginRollback() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state != message.TxnStateBegin {
		// unreachable code here.
		panic("invalid state for rollback")
	}
	s.state = message.TxnStateRollbacked
}

// AddNewMessage adds a new message to the session.
func (s *TxnSession) AddNewMessage(ctx context.Context, timetick uint64) error {
	// if the txn is expired, return error.
	if err := s.checkIfExpired(timetick); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state != message.TxnStateInFlight {
		return status.NewInvalidTransactionState("AddNewMessage", message.TxnStateInFlight, s.state)
	}
	s.inFlightCount++
	return nil
}

// AddNewMessageDone decreases the in flight count of the session.
// notify the commitedWait channel if the in flight count is 0 and commited waited.
func (s *TxnSession) AddNewMessageDone() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.inFlightCount--
	if s.doneWait != nil && s.inFlightCount == 0 {
		close(s.doneWait)
	}
}

// isExpiredOrDone checks if the session is expired or done.
func (s *TxnSession) IsExpiredOrDone(ts uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.isExpiredOrDone(ts)
}

// isExpiredOrDone checks if the session is expired or done.
func (s *TxnSession) isExpiredOrDone(ts uint64) bool {
	// A timeout txn or rollbacked/commited txn should be cleared.
	// OnCommit and OnRollback session should not be cleared before timeout to
	// avoid session clear callback to be called too early.
	return s.EndTSO() <= ts || s.state == message.TxnStateRollbacked || s.state == message.TxnStateCommited
}

// RequestCommitAndWait request commits the transaction and waits for the all messages sent.
func (s *TxnSession) RequestCommitAndWait(ctx context.Context, timetick uint64) error {
	waitCh, err := s.getCommitChan(timetick, message.TxnStateOnCommit)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waitCh:
		return nil
	}
}

// CommitDone marks the transaction as commited.
func (s *TxnSession) CommitDone() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state != message.TxnStateOnCommit {
		// unreachable code here.
		panic("invalid state for commit done")
	}
	s.state = message.TxnStateCommited
	s.cleanup()
}

// RequestRollback rolls back the transaction.
func (s *TxnSession) RequestRollback(ctx context.Context, timetick uint64) error {
	waitCh, err := s.getCommitChan(timetick, message.TxnStateOnRollback)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waitCh:
		return nil
	}
}

// RollbackDone marks the transaction as rollbacked.
func (s *TxnSession) RollbackDone() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state != message.TxnStateOnRollback {
		// unreachable code here.
		panic("invalid state for rollback done")
	}
	s.state = message.TxnStateRollbacked
	s.cleanup()
}

// RegisterCleanup registers the cleanup function for the session.
// It will be called when the session is expired or done.
// !!! A commited/rollbacked or expired session will never be seen by other components.
// so the cleanup function will always be called.
func (s *TxnSession) RegisterCleanup(f func(), ts uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isExpiredOrDone(ts) {
		panic("unreachable code: register cleanup for expired or done session")
	}
	s.cleanupCallbacks = append(s.cleanupCallbacks, f)
}

// Cleanup cleans up the session.
func (s *TxnSession) Cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cleanup()
}

// cleanup calls the cleanup functions.
func (s *TxnSession) cleanup() {
	for _, f := range s.cleanupCallbacks {
		f()
	}
	s.cleanupCallbacks = nil
}

// getCommitChan returns the channel for waiting the transaction commited.
func (s *TxnSession) getCommitChan(timetick uint64, state message.TxnState) (<-chan struct{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checkIfExpired(timetick); err != nil {
		return nil, err
	}

	if s.state != message.TxnStateInFlight {
		return nil, status.NewInvalidTransactionState("GetWaitChan", message.TxnStateInFlight, s.state)
	}
	s.state = state

	if s.doneWait == nil {
		s.doneWait = make(chan struct{})
		if s.inFlightCount == 0 {
			close(s.doneWait)
		}
	}
	return s.doneWait, nil
}

// checkIfExpired checks if the session is expired.
func (s *TxnSession) checkIfExpired(tt uint64) error {
	if tt >= s.expiredTimeTick {
		return status.NewTransactionExpired(s.expiredTimeTick, tt)
	}
	return nil
}

// WithTxnSession returns a new context with the TxnSession.
func WithTxnSession(ctx context.Context, session *TxnSession) context.Context {
	return context.WithValue(ctx, txnSessionKeyValue, session)
}

// GetTxnSessionFromContext returns the TxnSession from the context.
func GetTxnSessionFromContext(ctx context.Context) *TxnSession {
	if ctx == nil {
		return nil
	}
	if v := ctx.Value(txnSessionKeyValue); v != nil {
		if session, ok := v.(*TxnSession); ok {
			return session
		}
	}
	return nil
}

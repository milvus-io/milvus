package txn

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

// TxnSession is a session for a transaction.
type TxnSession struct {
	mu sync.Mutex

	txnContext      message.TxnContext // transaction id of the session
	expiredTimeTick uint64             // The expired time tick of the session.
	inFlightCount   int                // The message is in flight count of the session.
	state           message.TxnState   // The state of the session.
	commitedWait    chan struct{}      // The channel for waiting the transaction commited.
	rollback        bool               // The flag indicates the transaction is rollbacked.
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
	if s.commitedWait != nil && s.inFlightCount == 0 {
		close(s.commitedWait)
	}
}

// IsExpiredOrDone checks if the session is expired or done.
func (s *TxnSession) IsExpiredOrDone(ts uint64) bool {
	// A timeout txn or rollbacked/commited txn should be cleared.
	return s.EndTSO() <= ts || s.state == message.TxnStateRollbacked || s.state == message.TxnStateCommited
}

// RequestCommitAndWait request commits the transaction and waits for the all messages sent.
func (s *TxnSession) RequestCommitAndWait(ctx context.Context, timetick uint64) error {
	waitCh, err := s.getCommitChan(timetick)
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

// RequestRollback rolls back the transaction.
func (s *TxnSession) RequestRollback(timetick uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checkIfExpired(timetick); err != nil {
		return err
	}
	if s.state != message.TxnStateInFlight {
		return status.NewInvalidTransactionState("Rollback", message.TxnStateInFlight, s.state)
	}
	s.state = message.TxnStateRollbacked
	return nil
}

// getCommitChan returns the channel for waiting the transaction commited.
func (s *TxnSession) getCommitChan(timetick uint64) (<-chan struct{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkIfExpired(timetick); err != nil {
		return nil, err
	}

	if s.state != message.TxnStateInFlight {
		return nil, status.NewInvalidTransactionState("Commit", message.TxnStateInFlight, s.state)
	}
	s.state = message.TxnStateCommited

	if s.commitedWait == nil {
		s.commitedWait = make(chan struct{})
		if s.inFlightCount == 0 {
			close(s.commitedWait)
		}
	}
	return s.commitedWait, nil
}

// checkIfExpired checks if the session is expired.
func (s *TxnSession) checkIfExpired(tt uint64) error {
	if tt >= s.expiredTimeTick {
		return status.NewTransactionExpired(s.expiredTimeTick, tt)
	}
	return nil
}

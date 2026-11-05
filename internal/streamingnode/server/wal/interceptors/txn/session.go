package txn

import (
	"context"
	"math"
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

type txnSessionKeyType int

var txnSessionKeyValue txnSessionKeyType = 1

// newTxnSession creates a new transaction session.
func newTxnSession(
	vchannel string,
	txnContext message.TxnContext,
	timetick uint64,
	metricsGuard *metricsutil.TxnMetricsGuard,
) *TxnSession {
	return &TxnSession{
		mu:            sync.Mutex{},
		vchannel:      vchannel,
		lastTimetick:  timetick,
		txnContext:    txnContext,
		inFlightCount: 0,
		state:         message.TxnStateInFlight,
		doneWait:      nil,
		rollback:      false,
		metricsGuard:  metricsGuard,
	}
}

// TxnSession is a session for a transaction.
type TxnSession struct {
	mu               sync.Mutex
	vchannel         string             // The vchannel of the session.
	lastTimetick     uint64             // session last timetick.
	expired          bool               // The flag indicates the transaction has trigger expired once.
	txnContext       message.TxnContext // transaction id of the session
	inFlightCount    int                // The message is in flight count of the session.
	bodyCount        uint64             // Successfully appended logical bodies, excluding backend retries.
	state            message.TxnState   // The state of the session.
	doneWait         chan struct{}      // The channel for waiting the transaction committed.
	rollback         bool               // The flag indicates the transaction is rollbacked.
	cleanupCallbacks []func()           // The cleanup callbacks function for the session.
	referencesClosed bool
	beginMessage     message.ImmutableMessage
	invalidScope     bool
	successfulBodies []message.ImmutableMessage
	hasUpsert        bool
	hasNonInsert     bool                         // A successful body requires Transform consumption.
	metricsGuard     *metricsutil.TxnMetricsGuard // The metrics guard for the session.
}

// VChannel returns the vchannel of the session.
func (s *TxnSession) VChannel() string {
	return s.vchannel
}

// TxnContext returns the txn context of the session.
func (s *TxnSession) TxnContext() message.TxnContext {
	return s.txnContext
}

// ValidateUpsertScope rejects an invalid envelope before transaction admission changes state.
func (s *TxnSession) ValidateUpsertScope(vchannel string) error {
	_, collectionID, _, err := funcutil.ParseVChannel(vchannel)
	if err != nil || collectionID <= 0 || vchannel != s.vchannel || funcutil.IsControlChannel(vchannel) {
		return status.NewUnrecoverableError("Upsert transaction body has an invalid vchannel scope")
	}
	return nil
}

// AddNewMessage adds a new message to the session.
func (s *TxnSession) AddNewMessage(ctx context.Context, timetick uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// if the txn is expired, return error.
	if err := s.checkIfExpired(timetick); err != nil {
		return err
	}

	if s.state != message.TxnStateInFlight {
		return status.NewInvalidTransactionState("AddNewMessage", message.TxnStateInFlight, s.state)
	}
	s.inFlightCount++
	return nil
}

// AddNewMessageDoneAndKeepalive decreases the in flight count of the session and keepalive the session.
// notify the committedWait channel if the in flight count is 0 and committed waited.
func (s *TxnSession) AddNewMessageDoneAndKeepalive(timetick uint64, typ message.MessageType) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if typ != message.MessageTypeInsert && typ != message.MessageTypeUpsert {
		s.hasNonInsert = true
	}

	// make a refresh lease here.
	if s.lastTimetick < timetick {
		s.lastTimetick = timetick
	}
	s.inFlightCount--
	s.bodyCount++
	if s.doneWait != nil && s.inFlightCount == 0 {
		close(s.doneWait)
	}
}

// IsInsertOnly is stable after RequestCommitAndWait succeeds. Record successful
// body types before releasing their in-flight slots, including recovered bodies.
func (s *TxnSession) IsInsertOnly() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bodyCount > 0 && !s.hasNonInsert
}

// RecordBegin retains the successful Begin envelope for complete Commit validation.
func (s *TxnSession) RecordBegin(msg message.ImmutableMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.referencesClosed {
		return
	}
	s.beginMessage = msg
}

// BeginMessage returns the retained Begin envelope.
func (s *TxnSession) BeginMessage() message.ImmutableMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.beginMessage
}

// HasInvalidScope preserves invalid recovered-prefix evidence until transaction resolution.
func (s *TxnSession) HasInvalidScope() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.invalidScope
}

// RecordSuccessfulBody retains a successful body before its in-flight slot is released.
func (s *TxnSession) RecordSuccessfulBody(msg message.ImmutableMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.referencesClosed {
		return
	}
	_, collectionID, _, err := funcutil.ParseVChannel(msg.VChannel())
	s.invalidScope = s.invalidScope || err != nil || collectionID <= 0 || msg.VChannel() != s.vchannel || msg.TxnContext() == nil || msg.TxnContext().TxnID != s.txnContext.TxnID
	s.successfulBodies = append(s.successfulBodies, msg)
	s.hasUpsert = s.hasUpsert || msg.MessageType() == message.MessageTypeUpsert
	if msg.MessageType() == message.MessageTypeUpsert && message.MustAsImmutableUpsertMessageV2(msg).Header().GetDeleteRows() > 0 {
		s.hasNonInsert = true
	}
}

// HasUpsert reports whether a successful body uses the Upsert protocol.
func (s *TxnSession) HasUpsert() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hasUpsert
}

// SuccessfulBodies returns an independent reference list. Commit admission freezes its input first.
func (s *TxnSession) SuccessfulBodies() []message.ImmutableMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]message.ImmutableMessage(nil), s.successfulBodies...)
}

// AddNewMessageFail decreases the in flight count of the session but not refresh the lease.
func (s *TxnSession) AddNewMessageFail() {
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

// State returns the state of the session.
func (s *TxnSession) State() message.TxnState {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.state
}

// isExpiredOrDone checks if the session is expired or done.
func (s *TxnSession) isExpiredOrDone(ts uint64) bool {
	// A timeout txn or rollbacked/committed txn should be cleared.
	// OnCommit and OnRollback session should not be cleared before timeout to
	// avoid session clear callback to be called too early.
	return s.expiredTimeTick() <= ts || s.state == message.TxnStateRollbacked || s.state == message.TxnStateCommitted
}

// expiredTimeTick returns the expired time tick of the session.
func (s *TxnSession) expiredTimeTick() uint64 {
	if s.txnContext.Keepalive == message.TxnKeepaliveInfinite {
		return math.MaxUint64
	}
	return tsoutil.AddPhysicalDurationOnTs(s.lastTimetick, s.txnContext.Keepalive)
}

// RequestCommitAndWait request commits the transaction and waits for the all messages sent.
func (s *TxnSession) RequestCommitAndWait(ctx context.Context, timetick uint64, bodyCnt uint64) error {
	waitCh, err := s.getDoneChan(timetick, message.TxnStateOnCommit)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waitCh:
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.hasUpsert && bodyCnt != 0 && bodyCnt != s.bodyCount {
			// No Commit has reached WAL. End this attempt and release references;
			// the producer retries the entire transaction with a fresh TxnID.
			s.state = message.TxnStateRollbacked
			s.cleanup()
			return status.NewTransactionExpired("txn %d body count mismatch: expected %d, appended %d", s.txnContext.TxnID, bodyCnt, s.bodyCount)
		}
		// Classify only after all admitted appends finish: an in-flight body
		// may still succeed, and a failed append does not make a transaction nonempty.
		if s.bodyCount == 0 {
			s.state = message.TxnStateRollbacked
			s.cleanup()
			return status.NewInvalidArgument("cannot commit an empty transaction")
		}
		return nil
	}
}

// CommitDone marks the transaction as committed.
func (s *TxnSession) CommitDone() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state != message.TxnStateOnCommit {
		// unreachable code here.
		panic("invalid state for commit done")
	}
	s.state = message.TxnStateCommitted
	s.cleanup()
}

// RejectCommit marks the transaction as rollbacked after commit admission rejects it.
func (s *TxnSession) RejectCommit() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state != message.TxnStateOnCommit {
		// unreachable code here.
		panic("invalid state for commit rejection")
	}
	s.state = message.TxnStateRollbacked
	s.cleanup()
}

// RequestRollback rolls back the transaction.
func (s *TxnSession) RequestRollback(ctx context.Context, timetick uint64) error {
	waitCh, err := s.getDoneChan(timetick, message.TxnStateOnRollback)
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
// !!! A committed/rollbacked or expired session will never be seen by other components.
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

// abort terminates a session under the exclusive WAL lock. Marking the outcome
// lets LastConfirmedMessageID advance without waiting for the session's lease.
func (s *TxnSession) abort() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state != message.TxnStateCommitted {
		s.state = message.TxnStateRollbacked
	}
	s.cleanup()
}

// cleanup calls the cleanup functions.
func (s *TxnSession) cleanup() {
	for _, f := range s.cleanupCallbacks {
		f()
	}
	s.metricsGuard.Done(s.state)
	s.metricsGuard = nil
	s.cleanupCallbacks = nil
	s.referencesClosed = true
	s.beginMessage = nil
	s.invalidScope = false
	s.successfulBodies = nil
	s.hasUpsert = false
}

// getDoneChan returns the channel for waiting the transaction committed.
func (s *TxnSession) getDoneChan(timetick uint64, state message.TxnState) (<-chan struct{}, error) {
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
	if s.expired {
		return status.NewTransactionExpired("some message of txn %d has been expired, expired at %d, current %d", s.txnContext.TxnID, s.expiredTimeTick(), tt)
	}
	expiredTimeTick := s.expiredTimeTick()
	if tt >= expiredTimeTick {
		// once the session is expired, it will never be active again.
		s.expired = true
		return status.NewTransactionExpired("txn %d expired at %d, current %d", s.txnContext.TxnID, expiredTimeTick, tt)
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

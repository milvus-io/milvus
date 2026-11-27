package partialupdate

import (
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

// partialUpdateState owns write tracking and CAS admission for one WAL lifecycle.
type partialUpdateState struct {
	channel types.PChannelInfo
	// Immutable lower bound for proofs backed by this WAL lifecycle's state.
	historyStartTs uint64
	pkVersions     *pkVersionIndex
	fences         *collectionFenceIndex
	// incompleteTxnFences records replicated commits whose write set is not
	// fully known in this WAL lifecycle.
	incompleteTxnFences *vchannelFenceIndex
	rowLocks            *rowLockManager
	txnMu               sync.RWMutex
	txns                map[message.TxnID]*pendingTxn
}

func newPartialUpdateState(versionIndexTTL time.Duration, maxVersionIndexBytes int64) *partialUpdateState {
	return newPartialUpdateStateWithBudget(
		versionIndexTTL,
		newVersionByteBudget(maxVersionIndexBytes),
	)
}

func newPartialUpdateStateWithBudget(
	versionIndexTTL time.Duration,
	versionIndexBudget *versionByteBudget,
) *partialUpdateState {
	return &partialUpdateState{
		pkVersions:          newPKVersionIndexWithBudget(versionIndexTTL, versionIndexBudget),
		fences:              newCollectionFenceIndex(),
		incompleteTxnFences: newVChannelFenceIndex(),
		rowLocks:            globalRowLockManager,
		txns:                make(map[message.TxnID]*pendingTxn),
	}
}

// removeDroppedVChannel releases all proof state after DropCollection is
// durably appended under the vchannel exclusive lock.
func (s *partialUpdateState) removeDroppedVChannel(vchannel string, collectionID int64) {
	s.pkVersions.Remove(vchannel)
	s.incompleteTxnFences.Remove(vchannel)
	s.fences.Remove(vchannel, collectionID)
}

func (s *partialUpdateState) registerTxnCleanup(session *txn.TxnSession, timetick uint64) {
	if session == nil {
		return
	}
	txnID := session.TxnContext().TxnID
	s.txnMu.Lock()
	txnState := s.txnLocked(txnID)
	if txnState.cleanupRegistered {
		s.txnMu.Unlock()
		return
	}
	txnState.cleanupRegistered = true
	txnState.session = session
	s.txnMu.Unlock()

	// Session cleanup holds the session mutex, so register outside txnMu.
	// CDC may replace a session while replaying the same source transaction.
	session.RegisterCleanup(func() {
		s.txnMu.Lock()
		defer s.txnMu.Unlock()
		if s.txns[txnID] == txnState {
			delete(s.txns, txnID)
		}
	}, timetick)
}

// prepareTxnBegin follows the session created by TimeTick before the WAL append.
// A replayed Begin can reuse the source TxnID with a new session. Its write set
// and recovery flags must start empty, and cleanup must belong to that session.
// A failed Begin remains incomplete until another Begin is durably appended.
func (s *partialUpdateState) prepareTxnBegin(session *txn.TxnSession) {
	if session == nil {
		return
	}
	txnID := session.TxnContext().TxnID
	s.txnMu.Lock()
	defer s.txnMu.Unlock()
	if current := s.txns[txnID]; current == nil || current.session != session {
		s.txns[txnID] = &pendingTxn{session: session}
	}
}

// checkTxnWriteSetComplete reports whether the PKs of every body of a local
// transaction are tracked in the current WAL lifecycle. Local CAS proofs do not
// survive a WAL lifecycle change, so a recovered local CAS is never complete.
// A replicated commit is exempt: the source cluster already committed it, so
// it is admitted as before row locks and fences its vchannel when its write
// set is incomplete.
func checkTxnWriteSetComplete(msg message.MutableMessage, txnID message.TxnID, txnState *pendingTxn) error {
	if msg.ReplicateHeader() != nil {
		return nil
	}
	if message.HasPartialUpdateCAS(msg) &&
		(txnState == nil || !txnState.observedBegin || txnState.recovered) {
		return status.NewPartialUpdateRetryable(
			"partial update transaction %d has no complete runtime proof in the current WAL lifecycle", txnID,
		)
	}
	if txnState == nil || !txnState.observedBegin {
		return status.NewTransactionExpired(
			"transaction %d write set could not be recovered; replay the full transaction", txnID,
		)
	}
	return nil
}

// admitTxnBody enforces the body side of the transaction ordering contract for
// local transactions: BeginTxn takes effect before any body, and every body
// takes effect before CommitTxn.
//
// BeginTxn first: the producer learns the TxnID only from the BeginTxn
// response. A body without a live session finds no record here and is
// rejected by the TimeTick session lookup; it must not create a record that no
// cleanup would remove.
//
// Bodies before CommitTxn: a body is counted from here until its append
// returns. Once a CommitTxn has arrived, any later body is rejected.
func (s *partialUpdateState) admitTxnBody(txnID message.TxnID) (func(), error) {
	s.txnMu.Lock()
	defer s.txnMu.Unlock()
	txnState := s.txns[txnID]
	if txnState == nil {
		return func() {}, nil
	}
	if txnState.commitArrived {
		return nil, status.NewTransactionExpired(
			"transaction %d received a body after its CommitTxn arrived; replay the full transaction",
			txnID,
		)
	}
	txnState.inFlightBodies++
	return func() {
		s.txnMu.Lock()
		defer s.txnMu.Unlock()
		txnState.inFlightBodies--
	}, nil
}

// admitTxnCommit enforces the commit side of the ordering contract and returns
// the PKs to lock. It freezes the write set on arrival, so admitTxnBody rejects
// every later body, and rejects the commit while a body is still in flight:
// every body must have returned before CommitTxn is sent. Otherwise admission
// does not judge the commit: with no record it returns no PKs, and TimeTick and
// validateCommit decide the outcome.
func (s *partialUpdateState) admitTxnCommit(txnID message.TxnID) (primaryKeys, error) {
	s.txnMu.Lock()
	defer s.txnMu.Unlock()
	txnState := s.txns[txnID]
	if txnState == nil {
		return primaryKeys{}, nil
	}
	txnState.commitArrived = true
	if txnState.commitRejected || txnState.inFlightBodies > 0 {
		// A rejected transaction stays rejected, so a resent copy of the same
		// CommitTxn cannot commit it after its stale body returns.
		txnState.commitRejected = true
		return primaryKeys{}, status.NewTransactionExpired(
			"transaction %d had a body in flight when its CommitTxn arrived; replay the full transaction",
			txnID,
		)
	}
	return txnState.pks.clone(), nil
}

// validateCommit snapshots a completed transaction and validates local CAS
// proof before CommitTxn reaches the WAL.
func (s *partialUpdateState) validateCommit(msg message.MutableMessage, txnID message.TxnID) (*pendingTxn, error) {
	if msg == nil || msg.VChannel() == "" {
		return nil, status.NewUnrecoverableError("partial update commit vchannel is empty")
	}
	txnState := s.getTxn(txnID)
	if err := checkTxnWriteSetComplete(msg, txnID, txnState); err != nil {
		return nil, err
	}
	marker := message.HasPartialUpdateCAS(msg)
	if txnState != nil && txnState.meta != nil && !marker {
		return nil, status.NewUnrecoverableError("partial update transaction %d commit marker is missing", txnID)
	}
	if txnState == nil || !txnState.observedBegin {
		// Only a replicated commit gets here; publishCommit fences its vchannel.
		return txnState, nil
	}
	if txnState.meta == nil {
		if marker {
			return nil, status.NewUnrecoverableError("partial update transaction %d has no CAS proof", txnID)
		}
		return txnState, nil
	}
	if !txnState.casScopeSet {
		return nil, status.NewUnrecoverableError(
			"partial update transaction %d has no derived CAS scope",
			txnID,
		)
	}
	if txnState.pks.Len() == 0 {
		return nil, status.NewUnrecoverableError("partial update transaction %d has no primary key writes", txnID)
	}
	if msg.ReplicateHeader() != nil {
		return txnState, nil
	}
	if txnState.meta.GetObservedPchannelTerm() != s.channel.Term {
		return nil, status.NewPartialUpdateRetryable(
			"partial update observed term %d, current term %d",
			txnState.meta.GetObservedPchannelTerm(),
			s.channel.Term,
		)
	}
	if s.historyStartTs == 0 {
		return nil, status.NewUnrecoverableError("partial update WAL history start timestamp is unavailable")
	}
	if txnState.meta.GetReadTs() < s.historyStartTs {
		return nil, status.NewPartialUpdateRetryable(
			"partial update read ts %d is older than WAL history start ts %d, vchannel: %s",
			txnState.meta.GetReadTs(), s.historyStartTs, msg.VChannel(),
		)
	}
	if err := s.incompleteTxnFences.Verify(msg.VChannel(), txnState.meta.GetReadTs()); err != nil {
		return nil, err
	}
	if err := s.pkVersions.VerifyTyped(msg.VChannel(), txnState.pks, txnState.meta.GetReadTs(), msg.TimeTick()); err != nil {
		return nil, err
	}
	if err := s.fences.Verify(msg.VChannel(), txnState.collectionID, txnState.meta.GetReadTs()); err != nil {
		return nil, err
	}
	return txnState, nil
}

func (s *partialUpdateState) publishCommit(msg message.MutableMessage, txnState *pendingTxn) {
	if txnState == nil || !txnState.observedBegin {
		// A replicated commit whose write set is not fully known: invalidate
		// every earlier local CAS read on its vchannel.
		s.pkVersions.Advance(msg.TimeTick())
		s.incompleteTxnFences.Update(msg.VChannel(), msg.TimeTick())
		return
	}
	if txnState.pks.Len() > 0 {
		s.pkVersions.UpdateAllTyped(msg.VChannel(), txnState.pks, msg.TimeTick())
	} else {
		s.pkVersions.Advance(msg.TimeTick())
	}
}

// pendingTxn joins optional CAS proof and derived scope with writes observed in one runtime
// transaction. observedBegin distinguishes a complete runtime write set from
// a recovered transaction for which only a body suffix or CommitTxn was seen.
type pendingTxn struct {
	session           *txn.TxnSession
	pks               primaryKeys
	meta              *messagespb.PartialUpdateCAS
	collectionID      int64
	schemaVersion     int32
	casScopeSet       bool
	observedBegin     bool
	recovered         bool
	cleanupRegistered bool
	// inFlightBodies counts bodies between the lock interceptor and the end
	// of their append. commitArrived is set as soon as a CommitTxn reaches
	// admission; later bodies are rejected, so the write set cannot grow after
	// the commit snapshots it.
	// commitRejected keeps every later CommitTxn of the transaction rejected
	// once one arrived while a body was in flight.
	inFlightBodies int
	commitArrived  bool
	commitRejected bool
}

func (s *partialUpdateState) txnLocked(txnID message.TxnID) *pendingTxn {
	txnState := s.txns[txnID]
	if txnState == nil {
		txnState = &pendingTxn{}
		s.txns[txnID] = txnState
	}
	return txnState
}

func (s *partialUpdateState) recordTxnWritesTyped(txnID message.TxnID, pks primaryKeys) {
	if pks.Len() == 0 {
		return
	}
	s.txnMu.Lock()
	defer s.txnMu.Unlock()
	txnState := s.txnLocked(txnID)
	txnState.pks.append(pks)
}

func (s *partialUpdateState) recordTxnWrites(txnID message.TxnID, pks []any) {
	keys, err := primaryKeysFromAny(pks)
	if err != nil {
		panic(err)
	}
	s.recordTxnWritesTyped(txnID, keys)
}

func (s *partialUpdateState) recordTxnBegin(txnID message.TxnID) {
	s.txnMu.Lock()
	defer s.txnMu.Unlock()
	s.txnLocked(txnID).observedBegin = true
}

func (s *partialUpdateState) recordTxnCAS(
	txnID message.TxnID,
	meta *messagespb.PartialUpdateCAS,
	scope casInsertScope,
) error {
	if meta == nil {
		return nil
	}
	if scope.collectionID == 0 {
		return status.NewUnrecoverableError(
			"partial update txn %d carries an empty collection id",
			txnID,
		)
	}
	s.txnMu.Lock()
	defer s.txnMu.Unlock()
	txnState := s.txnLocked(txnID)
	if txnState.meta == nil {
		txnState.meta = proto.Clone(meta).(*messagespb.PartialUpdateCAS)
		txnState.collectionID = scope.collectionID
		txnState.schemaVersion = scope.schemaVersion
		txnState.casScopeSet = true
		return nil
	}
	if !proto.Equal(txnState.meta, meta) {
		return status.NewUnrecoverableError("partial update txn %d carries different CAS proof", txnID)
	}
	if !txnState.casScopeSet ||
		txnState.collectionID != scope.collectionID ||
		txnState.schemaVersion != scope.schemaVersion {
		return status.NewUnrecoverableError(
			"partial update txn %d mixes CAS scope collection/schema %d/%d and %d/%d",
			txnID,
			txnState.collectionID,
			txnState.schemaVersion,
			scope.collectionID,
			scope.schemaVersion,
		)
	}
	return nil
}

func (s *partialUpdateState) getTxn(txnID message.TxnID) *pendingTxn {
	s.txnMu.RLock()
	defer s.txnMu.RUnlock()
	txnState := s.txns[txnID]
	if txnState == nil {
		return nil
	}
	return &pendingTxn{
		session:           txnState.session,
		pks:               txnState.pks.clone(),
		meta:              cloneCASMeta(txnState.meta),
		collectionID:      txnState.collectionID,
		schemaVersion:     txnState.schemaVersion,
		casScopeSet:       txnState.casScopeSet,
		observedBegin:     txnState.observedBegin,
		recovered:         txnState.recovered,
		cleanupRegistered: txnState.cleanupRegistered,
	}
}

func cloneCASMeta(meta *messagespb.PartialUpdateCAS) *messagespb.PartialUpdateCAS {
	if meta == nil {
		return nil
	}
	return proto.Clone(meta).(*messagespb.PartialUpdateCAS)
}

func (s *partialUpdateState) deleteTxn(txnID message.TxnID) {
	s.txnMu.Lock()
	defer s.txnMu.Unlock()
	delete(s.txns, txnID)
}

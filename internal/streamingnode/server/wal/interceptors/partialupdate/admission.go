package partialupdate

import (
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility/primarykey"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

// partialUpdateState owns write history and CAS proof for one WAL lifecycle,
// and uses the process-wide row-lock manager for write coordination.
type partialUpdateState struct {
	channel types.PChannelInfo
	// Immutable lower bound for proofs backed by this WAL lifecycle's state.
	historyStartTs uint64
	pkVersions     *pkVersionIndex
	fences         *collectionFenceIndex
	rowLocks       *rowLockManager
	txnMu          sync.RWMutex
	txns           map[message.TxnID]*pendingTxn
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
		pkVersions: newPKVersionIndexWithBudget(versionIndexTTL, versionIndexBudget),
		fences:     newCollectionFenceIndex(),
		rowLocks:   globalRowLockManager,
		txns:       make(map[message.TxnID]*pendingTxn),
	}
}

// removeDroppedVChannel releases all proof state after DropCollection is
// durably appended under the vchannel exclusive lock.
func (s *partialUpdateState) removeDroppedVChannel(vchannel string, collectionID int64) {
	s.pkVersions.Remove(vchannel)
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

// completeTxnWriteSet requires every guarded body to have its PKs tracked.
// Recovery restores the durable prefix before accepting subsequent bodies.
// Legacy schema-less inserts may be explicitly exempt from row coordination.
func (s *partialUpdateState) completeTxnWriteSet(msg message.MutableMessage, txnID message.TxnID) (*pendingTxn, error) {
	txnState := s.getTxn(txnID)
	if err := checkTxnWriteSetComplete(msg, txnID, txnState); err != nil {
		return nil, err
	}
	return txnState, nil
}

func checkTxnWriteSetComplete(msg message.MutableMessage, txnID message.TxnID, txnState *pendingTxn) error {
	if msg.ReplicateHeader() == nil && message.HasPartialUpdateCAS(msg) && txnState != nil && txnState.recovered {
		return status.NewPartialUpdateRetryable("partial update transaction %d was recovered under a new WAL lifecycle", txnID)
	}
	if txnState == nil || !txnState.observedBegin || txnState.recoveryIncomplete {
		if message.HasPartialUpdateCAS(msg) && msg.ReplicateHeader() == nil {
			return status.NewPartialUpdateRetryable(
				"partial update transaction %d has no complete runtime write set in the current WAL lifecycle",
				txnID,
			)
		}
		return status.NewTransactionExpired(
			"transaction %d has no complete runtime write set in the current WAL lifecycle; replay the full transaction",
			txnID,
		)
	}
	return nil
}

// admitTxnBody counts a transaction body from the lock interceptor until its
// append returns. Bodies record their PKs downstream of TimeTick, so CommitTxn
// must not lock its write set while a body is still in flight.
func (s *partialUpdateState) admitTxnBody(txnID message.TxnID) (func(), error) {
	s.txnMu.Lock()
	defer s.txnMu.Unlock()
	// A live session always has a record, created at Begin or recovery. Do not
	// create one here: a late body of a cleaned-up transaction is rejected by
	// TimeTick and would otherwise leave a record that no cleanup removes.
	txnState := s.txns[txnID]
	if txnState == nil {
		return func() {}, nil
	}
	if txnState.commitAdmitted {
		return nil, status.NewTransactionExpired(
			"transaction %d received a body after its CommitTxn took row locks; replay the full transaction",
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

// admitTxnCommit validates that the transaction has a complete write set, then
// freezes it and returns the PKs to lock. Producers send CommitTxn only after
// every body append has returned; a body still in flight means that contract
// was broken, so the commit is rejected rather than locking a partial set.
func (s *partialUpdateState) admitTxnCommit(msg message.MutableMessage, txnID message.TxnID) (primarykey.Keys, error) {
	// Check, freeze and snapshot the live record in one critical section so
	// that no body can be admitted between the check and the freeze.
	s.txnMu.Lock()
	defer s.txnMu.Unlock()
	txnState := s.txns[txnID]
	if err := checkTxnWriteSetComplete(msg, txnID, txnState); err != nil {
		return primarykey.Keys{}, err
	}
	if txnState.inFlightBodies > 0 {
		if message.HasPartialUpdateCAS(msg) && msg.ReplicateHeader() == nil {
			return primarykey.Keys{}, status.NewPartialUpdateRetryable(
				"partial update transaction %d has a body append in flight at commit admission", txnID)
		}
		return primarykey.Keys{}, status.NewTransactionExpired(
			"transaction %d has a body append in flight at commit admission; replay the full transaction", txnID)
	}
	txnState.commitAdmitted = true
	return txnState.pks.Clone(), nil
}

// validateCommit snapshots a completed transaction and validates local CAS
// proof before CommitTxn reaches the WAL.
func (s *partialUpdateState) validateCommit(msg message.MutableMessage, txnID message.TxnID) (*pendingTxn, error) {
	if msg == nil || msg.VChannel() == "" {
		return nil, status.NewUnrecoverableError("partial update commit vchannel is empty")
	}
	txnState, err := s.completeTxnWriteSet(msg, txnID)
	if err != nil {
		return nil, err
	}
	marker := message.HasPartialUpdateCAS(msg)
	if txnState != nil && txnState.meta != nil && !marker {
		return nil, status.NewUnrecoverableError("partial update transaction %d commit marker is missing", txnID)
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
	if txnState.fenceCollection != 0 && txnState.fenceCollection != txnState.collectionID {
		return nil, status.NewUnrecoverableError("partial update transaction %d mixes collection ids", txnID)
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
	if err := s.pkVersions.VerifyTyped(msg.VChannel(), txnState.pks, txnState.meta.GetReadTs(), msg.TimeTick()); err != nil {
		return nil, err
	}
	if err := s.fences.Verify(msg.VChannel(), txnState.collectionID, txnState.meta.GetReadTs()); err != nil {
		return nil, err
	}
	return txnState, nil
}

func (s *partialUpdateState) publishCommit(msg message.MutableMessage, txnState *pendingTxn) {
	if txnState == nil || !txnState.observedBegin || txnState.recoveryIncomplete {
		// Commit validation rejects these states before append. Do not publish
		// an incomplete write set if a future caller bypasses that validation.
		return
	}
	if txnState.pks.Len() > 0 {
		s.pkVersions.UpdateAllTyped(msg.VChannel(), txnState.pks, msg.TimeTick())
	} else {
		s.pkVersions.Advance(msg.TimeTick())
	}
	if txnState.fenceCollection != 0 {
		s.fences.Update(msg.VChannel(), txnState.fenceCollection, msg.TimeTick())
	}
}

// pendingTxn joins optional CAS proof and derived scope with the full guarded
// write set, including the recovered durable prefix. A body suffix without its
// Begin or a recovery decode failure must not masquerade as a complete set.
type pendingTxn struct {
	session            *txn.TxnSession
	pks                primarykey.Keys
	meta               *messagespb.PartialUpdateCAS
	collectionID       int64
	schemaVersion      int32
	casScopeSet        bool
	fenceCollection    int64
	observedBegin      bool
	recovered          bool
	recoveryIncomplete bool
	cleanupRegistered  bool
	// inFlightBodies counts bodies between the lock interceptor and the end
	// of their append. commitAdmitted is set once CommitTxn has taken its row
	// locks; later bodies are rejected so the locked write set cannot grow.
	inFlightBodies int
	commitAdmitted bool
}

func (s *partialUpdateState) txnLocked(txnID message.TxnID) *pendingTxn {
	txnState := s.txns[txnID]
	if txnState == nil {
		txnState = &pendingTxn{}
		s.txns[txnID] = txnState
	}
	return txnState
}

func (s *partialUpdateState) recordTxnWritesTyped(txnID message.TxnID, pks primarykey.Keys) {
	if pks.Len() == 0 {
		return
	}
	s.txnMu.Lock()
	defer s.txnMu.Unlock()
	txnState := s.txnLocked(txnID)
	txnState.pks.Append(pks)
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

func (s *partialUpdateState) recordTxnFence(txnID message.TxnID, collectionID int64) {
	if collectionID == 0 {
		return
	}
	s.txnMu.Lock()
	defer s.txnMu.Unlock()
	s.txnLocked(txnID).fenceCollection = collectionID
}

func (s *partialUpdateState) getTxn(txnID message.TxnID) *pendingTxn {
	s.txnMu.RLock()
	defer s.txnMu.RUnlock()
	txnState := s.txns[txnID]
	if txnState == nil {
		return nil
	}
	return &pendingTxn{
		session:            txnState.session,
		pks:                txnState.pks.Clone(),
		meta:               cloneCASMeta(txnState.meta),
		collectionID:       txnState.collectionID,
		schemaVersion:      txnState.schemaVersion,
		casScopeSet:        txnState.casScopeSet,
		fenceCollection:    txnState.fenceCollection,
		observedBegin:      txnState.observedBegin,
		recovered:          txnState.recovered,
		recoveryIncomplete: txnState.recoveryIncomplete,
		cleanupRegistered:  txnState.cleanupRegistered,
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

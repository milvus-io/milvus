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

// partialUpdateState owns write tracking and CAS admission for one WAL term.
type partialUpdateState struct {
	channel             types.PChannelInfo
	pkVersions          *pkVersionIndex
	fences              *collectionFenceIndex
	incompleteTxnFences *vchannelFenceIndex
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
	if !s.markTxnCleanup(txnID) {
		return
	}
	session.RegisterCleanup(func() {
		s.deleteTxn(txnID)
	}, timetick)
}

// validateCommit snapshots a completed transaction and validates local CAS
// proof before CommitTxn reaches the WAL.
func (s *partialUpdateState) validateCommit(msg message.MutableMessage, txnID message.TxnID) (*pendingTxn, error) {
	if msg == nil || msg.VChannel() == "" {
		return nil, status.NewUnrecoverableError("partial update commit vchannel is empty")
	}
	txnState := s.getTxn(txnID)
	marker := message.HasPartialUpdateCAS(msg)
	if txnState != nil && txnState.meta != nil && !marker {
		return nil, status.NewUnrecoverableError("partial update transaction %d commit marker is missing", txnID)
	}
	if txnState == nil || !txnState.observedBegin {
		if marker && msg.ReplicateHeader() == nil {
			return nil, status.NewPartialUpdateRetryable(
				"partial update transaction %d has no complete runtime proof in the current WAL lifecycle",
				txnID,
			)
		}
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
	if txnState.fenceCollection != 0 && txnState.collectionID != txnState.fenceCollection {
		return nil, status.NewUnrecoverableError(
			"partial update transaction %d mixes collection ids %d and %d",
			txnID,
			txnState.collectionID,
			txnState.fenceCollection,
		)
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
		s.pkVersions.Advance(msg.TimeTick())
		s.incompleteTxnFences.Update(msg.VChannel(), msg.TimeTick())
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

// pendingTxn joins optional CAS proof and derived scope with writes observed in one runtime
// transaction. observedBegin distinguishes a complete runtime write set from
// a recovered transaction for which only a body suffix or CommitTxn was seen.
type pendingTxn struct {
	pks               primaryKeys
	meta              *messagespb.PartialUpdateCAS
	collectionID      int64
	schemaVersion     int32
	casScopeSet       bool
	fenceCollection   int64
	observedBegin     bool
	cleanupRegistered bool
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

func (s *partialUpdateState) recordTxnFence(txnID message.TxnID, collectionID int64) {
	if collectionID == 0 {
		return
	}
	s.txnMu.Lock()
	defer s.txnMu.Unlock()
	s.txnLocked(txnID).fenceCollection = collectionID
}

func (s *partialUpdateState) markTxnCleanup(txnID message.TxnID) bool {
	s.txnMu.Lock()
	defer s.txnMu.Unlock()
	txnState := s.txnLocked(txnID)
	if txnState.cleanupRegistered {
		return false
	}
	txnState.cleanupRegistered = true
	return true
}

func (s *partialUpdateState) getTxn(txnID message.TxnID) *pendingTxn {
	s.txnMu.RLock()
	defer s.txnMu.RUnlock()
	txnState := s.txns[txnID]
	if txnState == nil {
		return nil
	}
	return &pendingTxn{
		pks:               txnState.pks.clone(),
		meta:              cloneCASMeta(txnState.meta),
		collectionID:      txnState.collectionID,
		schemaVersion:     txnState.schemaVersion,
		casScopeSet:       txnState.casScopeSet,
		fenceCollection:   txnState.fenceCollection,
		observedBegin:     txnState.observedBegin,
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

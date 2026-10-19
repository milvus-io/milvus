package replicates

import (
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// newReplicateTxnHelper creates a new replicate txn helper.
func newReplicateTxnHelper() *replicateTxnHelper {
	return &replicateTxnHelper{
		currentTxn: nil,
		messageIDs: typeutil.NewSet[string](),
	}
}

// replicateTxnHelper is a helper for replicating a txn.
// It is used to handle and deduplicate the txn messages.
type replicateTxnHelper struct {
	currentTxn *message.TxnContext
	messageIDs typeutil.Set[string]
	vchannel   string
}

// CurrentTxn returns the current txn context.
func (s *replicateTxnHelper) CurrentTxn() *message.TxnContext {
	return s.currentTxn
}

func (s *replicateTxnHelper) StartBegin(txn *message.TxnContext) error {
	if s.currentTxn != nil {
		if s.currentTxn.TxnID == txn.TxnID {
			return status.NewIgnoreOperation("txn message is already in progress, txnID: %d", s.currentTxn.TxnID)
		}
		return status.NewReplicateViolation("begin txn violation, txnID: %d, incoming: %d", s.currentTxn.TxnID, txn.TxnID)
	}
	return nil
}

func (s *replicateTxnHelper) BeginDone(txn *message.TxnContext, vchannel string) {
	s.currentTxn = txn
	s.messageIDs = typeutil.NewSet[string]()
	s.vchannel = vchannel
}

func (s *replicateTxnHelper) AddNewMessage(txn *message.TxnContext, replicateHeader *message.ReplicateHeader) error {
	if s.currentTxn == nil {
		return status.NewTransactionExpired("replicated txn %d has no active begin; replay the transaction", txn.TxnID)
	}
	if s.currentTxn.TxnID != txn.TxnID {
		return status.NewReplicateViolation("add new txn message with different txn, current: %d, incoming: %d", s.currentTxn.TxnID, txn.TxnID)
	}
	if s.messageIDs.Contain(replicateHeader.MessageID.Marshal()) {
		return status.NewIgnoreOperation("txn message is already in progress, txnID: %d, messageID: %d", s.currentTxn.TxnID, replicateHeader.MessageID)
	}
	return nil
}

func (s *replicateTxnHelper) AddNewMessageDone(replicateHeader *message.ReplicateHeader) {
	s.messageIDs.Insert(replicateHeader.MessageID.Marshal())
}

func (s *replicateTxnHelper) StartCommit(txn *message.TxnContext, bodyCnt uint64) error {
	if s.currentTxn == nil {
		return status.NewTransactionExpired("replicated txn %d has no active begin; replay the transaction", txn.TxnID)
	}
	if s.currentTxn.TxnID != txn.TxnID {
		return status.NewReplicateViolation("commit txn with different txn, current: %d, incoming: %d", s.currentTxn.TxnID, txn.TxnID)
	}
	// Duplicate source IDs never reach the WAL or increase this count. Check
	// before preparing Commit so an incomplete replay can continue receiving.
	if bodyCnt != 0 && bodyCnt != uint64(len(s.messageIDs)) {
		return status.NewTransactionExpired("replicated txn %d body count mismatch: expected %d, received %d; replay the transaction", txn.TxnID, bodyCnt, len(s.messageIDs))
	}
	return nil
}

func (s *replicateTxnHelper) CommitDone(txn *message.TxnContext) {
	s.clear()
}

func (s *replicateTxnHelper) clear() {
	s.currentTxn = nil
	s.messageIDs = nil
	s.vchannel = ""
}

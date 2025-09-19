package replicates

import (
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
}

// CurrentTxn returns the current txn context.
func (s *replicateTxnHelper) CurrentTxn() *message.TxnContext {
	return s.currentTxn
}

func (s *replicateTxnHelper) StartBegin(txn *message.TxnContext, replicateHeader *message.ReplicateHeader) error {
	if s.currentTxn != nil {
		if s.currentTxn.TxnID == txn.TxnID {
			return status.NewIgnoreOperation("txn message is already in progress, txnID: %d", s.currentTxn.TxnID)
		}
		return status.NewReplicateViolation("begin txn violation, txnID: %d, incoming: %d", s.currentTxn.TxnID, txn.TxnID)
	}
	return nil
}

func (s *replicateTxnHelper) BeginDone(txn *message.TxnContext) {
	s.currentTxn = txn
	s.messageIDs = typeutil.NewSet[string]()
}

func (s *replicateTxnHelper) AddNewMessage(txn *message.TxnContext, replicateHeader *message.ReplicateHeader) error {
	if s.currentTxn == nil {
		return status.NewReplicateViolation("add new txn message without new txn, incoming: %d", s.currentTxn.TxnID, txn.TxnID)
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

func (s *replicateTxnHelper) StartCommit(txn *message.TxnContext) error {
	if s.currentTxn == nil {
		return status.NewIgnoreOperation("commit txn without txn, maybe already committed, txnID: %d", txn.TxnID)
	}
	if s.currentTxn.TxnID != txn.TxnID {
		return status.NewReplicateViolation("commit txn with different txn, current: %d, incoming: %d", s.currentTxn.TxnID, txn.TxnID)
	}
	s.currentTxn = nil
	s.messageIDs = nil
	return nil
}

func (s *replicateTxnHelper) CommitDone(txn *message.TxnContext) {
	s.currentTxn = nil
	s.messageIDs = nil
}

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
	currentTxn    *message.TxnContext
	messageIDs    typeutil.Set[string]
	vchannel      string
	lastMessageID message.MessageID
	chained       bool
}

// CurrentTxn returns the current txn context.
func (s *replicateTxnHelper) CurrentTxn() *message.TxnContext {
	return s.currentTxn
}

func (s *replicateTxnHelper) StartBegin(txn *message.TxnContext, replicateHeader *message.ReplicateHeader) error {
	if previous := replicateHeader.PrevTxnMessageID; previous != nil && !previous.EQ(replicateHeader.MessageID) {
		return status.NewReplicateViolation("transaction begin must reference itself")
	}
	if s.currentTxn != nil {
		if s.currentTxn.TxnID == txn.TxnID {
			return status.NewIgnoreOperation("txn message is already in progress, txnID: %d", s.currentTxn.TxnID)
		}
		return status.NewReplicateViolation("begin txn violation, txnID: %d, incoming: %d", s.currentTxn.TxnID, txn.TxnID)
	}
	return nil
}

func (s *replicateTxnHelper) BeginDone(txn *message.TxnContext, header *message.ReplicateHeader, vchannel string) {
	s.currentTxn = txn
	s.messageIDs = typeutil.NewSet[string]()
	s.vchannel = vchannel
	s.lastMessageID = header.MessageID
	s.chained = header.PrevTxnMessageID != nil
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
	return s.checkPrevious(replicateHeader)
}

func (s *replicateTxnHelper) AddNewMessageDone(replicateHeader *message.ReplicateHeader) {
	s.messageIDs.Insert(replicateHeader.MessageID.Marshal())
	s.lastMessageID = replicateHeader.MessageID
	s.chained = s.chained || replicateHeader.PrevTxnMessageID != nil
}

func (s *replicateTxnHelper) StartCommit(txn *message.TxnContext, header *message.ReplicateHeader) error {
	if s.currentTxn == nil {
		return status.NewTransactionExpired("replicated txn %d has no active begin; replay the transaction", txn.TxnID)
	}
	if s.currentTxn.TxnID != txn.TxnID {
		return status.NewReplicateViolation("commit txn with different txn, current: %d, incoming: %d", s.currentTxn.TxnID, txn.TxnID)
	}
	// Check before preparing the local session: an old stream must not commit
	// a prefix that is missing a body after a flush or reconnect.
	return s.checkPrevious(header)
}

func (s *replicateTxnHelper) CommitDone(txn *message.TxnContext) {
	s.clear()
}

func (s *replicateTxnHelper) clear() {
	s.currentTxn = nil
	s.messageIDs = nil
	s.lastMessageID = nil
	s.vchannel = ""
	s.chained = false
}

func (s *replicateTxnHelper) checkPrevious(header *message.ReplicateHeader) error {
	// Legacy transactions can be replayed during an upgrade, but a checked
	// prefix must never be completed by an unchecked message from an old stream.
	if s.chained && header.PrevTxnMessageID == nil {
		return status.NewTransactionExpired("replicated txn %d requires a predecessor; replay the transaction", s.currentTxn.TxnID)
	}
	if previous := header.PrevTxnMessageID; previous != nil && !previous.EQ(s.lastMessageID) {
		return status.NewTransactionExpired("replicated txn %d has an incomplete prefix; replay the transaction", s.currentTxn.TxnID)
	}
	return nil
}

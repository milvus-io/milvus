package ack

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type uncommitedTxnInfo struct {
	session   *txn.TxnSession   // if nil, it's a non-txn(autocommit) message.
	messageID message.MessageID // the message id of the txn begins.
}

// newLastConfirmedManager creates a new last confirmed manager.
func newLastConfirmedManager(lastConfirmedMessageID message.MessageID) *lastConfirmedManager {
	return &lastConfirmedManager{
		lastConfirmedMessageID: lastConfirmedMessageID,
		notDoneTxnMessage:      typeutil.NewHeap[*uncommitedTxnInfo](&uncommitedTxnInfoOrderByMessageID{}),
	}
}

// lastConfirmedManager manages the last confirmed message id.
type lastConfirmedManager struct {
	lastConfirmedMessageID message.MessageID
	notDoneTxnMessage      typeutil.Heap[*uncommitedTxnInfo]
}

// AddConfirmedDetails adds the confirmed details.
func (m *lastConfirmedManager) AddConfirmedDetails(details sortedDetails, ts uint64) {
	for _, detail := range details {
		if detail.IsSync || detail.Err != nil {
			continue
		}
		m.notDoneTxnMessage.Push(&uncommitedTxnInfo{
			session:   detail.TxnSession,
			messageID: detail.MessageID,
		})
	}
	m.updateLastConfirmedMessageID(ts)
}

// GetLastConfirmedMessageID returns the last confirmed message id.
func (m *lastConfirmedManager) GetLastConfirmedMessageID() message.MessageID {
	return m.lastConfirmedMessageID
}

// updateLastConfirmedMessageID updates the last confirmed message id.
func (m *lastConfirmedManager) updateLastConfirmedMessageID(ts uint64) {
	for m.notDoneTxnMessage.Len() > 0 &&
		(m.notDoneTxnMessage.Peek().session == nil || m.notDoneTxnMessage.Peek().session.IsExpiredOrDone(ts)) {
		info := m.notDoneTxnMessage.Pop()
		if m.lastConfirmedMessageID.LT(info.messageID) {
			m.lastConfirmedMessageID = info.messageID
		}
	}
}

// uncommitedTxnInfoOrderByMessageID is the heap array of the txnSession.
type uncommitedTxnInfoOrderByMessageID []*uncommitedTxnInfo

func (h uncommitedTxnInfoOrderByMessageID) Len() int {
	return len(h)
}

func (h uncommitedTxnInfoOrderByMessageID) Less(i, j int) bool {
	return h[i].messageID.LT(h[j].messageID)
}

func (h uncommitedTxnInfoOrderByMessageID) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *uncommitedTxnInfoOrderByMessageID) Push(x interface{}) {
	*h = append(*h, x.(*uncommitedTxnInfo))
}

// Pop pop the last one at len.
func (h *uncommitedTxnInfoOrderByMessageID) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// Peek returns the element at the top of the heap.
func (h *uncommitedTxnInfoOrderByMessageID) Peek() interface{} {
	return (*h)[0]
}

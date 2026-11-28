package txn

import (
	"container/list"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// expiredTxns retains only definite pre-commit outcomes, under TxnManager.mu.
// It owns no sessions, callbacks or LCMID pins. Insertion follows the manager's
// monotonic TimeTick, so a FIFO suffices for both age and capacity eviction.
type expiredTxns struct {
	entries  map[message.TxnID]*list.Element
	order    list.List
	timeTick uint64
}

type expiredTxn struct {
	id       message.TxnID
	timeTick uint64
}

func (t *expiredTxns) add(id message.TxnID) {
	if t.entries == nil {
		t.entries = make(map[message.TxnID]*list.Element)
	}
	if _, ok := t.entries[id]; !ok {
		t.entries[id] = t.order.PushBack(expiredTxn{id: id, timeTick: t.timeTick})
	}
	t.advance(t.timeTick)
}

func (t *expiredTxns) remove(id message.TxnID) {
	if entry, ok := t.entries[id]; ok {
		t.order.Remove(entry)
		delete(t.entries, id)
	}
}

func (t *expiredTxns) contains(id message.TxnID) bool {
	t.advance(t.timeTick)
	_, ok := t.entries[id]
	return ok
}

func (t *expiredTxns) advance(ts uint64) {
	t.timeTick = max(t.timeTick, ts)
	if t.order.Len() == 0 {
		return
	}
	cfg := &paramtable.Get().StreamingCfg
	limit := max(0, cfg.TxnTombstoneMaxCount.GetAsInt())
	lifetime := cfg.TxnTombstoneMaxLifetime.GetAsDurationByParse()
	for first := t.order.Front(); first != nil; first = t.order.Front() {
		entry := first.Value.(expiredTxn)
		if t.order.Len() <= limit && lifetime > 0 &&
			tsoutil.AddPhysicalDurationOnTs(entry.timeTick, lifetime) > t.timeTick {
			break
		}
		t.remove(entry.id)
	}
}

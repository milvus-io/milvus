package utility

import (
	"container/heap"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// TxnTombstone contains a successful Commit result, without retaining a session
// or transaction bodies. A missing tombstone is not proof of an aborted txn.
type TxnTombstone struct {
	VChannel  string
	MessageID message.MessageID
	Result    ExtraAppendResult
}

func (t *TxnTombstone) clone() *TxnTombstone {
	copy := *t
	txnCtx := *t.Result.TxnCtx
	copy.Result.TxnCtx = &txnCtx
	if t.Result.Extra != nil {
		copy.Result.Extra = proto.Clone(t.Result.Extra)
	}
	return &copy
}

type txnTombstoneKey struct {
	id       message.TxnID
	vchannel string
}

func (t *TxnTombstone) key() txnTombstoneKey {
	return txnTombstoneKey{t.Result.TxnCtx.TxnID, t.VChannel}
}

// TxnTombstones is a bounded, best-effort result window, separate from active
// transactions. TimeTick aging preserves the original age across WAL recovery.
type TxnTombstones struct {
	mu       sync.Mutex
	entries  map[txnTombstoneKey]*TxnTombstone
	oldest   txnTombstoneHeap
	timeTick uint64
}

func NewTxnTombstones() *TxnTombstones {
	return &TxnTombstones{entries: make(map[txnTombstoneKey]*TxnTombstone)}
}

// Add publishes an immutable copy. Repeated commits never renew retention.
func (t *TxnTombstones) Add(result *TxnTombstone) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.entries[result.key()]; ok {
		return
	}
	result = result.clone()
	t.entries[result.key()] = result
	heap.Push(&t.oldest, result)
	t.advance(result.Result.TimeTick)
}

func (t *TxnTombstones) Get(id message.TxnID, vchannel string) *TxnTombstone {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.advance(t.timeTick)
	if result, ok := t.entries[txnTombstoneKey{id, vchannel}]; ok {
		return result.clone()
	}
	return nil
}

// Advance ages results even on an idle WAL. Tombstones do not pin LCMID.
func (t *TxnTombstones) Advance(timeTick uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.advance(timeTick)
}

func (t *TxnTombstones) advance(timeTick uint64) {
	t.timeTick = max(t.timeTick, timeTick)
	cfg := &paramtable.Get().StreamingCfg
	limit := max(0, cfg.TxnTombstoneMaxCount.GetAsInt())
	lifetime := cfg.TxnTombstoneMaxLifetime.GetAsDurationByParse()
	for len(t.oldest) > 0 {
		oldest := t.oldest[0]
		if len(t.oldest) <= limit && lifetime > 0 &&
			tsoutil.AddPhysicalDurationOnTs(oldest.Result.TimeTick, lifetime) > t.timeTick {
			break
		}
		heap.Pop(&t.oldest)
		delete(t.entries, oldest.key())
	}
}

// Observe reconstructs native Commit results only from complete transactions
// seen during startup. CDC retains its own source-checkpoint deduplication.
// Results before the replay floor are deliberately outside this best-effort cache.
func (t *TxnTombstones) Observe(msg message.ImmutableMessage) {
	t.Advance(msg.TimeTick())
	transaction, ok := msg.(message.ImmutableTxnMessage)
	if !ok {
		return
	}
	commit := transaction.Commit()
	if commit.ReplicateHeader() != nil {
		return
	}
	t.Add(&TxnTombstone{VChannel: commit.VChannel(), MessageID: commit.MessageID(), Result: ExtraAppendResult{
		TimeTick: commit.TimeTick(), LastConfirmedMessageID: commit.LastConfirmedMessageID(), TxnCtx: commit.TxnContext(),
	}})
}

type txnTombstoneHeap []*TxnTombstone

func (h txnTombstoneHeap) Len() int            { return len(h) }
func (h txnTombstoneHeap) Less(i, j int) bool  { return h[i].Result.TimeTick < h[j].Result.TimeTick }
func (h txnTombstoneHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *txnTombstoneHeap) Push(x interface{}) { *h = append(*h, x.(*TxnTombstone)) }
func (h *txnTombstoneHeap) Pop() interface{} {
	old := *h
	last := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	return last
}

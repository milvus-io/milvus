package utils

import (
	"math"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// CurrentTimeTickProvider returns the current MVCC timetick of a vchannel.
type CurrentTimeTickProvider func(vchannel string) (uint64, bool)

// TxnActiveChecker reports whether a transaction is still tracked by the txn
// manager. It lets the buffer reclaim transactions that ended without a commit
// or rollback flowing through the interceptor (abort, expiry, failover), which
// the timetick-based expiry alone cannot cover (e.g. infinite-keepalive txns).
type TxnActiveChecker func(txnID message.TxnID) bool

// NewTxnInsertResultBuffers creates per-vchannel buffers for txn insert results.
func NewTxnInsertResultBuffers(currentTimeTick CurrentTimeTickProvider, txnActive TxnActiveChecker) *TxnInsertResultBuffers {
	return &TxnInsertResultBuffers{
		builders:        make(map[string]map[message.TxnID]*txnInsertResultBuilder),
		currentTimeTick: currentTimeTick,
		txnActive:       txnActive,
	}
}

// TxnInsertResultBuffers manages txn insert result buffers by vchannel. A single
// mutex guards the whole structure: the per-op work is cheap map bookkeeping done
// after the (already concurrent) WAL append, and a single lock keeps "drop the
// vchannel entry once its last txn is gone" race-free, so the map cannot
// accumulate one empty entry per distinct vchannel forever.
type TxnInsertResultBuffers struct {
	mu              sync.Mutex
	builders        map[string]map[message.TxnID]*txnInsertResultBuilder
	currentTimeTick CurrentTimeTickProvider
	txnActive       TxnActiveChecker
}

// Add records one insert result from a txn body message.
func (bs *TxnInsertResultBuffers) Add(msg message.MutableMessage, result *messagespb.IdempotentInsertResult, timeTick uint64) {
	key, hasTxnKey := keyOfTxnMessage(msg)
	if !hasTxnKey {
		return
	}
	expiredTimeTick := txnExpiredTimeTick(msg.TxnContext(), timeTick)
	currentTimeTick, hasCurrentTimeTick := bs.vchannelTimeTick(key.vchannel)

	bs.mu.Lock()
	defer bs.mu.Unlock()
	builders := bs.builders[key.vchannel]
	if builders == nil {
		builders = make(map[message.TxnID]*txnInsertResultBuilder)
		bs.builders[key.vchannel] = builders
	}
	builder := builders[key.txnID]
	if builder == nil {
		builder = &txnInsertResultBuilder{}
		builders[key.txnID] = builder
	}
	builder.add(result, expiredTimeTick)
	bs.clearExpiredLocked(key.vchannel, currentTimeTick, hasCurrentTimeTick)
}

// Build merges buffered insert results for a txn commit message.
func (bs *TxnInsertResultBuffers) Build(msg message.MutableMessage) *messagespb.IdempotentInsertResult {
	key, hasTxnKey := keyOfTxnMessage(msg)
	if !hasTxnKey {
		return nil
	}
	currentTimeTick, hasCurrentTimeTick := bs.vchannelTimeTick(key.vchannel)

	bs.mu.Lock()
	defer bs.mu.Unlock()
	bs.clearExpiredLocked(key.vchannel, currentTimeTick, hasCurrentTimeTick)
	builder := bs.builders[key.vchannel][key.txnID]
	if builder == nil {
		return nil
	}
	return builder.build()
}

// Remove drops buffered insert results for a transaction.
func (bs *TxnInsertResultBuffers) Remove(msg message.MutableMessage) {
	key, hasTxnKey := keyOfTxnMessage(msg)
	if !hasTxnKey {
		return
	}
	currentTimeTick, hasCurrentTimeTick := bs.vchannelTimeTick(key.vchannel)

	bs.mu.Lock()
	defer bs.mu.Unlock()
	if builders := bs.builders[key.vchannel]; builders != nil {
		delete(builders, key.txnID)
	}
	bs.clearExpiredLocked(key.vchannel, currentTimeTick, hasCurrentTimeTick)
}

// RemoveVChannel drops every buffered transaction result for a reclaimed
// vchannel. DropCollection is the last message for that vchannel, so normal
// Add/Build/Remove-triggered sweeping will not get another chance to run.
func (bs *TxnInsertResultBuffers) RemoveVChannel(vchannel string) {
	if vchannel == "" {
		return
	}
	bs.mu.Lock()
	defer bs.mu.Unlock()
	delete(bs.builders, vchannel)
}

// clearExpiredLocked reclaims expired/untracked txn builders for a vchannel and
// drops the vchannel entry entirely once it holds no builders, so the outer map
// does not leak one empty entry per distinct vchannel. Caller must hold bs.mu.
func (bs *TxnInsertResultBuffers) clearExpiredLocked(vchannel string, currentTimeTick uint64, hasCurrentTimeTick bool) {
	builders, ok := bs.builders[vchannel]
	if !ok {
		return
	}
	for txnID, builder := range builders {
		// Reclaim buffers whose transaction is no longer tracked by the txn
		// manager. These transactions ended without a commit/rollback message
		// reaching the interceptor (abort, expiry, failover), so the
		// timetick-based expiry below cannot reclaim them -- in particular it
		// never fires for infinite-keepalive (replicated) transactions.
		if bs.txnActive != nil && !bs.txnActive(txnID) {
			delete(builders, txnID)
			continue
		}
		if !hasCurrentTimeTick {
			continue
		}
		expiredTimeTick := builder.expiredTimeTickValue()
		if expiredTimeTick == 0 || expiredTimeTick > currentTimeTick {
			continue
		}
		delete(builders, txnID)
	}
	if len(builders) == 0 {
		delete(bs.builders, vchannel)
	}
}

func (bs *TxnInsertResultBuffers) vchannelTimeTick(vchannel string) (uint64, bool) {
	if bs.currentTimeTick == nil {
		return 0, false
	}
	return bs.currentTimeTick(vchannel)
}

type txnKey struct {
	vchannel string
	txnID    message.TxnID
}

func keyOfTxnMessage(msg message.MutableMessage) (key txnKey, hasTxnKey bool) {
	txnCtx := msg.TxnContext()
	if txnCtx == nil {
		return txnKey{}, false
	}
	return txnKey{
		vchannel: msg.VChannel(),
		txnID:    txnCtx.TxnID,
	}, true
}

func txnExpiredTimeTick(txnCtx *message.TxnContext, timeTick uint64) uint64 {
	if txnCtx == nil || timeTick == 0 {
		return 0
	}
	if txnCtx.Keepalive == message.TxnKeepaliveInfinite {
		return math.MaxUint64
	}
	return tsoutil.AddPhysicalDurationOnTs(timeTick, txnCtx.Keepalive)
}

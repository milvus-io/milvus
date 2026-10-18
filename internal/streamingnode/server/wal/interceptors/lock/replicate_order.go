package lock

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// replicateAppendOrder places local flushes between replicated transactions.
// Admission happens before WAL locks, so waiting flushes cannot block CommitTxn.
// The zero value is ready for an empty WAL.
type replicateAppendOrder struct {
	mu      sync.Mutex
	changed chan struct{}
	closed  bool
	busy    bool // A replicated append or local flush is admitted.
	current *replicatedTxn
	flushes int // Waiting flushes take precedence over the next replicated txn.
}

type replicatedTxn struct {
	clusterID string
	id        message.TxnID
	timeTick  uint64 // Source commit timestamp shared by the replicated txn messages.
}

func newReplicateAppendOrder(snapshot *recovery.RecoverySnapshot) *replicateAppendOrder {
	order := &replicateAppendOrder{}
	if snapshot == nil || snapshot.PChannelControl.GetReplicateCheckpoint() == nil {
		return order
	}
	source := snapshot.PChannelControl.GetReplicateCheckpoint().GetClusterId()
	for _, builder := range snapshot.TxnBuffer.GetUncommittedMessageBuilder() {
		begin, _ := builder.Messages()
		if rh := begin.ReplicateHeader(); rh != nil && rh.ClusterID == source {
			order.current = &replicatedTxn{clusterID: source, id: begin.TxnContext().TxnID, timeTick: rh.TimeTick}
			break // Replication admits only one transaction per PChannel.
		}
	}
	return order
}

func isLocalFlush(msg message.MutableMessage) bool {
	if msg.ReplicateHeader() != nil || !msg.IsUnreplicable() {
		return false
	}
	return msg.MessageType() == message.MessageTypeFlushAll ||
		(msg.MessageType() == message.MessageTypeManualFlush && !funcutil.IsControlChannel(msg.VChannel()))
}

// acquire returns a completion callback for admitted operations. Replication
// duplicates older than the current transaction must pass too: reconnect may
// replay those messages before it can reach the current transaction's Commit.
func (o *replicateAppendOrder) acquire(ctx context.Context, msg message.MutableMessage) (func(error), error) {
	flush := isLocalFlush(msg)
	rh := msg.ReplicateHeader()
	if !flush && (rh == nil || msg.IsUnreplicable() || msg.MessageType().IsSelfControlled()) {
		return nil, nil
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	if flush {
		o.flushes++
		defer func() {
			// Admission reserves busy before unregistering; cancellation only
			// removes this attempt. Persisted broadcasts remain retryable.
			o.flushes--
			o.notifyLocked()
		}()
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if o.closed {
			return nil, status.NewOnShutdownError("replication append order is closing")
		}
		ready := !o.busy
		if flush {
			ready = ready && o.current == nil
		} else if o.flushes > 0 {
			// All messages in the current source txn share its commit timestamp.
			// Older replay traffic is deduplicated by the replicate interceptor.
			ready = ready && o.current != nil && rh.ClusterID == o.current.clusterID &&
				((msg.TxnContext() != nil && msg.TxnContext().TxnID == o.current.id) || rh.TimeTick <= o.current.timeTick)
		}
		if ready {
			o.busy = true
			var begun *replicatedTxn
			if !flush && msg.MessageType() == message.MessageTypeBeginTxn && o.current == nil {
				begun = &replicatedTxn{clusterID: rh.ClusterID, id: msg.TxnContext().TxnID, timeTick: rh.TimeTick}
				o.current = begun
			}
			return func(err error) { o.finish(msg, begun, err) }, nil
		}
		if o.changed == nil {
			o.changed = make(chan struct{})
		}
		changed := o.changed
		o.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		o.mu.Lock()
	}
}

func (o *replicateAppendOrder) finish(msg message.MutableMessage, begun *replicatedTxn, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	// busy prevents another Begin until this completion; promotion can only
	// clear current, so a failed new Begin cannot erase a different transaction.
	if begun != nil && err != nil {
		o.current = nil
	} else if err == nil && msg.MessageType() == message.MessageTypeCommitTxn && o.current != nil &&
		msg.ReplicateHeader().ClusterID == o.current.clusterID && msg.TxnContext().TxnID == o.current.id {
		o.current = nil
	}
	o.busy = false
	o.notifyLocked()
}

// forcePromoted is called under the PChannel write lock, after successful append
// and transaction rollback. It must precede unlocking, or a new Begin can race it.
func (o *replicateAppendOrder) forcePromoted() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.current = nil
	o.notifyLocked()
}

func (o *replicateAppendOrder) close() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.closed = true
	o.notifyLocked()
}

func (o *replicateAppendOrder) notifyLocked() {
	if o.changed != nil {
		close(o.changed)
		o.changed = nil
	}
}

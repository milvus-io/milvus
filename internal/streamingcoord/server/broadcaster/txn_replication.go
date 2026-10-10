package broadcaster

import (
	"maps"
	"math"
	"slices"
	"sort"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (g *broadcastTxn) members() []*broadcastTask {
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Clone(g.tasks)
}

// callbackReady never waits while holding callback resource locks. Replication
// may deliver different channels out of order, including Body before Begin.
func (g *broadcastTxn) callbackReady(sequence uint32) bool {
	if sequence == 0 {
		return true
	}
	for _, member := range g.members() {
		if member.Header().Txn.GetSequence() == sequence-1 {
			select {
			case <-member.done:
				return true
			default:
				return false
			}
		}
	}
	return false
}

// Replicated members use the same task records and atomic completion as local
// members, but acquire no primary business locks on the secondary.
func (bm *broadcastTaskManager) getOrCreateTxnTask(msg message.ImmutableMessage) (*broadcastTask, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	header := msg.BroadcastHeader()
	if task := bm.tasks[header.BroadcastID]; task != nil {
		if !proto.Equal(task.Header().Txn, header.Txn) {
			return nil, merr.WrapErrServiceInternalMsg("broadcast ID reused with a different transaction")
		}
		if !maps.Equal(task.Header().ResourceKeys, header.ResourceKeys) ||
			!maps.Equal(typeutil.NewSet(task.Header().VChannels...), typeutil.NewSet(header.VChannels...)) {
			return nil, merr.WrapErrServiceInternalMsg("broadcast ID reused with different transaction resources or channels")
		}
		return task, nil
	}
	if msg.ReplicateHeader() == nil {
		return nil, nil
	}
	group := bm.txns[header.Txn.GetTxnId()]
	if group == nil {
		group = &broadcastTxn{manager: bm, id: header.Txn.GetTxnId(), op: make(chan struct{}, 1)}
	}
	members := group.members()
	protos := make([]*streamingpb.BroadcastTask, 0, len(members)+1)
	for _, member := range members {
		member.mu.Lock()
		protos = append(protos, proto.Clone(member.task).(*streamingpb.BroadcastTask))
		member.mu.Unlock()
	}
	incoming := &streamingpb.BroadcastTask{Message: msg.IntoBroadcastMutableMessage().IntoMessageProto(), State: streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED}
	protos = append(protos, incoming)
	sort.Slice(protos, func(i, j int) bool {
		return txnHeader(protos[i]).Txn.GetSequence() < txnHeader(protos[j]).Txn.GetSequence()
	})
	if err := validateBroadcastTxn(protos); err != nil {
		return nil, err
	}
	task := newBroadcastTaskFromImmutableMessage(msg, bm.metrics, bm.ackScheduler)
	task.SetLogger(bm.Logger())
	task.txn = group
	group.mu.Lock()
	group.tasks = append(group.tasks, task)
	sort.Slice(group.tasks, func(i, j int) bool {
		// Transaction message identities never change after registration.
		return group.tasks[i].msg.BroadcastHeader().Txn.GetSequence() < group.tasks[j].msg.BroadcastHeader().Txn.GetSequence()
	})
	group.mu.Unlock()
	bm.tasks[header.BroadcastID] = task
	bm.txns[group.id] = group
	if header.Txn.GetKind() == messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_BEGIN {
		bm.idempotencyIndex.Add(task.IdempotencyScope(), header.BroadcastID)
	}
	return task, nil
}

func txnHeader(task *streamingpb.BroadcastTask) *message.BroadcastHeader {
	return message.NewBroadcastMutableMessageBeforeAppend(task.Message.Payload, task.Message.Properties).BroadcastHeader()
}

// After the WAL fence no missing member can arrive from the former primary.
// Missing channel copies can be supplemented; a wholly missing message cannot.
func (bm *broadcastTaskManager) validateTxnPromotion() error {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	for _, group := range bm.txns {
		for i, member := range group.members() {
			if member.Header().Txn.GetSequence() != uint32(i) {
				return merr.WrapErrServiceUnavailable("cannot promote a broadcast transaction with missing members")
			}
		}
	}
	return nil
}

// prepareTxnPromotion runs after replay and the configuration callback. The force-promote task still owns Cluster X and admission exclusively.
// Its successful completion releases X, installs the open groups' guards, then
// opens admission. No public acquisition can enter the gap.
func (bm *broadcastTaskManager) prepareTxnPromotion(task *broadcastTask) {
	bm.mu.Lock()
	groups := make([]*broadcastTxn, 0, len(bm.txns))
	for _, group := range bm.txns {
		groups = append(groups, group)
	}
	bm.mu.Unlock()
	task.mu.Lock()
	defer task.mu.Unlock()
	if task.guards == nil {
		return
	} // replicated configuration, not a local promotion
	task.guards.afterUnlock = func() {
		for _, group := range groups {
			group.mu.Lock()
			if group.begin != nil && group.begin.State != streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE && group.guards == nil {
				guards, err := bm.resourceKeyLocker.FastLock(txnHeader(group.begin).ResourceKeys.Collect()...)
				if err != nil {
					panic(err)
				} // fenced, drained groups must be mutually compatible
				group.guards = guards
			}
			group.mu.Unlock()
		}
		bm.admission.Release(math.MaxInt64)
	}
}

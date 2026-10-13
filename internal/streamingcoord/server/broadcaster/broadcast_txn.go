package broadcaster

import (
	"context"
	"maps"
	"sort"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// saveMember is called with the member mutex held. The group mutex serializes
// durable updates; it never acquires a task mutex or manager.mu.
func (g *broadcastTxn) saveMember(ctx context.Context, member *streamingpb.BroadcastTask, completing bool) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	header := message.NewBroadcastMutableMessageBeforeAppend(member.Message.Payload, member.Message.Properties).BroadcastHeader()
	changes := map[uint64]*streamingpb.BroadcastTask{header.BroadcastID: member}
	closing := completing && header.Txn.GetKind() == messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_COMMIT
	var begin *streamingpb.BroadcastTask
	if closing {
		begin = proto.Clone(g.begin).(*streamingpb.BroadcastTask)
		begin.State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE
		changes[g.beginID] = begin
	}
	if err := resource.Resource().StreamingCatalog().SaveBroadcastTasks(ctx, changes); err != nil {
		return err
	}
	if header.Txn.GetKind() == messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_BEGIN {
		g.begin = proto.Clone(member).(*streamingpb.BroadcastTask)
		g.beginID = header.BroadcastID
	}
	if closing {
		g.begin = begin
		if g.guards != nil {
			g.guards.Unlock()
			g.guards = nil
		}
	}
	return nil
}

func (g *broadcastTxn) drop(ctx context.Context) error {
	if err := g.acquire(ctx); err != nil {
		return err
	}
	defer g.release()
	if g.deleted {
		return nil
	}
	g.mu.Lock()
	if g.begin == nil || g.begin.State != streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE {
		g.mu.Unlock()
		return merr.WrapErrServiceInternalMsg("cannot GC an open broadcast transaction")
	}
	g.mu.Unlock()
	members := g.members()
	removals := make(map[uint64]*streamingpb.BroadcastTask, len(members))
	for _, task := range members {
		removals[task.Header().BroadcastID] = &streamingpb.BroadcastTask{State: streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE}
	}
	err := resource.Resource().StreamingCatalog().SaveBroadcastTasks(ctx, removals)
	if err != nil {
		return err
	}
	g.deleted = true
	// Stable task references held by duplicate callers still yield the original result.
	bm := g.manager
	bm.mu.Lock()
	delete(bm.txns, g.id)
	for _, task := range members {
		task.mu.Lock()
		task.ObserveStateChanged(streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE)
		task.mu.Unlock()
		bm.idempotencyIndex.Remove(task.IdempotencyScope(), task.Header().BroadcastID)
		delete(bm.tasks, task.Header().BroadcastID)
	}
	bm.mu.Unlock()
	return nil
}

func validateBroadcastTxn(tasks []*streamingpb.BroadcastTask) error {
	if len(tasks) == 0 || len(tasks) > maxTxnMembers {
		return merr.WrapErrServiceInternalMsg("invalid broadcast transaction member count")
	}
	first := message.NewBroadcastMutableMessageBeforeAppend(tasks[0].Message.Payload, tasks[0].Message.Properties).BroadcastHeader()
	closed := first.Txn.GetSequence() == 0 && tasks[0].State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE
	previous := int64(-1)
	contiguous := true
	ids := make(map[uint64]bool, len(tasks))
	for idx, task := range tasks {
		msg := message.NewBroadcastMutableMessageBeforeAppend(task.GetMessage().GetPayload(), task.GetMessage().GetProperties())
		header := msg.BroadcastHeader()
		tc := header.Txn
		if first.Txn.GetTxnId() == 0 || tc.GetTxnId() != first.Txn.GetTxnId() || int64(tc.GetSequence()) <= previous || tc.GetSequence() >= maxTxnMembers || header.BroadcastID == 0 || ids[header.BroadcastID] {
			return merr.WrapErrServiceInternalMsg("invalid broadcast transaction identity or sequence")
		}
		ids[header.BroadcastID] = true
		contiguous = contiguous && int(tc.GetSequence()) == idx
		previous = int64(tc.GetSequence())
		if (!contiguous && msg.ReplicateHeader() == nil) || msg.IsUnreplicable() {
			return merr.WrapErrServiceInternalMsg("non-replicated transaction has a missing predecessor or unreplicable member")
		}
		if !maps.Equal(first.ResourceKeys, header.ResourceKeys) || (tc.GetSequence() > 0 && message.BroadcastAdmissionKeyOf(msg) != "") {
			return merr.WrapErrServiceInternalMsg("invalid broadcast transaction ownership")
		}
		if !maps.Equal(typeutil.NewSet(first.VChannels...), typeutil.NewSet(header.VChannels...)) {
			return merr.WrapErrServiceInternalMsg("inconsistent broadcast transaction channels")
		}
		for key := range header.ResourceKeys {
			if key.Domain == messagespb.ResourceDomain_ResourceDomainIdempotency {
				return merr.WrapErrServiceInternalMsg("transaction persisted an admission resource lock")
			}
		}
		kind := tc.GetKind()
		if (tc.GetSequence() == 0 && kind != messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_BEGIN) ||
			(tc.GetSequence() > 0 && kind != messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_BODY && kind != messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_COMMIT) ||
			(kind == messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_COMMIT && idx != len(tasks)-1) {
			return merr.WrapErrServiceInternalMsg("invalid broadcast transaction member kind")
		}
		switch task.State {
		case streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED:
			if msg.ReplicateHeader() == nil || closed {
				return merr.WrapErrServiceInternalMsg("invalid replicated transaction member")
			}
		case streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING:
			if closed || idx != len(tasks)-1 {
				return merr.WrapErrServiceInternalMsg("broadcast transaction has an incomplete predecessor")
			}
		case streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TXN_INFLIGHT:
			if tc.GetSequence() != 0 || closed {
				return merr.WrapErrServiceInternalMsg("only an open Begin can be TXN_INFLIGHT")
			}
		case streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE:
			if !contiguous || (kind == messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_COMMIT && !closed) {
				return merr.WrapErrServiceInternalMsg("terminal completed without closing Begin")
			}
		default:
			return merr.WrapErrServiceInternalMsg("unsupported broadcast transaction member state")
		}
		if closed && idx == len(tasks)-1 && kind != messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_COMMIT {
			return merr.WrapErrServiceInternalMsg("closed broadcast transaction has no terminal")
		}
	}
	return nil
}

// splitBroadcastTasks rebuilds groups from the ordinary task catalog. Members
// are loaded without ordering guarantees and must never recover as individual tasks.
func splitBroadcastTasks(tasks []*streamingpb.BroadcastTask) ([]*streamingpb.BroadcastTask, [][]*streamingpb.BroadcastTask, error) {
	ordinary := make([]*streamingpb.BroadcastTask, 0, len(tasks))
	byTxn := make(map[uint64][]*streamingpb.BroadcastTask)
	headerOf := func(task *streamingpb.BroadcastTask) *message.BroadcastHeader {
		return message.NewBroadcastMutableMessageBeforeAppend(task.Message.Payload, task.Message.Properties).BroadcastHeader()
	}
	for _, task := range tasks {
		header := headerOf(task)
		if header.Txn == nil {
			ordinary = append(ordinary, task)
			continue
		}
		id := header.Txn.GetTxnId()
		byTxn[id] = append(byTxn[id], task)
	}
	groups := make([][]*streamingpb.BroadcastTask, 0, len(byTxn))
	for _, members := range byTxn {
		sort.Slice(members, func(i, j int) bool {
			return headerOf(members[i]).Txn.GetSequence() < headerOf(members[j]).Txn.GetSequence()
		})
		if err := validateBroadcastTxn(members); err != nil {
			return nil, nil, err
		}
		groups = append(groups, members)
	}
	return ordinary, groups, nil
}

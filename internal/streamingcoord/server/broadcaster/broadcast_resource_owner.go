package broadcaster

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// WithResourceKeysForMessage reserves an idempotency scope before waiting for
// keys. A retry of an accepted Begin waits for its ACK, not for the import job.
// Reservations disappear on pre-broadcast failure; they are never acceptance.
func (bm *broadcastTaskManager) WithResourceKeysForMessage(ctx context.Context, msgType message.MessageType, key message.IdempotencyKey, resourceKeys ...message.ResourceKey) (BroadcastAPI, error) {
	scope := idempotencyScope(msgType, key)
	if scope == "" {
		return bm.WithResourceKeys(ctx, resourceKeys...)
	}
	for {
		if err := bm.checkClusterRole(ctx); err != nil {
			return nil, err
		}
		bm.mu.Lock()
		if id, ok := bm.idempotencyIndex.Get(scope); ok {
			if task, ok := bm.tasks[id]; ok {
				bm.mu.Unlock()
				return &admittedBroadcast{scope: scope, duplicate: task}, nil
			}
		}
		if preparing, ok := bm.admissions[scope]; ok {
			bm.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-preparing:
				continue
			}
		}
		ready := make(chan struct{})
		bm.admissions[scope] = ready
		bm.mu.Unlock()
		var once sync.Once
		release := func() {
			once.Do(func() {
				bm.mu.Lock()
				delete(bm.admissions, scope)
				close(ready)
				bm.mu.Unlock()
			})
		}
		api, err := bm.WithResourceKeys(ctx, resourceKeys...)
		if err != nil {
			release()
			return nil, err
		}
		return &admittedBroadcast{scope: scope, api: api, release: release}, nil
	}
}

type admittedBroadcast struct {
	scope     string
	api       BroadcastAPI
	duplicate *broadcastTask
	release   func()
}

func (b *admittedBroadcast) Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	if b.release != nil {
		defer b.release()
	}
	if idempotencyScopeOfMessage(msg) != b.scope {
		b.Close()
		return nil, merr.WrapErrServiceInternalMsg("broadcast does not match its admitted idempotency scope")
	}
	if b.duplicate != nil {
		result, err := b.duplicate.BlockUntilDone(ctx)
		if err != nil {
			return nil, err
		}
		result.Duplicated = b.duplicate.BroadcastMessage()
		return result, nil
	}
	return b.api.Broadcast(ctx, msg)
}

func (b *admittedBroadcast) Close() {
	if b.api != nil {
		b.api.Close()
	}
	if b.release != nil {
		b.release()
	}
}

// BroadcastWithResourceKeyOwner registers one End under the manager mutex and
// borrows the Begin's keys. It must never reacquire them: an exclusive DDL may
// already be waiting for the import to finish. Legacy jobs fall back to their
// ordinary broadcast path at the caller.
func (bm *broadcastTaskManager) BroadcastWithResourceKeyOwner(ctx context.Context, msg message.BroadcastMutableMessage) (bool, error) {
	jobID, begin := registry.BroadcastPair(msg)
	if begin || jobID == 0 {
		return false, merr.WrapErrServiceInternalMsg("message is not a paired broadcast End")
	}
	if !bm.lifetime.Add(typeutil.LifetimeStateWorking) {
		return true, merr.WrapErrServiceNotReadyMsg("broadcaster is closing")
	}
	defer bm.lifetime.Done()

	bm.mu.Lock()
	owner := bm.resourceKeyOwners[jobID]
	bm.mu.Unlock()
	if owner == nil {
		return false, nil
	}
	if err := bm.checkClusterRole(ctx); err != nil {
		return true, err
	}
	// Begin's callback must finish before an End can execute, including when
	// the checker sees a newly created Failed job before Begin has returned.
	if _, err := owner.BlockUntilDone(ctx); err != nil {
		return true, err
	}
	id, err := resource.Resource().IDAllocator().Allocate(ctx)
	if err != nil {
		return true, merr.Wrap(err, "allocate paired broadcast ID")
	}

	bm.mu.Lock()
	ownerID, released := owner.resourceKeyOwnership()
	if released {
		bm.mu.Unlock()
		// The owner is retained only for deduplication; the import job's state
		// already protects public Commit/Abort retries after End GC.
		return true, nil
	}
	end := bm.resourceKeyEnds[ownerID]
	if end != nil {
		bm.mu.Unlock()
		if end.BroadcastMessage().MessageTypeWithVersion() != msg.MessageTypeWithVersion() {
			return true, merr.WrapErrImportSysFailedMsg("import %d already has a different terminal broadcast", jobID)
		}
		_, err := end.BlockUntilDone(ctx)
		return true, err
	}

	msg = msg.OverwriteBroadcastHeader(id, owner.Header().ResourceKeys.Collect()...)
	ctx, span := message.StartSpanForMessage(ctx, msg, message.SpanNameWALBroadcast)
	defer span.End()
	message.InjectTraceContext(ctx, msg)
	end = newBroadcastTaskFromBroadcastMessage(msg, bm.metrics, bm.ackScheduler)
	end.SetLogger(bm.Logger())
	end.task.ResourceKeyOwnerId = ownerID
	bm.tasks[id] = end
	bm.resourceKeyEnds[ownerID] = end
	bm.mu.Unlock()

	// InitializeRecovery persists End before append. A request timeout after
	// registration leaves this same task responsible for finishing the End.
	_, err = bm.broadcastScheduler.AddTask(ctx, newPendingBroadcastTask(end))
	return true, err
}

// completeResourceKeyOwner runs after this task's ACK state is persisted. Open
// Begins are excluded from GC; Ends first durably release their owner's keys.
func (bm *broadcastTaskManager) completeResourceKeyOwner(ctx context.Context, task *broadcastTask) ([]uint64, error) {
	ownerID, _ := task.resourceKeyOwnership()
	id := task.Header().BroadcastID
	if ownerID == 0 {
		return []uint64{id}, nil
	}
	if ownerID == id {
		return nil, nil
	}
	owner, ok := bm.getBroadcastTaskByID(ownerID)
	if !ok {
		return nil, merr.WrapErrServiceInternalMsg("resource key owner %d is missing", ownerID)
	}
	if err := owner.releaseResourceKeys(ctx); err != nil {
		return nil, err
	}
	task.mu.Lock()
	task.notifyDoneLocked()
	task.mu.Unlock()
	return []uint64{ownerID, id}, nil
}

func (b *broadcastTask) resourceKeyOwnership() (uint64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.task.GetResourceKeyOwnerId(), b.task.GetResourceKeysReleased()
}

func (b *broadcastTask) holdsResourceKeys() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ownerID := b.task.GetResourceKeyOwnerId(); ownerID != 0 {
		return ownerID == b.header().BroadcastID && !b.task.GetResourceKeysReleased()
	}
	return b.task.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING ||
		b.task.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_WAIT_ACK
}

func (b *broadcastTask) releaseResourceKeys(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.task.GetResourceKeysReleased() {
		b.task.ResourceKeysReleased = true
		b.dirty = true
	}
	if err := b.saveTaskIfDirty(ctx, b.Logger()); err != nil {
		return err
	}
	if b.guards != nil {
		b.guards.Unlock()
	}
	return nil
}

func (b *broadcastTask) notifyDoneLocked() {
	select {
	case <-b.done:
	default:
		close(b.done)
	}
}

// finishRecoveredResourceKeyOwners closes the crash window between durable End
// ACK and durable owner release, before restoring locks or starting GC. Without
// this pass a later DDL's PENDING record could conflict with a completed owner.
func finishRecoveredResourceKeyOwners(ctx context.Context, tasks []*streamingpb.BroadcastTask) error {
	completed := make(map[uint64]struct{})
	for _, task := range tasks {
		msg := message.NewBroadcastMutableMessageBeforeAppend(task.Message.Payload, task.Message.Properties)
		ownerID := task.GetResourceKeyOwnerId()
		if ownerID != 0 && ownerID != msg.BroadcastHeader().BroadcastID && task.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE {
			completed[ownerID] = struct{}{}
		}
	}
	for _, task := range tasks {
		ownerID := task.GetResourceKeyOwnerId()
		if _, ok := completed[ownerID]; !ok || task.GetResourceKeysReleased() {
			continue
		}
		msg := message.NewBroadcastMutableMessageBeforeAppend(task.Message.Payload, task.Message.Properties)
		if ownerID != msg.BroadcastHeader().BroadcastID {
			continue
		}
		task.ResourceKeysReleased = true
		if err := resource.Resource().StreamingCatalog().SaveBroadcastTask(ctx, ownerID, task); err != nil {
			return err
		}
	}
	return nil
}

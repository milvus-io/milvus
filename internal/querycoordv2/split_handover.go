package querycoordv2

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/datacoord/split"
	"github.com/milvus-io/milvus/internal/views/coord/balancer"
	qvresource "github.com/milvus-io/milvus/internal/views/coord/resource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type splitHandover struct {
	backend qvresource.SplitLifecycle
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	mu      sync.Mutex
	running map[int64]bool
}

func (r *qviewsRuntime) startSplitHandover(ctx context.Context) {
	if r.splitHandover == nil {
		return
	}
	h := r.splitHandover
	ctx, cancel := context.WithCancel(ctx)
	h.cancel = cancel
	h.running = make(map[int64]bool)
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			for _, task := range h.backend.ListShardSplits() {
				if task.GetState() < datapb.SplitShardTaskState_SplitShardTaskRewriteDone || task.GetState() >= datapb.SplitShardTaskState_SplitShardTaskRetired {
					continue
				}
				h.mu.Lock()
				if h.running[task.GetCollectionId()] {
					h.mu.Unlock()
					continue
				}
				h.running[task.GetCollectionId()] = true
				h.mu.Unlock()
				h.wg.Add(1)
				go func() {
					defer h.wg.Done()
					defer func() { h.mu.Lock(); delete(h.running, task.GetCollectionId()); h.mu.Unlock() }()
					if err := runSplitHandover(ctx, h.backend, task, r.awaitSplitPublication); err != nil && ctx.Err() == nil {
						mlog.Warn(ctx, "split read handover will retry", mlog.FieldTaskID(task.GetTaskId()), mlog.Err(err))
					}
				}()
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	// The version gate remains independently closed until a release declares
	// its minimum version. Recovered tasks still finish when triggering is off.
	split.SetReadPathReady(true)
}

func (h *splitHandover) stop() {
	if h != nil && h.cancel != nil {
		h.cancel()
		h.wg.Wait()
		split.SetReadPathReady(false)
	}
}

// runSplitHandover is restartable from the persisted data-plane phase. The
// schema guard covers adoption through g2 withdrawal; folding the retained L0
// history happens outside it, then retirement reacquires the guard.
func runSplitHandover(ctx context.Context, backend qvresource.SplitLifecycle, task *datapb.SplitShardTask, publish func(context.Context, *datapb.SplitShardTask) error) error {
	release, err := qvresource.AcquireSplitLifecycle(ctx, task.GetCollectionId())
	if err != nil {
		return err
	}
	if err = backend.AdoptShardSplit(ctx, task.GetTaskId()); err != nil {
		release()
		return err
	}
	if err = publish(ctx, task); err != nil {
		release()
		return err
	}
	if err = backend.ResumeShardSplitL0(ctx, task.GetTaskId()); err != nil {
		release()
		return err
	}
	release()
	for {
		ready, err := backend.ReleaseShardSplitPins(ctx, task.GetTaskId())
		if err != nil {
			return err
		}
		if ready {
			break
		}
		if err := waitSplitTick(ctx); err != nil {
			return err
		}
	}
	release, err = qvresource.AcquireSplitLifecycle(ctx, task.GetCollectionId())
	if err != nil {
		return err
	}
	defer release()
	return backend.RetireShardSplit(ctx, task.GetTaskId())
}

func waitSplitTick(ctx context.Context) error {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *qviewsRuntime) awaitSplitPublication(ctx context.Context, task *datapb.SplitShardTask) error {
	if r.resources == nil || r.balanceCache == nil {
		return merr.WrapErrServiceNotReadyMsg("split serving resources unavailable")
	}
	if r.splitServingSchemas != nil {
		if entry := r.balanceCache.GetCollection(task.GetCollectionId()); entry != nil && entry.DataView() != nil {
			r.splitServingSchemas.Store(task.GetCollectionId(), entry.DataView().SchemaVersion)
			defer r.splitServingSchemas.Delete(task.GetCollectionId())
		}
	}
	r.balancer.Trigger(balancer.TriggerScope{DirtyCollections: []int64{task.GetCollectionId()}})
	for {
		// DropCollection removes the task. It owns teardown from then on.
		exists := false
		if r.splitHandover != nil {
			for _, current := range r.splitHandover.backend.ListShardSplits() {
				if current.GetTaskId() == task.GetTaskId() {
					exists = true
					break
				}
			}
		}
		if !exists {
			return nil
		}
		complete, err := r.splitPublicationReady(ctx, task)
		if err != nil && ctx.Err() == nil {
			mlog.RatedWarn(ctx, 1, "waiting for split serving generation", mlog.FieldTaskID(task.GetTaskId()), mlog.Err(err))
		}
		if complete && err == nil {
			return nil
		}
		if err := waitSplitTick(ctx); err != nil {
			return err
		}
	}
}

func (r *qviewsRuntime) splitPublicationReady(ctx context.Context, task *datapb.SplitShardTask) (bool, error) {
	id := task.GetCollectionId()
	entry := r.balanceCache.GetCollection(id)
	if entry == nil || entry.DataView() == nil {
		return false, nil
	}
	native := entry.DataView()
	if native.Shard(task.GetSourceVchannel()) != nil {
		return false, nil
	}
	cfg := r.loadConfigStore.GetConfig(id)
	if cfg == nil {
		return r.shardViewRegistry.ServingTransitionComplete(id, 0, true), nil
	}
	ref, err := r.resources.DataViews.Get(ctx, id, native.DataVersion.IntoProto())
	if err != nil {
		return false, err
	}
	if ref == nil {
		return false, nil
	}
	defer ref.Deref()
	published, err := r.resolveServingGeneration(ctx, ref.DataView(), cfg)
	if err != nil {
		return false, err
	}
	for _, shard := range published.GetShards() {
		if shard.GetVchannel() == task.GetSourceVchannel() {
			return false, nil
		}
	}
	return published.GetShardSetEpoch() == native.ShardSetEpoch && r.shardViewRegistry.ServingTransitionComplete(id, published.GetShardSetEpoch(), false), nil
}

// finishSplitAdoptionBeforeSchema closes the restart ordering gap: an ALTER
// callback may win the lifecycle guard before the background worker resumes an
// already durable adoption. Finish g2 at the old DataView schema first.
func (r *qviewsRuntime) finishSplitAdoptionBeforeSchema(ctx context.Context, id int64) error {
	if r.splitHandover == nil {
		return merr.WrapErrServiceNotReadyMsg("split lifecycle handover unavailable")
	}
	for _, task := range r.splitHandover.backend.ListShardSplits() {
		if task.GetCollectionId() == id && task.GetState() == datapb.SplitShardTaskState_SplitShardTaskAdopted {
			if err := r.awaitSplitPublication(ctx, task); err != nil {
				return err
			}
		}
	}
	return nil
}

// Only a lifecycle-guarded adoption waiter installs an override. Normal
// pending-schema gating remains unchanged outside that old-generation repair.
func withSplitServingSchema(base balancer.SchemaVersionProvider, overrides *sync.Map) balancer.SchemaVersionProvider {
	if base == nil {
		return nil
	}
	return func(ctx context.Context, id int64) (int32, error) {
		if value, ok := overrides.Load(id); ok {
			return value.(int32), nil
		}
		if base == nil {
			return 0, nil
		}
		return base(ctx, id)
	}
}

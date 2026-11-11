package querycoordv2

import (
	"context"
	"slices"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/loadstatus"
	"github.com/milvus-io/milvus/internal/views/coord/readiness"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// warmCollectionOnce owns an ephemeral request, including its readiness wait.
// Neither durable load intent nor recovery can recreate it. The subscription
// was acquired under the collection DDL guard before accepting the load.
func (r *qviewsRuntime) warmCollectionOnce(ctx context.Context, collectionID int64, channels []string, subscription *readiness.Subscription) {
	cfg := r.loadConfigStore.GetConfig(collectionID)
	r.warmMu.Lock()
	if r.warmClosed || cfg == nil {
		r.warmMu.Unlock()
		subscription.Close()
		return
	}
	if r.warmCtx == nil {
		r.warmCtx, r.warmCancel = context.WithCancel(context.Background())
	}
	lifetime, cancelLifetime := contextutil.MergeContext(context.WithoutCancel(ctx), r.warmCtx)
	ctx, cancel := context.WithTimeout(lifetime, paramtable.Get().QueryCoordCfg.LoadTimeoutSeconds.GetAsDuration(time.Second))
	r.warmWG.Add(1)
	r.warmMu.Unlock()
	channels = slices.Clone(channels)
	go func() {
		defer r.warmWG.Done()
		defer cancel()
		defer cancelLifetime()
		defer subscription.Close()
		if !r.waitForWarmup(ctx, cfg, channels, subscription) {
			return
		}
		// Snapshot only the serving generation. No new View, placement intent,
		// persistent task or retry onto a replacement worker is created.
		type target struct {
			node qviews.WorkNode
			req  *viewpb.PrewarmOnViewRequest
		}
		var targets []target
		err := r.loadConfigStore.TryWithCurrentConfig(cfg, func() error {
			for shard, stats := range r.shardViewRegistry.SnapshotForCollection(collectionID).StatsMap() {
				if stats.UpVersion == nil || stats.UpLoadInfoVersion != cfg.Version {
					continue
				}
				req := &viewpb.PrewarmOnViewRequest{ShardId: shard.IntoProto(), Version: stats.UpVersion.IntoProto()}
				targets = append(targets, target{qviews.StreamingNode{PChannel: funcutil.ToPhysicalChannel(shard.VChannel)}, req})
				for _, id := range stats.UpNodes {
					targets = append(targets, target{qviews.QueryNode{ID: id}, req})
				}
			}
			return nil
		})
		if err != nil {
			mlog.Info(ctx, "discard one-time collection warmup", mlog.FieldCollectionID(collectionID), mlog.Err(err))
			return
		}
		// Release/config replacement cancels this request, while ordinary View
		// changes merely notify the watcher and preserve the borrowed resources.
		watchCtx, stopWatch := context.WithCancel(ctx)
		watchDone := make(chan struct{})
		go func() {
			defer close(watchDone)
			for {
				changed, released := subscription.Observe()
				if released || r.loadConfigStore.GetConfig(collectionID) != cfg {
					cancel()
					return
				}
				select {
				case <-watchCtx.Done():
					return
				case <-r.readyChanges.Done():
					cancel()
					return
				case <-changed:
				}
			}
		}()
		defer func() { stopWatch(); <-watchDone }()
		var calls errgroup.Group
		calls.SetLimit(max(1, hardware.GetCPUNum()))
		for _, target := range targets {
			if ctx.Err() != nil {
				break
			}
			calls.Go(func() error {
				if ctx.Err() == nil {
					if err := r.prewarm(ctx, target.node, target.req); err != nil {
						mlog.Info(ctx, "one-time prewarm ended", mlog.FieldCollectionID(collectionID), mlog.Stringer("worker", target.node), mlog.Err(err))
					}
				}
				return nil
			})
		}
		_ = calls.Wait()
	}()
}

func (r *qviewsRuntime) waitForWarmup(ctx context.Context, cfg *loadmgr.LoadConfig, channels []string, subscription *readiness.Subscription) bool {
	for {
		changed, released := subscription.Observe()
		if released || ctx.Err() != nil || r.loadConfigStore.GetConfig(cfg.CollectionID) != cfg {
			return false
		}
		progress := loadstatus.Get(r.loadConfigStore, r.shardViewRegistry, cfg.CollectionID, channels)
		if _, released := subscription.Observe(); released {
			return false
		}
		if progress.Config == cfg && progress.Ready() {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-r.readyChanges.Done():
			return false
		case <-changed:
		}
	}
}

func (r *qviewsRuntime) stopWarmups() {
	r.warmMu.Lock()
	r.warmClosed = true
	if r.warmCancel != nil {
		r.warmCancel()
	}
	r.warmMu.Unlock()
	r.warmWG.Wait()
}

package querycoordv2

import (
	"context"
	"slices"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/readiness"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// PrewarmCollection accepts local cache work independently of loading. It never
// creates load intent, waits for load completion, or changes the load result.
func (s *Server) PrewarmCollection(ctx context.Context, req *querypb.PrewarmCollectionRequest) (*commonpb.Status, error) {
	return merr.Status(s.prewarmCollection(ctx, req.GetCollectionID())), nil
}

func (s *Server) prewarmCollection(ctx context.Context, collectionID int64) error {
	if err := merr.CheckHealthy(s.State()); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	if !paramtable.Get().QueryCoordCfg.EnableLoadCollectionWarmupHint.GetAsBool() {
		return merr.Wrap(merr.ErrServiceUnimplemented, "prewarm requires queryCoord.enableLoadCollectionWarmupHint after a full-cluster upgrade")
	}
	coll, err := s.meta.GetCollectionInfo(collectionID)
	if err != nil {
		return err
	}
	if typeutil.IsExternalCollection(coll.Schema) {
		return merr.WrapErrParameterInvalidMsg("load warmup hint is not supported for external collections")
	}
	r := s.qviewsRuntime
	cfg := r.loadConfigStore.GetConfig(collectionID)
	if cfg == nil {
		return merr.WrapErrCollectionNotLoaded(collectionID)
	}
	// Fence subscription and admission against release/reload. No DDL broadcast
	// or allocator call is needed; this guard only protects the borrowed config.
	var subscription *readiness.Subscription
	if err := r.loadConfigStore.TryWithCurrentConfig(cfg, func() error {
		subscription = r.readyChanges.Subscribe(collectionID)
		return nil
	}); err != nil {
		return err
	}
	r.warmCollectionOnce(ctx, cfg, coll.VChannelNames, subscription)
	return nil
}

// warmCollectionOnce owns an ephemeral request. Each shard waits for its own Up
// view; neither load completion nor another shard's readiness gates dispatch.
func (r *qviewsRuntime) warmCollectionOnce(ctx context.Context, cfg *loadmgr.LoadConfig, channels []string, subscription *readiness.Subscription) {
	r.warmMu.Lock()
	if r.warmClosed {
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
		// Release/config replacement cancels pending and running work. Ordinary
		// View changes preserve borrowed resources and never schedule a new pass.
		watchCtx, stopWatch := context.WithCancel(ctx)
		watchDone := make(chan struct{})
		go func() {
			defer close(watchDone)
			for {
				changed, released := subscription.Observe()
				if released || r.loadConfigStore.GetConfig(cfg.CollectionID) != cfg {
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
		r.prewarmShards(ctx, cfg, channels, subscription)
	}()
}

func (r *qviewsRuntime) prewarmShards(ctx context.Context, cfg *loadmgr.LoadConfig, channels []string, subscription *readiness.Subscription) {
	pending := make(map[qviews.ShardID]struct{}, len(cfg.Replicas)*len(channels))
	for _, replica := range cfg.Replicas {
		for _, channel := range channels {
			pending[qviews.ShardID{ReplicaID: replica.ReplicaID, VChannel: channel}] = struct{}{}
		}
	}
	type target struct {
		node qviews.WorkNode
		req  *viewpb.PrewarmOnViewRequest
	}
	var calls errgroup.Group
	calls.SetLimit(max(1, hardware.GetCPUNum()))
	defer func() { _ = calls.Wait() }()
	for len(pending) > 0 && ctx.Err() == nil {
		changed, released := subscription.Observe()
		if released {
			return
		}
		var targets []target
		err := r.loadConfigStore.TryWithCurrentConfig(cfg, func() error {
			for shard, stats := range r.shardViewRegistry.SnapshotForCollection(cfg.CollectionID).StatsMap() {
				if _, waiting := pending[shard]; !waiting || stats.UpVersion == nil || stats.UpLoadInfoVersion != cfg.Version {
					continue
				}
				delete(pending, shard)
				req := &viewpb.PrewarmOnViewRequest{ShardId: shard.IntoProto(), Version: stats.UpVersion.IntoProto()}
				targets = append(targets, target{qviews.StreamingNode{PChannel: funcutil.ToPhysicalChannel(shard.VChannel)}, req})
				for _, id := range stats.UpNodes {
					targets = append(targets, target{qviews.QueryNode{ID: id}, req})
				}
			}
			return nil
		})
		if err != nil {
			mlog.Info(ctx, "discard pending collection prewarm", mlog.FieldCollectionID(cfg.CollectionID), mlog.Err(err))
			return
		}
		for _, target := range targets {
			if ctx.Err() != nil {
				return
			}
			calls.Go(func() error {
				if ctx.Err() == nil {
					if err := r.prewarm(ctx, target.node, target.req); err != nil {
						mlog.Info(ctx, "one-time prewarm ended", mlog.FieldCollectionID(cfg.CollectionID), mlog.Stringer("worker", target.node), mlog.Err(err))
					}
				}
				return nil
			})
		}
		if len(pending) == 0 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-r.readyChanges.Done():
			return
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

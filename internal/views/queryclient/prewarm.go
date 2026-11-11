package queryclient

import (
	"context"
	"slices"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const prewarmConcurrency = 16

// Prewarm warms each configured replica once using the client's query route.
func Prewarm(ctx context.Context, client Client, collectionID int64) error {
	warmer, ok := client.(interface {
		Prewarm(context.Context, int64) error
	})
	if !ok {
		return merr.Wrap(merr.ErrServiceUnimplemented, "query client does not support prewarm")
	}
	return warmer.Prewarm(ctx, collectionID)
}

func (c *legacyOnlyClient) Prewarm(ctx context.Context, collectionID int64) error {
	return c.legacy.prewarm(ctx, collectionID)
}

func (c *viewQueryClientImpl) Prewarm(ctx context.Context, collectionID int64) error {
	return c.legacyClient.prewarm(ctx, collectionID)
}

// prewarm captures the configured replica set, then independently waits for each
// shard to become Up. A target's advertised View and owner are selected once.
// Neither plans nor executions are retried, even when ordinary queries could
// recover by selecting a newer View or another worker.
func (c *legacyClient) prewarm(ctx context.Context, collectionID int64) (err error) {
	defer func() { err = toMilvusError(err) }()
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	client := c.shardClient
	// Closing the query client cancels planning, waits and borrowed worker calls.
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		select {
		case <-ctx.Done():
		case <-client.replicas.done:
			cancel(merr.WrapErrServiceNotReadyMsg("query client closed"))
		}
	}()
	defer func() { cancel(nil); <-stopped }()
	info, err := client.prewarmLoadInfo(ctx, collectionID)
	if err != nil {
		return err
	}
	if info.GetVersion() == 0 || len(info.GetReplicaIds()) == 0 {
		return merr.WrapErrServiceNotReadyMsg("prewarm requires a configured replica set")
	}
	// This request owns the load-config probe; QC owns no prewarm task.
	// View changes do not cancel borrowers.
	checked := make(chan struct{})
	go func() {
		defer close(checked)
		client.watchPrewarmLoad(ctx, cancel, collectionID, info.Version)
	}()
	defer func() { cancel(nil); <-checked }()
	vchannels, err := c.shardResolver.ResolveVChannels(ctx, collectionID)
	if err != nil {
		return err
	}
	replicas := slices.Clone(info.ReplicaIds)
	slices.Sort(replicas)
	replicas = slices.Compact(replicas)
	slots := semaphore.NewWeighted(prewarmConcurrency)
	var tasks errgroup.Group // One failed replica must not cancel the others.
	for _, vc := range vchannels {
		for _, replica := range replicas {
			tasks.Go(func() error {
				return client.prewarmShard(ctx, slots, collectionID, info.Version, qviews.ShardID{VChannel: vc, ReplicaID: replica})
			})
		}
	}
	err = tasks.Wait()
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	return err
}

func (c *shardViewQueryClient) prewarmShard(ctx context.Context, slots *semaphore.Weighted, collectionID int64, loadVersion uint64, shard qviews.ShardID) error {
	route, err := c.replicas.route(shard.VChannel)
	if err != nil {
		return err
	}
	version, err := route.waitPrewarmView(ctx, shard.ReplicaID)
	if err != nil {
		return err
	}
	req := &viewpb.GetPrewarmPlanRequest{CollectionId: collectionID, ShardId: shard.IntoProto(), Version: version.IntoProto(), LoadInfoVersion: loadVersion}
	if err := slots.Acquire(ctx, 1); err != nil {
		return err
	}
	plan, err := c.queryPlanClient.GetPrewarmPlan(ctx, shard, req)
	slots.Release(1)
	if err != nil {
		return err
	}
	if plan == nil || plan.Plan == nil || !proto.Equal(plan.Plan.ShardId, req.ShardId) || !proto.Equal(plan.Plan.Version, req.Version) {
		return viewerror.NewViewInvalidated("prewarm plan does not match the selected View")
	}
	var workers errgroup.Group
	for _, node := range workNodesFromPlan(plan.Plan) {
		workers.Go(func() error {
			if err := slots.Acquire(ctx, 1); err != nil {
				return err
			}
			defer slots.Release(1)
			return c.queryServiceClient.PrewarmOnView(ctx, node, plan)
		})
	}
	return workers.Wait()
}

func (c *shardViewQueryClient) prewarmLoadInfo(ctx context.Context, collectionID int64) (*querypb.GetQueryViewLoadInfoResponse, error) {
	if c.loadChecks == nil || c.loadChecks.client == nil {
		return nil, merr.WrapErrServiceInternalMsg("prewarm requires a Coordinator load-info client")
	}
	info, err := c.loadChecks.client.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: collectionID})
	if err := merr.CheckRPCCall(info, err); err != nil {
		return nil, err
	}
	return info, nil
}

func (c *shardViewQueryClient) watchPrewarmLoad(ctx context.Context, cancel context.CancelCauseFunc, collectionID int64, version uint64) {
	ticker := time.NewTicker(loadStateCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			probeCtx, stop := context.WithTimeout(ctx, loadStateCheckInterval)
			info, err := c.prewarmLoadInfo(probeCtx, collectionID)
			stop()
			if err != nil {
				cancel(err)
				return
			}
			if info.GetVersion() != version {
				cancel(viewerror.NewViewInvalidated("prewarm load configuration changed"))
				return
			}
		}
	}
}

func (r *replicaRoute) waitPrewarmView(ctx context.Context, replicaID int64) (qviews.QueryViewVersion, error) {
	for {
		if err := ctx.Err(); err != nil {
			return qviews.QueryViewVersion{}, err
		}
		r.mu.Lock()
		closed, snapshot, changed := r.closed, r.snapshot, r.changed
		r.mu.Unlock()
		if closed {
			return qviews.QueryViewVersion{}, merr.WrapErrServiceNotReadyMsg("replica router closed")
		}
		if snapshot.Valid {
			for _, replica := range snapshot.Replicas {
				if replica.ReplicaID == replicaID {
					return replica.MaxUpViewVersion, nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return qviews.QueryViewVersion{}, ctx.Err()
		case <-changed:
		}
	}
}

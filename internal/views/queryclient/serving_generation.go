package queryclient

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/milvus-io/milvus/internal/views/queryclient/resolver"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type servingShardContextKey struct{}

type servingGenerationRequirement interface {
	RequiresServingGeneration(context.Context, int64) ([]string, bool, error)
}

// servingResolver resolves every request from a fresh publication. It retains
// no generation cache, so a whole-request retry always gets the new shard set.
type servingResolver struct {
	resolver.ShardResolver
	client LoadInfoClient
}

func withServingResolver(fallback resolver.ShardResolver, client LoadInfoClient) resolver.ShardResolver {
	if client == nil {
		return fallback
	}
	return &servingResolver{ShardResolver: fallback, client: client}
}

func (r *servingResolver) ResolveShards(ctx context.Context, id int64) ([]resolver.Shard, error) {
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	if r.ShardResolver == nil {
		return r.publishedShards(ctx, id)
	}
	marker, ok := r.ShardResolver.(servingGenerationRequirement)
	if !ok {
		return resolver.ResolveShards(ctx, r.ShardResolver, id)
	}
	owners, split, err := marker.RequiresServingGeneration(ctx, id)
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	if err != nil {
		return nil, err
	}
	if !split {
		result := make([]resolver.Shard, len(owners))
		for i, owner := range owners {
			result[i] = resolver.Shard{VChannel: owner}
		}
		return result, nil
	}

	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = 20 * time.Millisecond
	bo.MaxInterval = 200 * time.Millisecond
	bo.MaxElapsedTime = 0
	bo.Reset()
	for {
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		if split {
			published, err := r.publishedShards(ctx, id)
			if ctx.Err() != nil {
				return nil, context.Cause(ctx)
			}
			if err != nil {
				return nil, err
			}
			names := make(map[string]bool, len(published))
			for _, shard := range published {
				names[shard.VChannel] = true
			}
			covered := true
			for _, owner := range owners {
				covered = covered && names[owner]
			}
			if covered {
				return published, nil
			}
		}
		// Write owners are only a coverage lower bound, never the read list.
		// The publisher certifies complete lineage on every replica. Before it
		// advances, a fenced source's old publication cannot plan safely.
		timer := time.NewTimer(bo.NextBackOff())
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, context.Cause(ctx)
		case <-timer.C:
		}
		if refresher, ok := r.ShardResolver.(interface{ RefreshServingGeneration(context.Context, int64) }); ok {
			refresher.RefreshServingGeneration(ctx, id)
		}
		owners, split, err = marker.RequiresServingGeneration(ctx, id)
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		if err != nil {
			return nil, err
		}
		// An older in-flight metadata fill can land after refresh. Once this
		// request observed a split, a false marker must not restore g0 routing.
	}
}

func (r *servingResolver) publishedShards(ctx context.Context, id int64) ([]resolver.Shard, error) {
	info, err := r.client.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: id})
	if err = merr.CheckRPCCall(info, err); err != nil {
		return nil, err
	}
	if len(info.GetServingShards()) == 0 {
		return nil, merr.WrapErrServiceNotReadyMsg("split collection %d serving generation is not published", id)
	}

	result := make([]resolver.Shard, 0, len(info.GetServingShards()))
	for _, shard := range info.GetServingShards() {
		next := resolver.Shard{VChannel: shard.GetVchannel(), ShardSetEpoch: info.GetShardSetEpoch()}
		if lineage := shard.GetLineage(); lineage != nil {
			next.Lineage = &resolver.ShardLineage{VChannels: append([]string(nil), lineage.GetVchannels()...), SwitchTimeTick: lineage.GetSwitchTimetick()}
		}
		result = append(result, next)
	}
	return result, nil
}

func retryServingGeneration(ctx context.Context, shards resolver.ShardResolver, attempts int, collectionID int64, operation func() error) error {
	_, serving := shards.(*servingResolver)
	for attempt := 0; ; attempt++ {
		err := operation()
		if err == nil || !serving || attempt+1 >= attempts {
			return err
		}
		ve := viewerror.AsViewError(err)
		if !ve.IsViewNotFound() && !ve.IsViewInvalidated() {
			return err
		}
		if current, ok := shards.(*servingResolver); ok {
			if current.ShardResolver != nil {
				if _, ok := current.ShardResolver.(servingGenerationRequirement); !ok {
					return err
				}
			}
			if refresher, ok := current.ShardResolver.(interface{ RefreshServingGeneration(context.Context, int64) }); ok {
				refresher.RefreshServingGeneration(ctx, collectionID)
			}
			if marker, ok := current.ShardResolver.(servingGenerationRequirement); ok {
				_, split, probeErr := marker.RequiresServingGeneration(ctx, collectionID)
				if probeErr != nil {
					return probeErr
				}
				if !split {
					return err
				}
			}
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

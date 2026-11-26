package queryclient

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/errors"
	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/milvus-io/milvus/internal/views/queryclient/resolver"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type servingShardContextKey struct{}

type servingGenerationRequirement interface {
	RequiresServingGeneration(context.Context, int64) ([]string, bool, error)
}

const servingPublicationCacheCapacity = 1024

// servingResolver owns a cluster-local cache of complete publications. Capacity
// limits memory only: the SN checks the identity on every planning request.
type servingResolver struct {
	resolver.ShardResolver
	client       LoadInfoClient
	mu           sync.Mutex
	publications *lru.Cache[int64, *servingPublication]
}

type servingPublication struct {
	shards      []resolver.Shard
	inflight    *servingPublicationCall
	invalidated bool
}

type servingPublicationCall struct {
	done          chan struct{}
	shards        []resolver.Shard
	err           error
	ownerCanceled bool
}

func withServingResolver(fallback resolver.ShardResolver, client LoadInfoClient) resolver.ShardResolver {
	if client == nil {
		return fallback
	}
	cache, _ := lru.New[int64, *servingPublication](servingPublicationCacheCapacity)
	return &servingResolver{ShardResolver: fallback, client: client, publications: cache}
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
			r.invalidate(id)
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
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval, bo.MaxInterval, bo.MaxElapsedTime = 20*time.Millisecond, 200*time.Millisecond, 0
	bo.Reset()
	for {
		shards, err := r.cachedPublication(ctx, id)
		if !errors.Is(err, merr.ErrServiceNotReady) {
			return shards, err
		}
		timer := time.NewTimer(bo.NextBackOff())
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, context.Cause(ctx)
		case <-timer.C:
		}
	}
}

func (r *servingResolver) cachedPublication(ctx context.Context, id int64) ([]resolver.Shard, error) {
	for {
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		r.mu.Lock()
		entry, ok := r.publications.Get(id)
		if !ok {
			entry = &servingPublication{}
			r.publications.Add(id, entry)
		}
		if entry.shards != nil {
			shards := cloneServingShards(entry.shards)
			r.mu.Unlock()
			return shards, nil
		}
		call := entry.inflight
		if call != nil {
			r.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, context.Cause(ctx)
			case <-call.done:
			}
		} else {
			call = &servingPublicationCall{done: make(chan struct{})}
			entry.inflight = call
			r.mu.Unlock()
			call.shards, call.err = r.fetchPublication(ctx, id)
			if ctx.Err() != nil {
				call.ownerCanceled = true
				call.err = context.Cause(ctx)
			}
			r.mu.Lock()
			current, retained := r.publications.Peek(id)
			if retained && current == entry && call.err == nil {
				entry.shards = call.shards
			}
			entry.inflight = nil
			close(call.done)
			r.mu.Unlock()
		}
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		r.mu.Lock()
		invalidated := entry.invalidated
		r.mu.Unlock()
		if invalidated || call.ownerCanceled {
			continue
		}
		return cloneServingShards(call.shards), call.err
	}
}

func cloneServingShards(shards []resolver.Shard) []resolver.Shard {
	result := append([]resolver.Shard(nil), shards...)
	for i := range result {
		if lineage := result[i].Lineage; lineage != nil {
			copy := *lineage
			copy.VChannels = append([]string(nil), lineage.VChannels...)
			result[i].Lineage = &copy
		}
	}
	return result
}

// Invalidating removes the entry identity, so an older RPC cannot refill it.
// Eviction is different: callers already using an evicted entry can finish,
// but its completion cannot restore that entry to the bounded cache.
func (r *servingResolver) invalidate(id int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if id == 0 {
		for _, entry := range r.publications.Values() {
			entry.invalidated = true
		}
		r.publications.Purge()
		return
	}
	if entry, ok := r.publications.Peek(id); ok {
		entry.invalidated = true
		r.publications.Remove(id)
	}
}

// InvalidateServingGeneration accepts nil when QueryView serving is disabled.
// Collection zero clears this client's cluster-local publications (DropDatabase).
func InvalidateServingGeneration(client Client, collectionID int64) {
	if current, ok := client.(interface{ invalidateServingGeneration(int64) }); ok {
		current.invalidateServingGeneration(collectionID)
	}
}

func (c *legacyOnlyClient) invalidateServingGeneration(id int64) {
	if r, ok := c.legacy.shardResolver.(*servingResolver); ok {
		r.invalidate(id)
	}
	c.legacy.shardClient.loadChecks.invalidate(id)
}

func (c *viewQueryClientImpl) invalidateServingGeneration(id int64) {
	if r, ok := c.shardResolver.(*servingResolver); ok {
		r.invalidate(id)
	}
	c.shardClient.loadChecks.invalidate(id)
}

func (r *servingResolver) fetchPublication(ctx context.Context, id int64) ([]resolver.Shard, error) {
	info, err := r.client.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: id})
	if err = merr.CheckRPCCall(info, err); err != nil {
		return nil, err
	}
	if len(info.GetServingShards()) == 0 {
		return nil, merr.WrapErrServiceNotReadyMsg("split collection %d serving generation is not published", id)
	}

	result := make([]resolver.Shard, 0, len(info.GetServingShards()))
	for _, shard := range info.GetServingShards() {
		next := resolver.Shard{VChannel: shard.GetVchannel(), ShardSetEpoch: info.GetShardSetEpoch(), LoadInfoVersion: info.GetVersion(), ReadSchemaVersion: info.GetReadSchemaVersion()}
		if lineage := shard.GetLineage(); lineage != nil {
			next.Lineage = &resolver.ShardLineage{VChannels: append([]string(nil), lineage.GetVchannels()...), SwitchTimeTick: lineage.GetSwitchTimetick()}
		}
		result = append(result, next)
	}
	return result, nil
}

func retryServingGeneration(ctx context.Context, shards resolver.ShardResolver, attempts int, collectionID int64, operation func() error) error {
	current, serving := shards.(*servingResolver)
	for attempt := 0; ; attempt++ {
		err := operation()
		if err == nil || !serving || !retryableQueryError(err) {
			return err
		}
		ve := viewerror.AsViewError(err)
		invalidated := ve.IsViewNotFound() || ve.IsViewInvalidated()
		if invalidated {
			current.invalidate(collectionID)
		}
		if attempt+1 >= attempts {
			return err
		}
		if current.ShardResolver != nil {
			if _, ok := current.ShardResolver.(servingGenerationRequirement); !ok {
				return err
			}
		}
		if invalidated {
			if refresher, ok := current.ShardResolver.(interface{ RefreshServingGeneration(context.Context, int64) }); ok {
				refresher.RefreshServingGeneration(ctx, collectionID)
			}
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
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

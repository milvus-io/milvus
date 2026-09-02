package resolver

import (
	"context"
	"sort"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

var (
	ErrShardResolverClosed = errors.New("shard resolver is closed")
)

var _ ShardResolver = (*ShardResolverImpl)(nil)

// ShardReplicas contains all replicas of a shard (vchannel), with the primary replica identified.
type ShardReplicas struct {
	VChannel       string
	PrimaryShardID qviews.ShardID   // The primary replica (owns WAL).
	ShardIDs       []qviews.ShardID // All replicas including primary.
}

// ShardResolver resolves shard topology for a collection.
type ShardResolver interface {
	// ResolveVChannels returns all vchannels of a collection.
	// Used by the collection-level client to determine the shard fanout.
	// It blocks until the first assignment discovery snapshot is ready.
	ResolveVChannels(ctx context.Context, collectionID int64) ([]string, error)

	// ResolveShard returns the replicas of a single shard identified by vchannel.
	// Used by the shard-level client for replica selection and consistency routing.
	// It blocks until the first assignment discovery snapshot is ready.
	ResolveShard(ctx context.Context, collectionID int64, vchannel string) (*ShardReplicas, error)
}

func NewShardResolverImpl(w types.AssignmentDiscoverWatcher) *ShardResolverImpl {
	t := &ShardResolverImpl{
		taskNotifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		w:            w,
		cond:         syncutil.NewContextCond(&sync.Mutex{}),
	}
	go t.watch()
	return t
}

type ShardResolverImpl struct {
	taskNotifier *syncutil.AsyncTaskNotifier[struct{}]

	w    types.AssignmentDiscoverWatcher
	cond *syncutil.ContextCond

	closed bool
	cache  *shardResolverCache
}

type shardResolverCache struct {
	collectionVChannels map[int64][]string
	shardReplicas       map[collectionVChannelKey]*ShardReplicas
}

type collectionVChannelKey struct {
	collectionID int64
	vchannel     string
}

func (t *ShardResolverImpl) ResolveVChannels(ctx context.Context, collectionID int64) ([]string, error) {
	cache, err := t.getCache(ctx)
	if err != nil {
		return nil, err
	}
	vchannels, ok := cache.collectionVChannels[collectionID]
	if !ok || len(vchannels) == 0 {
		return nil, merr.WrapErrCollectionNotLoaded(collectionID)
	}
	return append([]string(nil), vchannels...), nil
}

func (t *ShardResolverImpl) ResolveShard(ctx context.Context, collectionID int64, vchannel string) (*ShardReplicas, error) {
	cache, err := t.getCache(ctx)
	if err != nil {
		return nil, err
	}
	replicas := cache.shardReplicas[collectionVChannelKey{collectionID: collectionID, vchannel: vchannel}]
	if replicas == nil {
		return nil, errors.Errorf("shard replicas not found: collection=%d, vchannel=%s", collectionID, vchannel)
	}
	return cloneShardReplicas(replicas), nil
}

func (t *ShardResolverImpl) Close() {
	t.cond.LockAndBroadcast()
	if t.closed {
		t.cond.L.Unlock()
		return
	}
	t.closed = true
	t.taskNotifier.Cancel()
	t.cond.L.Unlock()
	t.taskNotifier.BlockUntilFinish()
}

func (t *ShardResolverImpl) watch() {
	defer t.taskNotifier.Finish(struct{}{})
	_ = t.w.AssignmentDiscover(t.taskNotifier.Context(), func(assignments *types.VersionedStreamingNodeAssignments) error {
		cache := buildShardResolverCache(assignments)
		t.cond.LockAndBroadcast()
		t.cache = &cache
		t.cond.L.Unlock()
		return nil
	})
}

func (t *ShardResolverImpl) getCache(ctx context.Context) (shardResolverCache, error) {
	t.cond.L.Lock()
	for t.cache == nil && !t.closed {
		if err := t.cond.Wait(ctx); err != nil {
			// ContextCond.Wait does not re-acquire the lock when it returns an error.
			return shardResolverCache{}, err
		}
	}
	defer t.cond.L.Unlock()
	if t.cache != nil {
		return *t.cache, nil
	}
	return shardResolverCache{}, ErrShardResolverClosed
}

func buildShardResolverCache(assignments *types.VersionedStreamingNodeAssignments) shardResolverCache {
	cache := shardResolverCache{
		collectionVChannels: make(map[int64][]string),
		shardReplicas:       make(map[collectionVChannelKey]*ShardReplicas),
	}
	vchannelSets := make(map[int64]map[string]struct{})
	for _, assignment := range assignments.Assignments {
		pchannelPrimary := make(map[string]bool, len(assignment.Channels)+len(assignment.SecondaryChannels))
		for pchannel := range assignment.Channels {
			pchannelPrimary[pchannel] = true
		}
		for pchannel := range assignment.SecondaryChannels {
			if _, ok := pchannelPrimary[pchannel]; !ok {
				pchannelPrimary[pchannel] = false
			}
		}
		for _, pchannelAssignment := range assignment.ShardAssignment.PChannelAssignments {
			primary, ok := pchannelPrimary[pchannelAssignment.PChannel]
			if !ok {
				continue
			}
			for _, entry := range pchannelAssignment.Entries {
				vchannel := funcutil.GetVirtualChannel(pchannelAssignment.PChannel, entry.CollectionID, int(entry.ShardIndex))
				shardID := qviews.ShardID{ReplicaID: entry.ReplicaID, VChannel: vchannel}
				key := collectionVChannelKey{collectionID: entry.CollectionID, vchannel: vchannel}
				replicas := cache.shardReplicas[key]
				if replicas == nil {
					replicas = &ShardReplicas{VChannel: vchannel}
					cache.shardReplicas[key] = replicas
				}
				replicas.ShardIDs = append(replicas.ShardIDs, shardID)
				if primary {
					replicas.PrimaryShardID = shardID
				}
				if vchannelSets[entry.CollectionID] == nil {
					vchannelSets[entry.CollectionID] = make(map[string]struct{})
				}
				vchannelSets[entry.CollectionID][vchannel] = struct{}{}
			}
		}
	}
	for collectionID, vchannels := range vchannelSets {
		cache.collectionVChannels[collectionID] = make([]string, 0, len(vchannels))
		for vchannel := range vchannels {
			cache.collectionVChannels[collectionID] = append(cache.collectionVChannels[collectionID], vchannel)
		}
		sort.Strings(cache.collectionVChannels[collectionID])
	}
	for _, replicas := range cache.shardReplicas {
		sort.Slice(replicas.ShardIDs, func(i, j int) bool {
			if replicas.ShardIDs[i].VChannel != replicas.ShardIDs[j].VChannel {
				return replicas.ShardIDs[i].VChannel < replicas.ShardIDs[j].VChannel
			}
			return replicas.ShardIDs[i].ReplicaID < replicas.ShardIDs[j].ReplicaID
		})
	}
	return cache
}

func cloneShardReplicas(replicas *ShardReplicas) *ShardReplicas {
	return &ShardReplicas{
		VChannel:       replicas.VChannel,
		PrimaryShardID: replicas.PrimaryShardID,
		ShardIDs:       append([]qviews.ShardID(nil), replicas.ShardIDs...),
	}
}

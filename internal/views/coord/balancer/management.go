package balancer

import (
	"context"
	"slices"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Management exposes the same replica ownership and lifecycle used by automatic
// scheduling. Manual migration is a one-time placement change, not a node pin.
type Management interface {
	Placement(context.Context, int64) (*CollectionPlacement, error)
	ManualPlace(context.Context, PlacementScope, func(map[qviews.ShardID]*qviews.QueryViewAtCoordBuilder) error) (map[qviews.ShardID]ManualPlaceResult, error)
	MoveSegments(context.Context, int64, int64, []int64, []int64) (map[qviews.ShardID]ManualPlaceResult, error)
}

type CollectionPlacement struct {
	Config        *loadmgr.LoadConfig
	ConfigVersion uint64
	Nodes         map[int64][]int64
	Shards        map[qviews.ShardID]*coordview.ShardStats
	LayoutReady   bool
}

func (b *DefaultBalancer) Placement(ctx context.Context, collectionID int64) (*CollectionPlacement, error) {
	if err := b.cache.WaitForReady(ctx); err != nil {
		return nil, err
	}
	b.reconcileMu.Lock()
	defer b.reconcileMu.Unlock()
	return b.placementLocked(collectionID)
}

func (b *DefaultBalancer) placementLocked(collectionID int64) (*CollectionPlacement, error) {
	c := b.cache.GetCollection(collectionID)
	result := &CollectionPlacement{Nodes: make(map[int64][]int64), Shards: make(map[qviews.ShardID]*coordview.ShardStats), LayoutReady: true}
	if c == nil || c.LoadConfig() == nil {
		return result, nil
	}
	result.Config, result.ConfigVersion = c.LoadConfig(), c.ConfigVersion()
	policy, ok := b.policy.(*DefaultBalancePolicy)
	if !ok {
		return nil, merr.Wrap(merr.ErrServiceUnimplemented, "balance policy does not expose replica placement")
	}
	policy.mu.Lock()
	defer policy.mu.Unlock()
	reader := newPlanningContext(b.cache)
	for _, replica := range result.Config.Replicas {
		layout := policy.layouts.domains[collectionID][replica.ResourceGroup]
		if layout == nil {
			result.LayoutReady = false
			continue
		}
		nodes := reader.CandidateNodes(replica.ResourceGroup)
		slices.Sort(nodes)
		var replicas []int64
		for _, r := range result.Config.Replicas {
			if r.ResourceGroup == replica.ResourceGroup {
				replicas = append(replicas, r.ReplicaID)
			}
		}
		slices.Sort(replicas)
		if !slices.Equal(nodes, layout.nodes) || !slices.Equal(replicas, layout.replicas) {
			result.LayoutReady = false
			continue
		}
		result.Nodes[replica.ReplicaID] = slices.Clone(layout.targets[replica.ReplicaID])
	}
	c.RangeShards(func(s *cache.ShardEntry) bool { result.Shards[s.ID()] = s.Stats(); return true })
	return result, nil
}

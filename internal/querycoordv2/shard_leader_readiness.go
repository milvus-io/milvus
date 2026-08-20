package querycoordv2

import (
	"context"

	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
)

// GetShardLeaderReadinessByResourceGroup reports whether the replicas of
// collectionID that live in resource group rgName can serve every shard of it
// right now; an empty rgName means every replica of the collection. See
// utils.ShardLeaderReadinessByResourceGroup for why this cannot be derived
// from GetShardLeaders, and for the gate it uses instead of the
// collection-wide checkLoadStatus.
//
// The computation lives in the utils package rather than here so that the
// observers, which hold the same read-only stores and cannot import this
// package, can reach it too. This method exists so external callers, which
// reach querycoord through Server, keep a stable entry point - the same split
// GetLoadPercentageByResourceGroup uses.
func (s *Server) GetShardLeaderReadinessByResourceGroup(ctx context.Context, collectionID int64, rgName string) (utils.ShardLeaderReadiness, error) {
	return utils.ShardLeaderReadinessByResourceGroup(ctx, s.meta, s.targetMgr, s.dist, s.nodeMgr, collectionID, rgName)
}

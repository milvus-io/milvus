package querycoordv2

import (
	"context"

	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
)

// GetLoadPercentageByResourceGroup reports how loaded collectionID is,
// restricted to the replica(s) that live in resource group rgName; an empty
// rgName means every replica of the collection. See
// utils.LoadPercentageByResourceGroup for the semantics of the returned
// percentage, including the deliberate difference between -1 ("no replica in
// this resource group") and 0 ("a replica is there but carries nothing yet").
//
// The computation itself lives in the utils package rather than here because
// CollectionObserver needs the same figure and cannot import this package.
// This method exists so external callers, which reach querycoord through
// Server, keep a stable entry point.
func (s *Server) GetLoadPercentageByResourceGroup(ctx context.Context, collectionID int64, rgName string) (int32, error) {
	return utils.LoadPercentageByResourceGroup(ctx, s.meta, s.targetMgr, s.dist, collectionID, rgName)
}

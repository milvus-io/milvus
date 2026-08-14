package querycoordv2

import (
	"context"
	"maps"

	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// This file is querycoord's seam for the load-placement-scope capability. It
// declares WHERE querycoord asks the installed extension whether a load request
// states the collection's whole desired placement or only the placement of the
// resource groups it names; the answer lives outside this tree.
//
// With no capability installed the function below returns its input, so a stock
// binary computes the placement it always did.

// completePlacementForOutOfScopeResourceGroups returns the per-resource-group
// replica counts to record for this load request.
//
// Natively that is exactly what the request asked for: a load request states
// the collection's whole placement, so a resource group missing from expected
// is a resource group asking for zero replicas, and the reconciliation that
// builds the broadcast record hands the replica living there to a resource
// group the request does ask for. That is correct for a caller that names every
// resource group the collection is to live in, which is every native caller.
//
// When the installed capability says this request speaks only for the resource
// groups it names, the counts the collection already has in the resource groups
// it did not name are carried through alongside the requested ones. The result
// is the cumulative request a native caller would have sent, so the
// reconciliation leaves those replicas where they are - and the record it
// builds describes a placement that grew rather than one that moved, which is
// what lets the load job downstream recognize it as an incremental expansion
// and keep the collection's serving state intact.
//
// expected is the non-nil map utils.AssignReplica just returned. It is never
// mutated, because the caller may still log it.
func completePlacementForOutOfScopeResourceGroups(
	ctx context.Context,
	collectionID int64,
	resourceGroups []string,
	expected map[string]int,
	current job.CurrentLoadConfig,
) map[string]int {
	scope := extension.Caps().LoadPlacement
	if scope == nil {
		return expected
	}
	if !scope.ScopedToNamedResourceGroups(ctx, collectionID, resourceGroups) {
		return expected
	}

	named := typeutil.NewSet(resourceGroups...)
	completed := maps.Clone(expected)
	carried := make([]string, 0)
	for rgName, replicaNumber := range current.GetReplicaNumber() {
		if named.Contain(rgName) {
			continue
		}
		completed[rgName] = replicaNumber
		carried = append(carried, rgName)
	}
	if len(carried) > 0 {
		mlog.Info(ctx, "load request is scoped to the resource groups it names, keeping the placement of the others",
			mlog.Int64("collectionID", collectionID),
			mlog.Strings("requestedResourceGroups", resourceGroups),
			mlog.Strings("keptResourceGroups", carried),
		)
	}
	return completed
}

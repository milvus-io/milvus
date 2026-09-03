// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package querycoordv2

import (
	"context"
	"maps"

	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// completePlacementForOutOfScopeResourceGroups returns the per-resource-group
// replica counts to record for this load request.
//
// Natively that is exactly what the request asked for: a load request states
// the collection's whole placement, so a resource group missing from expected
// is a resource group asking for zero replicas, and the reconciliation that
// builds the broadcast record hands the replica living there to a resource
// group the request does ask for.
//
// With queryCoord.resourceGroupScopedLoad on, a request speaks only for the
// resource groups it names: the counts the collection already has in the
// groups it did not name are carried through alongside the requested ones.
// The result is the cumulative request a native caller would have sent, so
// the reconciliation leaves those replicas where they are, and the record it
// builds describes a placement that grew rather than one that moved - which is
// what lets the load job recognize it as an incremental expansion and keep the
// collection's serving state intact.
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
	if !paramtable.Get().QueryCoordCfg.ResourceGroupScopedLoad.GetAsBool() {
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

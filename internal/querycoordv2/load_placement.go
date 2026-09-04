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
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// completePlacementForOutOfScopeResourceGroups returns the per-resource-group
// replica counts to record for this load request.
//
// On a stock binary the answer is always what utils.AssignReplica produced: a
// load request states the whole placement, and a second LoadCollection naming
// rg_1 on a collection loaded in rg_0 moves the replica to rg_1, as it always
// has (job/load_config_test.go pins the reconciliation that does the moving).
//
// With a form installed (extension.FormInstalled) a load request that NAMES
// resource groups speaks only for those: the counts the collection already has
// in the groups it did not name are carried through alongside the requested
// ones. A request that names none still speaks for the whole placement. The
// result is the cumulative placement the collection ends up with, so the
// reconciliation leaves those replicas where they are, and the record it
// builds describes a placement that grew rather than one that moved - which is
// what lets the load job recognize a request that only adds resource groups
// as a pure expansion and keep the collection's serving state intact. A
// distribution that loads one collection into several resource groups
// independently needs that reading; a stock deployment, whose LoadCollection
// has always meant "this is the placement", keeps its contract, including the
// request's replica_number being the total.
//
// requestedResourceGroups is the list the REQUEST named, not the one the load
// path defaulted: see getLoadReplicaConfigForRequest, which returns the two
// separately, for why the difference decides the answer.
//
// expected is the non-nil map utils.AssignReplica just returned. It is never
// mutated, because the caller may still log it.
func completePlacementForOutOfScopeResourceGroups(
	ctx context.Context,
	collectionID int64,
	requestedResourceGroups []string,
	expected map[string]int,
	current job.CurrentLoadConfig,
) map[string]int {
	if !extension.FormInstalled() || len(requestedResourceGroups) == 0 {
		// A stock binary states the whole placement on every request, and a
		// form does so for a request naming no group: what AssignReplica
		// produced is the whole answer, and carrying anything over would add
		// replicas the request did not ask for.
		return expected
	}

	named := typeutil.NewSet(requestedResourceGroups...)
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
			mlog.Strings("requestedResourceGroups", requestedResourceGroups),
			mlog.Strings("keptResourceGroups", carried),
		)
	}
	return completed
}

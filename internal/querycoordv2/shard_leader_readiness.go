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

	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
	// The health gate is what actually makes this safe to call on a Server
	// that is still coming up -- see the note on
	// GetLoadPercentageByResourceGroup for why the nil checks downstream
	// cannot supply that on their own.
	//
	// The error is RETURNED, not swallowed into a bare not-ready verdict.
	// Reason alone cannot carry it: ShardLeadersReasonCoordinatorNotReady is
	// also what the utils function reports when its stores are nil, and more
	// importantly a caller polling this surface while waiting for a group to
	// come up cannot otherwise tell a coordinator restart (retriable, and not
	// about this resource group at all) from "this group's shards have no
	// serviceable leader yet". One burns a caller's timeout budget and should
	// not count against it; the other is exactly what the budget is for.
	// CheckHealthy also distinguishes "still starting" from "abnormal or
	// stopping", which a bare verdict erases. Reason stays set so a caller
	// that only logs still gets the same string; the two are not in conflict.
	// This matches GetLoadPercentageByResourceGroup, which returns
	// ErrServiceNotReady for the identical condition.
	if err := merr.CheckHealthy(s.State()); err != nil {
		return utils.ShardLeaderReadiness{Reason: utils.ShardLeadersReasonCoordinatorNotReady}, err
	}
	return utils.ShardLeaderReadinessByResourceGroup(ctx, s.meta, s.targetMgr, s.dist, s.nodeMgr, collectionID, rgName)
}

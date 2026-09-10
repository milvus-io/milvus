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

package utils

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// CheckDelegatorCapacity refuses a load whose replicas would outnumber the
// streaming query nodes of the pool they are served from, so that no replica
// is admitted only to wait forever for a delegator it can never receive.
//
// With the streaming service on, every replica needs a streaming query node
// for its delegator: the channel checker places delegators on the replica's
// streaming nodes only, and the replica manager hands those out without
// overlap between the replicas of a pool. Which pool a replica is served from
// is not its own group's node set: the replica manager may fold the replicas
// of a group without streaming nodes into the default group's pool, or pool
// every streaming node for every replica of the collection (see
// meta.GroupIntoSQNodePools). So the bound is taken over the collection's
// WHOLE layout - the replicas it already holds plus the request - grouped
// into pools exactly as the assignment will group them.
//
// replicaNumInRG is what AssignReplica produced for the request. A scoped
// request (scoped == true) states the count of the groups it names and leaves
// the collection's other groups as they are; a whole-placement request
// replaces the layout, and the replicas it does not name are on their way
// out.
//
// Only what the request ADDS is admitted. A pool that holds more replicas
// than nodes refuses the request with ErrResourceGroupNodeNotEnough naming
// the pool when the request adds a replica to that pool, as does a replica
// the request adds that belongs to no pool (a group without streaming nodes
// under strict isolation). A pool short of a node because one is restarting
// is not the request's doing: the load that placed the collection's
// replicas, re-sent, adds nothing and is the no-op it always was, and an
// expansion into another pool is judged on its own pool.
//
// Only an installed form with the streaming service on runs this. A stock
// binary never runs a resource group on streaming nodes alone and keeps the
// admission it always had, so it returns nil without looking.
func CheckDelegatorCapacity(ctx context.Context, m *meta.Meta, collectionID int64, replicaNumInRG map[string]int, scoped bool) error {
	if !extension.FormInstalled() || !streamingutil.IsStreamingServiceEnabled() {
		return nil
	}

	existing := make(map[string]int)
	for _, replica := range m.GetByCollection(ctx, collectionID) {
		existing[replica.GetResourceGroup()]++
	}
	layout := make(map[string]int)
	if scoped {
		maps.Copy(layout, existing)
	}
	maps.Copy(layout, replicaNumInRG)

	// One entry per replica, groups in a fixed order so that a refusal reads
	// the same for the same layout. The replicas a group already holds come
	// first and are kept; the rest are what the request adds.
	rgOfReplicas := make([]string, 0)
	added := make([]bool, 0)
	for _, rgName := range slices.Sorted(maps.Keys(layout)) {
		for i := 0; i < layout[rgName]; i++ {
			rgOfReplicas = append(rgOfReplicas, rgName)
			added = append(added, i >= existing[rgName])
		}
	}

	strictIsolation := paramtable.Get().StreamingCfg.StrictResourceGroupIsolationEnabled.GetAsBool()
	sqnNodesByRG := snmanager.StaticStreamingNodeManager.GetStreamingQueryNodeIDsByResourceGroup()
	pools, unpooled := meta.GroupIntoSQNodePools(rgOfReplicas, sqnNodesByRG, strictIsolation)

	for _, i := range unpooled {
		if !added[i] {
			continue
		}
		rgName := rgOfReplicas[i]
		mlog.Warn(ctx, "refusing a load whose replica has no streaming query node to serve it under strict isolation",
			mlog.Int64("collectionID", collectionID),
			mlog.String("resourceGroup", rgName),
			mlog.Int("replicas", layout[rgName]))
		return merr.WrapErrResourceGroupNodeNotEnough(rgName, 0, layout[rgName],
			"no streaming query node serves this resource group under strict isolation")
	}
	for _, poolName := range slices.Sorted(maps.Keys(pools)) {
		pool := pools[poolName]
		// The groups the request adds to this pool: the pool is named in the
		// refusal, and it may not be the group the operator asked for.
		addedIn := make(map[string]struct{})
		for _, i := range pool.Replicas {
			if added[i] {
				addedIn[rgOfReplicas[i]] = struct{}{}
			}
		}
		if len(addedIn) == 0 || len(pool.Replicas) <= pool.Nodes.Len() {
			continue
		}
		requested := strings.Join(slices.Sorted(maps.Keys(addedIn)), ",")
		mlog.Warn(ctx, "refusing a load whose replicas outnumber the streaming query nodes of their pool",
			mlog.Int64("collectionID", collectionID),
			mlog.String("pool", poolName),
			mlog.Int("streamingQueryNodes", pool.Nodes.Len()),
			mlog.Int("replicas", len(pool.Replicas)),
			mlog.String("requestedResourceGroups", requested),
			mlog.Any("layout", layout))
		return merr.WrapErrResourceGroupNodeNotEnough(poolName, pool.Nodes.Len(), len(pool.Replicas),
			fmt.Sprintf("the replicas of the collection served from this pool of streaming query nodes outnumber its nodes; the load into resource group [%s] adds to it", requested))
	}
	return nil
}

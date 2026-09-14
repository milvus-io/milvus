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
	"fmt"
	"sort"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// GetLoadConfigCompliance checks every involved collection against its effective
// target, including collections removed from load metadata but still releasing.
// A group with an empty reason is ready. The empty group name carries failures
// whose resource group cannot be determined. Callers must check these globally.
func (s *Server) GetLoadConfigCompliance(ctx context.Context) (map[string]string, error) {
	if err := merr.CheckHealthy(s.State()); err != nil {
		return nil, err
	}
	result := make(map[string]string)
	record := func(rg, reason string) {
		if result[rg] == "" {
			result[rg] = reason
		}
	}
	collections := make(map[int64]*meta.Collection)
	ids := typeutil.NewUniqueSet(s.meta.GetCollectionIDs()...)
	for _, collection := range s.meta.GetAllCollections(ctx) {
		id := collection.GetCollectionID()
		collections[id] = collection
		ids.Insert(id)
	}
	// Read distribution independently of load registration: release removes the
	// collection registration before QueryNodes acknowledge resource cleanup.
	segments := s.dist.SegmentDistManager.GetByFilter()
	channels := s.dist.ChannelDistManager.GetByFilter()
	resources := make(map[int64]map[int64]int)
	addResource := func(collectionID, nodeID int64) {
		ids.Insert(collectionID)
		if resources[collectionID] == nil {
			resources[collectionID] = make(map[int64]int)
		}
		resources[collectionID][nodeID]++
	}
	for _, segment := range segments {
		addResource(segment.GetCollectionID(), segment.Node)
	}
	for _, ch := range channels {
		addResource(ch.GetCollectionID(), ch.Node)
	}
	orderedIDs := ids.Collect()
	sort.Slice(orderedIDs, func(i, j int) bool { return orderedIDs[i] < orderedIDs[j] })
	cfg := &paramtable.Get().QueryCoordCfg
	clusterCount := cfg.ClusterLevelLoadReplicaNumber.GetAsInt32()
	clusterRGs := cfg.ClusterLevelLoadResourceGroups.GetAsStrings()
	forceOverride := cfg.ClusterLevelLoadForceOverrideUserReplicaMode.GetAsBool()
	// Match the watcher's validity rules: incomplete config is not an effective
	// override. Existing collections retain their accepted targets in that case.
	clusterConfigValid := clusterCount > 0 && (len(clusterRGs) == 1 || len(clusterRGs) == int(clusterCount))
	for _, id := range orderedIDs {
		collection := collections[id]
		replicas := append([]*meta.Replica(nil), s.meta.GetByCollection(ctx, id)...)
		sort.Slice(replicas, func(i, j int) bool { return replicas[i].GetID() < replicas[j].GetID() })
		expected := make(map[string]int)
		expectedTotal := 0
		if collection != nil {
			expectedTotal = int(collection.GetReplicaNumber())
			for rg, count := range collection.GetResourceGroupReplicaNumbers() {
				expected[rg] = int(count)
			}
			if clusterConfigValid && (!collection.GetUserSpecifiedReplicaMode() || forceOverride) {
				// This shape was validated above; normalization does not inspect
				// nodes or impose node quota/ownership convergence requirements.
				expected, _ = utils.ReplicaCounts(clusterRGs, clusterCount)
				expectedTotal = int(clusterCount)
			}
			if len(expected) == 0 {
				record("", fmt.Sprintf("collection %d: effective resource group load target is unknown", id))
			}
		}
		actual := make(map[string]int)
		for _, replica := range replicas {
			actual[replica.GetResourceGroup()]++
			record(replica.GetResourceGroup(), "")
			if collection != nil && len(expected) == 0 {
				record(replica.GetResourceGroup(), fmt.Sprintf("collection %d: effective resource group load target is unknown", id))
			}
		}
		for rg := range expected {
			record(rg, "")
		}
		if len(replicas) != expectedTotal {
			// The total is a collection obligation. RG errors below are derived
			// from each group's own delta, never spread across healthy groups.
			record("", fmt.Sprintf("collection %d: replica count mismatch (expected %d, actual %d)", id, expectedTotal, len(replicas)))
		}
		groups := typeutil.NewSet[string]()
		for rg := range actual {
			groups.Insert(rg)
		}
		for rg := range expected {
			groups.Insert(rg)
		}
		// If legacy intent is unknown we cannot invent the expected distribution.
		if collection == nil || len(expected) != 0 {
			for rg := range groups {
				if actual[rg] != expected[rg] {
					record(rg, fmt.Sprintf("collection %d: resource group %s replica count mismatch (expected %d, actual %d)", id, rg, expected[rg], actual[rg]))
				}
			}
		}
		validNodes := typeutil.NewUniqueSet()
		if collection != nil {
			for _, replica := range replicas {
				rg := replica.GetResourceGroup()
				if err := s.checkReplicaServiceable(ctx, replica); err != nil {
					record(rg, fmt.Sprintf("collection %d: %s", id, err.Error()))
				}
				if !replica.IsQueryVisible() {
					record(rg, fmt.Sprintf("collection %d: replica %d (rg=%s) is not query visible", id, replica.GetID(), rg))
				}
				// RO and RO SQ nodes are draining, even while still referenced by
				// a surviving replica. Their remaining resources must be released.
				validNodes.Insert(replica.GetRWNodes()...)
				validNodes.Insert(replica.GetRWSQNodes()...)
			}
		}
		leaked := make(map[string]int)
		for nodeID, count := range resources[id] {
			if !validNodes.Contain(nodeID) {
				rg := s.meta.GetResourceGroupByNodeID(nodeID)
				if rg == "" {
					// Embedded streaming QueryNodes bypass ResourceManager;
					// their session labels carry the actual source RG.
					if node := s.nodeMgr.Get(nodeID); node != nil && node.IsEmbeddedQueryNodeInStreamingNode() {
						rg = node.ResourceGroupName()
						if rg == "" {
							rg = meta.DefaultResourceGroupName
						}
					}
				}
				leaked[rg] += count
			}
		}
		for rg, count := range leaked {
			record(rg, fmt.Sprintf("collection %d: resources not fully released (leaked=%d)", id, count))
		}
	}
	return result, nil
}

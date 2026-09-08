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
	"sort"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/featureusage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// featureUsageFanoutTimeout bounds one QueryNode's GetFeatureUsage call.
const featureUsageFanoutTimeout = 10 * time.Second

// FeatureUsageEntries computes the loaded group of the feature usage report
// from QueryCoord's in-memory load metadata: how many collections are loaded,
// how many were loaded with a subset of their fields, how many have replicas
// outside the default resource group, and the distribution of effective
// replica numbers. It reads only local memory and holds no state.
func (s *Server) FeatureUsageEntries(ctx context.Context, cc featureusage.CollectionContext) []*internalpb.FeatureEntry {
	if s.meta == nil || s.meta.CollectionManager == nil {
		return nil
	}
	collections := s.meta.GetAllCollections(ctx)
	cols := make([]featureusage.LoadedCollection, 0, len(collections))
	for _, collection := range collections {
		if collection == nil || collection.CollectionLoadInfo == nil {
			continue
		}
		// Loading and failed-to-recover collections are in the manager too;
		// the group counts what is loaded.
		if collection.GetStatus() != querypb.LoadStatus_Loaded {
			continue
		}
		fieldCount, known := cc.FieldCount[collection.GetCollectionID()]
		if cc.Known() && !known {
			// Dropped in RootCoord, not yet released here.
			continue
		}
		if !cc.Known() {
			// No RootCoord snapshot in hand: fall back to the schema copy the
			// manager may hold. Recovery leaves it nil, in which case the
			// collection reads as a full load.
			fieldCount = loadableFieldCount(s.meta.GetCollectionSchema(ctx, collection.GetCollectionID()))
		}
		col := featureusage.LoadedCollection{
			ReplicaNumber:    collection.GetReplicaNumber(),
			LoadFieldsSubset: isLoadFieldsSubset(collection.GetLoadFields(), fieldCount),
		}
		if s.meta.ReplicaManager != nil {
			for _, replica := range s.meta.GetByCollection(ctx, collection.GetCollectionID()) {
				col.ResourceGroups = append(col.ResourceGroups, replica.GetResourceGroup())
			}
		}
		cols = append(cols, col)
	}
	return featureusage.ComputeLoadedEntries(cols)
}

// isLoadFieldsSubset reports whether the persisted load-field list names
// fewer fields than the collection has. QueryCoord stores an explicit list
// for every loaded collection, the full list of the moment when the request
// named none, so a collection loaded in full and then given a new field reads
// as a partial load until it is loaded again; the new field is indeed not
// loaded (see the design doc's known limitations). An empty list, which only
// a collection recovered from pre-list metadata carries, is a full load.
func isLoadFieldsSubset(loadFields []int64, fieldCount int) bool {
	return len(loadFields) > 0 && fieldCount > 0 && len(loadFields) < fieldCount
}

// loadableFieldCount counts, like CollectionContext does from the RootCoord
// model, the fields a load request can name.
func loadableFieldCount(schema *schemapb.CollectionSchema) int {
	if schema == nil {
		return 0
	}
	total := 0
	for _, field := range schema.GetFields() {
		if !common.IsSystemField(field.GetFieldID()) {
			total++
		}
	}
	for _, structField := range schema.GetStructArrayFields() {
		total += len(structField.GetFields())
	}
	return total
}

// CollectQueryNodeFeatureUsage fans GetFeatureUsage out to every QueryNode in
// the node manager, concurrently, each under its own timeout. The result has
// one node per QueryNode in node id order; an unreachable node is reported
// with reachable=false and its error, never omitted.
func (s *Server) CollectQueryNodeFeatureUsage(ctx context.Context, req *internalpb.GetFeatureUsageRequest) []*internalpb.FeatureUsageNode {
	if s.nodeMgr == nil || s.cluster == nil {
		return nil
	}
	var (
		mu    sync.Mutex
		wg    sync.WaitGroup
		nodes []*internalpb.FeatureUsageNode
	)
	for _, info := range s.nodeMgr.GetAll() {
		nodeID := info.ID()
		wg.Add(1)
		go func() {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, featureUsageFanoutTimeout)
			defer cancel()

			node := &internalpb.FeatureUsageNode{Role: typeutil.QueryNodeRole, NodeId: nodeID}
			resp, err := s.cluster.GetFeatureUsage(cctx, nodeID, req)
			if err == nil {
				err = merr.Error(resp.GetStatus())
			}
			if err != nil {
				node.Error = err.Error()
			} else {
				node.Reachable = true
				node.NodeStartTime = resp.GetNodeStartTime()
				node.Entries = resp.GetEntries()
			}
			mu.Lock()
			nodes = append(nodes, node)
			mu.Unlock()
		}()
	}
	wg.Wait()
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].NodeId < nodes[j].NodeId })
	return nodes
}

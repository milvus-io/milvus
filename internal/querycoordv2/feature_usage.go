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
func (s *Server) FeatureUsageEntries(ctx context.Context) []*internalpb.FeatureEntry {
	if s.meta == nil || s.meta.CollectionManager == nil {
		return nil
	}
	collections := s.meta.GetAllCollections(ctx)
	cols := make([]featureusage.LoadedCollection, 0, len(collections))
	for _, collection := range collections {
		if collection == nil || collection.CollectionLoadInfo == nil {
			continue
		}
		col := featureusage.LoadedCollection{
			ReplicaNumber:    collection.GetReplicaNumber(),
			LoadFieldsSubset: isLoadFieldsSubset(collection.GetLoadFields(), collection.Schema),
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

// isLoadFieldsSubset reports whether the load request named fewer fields than
// the schema has. Load metadata written before load_fields existed is upgraded
// to the full field list on recovery, so an empty list means "everything".
func isLoadFieldsSubset(loadFields []int64, schema *schemapb.CollectionSchema) bool {
	if len(loadFields) == 0 || schema == nil {
		return false
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
	return len(loadFields) < total
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

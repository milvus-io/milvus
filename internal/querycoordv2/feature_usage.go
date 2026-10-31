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

	"github.com/milvus-io/milvus/internal/featureusage"
	"github.com/milvus-io/milvus/internal/views/coord/loadstatus"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// featureUsageFanoutTimeout bounds one QueryNode's GetFeatureUsage call.
const featureUsageFanoutTimeout = 10 * time.Second

// FeatureUsageEntries counts only collections whose complete expected shard set
// is Up for the current QueryView load configuration.
func (s *Server) FeatureUsageEntries(ctx context.Context, cc featureusage.CollectionContext) []*internalpb.FeatureEntry {
	if s.qviewsRuntime == nil {
		return nil
	}
	runtime := s.qviewsRuntime
	cols := make([]featureusage.LoadedCollection, 0)
	for id := range runtime.loadConfigStore.Snapshot().ConfigsMap() {
		if _, known := cc.FieldCount[id]; cc.Known() && !known {
			continue
		}
		channels, err := runtime.resources.VChannels(ctx, id)
		if err != nil {
			continue
		}
		progress := loadstatus.Get(runtime.loadConfigStore, runtime.shardViewRegistry, id, channels)
		if !progress.Ready() {
			continue
		}
		cfg := progress.Config
		fields := make(map[int64]struct{}, len(cfg.LoadFields))
		for _, field := range cfg.LoadFields {
			fields[field.GetFieldId()] = struct{}{}
		}
		subset := false
		if len(fields) > 0 {
			for _, field := range cfg.RequiredFields {
				if _, ok := fields[field.GetFieldId()]; !ok {
					subset = true
					break
				}
			}
		}
		col := featureusage.LoadedCollection{ReplicaNumber: int32(len(cfg.Replicas)), LoadFieldsSubset: subset}
		for _, replica := range cfg.Replicas {
			col.ResourceGroups = append(col.ResourceGroups, replica.ResourceGroup)
		}
		cols = append(cols, col)
	}
	return featureusage.ComputeLoadedEntries(cols)
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
				node.CountersEnabled = resp.GetCountersEnabled()
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

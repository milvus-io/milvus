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

package coordinator

import (
	"context"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/featureusage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// featureUsageFanoutTimeout bounds one node's GetFeatureUsage call. It is the
// same value quotaCenter uses for its GetMetrics fan-out.
const featureUsageFanoutTimeout = 10 * time.Second

// GetFeatureUsage builds the merged feature usage report: the static groups
// computed from this process's rootcoord and datacoord metadata, plus the
// request counters of every reachable Proxy. Nothing is cached between calls
// and no timer exists; if nobody calls, nothing is computed.
//
// An unreachable Proxy is reported with reachable=false and its error, never
// omitted: an omitted node would read as "this feature is unused".
func (s *mixCoordImpl) GetFeatureUsage(ctx context.Context, req *internalpb.GetFeatureUsageRequest) (*internalpb.FeatureUsageReport, error) {
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &internalpb.FeatureUsageReport{Status: merr.Status(err)}, nil
	}

	static, err := s.rootcoordServer.GetFeatureUsage(ctx, req)
	if err == nil {
		err = merr.Error(static.GetStatus())
	}
	if err != nil {
		// A half-computed static section is never returned; the consumer could
		// not tell it from a complete one.
		mlog.Warn(ctx, "feature usage: static statistics failed", mlog.Err(err))
		return &internalpb.FeatureUsageReport{Status: merr.Status(err)}, nil
	}
	entries := append(static.GetEntries(), s.datacoordServer.FeatureUsageEntries()...)
	if s.queryCoordServer != nil {
		entries = append(entries, s.queryCoordServer.FeatureUsageEntries(ctx)...)
	}
	// DataCoord-side counters (import file types, compaction types) live in this process.
	entries = append(entries, featureusage.SnapshotFor(featureusage.RoleMixCoord)...)

	report := &internalpb.FeatureUsageReport{
		Status:       merr.Success(),
		CollectedAt:  time.Now().Unix(),
		BuildVersion: os.Getenv(metricsinfo.GitBuildTagsEnvKey),
		DeployMode:   os.Getenv(metricsinfo.DeployModeEnvKey),
		Nodes: []*internalpb.FeatureUsageNode{{
			Role:          typeutil.MixCoordRole,
			NodeId:        paramtable.GetNodeID(),
			NodeStartTime: paramtable.GetCreateTime().Unix(),
			Reachable:     true,
			Entries:       entries,
		}},
	}
	report.Nodes = append(report.Nodes, s.collectProxyFeatureUsage(ctx, req)...)
	if s.queryCoordServer != nil {
		report.Nodes = append(report.Nodes, s.queryCoordServer.CollectQueryNodeFeatureUsage(ctx, req)...)
	}
	return report, nil
}

// collectProxyFeatureUsage fans GetFeatureUsage out to every Proxy known to the
// proxy client manager, concurrently, each under its own timeout. The result
// has one node per Proxy in node id order, reachable or not.
func (s *mixCoordImpl) collectProxyFeatureUsage(ctx context.Context, req *internalpb.GetFeatureUsageRequest) []*internalpb.FeatureUsageNode {
	mgr := s.proxyClientManager
	if mgr == nil && s.rootcoordServer != nil {
		// The live manager, fed by the proxy session watcher, belongs to rootcoord;
		// mixCoordImpl's own field is only set in tests.
		mgr = s.rootcoordServer.GetProxyClientManager()
	}
	if mgr == nil {
		return nil
	}
	clients := mgr.GetProxyClients()
	if clients == nil {
		return nil
	}

	var (
		mu    sync.Mutex
		wg    sync.WaitGroup
		nodes []*internalpb.FeatureUsageNode
	)
	clients.Range(func(nodeID int64, client types.ProxyClient) bool {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, featureUsageFanoutTimeout)
			defer cancel()

			node := &internalpb.FeatureUsageNode{Role: typeutil.ProxyRole, NodeId: nodeID}
			resp, err := client.GetFeatureUsage(cctx, req)
			if err == nil {
				err = merr.Error(resp.GetStatus())
			}
			if err != nil {
				node.Error = err.Error()
			} else {
				node.Reachable = true
				node.NodeStartTime = resp.GetNodeStartTime()
				node.Entries = resp.GetEntries()
				if resp.GetNodeId() != 0 {
					node.NodeId = resp.GetNodeId()
				}
			}
			mu.Lock()
			nodes = append(nodes, node)
			mu.Unlock()
		}()
		return true
	})
	wg.Wait()
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].NodeId < nodes[j].NodeId })
	return nodes
}

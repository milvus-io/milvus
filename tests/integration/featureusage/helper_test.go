// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package featureusage

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/featureusage"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

// Suite is the whole package: one cluster, every test method, and a coverage
// check at the end. It is a single suite on purpose. The check asserts that
// every counter the report can emit was actually exercised, which is only
// meaningful if the counters accumulate in one set of processes.
type Suite struct {
	integration.MiniClusterSuite

	// collection is the shared workload collection, built on first use.
	collection string
	dim        int
}

func (s *Suite) SetupSuite() {
	s.WithMilvusConfig(paramtable.Get().CommonCfg.FeatureUsageEnabled.Key, "true")
	s.WithMilvusConfig(paramtable.Get().CommonCfg.FeatureUsageCountersEnabled.Key, "true")
	// Two QueryNode paths are off by default and would report zero forever.
	// Turning them on here is what lets the coverage check demand them.
	s.WithMilvusConfig(paramtable.Get().QueryNodeCfg.EnableSegmentPrune.Key, "true")
	s.WithMilvusConfig(paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.Key, "true")
	s.WithMilvusConfig(paramtable.Get().AutoIndexConfig.TwoStageSearchMinTopk.Key, "1")
	s.WithMilvusConfig(paramtable.Get().AutoIndexConfig.TwoStageSearchMinNumSegments.Key, "1")
	// A small segment size makes the compaction paths reachable on a test-sized
	// dataset: clustering compaction emits more than one segment, which is what
	// segment pruning needs, and the level-zero and sort triggers fire quickly.
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.SegmentMaxSize.Key, "4")
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.ClusteringCompactionPreferSegmentSizeRatio.Key, "0.5")
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.ClusteringCompactionEnable.Key, "true")
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.GlobalCompactionInterval.Key, "5")
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.LevelZeroCompactionTriggerDeltalogMinNum.Key, "1")
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.LevelZeroCompactionTriggerMinSize.Key, "1")
	s.MiniClusterSuite.SetupSuite()
}

// report reads the merged report from MixCoord, the same call the Proxy's
// management endpoint makes.
func (s *Suite) report(ctx context.Context) *internalpb.FeatureUsageReport {
	resp, err := s.Cluster.MixCoordClient.GetFeatureUsage(ctx, &internalpb.GetFeatureUsageRequest{})
	s.Require().NoError(err)
	s.Require().NoError(merr.Error(resp.GetStatus()))
	s.Require().NotEmpty(resp.GetNodes(), "a report always has at least the mixcoord node")
	for _, n := range resp.GetNodes() {
		s.Require().True(n.GetReachable(), "node %s/%d unreachable: %s", n.GetRole(), n.GetNodeId(), n.GetError())
	}
	return resp
}

// counters flattens the request-group counters of every node with the given
// role into name -> value. Several nodes of one role are summed, which is what
// a consumer does after its own per-node reasoning.
func counters(report *internalpb.FeatureUsageReport, role string) map[string]int64 {
	out := map[string]int64{}
	for _, n := range report.GetNodes() {
		if n.GetRole() != role {
			continue
		}
		for _, e := range n.GetEntries() {
			if e.GetGroup() == featureusage.GroupRequest {
				out[e.GetName()] += e.GetValue()
			}
		}
	}
	return out
}

// entries flattens one group of one role into name(+bucket) -> value.
func entries(report *internalpb.FeatureUsageReport, role, group string) map[string]int64 {
	out := map[string]int64{}
	for _, n := range report.GetNodes() {
		if n.GetRole() != role {
			continue
		}
		for _, e := range n.GetEntries() {
			if e.GetGroup() != group {
				continue
			}
			key := e.GetName()
			if e.GetBucket() != "" {
				key += "|" + e.GetBucket()
			}
			out[key] += e.GetValue()
		}
	}
	return out
}

// perNodeEntries returns one map per node of the given role. The report keeps
// nodes separate on purpose, and a StreamingNode reports under the QueryNode
// role too, so anything asserted per node has to be read this way rather than
// through the summing helpers above.
func perNodeEntries(report *internalpb.FeatureUsageReport, role, group string) []map[string]int64 {
	var out []map[string]int64
	for _, n := range report.GetNodes() {
		if n.GetRole() != role {
			continue
		}
		m := map[string]int64{}
		for _, e := range n.GetEntries() {
			if e.GetGroup() == group {
				m[e.GetName()] += e.GetValue()
			}
		}
		out = append(out, m)
	}
	return out
}

// requireOnlyDelta asserts that between before and after exactly the named
// counters moved, by exactly the named amounts. It is the assertion that makes
// this suite a regression test rather than a smoke test: a new hook that fires
// on the wrong request, or an old one that stops firing, fails here.
func requireOnlyDelta(t *testing.T, before, after map[string]int64, want map[string]int64) {
	t.Helper()
	moved := map[string]int64{}
	for name, v := range after {
		if d := v - before[name]; d != 0 {
			moved[name] = d
		}
	}
	for name, v := range before {
		if _, ok := after[name]; !ok && v != 0 {
			moved[name] = -v
		}
	}
	require.Equal(t, want, moved, "counters that moved")
}

// reportJSON is the serialized form the HTTP endpoint returns; the leak checks
// search it as one string.
func reportJSON(t *testing.T, report *internalpb.FeatureUsageReport) string {
	t.Helper()
	b, err := json.Marshal(report)
	require.NoError(t, err)
	return string(b)
}

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

package searchprofile

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

var ansi = regexp.MustCompile("\x1b\\[[0-9;]*m")

func renderSample() string {
	return Render(&Summary{Type: "Search", Database: "default", Collection: "c", Segments: 2}, 3*time.Millisecond,
		[]*internalpb.StageCost{
			{Stage: ProxyQueue, SumUs: 300, MaxUs: 300, Count: 1, MaxNodeId: 1},
			{Stage: ProxyExecute, SumUs: 2000, MaxUs: 2000, Count: 1, MaxNodeId: 1},
			{Stage: ProxyShardRPC, SumUs: 1500, MaxUs: 1500, Count: 1, MaxNodeId: 2, MaxId: 2},
			{Stage: QNSegmentSearch, SumUs: 1200, MaxUs: 700, Count: 2, MaxNodeId: 2, MaxId: 11},
			{Stage: OperatorPrefix + "PhyVectorSearchNode", SumUs: 1000, MaxUs: 600, Count: 1, MaxNodeId: 2, MaxId: 11},
			{Stage: ProxyPostExecute, SumUs: 100, MaxUs: 100, Count: 1, MaxNodeId: 1},
			{Stage: ProxyOpPrefix + "reduce", SumUs: 50, MaxUs: 50, Count: 1, MaxNodeId: 1},
		})
}

func TestRenderTree(t *testing.T) {
	// qn.fanout did not run, so qn.segment_search hangs under proxy.shard_rpc
	require.Equal(t, strings.Join([]string{
		"Summary",
		"    Type: Search",
		"    Collection: default.c",
		"    State: Finished",
		"    TotalTime: 3.000ms",
		"    Segments: 2",
		"    Top Most Time-consuming Stages:",
		"        1. op.PhyVectorSearchNode : 1.000ms (33.33%)",
		"        2. proxy.execute : 0.500ms (16.67%)",
		"        3. proxy.queue : 0.300ms (10.00%)",
		"        4. proxy.shard_rpc : 0.300ms (10.00%)",
		"        5. qn.segment_search : 0.200ms (6.67%)",
		"Stages",
		"├──proxy.queue [Planner]",
		"│      TotalTime: 0.300ms (10.00%)",
		"│      Node: 1",
		"├──proxy.execute [Execution]",
		"│  │   TotalTime: 2.000ms (66.67%) [SelfTime: 0.500ms (16.67%)]",
		"│  │   Node: 1",
		"│  └──proxy.shard_rpc [Fragment]",
		"│     │   TotalTime: 1.500ms (50.00%) [SelfTime: 0.300ms (10.00%)]",
		"│     │   Node: 2, Id: 2",
		"│     └──qn.segment_search [Pipeline]",
		"│        │   TotalTime: 1.200ms (40.00%) [SelfTime: 0.200ms (6.67%)]",
		"│        │   Count: 2, MaxTime: 0.700ms",
		"│        │   Node: 2, Id: 11",
		"│        └──op.PhyVectorSearchNode [Operator]",
		"│               TotalTime: 1.000ms (33.33%)",
		"│               Node: 2, Id: 11",
		"└──proxy.post_execute [Execution]",
		"   │   TotalTime: 0.100ms (3.33%) [SelfTime: 0.050ms (1.67%)]",
		"   │   Node: 1",
		"   └──proxy.op.reduce [Execution]",
		"          TotalTime: 0.050ms (1.67%)",
		"          Node: 1",
		"",
	}, "\n"), ansi.ReplaceAllString(renderSample(), ""))
}

func TestRenderColorsByOwnShare(t *testing.T) {
	out := renderSample()
	// over 30% of the total on its own: red
	require.Contains(t, out, colorHot+"│        └──op.PhyVectorSearchNode [Operator]"+colorReset)
	// 15-30%: salmon, though its children took most of its time
	require.Contains(t, out, colorWarm+"├──proxy.execute [Execution]"+colorReset)
	// under 15%: plain, however much its children took
	require.Contains(t, out, colorReset+"│  └──proxy.shard_rpc [Fragment]"+colorReset)
}

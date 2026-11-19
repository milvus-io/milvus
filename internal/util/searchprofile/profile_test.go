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
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

func TestObserveKeepsSumCountAndSlowest(t *testing.T) {
	r := NewRecorder(7)
	r.Observe(QNSegmentSearch, 3*time.Millisecond, 100)
	r.Observe(QNSegmentSearch, 9*time.Millisecond, 101)
	r.Observe(QNSegmentSearch, 5*time.Millisecond, 102)

	stages := r.Stages()
	require.Len(t, stages, 1)
	s := stages[0]
	require.Equal(t, QNSegmentSearch, s.GetStage())
	require.EqualValues(t, 17000, s.GetSumUs())
	require.EqualValues(t, 9000, s.GetMaxUs())
	require.EqualValues(t, 3, s.GetCount())
	require.EqualValues(t, 7, s.GetMaxNodeId())
	require.EqualValues(t, 101, s.GetMaxId())
}

func TestMergeAcrossNodes(t *testing.T) {
	worker1 := NewRecorder(1)
	worker1.Observe(QNSegmentSearch, 4*time.Millisecond, 10)
	worker1.Observe(QNQueue, time.Millisecond, 0)
	worker2 := NewRecorder(2)
	worker2.Observe(QNSegmentSearch, 8*time.Millisecond, 20)
	worker2.Observe(QNQueue, 2*time.Millisecond, 0)

	delegator := NewRecorder(1)
	delegator.Observe(QNWaitTSafe, 6*time.Millisecond, 0)
	delegator.Merge(worker1.Stages())
	delegator.Merge(worker2.Stages())

	byName := map[string]*internalpb.StageCost{}
	for _, s := range delegator.Stages() {
		byName[s.GetStage()] = s
	}
	seg := byName[QNSegmentSearch]
	require.EqualValues(t, 12000, seg.GetSumUs())
	require.EqualValues(t, 2, seg.GetCount())
	require.EqualValues(t, 8000, seg.GetMaxUs())
	require.EqualValues(t, 2, seg.GetMaxNodeId())
	require.EqualValues(t, 20, seg.GetMaxId())
	require.EqualValues(t, 3000, byName[QNQueue].GetSumUs())
	require.EqualValues(t, 6000, byName[QNWaitTSafe].GetSumUs())
}

func TestMergeDoesNotAliasInput(t *testing.T) {
	in := []*internalpb.StageCost{{Stage: QNQueue, SumUs: 5, MaxUs: 5, Count: 1}}
	r := NewRecorder(1)
	r.Merge(in)
	r.Merge(in)
	require.EqualValues(t, 5, in[0].GetSumUs())
	require.EqualValues(t, 10, r.Stages()[0].GetSumUs())
}

func TestStagesInExecutionOrder(t *testing.T) {
	r := NewRecorder(1)
	r.Observe(ProxyPostExecute, time.Millisecond, 0)
	r.Observe(ProxyOpPrefix+"requery", time.Millisecond, 0)
	r.Observe(QNShardReduce, time.Millisecond, 0)
	r.Observe("unknown", time.Millisecond, 0)
	r.Observe(ProxyOpPrefix+"reduce", time.Millisecond, 0)
	r.Observe(ProxyQueue, time.Millisecond, 0)
	r.Observe(QNWorkerReduce, time.Millisecond, 0)
	r.Observe(OperatorPrefix+"PhyVectorSearchNode", time.Millisecond, 0)
	r.Observe(QNSegmentSearch, time.Millisecond, 0)

	var names []string
	for _, s := range r.Stages() {
		names = append(names, s.GetStage())
	}
	require.Equal(t, []string{
		ProxyQueue, QNSegmentSearch, OperatorPrefix + "PhyVectorSearchNode", QNWorkerReduce,
		QNShardReduce, ProxyPostExecute,
		ProxyOpPrefix + "requery", ProxyOpPrefix + "reduce", "unknown",
	}, names)
}

func TestLayer(t *testing.T) {
	for stage, layer := range map[string]string{
		ProxyQueue:                             LayerPlanner,
		ProxyPreExecute:                        LayerPlanner,
		ProxyExecute:                           LayerExecution,
		ProxyShardReduce:                       LayerExecution,
		ProxyOpPrefix + "reduce":               LayerExecution,
		ProxyShardRPC:                          LayerFragment,
		QNShardReduce:                          LayerFragment,
		QNQueue:                                LayerPipeline,
		QNSegmentSearch:                        LayerPipeline,
		OperatorPrefix + "PhyFilterBitsNode":   LayerOperator,
		OperatorPrefix + "PhyVectorSearchNode": LayerOperator,
		"unknown":                              "",
	} {
		require.Equal(t, layer, Layer(stage), stage)
	}
}

func TestNilRecorderIgnoresCalls(t *testing.T) {
	var r *Recorder
	r.Observe(QNQueue, time.Millisecond, 0)
	r.ObserveSince(QNQueue, time.Now(), 0)
	r.Merge([]*internalpb.StageCost{{Stage: QNQueue}})
	require.Nil(t, r.Stages())
	require.Nil(t, FromContext(context.Background()))
}

func TestChildKeepsNodeAndStartsEmpty(t *testing.T) {
	var nilRecorder *Recorder
	require.Nil(t, nilRecorder.Child())

	parent := NewRecorder(7)
	parent.Observe(ProxyExecute, time.Millisecond, 0)
	child := parent.Child()
	require.Nil(t, child.Stages())
	child.Observe(ProxyShardRPC, time.Millisecond, 11)
	require.EqualValues(t, 7, child.Stages()[0].GetMaxNodeId())
	require.Len(t, parent.Stages(), 1)
}

func TestContextCarriesRecorder(t *testing.T) {
	r := NewRecorder(1)
	ctx := WithRecorder(context.Background(), r)
	require.Same(t, r, FromContext(ctx))
	// a callee that keeps its own record shadows the caller's
	require.Nil(t, FromContext(WithRecorder(ctx, nil)))
}

func TestConcurrentObserve(t *testing.T) {
	r := NewRecorder(1)
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			r.Observe(QNSegmentSearch, time.Millisecond, id)
		}(int64(i))
	}
	wg.Wait()
	require.EqualValues(t, 64, r.Stages()[0].GetCount())
}

func TestFormat(t *testing.T) {
	stages := []*internalpb.StageCost{
		{Stage: ProxyQueue, SumUs: 300, MaxUs: 300, Count: 1, MaxNodeId: 9},
		{Stage: QNSegmentSearch, SumUs: 1204600, MaxUs: 198300, Count: 36, MaxNodeId: 3, MaxId: 4567},
		{Stage: QNQueue, SumUs: 3000, MaxUs: 2000, Count: 2, MaxNodeId: 2},
	}
	require.Equal(t,
		"proxy.queue=0.300ms; "+
			"qn.segment_search: n=36 sum=1204.600ms max=198.300ms (node=3 id=4567); "+
			"qn.queue: n=2 sum=3.000ms max=2.000ms (node=2)",
		Format(stages))
	require.Equal(t, "", Format(nil))
}

func TestToSlowQueryStagesJSON(t *testing.T) {
	out := ToSlowQueryStages([]*internalpb.StageCost{
		{Stage: QNSegmentSearch, SumUs: 1500, MaxUs: 1000, Count: 2, MaxNodeId: 3, MaxId: 4567},
	})
	b, err := json.Marshal(out)
	require.NoError(t, err)
	require.JSONEq(t,
		`[{"stage":"qn.segment_search","layer":"Pipeline","count":2,"sum":"1.500ms","max":"1.000ms","max_node_id":"3","max_id":"4567"}]`,
		string(b))
	require.Nil(t, ToSlowQueryStages(nil))
}

func TestMarshalReport(t *testing.T) {
	report, err := MarshalReport(&Summary{Type: "Search", Database: "default", Collection: "c"},
		12500*time.Microsecond, []*internalpb.StageCost{
			{Stage: ProxyQueue, SumUs: 300, MaxUs: 300, Count: 1, MaxNodeId: 9},
			{Stage: QNSegmentSearch, SumUs: 900, MaxUs: 500, Count: 2, MaxNodeId: 3, MaxId: 11},
		})
	require.NoError(t, err)
	require.JSONEq(t,
		`{"summary":{"total":"12.500ms","type":"Search","database":"default","collection":"c","segments":2},
		  "stages":[{"stage":"proxy.queue","layer":"Planner","count":1,"sum":"0.300ms","max":"0.300ms","max_node_id":"9"},
		            {"stage":"qn.segment_search","layer":"Pipeline","count":2,"sum":"0.900ms","max":"0.500ms","max_node_id":"3","max_id":"11"}]}`,
		report)

	// A request that recorded nothing still reports an empty list.
	report, err = MarshalReport(&Summary{}, time.Millisecond, nil)
	require.NoError(t, err)
	require.JSONEq(t,
		`{"summary":{"total":"1.000ms","type":"","database":"","collection":"","segments":0},"stages":[]}`,
		report)
}

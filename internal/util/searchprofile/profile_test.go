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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
)

func TestObserveKeepsSumCountAndSlowest(t *testing.T) {
	r := NewRecorder(7)
	r.Observe(QNSegmentSearch, 3*time.Millisecond, 100)
	r.Observe(QNSegmentSearch, 9*time.Millisecond, 101)
	r.Observe(QNSegmentSearch, 5*time.Millisecond, 102)

	require.Equal(t, []Stage{{
		Name: QNSegmentSearch, SumUs: 17000, MaxUs: 9000, Count: 3, MaxNodeID: 7, MaxID: 101,
	}}, r.Stages())
}

func TestMergeAcrossNodes(t *testing.T) {
	worker1 := NewRecorder(1)
	worker1.Observe(QNSegmentSearch, 4*time.Millisecond, 10)
	worker1.Observe(QNExecute, time.Millisecond, 0)
	worker2 := NewRecorder(2)
	worker2.Observe(QNSegmentSearch, 8*time.Millisecond, 20)
	worker2.Observe(QNExecute, 2*time.Millisecond, 0)

	proxy := NewRecorder(9)
	proxy.Observe(ProxyQueryPlan, 6*time.Millisecond, 0)
	proxy.Merge(worker1.Stages())
	proxy.Merge(worker2.Stages())

	byName := map[string]Stage{}
	for _, s := range proxy.Stages() {
		byName[s.Name] = s
	}
	require.Equal(t, Stage{
		Name: QNSegmentSearch, SumUs: 12000, MaxUs: 8000, Count: 2, MaxNodeID: 2, MaxID: 20,
	}, byName[QNSegmentSearch])
	require.EqualValues(t, 3000, byName[QNExecute].SumUs)
	require.EqualValues(t, 6000, byName[ProxyQueryPlan].SumUs)
}

func TestStagesReturnsCopy(t *testing.T) {
	r := NewRecorder(1)
	r.Observe(QNExecute, time.Millisecond, 0)
	out := r.Stages()
	out[0].SumUs = 0
	require.EqualValues(t, 1000, r.Stages()[0].SumUs)
}

func TestNilRecorderIgnoresCalls(t *testing.T) {
	var r *Recorder
	r.Observe(QNSegmentSearch, time.Millisecond, 0)
	r.ObserveSince(QNSegmentSearch, time.Now(), 0)
	r.Merge([]Stage{{Name: QNSegmentSearch}})
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
	require.EqualValues(t, 7, child.Stages()[0].MaxNodeID)
	require.Len(t, parent.Stages(), 1)
}

func TestContextCarriesRecorder(t *testing.T) {
	r := NewRecorder(1)
	ctx := WithRecorder(context.Background(), r)
	require.Same(t, r, FromContext(ctx))
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
	require.EqualValues(t, 64, r.Stages()[0].Count)
}

// Stages travel in the Status and are removed once taken, so they go no
// further than the node that takes them.
func TestAttachAndTake(t *testing.T) {
	stages := []Stage{{Name: QNSegmentSearch, SumUs: 5, MaxUs: 5, Count: 1, MaxNodeID: 3, MaxID: 11}}
	status := &commonpb.Status{ExtraInfo: map[string]string{"other": "kept"}}

	Attach(status, stages)
	require.Contains(t, status.GetExtraInfo(), ExtraInfoKey)

	require.Equal(t, stages, Take(status))
	require.Equal(t, map[string]string{"other": "kept"}, status.GetExtraInfo())
	require.Nil(t, Take(status))
}

func TestAttachAndTakeNothing(t *testing.T) {
	Attach(nil, []Stage{{Name: QNSegmentSearch}})
	status := &commonpb.Status{}
	Attach(status, nil)
	require.Nil(t, status.GetExtraInfo())
	require.Nil(t, Take(nil))
	require.Nil(t, Take(&commonpb.Status{ExtraInfo: map[string]string{ExtraInfoKey: "not json"}}))
}

func TestFormat(t *testing.T) {
	stages := []Stage{
		{Name: ProxyQueue, SumUs: 300, MaxUs: 300, Count: 1, MaxNodeID: 9},
		{Name: QNSegmentSearch, SumUs: 1204600, MaxUs: 198300, Count: 36, MaxNodeID: 3, MaxID: 4567},
		{Name: QNExecute, SumUs: 3000, MaxUs: 2000, Count: 2, MaxNodeID: 2},
	}
	require.Equal(t,
		"proxy.queue=0.300ms; "+
			"qn.segment_search: n=36 sum=1204.600ms max=198.300ms (node=3 id=4567); "+
			"qn.execute: n=2 sum=3.000ms max=2.000ms (node=2)",
		Format(stages))
	require.Equal(t, "", Format(nil))
}

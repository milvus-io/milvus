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

package tasks

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/searchprofile"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func stageByName(stages []*internalpb.StageCost) map[string]*internalpb.StageCost {
	m := make(map[string]*internalpb.StageCost, len(stages))
	for _, s := range stages {
		m[s.GetStage()] = s
	}
	return m
}

// Every request of a merged group reports the group's segment search and its
// own time in queue.
func TestAttachStageCostsToMergedGroup(t *testing.T) {
	const nodeID = 5
	old := paramtable.GetNodeID()
	paramtable.SetNodeID(nodeID)
	defer paramtable.SetNodeID(old)
	other := &SearchTask{
		// a StreamingNode's tasks carry server ID 0; the profile names the node
		queueDur: 7 * time.Millisecond,
		result:   &internalpb.SearchResults{},
	}
	owner := &SearchTask{
		queueDur:  2 * time.Millisecond,
		originNqs: []int64{1, 1},
		others:    []*SearchTask{other},
		result:    &internalpb.SearchResults{},
		profile:   searchprofile.NewRecorder(nodeID),
	}
	owner.profile.Observe(searchprofile.QNSegmentSearch, 3*time.Millisecond, 11)
	owner.profile.Observe(searchprofile.QNSegmentSearch, 9*time.Millisecond, 12)
	owner.profile.Observe(searchprofile.QNWorkerReduce, time.Millisecond, 0)

	owner.attachStageCosts()

	for _, tc := range []struct {
		task  *SearchTask
		queue int64
	}{{owner, 2000}, {other, 7000}} {
		stages := stageByName(tc.task.result.GetStageCosts())
		require.EqualValues(t, tc.queue, stages[searchprofile.QNQueue].GetSumUs())
		require.EqualValues(t, nodeID, stages[searchprofile.QNQueue].GetMaxNodeId())
		seg := stages[searchprofile.QNSegmentSearch]
		require.EqualValues(t, 2, seg.GetCount())
		require.EqualValues(t, 12000, seg.GetSumUs())
		require.EqualValues(t, 12, seg.GetMaxId())
		require.EqualValues(t, 1000, stages[searchprofile.QNWorkerReduce].GetSumUs())
	}
}

// A task run directly, without the scheduler, reports no time in queue.
func TestAttachStageCostsWithoutQueue(t *testing.T) {
	task := &SearchTask{
		originNqs: []int64{1},
		result:    &internalpb.SearchResults{},
		profile:   searchprofile.NewRecorder(1),
	}
	task.profile.Observe(searchprofile.QNSegmentSearch, time.Millisecond, 11)
	task.attachStageCosts()
	stages := stageByName(task.result.GetStageCosts())
	require.NotContains(t, stages, searchprofile.QNQueue)
	require.Contains(t, stages, searchprofile.QNSegmentSearch)
}

// A member that has no result, e.g. one canceled while the group ran, is
// skipped.
func TestAttachStageCostsSkipsMissingResult(t *testing.T) {
	other := &SearchTask{}
	owner := &SearchTask{
		originNqs: []int64{1, 1},
		others:    []*SearchTask{other},
		result:    &internalpb.SearchResults{},
		profile:   searchprofile.NewRecorder(1),
	}
	owner.profile.Observe(searchprofile.QNSegmentSearch, time.Millisecond, 11)
	owner.attachStageCosts()
	require.Nil(t, other.result)
	require.NotEmpty(t, owner.result.GetStageCosts())
}

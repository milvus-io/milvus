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

package scheduler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type phaseObserver struct{ phases []string }

func (o *phaseObserver) BeginAttempt() func()     { return func() {} }
func (o *phaseObserver) SetRequestPhase(s string) { o.phases = append(o.phases, s) }

type observedTask struct {
	*mockDqlTask
	internal, subtask bool
}

func (t *observedTask) IsSubTask() bool      { return t.subtask }
func (t *observedTask) IsInternalTask() bool { return t.internal }

func TestDQLStageFollowsOnlyAcceptedExternalTasks(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name                    string
		msgType                 commonpb.MsgType
		internal, subtask, full bool
		wantPhases              bool
	}{
		{name: "search", msgType: commonpb.MsgType_Search, wantPhases: true},
		{name: "retrieve", msgType: commonpb.MsgType_Retrieve, wantPhases: true},
		{name: "requery", msgType: commonpb.MsgType_Retrieve, subtask: true},
		{name: "internal read", msgType: commonpb.MsgType_Retrieve, internal: true},
		{name: "upsert", msgType: commonpb.MsgType_Upsert},
		{name: "full", msgType: commonpb.MsgType_Retrieve, full: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observer := &phaseObserver{}
			task := &observedTask{mockDqlTask: newMockDqlTask(extension.WithDQLObserver(context.Background(), observer)), internal: tc.internal, subtask: tc.subtask}
			task.tType = tc.msgType
			queue := newBaseTaskQueue(newMockTsoAllocator())
			if tc.full {
				queue.SetMaxTaskNum(0)
				require.Error(t, queue.Enqueue(task))
				require.Empty(t, observer.phases)
				return
			}
			require.NoError(t, queue.Enqueue(task))
			queue.PopUnissuedTask()
			(&TaskScheduler{}).processTask(task, queue)
			require.NoError(t, task.WaitToFinish())
			if tc.wantPhases {
				require.Equal(t, []string{"queue", "execute"}, observer.phases)
			} else {
				require.Empty(t, observer.phases)
			}
		})
	}
}

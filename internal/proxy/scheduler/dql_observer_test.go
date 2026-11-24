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

	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
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
		internal, subtask, full bool
	}{
		{"external", false, false, false}, {"requery", false, true, false}, {"search-by-pk", true, false, false}, {"full", false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observer := &phaseObserver{}
			task := &observedTask{mockDqlTask: newMockDqlTask(extension.WithDQLObserver(context.Background(), observer)), internal: tc.internal, subtask: tc.subtask}
			task.name = taskmodel.QueryTaskName
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
			if tc.internal || tc.subtask {
				require.Empty(t, observer.phases)
			} else {
				require.Equal(t, []string{"queue", "execute"}, observer.phases)
			}
		})
	}
}

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
package dql

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/scheduler"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type queryPhaseRecorder struct{ phases []string }

func (o *queryPhaseRecorder) BeginAttempt() func()         { return func() {} }
func (o *queryPhaseRecorder) SetRequestPhase(phase string) { o.phases = append(o.phases, phase) }

func TestQueryTaskReportsSchedulerPhases(t *testing.T) {
	paramtable.Init()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	observer := &queryPhaseRecorder{}
	ctx = extension.WithDQLObserver(ctx, observer)
	sched, err := scheduler.NewTaskScheduler(ctx, &mockTsoAllocator{})
	require.NoError(t, err)
	require.NoError(t, sched.Start())
	defer sched.Close()

	// An empty collection name stops the task after dispatch, before it needs a backend.
	task := NewQueryTask(ctx, &namespaceRequeryMockNode{}, &milvuspb.QueryRequest{}, nil, &internalpb.RetrieveRequest{}, nil, false)
	require.NoError(t, sched.DqQueue.Enqueue(task))
	require.ErrorContains(t, task.WaitToFinish(), "collection name should not be empty")
	require.Equal(t, []string{"queue", "execute"}, observer.phases)
}

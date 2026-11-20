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
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/searchutil/scheduler"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestSchedulerMergedSearchMemberCancellation(t *testing.T) {
	paramtable.Init()
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	memberCtx, cancelMember := context.WithCancel(ctx)
	defer cancelMember()

	blocker := newGroupMember(ctx, 1)
	pending := newGroupMember(ctx, 1)
	pending.req.Req.CollectionID++
	leader := newGroupMember(ctx, 1)
	member := newGroupMember(memberCtx, 1)

	blocked := make(chan struct{})
	releaseBlocker := make(chan struct{})
	started := make(chan []*SearchTask, 1)
	gpu := mockey.Mock((*SearchTask).IsGpuIndex).Return(false).Build()
	defer gpu.UnPatch()
	preExecute := mockey.Mock((*SearchTask).PreExecute).To(func(task *SearchTask) error {
		if task == blocker {
			close(blocked)
			select {
			case <-releaseBlocker:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}).Build()
	defer preExecute.UnPatch()
	execute := mockey.Mock((*SearchTask).Execute).To(func(task *SearchTask) error {
		if task != leader {
			return nil
		}
		restore := task.useGroupContext()
		defer restore()
		started <- task.members()
		<-memberCtx.Done()
		if err := task.Context().Err(); err != nil {
			return err
		}
		task.result = &internalpb.SearchResults{NumQueries: 1, TopK: 10}
		return nil
	}).Build()
	defer execute.UnPatch()

	queue := scheduler.NewScheduler("fifo")
	queue.Start()
	defer func() {
		stop()
		queue.Stop()
	}()
	require.NoError(t, queue.Add(blocker))
	select {
	case <-blocked:
	case <-ctx.Done():
		t.Fatal("scheduler worker did not reach the gate")
	}
	// While the worker is blocked, the scheduler takes pending as its
	// next dispatch. The two Search tasks then remain in its merge queue.
	require.NoError(t, queue.Add(pending))
	require.NoError(t, queue.Add(leader))
	require.NoError(t, queue.Add(member))
	close(releaseBlocker)

	select {
	case group := <-started:
		require.Len(t, group, 2)
		require.Same(t, leader, group[0])
		require.Same(t, member, group[1])
	case <-ctx.Done():
		t.Fatal("merged group did not start")
	}
	cancelMember()
	require.ErrorIs(t, waitResult(t, member), context.Canceled)
	require.NoError(t, waitResult(t, leader))
	require.EqualValues(t, 1, leader.SearchResult().GetNumQueries())
	require.EqualValues(t, 10, leader.SearchResult().GetTopK())
}

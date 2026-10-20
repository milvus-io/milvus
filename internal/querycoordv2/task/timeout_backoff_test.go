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

package task

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestSegmentTimeoutBackoff(t *testing.T) {
	paramtable.Init()
	replica := newTestReplica(1000, 1)
	newTask := func(seg int64) *SegmentTask {
		task, err := NewSegmentTask(context.Background(), 10*time.Second, testSource("test"), 1000, replica, 0,
			NewSegmentAction(1, ActionTypeGrow, "shard-0", seg))
		require.NoError(t, err)
		return task
	}
	const seg = 4242
	base := time.Minute
	require.Equal(t, base, SegmentTaskTimeout(seg, base))

	// A load that ends on its deadline (as the executor reports it, through
	// merr.CheckRPCCall) is retried under the retry timeout, as often as it
	// takes.
	for i := 0; i < 3; i++ {
		task := newTask(seg)
		task.Fail(merr.CheckRPCCall(nil, context.DeadlineExceeded))
		noteSegmentTaskDone(task)
		require.Equal(t, 5*time.Minute, SegmentTaskTimeout(seg, base))
	}
	require.Equal(t, 20*time.Minute, SegmentTaskTimeout(seg, 20*time.Minute), "never below the base")

	// The scheduler may cancel a task on its deadline before the executor
	// fails it; a cancel on the deadline, or a gRPC DeadlineExceeded status
	// from the RPC, counts the same.
	task := newTask(seg)
	task.SetStatus(TaskStatusSucceeded)
	noteSegmentTaskDone(task)
	task = newTask(seg)
	task.Cancel(context.DeadlineExceeded)
	noteSegmentTaskDone(task)
	require.Equal(t, 5*time.Minute, SegmentTaskTimeout(seg, base))
	task = newTask(seg)
	task.SetStatus(TaskStatusSucceeded)
	noteSegmentTaskDone(task)
	task = newTask(seg)
	task.Fail(status.Error(codes.DeadlineExceeded, "context deadline exceeded"))
	noteSegmentTaskDone(task)
	require.Equal(t, 5*time.Minute, SegmentTaskTimeout(seg, base))

	// Other failures leave it; a successful load resets it.
	task = newTask(seg)
	task.Fail(merr.WrapErrSegmentRequestResourceFailed("Memory"))
	noteSegmentTaskDone(task)
	require.Equal(t, 5*time.Minute, SegmentTaskTimeout(seg, base))
	task = newTask(seg)
	task.SetStatus(TaskStatusSucceeded)
	noteSegmentTaskDone(task)
	require.Equal(t, base, SegmentTaskTimeout(seg, base))

	// Retry timeout 0 keeps the base for every attempt.
	task = newTask(seg)
	task.Fail(merr.CheckRPCCall(nil, context.DeadlineExceeded))
	noteSegmentTaskDone(task)
	key := paramtable.Get().QueryCoordCfg.SegmentTaskRetryTimeout.Key
	paramtable.Get().Save(key, "0")
	defer paramtable.Get().Reset(key)
	require.Equal(t, base, SegmentTaskTimeout(seg, base))
}

func TestChannelTimeoutBackoff(t *testing.T) {
	paramtable.Init()
	replica := newTestReplica(1000, 1)
	newTask := func(node int64) *ChannelTask {
		task, err := NewChannelTask(context.Background(), 10*time.Second, testSource("test"), 1000, replica,
			NewChannelAction(node, ActionTypeGrow, "ch-1"))
		require.NoError(t, err)
		return task
	}
	base := time.Minute
	require.Equal(t, base, ChannelTaskTimeout("ch-1", 7, base))

	// A watch that timed out on a node is retried there under the retry
	// timeout; the same channel on another node is unaffected.
	task := newTask(7)
	task.Fail(merr.CheckRPCCall(nil, context.DeadlineExceeded))
	noteChannelTaskDone(task)
	require.Equal(t, 5*time.Minute, ChannelTaskTimeout("ch-1", 7, base))
	require.Equal(t, base, ChannelTaskTimeout("ch-1", 8, base))

	task = newTask(7)
	task.SetStatus(TaskStatusSucceeded)
	noteChannelTaskDone(task)
	require.Equal(t, base, ChannelTaskTimeout("ch-1", 7, base))
}

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

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSegmentTimeoutBackoff(t *testing.T) {
	replica := newTestReplica(1000, 1)
	newTask := func(seg int64) *SegmentTask {
		task, err := NewSegmentTask(context.Background(), 10*time.Second, testSource("test"), 1000, replica, 0,
			NewSegmentAction(1, ActionTypeGrow, "shard-0", seg))
		require.NoError(t, err)
		return task
	}
	const seg = 4242
	require.EqualValues(t, 1, SegmentTimeoutFactor(seg))

	// Loads that end on their deadline (as the executor reports them, through
	// merr.CheckRPCCall) double the next timeout, up to 8x.
	for want := 2; want <= 8; want *= 2 {
		task := newTask(seg)
		task.Fail(merr.CheckRPCCall(nil, context.DeadlineExceeded))
		noteSegmentTaskDone(task)
		require.EqualValues(t, want, SegmentTimeoutFactor(seg))
	}
	task := newTask(seg)
	task.Fail(merr.CheckRPCCall(nil, context.DeadlineExceeded))
	noteSegmentTaskDone(task)
	require.EqualValues(t, 8, SegmentTimeoutFactor(seg), "capped")

	// Other failures leave it; a successful load resets it.
	task = newTask(seg)
	task.Fail(merr.WrapErrSegmentRequestResourceFailed("Memory"))
	noteSegmentTaskDone(task)
	require.EqualValues(t, 8, SegmentTimeoutFactor(seg))
	task = newTask(seg)
	task.SetStatus(TaskStatusSucceeded)
	noteSegmentTaskDone(task)
	require.EqualValues(t, 1, SegmentTimeoutFactor(seg))
}

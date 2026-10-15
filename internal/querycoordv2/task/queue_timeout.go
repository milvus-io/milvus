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
	"time"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Execution deadlines begin at dispatch. Independently bound the initial queue
// wait for all checker sources, including recovery, so unschedulable requests
// cannot occupy the outstanding-load budget forever.
func (scheduler *taskScheduler) queueWaitError(t Task, now time.Time) error {
	if scheduler.admittedAt == nil {
		return nil
	}
	if _, armed := t.Context().Deadline(); armed {
		return nil
	}
	accepted, ok := scheduler.admittedAt.Get(t.ID())
	if !ok {
		return nil
	}
	timeout := paramtable.Get().QueryCoordCfg.SegmentTaskTimeout.GetAsDuration(time.Millisecond)
	if _, channel := t.(*ChannelTask); channel {
		timeout = paramtable.Get().QueryCoordCfg.ChannelTaskTimeout.GetAsDuration(time.Millisecond)
	}
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	if timeout > 0 && now.Sub(accepted) >= timeout {
		return merr.Wrap(context.DeadlineExceeded, "task exceeded scheduler queue wait")
	}
	return nil
}

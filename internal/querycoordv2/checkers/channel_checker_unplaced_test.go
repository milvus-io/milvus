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

package checkers

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func testReplica() *meta.Replica {
	return meta.NewReplica(&querypb.Replica{ID: 1, CollectionID: 100})
}

// A replica that wants a delegator and has no node to put it on is not
// converged, and says so. The distinction matters to Check, which would
// otherwise cache it as settled: the node it waits for joins the REPLICA, and
// that moves neither version the cache keys on, so the entry would never be
// invalidated and the checker would stop looking - forever.
func TestCheckReplicaReportsADelegatorItCouldNotPlace(t *testing.T) {
	c := &ChannelChecker{}
	lacking := []*meta.DmChannel{{}}

	diff := mockey.Mock((*ChannelChecker).getDmChannelDiff).Return(lacking, nil).Build()
	defer diff.UnPatch()
	loads := mockey.Mock((*ChannelChecker).createChannelLoadTask).Return([]task.Task{}).Build()
	defer loads.UnPatch()
	reduces := mockey.Mock((*ChannelChecker).createChannelReduceTasks).Return([]task.Task{}).Build()
	defer reduces.UnPatch()
	repeated := mockey.Mock((*ChannelChecker).findRepeatedChannels).Return(nil).Build()
	defer repeated.UnPatch()
	trace := mockey.Mock((*ChannelChecker).getTraceCtx).Return(context.Background()).Build()
	defer trace.UnPatch()

	tasks, unplaced := c.checkReplica(context.Background(), testReplica())
	assert.Empty(t, tasks)
	assert.True(t, unplaced, "a channel that was lacking and produced no task is a delegator with nowhere to go")
}

// A replica whose lack produced a task is progressing, not stuck: Check
// already refuses to cache it because a task was generated, and reporting
// "unplaced" as well would be redundant, not wrong - but the flag must mean
// what it says, so it is false here.
func TestCheckReplicaReportsNothingUnplacedWhenATaskWasMade(t *testing.T) {
	c := &ChannelChecker{}
	lacking := []*meta.DmChannel{{}}

	diff := mockey.Mock((*ChannelChecker).getDmChannelDiff).Return(lacking, nil).Build()
	defer diff.UnPatch()
	loads := mockey.Mock((*ChannelChecker).createChannelLoadTask).Return([]task.Task{&task.ChannelTask{}}).Build()
	defer loads.UnPatch()
	reduces := mockey.Mock((*ChannelChecker).createChannelReduceTasks).Return([]task.Task{}).Build()
	defer reduces.UnPatch()
	repeated := mockey.Mock((*ChannelChecker).findRepeatedChannels).Return(nil).Build()
	defer repeated.UnPatch()
	trace := mockey.Mock((*ChannelChecker).getTraceCtx).Return(context.Background()).Build()
	defer trace.UnPatch()
	// The task this returns is a bare value, so the annotations milvus puts on
	// a real one are stubbed out; what is under test is the flag, not them.
	reason := mockey.Mock(task.SetReason).Return().Build()
	defer reason.UnPatch()
	priority := mockey.Mock(task.SetPriority).Return().Build()
	defer priority.UnPatch()

	_, unplaced := c.checkReplica(context.Background(), testReplica())
	assert.False(t, unplaced)
}

// A converged replica lacks nothing, so there is nothing to fail to place.
// This is the case the version cache exists for, and it must stay cacheable.
func TestCheckReplicaReportsNothingUnplacedWhenNothingWasLacking(t *testing.T) {
	c := &ChannelChecker{}

	diff := mockey.Mock((*ChannelChecker).getDmChannelDiff).Return(nil, nil).Build()
	defer diff.UnPatch()
	loads := mockey.Mock((*ChannelChecker).createChannelLoadTask).Return([]task.Task{}).Build()
	defer loads.UnPatch()
	reduces := mockey.Mock((*ChannelChecker).createChannelReduceTasks).Return([]task.Task{}).Build()
	defer reduces.UnPatch()
	repeated := mockey.Mock((*ChannelChecker).findRepeatedChannels).Return(nil).Build()
	defer repeated.UnPatch()
	trace := mockey.Mock((*ChannelChecker).getTraceCtx).Return(context.Background()).Build()
	defer trace.UnPatch()

	tasks, unplaced := c.checkReplica(context.Background(), testReplica())
	assert.Empty(t, tasks)
	assert.False(t, unplaced)
}

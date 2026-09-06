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

package vchannel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

// newSegmentMetaWithCheckpoint builds a growing segment meta whose L1 commit
// is done (not a materialization blocker) and whose durable checkpoint is the
// largest timetick whose insert data is already flushed.
func newSegmentMetaWithCheckpoint(segmentID int64, checkpoint uint64) *streamingpb.SegmentAssignmentMeta {
	meta := newMaterializationBlockerMeta(segmentID, 10, true)
	meta.CheckpointTimeTick = checkpoint
	meta.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING
	return meta
}

// newFlushedSegmentMetaWithCheckpoint builds a flushed segment meta: its data
// is durably in object storage, so it must not constrain the vchannel flush
// checkpoint.
func newFlushedSegmentMetaWithCheckpoint(segmentID int64, checkpoint uint64) *streamingpb.SegmentAssignmentMeta {
	meta := newSegmentMetaWithCheckpoint(segmentID, checkpoint)
	meta.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED
	return meta
}

// persistDirtySnapshots mimics one recovery persist round: it consumes every
// dirty snapshot of the module and marks it persisted, advancing the
// persisted frontiers that FlushCheckpointTimeTick reads. Without it the
// in-memory meta advances but the persisted values (what a crash-recovery
// would observe) stay behind.
func persistDirtySnapshots(module *VChannelRecoveryModule) {
	for _, snap := range module.ConsumeDirtySnapshots() {
		snap.MarkPersisted()
	}
}

// observeVChannelBarrier observes one ManualFlush message of the module's
// vchannel; it is classified as a transform barrier.
func observeVChannelBarrier(t *testing.T, module *VChannelRecoveryModule, vchannel string, timetick uint64) {
	t.Helper()
	mutable := message.NewManualFlushMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.ManualFlushMessageHeader{}).
		WithBody(&message.ManualFlushMessageBody{}).
		MustBuildMutable()
	raw := mutable.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
	owner := message.NewOwnedImmutableMessage(raw, nil)
	retained := owner.Clone()
	require.True(t, module.ObserveMessage(context.Background(), retained))
	retained.Release()
	owner.Release()
}

func TestVChannelFlushCheckpointMinOfMaterializedAndGrowing(t *testing.T) {
	// The vchannel flush checkpoint is min of the materialized frontier and
	// every growing segment's durable checkpoint — all read from the
	// persisted metas, so nothing is reported before it is durable.
	ctx := context.Background()
	scheduler := &recordingVChannelScheduler{}
	segmentMetas := map[int64]*streamingpb.SegmentAssignmentMeta{
		1: newSegmentMetaWithCheckpoint(1, 150),
		2: newSegmentMetaWithCheckpoint(2, 200),
	}
	module := newMaterializeBoundTestModule(t, scheduler, segmentMetas)
	// delete@100 and delete@400: observation schedules one task, the
	// cap-batch continuation inside materialize chases the frontier to 400.
	observeVChannelDelete(t, module, "v1", 100)
	observeVChannelDelete(t, module, "v1", 400)
	require.Len(t, scheduler.tasks, 1)
	require.NoError(t, scheduler.tasks[0].Execute(ctx))
	require.Len(t, scheduler.tasks, 2)
	require.NoError(t, scheduler.tasks[1].Execute(ctx))
	assert.Equal(t, uint64(400), module.transformLog.MaterializedTimeTick())
	// The in-memory frontier is 400 but it is not persisted yet: the flush
	// checkpoint must not report it.
	assert.Equal(t, uint64(0), module.FlushCheckpointTimeTick())
	// After a persist round the persisted frontier is 400; growing segment 1
	// (durable checkpoint 150) pins the result.
	persistDirtySnapshots(module)
	assert.Equal(t, uint64(150), module.FlushCheckpointTimeTick())
}

func TestVChannelFlushCheckpointIgnoresFlushedSegments(t *testing.T) {
	// A flushed segment has its data durably in object storage; it no longer
	// constrains the vchannel flush checkpoint.
	ctx := context.Background()
	scheduler := &recordingVChannelScheduler{}
	segmentMetas := map[int64]*streamingpb.SegmentAssignmentMeta{
		1: newFlushedSegmentMetaWithCheckpoint(1, 150), // flushed below, must not pin
		2: newSegmentMetaWithCheckpoint(2, 200),
	}
	module := newMaterializeBoundTestModule(t, scheduler, segmentMetas)
	observeVChannelDelete(t, module, "v1", 400)
	require.Len(t, scheduler.tasks, 1)
	require.NoError(t, scheduler.tasks[0].Execute(ctx))
	assert.Equal(t, uint64(400), module.transformLog.MaterializedTimeTick())
	persistDirtySnapshots(module)
	assert.Equal(t, uint64(200), module.FlushCheckpointTimeTick())
}

func TestVChannelFlushCheckpointBarrierAdvancesFrontier(t *testing.T) {
	// A flush barrier advances the materialized frontier even on a vchannel
	// with no delete records; without it the frontier stays at its initial
	// value and the flush checkpoint would never reach the flush boundary.
	ctx := context.Background()
	scheduler := &recordingVChannelScheduler{}
	module := newMaterializeBoundTestModule(t, scheduler, nil)
	assert.Equal(t, uint64(0), module.FlushCheckpointTimeTick())
	observeVChannelBarrier(t, module, "v1", 200)
	require.Len(t, scheduler.tasks, 1)
	require.NoError(t, scheduler.tasks[0].Execute(ctx))
	assert.Equal(t, uint64(200), module.transformLog.MaterializedTimeTick())
	persistDirtySnapshots(module)
	assert.Equal(t, uint64(200), module.FlushCheckpointTimeTick())
}

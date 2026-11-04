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

package stats

import (
	"context"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	ctask "github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestNotifySegmentStatsBuild(t *testing.T) {
	meta.DrainStatsBuildChForTest()
	defer meta.DrainStatsBuildChForTest()

	meta.NotifySegmentStatsBuild(10, 20, 10)

	assert.Equal(t, []UniqueID{10, 20, 10}, meta.DrainStatsBuildChForTest())
}

func TestNotifySegmentStatsBuildWaitsForConsumer(t *testing.T) {
	meta.SetStatsBuildConsumerForTest(t, make(chan struct{}))
	meta.FillStatsBuildChForTest(t)
	ch := meta.GetStatsBuildChSingleton()
	time.AfterFunc(20*time.Millisecond, func() { <-ch })

	meta.NotifySegmentStatsBuild(100)

	segIDs := meta.DrainStatsBuildChForTest()
	assert.Equal(t, UniqueID(100), segIDs[len(segIDs)-1], "the segment must wait for room, not be dropped")
}

func TestNotifySegmentStatsBuildDropsWithoutConsumer(t *testing.T) {
	stopped := make(chan struct{})
	close(stopped)
	for _, done := range []<-chan struct{}{nil, stopped} {
		meta.SetStatsBuildConsumerForTest(t, done)
		meta.FillStatsBuildChForTest(t)

		meta.NotifySegmentStatsBuild(100)

		assert.NotContains(t, meta.DrainStatsBuildChForTest(), UniqueID(100))
	}
}

func TestNotifyCollectionStatsBuildNeverBlocks(t *testing.T) {
	ch := make(chan UniqueID, 1)
	meta.NotifyCollectionStatsBuild(ch, 1)
	meta.NotifyCollectionStatsBuild(ch, 2)
	meta.NotifyCollectionStatsBuild(nil, 3)

	assert.Equal(t, UniqueID(1), <-ch)
	assert.Empty(t, ch)
}

// A notification round takes only the notified segments, deduplicated; the
// ticker also takes the pending ones, and a notification supersedes a pending
// retry.
func TestStatsInspector_PopStatsSegments(t *testing.T) {
	inspector := NewStatsInspector(context.Background(), nil, nil, nil, nil, nil, nil, nil, nil)
	inspector.requeuePendingStatsSegments(1, 2)
	inspector.notifyStatsSegments(2, 3, 2)

	assert.ElementsMatch(t, []UniqueID{2, 3}, inspector.popStatsSegments(false))
	assert.ElementsMatch(t, []UniqueID{1}, inspector.popStatsSegments(true))
	assert.Empty(t, inspector.popStatsSegments(true))
}

// Senders must not depend on the trigger loop: with only the forwarder running,
// more segments than the channel holds still go through.
func TestStatsInspector_ForwardStatsBuildChWithoutTriggerLoop(t *testing.T) {
	meta.DrainStatsBuildChForTest()
	defer meta.DrainStatsBuildChForTest()

	// The deadline bounds the senders' wait if forwarding regresses.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	inspector := NewStatsInspector(ctx, nil, nil, nil, nil, nil, nil, nil, nil)
	defer inspector.Stop()
	meta.SetStatsBuildConsumerForTest(t, ctx.Done())
	inspector.loopWg.Add(1)
	go inspector.forwardStatsBuildCh(inspector.ctx)

	total := 2 * cap(meta.GetStatsBuildChSingleton())
	meta.NotifySegmentStatsBuild(lo.RangeFrom(UniqueID(0), total)...)

	got := typeutil.NewUniqueSet()
	assert.Eventually(t, func() bool {
		got.Insert(inspector.popStatsSegments(false)...)
		return got.Len() == total
	}, 5*time.Second, 10*time.Millisecond)
	assert.Len(t, inspector.notifiedStatsSegsCh, 1, "forwarding must wake the trigger loop")
}

// Producers notify only after their metadata mutation commits.
func TestStatsNotifyNothingWhenCompactionMutationFails(t *testing.T) {
	meta.DrainStatsBuildChForTest()
	defer meta.DrainStatsBuildChForTest()
	mt := ctask.NewMockCompactionMeta(t)
	mt.EXPECT().CompleteCompactionMutation(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, merr.WrapErrServiceInternalMsg("injected"))
	task := ctask.NewMixCompactionTask(&datapb.CompactionTask{CollectionID: 100, Type: datapb.CompactionType_MixCompaction}, nil, mt, nil, nil)

	require.Error(t, task.SaveSegmentMeta(&datapb.CompactionPlanResult{Segments: []*datapb.CompactionSegment{{SegmentID: 2}}}))

	assert.Empty(t, meta.DrainStatsBuildChForTest())
}

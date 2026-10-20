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

package datacoord

import (
	"context"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func drainStatsBuildChForTest() []UniqueID {
	ch := getStatsBuildChSingleton()
	var segIDs []UniqueID
	for {
		select {
		case segID := <-ch:
			segIDs = append(segIDs, segID)
		default:
			return segIDs
		}
	}
}

func fillStatsBuildChForTest(t *testing.T) {
	ch := getStatsBuildChSingleton()
	for len(ch) < cap(ch) {
		ch <- 0
	}
	t.Cleanup(func() { drainStatsBuildChForTest() })
}

func setStatsBuildConsumerForTest(t *testing.T, done <-chan struct{}) {
	prev, _ := statsBuildConsumerDone.Load().(<-chan struct{})
	setStatsBuildConsumer(done)
	t.Cleanup(func() { setStatsBuildConsumer(prev) })
}

func TestNotifySegmentStatsBuild(t *testing.T) {
	drainStatsBuildChForTest()
	defer drainStatsBuildChForTest()

	notifySegmentStatsBuild(10, 20, 10)

	assert.Equal(t, []UniqueID{10, 20, 10}, drainStatsBuildChForTest())
}

func TestNotifySegmentStatsBuildWaitsForConsumer(t *testing.T) {
	setStatsBuildConsumerForTest(t, make(chan struct{}))
	fillStatsBuildChForTest(t)
	ch := getStatsBuildChSingleton()
	time.AfterFunc(20*time.Millisecond, func() { <-ch })

	notifySegmentStatsBuild(100)

	segIDs := drainStatsBuildChForTest()
	assert.Equal(t, UniqueID(100), segIDs[len(segIDs)-1], "the segment must wait for room, not be dropped")
}

func TestNotifySegmentStatsBuildDropsWithoutConsumer(t *testing.T) {
	stopped := make(chan struct{})
	close(stopped)
	for _, done := range []<-chan struct{}{nil, stopped} {
		setStatsBuildConsumerForTest(t, done)
		fillStatsBuildChForTest(t)

		notifySegmentStatsBuild(100)

		assert.NotContains(t, drainStatsBuildChForTest(), UniqueID(100))
	}
}

func TestNotifyCollectionStatsBuildNeverBlocks(t *testing.T) {
	ch := make(chan UniqueID, 1)
	notifyCollectionStatsBuild(ch, 1)
	notifyCollectionStatsBuild(ch, 2)
	notifyCollectionStatsBuild(nil, 3)

	assert.Equal(t, UniqueID(1), <-ch)
	assert.Empty(t, ch)
}

// A notification round takes only the notified segments, deduplicated; the
// ticker also takes the pending ones, and a notification supersedes a pending
// retry.
func TestStatsInspector_PopStatsSegments(t *testing.T) {
	inspector := newStatsInspector(context.Background(), nil, nil, nil, nil, nil, nil, nil)
	inspector.requeuePendingStatsSegments(1, 2)
	inspector.notifyStatsSegments(2, 3, 2)

	assert.ElementsMatch(t, []UniqueID{2, 3}, inspector.popStatsSegments(false))
	assert.ElementsMatch(t, []UniqueID{1}, inspector.popStatsSegments(true))
	assert.Empty(t, inspector.popStatsSegments(true))
}

// Senders must not depend on the trigger loop: with only the forwarder running,
// more segments than the channel holds still go through.
func TestStatsInspector_ForwardStatsBuildChWithoutTriggerLoop(t *testing.T) {
	drainStatsBuildChForTest()
	defer drainStatsBuildChForTest()

	// The deadline bounds the senders' wait if forwarding regresses.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	inspector := newStatsInspector(ctx, nil, nil, nil, nil, nil, nil, nil)
	defer inspector.Stop()
	setStatsBuildConsumerForTest(t, ctx.Done())
	inspector.loopWg.Add(1)
	go inspector.forwardStatsBuildCh(inspector.ctx)

	total := 2 * cap(getStatsBuildChSingleton())
	notifySegmentStatsBuild(lo.RangeFrom(UniqueID(0), total)...)

	got := typeutil.NewUniqueSet()
	assert.Eventually(t, func() bool {
		got.Insert(inspector.popStatsSegments(false)...)
		return got.Len() == total
	}, 5*time.Second, 10*time.Millisecond)
	assert.Len(t, inspector.notifiedStatsSegsCh, 1, "forwarding must wake the trigger loop")
}

// Producers notify only after their metadata mutation commits.
func TestStatsNotifyNothingWhenCompactionMutationFails(t *testing.T) {
	drainStatsBuildChForTest()
	defer drainStatsBuildChForTest()
	mt := NewMockCompactionMeta(t)
	mt.EXPECT().CompleteCompactionMutation(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, merr.WrapErrServiceInternalMsg("injected"))
	task := newMixCompactionTask(&datapb.CompactionTask{CollectionID: 100, Type: datapb.CompactionType_MixCompaction}, nil, mt, nil)

	require.Error(t, task.saveSegmentMeta(&datapb.CompactionPlanResult{Segments: []*datapb.CompactionSegment{{SegmentID: 2}}}))

	assert.Empty(t, drainStatsBuildChForTest())
}

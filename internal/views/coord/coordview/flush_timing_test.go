// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package coordview

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestPendingCohortKeepsFirstEnqueueAndExclusiveReasons(t *testing.T) {
	tasks := &capturedDirtyViewTaskScheduler{}
	s := newDirtyViewFlushScheduler(newMockCatalog(), newMockSyncer(), 128, tasks)
	defer s.Close()
	sid := qviews.ShardID{VChannel: "timing", ReplicaID: 1}
	batch := s.Begin()
	s.Submit(dirtyPersistEvent(sid, 1))
	pending := s.pending[sid]
	created := pending.timing.created
	require.Equal(t, waitBatch, pending.timing.reason)
	s.Submit(dirtyPersistEvent(sid, 2))
	require.Equal(t, created, pending.timing.created)
	batch.Commit()
	require.Equal(t, waitEligible, pending.timing.reason)
	first := tasks.snapshot()[0]
	claimed := s.claim()
	require.True(t, pending.timing.finished)
	// While the first event is inflight, a second event waits on the shard lane.
	s.Submit(dirtyPersistEvent(sid, 3))
	second := s.pending[sid]
	require.Equal(t, waitShard, second.timing.reason)
	s.complete(first.(*dirtyViewFlushTask), claimed, nil)
	require.Equal(t, waitEligible, second.timing.reason)
	require.GreaterOrEqual(t, s.oldestPendingAge(), float64(0))
	second.timing.since = time.Now().Add(-time.Millisecond)
	s.Close()
	require.True(t, second.timing.finished)
	require.GreaterOrEqual(t, second.timing.durations[waitEligible], time.Millisecond)
	require.Zero(t, s.oldestPendingAge())
}

func TestFlushFailureDoesNotSyncOrReleaseReferences(t *testing.T) {
	catalog := newMockCatalog()
	catalog.saveErr = errors.New("injected catalog failure")
	stream := newMockSyncer()
	s := newDirtyViewFlushScheduler(catalog, stream, 128, &capturedDirtyViewTaskScheduler{})
	defer s.Close()
	event := dirtyPersistEvent(testShardID, 1)
	released := false
	event.afterPersist = append(event.afterPersist, func() { released = true })
	pb := event.persists[0]
	event.syncs = []syncer.SyncView{{View: qviews.NewFullQueryViewAtStreamingNode(pb.Meta, pb.StreamingNode, pb.QueryNode)}}
	s.Submit(event)
	require.ErrorIs(t, s.flushBatch(context.Background(), s.claim()), catalog.saveErr)
	require.False(t, released)
	require.Empty(t, stream.syncCalls)
}

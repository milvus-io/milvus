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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type discoveryTestHandler struct {
	Handler
	get func(context.Context, int64) (*collectionInfo, error)
}

func (h *discoveryTestHandler) GetCollection(ctx context.Context, id int64) (*collectionInfo, error) {
	return h.get(ctx, id)
}

type discoveryTestBroker struct {
	broker.Broker
	get func(context.Context, ...int64) ([]*internalpb.FileResourceInfo, error)
}

func (b *discoveryTestBroker) GetFileResources(ctx context.Context, ids ...int64) ([]*internalpb.FileResourceInfo, error) {
	return b.get(ctx, ids...)
}

func TestStatsDiscoveryCollectionCacheMiss(t *testing.T) {
	f := newDiscoveryFixture(t, "event")
	col := f.mt.GetCollection(1)
	f.mt.collections.Remove(1)
	f.mt.segments.SetSegment(1, discoverySegment(1, true))
	key := statsReconcileKey{1, indexpb.StatsSubJob_TextIndexJob}
	result, err := f.si.reconcileStats(key, make(map[int64]statsFieldRules))
	require.NoError(t, err)
	require.Equal(t, statsDeferred, result, "cache miss is not proof of deletion")
	f.si.handler = &discoveryTestHandler{get: func(context.Context, int64) (*collectionInfo, error) {
		return nil, merr.WrapErrServiceInternalMsg("temporary lookup failure")
	}}
	result, err = f.si.reconcileStats(key, make(map[int64]statsFieldRules))
	require.Error(t, err)
	require.Equal(t, statsDeferred, result)
	f.si.handler = &discoveryTestHandler{get: func(context.Context, int64) (*collectionInfo, error) {
		return nil, merr.WrapErrCollectionNotFound(1)
	}}
	result, err = f.si.reconcileStats(key, make(map[int64]statsFieldRules))
	require.NoError(t, err)
	require.Equal(t, statsNotNeeded, result)
	f.si.handler = &discoveryTestHandler{get: func(context.Context, int64) (*collectionInfo, error) {
		f.mt.AddCollection(col)
		return col, nil
	}}
	result, err = f.si.reconcileStats(key, make(map[int64]statsFieldRules))
	require.NoError(t, err)
	require.Equal(t, statsSubmitted, result)
}

func TestStatsDiscoveryResourcesAndSchemaRace(t *testing.T) {
	f := newDiscoveryFixture(t, "event")
	setDiscoveryTestParam(t, &Params.CommonCfg.DNFileResourceMode, "ref")
	col := f.mt.GetClonedCollectionInfo(1)
	col.Schema.FileResourceIds = []int64{7}
	f.mt.AddCollection(col)
	f.mt.segments.SetSegment(1, discoverySegment(1, true))
	key := statsReconcileKey{1, indexpb.StatsSubJob_TextIndexJob}
	resource := &internalpb.FileResourceInfo{}
	f.mt.broker = &discoveryTestBroker{get: func(context.Context, ...int64) ([]*internalpb.FileResourceInfo, error) {
		return nil, merr.WrapErrServiceInternalMsg("temporary resource lookup failure")
	}}
	result, err := f.si.reconcileStats(key, make(map[int64]statsFieldRules))
	require.Error(t, err)
	require.Equal(t, statsDeferred, result)
	require.Zero(t, f.alloc.calls.Load())
	f.mt.broker = &discoveryTestBroker{get: func(_ context.Context, ids ...int64) ([]*internalpb.FileResourceInfo, error) {
		require.Equal(t, []int64{7}, ids)
		changed := f.mt.GetClonedCollectionInfo(1)
		changed.Schema.FileResourceIds = []int64{8}
		f.mt.AddCollection(changed)
		return []*internalpb.FileResourceInfo{resource}, nil
	}}
	result, err = f.si.reconcileStats(key, make(map[int64]statsFieldRules))
	require.NoError(t, err)
	require.Equal(t, statsDeferred, result, "resources fetched for an old schema must not be submitted")
	require.Zero(t, f.alloc.calls.Load())
	f.mt.broker = &discoveryTestBroker{get: func(_ context.Context, ids ...int64) ([]*internalpb.FileResourceInfo, error) {
		require.Equal(t, []int64{8}, ids)
		return []*internalpb.FileResourceInfo{resource}, nil
	}}
	result, err = f.si.reconcileStats(key, make(map[int64]statsFieldRules))
	require.NoError(t, err)
	require.Equal(t, statsSubmitted, result)
	st := f.mt.statsTaskMeta.GetStatsTaskBySegmentID(1, key.subjob)
	require.Len(t, st.GetFileResources(), 1)

	// Successful resource lookups are shared only within this bounded batch.
	calls := 0
	f.mt.broker = &discoveryTestBroker{get: func(context.Context, ...int64) ([]*internalpb.FileResourceInfo, error) {
		calls++
		return []*internalpb.FileResourceInfo{resource}, nil
	}}
	rules := make(map[int64]statsFieldRules)
	for _, id := range []int64{2, 3} {
		f.mt.segments.SetSegment(id, discoverySegment(id, true))
		result, err := f.si.reconcileStats(statsReconcileKey{id, key.subjob}, rules)
		require.NoError(t, err)
		require.Equal(t, statsSubmitted, result)
	}
	require.Equal(t, 1, calls)
}

func TestStatsDiscoveryCancellationDuringDependency(t *testing.T) {
	f := newDiscoveryFixture(t, "event")
	setDiscoveryTestParam(t, &Params.CommonCfg.DNFileResourceMode, "ref")
	col := f.mt.GetClonedCollectionInfo(1)
	col.Schema.FileResourceIds = []int64{7}
	f.mt.AddCollection(col)
	entered := make(chan struct{})
	var once sync.Once
	f.mt.broker = &discoveryTestBroker{get: func(ctx context.Context, _ ...int64) ([]*internalpb.FileResourceInfo, error) {
		once.Do(func() { close(entered) })
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	require.NoError(t, f.mt.AddSegment(context.Background(), discoverySegment(1, true)))
	f.si.Start()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("resource lookup not reached")
	}
	stopped := make(chan struct{})
	go func() { f.si.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("stop did not cancel resource lookup")
	}
	require.False(t, f.mt.statsTaskMeta.HasStatsTask(1, indexpb.StatsSubJob_TextIndexJob))
}

func TestStatsDiscoveryConcurrentSubmission(t *testing.T) {
	f := newDiscoveryFixture(t, "event")
	f.mt.segments.SetSegment(1, discoverySegment(1, true))
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- f.si.SubmitStatsTask(1, 1, indexpb.StatsSubJob_TextIndexJob, true, nil)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.EqualValues(t, 1, f.alloc.calls.Load())
	require.EqualValues(t, 1, f.scheduler.enqueued.Load())
}

func TestStatsDiscoveryRecoveryAfterPersistBeforeEnqueue(t *testing.T) {
	f := newDiscoveryFixture(t, "event")
	f.mt.segments.SetSegment(1, discoverySegment(1, true))
	// Simulate a process exit after persistence but before scheduler.Enqueue.
	require.NoError(t, f.catalog.SaveStatsTask(context.Background(), &indexpb.StatsTask{
		TaskID: 9000, CollectionID: 1, PartitionID: 2, SegmentID: 1, TargetSegmentID: 1,
		SubJobType: indexpb.StatsSubJob_TextIndexJob, State: indexpb.JobState_JobStateInit,
	}))
	reloaded, err := newStatsTaskMeta(context.Background(), f.catalog)
	require.NoError(t, err)
	f.mt.statsTaskMeta = reloaded
	reloaded.statsDiscovery.Store(f.si.discovery)
	f.si.Start()
	f.waitTasks(t, 2)
	require.EqualValues(t, 9000, reloaded.GetStatsTaskBySegmentID(1, indexpb.StatsSubJob_TextIndexJob).GetTaskID())
	require.EqualValues(t, 1, f.alloc.calls.Load(), "only the missing JSON task needs a new ID")
}

func TestStatsDiscoveryBatchDeleteNotifications(t *testing.T) {
	for _, op := range []string{"truncate", "drop_channel", "batch_delete"} {
		t.Run(op, func(t *testing.T) {
			f := newDiscoveryFixture(t, "event")
			segment := discoverySegment(1, false)
			require.NoError(t, f.mt.AddSegment(context.Background(), segment))
			drainDiscovery(f.si.discovery)
			switch op {
			case "truncate":
				require.NoError(t, f.mt.TruncateChannelByTime(context.Background(), segment.GetInsertChannel(), ^uint64(0)))
			case "drop_channel":
				require.NoError(t, f.mt.UpdateDropChannelSegmentInfo(context.Background(), segment.GetInsertChannel(), nil))
			case "batch_delete":
				require.NoError(t, f.mt.DropSegmentsOfPartition(context.Background(), []int64{2}))
				drainDiscovery(f.si.discovery)
				err := f.mt.DropSegment(context.Background(), 1)
				require.NoError(t, err)
			}
			require.Equal(t, 2, discoveryPending(f.si.discovery))
			result, err := f.si.reconcileStats(statsReconcileKey{1, indexpb.StatsSubJob_TextIndexJob}, make(map[int64]statsFieldRules))
			require.NoError(t, err)
			require.Equal(t, statsNotNeeded, result)
		})
	}
}

func TestStatsDiscoveryRelevantMetadataChanges(t *testing.T) {
	old := discoverySegment(1, true)
	cases := []struct {
		name    string
		mutate  func(*datapb.SegmentInfo)
		changed bool
	}{
		{"rows", func(s *datapb.SegmentInfo) { s.NumOfRows++ }, false},
		{"schema", func(s *datapb.SegmentInfo) { s.SchemaVersion++ }, true},
		{"manifest", func(s *datapb.SegmentInfo) { s.ManifestPath = "new/manifest" }, true},
		{"import", func(s *datapb.SegmentInfo) { s.IsImporting = true }, true},
		{"visibility", func(s *datapb.SegmentInfo) { s.IsInvisible = true }, true},
		{"json_stats", func(s *datapb.SegmentInfo) { s.JsonKeyStats = map[int64]*datapb.JsonKeyStats{102: {}} }, true},
		{"text_stats", func(s *datapb.SegmentInfo) { s.TextStatsLogs = map[int64]*datapb.TextIndexStats{101: {}} }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			updated := old.Clone()
			tc.mutate(updated.SegmentInfo)
			require.Equal(t, tc.changed, statsSegmentChanged(old, updated))
		})
	}
}

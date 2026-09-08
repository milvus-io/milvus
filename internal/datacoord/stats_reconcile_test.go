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
	"iter"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type discoveryTestAllocator struct {
	allocator.Allocator
	calls atomic.Int64
	fail  atomic.Bool
}

func (a *discoveryTestAllocator) AllocID(context.Context) (int64, error) {
	id := a.calls.Add(1)
	if a.fail.Load() {
		return 0, merr.WrapErrServiceInternalMsg("injected allocation failure")
	}
	return id + 1000, nil
}

type discoveryTestScheduler struct {
	task.GlobalScheduler
	pending  atomic.Int64
	enqueued atomic.Int64
}

func (s *discoveryTestScheduler) GetPendingTaskCount(kind taskcommon.Type) int {
	if kind != taskcommon.Stats {
		panic("admission must be stats-scoped")
	}
	return int(s.pending.Load())
}
func (s *discoveryTestScheduler) Enqueue(task.Task) { s.enqueued.Add(1) }

type discoveryTestCatalog struct {
	metastore.DataCoordCatalog
	mu                 sync.Mutex
	tasks              map[int64]*indexpb.StatsTask
	metadataErr        error
	failSave, failDrop atomic.Bool
}

func (c *discoveryTestCatalog) SaveStatsTask(_ context.Context, st *indexpb.StatsTask) error {
	if c.failSave.Load() {
		return merr.WrapErrServiceInternalMsg("injected task persistence failure")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tasks[st.GetTaskID()] = proto.Clone(st).(*indexpb.StatsTask)
	return nil
}

func (c *discoveryTestCatalog) DropStatsTask(_ context.Context, id int64) error {
	if c.failDrop.Load() {
		return merr.WrapErrServiceInternalMsg("injected task cleanup failure")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.tasks, id)
	return nil
}

func (c *discoveryTestCatalog) ListStatsTasks(context.Context) ([]*indexpb.StatsTask, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	tasks := make([]*indexpb.StatsTask, 0, len(c.tasks))
	for _, st := range c.tasks {
		tasks = append(tasks, proto.Clone(st).(*indexpb.StatsTask))
	}
	return tasks, nil
}

type discoveryFixture struct {
	si        *statsInspector
	mt        *meta
	alloc     *discoveryTestAllocator
	scheduler *discoveryTestScheduler
	catalog   *discoveryTestCatalog
}

func setDiscoveryTestParam(t testing.TB, item *paramtable.ParamItem, value string) {
	t.Helper()
	old := item.GetValue()
	require.NoError(t, Params.Save(item.Key, value))
	t.Cleanup(func() { require.NoError(t, Params.Save(item.Key, old)) })
}

func newDiscoveryFixture(t testing.TB, mode string) *discoveryFixture {
	t.Helper()
	setDiscoveryTestParam(t, &Params.DataCoordCfg.StatsDiscoveryMode, mode)
	setDiscoveryTestParam(t, &Params.DataCoordCfg.GCInterval, "3600")
	setDiscoveryTestParam(t, &Params.DataCoordCfg.TaskCheckInterval, "3600")
	setDiscoveryTestParam(t, &Params.CommonCfg.EnabledJSONKeyStats, "true")
	setDiscoveryTestParam(t, &Params.DataCoordCfg.JSONStatsTriggerCount, "10")
	f := &discoveryFixture{
		mt:        &meta{ctx: context.Background(), segments: NewSegmentsInfo()},
		alloc:     &discoveryTestAllocator{},
		scheduler: &discoveryTestScheduler{},
		catalog:   &discoveryTestCatalog{tasks: make(map[int64]*indexpb.StatsTask)},
	}
	f.mt.catalog = f.catalog
	f.mt.collections = typeutil.NewConcurrentMap[int64, *collectionInfo]()
	f.mt.AddCollection(&collectionInfo{ID: 1, Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 101, DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_match", Value: "true"}}},
		{FieldID: 102, DataType: schemapb.DataType_JSON},
	}}})
	var err error
	f.mt.statsTaskMeta, err = newStatsTaskMeta(context.Background(), f.catalog)
	require.NoError(t, err)
	f.si = newStatsInspector(context.Background(), f.mt, f.scheduler, f.alloc, nil, nil, newIndexEngineVersionManager())
	f.si.discoveryOptions.scanInterval = time.Millisecond
	f.si.discoveryOptions.retryInterval = 5 * time.Millisecond
	f.si.discoveryOptions.retryMaxInterval = 20 * time.Millisecond
	f.si.discoveryOptions.reconcileInterval = time.Hour
	t.Cleanup(f.si.Stop)
	return f
}

func discoverySegment(id int64, sorted bool) *SegmentInfo {
	return NewSegmentInfo(&datapb.SegmentInfo{
		ID: id, CollectionID: 1, PartitionID: 2, InsertChannel: "stats-discovery-test",
		State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
		IsSorted: sorted, NumOfRows: 100,
	})
}

func discoveryPending(q *statsReconcileQueue) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending)
}

func (f *discoveryFixture) waitTasks(t *testing.T, count int) {
	t.Helper()
	require.Eventually(t, func() bool {
		return f.mt.statsTaskMeta.tasks.Len() == count && f.scheduler.enqueued.Load() >= int64(count)
	}, 5*time.Second, time.Millisecond)
}

func drainDiscovery(q *statsReconcileQueue) {
	for {
		work, ok := q.pop(time.Now().Add(time.Hour))
		if !ok {
			return
		}
		q.complete(work, false, time.Now(), time.Second, time.Minute)
	}
}

func TestStatsDiscoveryFlushSortAndDuplicate(t *testing.T) {
	f := newDiscoveryFixture(t, "event")
	segment := discoverySegment(1, false)
	segment.State = commonpb.SegmentState_Growing
	require.NoError(t, f.mt.AddSegment(context.Background(), segment))
	f.si.Start()
	require.Eventually(t, func() bool { return discoveryPending(f.si.discovery) == 0 }, time.Second, time.Millisecond)
	require.Zero(t, f.scheduler.enqueued.Load())
	require.NoError(t, f.mt.SetState(context.Background(), 1, commonpb.SegmentState_Flushing))
	require.Eventually(t, func() bool { return discoveryPending(f.si.discovery) == 0 }, time.Second, time.Millisecond)
	require.Zero(t, f.scheduler.enqueued.Load(), "flushing but unsorted is not eligible")
	require.NoError(t, f.mt.UpdateSegmentsInfo(context.Background(), func(p *updateSegmentPack) bool { p.Get(1).IsSorted = true; return true }))
	f.waitTasks(t, 2)
	for range 100 {
		f.mt.notifyStatsSegments(1, 1)
	}
	require.Eventually(t, func() bool { return discoveryPending(f.si.discovery) == 0 }, time.Second, time.Millisecond)
	require.EqualValues(t, 2, f.alloc.calls.Load())
	require.EqualValues(t, 2, f.scheduler.enqueued.Load())
}

func TestStatsDiscoveryRetryWithoutNewEvent(t *testing.T) {
	for _, failure := range []string{"admission", "allocation", "persistence"} {
		t.Run(failure, func(t *testing.T) {
			f := newDiscoveryFixture(t, "event")
			switch failure {
			case "admission":
				f.scheduler.pending.Store(100000)
			case "allocation":
				f.alloc.fail.Store(true)
			case "persistence":
				f.catalog.failSave.Store(true)
			}
			require.NoError(t, f.mt.AddSegment(context.Background(), discoverySegment(1, true)))
			f.si.Start()
			require.Eventually(t, func() bool {
				q := f.si.discovery
				q.mu.Lock()
				defer q.mu.Unlock()
				for _, entry := range q.pending {
					if entry.failures > 0 {
						return true
					}
				}
				return false
			}, time.Second, time.Millisecond)
			require.Zero(t, f.mt.statsTaskMeta.tasks.Len())
			require.Zero(t, f.scheduler.enqueued.Load())
			f.scheduler.pending.Store(0)
			f.alloc.fail.Store(false)
			f.catalog.failSave.Store(false)
			// No metadata mutation, notification or periodic full scan after recovery.
			f.waitTasks(t, 2)
		})
	}
}

func TestStatsDiscoveryOverflowAndLostNotification(t *testing.T) {
	f := newDiscoveryFixture(t, "event")
	q := newStatsReconcileQueue(8, 1)
	f.si.discovery = q
	f.mt.statsDiscovery.Store(q)
	f.mt.statsTaskMeta.statsDiscovery.Store(q)
	for id := int64(1); id <= 40; id++ {
		segment := discoverySegment(id, true)
		if id == 40 {
			// Simulate commit/cache publication followed by crash before notification.
			f.mt.statsDiscovery.Store(nil)
			require.NoError(t, f.mt.AddSegment(context.Background(), segment))
			f.mt.statsDiscovery.Store(q)
		} else {
			require.NoError(t, f.mt.AddSegment(context.Background(), segment))
		}
	}
	require.Positive(t, q.overflows)
	require.LessOrEqual(t, len(q.pending), 8)
	f.si.Start()
	f.waitTasks(t, 80)
	require.Eventually(t, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		return len(q.pending) == 0 && len(q.scopes) == 0
	}, 5*time.Second, time.Millisecond, "all scan generations must converge")
}

func TestStatsDiscoverySchemaAndConfig(t *testing.T) {
	f := newDiscoveryFixture(t, "event")
	setDiscoveryTestParam(t, &Params.CommonCfg.EnabledJSONKeyStats, "false")
	collection := f.mt.GetClonedCollectionInfo(1)
	collection.Schema.Fields = collection.Schema.Fields[1:] // JSON only
	f.mt.AddCollection(collection)
	require.NoError(t, f.mt.AddSegment(context.Background(), discoverySegment(1, true)))
	f.si.Start()
	require.Eventually(t, func() bool { return discoveryPending(f.si.discovery) == 0 }, time.Second, time.Millisecond)
	require.Zero(t, f.mt.statsTaskMeta.tasks.Len())
	require.NoError(t, Params.Save(Params.CommonCfg.EnabledJSONKeyStats.Key, "true"))
	f.waitTasks(t, 1)
	require.True(t, f.mt.statsTaskMeta.HasStatsTask(1, indexpb.StatsSubJob_JsonKeyIndexJob))
	collection = f.mt.GetClonedCollectionInfo(1)
	collection.Schema.Fields = append(collection.Schema.Fields, &schemapb.FieldSchema{
		FieldID: 103, DataType: schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{{Key: "enable_match", Value: "true"}},
	})
	f.mt.AddCollection(collection) // no segment event; collection expansion must discover it.
	f.waitTasks(t, 2)
}

func TestStatsDiscoveryTerminalTaskCleanup(t *testing.T) {
	f := newDiscoveryFixture(t, "event")
	require.NoError(t, f.mt.AddSegment(context.Background(), discoverySegment(1, true)))
	result, err := f.si.reconcileStats(statsReconcileKey{1, indexpb.StatsSubJob_TextIndexJob}, make(map[int64]statsFieldRules))
	require.NoError(t, err)
	require.Equal(t, statsSubmitted, result)
	st := f.mt.statsTaskMeta.GetStatsTaskBySegmentID(1, indexpb.StatsSubJob_TextIndexJob)
	require.NoError(t, f.mt.statsTaskMeta.FinishTask(st.GetTaskID(), &workerpb.StatsResult{State: indexpb.JobState_JobStateFailed}))
	result, err = f.si.reconcileStats(statsReconcileKey{1, st.GetSubJobType()}, make(map[int64]statsFieldRules))
	require.NoError(t, err)
	require.Equal(t, statsExisting, result)
	drainDiscovery(f.si.discovery)
	f.catalog.failDrop.Store(true)
	require.Error(t, f.mt.statsTaskMeta.DropStatsTask(context.Background(), st.GetTaskID()))
	require.Zero(t, discoveryPending(f.si.discovery))
	require.True(t, f.mt.statsTaskMeta.HasStatsTask(1, st.GetSubJobType()))
	f.catalog.failDrop.Store(false)
	require.NoError(t, f.mt.statsTaskMeta.DropStatsTask(context.Background(), st.GetTaskID()))
	require.Equal(t, 1, discoveryPending(f.si.discovery))
	f.si.Start()
	f.waitTasks(t, 2)
	require.NotEqual(t, st.GetTaskID(), f.mt.statsTaskMeta.GetStatsTaskBySegmentID(1, st.GetSubJobType()).GetTaskID())
}

func TestStatsDiscoveryMetadataPublication(t *testing.T) {
	f := newDiscoveryFixture(t, "event")
	q := f.si.discovery
	segment := discoverySegment(1, false)
	f.catalog.metadataErr = merr.WrapErrServiceInternalMsg("injected")
	require.Error(t, f.mt.AddSegment(context.Background(), segment))
	require.Zero(t, discoveryPending(q), "failed persistence must not notify")
	f.catalog.metadataErr = nil
	require.NoError(t, f.mt.AddSegment(context.Background(), segment))
	require.Equal(t, 2, discoveryPending(q))
	drainDiscovery(q)
	require.NoError(t, f.mt.UpdateSegmentsInfo(context.Background(), func(p *updateSegmentPack) bool { p.Get(1).NumOfRows++; return true }))
	require.Zero(t, discoveryPending(q), "unrelated statistics do not cause discovery")
	f.catalog.metadataErr = merr.WrapErrServiceInternalMsg("injected")
	require.Error(t, f.mt.SetState(context.Background(), 1, commonpb.SegmentState_Dropped))
	require.Zero(t, discoveryPending(q))
	f.catalog.metadataErr = nil
	require.NoError(t, f.mt.DropSegmentsOfPartition(context.Background(), []int64{2}))
	require.Equal(t, 2, discoveryPending(q))
	result, err := f.si.reconcileStats(statsReconcileKey{1, indexpb.StatsSubJob_TextIndexJob}, make(map[int64]statsFieldRules))
	require.NoError(t, err)
	require.Equal(t, statsNotNeeded, result)
	require.NoError(t, f.mt.DropSegment(context.Background(), 1))
	f.mt.notifyStatsChange(nil, segment) // late events reread the current metadata.
	result, err = f.si.reconcileStats(statsReconcileKey{1, indexpb.StatsSubJob_TextIndexJob}, make(map[int64]statsFieldRules))
	require.NoError(t, err)
	require.Equal(t, statsNotNeeded, result)
	require.Nil(t, f.mt.GetHealthySegment(context.Background(), 1))
}

func TestStatsDiscoveryShadowAndPoll(t *testing.T) {
	for _, mode := range []string{"shadow", "poll"} {
		t.Run(mode, func(t *testing.T) {
			f := newDiscoveryFixture(t, mode)
			require.NoError(t, f.mt.AddSegment(context.Background(), discoverySegment(1, true)))
			if mode == "poll" {
				require.Nil(t, f.si.discovery)
				require.Nil(t, f.mt.statsDiscovery.Load())
			} else {
				result, err := f.si.reconcileStats(statsReconcileKey{1, indexpb.StatsSubJob_TextIndexJob}, make(map[int64]statsFieldRules))
				require.NoError(t, err)
				require.Equal(t, statsWouldSubmit, result)
				require.Zero(t, f.alloc.calls.Load())
				require.Zero(t, f.mt.statsTaskMeta.tasks.Len())
				require.Zero(t, f.scheduler.enqueued.Load())
			}
		})
	}
}

func TestStatsDiscoveryStreamingScan(t *testing.T) {
	f := newDiscoveryFixture(t, "event")
	for id := int64(1); id <= 1000; id++ {
		f.mt.segments.SetSegment(id, discoverySegment(id, false))
	}
	for id := int64(1); id <= 900; id++ {
		f.mt.segments.DropSegment(id)
	}
	next, stop := iter.Pull2(f.mt.rangeStatsSegments(0))
	_, _, ok := next()
	require.True(t, ok)
	// A suspended iterator must not hold a metadata lock needed by writers.
	done := make(chan struct{})
	go func() {
		f.mt.segMu.Lock()
		f.mt.setSegmentAndNotifyStats(2000, discoverySegment(2000, true))
		f.mt.segMu.Unlock()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("suspended scan blocked a writer")
	}
	stop()
	_, _, ok = next()
	require.False(t, ok)
	// Count only what the scan below enqueues, not segment 2000's notification.
	drainDiscovery(f.si.discovery)
	f.si.discoveryOptions.scanBatchSize = 3
	f.si.discovery.requestScan(0, false)
	var cursors []*statsScanCursor
	round := 0
	f.si.advanceStatsScans(&cursors, &round)
	require.Len(t, cursors, 1, "a scan must stop after its configured batch budget")
	require.LessOrEqual(t, discoveryPending(f.si.discovery), 6)
	for _, cursor := range cursors {
		cursor.stop()
	}
}

func TestStatsDiscoveryStopAndRestart(t *testing.T) {
	f := newDiscoveryFixture(t, "event")
	require.NoError(t, f.mt.AddSegment(context.Background(), discoverySegment(1, true)))
	f.si.Start()
	f.waitTasks(t, 2)
	f.si.Stop()
	oldQueue := f.si.discovery
	require.Nil(t, f.mt.statsDiscovery.Load())
	require.Empty(t, f.si.discoveryUnwatch)
	require.NoError(t, Params.Save(Params.CommonCfg.EnabledJSONKeyStats.Key, "false"))
	require.Empty(t, oldQueue.scopes, "old config callbacks must be removed")
	require.NoError(t, Params.Save(Params.CommonCfg.EnabledJSONKeyStats.Key, "true"))
	// Reload tasks as a new coordinator would, without sharing in-memory dedup state.
	reloaded, err := newStatsTaskMeta(context.Background(), f.catalog)
	require.NoError(t, err)
	f.mt.statsTaskMeta = reloaded
	next := newStatsInspector(context.Background(), f.mt, f.scheduler, f.alloc, nil, nil, newIndexEngineVersionManager())
	next.discoveryOptions.scanInterval = time.Millisecond
	t.Cleanup(next.Stop)
	next.Start()
	require.Eventually(t, func() bool { return f.scheduler.enqueued.Load() == 4 }, time.Second, time.Millisecond)
	require.EqualValues(t, 2, f.alloc.calls.Load(), "recovery must reuse persisted task IDs")
	f.si.Stop() // stopping an old inspector must not detach the replacement.
	require.Same(t, next.discovery, f.mt.statsDiscovery.Load())
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() { defer wg.Done(); next.Start(); next.Stop() }()
	}
	wg.Wait()
	require.Nil(t, f.mt.statsDiscovery.Load())
}

func TestStatsDiscoveryEligibility(t *testing.T) {
	cases := []struct {
		name     string
		job      indexpb.StatsSubJob
		external bool
		change   func(*datapb.SegmentInfo)
		want     statsSubmitResult
	}{
		{"text", indexpb.StatsSubJob_TextIndexJob, false, func(*datapb.SegmentInfo) {}, statsWouldSubmit},
		{"unsorted", indexpb.StatsSubJob_TextIndexJob, false, func(s *datapb.SegmentInfo) { s.IsSorted = false }, statsNotNeeded},
		{"namespace_sorted", indexpb.StatsSubJob_TextIndexJob, false, func(s *datapb.SegmentInfo) { s.IsSorted = false; s.IsSortedByNamespace = true }, statsWouldSubmit},
		{"l0", indexpb.StatsSubJob_TextIndexJob, false, func(s *datapb.SegmentInfo) { s.Level = datapb.SegmentLevel_L0 }, statsNotNeeded},
		{"dropped", indexpb.StatsSubJob_TextIndexJob, false, func(s *datapb.SegmentInfo) { s.State = commonpb.SegmentState_Dropped }, statsNotNeeded},
		{"json", indexpb.StatsSubJob_JsonKeyIndexJob, false, func(*datapb.SegmentInfo) {}, statsWouldSubmit},
		{"json_current", indexpb.StatsSubJob_JsonKeyIndexJob, false, func(s *datapb.SegmentInfo) {
			s.JsonKeyStats = map[int64]*datapb.JsonKeyStats{102: {JsonKeyStatsDataFormat: common.JSONStatsDataFormatVersion}}
		}, statsNotNeeded},
		{"external_text", indexpb.StatsSubJob_TextIndexJob, true, func(s *datapb.SegmentInfo) { s.IsSorted = false }, statsWouldSubmit},
		{"external_json_v2", indexpb.StatsSubJob_JsonKeyIndexJob, true, func(s *datapb.SegmentInfo) { s.StorageVersion = storage.StorageV2 }, statsNotNeeded},
		{"external_json_v3", indexpb.StatsSubJob_JsonKeyIndexJob, true, func(s *datapb.SegmentInfo) {
			s.IsSorted = false
			s.StorageVersion = storage.StorageV3
			s.ManifestPath = "manifest/1"
		}, statsWouldSubmit},
		{"sort_disabled", indexpb.StatsSubJob_Sort, false, func(*datapb.SegmentInfo) {}, statsNotNeeded},
		{"bm25_disabled", indexpb.StatsSubJob_BM25Job, false, func(*datapb.SegmentInfo) {}, statsNotNeeded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newDiscoveryFixture(t, "shadow")
			if tc.external {
				col := f.mt.GetClonedCollectionInfo(1)
				col.Schema.ExternalSource = "s3://test"
				for _, field := range col.Schema.Fields {
					field.ExternalField = "external_column"
				}
				f.mt.AddCollection(col)
			}
			segment := discoverySegment(1, true)
			tc.change(segment.SegmentInfo)
			f.mt.segments.SetSegment(1, segment)
			got, err := f.si.reconcileStats(statsReconcileKey{1, tc.job}, make(map[int64]statsFieldRules))
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			if statsJobSlot(tc.job) >= 0 {
				f.si.triggerStatsTasks(0)
				require.Equal(t, tc.want == statsWouldSubmit, f.mt.statsTaskMeta.HasStatsTask(1, tc.job),
					"legacy and event discovery must agree on the same metadata")
			}
		})
	}
}

func (c *discoveryTestCatalog) AddSegment(context.Context, *datapb.SegmentInfo) error {
	return c.metadataErr
}

func (c *discoveryTestCatalog) AlterSegments(context.Context, []*datapb.SegmentInfo, ...metastore.BinlogsIncrement) error {
	return c.metadataErr
}

func (c *discoveryTestCatalog) DropSegment(context.Context, *datapb.SegmentInfo) error {
	return c.metadataErr
}

func (c *discoveryTestCatalog) SaveDroppedSegmentsInBatch(context.Context, []*datapb.SegmentInfo) error {
	return c.metadataErr
}

func (c *discoveryTestCatalog) Update(context.Context, ...metastore.UpdateAction) error {
	return c.metadataErr
}

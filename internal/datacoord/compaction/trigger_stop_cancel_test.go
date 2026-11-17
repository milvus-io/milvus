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

package compaction

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/policy"
	ctask "github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metacache"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	rootmocks "github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type recordingInspector struct {
	mu    sync.Mutex
	tasks []*datapb.CompactionTask
	full  bool
}

func (r *recordingInspector) Start()                                            {}
func (r *recordingInspector) Stop()                                             {}
func (r *recordingInspector) RemoveTasksByChannel(string)                       {}
func (r *recordingInspector) GetCompactionTasksNumBySignalID(int64) int         { return 0 }
func (r *recordingInspector) getCompactionTasksNum(...compactionTaskFilter) int { return 0 }
func (r *recordingInspector) checkAndSetSegmentStating(string, int64) bool      { return false }
func (r *recordingInspector) isFull() bool                                      { return r.full }
func (r *recordingInspector) GetCompactionInfo(context.Context, int64) *policy.CompactionInfo {
	return nil
}

func (r *recordingInspector) EnqueueCompaction(task *datapb.CompactionTask) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tasks = append(r.tasks, task)
	return nil
}

func (r *recordingInspector) snapshot() []*datapb.CompactionTask {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*datapb.CompactionTask, len(r.tasks))
	copy(out, r.tasks)
	return out
}

func newFuncHandler(t *testing.T, fn func(ctx context.Context, collectionID UniqueID) (*meta.CollectionInfo, error)) meta.Handler {
	t.Helper()
	h := meta.NewNMockHandler(t)
	h.EXPECT().GetCollection(mock.Anything, mock.Anything).RunAndReturn(fn).Maybe()
	return h
}

func buildTestMetaWithCandidateGroups(t *testing.T, groupCount int, level datapb.SegmentLevel) (*meta.Meta, *index.IndexMeta, map[int64]*meta.CollectionInfo) {
	t.Helper()
	paramtable.Init()

	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Maybe()
	metaStore := metacache.NewMetaStore(catalog)

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  101,
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "128"},
				},
			},
		},
	}

	im := &index.IndexMeta{
		SegmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		Indexes:        make(map[UniqueID]map[UniqueID]*model.Index),
	}

	segments := make(map[int64]*meta.SegmentInfo, groupCount)
	collections := make(map[int64]*meta.CollectionInfo, groupCount)

	for i := 0; i < groupCount; i++ {
		collID := int64(1001 + i)
		segID := int64(5001 + i)
		indexID := int64(9001 + i)
		collInfo := &meta.CollectionInfo{
			ID:     collID,
			Schema: schema,
		}
		collections[collID] = collInfo
		metaStore.PutCollection(collInfo)

		im.Indexes[collID] = map[UniqueID]*model.Index{
			indexID: {
				CollectionID: collID,
				FieldID:      101,
				IndexID:      indexID,
			},
		}
		segIdxMap := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		segIdxMap.Insert(indexID, &model.SegmentIndex{
			SegmentID:    segID,
			CollectionID: collID,
			PartitionID:  1,
			IndexID:      indexID,
			BuildID:      indexID,
			IndexState:   commonpb.IndexState_Finished,
		})
		im.SegmentIndexes.Insert(segID, segIdxMap)

		// Delete ratio = 50 / 100 = 0.5 (>= SingleCompactionRatioThreshold 0.2),
		// so global non-force signals also select every candidate segment.
		segments[segID] = &meta.SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:             segID,
				CollectionID:   collID,
				PartitionID:    1,
				LastExpireTime: 100,
				NumOfRows:      100,
				MaxRowNum:      300,
				InsertChannel:  fmt.Sprintf("ch-%d", i),
				State:          commonpb.SegmentState_Flushed,
				Level:          level,
				IsSorted:       true,
				Binlogs: []*datapb.FieldBinlog{{
					FieldID: 101,
					Binlogs: []*datapb.Binlog{{EntriesNum: 100, LogID: segID, MemorySize: 1024}},
				}},
				Deltalogs: []*datapb.FieldBinlog{{
					Binlogs: []*datapb.Binlog{{EntriesNum: 50, LogID: segID + 10000, MemorySize: 512}},
				}},
			},
		}
	}

	m := &meta.Meta{
		Segments:  meta.NewSegmentsInfoWithSegments(segments),
		MetaStore: metaStore,
	}
	return m, im, collections
}

func newIncrementingRootCoordAllocator(t *testing.T) (*rootmocks.MixCoord, allocator.Allocator) {
	t.Helper()
	var nextID atomic.Int64
	nextID.Store(10000)

	mc := rootmocks.NewMixCoord(t)
	mc.EXPECT().AllocID(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		count := int64(req.GetCount())
		if count <= 0 {
			count = 1
		}
		start := nextID.Add(count) - count
		return &rootcoordpb.AllocIDResponse{
			Status: merr.Success(),
			ID:     start,
			Count:  uint32(count),
		}, nil
	}).Maybe()
	return mc, allocator.NewRootCoordAllocator(mc)
}

func TestCompactionTriggerStopCancelsGlobalBatchAndNotifiesWaiters(t *testing.T) {
	pt := paramtable.Get()
	pt.Save(pt.DataCoordCfg.IndexBasedCompaction.Key, "false")
	defer pt.Reset(pt.DataCoordCfg.IndexBasedCompaction.Key)

	const groupCount = 20
	m, im, _ := buildTestMetaWithCandidateGroups(t, groupCount, datapb.SegmentLevel_L1)

	// Pre-populate an already-persisted compaction task in CompactionTaskMeta to verify
	// stopping trigger planning leaves durable recovery state untouched.
	taskMeta := ctask.NewCompactionTaskMetaOverCatalog(nil)
	persistedTask := &datapb.CompactionTask{
		PlanID:        777,
		TriggerID:     888,
		CollectionID:  1001,
		State:         datapb.CompactionTaskState_pipelining,
		Type:          datapb.CompactionType_MixCompaction,
		InputSegments: []int64{42},
	}
	taskMeta.SaveCompactionTaskMemory(persistedTask)

	var lookupCalls atomic.Int32
	var enteredOnce sync.Once
	enteredFirstLookup := make(chan struct{})
	firstLookupErr := make(chan error, 1)

	handler := newFuncHandler(t, func(ctx context.Context, _ UniqueID) (*meta.CollectionInfo, error) {
		callNum := lookupCalls.Add(1)
		enteredOnce.Do(func() {
			close(enteredFirstLookup)
		})
		<-ctx.Done()
		if callNum == 1 {
			firstLookupErr <- ctx.Err()
		}
		return nil, ctx.Err()
	})

	_, alloc := newIncrementingRootCoordAllocator(t)
	inspector := &recordingInspector{}

	tr := NewCompactionTrigger(m, im, inspector, alloc, handler, index.NewTestVersionManager())
	tr.testingOnly = true // disable periodic ticker; drive deterministic global signal directly
	tr.Start()

	// Caller 1: global compaction signal (CollectionID == 0) with waitResult == true.
	caller1Done := make(chan error, 1)
	go func() {
		_, err := tr.TriggerCompaction(context.Background(), NewCompactionSignal().WithIsForce(true).WithWaitResult(true))
		caller1Done <- err
	}()

	select {
	case <-enteredFirstLookup:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first candidate group's GetCollection lookup")
	}

	// Caller 2: another waiting signal queued while work() is blocked on Caller 1's group 1 lookup.
	caller2Allocated := make(chan struct{})
	caller2Done := make(chan error, 1)
	go func() {
		sig := NewCompactionSignal().WithCollectionID(1002).WithIsForce(true).WithWaitResult(true)
		// Send via TriggerCompaction: signal ID allocation succeeds, signal queues in manualSignals,
		// and TriggerCompaction blocks waiting on sig.resultCh until Stop() wakes it.
		close(caller2Allocated)
		_, err := tr.TriggerCompaction(context.Background(), sig)
		caller2Done <- err
	}()

	<-caller2Allocated
	// Give caller 2 a brief moment to enqueue into manualSignals and wait on resultCh;
	// even if Stop() races before enqueue, TriggerCompaction must still return ErrServiceUnavailable promptly.
	time.Sleep(10 * time.Millisecond)

	stopStart := time.Now()
	stopDone := make(chan struct{})
	go func() {
		tr.Stop()
		close(stopDone)
	}()

	select {
	case err := <-firstLookupErr:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(1 * time.Second):
		t.Fatal("in-flight GetCollection lookup did not observe Stop() cancellation")
	}

	select {
	case <-stopDone:
		// Without lifecycle cancellation, 20 groups * 1s per-group timeout would take ~20s.
		require.Less(t, time.Since(stopStart), 800*time.Millisecond)
	case <-time.After(2 * time.Second):
		t.Fatal("compactionTrigger.Stop() hung waiting for batch planning")
	}

	// Remaining 19 candidate groups must NOT be attempted after Stop().
	require.Equal(t, int32(1), lookupCalls.Load())

	// Both waiting callers must be notified promptly rather than hanging on resultCh.
	select {
	case err := <-caller1Done:
		require.Error(t, err)
		require.True(t, merr.IsCanceledOrTimeout(err) || errorsIsServiceUnavailable(err), "unexpected caller1 error: %v", err)
	case <-time.After(1 * time.Second):
		t.Fatal("active TriggerCompaction caller hung after Stop()")
	}

	select {
	case err := <-caller2Done:
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	case <-time.After(1 * time.Second):
		t.Fatal("queued TriggerCompaction caller hung after Stop()")
	}

	// Subsequent TriggerCompaction calls after Stop() must fail immediately with typed error.
	_, postStopErr := tr.TriggerCompaction(context.Background(), NewCompactionSignal().WithIsForce(true))
	require.ErrorIs(t, postStopErr, merr.ErrServiceUnavailable)

	// Zero new compaction tasks may be enqueued after Stop().
	require.Empty(t, inspector.snapshot())

	// Pre-existing persisted compaction tasks in CompactionTaskMeta (indexed by TriggerID) remain intact for recovery.
	recoveredTasks := taskMeta.GetCompactionTasks()[888]
	require.Len(t, recoveredTasks, 1)
	require.Equal(t, int64(777), recoveredTasks[0].GetPlanID())
}

func errorsIsServiceUnavailable(err error) bool {
	return err != nil && (merr.Code(err) == merr.Code(merr.ErrServiceUnavailable))
}

func TestCompactionTriggerStopCancelsIndexFilterAndIDAllocation(t *testing.T) {
	t.Run("cancels_index_based_segment_filter_mid_batch", func(t *testing.T) {
		pt := paramtable.Get()
		pt.Save(pt.DataCoordCfg.IndexBasedCompaction.Key, "true")
		defer pt.Reset(pt.DataCoordCfg.IndexBasedCompaction.Key)

		m, im, _ := buildTestMetaWithCandidateGroups(t, 10, datapb.SegmentLevel_L1)
		var filterLookupCalls atomic.Int32
		enteredFilterLookup := make(chan struct{})
		var enteredOnce sync.Once

		handler := newFuncHandler(t, func(ctx context.Context, _ UniqueID) (*meta.CollectionInfo, error) {
			// In handleSignal, IndexBasedCompaction runs FilterInIndexedSegments before getCollection.
			filterLookupCalls.Add(1)
			enteredOnce.Do(func() {
				close(enteredFilterLookup)
			})
			<-ctx.Done()
			return nil, ctx.Err()
		})

		_, alloc := newIncrementingRootCoordAllocator(t)
		inspector := &recordingInspector{}

		tr := NewCompactionTrigger(m, im, inspector, alloc, handler, index.NewTestVersionManager())
		tr.testingOnly = true
		tr.Start()

		done := make(chan error, 1)
		go func() {
			_, err := tr.TriggerCompaction(context.Background(), NewCompactionSignal().WithIsForce(true).WithWaitResult(true))
			done <- err
		}()

		select {
		case <-enteredFilterLookup:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for FilterInIndexedSegments GetCollection call")
		}

		tr.Stop()

		select {
		case err := <-done:
			require.Error(t, err)
		case <-time.After(1 * time.Second):
			t.Fatal("TriggerCompaction did not unblock after Stop() during FilterInIndexedSegments")
		}

		require.Equal(t, int32(1), filterLookupCalls.Load())
		require.Empty(t, inspector.snapshot())
	})

	t.Run("cancels_plan_id_allocation_and_skips_enqueue", func(t *testing.T) {
		m, im, collections := buildTestMetaWithCandidateGroups(t, 5, datapb.SegmentLevel_L1)
		handler := newFuncHandler(t, func(ctx context.Context, collID UniqueID) (*meta.CollectionInfo, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return collections[collID], nil
		})

		var allocCalls atomic.Int32
		enteredPlanAlloc := make(chan struct{})
		var enteredOnce sync.Once
		mc := rootmocks.NewMixCoord(t)
		mc.EXPECT().AllocID(mock.Anything, mock.Anything).RunAndReturn(func(rpcCtx context.Context, req *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error) {
			call := allocCalls.Add(1)
			// Call 1 is signal ID allocation (count == 1); call 2 is batch plan ID allocation in handleSignal.
			if call == 1 {
				return &rootcoordpb.AllocIDResponse{
					Status: merr.Success(),
					ID:     1000,
					Count:  1,
				}, nil
			}
			enteredOnce.Do(func() {
				close(enteredPlanAlloc)
			})
			<-rpcCtx.Done()
			return nil, rpcCtx.Err()
		})

		alloc := allocator.NewRootCoordAllocator(mc)
		inspector := &recordingInspector{}

		tr := NewCompactionTrigger(m, im, inspector, alloc, handler, index.NewTestVersionManager())
		tr.testingOnly = true
		tr.Start()

		done := make(chan error, 1)
		go func() {
			_, err := tr.TriggerCompaction(context.Background(), NewCompactionSignal().WithIsForce(true).WithWaitResult(true))
			done <- err
		}()

		select {
		case <-enteredPlanAlloc:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for plan ID allocation RPC")
		}

		tr.Stop()

		select {
		case err := <-done:
			require.Error(t, err)
		case <-time.After(1 * time.Second):
			t.Fatal("TriggerCompaction did not unblock after Stop() during plan ID allocation")
		}

		// Plan ID allocation was attempted only for group 1; groups 2..5 were skipped.
		require.Equal(t, int32(2), allocCalls.Load())
		require.Empty(t, inspector.snapshot())
	})
}

func TestCompactionTriggerGlobalSignalControls(t *testing.T) {
	t.Run("single_group_error_continues_remaining_groups_when_not_stopping", func(t *testing.T) {
		pt := paramtable.Get()
		pt.Save(pt.DataCoordCfg.IndexBasedCompaction.Key, "false")
		defer pt.Reset(pt.DataCoordCfg.IndexBasedCompaction.Key)

		const groupCount = 4
		m, im, collections := buildTestMetaWithCandidateGroups(t, groupCount, datapb.SegmentLevel_L1)

		var calls atomic.Int32
		handler := newFuncHandler(t, func(ctx context.Context, collID UniqueID) (*meta.CollectionInfo, error) {
			require.NoError(t, ctx.Err())
			calls.Add(1)
			if collID == 1001 {
				return nil, context.DeadlineExceeded
			}
			return collections[collID], nil
		})

		_, alloc := newIncrementingRootCoordAllocator(t)
		inspector := &recordingInspector{}

		tr := NewCompactionTrigger(m, im, inspector, alloc, handler, index.NewTestVersionManager())
		tr.testingOnly = true
		tr.Start()
		defer tr.Stop()

		_, err := tr.TriggerCompaction(context.Background(), NewCompactionSignal().WithIsForce(true).WithWaitResult(true))
		require.NoError(t, err)
		require.Equal(t, int32(groupCount), calls.Load())
		require.Len(t, inspector.snapshot(), groupCount-1)
	})

	t.Run("normal_global_batch_plans_all_eligible_groups", func(t *testing.T) {
		const groupCount = 4
		m, im, collections := buildTestMetaWithCandidateGroups(t, groupCount, datapb.SegmentLevel_L1)

		handler := newFuncHandler(t, func(ctx context.Context, collID UniqueID) (*meta.CollectionInfo, error) {
			require.NoError(t, ctx.Err())
			return collections[collID], nil
		})

		_, alloc := newIncrementingRootCoordAllocator(t)
		inspector := &recordingInspector{}

		tr := NewCompactionTrigger(m, im, inspector, alloc, handler, index.NewTestVersionManager())
		tr.testingOnly = true
		tr.Start()
		defer tr.Stop()

		_, err := tr.TriggerCompaction(context.Background(), NewCompactionSignal().WithIsForce(true).WithWaitResult(true))
		require.NoError(t, err)
		require.Len(t, inspector.snapshot(), groupCount)
	})
}

func TestCompactionTriggerManagerStopCancelsPolicySweepAndManualTrigger(t *testing.T) {
	const groupCount = 10
	m, im, collections := buildTestMetaWithCandidateGroups(t, groupCount, datapb.SegmentLevel_L2)

	var lookupCalls atomic.Int32
	enteredLookup := make(chan struct{})
	var enteredOnce sync.Once

	handler := newFuncHandler(t, func(ctx context.Context, collID UniqueID) (*meta.CollectionInfo, error) {
		call := lookupCalls.Add(1)
		if call == 1 {
			enteredOnce.Do(func() {
				close(enteredLookup)
			})
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return collections[collID], nil
	})

	_, alloc := newIncrementingRootCoordAllocator(t)
	inspector := &recordingInspector{}

	mgr := NewCompactionTriggerManager(alloc, handler, inspector, m, im, nil, nil, nil, index.NewTestVersionManager())

	sweepDone := make(chan struct{})
	go func() {
		mgr.handleTicker(mgr.lifecycleCtx(), SingleTicker)
		close(sweepDone)
	}()

	select {
	case <-enteredLookup:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for SingleCompactionPolicy GetCollection lookup")
	}

	mgr.Stop()

	select {
	case <-sweepDone:
	case <-time.After(1 * time.Second):
		t.Fatal("CompactionTriggerManager.handleTicker did not abort on Stop()")
	}

	// Only the first collection was attempted; remaining 9 collections were skipped.
	require.Equal(t, int32(1), lookupCalls.Load())
	require.Empty(t, inspector.snapshot())

	// Subsequent ManualTrigger after Stop() must reject new work immediately.
	_, err := mgr.ManualTrigger(context.Background(), &milvuspb.ManualCompactionRequest{
		CollectionID: 1001,
		L0Compaction: true,
	})
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
}

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
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
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
	mu           sync.Mutex
	tasks        []*datapb.CompactionTask
	full         bool
	onEnqueue    func(*datapb.CompactionTask) error
	onEnqueueCtx func(context.Context, *datapb.CompactionTask) error
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
	return r.EnqueueCompactionWithContext(context.Background(), task)
}

func (r *recordingInspector) EnqueueCompactionWithContext(ctx context.Context, task *datapb.CompactionTask) error {
	r.mu.Lock()
	cbCtx := r.onEnqueueCtx
	cb := r.onEnqueue
	r.mu.Unlock()
	if cbCtx != nil {
		if err := cbCtx(ctx, task); err != nil {
			return err
		}
	} else if cb != nil {
		if err := cb(task); err != nil {
			return err
		}
	}
	r.mu.Lock()
	r.tasks = append(r.tasks, task)
	r.mu.Unlock()
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
		releaseFilterLookup := make(chan struct{})
		filterLookupErr := make(chan error, 1)
		var enteredOnce sync.Once
		var releaseOnce sync.Once
		t.Cleanup(func() {
			releaseOnce.Do(func() { close(releaseFilterLookup) })
		})

		handler := newFuncHandler(t, func(ctx context.Context, _ UniqueID) (*meta.CollectionInfo, error) {
			// In handleSignal, IndexBasedCompaction runs FilterInIndexedSegments before getCollection.
			callNum := filterLookupCalls.Add(1)
			enteredOnce.Do(func() {
				close(enteredFilterLookup)
			})
			select {
			case <-ctx.Done():
				if callNum == 1 {
					filterLookupErr <- ctx.Err()
				}
				return nil, ctx.Err()
			case <-releaseFilterLookup:
				return nil, context.DeadlineExceeded
			}
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

		stopStart := time.Now()
		stopDone := make(chan struct{})
		go func() {
			tr.Stop()
			close(stopDone)
		}()
		select {
		case <-stopDone:
			require.Less(t, time.Since(stopStart), 800*time.Millisecond)
		case <-time.After(2 * time.Second):
			t.Fatal("compactionTrigger.Stop() hung during FilterInIndexedSegments")
		}

		select {
		case err := <-filterLookupErr:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(1 * time.Second):
			t.Fatal("FilterInIndexedSegments did not observe Stop() context cancellation")
		}

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
		planAllocErr := make(chan error, 1)
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
			planAllocErr <- rpcCtx.Err()
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

		stopStart := time.Now()
		tr.Stop()
		require.Less(t, time.Since(stopStart), 800*time.Millisecond)

		select {
		case err := <-planAllocErr:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(1 * time.Second):
			t.Fatal("plan ID allocation did not observe Stop() context cancellation")
		}

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

func TestCompactionTriggerManagerStopDuringInFlightManualTriggerAfterViewGeneration(t *testing.T) {
	paramtable.Init()

	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Maybe()
	metaStore := metacache.NewMetaStore(catalog)

	const collID = int64(2001)
	collInfo := &meta.CollectionInfo{
		ID:     collID,
		Schema: &schemapb.CollectionSchema{},
	}
	metaStore.PutCollection(collInfo)

	l0Seg := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            6001,
			CollectionID:  collID,
			PartitionID:   1,
			InsertChannel: "ch-l0",
			State:         commonpb.SegmentState_Flushed,
			Level:         datapb.SegmentLevel_L0,
			NumOfRows:     10,
			DmlPosition:   &msgpb.MsgPosition{Timestamp: 50},
		},
	}
	growingSeg := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            6002,
			CollectionID:  collID,
			PartitionID:   1,
			InsertChannel: "ch-l0",
			State:         commonpb.SegmentState_Growing,
			StartPosition: &msgpb.MsgPosition{Timestamp: 100},
		},
	}
	m := &meta.Meta{
		Segments: meta.NewSegmentsInfoWithSegments(map[int64]*meta.SegmentInfo{
			6001: l0Seg,
			6002: growingSeg,
		}),
		MetaStore: metaStore,
	}

	handler := newFuncHandler(t, func(ctx context.Context, _ UniqueID) (*meta.CollectionInfo, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return collInfo, nil
	})

	var allocCalls atomic.Int32
	enteredSubmitAlloc := make(chan struct{})
	var enteredOnce sync.Once
	mc := rootmocks.NewMixCoord(t)
	mc.EXPECT().AllocID(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, _ *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error) {
		call := allocCalls.Add(1)
		// Call 1 allocates triggerID in l0Policy.TriggerOneCollection (generating the L0 view).
		// Call 2 runs inside notify() -> SubmitL0ViewToScheduler after views are generated.
		if call == 1 {
			return &rootcoordpb.AllocIDResponse{
				Status: merr.Success(),
				ID:     9001,
				Count:  1,
			}, nil
		}
		enteredOnce.Do(func() {
			close(enteredSubmitAlloc)
		})
		<-ctx.Done()
		return nil, ctx.Err()
	})

	alloc := allocator.NewRootCoordAllocator(mc)
	inspector := &recordingInspector{}
	mgr := NewCompactionTriggerManager(alloc, handler, inspector, m, nil, nil, nil, nil, index.NewTestVersionManager())

	type manualResult struct {
		triggerID int64
		err       error
	}
	resCh := make(chan manualResult, 1)
	go func() {
		// Keep the caller's own RPC ctx alive so only Stop() cancels the merged ctx.
		id, err := mgr.ManualTrigger(context.Background(), &milvuspb.ManualCompactionRequest{
			CollectionID: collID,
			L0Compaction: true,
		})
		resCh <- manualResult{triggerID: id, err: err}
	}()

	select {
	case <-enteredSubmitAlloc:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for SubmitL0ViewToScheduler AllocID entry barrier")
	}

	mgr.Stop()

	select {
	case res := <-resCh:
		require.ErrorIs(t, res.err, merr.ErrServiceUnavailable)
		require.Zero(t, res.triggerID)
	case <-time.After(1 * time.Second):
		t.Fatal("ManualTrigger did not return after Stop()")
	}

	require.Empty(t, inspector.snapshot())
}

func TestManualTriggerReturnsTriggerIDWhenEnqueuedBeforeStop(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.DataCoordCfg.EnableAutoCompaction.Key, "true")
	defer pt.Reset(pt.DataCoordCfg.EnableAutoCompaction.Key)
	pt.Save(pt.DataCoordCfg.EnableCompaction.Key, "true")
	defer pt.Reset(pt.DataCoordCfg.EnableCompaction.Key)

	const collID = int64(2002)
	const channel = "by-dev-rootcoord-dml_1_2002v0"

	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Maybe()
	metaStore := metacache.NewMetaStore(catalog)

	collInfo := &meta.CollectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			},
		},
	}
	metaStore.PutCollection(collInfo)
	l0Seg := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            7001,
			CollectionID:  collID,
			PartitionID:   10,
			InsertChannel: channel,
			State:         commonpb.SegmentState_Flushed,
			Level:         datapb.SegmentLevel_L0,
			NumOfRows:     10,
			DmlPosition:   &msgpb.MsgPosition{Timestamp: 50},
			Deltalogs:     []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{EntriesNum: 10, MemorySize: 1024}}}},
		},
	}
	growingSeg := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            7002,
			CollectionID:  collID,
			PartitionID:   10,
			InsertChannel: channel,
			State:         commonpb.SegmentState_Growing,
			StartPosition: &msgpb.MsgPosition{Timestamp: 100},
		},
	}
	m := &meta.Meta{
		Segments: meta.NewSegmentsInfoWithSegments(map[int64]*meta.SegmentInfo{
			7001: l0Seg,
			7002: growingSeg,
		}),
		MetaStore: metaStore,
	}

	handler := newFuncHandler(t, func(ctx context.Context, _ UniqueID) (*meta.CollectionInfo, error) {
		return collInfo, nil
	})

	_, alloc := newIncrementingRootCoordAllocator(t)
	enqueuedCh := make(chan struct{})
	releaseEnqueue := make(chan struct{})
	inspector := &recordingInspector{
		onEnqueue: func(_ *datapb.CompactionTask) error {
			close(enqueuedCh)
			<-releaseEnqueue
			return nil
		},
	}
	mgr := NewCompactionTriggerManager(alloc, handler, inspector, m, nil, nil, nil, nil, index.NewTestVersionManager())

	type manualResult struct {
		triggerID int64
		err       error
	}
	resCh := make(chan manualResult, 1)
	go func() {
		id, err := mgr.ManualTrigger(context.Background(), &milvuspb.ManualCompactionRequest{
			CollectionID: collID,
			L0Compaction: true,
		})
		resCh <- manualResult{triggerID: id, err: err}
	}()

	select {
	case <-enqueuedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for EnqueueCompaction")
	}

	mgr.Stop()
	close(releaseEnqueue)

	select {
	case res := <-resCh:
		require.NoError(t, res.err)
		require.NotZero(t, res.triggerID)
	case <-time.After(1 * time.Second):
		t.Fatal("ManualTrigger did not return after Stop()")
	}

	require.Len(t, inspector.snapshot(), 1)
}

func TestCompactionTriggerReturnsSignalIDWhenEnqueuedBeforeStop(t *testing.T) {
	pt := paramtable.Get()
	pt.Save(pt.DataCoordCfg.IndexBasedCompaction.Key, "false")
	defer pt.Reset(pt.DataCoordCfg.IndexBasedCompaction.Key)

	m, im, colls := buildTestMetaWithCandidateGroups(t, 3, datapb.SegmentLevel_L1)

	handler := newFuncHandler(t, func(ctx context.Context, collID UniqueID) (*meta.CollectionInfo, error) {
		return colls[collID], nil
	})

	_, alloc := newIncrementingRootCoordAllocator(t)
	enqueuedFirst := make(chan struct{})
	releaseFirst := make(chan struct{})
	var firstOnce sync.Once
	inspector := &recordingInspector{
		onEnqueue: func(_ *datapb.CompactionTask) error {
			firstOnce.Do(func() {
				close(enqueuedFirst)
				<-releaseFirst
			})
			return nil
		},
	}

	tr := NewCompactionTrigger(m, im, inspector, alloc, handler, index.NewTestVersionManager())
	tr.testingOnly = true
	tr.Start()
	defer tr.Stop()

	type triggerRes struct {
		id  UniqueID
		err error
	}
	resCh := make(chan triggerRes, 1)
	go func() {
		id, err := tr.TriggerCompaction(context.Background(), NewCompactionSignal().WithIsForce(true).WithWaitResult(true))
		resCh <- triggerRes{id: id, err: err}
	}()

	select {
	case <-enqueuedFirst:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first bucket EnqueueCompaction")
	}

	stopDone := make(chan struct{})
	go func() {
		tr.Stop()
		close(stopDone)
	}()

	// Wait until Stop() has canceled lifecycleCtx before releasing the in-flight EnqueueCompaction.
	require.Eventually(t, func() bool {
		return tr.isStopped()
	}, 2*time.Second, time.Millisecond)

	close(releaseFirst)

	select {
	case <-stopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("compactionTrigger.Stop() did not finish")
	}

	select {
	case res := <-resCh:
		require.NoError(t, res.err)
		require.Greater(t, res.id, UniqueID(0))
	case <-time.After(1 * time.Second):
		t.Fatal("TriggerCompaction did not return after Stop()")
	}

	// Only the first task should be enqueued; remaining groups stop planning cleanly.
	require.Len(t, inspector.snapshot(), 1)
}

func TestCompactionTriggerLateEnqueueAfterWorkDoneReturnsServiceUnavailable(t *testing.T) {
	m, im, colls := buildTestMetaWithCandidateGroups(t, 1, datapb.SegmentLevel_L1)
	handler := newFuncHandler(t, func(_ context.Context, collID UniqueID) (*meta.CollectionInfo, error) {
		return colls[collID], nil
	})
	_, alloc := newIncrementingRootCoordAllocator(t)

	tr := NewCompactionTrigger(m, im, &recordingInspector{}, alloc, handler, index.NewTestVersionManager())
	tr.testingOnly = true
	tr.Start()
	// Ensure work() has exited, run drainSignals(), and closed workDone before running result wait checks.
	tr.Stop()

	stoppedErr := merr.WrapErrServiceUnavailable("compaction trigger is stopped")

	t.Run("unconsumed_signal_with_empty_resultCh_returns_ServiceUnavailable", func(t *testing.T) {
		sig := NewCompactionSignal().WithID(1000).WithIsForce(true).WithWaitResult(true)
		tr.manualSignals <- sig

		type waitRes struct {
			id  UniqueID
			err error
		}
		resCh := make(chan waitRes, 1)
		go func() {
			id, err := tr.awaitSignalResult(context.Background(), 1000, sig, stoppedErr)
			resCh <- waitRes{id: id, err: err}
		}()

		select {
		case res := <-resCh:
			require.Equal(t, UniqueID(-1), res.id)
			require.ErrorIs(t, res.err, merr.ErrServiceUnavailable)
		case <-time.After(1 * time.Second):
			t.Fatal("awaitSignalResult hung after workDone was closed")
		}
	})

	t.Run("completed_signal_with_buffered_result_preserves_signalID", func(t *testing.T) {
		sig := NewCompactionSignal().WithID(1001).WithIsForce(true).WithWaitResult(true)
		sig.Notify(nil)

		type waitRes struct {
			id  UniqueID
			err error
		}
		resCh := make(chan waitRes, 1)
		go func() {
			id, err := tr.awaitSignalResult(context.Background(), 1001, sig, stoppedErr)
			resCh <- waitRes{id: id, err: err}
		}()

		select {
		case res := <-resCh:
			require.Equal(t, UniqueID(1001), res.id)
			require.NoError(t, res.err)
		case <-time.After(1 * time.Second):
			t.Fatal("awaitSignalResult did not return buffered result after workDone closed")
		}
	})
}

func TestCompactionTriggerCancelledQueuedManualSignalNotPlanned(t *testing.T) {
	pt := paramtable.Get()
	pt.Save(pt.DataCoordCfg.IndexBasedCompaction.Key, "false")
	defer pt.Reset(pt.DataCoordCfg.IndexBasedCompaction.Key)

	m, im, colls := buildTestMetaWithCandidateGroups(t, 2, datapb.SegmentLevel_L1)

	enteredSignalA := make(chan struct{})
	releaseSignalA := make(chan struct{})
	var enteredOnce sync.Once
	var releaseOnce sync.Once
	releaseA := func() {
		releaseOnce.Do(func() {
			close(releaseSignalA)
		})
	}
	var coll1002Lookups atomic.Int32

	handler := newFuncHandler(t, func(_ context.Context, collID UniqueID) (*meta.CollectionInfo, error) {
		switch collID {
		case 1001:
			enteredOnce.Do(func() {
				close(enteredSignalA)
			})
			<-releaseSignalA
		case 1002:
			coll1002Lookups.Add(1)
		}
		return colls[collID], nil
	})

	_, alloc := newIncrementingRootCoordAllocator(t)
	inspector := &recordingInspector{}

	tr := NewCompactionTrigger(m, im, inspector, alloc, handler, index.NewTestVersionManager())
	tr.testingOnly = true
	tr.Start()
	defer tr.Stop()
	defer releaseA()

	caller1Done := make(chan error, 1)
	go func() {
		_, err := tr.TriggerCompaction(context.Background(), NewCompactionSignal().WithCollectionID(1001).WithIsForce(true).WithWaitResult(true))
		caller1Done <- err
	}()

	select {
	case <-enteredSignalA:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for signal A to enter handleSignal")
	}

	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	type triggerRes struct {
		id  UniqueID
		err error
	}
	caller2Done := make(chan triggerRes, 1)
	go func() {
		sig := NewCompactionSignal().WithCollectionID(1002).WithIsForce(true).WithWaitResult(true)
		id, err := tr.TriggerCompaction(ctxB, sig)
		caller2Done <- triggerRes{id: id, err: err}
	}()

	require.Eventually(t, func() bool {
		return len(tr.manualSignals) == 1
	}, 2*time.Second, time.Millisecond)

	cancelB()

	select {
	case res := <-caller2Done:
		require.Equal(t, UniqueID(-1), res.id)
		require.ErrorIs(t, res.err, context.Canceled)
	case <-time.After(1 * time.Second):
		t.Fatal("queued TriggerCompaction caller did not return after ctx cancel")
	}

	releaseA()

	select {
	case err := <-caller1Done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("signal A did not complete")
	}

	require.Eventually(t, func() bool {
		return len(tr.manualSignals) == 0
	}, 2*time.Second, time.Millisecond)
	tr.Stop()

	require.Zero(t, coll1002Lookups.Load(), "canceled queued manual signal must not be planned")
	for _, task := range inspector.snapshot() {
		require.NotEqual(t, int64(1002), task.GetCollectionID(), "canceled queued manual signal must not enqueue tasks")
	}
}

func TestInspectorEnqueueCompactionWithContextAndRollback(t *testing.T) {
	newTestTask := func(planID, triggerID, collID, segID int64) *datapb.CompactionTask {
		return &datapb.CompactionTask{
			PlanID:        planID,
			TriggerID:     triggerID,
			CollectionID:  collID,
			PartitionID:   1,
			Channel:       "ch-0",
			Type:          datapb.CompactionType_MixCompaction,
			State:         datapb.CompactionTaskState_pipelining,
			InputSegments: []int64{segID},
		}
	}

	t.Run("cancels_AllocTimestamp_and_releases_segments", func(t *testing.T) {
		m, _, _ := buildTestMetaWithCandidateGroups(t, 1, datapb.SegmentLevel_L1)
		catalog := catalogmocks.NewDataCoordCatalog(t)
		taskMeta := ctask.NewCompactionTaskMetaOverCatalog(catalog)

		enteredAllocTS := make(chan struct{})
		allocTSObservedErr := make(chan error, 1)
		mc := rootmocks.NewMixCoord(t)
		mc.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).RunAndReturn(func(rpcCtx context.Context, _ *rootcoordpb.AllocTimestampRequest) (*rootcoordpb.AllocTimestampResponse, error) {
			close(enteredAllocTS)
			<-rpcCtx.Done()
			allocTSObservedErr <- rpcCtx.Err()
			return nil, rpcCtx.Err()
		})

		insp := NewCompactionInspector(m, taskMeta, nil, nil, nil, nil, nil, allocator.NewRootCoordAllocator(mc), nil, nil, nil, index.NewTestVersionManager())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		errCh := make(chan error, 1)
		go func() {
			errCh <- insp.EnqueueCompactionWithContext(ctx, newTestTask(3001, 2001, 1001, 5001))
		}()

		select {
		case <-enteredAllocTS:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for AllocTimestamp")
		}

		cancel()

		select {
		case err := <-allocTSObservedErr:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(1 * time.Second):
			t.Fatal("AllocTimestamp did not observe context cancellation")
		}

		select {
		case err := <-errCh:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(1 * time.Second):
			t.Fatal("EnqueueCompactionWithContext did not return after cancel")
		}

		require.False(t, m.GetSegment(context.Background(), 5001).IsCompacting)
		require.Nil(t, insp.getCompactionTask(3001))
	})

	t.Run("cancels_SaveCompactionTask_and_releases_segments", func(t *testing.T) {
		m, _, _ := buildTestMetaWithCandidateGroups(t, 1, datapb.SegmentLevel_L1)
		enteredSave := make(chan struct{})
		saveObservedErr := make(chan error, 1)
		persisted := make(map[int64]*datapb.CompactionTask)
		var persistedMu sync.Mutex

		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).RunAndReturn(func(saveCtx context.Context, task *datapb.CompactionTask) error {
			close(enteredSave)
			<-saveCtx.Done()
			// Simulate an ambiguous write where etcd committed before the cancel response returned.
			persistedMu.Lock()
			persisted[task.GetPlanID()] = task
			persistedMu.Unlock()
			saveObservedErr <- saveCtx.Err()
			return saveCtx.Err()
		})
		catalog.EXPECT().DropCompactionTask(mock.Anything, mock.Anything).RunAndReturn(func(dropCtx context.Context, task *datapb.CompactionTask) error {
			require.NoError(t, dropCtx.Err())
			persistedMu.Lock()
			delete(persisted, task.GetPlanID())
			persistedMu.Unlock()
			return nil
		}).Once()
		taskMeta := ctask.NewCompactionTaskMetaOverCatalog(catalog)

		mc := rootmocks.NewMixCoord(t)
		mc.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocTimestampResponse{
			Status:    merr.Success(),
			Timestamp: 1000,
			Count:     1,
		}, nil)

		insp := NewCompactionInspector(m, taskMeta, nil, nil, nil, nil, nil, allocator.NewRootCoordAllocator(mc), nil, nil, nil, index.NewTestVersionManager())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		errCh := make(chan error, 1)
		go func() {
			errCh <- insp.EnqueueCompactionWithContext(ctx, newTestTask(3002, 2002, 1001, 5001))
		}()

		select {
		case <-enteredSave:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for SaveCompactionTask")
		}

		cancel()

		select {
		case err := <-saveObservedErr:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(1 * time.Second):
			t.Fatal("SaveCompactionTask did not observe context cancellation")
		}

		select {
		case err := <-errCh:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(1 * time.Second):
			t.Fatal("EnqueueCompactionWithContext did not return after cancel")
		}

		persistedMu.Lock()
		require.Empty(t, persisted, "ambiguous canceled save must be cleaned up from catalog")
		persistedMu.Unlock()
		require.False(t, m.GetSegment(context.Background(), 5001).IsCompacting)
		require.Nil(t, insp.getCompactionTask(3002))
		require.Empty(t, taskMeta.GetCompactionTasks()[2002])
	})

	t.Run("drops_persisted_task_meta_when_queue_is_full", func(t *testing.T) {
		m, _, _ := buildTestMetaWithCandidateGroups(t, 2, datapb.SegmentLevel_L1)
		var droppedPlanIDs []int64
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		catalog.EXPECT().DropCompactionTask(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, task *datapb.CompactionTask) error {
			droppedPlanIDs = append(droppedPlanIDs, task.GetPlanID())
			return nil
		}).Once()
		taskMeta := ctask.NewCompactionTaskMetaOverCatalog(catalog)

		mc := rootmocks.NewMixCoord(t)
		mc.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocTimestampResponse{
			Status:    merr.Success(),
			Timestamp: 1000,
			Count:     1,
		}, nil)

		insp := NewCompactionInspector(m, taskMeta, nil, nil, nil, nil, nil, allocator.NewRootCoordAllocator(mc), nil, nil, nil, index.NewTestVersionManager())
		insp.queueTasks = NewCompactionQueue(1, getPrioritizer())

		require.NoError(t, insp.EnqueueCompactionWithContext(context.Background(), newTestTask(4001, 2101, 1001, 5001)))
		err := insp.EnqueueCompactionWithContext(context.Background(), newTestTask(4002, 2102, 1002, 5002))
		require.ErrorIs(t, err, errFull)

		require.Equal(t, []int64{4002}, droppedPlanIDs)
		require.Empty(t, taskMeta.GetCompactionTasks()[2102])
		require.Len(t, taskMeta.GetCompactionTasks()[2101], 1)
		statsJSON := taskMeta.TaskStatsJSON()
		require.Contains(t, statsJSON, `"plan_id":"4001"`)
		require.NotContains(t, statsJSON, `"plan_id":"4002"`)
		require.False(t, m.GetSegment(context.Background(), 5002).IsCompacting)
		require.True(t, m.GetSegment(context.Background(), 5001).IsCompacting)
	})
}

func TestCompactionTriggerStopDuringOnlyBucketEnqueueReturnsServiceUnavailable(t *testing.T) {
	pt := paramtable.Get()
	pt.Save(pt.DataCoordCfg.IndexBasedCompaction.Key, "false")
	defer pt.Reset(pt.DataCoordCfg.IndexBasedCompaction.Key)

	m, im, colls := buildTestMetaWithCandidateGroups(t, 1, datapb.SegmentLevel_L1)
	handler := newFuncHandler(t, func(_ context.Context, collID UniqueID) (*meta.CollectionInfo, error) {
		return colls[collID], nil
	})
	_, alloc := newIncrementingRootCoordAllocator(t)

	enteredEnqueue := make(chan struct{})
	releaseEnqueue := make(chan struct{})
	var enteredOnce sync.Once
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseEnqueue) })
	})
	inspector := &recordingInspector{
		onEnqueueCtx: func(ctx context.Context, _ *datapb.CompactionTask) error {
			enteredOnce.Do(func() {
				close(enteredEnqueue)
			})
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-releaseEnqueue:
				return context.DeadlineExceeded
			}
		},
	}

	tr := NewCompactionTrigger(m, im, inspector, alloc, handler, index.NewTestVersionManager())
	tr.testingOnly = true
	tr.Start()

	type triggerRes struct {
		id  UniqueID
		err error
	}
	resCh := make(chan triggerRes, 1)
	go func() {
		id, err := tr.TriggerCompaction(context.Background(), NewCompactionSignal().WithCollectionID(1001).WithIsForce(true).WithWaitResult(true))
		resCh <- triggerRes{id: id, err: err}
	}()

	select {
	case <-enteredEnqueue:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for EnqueueCompactionWithContext")
	}

	stopStart := time.Now()
	stopDone := make(chan struct{})
	go func() {
		tr.Stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
		require.Less(t, time.Since(stopStart), 800*time.Millisecond)
	case <-time.After(2 * time.Second):
		t.Fatal("compactionTrigger.Stop() hung during EnqueueCompactionWithContext")
	}

	select {
	case res := <-resCh:
		require.ErrorIs(t, res.err, merr.ErrServiceUnavailable)
	case <-time.After(1 * time.Second):
		t.Fatal("TriggerCompaction did not return after Stop() during enqueue")
	}

	require.Empty(t, inspector.snapshot())
}

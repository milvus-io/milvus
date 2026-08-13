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

package compactor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datanode/resource"
	"github.com/milvus-io/milvus/internal/util/taskresource"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// useRecordingGuard routes the executor's admission calls to a double for the
// duration of the test. The process-wide guard is deliberately left out of the
// unit tests: it samples the machine's real memory in the background, so a test
// that reserved from it would pass or hang depending on what else the host
// happened to be doing.
func useRecordingGuard(t *testing.T) *resource.RecordingGuard {
	g := resource.NewRecordingGuard()
	mk := mockey.Mock(resource.GetGuard).Return(g).Build()
	t.Cleanup(func() { mk.UnPatch() })
	return g
}

// planWithBinlogs is a compaction plan with enough of a body that its derived
// requirement is not the estimator's floor, so a test comparing against it
// cannot be satisfied by any old figure.
func planWithBinlogs(planID int64) *datapb.CompactionPlan {
	return &datapb.CompactionPlan{
		PlanID: planID,
		Type:   datapb.CompactionType_MixCompaction,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
			{
				SegmentID: 1000,
				FieldBinlogs: []*datapb.FieldBinlog{
					{Binlogs: []*datapb.Binlog{{MemorySize: 3 << 30, EntriesNum: 1000000}}},
				},
			},
		},
	}
}

func TestCompactionExecutor(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable())
	// Every subtest below that runs a task goes through the guard; route them
	// all at a double so none of them reserves from the process-wide ledger.
	useRecordingGuard(t)

	t.Run("Test_Enqueue_Success", func(t *testing.T) {
		ex := NewExecutor()
		mockC := NewMockCompactor(t)
		mockC.EXPECT().GetPlanID().Return(int64(1))
		mockC.EXPECT().GetSlotUsage().Return(int64(8))

		succeed, err := ex.Enqueue(mockC)
		assert.True(t, succeed)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(ex.taskCh))
		assert.Equal(t, int64(8), ex.Slots())

		ex.mu.RLock()
		task, exists := ex.tasks[1]
		ex.mu.RUnlock()
		assert.True(t, exists)
		assert.Equal(t, datapb.CompactionTaskState_executing, task.state)
	})

	t.Run("Test_Enqueue_Duplicate", func(t *testing.T) {
		ex := NewExecutor()
		mockC := NewMockCompactor(t)
		mockC.EXPECT().GetPlanID().Return(int64(1)).Times(2)
		mockC.EXPECT().GetSlotUsage().Return(int64(8))
		mockC.EXPECT().GetChannelName().Return("ch1")

		succeed, err := ex.Enqueue(mockC)
		assert.True(t, succeed)
		assert.NoError(t, err)

		succeed, err = ex.Enqueue(mockC)
		assert.False(t, succeed)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, merr.ErrDuplicatedCompactionTask))
		assert.Equal(t, 1, len(ex.taskCh))
	})

	t.Run("Test_Slots_NotBlocked_WhenEnqueueWaitsOnFullQueue", func(t *testing.T) {
		ex := NewExecutor()
		for i := 0; i < cap(ex.taskCh); i++ {
			ex.taskCh <- nil
		}

		enqueueHoldingLock := make(chan struct{})
		mockC := NewMockCompactor(t)
		mockC.EXPECT().GetPlanID().Return(int64(100))
		mockC.EXPECT().GetSlotUsage().Run(func() {
			close(enqueueHoldingLock)
		}).Return(int64(8))

		enqueueDone := make(chan struct{})
		go func() {
			defer close(enqueueDone)
			succeed, err := ex.Enqueue(mockC)
			assert.True(t, succeed)
			assert.NoError(t, err)
		}()

		require.Eventually(t, func() bool {
			select {
			case <-enqueueHoldingLock:
				return true
			default:
				return false
			}
		}, time.Second, 10*time.Millisecond)

		slotsDone := make(chan int64, 1)
		go func() {
			slotsDone <- ex.Slots()
		}()

		var slotsBlocked bool
		select {
		case slots := <-slotsDone:
			assert.Equal(t, int64(8), slots)
		case <-time.After(100 * time.Millisecond):
			slotsBlocked = true
		}

		<-ex.taskCh
		require.Eventually(t, func() bool {
			select {
			case <-enqueueDone:
				return true
			default:
				return false
			}
		}, time.Second, 10*time.Millisecond)

		if slotsBlocked {
			require.Eventually(t, func() bool {
				select {
				case <-slotsDone:
					return true
				default:
					return false
				}
			}, time.Second, 10*time.Millisecond)
			require.Fail(t, "Slots blocked while Enqueue waited on a full task queue")
		}
	})

	t.Run("Test_Enqueue_DefaultSlotUsage", func(t *testing.T) {
		testCases := []struct {
			name              string
			compactionType    datapb.CompactionType
			expectedSlotUsage int64
		}{
			{
				name:              "MixCompaction",
				compactionType:    datapb.CompactionType_MixCompaction,
				expectedSlotUsage: paramtable.Get().DataCoordCfg.MixCompactionSlotUsage.GetAsInt64(),
			},
			{
				name:              "Level0DeleteCompaction",
				compactionType:    datapb.CompactionType_Level0DeleteCompaction,
				expectedSlotUsage: paramtable.Get().DataCoordCfg.L0DeleteCompactionSlotUsage.GetAsInt64(),
			},
			{
				name:              "ClusteringCompaction",
				compactionType:    datapb.CompactionType_ClusteringCompaction,
				expectedSlotUsage: paramtable.Get().DataCoordCfg.ClusteringCompactionSlotUsage.GetAsInt64(),
			},
			{
				name:              "BumpSchemaVersionCompaction",
				compactionType:    datapb.CompactionType_BumpSchemaVersionCompaction,
				expectedSlotUsage: paramtable.Get().DataCoordCfg.BumpSchemaVersionCompactionSlotUsage.GetAsInt64(),
			},
		}

		for i, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				ex := NewExecutor()
				mockC := NewMockCompactor(t)
				mockC.EXPECT().GetPlanID().Return(int64(i + 10))
				mockC.EXPECT().GetSlotUsage().Return(int64(0)).Times(2)
				mockC.EXPECT().GetCompactionType().Return(tc.compactionType)

				succeed, err := ex.Enqueue(mockC)
				assert.True(t, succeed)
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedSlotUsage, ex.Slots())
			})
		}
	})

	t.Run("Test_ExecuteTask_Success", func(t *testing.T) {
		ex := NewExecutor()
		mockC := NewMockCompactor(t)

		planID := int64(1)
		result := &datapb.CompactionPlanResult{
			PlanID: planID,
			State:  datapb.CompactionTaskState_completed,
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:  100,
					NumOfRows:  1000,
					InsertLogs: nil,
					Deltalogs:  nil,
				},
			},
		}

		mockC.EXPECT().GetCompactionType().Return(datapb.CompactionType_MixCompaction)
		mockC.EXPECT().GetPlanID().Return(planID).Times(2)
		mockC.EXPECT().GetCollection().Return(int64(1))
		mockC.EXPECT().GetChannelName().Return("ch1")
		mockC.EXPECT().GetSlotUsage().Return(int64(8)).Times(3)
		mockC.EXPECT().GetPlan().Return(nil)
		mockC.EXPECT().Compact().Return(result, nil)
		mockC.EXPECT().Complete().Return()
		mockC.EXPECT().GetStorageConfig().Return(nil)

		succeed, err := ex.Enqueue(mockC)
		assert.True(t, succeed)
		assert.NoError(t, err)

		ex.executeTask(context.Background(), mockC)

		ex.mu.RLock()
		task, exists := ex.tasks[planID]
		ex.mu.RUnlock()
		assert.True(t, exists)
		assert.Equal(t, datapb.CompactionTaskState_completed, task.state)
		assert.Equal(t, result, task.result)
		assert.Equal(t, int64(0), ex.Slots())
	})

	t.Run("Test_ExecuteTask_Failure", func(t *testing.T) {
		ex := NewExecutor()
		mockC := NewMockCompactor(t)

		planID := int64(2)
		mockC.EXPECT().GetCompactionType().Return(datapb.CompactionType_MixCompaction)
		mockC.EXPECT().GetPlanID().Return(planID).Times(2)
		mockC.EXPECT().GetCollection().Return(int64(1))
		mockC.EXPECT().GetChannelName().Return("ch1")
		mockC.EXPECT().GetSlotUsage().Return(int64(8)).Times(3)
		mockC.EXPECT().GetPlan().Return(nil)
		mockC.EXPECT().Compact().Return(nil, errors.New("compaction failed"))
		mockC.EXPECT().Complete().Return()
		mockC.EXPECT().GetStorageConfig().Return(nil)

		succeed, err := ex.Enqueue(mockC)
		assert.True(t, succeed)
		assert.NoError(t, err)

		ex.executeTask(context.Background(), mockC)

		ex.mu.RLock()
		task, exists := ex.tasks[planID]
		ex.mu.RUnlock()
		assert.True(t, exists)
		assert.Equal(t, datapb.CompactionTaskState_failed, task.state)
		assert.Nil(t, task.result)
		assert.Equal(t, int64(0), ex.Slots())
	})

	t.Run("Test_RemoveTask", func(t *testing.T) {
		ex := NewExecutor()

		completedTask := &taskState{
			compactor: NewMockCompactor(t),
			state:     datapb.CompactionTaskState_completed,
			result:    &datapb.CompactionPlanResult{PlanID: 1},
		}

		executingTask := &taskState{
			compactor: NewMockCompactor(t),
			state:     datapb.CompactionTaskState_executing,
			result:    nil,
		}

		failedTask := &taskState{
			compactor: NewMockCompactor(t),
			state:     datapb.CompactionTaskState_failed,
			result:    nil,
		}

		completedTask.compactor.(*MockCompactor).EXPECT().GetChannelName().Return("ch1").Maybe()
		executingTask.compactor.(*MockCompactor).EXPECT().GetChannelName().Return("ch2").Maybe()
		failedTask.compactor.(*MockCompactor).EXPECT().GetChannelName().Return("ch3").Maybe()

		ex.tasks[1] = completedTask
		ex.tasks[2] = executingTask
		ex.tasks[3] = failedTask

		ex.RemoveTask(1)
		assert.Equal(t, 2, len(ex.tasks))

		ex.RemoveTask(2)
		assert.Equal(t, 2, len(ex.tasks))

		ex.RemoveTask(3)
		assert.Equal(t, 1, len(ex.tasks))

		_, exists := ex.tasks[2]
		assert.True(t, exists)
	})

	t.Run("Test_GetResults_SinglePlan", func(t *testing.T) {
		ex := NewExecutor()

		result := &datapb.CompactionPlanResult{
			PlanID: 1,
			State:  datapb.CompactionTaskState_completed,
		}

		ex.tasks[1] = &taskState{
			compactor: NewMockCompactor(t),
			state:     datapb.CompactionTaskState_completed,
			result:    result,
		}

		results := ex.GetResults(1)
		assert.Equal(t, 1, len(results))
		assert.Equal(t, result, results[0])
	})

	t.Run("Test_GetResults_NonExistentPlan", func(t *testing.T) {
		ex := NewExecutor()

		results := ex.GetResults(999)
		assert.Equal(t, 1, len(results))
		assert.Equal(t, int64(999), results[0].PlanID)
		assert.Equal(t, datapb.CompactionTaskState_failed, results[0].State)
	})

	t.Run("Test_GetResults_All", func(t *testing.T) {
		ex := NewExecutor()

		mockC1 := NewMockCompactor(t)
		ex.tasks[1] = &taskState{
			compactor: mockC1,
			state:     datapb.CompactionTaskState_executing,
			result:    nil,
		}

		mockC2 := NewMockCompactor(t)
		ex.tasks[2] = &taskState{
			compactor: mockC2,
			state:     datapb.CompactionTaskState_completed,
			result: &datapb.CompactionPlanResult{
				PlanID: 2,
				State:  datapb.CompactionTaskState_completed,
				Type:   datapb.CompactionType_MixCompaction,
			},
		}

		mockC3 := NewMockCompactor(t)
		ex.tasks[3] = &taskState{
			compactor: mockC3,
			state:     datapb.CompactionTaskState_completed,
			result: &datapb.CompactionPlanResult{
				PlanID: 3,
				State:  datapb.CompactionTaskState_completed,
				Type:   datapb.CompactionType_Level0DeleteCompaction,
			},
		}

		results := ex.GetResults(0)
		assert.Equal(t, 3, len(results))

		planIDs := make(map[int64]bool)
		for _, r := range results {
			planIDs[r.PlanID] = true
		}
		assert.True(t, planIDs[1])
		assert.True(t, planIDs[2])
		assert.True(t, planIDs[3])

		assert.Equal(t, 2, len(ex.tasks))
		_, exists := ex.tasks[3]
		assert.False(t, exists)
	})

	t.Run("Test_Start_Context_Cancel", func(t *testing.T) {
		ex := NewExecutor()
		ctx, cancel := context.WithCancel(context.Background())

		done := make(chan bool)
		go func() {
			ex.Start(ctx)
			done <- true
		}()

		cancel()

		select {
		case <-done:
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Start didn't return after context cancel")
		}
	})

	t.Run("Test_Concurrent_Operations", func(t *testing.T) {
		ex := NewExecutor()
		numTasks := 20
		var wg sync.WaitGroup

		for i := 0; i < numTasks; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				mockC := NewMockCompactor(t)
				mockC.EXPECT().GetPlanID().Return(int64(id))
				mockC.EXPECT().GetSlotUsage().Return(int64(1))
				mockC.EXPECT().GetChannelName().Return("ch1").Maybe()

				ex.Enqueue(mockC)
			}(i)
		}

		wg.Wait()

		assert.Equal(t, numTasks, len(ex.tasks))
		assert.Equal(t, int64(numTasks), ex.Slots())
	})

	t.Run("Test_CompleteTask_SlotAdjustment", func(t *testing.T) {
		ex := NewExecutor()
		mockC := NewMockCompactor(t)

		planID := int64(1)
		slotUsage := int64(10)

		mockC.EXPECT().GetPlanID().Return(planID)
		mockC.EXPECT().GetSlotUsage().Return(slotUsage).Times(2)
		mockC.EXPECT().Complete().Return()
		mockC.EXPECT().GetStorageConfig().Return(nil)

		ex.Enqueue(mockC)
		assert.Equal(t, slotUsage, ex.Slots())

		result := &datapb.CompactionPlanResult{PlanID: planID}
		ex.completeTask(planID, result)

		assert.Equal(t, int64(0), ex.Slots())

		ex.mu.RLock()
		task := ex.tasks[planID]
		ex.mu.RUnlock()
		assert.Equal(t, datapb.CompactionTaskState_completed, task.state)
		assert.Equal(t, result, task.result)
	})

	t.Run("Test_CompleteTask_NegativeSlotProtection", func(t *testing.T) {
		ex := NewExecutor()

		ex.usingSlots = -5

		mockC := NewMockCompactor(t)
		mockC.EXPECT().GetSlotUsage().Return(int64(10))
		mockC.EXPECT().Complete().Return()
		mockC.EXPECT().GetStorageConfig().Return(nil)

		ex.tasks[1] = &taskState{
			compactor: mockC,
			state:     datapb.CompactionTaskState_executing,
		}

		ex.completeTask(1, nil)

		assert.Equal(t, int64(0), ex.Slots())
	})

	t.Run("Test_CompleteTask_DoesNotHoldLockDuringCallbacks", func(t *testing.T) {
		ex := NewExecutor()
		mockC := NewMockCompactor(t)
		planID := int64(10)
		slotUsage := int64(8)

		ex.tasks[planID] = &taskState{
			compactor: mockC,
			state:     datapb.CompactionTaskState_executing,
		}
		ex.usingSlots = slotUsage

		callbackSlots := make(chan int64, 2)
		mockC.EXPECT().GetSlotUsage().Return(slotUsage)
		mockC.EXPECT().Complete().Run(func() {
			callbackSlots <- ex.Slots()
		}).Return()
		mockC.EXPECT().GetStorageConfig().Run(func() {
			callbackSlots <- ex.Slots()
		}).Return(nil)

		done := make(chan struct{})
		go func() {
			defer close(done)
			ex.completeTask(planID, &datapb.CompactionPlanResult{PlanID: planID})
		}()

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("completeTask blocked while invoking compactor callbacks")
		}

		require.Equal(t, int64(0), <-callbackSlots)
		require.Equal(t, int64(0), <-callbackSlots)
		assert.Equal(t, int64(0), ex.Slots())
	})

	t.Run("Test_Task_State_Transitions", func(t *testing.T) {
		ex := NewExecutor()
		mockC := NewMockCompactor(t)

		planID := int64(1)
		mockC.EXPECT().GetPlanID().Return(planID).Times(2)
		mockC.EXPECT().GetSlotUsage().Return(int64(5)).Times(3)
		mockC.EXPECT().GetPlan().Return(nil)
		mockC.EXPECT().GetCollection().Return(int64(1))
		mockC.EXPECT().GetChannelName().Return("ch1")
		mockC.EXPECT().Complete().Return()
		mockC.EXPECT().GetCompactionType().Return(datapb.CompactionType_MixCompaction)
		mockC.EXPECT().GetStorageConfig().Return(nil)

		ex.Enqueue(mockC)
		ex.mu.RLock()
		assert.Equal(t, datapb.CompactionTaskState_executing, ex.tasks[planID].state)
		ex.mu.RUnlock()

		mockC.EXPECT().Compact().Return(&datapb.CompactionPlanResult{
			PlanID: planID,
			State:  datapb.CompactionTaskState_completed,
		}, nil).Once()

		ex.executeTask(context.Background(), mockC)

		ex.mu.RLock()
		assert.Equal(t, datapb.CompactionTaskState_completed, ex.tasks[planID].state)
		ex.mu.RUnlock()
	})

	t.Run("Test_GetResults_ExecutingTask", func(t *testing.T) {
		ex := NewExecutor()

		ex.tasks[1] = &taskState{
			compactor: NewMockCompactor(t),
			state:     datapb.CompactionTaskState_executing,
			result:    nil,
		}

		results := ex.GetResults(1)
		assert.Equal(t, 1, len(results))
		assert.Equal(t, int64(1), results[0].PlanID)
		assert.Equal(t, datapb.CompactionTaskState_executing, results[0].State)
	})

	t.Run("Test_Multiple_ExecuteTask_WithMetrics", func(t *testing.T) {
		ex := NewExecutor()

		planIDs := []int64{1, 2, 3}
		for _, planID := range planIDs {
			mockC := NewMockCompactor(t)
			mockC.EXPECT().GetCompactionType().Return(datapb.CompactionType_MixCompaction)
			mockC.EXPECT().GetPlanID().Return(planID).Times(2)
			mockC.EXPECT().GetCollection().Return(int64(100))
			mockC.EXPECT().GetChannelName().Return("ch1")
			mockC.EXPECT().GetSlotUsage().Return(int64(4)).Times(3)
			mockC.EXPECT().GetPlan().Return(nil)
			mockC.EXPECT().Complete().Return()
			mockC.EXPECT().GetStorageConfig().Return(nil)

			result := &datapb.CompactionPlanResult{
				PlanID: planID,
				State:  datapb.CompactionTaskState_completed,
				Segments: []*datapb.CompactionSegment{
					{
						SegmentID: planID * 100,
						NumOfRows: planID * 1000,
						Deltalogs: []*datapb.FieldBinlog{
							{
								Binlogs: []*datapb.Binlog{
									{EntriesNum: 10},
								},
							},
						},
					},
				},
			}
			mockC.EXPECT().Compact().Return(result, nil)

			succeed, err := ex.Enqueue(mockC)
			require.True(t, succeed)
			require.NoError(t, err)

			ex.executeTask(context.Background(), mockC)
		}

		results := ex.GetResults(0)
		assert.Equal(t, 3, len(results))
		for _, result := range results {
			assert.Equal(t, datapb.CompactionTaskState_completed, result.State)
		}
	})

	t.Run("Test_CompleteTask_WithStorageConfig", func(t *testing.T) {
		ex := NewExecutor()
		mockC := NewMockCompactor(t)

		planID := int64(1)
		storageConfig := &indexpb.StorageConfig{
			StorageType: "minio",
			Address:     "localhost:9000",
			BucketName:  "test-bucket",
		}

		mockC.EXPECT().GetPlanID().Return(planID)
		mockC.EXPECT().GetSlotUsage().Return(int64(8)).Times(2)
		mockC.EXPECT().Complete().Return()
		mockC.EXPECT().GetStorageConfig().Return(storageConfig)

		ex.Enqueue(mockC)
		assert.Equal(t, int64(8), ex.Slots())

		result := &datapb.CompactionPlanResult{PlanID: planID}
		ex.completeTask(planID, result)

		assert.Equal(t, int64(0), ex.Slots())

		ex.mu.RLock()
		task := ex.tasks[planID]
		ex.mu.RUnlock()
		assert.Equal(t, datapb.CompactionTaskState_completed, task.state)
		assert.Equal(t, result, task.result)
	})
}

func TestExecutorAdmission(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable())

	newMock := func(t *testing.T, planID int64, plan *datapb.CompactionPlan) *MockCompactor {
		mockC := NewMockCompactor(t)
		mockC.EXPECT().GetPlanID().Return(planID).Maybe()
		mockC.EXPECT().GetCollection().Return(int64(1)).Maybe()
		mockC.EXPECT().GetChannelName().Return("ch1").Maybe()
		mockC.EXPECT().GetCompactionType().Return(datapb.CompactionType_MixCompaction).Maybe()
		mockC.EXPECT().GetSlotUsage().Return(int64(8)).Maybe()
		mockC.EXPECT().GetPlan().Return(plan).Maybe()
		mockC.EXPECT().Complete().Return().Maybe()
		mockC.EXPECT().GetStorageConfig().Return(nil).Maybe()
		return mockC
	}

	t.Run("reserves before compacting and releases afterwards", func(t *testing.T) {
		g := useRecordingGuard(t)
		planID := int64(4001)
		plan := planWithBinlogs(planID)

		mockC := newMock(t, planID, plan)
		mockC.EXPECT().Compact().RunAndReturn(func() (*datapb.CompactionPlanResult, error) {
			g.Note("compact")
			return &datapb.CompactionPlanResult{PlanID: planID}, nil
		}).Once()

		ex := NewExecutor()
		_, err := ex.Enqueue(mockC)
		require.NoError(t, err)
		ex.executeTask(context.Background(), mockC)

		// The reservation must bracket the work: taken before the first byte is
		// read, returned once the task is over.
		assert.Equal(t, []string{"acquire", "compact", "release"}, g.Events())

		acquires := g.Acquires()
		require.Len(t, acquires, 1)
		assert.Equal(t, planID, acquires[0].TaskID)
		assert.Equal(t, taskcommon.Compaction, acquires[0].Type)
		// Priced from the plan's own binlogs, not from anything the plan claims
		// about slots. Compared against an independently derived figure so a
		// requirement that is merely non-zero cannot satisfy this.
		assert.Equal(t, taskresource.RequirementForCompaction(plan), acquires[0].Req)
		assert.Equal(t, []int64{planID}, g.Releases())
	})

	t.Run("releases when the compaction fails", func(t *testing.T) {
		g := useRecordingGuard(t)
		planID := int64(4002)

		mockC := newMock(t, planID, planWithBinlogs(planID))
		mockC.EXPECT().Compact().Return(nil, errors.New("boom")).Once()

		ex := NewExecutor()
		_, err := ex.Enqueue(mockC)
		require.NoError(t, err)
		ex.executeTask(context.Background(), mockC)

		assert.Equal(t, []int64{planID}, g.Releases(), "a failed task must not leak its reservation")

		ex.mu.RLock()
		state := ex.tasks[planID].state
		ex.mu.RUnlock()
		assert.Equal(t, datapb.CompactionTaskState_failed, state)
	})

	t.Run("reserves at execution, not at enqueue", func(t *testing.T) {
		g := useRecordingGuard(t)
		planID := int64(4003)

		mockC := newMock(t, planID, planWithBinlogs(planID))

		ex := NewExecutor()
		_, err := ex.Enqueue(mockC)
		require.NoError(t, err)

		// Enqueue only queues. Reserving here would charge the node for work
		// that has not started, and would make waiting look like a stalled RPC
		// instead of a queued task.
		assert.Empty(t, g.Acquires())
		assert.Empty(t, g.TryAcquires())
	})

	t.Run("parks in Acquire instead of polling TryAcquire", func(t *testing.T) {
		g := useRecordingGuard(t)
		g.Block()
		planID := int64(4004)

		compacting := make(chan struct{})
		mockC := newMock(t, planID, planWithBinlogs(planID))
		mockC.EXPECT().Compact().RunAndReturn(func() (*datapb.CompactionPlanResult, error) {
			close(compacting)
			return &datapb.CompactionPlanResult{PlanID: planID}, nil
		}).Once()

		ex := NewExecutor()
		_, err := ex.Enqueue(mockC)
		require.NoError(t, err)

		done := make(chan struct{})
		go func() {
			defer close(done)
			ex.executeTask(context.Background(), mockC)
		}()

		// While the budget is unavailable no work may start...
		select {
		case <-compacting:
			require.Fail(t, "compaction started before its reservation was granted")
		case <-time.After(100 * time.Millisecond):
		}
		// ...and the wait must happen inside Acquire, where the guard can hold
		// the queue's head. A TryAcquire poll loop is invisible to that and can
		// starve a large task forever.
		assert.Empty(t, g.TryAcquires())

		g.Unblock()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			require.Fail(t, "compaction never ran after the guard admitted it")
		}
		assert.Equal(t, []string{"acquire", "release"}, g.Events())
		assert.Equal(t, []int64{planID}, g.Releases())
	})

	t.Run("gives up without releasing when the wait is cut short", func(t *testing.T) {
		g := useRecordingGuard(t)
		g.FailAcquire(context.Canceled)
		planID := int64(4005)

		mockC := newMock(t, planID, planWithBinlogs(planID))

		ex := NewExecutor()
		_, err := ex.Enqueue(mockC)
		require.NoError(t, err)
		require.Equal(t, int64(8), ex.Slots())

		ex.executeTask(context.Background(), mockC)

		// Compact is never expected on the mock, so running it would fail the
		// test outright.
		assert.Empty(t, g.Releases(), "a task that never acquired must not release")
		// The executor's own books have to come back to where they started, or
		// the node permanently believes it is busier than it is.
		assert.Equal(t, int64(0), ex.Slots())
		ex.mu.RLock()
		state := ex.tasks[planID].state
		ex.mu.RUnlock()
		assert.Equal(t, datapb.CompactionTaskState_failed, state)
	})

	t.Run("falls back to the legacy slot when the plan is missing", func(t *testing.T) {
		g := useRecordingGuard(t)
		planID := int64(4006)

		mockC := newMock(t, planID, nil)
		mockC.EXPECT().Compact().Return(&datapb.CompactionPlanResult{PlanID: planID}, nil).Once()

		ex := NewExecutor()
		_, err := ex.Enqueue(mockC)
		require.NoError(t, err)
		ex.executeTask(context.Background(), mockC)

		acquires := g.Acquires()
		require.Len(t, acquires, 1)
		// Without a plan there is nothing to recompute from, so the slot the
		// coordinator sent is folded in -- never zero, which would admit the
		// task for free.
		assert.Equal(t, taskresource.LegacySlotToRequirement(8), acquires[0].Req)
		assert.Greater(t, acquires[0].Req.Memory, int64(0))
	})
}

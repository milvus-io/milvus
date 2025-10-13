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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestCompactionExecutor(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable())

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

	t.Run("Test_Enqueue_DefaultSlotUsage", func(t *testing.T) {
		ex := NewExecutor()

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
		}

		for i, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				mockC := NewMockCompactor(t)
				mockC.EXPECT().GetPlanID().Return(int64(i + 10))
				mockC.EXPECT().GetSlotUsage().Return(int64(0)).Times(2)
				mockC.EXPECT().GetCompactionType().Return(tc.compactionType)

				succeed, err := ex.Enqueue(mockC)
				assert.True(t, succeed)
				assert.NoError(t, err)
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
		mockC.EXPECT().GetPlanID().Return(planID).Times(3)
		mockC.EXPECT().GetCollection().Return(int64(1))
		mockC.EXPECT().GetChannelName().Return("ch1")
		mockC.EXPECT().GetSlotUsage().Return(int64(8)).Times(2)
		mockC.EXPECT().Compact().Return(result, nil)
		mockC.EXPECT().Complete().Return()

		succeed, err := ex.Enqueue(mockC)
		assert.True(t, succeed)
		assert.NoError(t, err)

		ex.executeTask(mockC)

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
		mockC.EXPECT().GetPlanID().Return(planID).Times(3)
		mockC.EXPECT().GetCollection().Return(int64(1))
		mockC.EXPECT().GetChannelName().Return("ch1")
		mockC.EXPECT().GetSlotUsage().Return(int64(8)).Times(2)
		mockC.EXPECT().Compact().Return(nil, errors.New("compaction failed"))
		mockC.EXPECT().Complete().Return()

		succeed, err := ex.Enqueue(mockC)
		assert.True(t, succeed)
		assert.NoError(t, err)

		ex.executeTask(mockC)

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

		ex.tasks[1] = &taskState{
			compactor: mockC,
			state:     datapb.CompactionTaskState_executing,
		}

		ex.completeTask(1, nil)

		assert.Equal(t, int64(0), ex.Slots())
	})

	t.Run("Test_Task_State_Transitions", func(t *testing.T) {
		ex := NewExecutor()
		mockC := NewMockCompactor(t)

		planID := int64(1)
		mockC.EXPECT().GetPlanID().Return(planID).Times(3)
		mockC.EXPECT().GetSlotUsage().Return(int64(5)).Times(2)
		mockC.EXPECT().GetCollection().Return(int64(1))
		mockC.EXPECT().GetChannelName().Return("ch1")
		mockC.EXPECT().Complete().Return()
		mockC.EXPECT().GetCompactionType().Return(datapb.CompactionType_MixCompaction)

		ex.Enqueue(mockC)
		ex.mu.RLock()
		assert.Equal(t, datapb.CompactionTaskState_executing, ex.tasks[planID].state)
		ex.mu.RUnlock()

		mockC.EXPECT().Compact().Return(&datapb.CompactionPlanResult{
			PlanID: planID,
			State:  datapb.CompactionTaskState_completed,
		}, nil).Once()

		ex.executeTask(mockC)

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
			mockC.EXPECT().GetPlanID().Return(planID).Times(3)
			mockC.EXPECT().GetCollection().Return(int64(100))
			mockC.EXPECT().GetChannelName().Return("ch1")
			mockC.EXPECT().GetSlotUsage().Return(int64(4)).Times(2)
			mockC.EXPECT().Complete().Return()

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

			ex.executeTask(mockC)
		}

		results := ex.GetResults(0)
		assert.Equal(t, 3, len(results))
		for _, result := range results {
			assert.Equal(t, datapb.CompactionTaskState_completed, result.State)
		}
	})
}

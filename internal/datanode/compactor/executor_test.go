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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestCompactionExecutor(t *testing.T) {
	t.Run("Test execute", func(t *testing.T) {
		paramtable.Get().Init(paramtable.NewBaseTable())
		planID := int64(1)
		mockC := NewMockCompactor(t)
		mockC.EXPECT().GetPlanID().Return(planID)
		mockC.EXPECT().GetChannelName().Return("ch1")
		mockC.EXPECT().GetSlotUsage().Return(8)
		executor := NewExecutor()
		succeed, err := executor.Enqueue(mockC)
		assert.Equal(t, true, succeed)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(executor.taskCh))
		assert.EqualValues(t, 1, executor.executing.Len())

		mockC.EXPECT().Stop().Return().Once()
		executor.stopTask(planID)
	})

	t.Run("Test deplicate execute", func(t *testing.T) {
		paramtable.Get().Init(paramtable.NewBaseTable())
		planID := int64(1)
		mockC := NewMockCompactor(t)
		mockC.EXPECT().GetPlanID().Return(planID)
		mockC.EXPECT().GetChannelName().Return("ch1")
		mockC.EXPECT().GetSlotUsage().Return(8)
		executor := NewExecutor()
		succeed, err := executor.Enqueue(mockC)
		assert.Equal(t, true, succeed)
		assert.NoError(t, err)

		succeed2, err2 := executor.Enqueue(mockC)
		assert.Equal(t, false, succeed2)
		assert.Error(t, err2)
		assert.True(t, errors.Is(err2, merr.ErrDuplicatedCompactionTask))

		assert.EqualValues(t, 1, len(executor.taskCh))
		assert.EqualValues(t, 1, executor.executing.Len())

		mockC.EXPECT().Stop().Return().Once()
		executor.stopTask(planID)
	})

	t.Run("Test execute task with slot=0", func(t *testing.T) {
		paramtable.Get().Init(paramtable.NewBaseTable())
		planID := int64(1)
		mockC := NewMockCompactor(t)
		mockC.EXPECT().GetPlanID().Return(planID)
		mockC.EXPECT().GetChannelName().Return("ch1")
		mockC.EXPECT().GetCompactionType().Return(datapb.CompactionType_MixCompaction)
		mockC.EXPECT().GetSlotUsage().Return(0)
		executor := NewExecutor()

		succeed, err := executor.Enqueue(mockC)
		assert.Equal(t, true, succeed)
		assert.NoError(t, err)
		assert.Equal(t, int64(4), executor.Slots())
		assert.Equal(t, int64(4), executor.usingSlots)

		assert.EqualValues(t, 1, len(executor.taskCh))
		assert.EqualValues(t, 1, executor.executing.Len())

		mockC.EXPECT().Stop().Return().Once()
		executor.stopTask(planID)
	})

	t.Run("Test Start", func(t *testing.T) {
		ex := NewExecutor()
		ctx, cancel := context.WithCancel(context.TODO())
		cancel()
		go ex.Start(ctx)
	})

	t.Run("Test executeTask", func(t *testing.T) {
		tests := []struct {
			isvalid bool

			description string
		}{
			{true, "compact success"},
			{false, "compact return error"},
		}

		ex := NewExecutor()
		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				mockC := NewMockCompactor(t)
				mockC.EXPECT().GetPlanID().Return(int64(1))
				mockC.EXPECT().GetCollection().Return(int64(1))
				mockC.EXPECT().GetChannelName().Return("ch1")
				mockC.EXPECT().Complete().Return().Maybe()
				signal := make(chan struct{})
				if test.isvalid {
					mockC.EXPECT().Compact().RunAndReturn(
						func() (*datapb.CompactionPlanResult, error) {
							signal <- struct{}{}
							return &datapb.CompactionPlanResult{PlanID: 1}, nil
						}).Once()
					go ex.executeTask(mockC)
					<-signal
				} else {
					mockC.EXPECT().Compact().RunAndReturn(
						func() (*datapb.CompactionPlanResult, error) {
							signal <- struct{}{}
							return nil, errors.New("mock error")
						}).Once()
					go ex.executeTask(mockC)
					<-signal
				}
			})
		}
	})

	t.Run("test GetAllCompactionResults", func(t *testing.T) {
		ex := NewExecutor()

		mockC := NewMockCompactor(t)
		ex.executing.Insert(int64(1), mockC)

		ex.completedCompactor.Insert(int64(2), mockC)
		ex.completed.Insert(int64(2), &datapb.CompactionPlanResult{
			PlanID: 2,
			State:  datapb.CompactionTaskState_completed,
			Type:   datapb.CompactionType_MixCompaction,
		})

		ex.completedCompactor.Insert(int64(3), mockC)
		ex.completed.Insert(int64(3), &datapb.CompactionPlanResult{
			PlanID: 3,
			State:  datapb.CompactionTaskState_completed,
			Type:   datapb.CompactionType_Level0DeleteCompaction,
		})

		require.Equal(t, 2, ex.completed.Len())
		require.Equal(t, 2, ex.completedCompactor.Len())
		require.Equal(t, 1, ex.executing.Len())

		result := ex.GetResults(0)
		assert.Equal(t, 3, len(result))

		for _, res := range result {
			if res.PlanID == int64(1) {
				assert.Equal(t, res.GetState(), datapb.CompactionTaskState_executing)
			} else {
				assert.Equal(t, res.GetState(), datapb.CompactionTaskState_completed)
			}
		}

		assert.Equal(t, 1, ex.completed.Len())
		require.Equal(t, 1, ex.completedCompactor.Len())
		require.Equal(t, 1, ex.executing.Len())
	})
}

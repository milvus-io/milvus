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
		succeed, err := executor.Execute(mockC)
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
		succeed, err := executor.Execute(mockC)
		assert.Equal(t, true, succeed)
		assert.NoError(t, err)

		succeed2, err2 := executor.Execute(mockC)
		assert.Equal(t, false, succeed2)
		assert.Error(t, err2)
		assert.True(t, errors.Is(err2, merr.ErrDuplicatedCompactionTask))

		assert.EqualValues(t, 1, len(executor.taskCh))
		assert.EqualValues(t, 1, executor.executing.Len())

		mockC.EXPECT().Stop().Return().Once()
		executor.stopTask(planID)
	})

	t.Run("Test execute task slot usage larger than free slop", func(t *testing.T) {
		paramtable.Get().Init(paramtable.NewBaseTable())
		mockC := NewMockCompactor(t)
		mockC.EXPECT().GetSlotUsage().Return(100)
		executor := NewExecutor()

		succeed, err := executor.Execute(mockC)
		assert.Equal(t, false, succeed)
		assert.True(t, errors.Is(err, merr.ErrDataNodeSlotExhausted))

		assert.EqualValues(t, 0, len(executor.taskCh))
		assert.EqualValues(t, 0, executor.executing.Len())
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

		succeed, err := executor.Execute(mockC)
		assert.Equal(t, true, succeed)
		assert.NoError(t, err)
		assert.Equal(t, int64(8), executor.Slots())
		assert.Equal(t, int64(8), executor.usingSlots)

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

	t.Run("Test channel valid check", func(t *testing.T) {
		tests := []struct {
			expected bool
			channel  string
			desc     string
		}{
			{expected: true, channel: "ch1", desc: "no in dropped"},
			{expected: false, channel: "ch2", desc: "in dropped"},
		}
		ex := NewExecutor()
		ex.DiscardByDroppedChannel("ch2")
		for _, test := range tests {
			t.Run(test.desc, func(t *testing.T) {
				assert.Equal(t, test.expected, ex.isValidChannel(test.channel))
			})
		}
	})

	t.Run("test stop vchannel tasks", func(t *testing.T) {
		ex := NewExecutor()
		mc := NewMockCompactor(t)
		mc.EXPECT().GetPlanID().Return(int64(1))
		mc.EXPECT().GetChannelName().Return("mock")
		mc.EXPECT().Compact().Return(&datapb.CompactionPlanResult{PlanID: 1}, nil).Maybe()
		mc.EXPECT().GetSlotUsage().Return(8)
		mc.EXPECT().Stop().Return().Once()

		ex.Execute(mc)

		require.True(t, ex.executing.Contain(int64(1)))

		ex.DiscardByDroppedChannel("mock")
		assert.True(t, ex.dropped.Contain("mock"))
		assert.False(t, ex.executing.Contain(int64(1)))
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

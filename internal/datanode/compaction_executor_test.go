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

package datanode

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datanode/compaction"
	"github.com/milvus-io/milvus/internal/proto/datapb"
)

func TestCompactionExecutor(t *testing.T) {
	t.Run("Test execute", func(t *testing.T) {
		planID := int64(1)
		mockC := compaction.NewMockCompactor(t)
		mockC.EXPECT().GetPlanID().Return(planID).Once()
		mockC.EXPECT().GetChannelName().Return("ch1").Once()
		executor := newCompactionExecutor()
		executor.execute(mockC)

		assert.EqualValues(t, 1, len(executor.taskCh))
		assert.EqualValues(t, 1, executor.executing.Len())

		mockC.EXPECT().Stop().Return().Once()
		executor.stopTask(planID)
	})

	t.Run("Test start", func(t *testing.T) {
		ex := newCompactionExecutor()
		ctx, cancel := context.WithCancel(context.TODO())
		cancel()
		go ex.start(ctx)
	})

	t.Run("Test executeTask", func(t *testing.T) {
		tests := []struct {
			isvalid bool

			description string
		}{
			{true, "compact success"},
			{false, "compact return error"},
		}

		ex := newCompactionExecutor()
		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				mockC := compaction.NewMockCompactor(t)
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
					ex.executeWithState(mockC)
					<-signal
				} else {
					mockC.EXPECT().Compact().RunAndReturn(
						func() (*datapb.CompactionPlanResult, error) {
							signal <- struct{}{}
							return nil, errors.New("mock error")
						}).Once()
					ex.executeWithState(mockC)
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
		ex := newCompactionExecutor()
		ex.discardByDroppedChannel("ch2")
		for _, test := range tests {
			t.Run(test.desc, func(t *testing.T) {
				assert.Equal(t, test.expected, ex.isValidChannel(test.channel))
			})
		}
	})

	t.Run("test stop vchannel tasks", func(t *testing.T) {
		ex := newCompactionExecutor()
		mc := compaction.NewMockCompactor(t)
		mc.EXPECT().GetPlanID().Return(int64(1))
		mc.EXPECT().GetChannelName().Return("mock")
		mc.EXPECT().Compact().Return(&datapb.CompactionPlanResult{PlanID: 1}, nil).Maybe()
		mc.EXPECT().Stop().Return().Once()

		ex.execute(mc)

		require.True(t, ex.executing.Contain(int64(1)))

		ex.discardByDroppedChannel("mock")
		assert.True(t, ex.dropped.Contain("mock"))
		assert.False(t, ex.executing.Contain(int64(1)))
	})

	t.Run("test getAllCompactionResults", func(t *testing.T) {
		ex := newCompactionExecutor()

		mockC := compaction.NewMockCompactor(t)
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

		result := ex.getAllCompactionResults()
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

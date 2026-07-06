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
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestSplitTaskStateLabel(t *testing.T) {
	assert.Equal(t, "preparing", splitTaskStateLabel(datapb.SplitShardTaskState_SplitShardTaskPreparing))
	assert.Equal(t, "redistributing", splitTaskStateLabel(datapb.SplitShardTaskState_SplitShardTaskRedistributing))
	assert.Equal(t, "aborted", splitTaskStateLabel(datapb.SplitShardTaskState_SplitShardTaskAborted))
}

func TestShardSplitRefreshMetrics(t *testing.T) {
	metrics.DataCoordShardSplitTaskNum.Reset()
	m := &shardSplitManager{tasks: typeutil.NewConcurrentMap[int64, *datapb.SplitShardTask]()}
	m.tasks.Insert(1, &datapb.SplitShardTask{TaskId: 1, State: datapb.SplitShardTaskState_SplitShardTaskRedistributing})
	m.tasks.Insert(2, &datapb.SplitShardTask{TaskId: 2, State: datapb.SplitShardTaskState_SplitShardTaskRedistributing})
	m.tasks.Insert(3, &datapb.SplitShardTask{TaskId: 3, State: datapb.SplitShardTaskState_SplitShardTaskPreparing})
	// terminal tasks are not counted in the active-state gauge.
	m.tasks.Insert(4, &datapb.SplitShardTask{TaskId: 4, State: datapb.SplitShardTaskState_SplitShardTaskDone})

	m.refreshMetrics()

	assert.Equal(t, float64(2), testutil.ToFloat64(metrics.DataCoordShardSplitTaskNum.WithLabelValues("redistributing")))
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.DataCoordShardSplitTaskNum.WithLabelValues("preparing")))
	assert.Equal(t, float64(0), testutil.ToFloat64(metrics.DataCoordShardSplitTaskNum.WithLabelValues("fencing")))

	// a state that empties out is reset back to zero on the next refresh.
	m.tasks.Remove(3)
	m.refreshMetrics()
	assert.Equal(t, float64(0), testutil.ToFloat64(metrics.DataCoordShardSplitTaskNum.WithLabelValues("preparing")))
}

func TestShardSplitRecordTerminalMetrics(t *testing.T) {
	metrics.DataCoordShardSplitTaskTotal.Reset()
	m := &shardSplitManager{}
	m.recordTerminalMetrics(&datapb.SplitShardTask{
		State:     datapb.SplitShardTaskState_SplitShardTaskAborted,
		StartTime: 100,
		EndTime:   130,
	})
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.DataCoordShardSplitTaskTotal.WithLabelValues("aborted")))
}

func TestAbortTaskRecordsMetrics(t *testing.T) {
	metrics.DataCoordShardSplitTaskTotal.Reset()
	m := newSplitTestMeta(true, "v0", nil)
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListSplitShardTask(mock.Anything).Return(nil, nil).Once()
	catalog.EXPECT().SaveSplitShardTask(mock.Anything, mock.Anything).Return(nil).Once()
	manager, err := newShardSplitManager(context.Background(), m, catalog, allocator.NewMockAllocator(t), nil, nil, nil)
	assert.NoError(t, err)

	task := &datapb.SplitShardTask{
		TaskId:         7,
		CollectionId:   1,
		SourceVchannel: "v0",
		State:          datapb.SplitShardTaskState_SplitShardTaskPreparing,
	}
	manager.tasks.Insert(task.GetTaskId(), task)
	manager.abortTask(task, "test abort")

	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.DataCoordShardSplitTaskTotal.WithLabelValues("aborted")))
	got, _ := manager.tasks.Get(task.GetTaskId())
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAborted, got.GetState())
}

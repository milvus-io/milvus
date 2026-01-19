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
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
)

func TestCompactionTaskMetaSuite(t *testing.T) {
	suite.Run(t, new(CompactionTaskMetaSuite))
}

type CompactionTaskMetaSuite struct {
	suite.Suite
	catalog *mocks.DataCoordCatalog
	meta    *compactionTaskMeta
}

func (suite *CompactionTaskMetaSuite) SetupTest() {
	catalog := mocks.NewDataCoordCatalog(suite.T())
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	suite.catalog = catalog
	meta, err := newCompactionTaskMeta(context.TODO(), catalog)
	suite.NoError(err)
	suite.meta = meta
}

func newTestCompactionTaskMeta(t *testing.T) *compactionTaskMeta {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	meta, _ := newCompactionTaskMeta(context.TODO(), catalog)
	return meta
}

func (suite *CompactionTaskMetaSuite) TestGetCompactionTasksByCollection() {
	suite.meta.SaveCompactionTask(context.TODO(), &datapb.CompactionTask{
		TriggerID:    1,
		PlanID:       10,
		CollectionID: 100,
	})
	res := suite.meta.GetCompactionTasksByCollection(100)
	suite.Equal(1, len(res))
}

func (suite *CompactionTaskMetaSuite) TestGetCompactionTasksByCollectionAbnormal() {
	suite.meta.SaveCompactionTask(context.TODO(), &datapb.CompactionTask{
		TriggerID:    1,
		PlanID:       10,
		CollectionID: 100,
	})
	suite.meta.SaveCompactionTask(context.TODO(), &datapb.CompactionTask{
		TriggerID:    2,
		PlanID:       11,
		CollectionID: 101,
	})
	res := suite.meta.GetCompactionTasksByCollection(101)
	suite.Equal(1, len(res))
}

func (suite *CompactionTaskMetaSuite) TestTaskStatsJSON() {
	task1 := &datapb.CompactionTask{
		PlanID:         1,
		CollectionID:   100,
		Type:           datapb.CompactionType_MergeCompaction,
		State:          datapb.CompactionTaskState_completed,
		FailReason:     "",
		StartTime:      time.Now().Unix(),
		EndTime:        time.Now().Add(time.Hour).Unix(),
		TotalRows:      1000,
		InputSegments:  []int64{1, 2},
		ResultSegments: []int64{3},
	}
	task2 := &datapb.CompactionTask{
		PlanID:         2,
		CollectionID:   101,
		Type:           datapb.CompactionType_MergeCompaction,
		State:          datapb.CompactionTaskState_completed,
		FailReason:     "",
		StartTime:      time.Now().Unix(),
		EndTime:        time.Now().Add(time.Hour).Unix(),
		TotalRows:      2000,
		InputSegments:  []int64{4, 5},
		ResultSegments: []int64{6},
	}

	// testing return empty string
	actualJSON := suite.meta.TaskStatsJSON()
	suite.Equal("[]", actualJSON)

	err := suite.meta.SaveCompactionTask(context.TODO(), task1)
	suite.NoError(err)
	err = suite.meta.SaveCompactionTask(context.TODO(), task2)
	suite.NoError(err)

	expectedTasks := []*metricsinfo.CompactionTask{
		newCompactionTaskStats(task1),
		newCompactionTaskStats(task2),
	}
	expectedJSON, err := json.Marshal(expectedTasks)
	suite.NoError(err)

	actualJSON = suite.meta.TaskStatsJSON()
	suite.JSONEq(string(expectedJSON), actualJSON)
}

// TestReloadFromKV_PreAllocatedSegmentIDsCompatibility verifies that compatibility
// logic in reloadFromKV does NOT mark Level0DeleteCompaction tasks as failed when
// PreAllocatedSegmentIDs is nil, while still failing other unfinished tasks that
// require pre-allocated segment IDs.
func (suite *CompactionTaskMetaSuite) TestReloadFromKV_PreAllocatedSegmentIDsCompatibility() {
	// L0 delete compaction task does not use PreAllocatedSegmentIDs.
	l0Task := &datapb.CompactionTask{
		PlanID:    1,
		TriggerID: 1,
		Type:      datapb.CompactionType_Level0DeleteCompaction,
		State:     datapb.CompactionTaskState_executing,
	}

	// Clustering compaction task should require PreAllocatedSegmentIDs and be
	// marked as failed when the field is nil.
	clusteringTask := &datapb.CompactionTask{
		PlanID:    2,
		TriggerID: 2,
		Type:      datapb.CompactionType_ClusteringCompaction,
		State:     datapb.CompactionTaskState_executing,
	}

	catalog := mocks.NewDataCoordCatalog(suite.T())
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return([]*datapb.CompactionTask{l0Task, clusteringTask}, nil).Once()

	meta, err := newCompactionTaskMeta(context.TODO(), catalog)
	suite.NoError(err)

	l0Tasks := meta.GetCompactionTasksByTriggerID(1)
	suite.Equal(1, len(l0Tasks))
	suite.Equal(datapb.CompactionTaskState_executing, l0Tasks[0].State)

	clusteringTasks := meta.GetCompactionTasksByTriggerID(2)
	suite.Equal(1, len(clusteringTasks))
	suite.Equal(datapb.CompactionTaskState_failed, clusteringTasks[0].State)
}

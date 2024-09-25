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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/workerpb"
)

type AnalyzeMetaSuite struct {
	suite.Suite

	collectionID int64
	partitionID  int64
	fieldID      int64
	segmentIDs   []int64
}

func (s *AnalyzeMetaSuite) initParams() {
	s.collectionID = 100
	s.partitionID = 101
	s.fieldID = 102
	s.segmentIDs = []int64{1000, 1001, 1002, 1003}
}

func (s *AnalyzeMetaSuite) Test_AnalyzeMeta() {
	s.initParams()

	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return([]*indexpb.AnalyzeTask{
		{
			CollectionID: s.collectionID,
			PartitionID:  s.partitionID,
			FieldID:      s.fieldID,
			SegmentIDs:   s.segmentIDs,
			TaskID:       1,
			State:        indexpb.JobState_JobStateNone,
		},
		{
			CollectionID: s.collectionID,
			PartitionID:  s.partitionID,
			FieldID:      s.fieldID,
			SegmentIDs:   s.segmentIDs,
			TaskID:       2,
			State:        indexpb.JobState_JobStateInit,
		},
		{
			CollectionID: s.collectionID,
			PartitionID:  s.partitionID,
			FieldID:      s.fieldID,
			SegmentIDs:   s.segmentIDs,
			TaskID:       3,
			State:        indexpb.JobState_JobStateInProgress,
		},
		{
			CollectionID: s.collectionID,
			PartitionID:  s.partitionID,
			FieldID:      s.fieldID,
			SegmentIDs:   s.segmentIDs,
			TaskID:       4,
			State:        indexpb.JobState_JobStateRetry,
		},
		{
			CollectionID: s.collectionID,
			PartitionID:  s.partitionID,
			FieldID:      s.fieldID,
			SegmentIDs:   s.segmentIDs,
			TaskID:       5,
			State:        indexpb.JobState_JobStateFinished,
		},
		{
			CollectionID: s.collectionID,
			PartitionID:  s.partitionID,
			FieldID:      s.fieldID,
			SegmentIDs:   s.segmentIDs,
			TaskID:       6,
			State:        indexpb.JobState_JobStateFailed,
		},
	}, nil)

	catalog.EXPECT().SaveAnalyzeTask(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().DropAnalyzeTask(mock.Anything, mock.Anything).Return(nil)

	ctx := context.Background()

	am, err := newAnalyzeMeta(ctx, catalog)
	s.NoError(err)
	s.Equal(6, len(am.GetAllTasks()))

	s.Run("GetTask", func() {
		t := am.GetTask(1)
		s.NotNil(t)

		t = am.GetTask(100)
		s.Nil(t)
	})

	s.Run("AddAnalyzeTask", func() {
		t := &indexpb.AnalyzeTask{
			CollectionID: s.collectionID,
			PartitionID:  s.partitionID,
			FieldID:      s.fieldID,
			SegmentIDs:   s.segmentIDs,
			TaskID:       7,
		}

		err := am.AddAnalyzeTask(t)
		s.NoError(err)
		s.Equal(7, len(am.GetAllTasks()))

		err = am.AddAnalyzeTask(t)
		s.NoError(err)
		s.Equal(7, len(am.GetAllTasks()))
	})

	s.Run("DropAnalyzeTask", func() {
		err := am.DropAnalyzeTask(7)
		s.NoError(err)
		s.Equal(6, len(am.GetAllTasks()))
	})

	s.Run("UpdateVersion", func() {
		err := am.UpdateVersion(1)
		s.NoError(err)
		s.Equal(int64(1), am.GetTask(1).Version)
	})

	s.Run("BuildingTask", func() {
		err := am.BuildingTask(1, 1)
		s.NoError(err)
		s.Equal(indexpb.JobState_JobStateInProgress, am.GetTask(1).State)
	})

	s.Run("FinishTask", func() {
		err := am.FinishTask(1, &workerpb.AnalyzeResult{
			TaskID: 1,
			State:  indexpb.JobState_JobStateFinished,
		})
		s.NoError(err)
		s.Equal(indexpb.JobState_JobStateFinished, am.GetTask(1).State)
	})
}

func (s *AnalyzeMetaSuite) Test_failCase() {
	s.initParams()

	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, errors.New("error")).Once()
	ctx := context.Background()
	am, err := newAnalyzeMeta(ctx, catalog)
	s.Error(err)
	s.Nil(am)

	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return([]*indexpb.AnalyzeTask{
		{
			CollectionID: s.collectionID,
			PartitionID:  s.partitionID,
			FieldID:      s.fieldID,
			SegmentIDs:   s.segmentIDs,
			TaskID:       1,
			State:        indexpb.JobState_JobStateInit,
		},
		{
			CollectionID: s.collectionID,
			PartitionID:  s.partitionID,
			FieldID:      s.fieldID,
			SegmentIDs:   s.segmentIDs,
			TaskID:       2,
			State:        indexpb.JobState_JobStateFinished,
		},
	}, nil)
	am, err = newAnalyzeMeta(ctx, catalog)
	s.NoError(err)
	s.NotNil(am)
	s.Equal(2, len(am.GetAllTasks()))

	catalog.EXPECT().SaveAnalyzeTask(mock.Anything, mock.Anything).Return(errors.New("error"))
	catalog.EXPECT().DropAnalyzeTask(mock.Anything, mock.Anything).Return(errors.New("error"))
	s.Run("AddAnalyzeTask", func() {
		t := &indexpb.AnalyzeTask{
			CollectionID: s.collectionID,
			PartitionID:  s.partitionID,
			FieldID:      s.fieldID,
			SegmentIDs:   s.segmentIDs,
			TaskID:       1111,
		}
		err := am.AddAnalyzeTask(t)
		s.Error(err)
		s.Nil(am.GetTask(1111))
	})

	s.Run("DropAnalyzeTask", func() {
		err := am.DropAnalyzeTask(1)
		s.Error(err)
		s.NotNil(am.GetTask(1))
	})

	s.Run("UpdateVersion", func() {
		err := am.UpdateVersion(777)
		s.Error(err)

		err = am.UpdateVersion(1)
		s.Error(err)
		s.Equal(int64(0), am.GetTask(1).Version)
	})

	s.Run("BuildingTask", func() {
		err := am.BuildingTask(777, 1)
		s.Error(err)

		err = am.BuildingTask(1, 1)
		s.Error(err)
		s.Equal(int64(0), am.GetTask(1).NodeID)
		s.Equal(indexpb.JobState_JobStateInit, am.GetTask(1).State)
	})

	s.Run("FinishTask", func() {
		err := am.FinishTask(777, nil)
		s.Error(err)

		err = am.FinishTask(1, &workerpb.AnalyzeResult{
			TaskID: 1,
			State:  indexpb.JobState_JobStateFinished,
		})
		s.Error(err)
		s.Equal(indexpb.JobState_JobStateInit, am.GetTask(1).State)
	})

	s.Run("CheckCleanAnalyzeTask", func() {
		canRecycle, t := am.CheckCleanAnalyzeTask(1)
		s.False(canRecycle)
		s.Equal(indexpb.JobState_JobStateInit, t.GetState())

		canRecycle, t = am.CheckCleanAnalyzeTask(777)
		s.True(canRecycle)
		s.Nil(t)

		canRecycle, t = am.CheckCleanAnalyzeTask(2)
		s.True(canRecycle)
		s.Equal(indexpb.JobState_JobStateFinished, t.GetState())
	})
}

func TestAnalyzeMeta(t *testing.T) {
	suite.Run(t, new(AnalyzeMetaSuite))
}

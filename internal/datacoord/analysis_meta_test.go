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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
)

type AnalysisMetaSuite struct {
	suite.Suite

	collectionID int64
	partitionID  int64
	fieldID      int64
	segmentIDs   []int64
}

func (s *AnalysisMetaSuite) initParams() {
	s.collectionID = 100
	s.partitionID = 101
	s.fieldID = 102
	s.segmentIDs = []int64{1000, 1001, 1002, 1003}
}

func (s *AnalysisMetaSuite) Test_analysisMeta() {
	s.initParams()

	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().ListAnalysisTasks(mock.Anything).Return([]*model.AnalysisTask{
		{
			CollectionID: s.collectionID,
			PartitionID:  s.partitionID,
			FieldID:      s.fieldID,
			SegmentIDs:   s.segmentIDs,
			TaskID:       1,
			State:        commonpb.IndexState_IndexStateNone,
		},
		{
			CollectionID: s.collectionID,
			PartitionID:  s.partitionID,
			FieldID:      s.fieldID,
			SegmentIDs:   s.segmentIDs,
			TaskID:       2,
			State:        commonpb.IndexState_Unissued,
		},
		{
			CollectionID: s.collectionID,
			PartitionID:  s.partitionID,
			FieldID:      s.fieldID,
			SegmentIDs:   s.segmentIDs,
			TaskID:       3,
			State:        commonpb.IndexState_InProgress,
		},
		{
			CollectionID: s.collectionID,
			PartitionID:  s.partitionID,
			FieldID:      s.fieldID,
			SegmentIDs:   s.segmentIDs,
			TaskID:       4,
			State:        commonpb.IndexState_Retry,
		},
		{
			CollectionID: s.collectionID,
			PartitionID:  s.partitionID,
			FieldID:      s.fieldID,
			SegmentIDs:   s.segmentIDs,
			TaskID:       5,
			State:        commonpb.IndexState_Finished,
		},
		{
			CollectionID: s.collectionID,
			PartitionID:  s.partitionID,
			FieldID:      s.fieldID,
			SegmentIDs:   s.segmentIDs,
			TaskID:       6,
			State:        commonpb.IndexState_Failed,
		},
	}, nil)

	catalog.EXPECT().SaveAnalysisTask(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().DropAnalysisTask(mock.Anything, mock.Anything).Return(nil)

	ctx := context.Background()

	am, err := newAnalysisMeta(ctx, catalog)
	s.NoError(err)
	s.Equal(6, len(am.GetAllTasks()))

	s.Run("GetTask", func() {
		t := am.GetTask(1)
		s.NotNil(t)

		t = am.GetTask(100)
		s.Nil(t)
	})

	s.Run("AddAnalysisTask", func() {
		t := &model.AnalysisTask{
			CollectionID: s.collectionID,
			PartitionID:  s.partitionID,
			FieldID:      s.fieldID,
			SegmentIDs:   s.segmentIDs,
			TaskID:       7,
		}

		err := am.AddAnalysisTask(t)
		s.NoError(err)
		s.Equal(7, len(am.GetAllTasks()))

		err = am.AddAnalysisTask(t)
		s.NoError(err)
		s.Equal(7, len(am.GetAllTasks()))
	})

	s.Run("DropAnalysisTask", func() {
		err := am.DropAnalysisTask(7)
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
		s.Equal(commonpb.IndexState_InProgress, am.GetTask(1).State)
	})

	s.Run("FinishTask", func() {
		err := am.FinishTask(1, &indexpb.AnalysisResult{
			TaskID:                    1,
			State:                     commonpb.IndexState_Finished,
			CentroidsFile:             "a/b/c",
			SegmentOffsetMappingFiles: map[int64]string{1000: "1000/a", 1001: "1001/a", 1002: "1002/a", 1003: "1003/a"},
		})
		s.NoError(err)
		s.Equal(commonpb.IndexState_Finished, am.GetTask(1).State)
		s.Equal("a/b/c", am.GetTask(1).CentroidsFile)
	})
}

func (s *AnalysisMetaSuite) Test_failCase() {
	s.initParams()

	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().ListAnalysisTasks(mock.Anything).Return(nil, errors.New("error")).Once()
	ctx := context.Background()
	am, err := newAnalysisMeta(ctx, catalog)
	s.Error(err)
	s.Nil(am)

	catalog.EXPECT().ListAnalysisTasks(mock.Anything).Return([]*model.AnalysisTask{
		{
			CollectionID: s.collectionID,
			PartitionID:  s.partitionID,
			FieldID:      s.fieldID,
			SegmentIDs:   s.segmentIDs,
			TaskID:       1,
			State:        commonpb.IndexState_Unissued,
		},
	}, nil)
	am, err = newAnalysisMeta(ctx, catalog)
	s.NoError(err)
	s.NotNil(am)
	s.Equal(1, len(am.GetAllTasks()))

	catalog.EXPECT().SaveAnalysisTask(mock.Anything, mock.Anything).Return(errors.New("error"))
	catalog.EXPECT().DropAnalysisTask(mock.Anything, mock.Anything).Return(errors.New("error"))
	s.Run("AddAnalysisTask", func() {
		t := &model.AnalysisTask{
			TenantID:     "",
			CollectionID: s.collectionID,
			PartitionID:  s.partitionID,
			FieldID:      s.fieldID,
			SegmentIDs:   s.segmentIDs,
			TaskID:       1111,
		}
		err := am.AddAnalysisTask(t)
		s.Error(err)
		s.Nil(am.GetTask(1111))
	})

	s.Run("DropAnalysisTask", func() {
		err := am.DropAnalysisTask(1)
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
		s.Equal(commonpb.IndexState_Unissued, am.GetTask(1).State)
	})

	s.Run("FinishTask", func() {
		err := am.FinishTask(777, nil)
		s.Error(err)

		err = am.FinishTask(1, &indexpb.AnalysisResult{
			TaskID:                    1,
			State:                     commonpb.IndexState_Finished,
			CentroidsFile:             "a/b/c",
			SegmentOffsetMappingFiles: map[int64]string{1000: "1000/a", 1001: "1001/a", 1002: "1002/a", 1003: "1003/a"},
		})
		s.Error(err)
		s.Equal(commonpb.IndexState_Unissued, am.GetTask(1).State)
		s.Equal("", am.GetTask(1).CentroidsFile)
		s.Equal(0, len(am.GetTask(1).SegmentOffsetMappingFiles))
	})
}

func TestAnalysisMeta(t *testing.T) {
	suite.Run(t, new(AnalysisMetaSuite))
}

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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/metastore"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/indexparamcheck"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var (
	collID         = UniqueID(100)
	partID         = UniqueID(200)
	indexID        = UniqueID(300)
	fieldID        = UniqueID(400)
	indexName      = "_default_idx"
	segID          = UniqueID(500)
	buildID        = UniqueID(600)
	nodeID         = UniqueID(700)
	partitionKeyID = UniqueID(800)
)

func createIndexMeta(catalog metastore.DataCoordCatalog) *indexMeta {
	return &indexMeta{
		catalog: catalog,
		indexes: map[UniqueID]map[UniqueID]*model.Index{
			collID: {
				indexID: {
					TenantID:     "",
					CollectionID: collID,
					FieldID:      fieldID,
					IndexID:      indexID,
					IndexName:    indexName,
					IsDeleted:    false,
					CreateTime:   1,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "128",
						},
					},
					IndexParams: []*commonpb.KeyValuePair{
						{
							Key:   common.IndexTypeKey,
							Value: "HNSW",
						},
						{
							Key:   common.MetricTypeKey,
							Value: "L2",
						},
					},
				},
			},
		},
		segmentIndexes: map[UniqueID]map[UniqueID]*model.SegmentIndex{
			segID: {
				indexID: {
					SegmentID:     segID,
					CollectionID:  collID,
					PartitionID:   partID,
					NumRows:       1025,
					IndexID:       indexID,
					BuildID:       buildID,
					NodeID:        0,
					IndexVersion:  0,
					IndexState:    commonpb.IndexState_Unissued,
					FailReason:    "",
					IsDeleted:     false,
					CreateTime:    0,
					IndexFileKeys: nil,
					IndexSize:     1,
				},
			},
			segID + 1: {
				indexID: {
					SegmentID:     segID + 1,
					CollectionID:  collID,
					PartitionID:   partID,
					NumRows:       1026,
					IndexID:       indexID,
					BuildID:       buildID + 1,
					NodeID:        nodeID,
					IndexVersion:  1,
					IndexState:    commonpb.IndexState_InProgress,
					FailReason:    "",
					IsDeleted:     false,
					CreateTime:    1111,
					IndexFileKeys: nil,
					IndexSize:     1,
				},
			},
			segID + 2: {
				indexID: {
					SegmentID:     segID + 2,
					CollectionID:  collID,
					PartitionID:   partID,
					NumRows:       1026,
					IndexID:       indexID,
					BuildID:       buildID + 2,
					NodeID:        nodeID,
					IndexVersion:  1,
					IndexState:    commonpb.IndexState_InProgress,
					FailReason:    "",
					IsDeleted:     true,
					CreateTime:    1111,
					IndexFileKeys: nil,
					IndexSize:     1,
				},
			},
			segID + 3: {
				indexID: {
					SegmentID:     segID + 3,
					CollectionID:  collID,
					PartitionID:   partID,
					NumRows:       500,
					IndexID:       indexID,
					BuildID:       buildID + 3,
					NodeID:        0,
					IndexVersion:  0,
					IndexState:    commonpb.IndexState_Unissued,
					FailReason:    "",
					IsDeleted:     false,
					CreateTime:    1111,
					IndexFileKeys: nil,
					IndexSize:     1,
				},
			},
			segID + 4: {
				indexID: {
					SegmentID:     segID + 4,
					CollectionID:  collID,
					PartitionID:   partID,
					NumRows:       1026,
					IndexID:       indexID,
					BuildID:       buildID + 4,
					NodeID:        nodeID,
					IndexVersion:  1,
					IndexState:    commonpb.IndexState_Finished,
					FailReason:    "",
					IsDeleted:     false,
					CreateTime:    1111,
					IndexFileKeys: nil,
					IndexSize:     1,
				},
			},
			segID + 5: {
				indexID: {
					SegmentID:     segID + 5,
					CollectionID:  collID,
					PartitionID:   partID,
					NumRows:       1026,
					IndexID:       indexID,
					BuildID:       buildID + 5,
					NodeID:        0,
					IndexVersion:  1,
					IndexState:    commonpb.IndexState_Finished,
					FailReason:    "",
					IsDeleted:     false,
					CreateTime:    1111,
					IndexFileKeys: nil,
					IndexSize:     1,
				},
			},
			segID + 6: {
				indexID: {
					SegmentID:     segID + 6,
					CollectionID:  collID,
					PartitionID:   partID,
					NumRows:       1026,
					IndexID:       indexID,
					BuildID:       buildID + 6,
					NodeID:        0,
					IndexVersion:  1,
					IndexState:    commonpb.IndexState_Finished,
					FailReason:    "",
					IsDeleted:     false,
					CreateTime:    1111,
					IndexFileKeys: nil,
					IndexSize:     1,
				},
			},
			segID + 7: {
				indexID: {
					SegmentID:     segID + 7,
					CollectionID:  collID,
					PartitionID:   partID,
					NumRows:       1026,
					IndexID:       indexID,
					BuildID:       buildID + 7,
					NodeID:        0,
					IndexVersion:  1,
					IndexState:    commonpb.IndexState_Failed,
					FailReason:    "error",
					IsDeleted:     false,
					CreateTime:    1111,
					IndexFileKeys: nil,
					IndexSize:     1,
				},
			},
			segID + 8: {
				indexID: {
					SegmentID:     segID + 8,
					CollectionID:  collID,
					PartitionID:   partID,
					NumRows:       1026,
					IndexID:       indexID,
					BuildID:       buildID + 8,
					NodeID:        nodeID + 1,
					IndexVersion:  1,
					IndexState:    commonpb.IndexState_InProgress,
					FailReason:    "",
					IsDeleted:     false,
					CreateTime:    1111,
					IndexFileKeys: nil,
					IndexSize:     1,
				},
			},
			segID + 9: {
				indexID: {
					SegmentID:     segID + 9,
					CollectionID:  collID,
					PartitionID:   partID,
					NumRows:       500,
					IndexID:       indexID,
					BuildID:       buildID + 9,
					NodeID:        0,
					IndexVersion:  0,
					IndexState:    commonpb.IndexState_Unissued,
					FailReason:    "",
					IsDeleted:     false,
					CreateTime:    1111,
					IndexFileKeys: nil,
					IndexSize:     1,
				},
			},
			segID + 10: {
				indexID: {
					SegmentID:     segID + 10,
					CollectionID:  collID,
					PartitionID:   partID,
					NumRows:       500,
					IndexID:       indexID,
					BuildID:       buildID + 10,
					NodeID:        nodeID,
					IndexVersion:  0,
					IndexState:    commonpb.IndexState_Unissued,
					FailReason:    "",
					IsDeleted:     false,
					CreateTime:    1111,
					IndexFileKeys: nil,
					IndexSize:     1,
				},
			},
		},
		buildID2SegmentIndex: map[UniqueID]*model.SegmentIndex{
			buildID: {
				SegmentID:     segID,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       1025,
				IndexID:       indexID,
				BuildID:       buildID,
				NodeID:        0,
				IndexVersion:  0,
				IndexState:    commonpb.IndexState_Unissued,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    0,
				IndexFileKeys: nil,
				IndexSize:     1,
			},
			buildID + 1: {
				SegmentID:     segID + 1,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       1026,
				IndexID:       indexID,
				BuildID:       buildID + 1,
				NodeID:        nodeID,
				IndexVersion:  1,
				IndexState:    commonpb.IndexState_InProgress,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    1111,
				IndexFileKeys: nil,
				IndexSize:     1,
			},
			buildID + 2: {
				SegmentID:     segID + 2,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       1026,
				IndexID:       indexID,
				BuildID:       buildID + 2,
				NodeID:        nodeID,
				IndexVersion:  1,
				IndexState:    commonpb.IndexState_InProgress,
				FailReason:    "",
				IsDeleted:     true,
				CreateTime:    1111,
				IndexFileKeys: nil,
				IndexSize:     1,
			},
			buildID + 3: {
				SegmentID:     segID + 3,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       500,
				IndexID:       indexID,
				BuildID:       buildID + 3,
				NodeID:        0,
				IndexVersion:  0,
				IndexState:    commonpb.IndexState_Unissued,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    1111,
				IndexFileKeys: nil,
				IndexSize:     1,
			},
			buildID + 4: {
				SegmentID:     segID + 4,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       1026,
				IndexID:       indexID,
				BuildID:       buildID + 4,
				NodeID:        nodeID,
				IndexVersion:  1,
				IndexState:    commonpb.IndexState_Finished,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    1111,
				IndexFileKeys: nil,
				IndexSize:     1,
			},
			buildID + 5: {
				SegmentID:     segID + 5,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       1026,
				IndexID:       indexID,
				BuildID:       buildID + 5,
				NodeID:        0,
				IndexVersion:  1,
				IndexState:    commonpb.IndexState_Finished,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    1111,
				IndexFileKeys: nil,
				IndexSize:     1,
			},
			buildID + 6: {
				SegmentID:     segID + 6,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       1026,
				IndexID:       indexID,
				BuildID:       buildID + 6,
				NodeID:        0,
				IndexVersion:  1,
				IndexState:    commonpb.IndexState_Finished,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    1111,
				IndexFileKeys: nil,
				IndexSize:     1,
			},
			buildID + 7: {
				SegmentID:     segID + 7,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       1026,
				IndexID:       indexID,
				BuildID:       buildID + 7,
				NodeID:        0,
				IndexVersion:  1,
				IndexState:    commonpb.IndexState_Failed,
				FailReason:    "error",
				IsDeleted:     false,
				CreateTime:    1111,
				IndexFileKeys: nil,
				IndexSize:     1,
			},
			buildID + 8: {
				SegmentID:     segID + 8,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       1026,
				IndexID:       indexID,
				BuildID:       buildID + 8,
				NodeID:        nodeID + 1,
				IndexVersion:  1,
				IndexState:    commonpb.IndexState_InProgress,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    1111,
				IndexFileKeys: nil,
				IndexSize:     1,
			},
			buildID + 9: {
				SegmentID:     segID + 9,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       500,
				IndexID:       indexID,
				BuildID:       buildID + 9,
				NodeID:        0,
				IndexVersion:  0,
				IndexState:    commonpb.IndexState_Unissued,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    1111,
				IndexFileKeys: nil,
				IndexSize:     1,
			},
			buildID + 10: {
				SegmentID:     segID + 10,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       500,
				IndexID:       indexID,
				BuildID:       buildID + 10,
				NodeID:        nodeID,
				IndexVersion:  0,
				IndexState:    commonpb.IndexState_Unissued,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    1111,
				IndexFileKeys: nil,
				IndexSize:     1,
			},
		},
	}
}

type testMetaOption func(*meta)

func withAnalyzeMeta(am *analyzeMeta) testMetaOption {
	return func(mt *meta) {
		mt.analyzeMeta = am
	}
}

func withIndexMeta(im *indexMeta) testMetaOption {
	return func(mt *meta) {
		mt.indexMeta = im
	}
}

func withStatsTaskMeta(stm *statsTaskMeta) testMetaOption {
	return func(mt *meta) {
		mt.statsTaskMeta = stm
	}
}

func createMeta(catalog metastore.DataCoordCatalog, opts ...testMetaOption) *meta {
	mt := &meta{
		catalog: catalog,
		segments: &SegmentsInfo{
			segments: map[UniqueID]*SegmentInfo{
				1000: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:           1000,
						CollectionID: 10000,
						PartitionID:  10001,
						NumOfRows:    3000,
						State:        commonpb.SegmentState_Flushed,
						Binlogs:      []*datapb.FieldBinlog{{FieldID: 10002, Binlogs: []*datapb.Binlog{{LogID: 1}, {LogID: 2}, {LogID: 3}}}},
					},
				},
				1001: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:           1001,
						CollectionID: 10000,
						PartitionID:  10001,
						NumOfRows:    3000,
						State:        commonpb.SegmentState_Flushed,
						Binlogs:      []*datapb.FieldBinlog{{FieldID: 10002, Binlogs: []*datapb.Binlog{{LogID: 1}, {LogID: 2}, {LogID: 3}}}},
					},
				},
				1002: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:           1002,
						CollectionID: 10000,
						PartitionID:  10001,
						NumOfRows:    3000,
						State:        commonpb.SegmentState_Flushed,
						Binlogs:      []*datapb.FieldBinlog{{FieldID: 10002, Binlogs: []*datapb.Binlog{{LogID: 1}, {LogID: 2}, {LogID: 3}}}},
					},
				},
				segID: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      1025,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
				},
				segID + 1: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 1,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      1026,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
				},
				segID + 2: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 2,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      1026,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
				},
				segID + 3: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 3,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      500,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
				},
				segID + 4: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 4,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      1026,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
				},
				segID + 5: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 5,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      1026,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
				},
				segID + 6: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 6,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      1026,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
				},
				segID + 7: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 7,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      1026,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
				},
				segID + 8: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 8,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      1026,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
				},
				segID + 9: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 9,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      500,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
				},
				segID + 10: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 10,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      500,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
				},
			},
		},
	}

	for _, opt := range opts {
		opt(mt)
	}
	return mt
}

type taskSchedulerSuite struct {
	suite.Suite

	collectionID int64
	partitionID  int64
	fieldID      int64
	segmentIDs   []int64
	nodeID       int64
	duration     time.Duration
}

func (s *taskSchedulerSuite) initParams() {
	s.collectionID = collID
	s.partitionID = partID
	s.fieldID = fieldID
	s.nodeID = nodeID
	s.segmentIDs = []int64{1000, 1001, 1002}
	s.duration = time.Millisecond * 100
}

func (s *taskSchedulerSuite) createAnalyzeMeta(catalog metastore.DataCoordCatalog) *analyzeMeta {
	return &analyzeMeta{
		ctx:     context.Background(),
		catalog: catalog,
		tasks: map[int64]*indexpb.AnalyzeTask{
			1: {
				CollectionID: s.collectionID,
				PartitionID:  s.partitionID,
				FieldID:      s.fieldID,
				SegmentIDs:   s.segmentIDs,
				TaskID:       1,
				State:        indexpb.JobState_JobStateInit,
				FieldType:    schemapb.DataType_FloatVector,
			},
			2: {
				CollectionID: s.collectionID,
				PartitionID:  s.partitionID,
				FieldID:      s.fieldID,
				SegmentIDs:   s.segmentIDs,
				TaskID:       2,
				NodeID:       s.nodeID,
				State:        indexpb.JobState_JobStateInProgress,
				FieldType:    schemapb.DataType_FloatVector,
			},
			3: {
				CollectionID: s.collectionID,
				PartitionID:  s.partitionID,
				FieldID:      s.fieldID,
				SegmentIDs:   s.segmentIDs,
				TaskID:       3,
				NodeID:       s.nodeID,
				State:        indexpb.JobState_JobStateFinished,
				FieldType:    schemapb.DataType_FloatVector,
			},
			4: {
				CollectionID: s.collectionID,
				PartitionID:  s.partitionID,
				FieldID:      s.fieldID,
				SegmentIDs:   s.segmentIDs,
				TaskID:       4,
				NodeID:       s.nodeID,
				State:        indexpb.JobState_JobStateFailed,
				FieldType:    schemapb.DataType_FloatVector,
			},
			5: {
				CollectionID: s.collectionID,
				PartitionID:  s.partitionID,
				FieldID:      s.fieldID,
				SegmentIDs:   []int64{1001, 1002},
				TaskID:       5,
				NodeID:       s.nodeID,
				State:        indexpb.JobState_JobStateRetry,
				FieldType:    schemapb.DataType_FloatVector,
			},
		},
	}
}

func (s *taskSchedulerSuite) SetupSuite() {
	paramtable.Init()
	s.initParams()
	Params.DataCoordCfg.ClusteringCompactionMinCentroidsNum.SwapTempValue("0")
}

func (s *taskSchedulerSuite) TearDownSuite() {
	Params.DataCoordCfg.ClusteringCompactionMinCentroidsNum.SwapTempValue("16")
}

func (s *taskSchedulerSuite) scheduler(handler Handler) {
	ctx := context.Background()

	var once sync.Once

	paramtable.Get().Save(paramtable.Get().DataCoordCfg.TaskSlowThreshold.Key, "1")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.TaskSlowThreshold.Key)
	catalog := catalogmocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().SaveAnalyzeTask(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, task *indexpb.AnalyzeTask) error {
		once.Do(func() {
			time.Sleep(time.Second * 3)
		})
		return nil
	})
	catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil)
	//catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil)

	in := mocks.NewMockIndexNodeClient(s.T())
	in.EXPECT().CreateJobV2(mock.Anything, mock.Anything).Return(merr.Success(), nil)
	in.EXPECT().QueryJobsV2(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, request *workerpb.QueryJobsV2Request, option ...grpc.CallOption) (*workerpb.QueryJobsV2Response, error) {
			once.Do(func() {
				time.Sleep(time.Second * 3)
			})
			switch request.GetJobType() {
			case indexpb.JobType_JobTypeIndexJob:
				results := make([]*workerpb.IndexTaskInfo, 0)
				for _, buildID := range request.GetTaskIDs() {
					results = append(results, &workerpb.IndexTaskInfo{
						BuildID:             buildID,
						State:               commonpb.IndexState_Finished,
						IndexFileKeys:       []string{"file1", "file2", "file3"},
						SerializedSize:      1024,
						FailReason:          "",
						CurrentIndexVersion: 1,
						IndexStoreVersion:   1,
					})
				}
				return &workerpb.QueryJobsV2Response{
					Status:    merr.Success(),
					ClusterID: request.GetClusterID(),
					Result: &workerpb.QueryJobsV2Response_IndexJobResults{
						IndexJobResults: &workerpb.IndexJobResults{
							Results: results,
						},
					},
				}, nil
			case indexpb.JobType_JobTypeAnalyzeJob:
				results := make([]*workerpb.AnalyzeResult, 0)
				for _, taskID := range request.GetTaskIDs() {
					results = append(results, &workerpb.AnalyzeResult{
						TaskID:        taskID,
						State:         indexpb.JobState_JobStateFinished,
						CentroidsFile: fmt.Sprintf("%d/stats_file", taskID),
						FailReason:    "",
					})
				}
				return &workerpb.QueryJobsV2Response{
					Status:    merr.Success(),
					ClusterID: request.GetClusterID(),
					Result: &workerpb.QueryJobsV2Response_AnalyzeJobResults{
						AnalyzeJobResults: &workerpb.AnalyzeResults{
							Results: results,
						},
					},
				}, nil
			default:
				return &workerpb.QueryJobsV2Response{
					Status:    merr.Status(errors.New("unknown job type")),
					ClusterID: request.GetClusterID(),
				}, nil
			}
		})
	in.EXPECT().DropJobsV2(mock.Anything, mock.Anything).Return(merr.Success(), nil)

	workerManager := session.NewMockWorkerManager(s.T())
	workerManager.EXPECT().PickClient().Return(s.nodeID, in)
	workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true)

	mt := createMeta(catalog, withAnalyzeMeta(s.createAnalyzeMeta(catalog)), withIndexMeta(createIndexMeta(catalog)))

	cm := mocks.NewChunkManager(s.T())
	cm.EXPECT().RootPath().Return("root")

	scheduler := newTaskScheduler(ctx, mt, workerManager, cm, newIndexEngineVersionManager(), handler, nil)
	s.Equal(9, len(scheduler.tasks))
	s.Equal(indexpb.JobState_JobStateInit, scheduler.tasks[1].GetState())
	s.Equal(indexpb.JobState_JobStateInProgress, scheduler.tasks[2].GetState())
	s.Equal(indexpb.JobState_JobStateRetry, scheduler.tasks[5].GetState())
	s.Equal(indexpb.JobState_JobStateInit, scheduler.tasks[buildID].GetState())
	s.Equal(indexpb.JobState_JobStateInProgress, scheduler.tasks[buildID+1].GetState())
	s.Equal(indexpb.JobState_JobStateInit, scheduler.tasks[buildID+3].GetState())
	s.Equal(indexpb.JobState_JobStateInProgress, scheduler.tasks[buildID+8].GetState())
	s.Equal(indexpb.JobState_JobStateInit, scheduler.tasks[buildID+9].GetState())
	s.Equal(indexpb.JobState_JobStateInit, scheduler.tasks[buildID+10].GetState())

	mt.segments.DropSegment(segID + 9)

	scheduler.scheduleDuration = time.Millisecond * 500
	scheduler.collectMetricsDuration = time.Millisecond * 200
	scheduler.Start()

	s.Run("enqueue", func() {
		taskID := int64(6)
		newTask := &indexpb.AnalyzeTask{
			CollectionID: s.collectionID,
			PartitionID:  s.partitionID,
			FieldID:      s.fieldID,
			SegmentIDs:   s.segmentIDs,
			TaskID:       taskID,
		}
		err := scheduler.meta.analyzeMeta.AddAnalyzeTask(newTask)
		s.NoError(err)
		t := &analyzeTask{
			taskID: taskID,
			taskInfo: &workerpb.AnalyzeResult{
				TaskID:     taskID,
				State:      indexpb.JobState_JobStateInit,
				FailReason: "",
			},
		}
		scheduler.enqueue(t)
	})

	for {
		scheduler.RLock()
		taskNum := len(scheduler.tasks)
		scheduler.RUnlock()

		if taskNum == 0 {
			break
		}
		time.Sleep(time.Second)
	}

	scheduler.Stop()

	s.Equal(indexpb.JobState_JobStateFinished, mt.analyzeMeta.GetTask(1).GetState())
	s.Equal(indexpb.JobState_JobStateFinished, mt.analyzeMeta.GetTask(2).GetState())
	s.Equal(indexpb.JobState_JobStateFinished, mt.analyzeMeta.GetTask(3).GetState())
	s.Equal(indexpb.JobState_JobStateFailed, mt.analyzeMeta.GetTask(4).GetState())
	s.Equal(indexpb.JobState_JobStateFinished, mt.analyzeMeta.GetTask(5).GetState())
	s.Equal(indexpb.JobState_JobStateFinished, mt.analyzeMeta.GetTask(6).GetState())
	indexJob, exist := mt.indexMeta.GetIndexJob(buildID)
	s.True(exist)
	s.Equal(commonpb.IndexState_Finished, indexJob.IndexState)
	indexJob, exist = mt.indexMeta.GetIndexJob(buildID + 1)
	s.True(exist)
	s.Equal(commonpb.IndexState_Finished, indexJob.IndexState)
	indexJob, exist = mt.indexMeta.GetIndexJob(buildID + 2)
	s.True(exist)
	s.True(indexJob.IsDeleted)
	indexJob, exist = mt.indexMeta.GetIndexJob(buildID + 3)
	s.True(exist)
	s.Equal(commonpb.IndexState_Finished, indexJob.IndexState)
	indexJob, exist = mt.indexMeta.GetIndexJob(buildID + 4)
	s.True(exist)
	s.Equal(commonpb.IndexState_Finished, indexJob.IndexState)
	indexJob, exist = mt.indexMeta.GetIndexJob(buildID + 5)
	s.True(exist)
	s.Equal(commonpb.IndexState_Finished, indexJob.IndexState)
	indexJob, exist = mt.indexMeta.GetIndexJob(buildID + 6)
	s.True(exist)
	s.Equal(commonpb.IndexState_Finished, indexJob.IndexState)
	indexJob, exist = mt.indexMeta.GetIndexJob(buildID + 7)
	s.True(exist)
	s.Equal(commonpb.IndexState_Failed, indexJob.IndexState)
	indexJob, exist = mt.indexMeta.GetIndexJob(buildID + 8)
	s.True(exist)
	s.Equal(commonpb.IndexState_Finished, indexJob.IndexState)
	indexJob, exist = mt.indexMeta.GetIndexJob(buildID + 9)
	s.True(exist)
	// segment not healthy, wait for GC
	s.Equal(commonpb.IndexState_Unissued, indexJob.IndexState)
	indexJob, exist = mt.indexMeta.GetIndexJob(buildID + 10)
	s.True(exist)
	s.Equal(commonpb.IndexState_Finished, indexJob.IndexState)
}

func (s *taskSchedulerSuite) Test_scheduler() {
	handler := NewNMockHandler(s.T())
	handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(&collectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", IsPrimaryKey: true, IsPartitionKey: true, DataType: schemapb.DataType_Int64},
				{FieldID: s.fieldID, Name: "vec", TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "10"}}},
			},
		},
	}, nil)

	s.Run("test scheduler with indexBuilderV1", func() {
		paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("true")
		defer paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("false")
		s.scheduler(handler)
	})
}

func (s *taskSchedulerSuite) Test_analyzeTaskFailCase() {
	s.Run("segment info is nil", func() {
		ctx := context.Background()

		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		workerManager := session.NewMockWorkerManager(s.T())

		mt := createMeta(catalog,
			withAnalyzeMeta(&analyzeMeta{
				ctx:     context.Background(),
				catalog: catalog,
				tasks: map[int64]*indexpb.AnalyzeTask{
					1: {
						CollectionID: s.collectionID,
						PartitionID:  s.partitionID,
						FieldID:      s.fieldID,
						SegmentIDs:   s.segmentIDs,
						TaskID:       1,
						State:        indexpb.JobState_JobStateInit,
					},
				},
			}),
			withIndexMeta(&indexMeta{
				RWMutex: sync.RWMutex{},
				ctx:     ctx,
				catalog: catalog,
			}))

		handler := NewNMockHandler(s.T())
		scheduler := newTaskScheduler(ctx, mt, workerManager, nil, nil, handler, nil)

		mt.segments.DropSegment(1000)
		scheduler.scheduleDuration = s.duration
		scheduler.Start()

		// taskID 1 PreCheck failed --> state: Failed --> save
		catalog.EXPECT().SaveAnalyzeTask(mock.Anything, mock.Anything).Return(nil).Once()
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(nil, false).Once()

		for {
			scheduler.RLock()
			taskNum := len(scheduler.tasks)
			scheduler.RUnlock()

			if taskNum == 0 {
				break
			}
			time.Sleep(time.Second)
		}

		scheduler.Stop()
		s.Equal(indexpb.JobState_JobStateFailed, mt.analyzeMeta.GetTask(1).GetState())
	})

	s.Run("etcd save failed", func() {
		ctx := context.Background()

		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		catalog.EXPECT().DropAnalyzeTask(mock.Anything, mock.Anything).Return(nil)

		in := mocks.NewMockIndexNodeClient(s.T())

		workerManager := session.NewMockWorkerManager(s.T())

		mt := createMeta(catalog, withAnalyzeMeta(s.createAnalyzeMeta(catalog)), withIndexMeta(&indexMeta{
			RWMutex: sync.RWMutex{},
			ctx:     ctx,
			catalog: catalog,
		}))

		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(&collectionInfo{
			ID: collID,
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID: s.fieldID,
						Name:    "vec", DataType: schemapb.DataType_FloatVector,
						TypeParams: []*commonpb.KeyValuePair{
							{Key: "dim", Value: "10"},
						},
					},
				},
			},
		}, nil)

		scheduler := newTaskScheduler(ctx, mt, workerManager, nil, nil, handler, nil)

		// remove task in meta
		err := scheduler.meta.analyzeMeta.DropAnalyzeTask(1)
		s.NoError(err)
		err = scheduler.meta.analyzeMeta.DropAnalyzeTask(2)
		s.NoError(err)

		mt.segments.DropSegment(1000)
		scheduler.scheduleDuration = s.duration
		scheduler.Start()

		// taskID 5 state retry, drop task on worker --> state: Init
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
		in.EXPECT().DropJobsV2(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		// pick client fail --> state: init
		workerManager.EXPECT().PickClient().Return(0, nil).Once()

		// update version failed --> state: init
		workerManager.EXPECT().PickClient().Return(s.nodeID, in)
		catalog.EXPECT().SaveAnalyzeTask(mock.Anything, mock.Anything).Return(errors.New("catalog update version error")).Once()

		// assign task to indexNode fail --> state: retry
		catalog.EXPECT().SaveAnalyzeTask(mock.Anything, mock.Anything).Return(nil).Once()
		in.EXPECT().CreateJobV2(mock.Anything, mock.Anything).Return(&commonpb.Status{
			Code:      65535,
			Retriable: false,
			Detail:    "",
			ExtraInfo: nil,
			Reason:    "mock error",
		}, nil).Once()

		// drop task failed --> state: retry
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
		in.EXPECT().DropJobsV2(mock.Anything, mock.Anything).Return(merr.Status(errors.New("drop job failed")), nil).Once()

		// retry --> state: init
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
		in.EXPECT().DropJobsV2(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		// update state to building failed --> state: retry
		catalog.EXPECT().SaveAnalyzeTask(mock.Anything, mock.Anything).Return(nil).Once()
		in.EXPECT().CreateJobV2(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()
		catalog.EXPECT().SaveAnalyzeTask(mock.Anything, mock.Anything).Return(errors.New("catalog update building state error")).Once()

		// retry --> state: init
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
		in.EXPECT().DropJobsV2(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		// assign success --> state: InProgress
		catalog.EXPECT().SaveAnalyzeTask(mock.Anything, mock.Anything).Return(nil).Twice()
		in.EXPECT().CreateJobV2(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		// query result InProgress --> state: InProgress
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
		in.EXPECT().QueryJobsV2(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, request *workerpb.QueryJobsV2Request, option ...grpc.CallOption) (*workerpb.QueryJobsV2Response, error) {
				results := make([]*workerpb.AnalyzeResult, 0)
				for _, taskID := range request.GetTaskIDs() {
					results = append(results, &workerpb.AnalyzeResult{
						TaskID: taskID,
						State:  indexpb.JobState_JobStateInProgress,
					})
				}
				return &workerpb.QueryJobsV2Response{
					Status:    merr.Success(),
					ClusterID: request.GetClusterID(),
					Result: &workerpb.QueryJobsV2Response_AnalyzeJobResults{
						AnalyzeJobResults: &workerpb.AnalyzeResults{
							Results: results,
						},
					},
				}, nil
			}).Once()

		// query result Retry --> state: retry
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
		in.EXPECT().QueryJobsV2(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, request *workerpb.QueryJobsV2Request, option ...grpc.CallOption) (*workerpb.QueryJobsV2Response, error) {
				results := make([]*workerpb.AnalyzeResult, 0)
				for _, taskID := range request.GetTaskIDs() {
					results = append(results, &workerpb.AnalyzeResult{
						TaskID:     taskID,
						State:      indexpb.JobState_JobStateRetry,
						FailReason: "node analyze data failed",
					})
				}
				return &workerpb.QueryJobsV2Response{
					Status:    merr.Success(),
					ClusterID: request.GetClusterID(),
					Result: &workerpb.QueryJobsV2Response_AnalyzeJobResults{
						AnalyzeJobResults: &workerpb.AnalyzeResults{
							Results: results,
						},
					},
				}, nil
			}).Once()

		// retry --> state: init
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
		in.EXPECT().DropJobsV2(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		// init --> state: InProgress
		catalog.EXPECT().SaveAnalyzeTask(mock.Anything, mock.Anything).Return(nil).Twice()
		in.EXPECT().CreateJobV2(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		// query result failed --> state: retry
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
		in.EXPECT().QueryJobsV2(mock.Anything, mock.Anything).Return(&workerpb.QueryJobsV2Response{
			Status: merr.Status(errors.New("query job failed")),
		}, nil).Once()

		// retry --> state: init
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
		in.EXPECT().DropJobsV2(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		// init --> state: InProgress
		catalog.EXPECT().SaveAnalyzeTask(mock.Anything, mock.Anything).Return(nil).Twice()
		in.EXPECT().CreateJobV2(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		// query result not exists --> state: retry
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
		in.EXPECT().QueryJobsV2(mock.Anything, mock.Anything).Return(&workerpb.QueryJobsV2Response{
			Status:    merr.Success(),
			ClusterID: "",
			Result:    &workerpb.QueryJobsV2Response_AnalyzeJobResults{},
		}, nil).Once()

		// retry --> state: init
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
		in.EXPECT().DropJobsV2(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		// init --> state: InProgress
		catalog.EXPECT().SaveAnalyzeTask(mock.Anything, mock.Anything).Return(nil).Twice()
		in.EXPECT().CreateJobV2(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		// node not exist --> state: retry
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(nil, false).Once()

		// retry --> state: init
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
		in.EXPECT().DropJobsV2(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		// init --> state: InProgress
		catalog.EXPECT().SaveAnalyzeTask(mock.Anything, mock.Anything).Return(nil).Twice()
		in.EXPECT().CreateJobV2(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		// query result success --> state: finished
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
		in.EXPECT().QueryJobsV2(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, request *workerpb.QueryJobsV2Request, option ...grpc.CallOption) (*workerpb.QueryJobsV2Response, error) {
				results := make([]*workerpb.AnalyzeResult, 0)
				for _, taskID := range request.GetTaskIDs() {
					results = append(results, &workerpb.AnalyzeResult{
						TaskID: taskID,
						State:  indexpb.JobState_JobStateFinished,
						//CentroidsFile: fmt.Sprintf("%d/stats_file", taskID),
						//SegmentOffsetMappingFiles: map[int64]string{
						//	1000: "1000/offset_mapping",
						//	1001: "1001/offset_mapping",
						//	1002: "1002/offset_mapping",
						//},
						FailReason: "",
					})
				}
				return &workerpb.QueryJobsV2Response{
					Status:    merr.Success(),
					ClusterID: request.GetClusterID(),
					Result: &workerpb.QueryJobsV2Response_AnalyzeJobResults{
						AnalyzeJobResults: &workerpb.AnalyzeResults{
							Results: results,
						},
					},
				}, nil
			}).Once()
		// set job info failed --> state: Finished
		catalog.EXPECT().SaveAnalyzeTask(mock.Anything, mock.Anything).Return(errors.New("set job info failed")).Once()

		// set job success, drop job on task failed --> state: Finished
		catalog.EXPECT().SaveAnalyzeTask(mock.Anything, mock.Anything).Return(nil).Once()
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
		in.EXPECT().DropJobsV2(mock.Anything, mock.Anything).Return(merr.Status(errors.New("drop job failed")), nil).Once()

		// drop job success --> no task
		catalog.EXPECT().SaveAnalyzeTask(mock.Anything, mock.Anything).Return(nil).Once()
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
		in.EXPECT().DropJobsV2(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		for {
			scheduler.RLock()
			taskNum := len(scheduler.tasks)
			scheduler.RUnlock()

			if taskNum == 0 {
				break
			}
			time.Sleep(time.Second)
		}

		scheduler.Stop()
	})
}

func (s *taskSchedulerSuite) Test_indexTaskFailCase() {
	s.Run("HNSW", func() {
		ctx := context.Background()

		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		in := mocks.NewMockIndexNodeClient(s.T())
		workerManager := session.NewMockWorkerManager(s.T())

		mt := createMeta(catalog,
			withAnalyzeMeta(&analyzeMeta{
				ctx:     context.Background(),
				catalog: catalog,
			}),
			withIndexMeta(&indexMeta{
				RWMutex: sync.RWMutex{},
				ctx:     ctx,
				catalog: catalog,
				indexes: map[UniqueID]map[UniqueID]*model.Index{
					s.collectionID: {
						indexID: {
							CollectionID: s.collectionID,
							FieldID:      s.fieldID,
							IndexID:      indexID,
							IndexName:    indexName,
							TypeParams: []*commonpb.KeyValuePair{
								{
									Key:   common.DimKey,
									Value: "128",
								},
							},
							IndexParams: []*commonpb.KeyValuePair{
								{
									Key:   common.IndexTypeKey,
									Value: "HNSW",
								},
								{
									Key:   common.MetricTypeKey,
									Value: "L2",
								},
							},
						},
					},
				},
				buildID2SegmentIndex: map[UniqueID]*model.SegmentIndex{
					buildID: {
						SegmentID:    segID,
						CollectionID: s.collectionID,
						PartitionID:  s.partitionID,
						NumRows:      1025,
						IndexID:      indexID,
						BuildID:      buildID,
						IndexState:   commonpb.IndexState_Unissued,
					},
				},
				segmentIndexes: map[UniqueID]map[UniqueID]*model.SegmentIndex{
					segID: {
						buildID: {
							SegmentID:    segID,
							CollectionID: s.collectionID,
							PartitionID:  s.partitionID,
							NumRows:      1025,
							IndexID:      indexID,
							BuildID:      buildID,
							IndexState:   commonpb.IndexState_Unissued,
						},
					},
				},
			}))

		cm := mocks.NewChunkManager(s.T())
		cm.EXPECT().RootPath().Return("ut-index")

		handler := NewNMockHandler(s.T())
		scheduler := newTaskScheduler(ctx, mt, workerManager, cm, newIndexEngineVersionManager(), handler, nil)

		paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("True")
		defer paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("False")
		scheduler.Start()

		// get collection info failed --> init
		handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(nil, errors.New("mock error")).Once()

		// get collection info success, get dim failed --> init
		handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(&collectionInfo{
			ID: collID,
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "pk", IsPrimaryKey: true, IsPartitionKey: true, DataType: schemapb.DataType_Int64},
					{FieldID: s.fieldID, Name: "vec"},
				},
			},
		}, nil).Once()

		// assign failed --> retry
		workerManager.EXPECT().PickClient().Return(s.nodeID, in).Once()
		catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil).Once()
		in.EXPECT().CreateJobV2(mock.Anything, mock.Anything).Return(nil, errors.New("mock error")).Once()

		// retry --> init
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(nil, false).Once()

		// init --> inProgress
		workerManager.EXPECT().PickClient().Return(s.nodeID, in).Once()
		catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil).Twice()
		handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(&collectionInfo{
			ID: collID,
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "pk", IsPrimaryKey: true, IsPartitionKey: true, DataType: schemapb.DataType_Int64},
					{FieldID: s.fieldID, Name: "vec", TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "10"}}},
				},
			},
		}, nil).Once()
		in.EXPECT().CreateJobV2(mock.Anything, mock.Anything).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil).Once()

		// inProgress --> Finished
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
		in.EXPECT().QueryJobsV2(mock.Anything, mock.Anything).Return(&workerpb.QueryJobsV2Response{
			Status:    &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			ClusterID: "",
			Result: &workerpb.QueryJobsV2Response_IndexJobResults{
				IndexJobResults: &workerpb.IndexJobResults{
					Results: []*workerpb.IndexTaskInfo{
						{
							BuildID:        buildID,
							State:          commonpb.IndexState_Finished,
							IndexFileKeys:  []string{"file1", "file2"},
							SerializedSize: 1024,
						},
					},
				},
			},
		}, nil)

		// finished --> done
		catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil).Once()
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(nil, false).Once()

		for {
			scheduler.RLock()
			taskNum := len(scheduler.tasks)
			scheduler.RUnlock()

			if taskNum == 0 {
				break
			}
			time.Sleep(time.Second)
		}

		scheduler.Stop()

		indexJob, exist := mt.indexMeta.GetIndexJob(buildID)
		s.True(exist)
		s.Equal(commonpb.IndexState_Finished, indexJob.IndexState)
	})
}

func Test_taskSchedulerSuite(t *testing.T) {
	suite.Run(t, new(taskSchedulerSuite))
}

func (s *taskSchedulerSuite) Test_indexTaskWithMvOptionalScalarField() {
	ctx := context.Background()
	catalog := catalogmocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil)
	in := mocks.NewMockIndexNodeClient(s.T())

	workerManager := session.NewMockWorkerManager(s.T())
	workerManager.EXPECT().PickClient().Return(s.nodeID, in)
	workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true)

	minNumberOfRowsToBuild := paramtable.Get().DataCoordCfg.MinSegmentNumRowsToEnableIndex.GetAsInt64() + 1
	fieldsSchema := []*schemapb.FieldSchema{
		{
			FieldID:  fieldID,
			Name:     "vec",
			DataType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.DimKey,
					Value: "128",
				},
			},
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MetricTypeKey,
					Value: "L2",
				},
				{
					Key:   common.IndexTypeKey,
					Value: indexparamcheck.IndexHNSW,
				},
			},
		},
		{
			FieldID:        partitionKeyID,
			Name:           "scalar",
			DataType:       schemapb.DataType_VarChar,
			IsPartitionKey: true,
		},
	}
	mt := meta{
		catalog: catalog,
		collections: map[int64]*collectionInfo{
			collID: {
				ID: collID,
				Schema: &schemapb.CollectionSchema{
					Fields: fieldsSchema,
				},
				CreatedAt: 0,
			},
		},

		analyzeMeta: &analyzeMeta{
			ctx:     context.Background(),
			catalog: catalog,
		},

		indexMeta: &indexMeta{
			catalog: catalog,
			indexes: map[UniqueID]map[UniqueID]*model.Index{
				collID: {
					indexID: {
						TenantID:     "",
						CollectionID: collID,
						FieldID:      fieldID,
						IndexID:      indexID,
						IndexName:    indexName,
						IsDeleted:    false,
						CreateTime:   1,
						TypeParams: []*commonpb.KeyValuePair{
							{
								Key:   common.DimKey,
								Value: "128",
							},
						},
						IndexParams: []*commonpb.KeyValuePair{
							{
								Key:   common.MetricTypeKey,
								Value: "L2",
							},
							{
								Key:   common.IndexTypeKey,
								Value: indexparamcheck.IndexHNSW,
							},
						},
					},
				},
			},
			segmentIndexes: map[UniqueID]map[UniqueID]*model.SegmentIndex{
				segID: {
					indexID: {
						SegmentID:     segID,
						CollectionID:  collID,
						PartitionID:   partID,
						NumRows:       minNumberOfRowsToBuild,
						IndexID:       indexID,
						BuildID:       buildID,
						NodeID:        0,
						IndexVersion:  0,
						IndexState:    commonpb.IndexState_Unissued,
						FailReason:    "",
						IsDeleted:     false,
						CreateTime:    0,
						IndexFileKeys: nil,
						IndexSize:     0,
					},
				},
			},
			buildID2SegmentIndex: map[UniqueID]*model.SegmentIndex{
				buildID: {
					SegmentID:     segID,
					CollectionID:  collID,
					PartitionID:   partID,
					NumRows:       minNumberOfRowsToBuild,
					IndexID:       indexID,
					BuildID:       buildID,
					NodeID:        0,
					IndexVersion:  0,
					IndexState:    commonpb.IndexState_Unissued,
					FailReason:    "",
					IsDeleted:     false,
					CreateTime:    0,
					IndexFileKeys: nil,
					IndexSize:     0,
				},
			},
		},
		segments: &SegmentsInfo{
			segments: map[UniqueID]*SegmentInfo{
				segID: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      minNumberOfRowsToBuild,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
				},
			},
		},
	}

	cm := mocks.NewChunkManager(s.T())
	cm.EXPECT().RootPath().Return("ut-index")

	handler := NewNMockHandler(s.T())
	handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(&collectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Name:               "coll",
			Fields:             fieldsSchema,
			EnableDynamicField: false,
			Properties:         nil,
		},
	}, nil)

	paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("true")
	defer paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("false")
	scheduler := newTaskScheduler(ctx, &mt, workerManager, cm, newIndexEngineVersionManager(), handler, nil)

	waitTaskDoneFunc := func(sche *taskScheduler) {
		for {
			sche.RLock()
			taskNum := len(sche.tasks)
			sche.RUnlock()

			if taskNum == 0 {
				break
			}
			time.Sleep(time.Second)
		}
	}

	resetMetaFunc := func() {
		mt.indexMeta.buildID2SegmentIndex[buildID].IndexState = commonpb.IndexState_Unissued
		mt.indexMeta.segmentIndexes[segID][indexID].IndexState = commonpb.IndexState_Unissued
		mt.indexMeta.indexes[collID][indexID].IndexParams[1].Value = indexparamcheck.IndexHNSW
		mt.collections[collID].Schema.Fields[0].DataType = schemapb.DataType_FloatVector
		mt.collections[collID].Schema.Fields[1].IsPartitionKey = true
		mt.collections[collID].Schema.Fields[1].DataType = schemapb.DataType_VarChar
	}

	in.EXPECT().QueryJobsV2(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, request *workerpb.QueryJobsV2Request, option ...grpc.CallOption) (*workerpb.QueryJobsV2Response, error) {
			switch request.GetJobType() {
			case indexpb.JobType_JobTypeIndexJob:
				results := make([]*workerpb.IndexTaskInfo, 0)
				for _, buildID := range request.GetTaskIDs() {
					results = append(results, &workerpb.IndexTaskInfo{
						BuildID:             buildID,
						State:               commonpb.IndexState_Finished,
						IndexFileKeys:       []string{"file1", "file2"},
						SerializedSize:      1024,
						FailReason:          "",
						CurrentIndexVersion: 0,
						IndexStoreVersion:   0,
					})
				}
				return &workerpb.QueryJobsV2Response{
					Status:    merr.Success(),
					ClusterID: request.GetClusterID(),
					Result: &workerpb.QueryJobsV2Response_IndexJobResults{
						IndexJobResults: &workerpb.IndexJobResults{
							Results: results,
						},
					},
				}, nil
			default:
				return &workerpb.QueryJobsV2Response{
					Status: merr.Status(errors.New("unknown job type")),
				}, nil
			}
		})
	in.EXPECT().DropJobsV2(mock.Anything, mock.Anything).Return(merr.Success(), nil)

	s.Run("success to get opt field on startup", func() {
		in.EXPECT().CreateJobV2(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, in *workerpb.CreateJobV2Request, opts ...grpc.CallOption) (*commonpb.Status, error) {
				s.NotZero(len(in.GetIndexRequest().OptionalScalarFields), "optional scalar field should be set")
				return merr.Success(), nil
			}).Once()
		s.Equal(1, len(scheduler.tasks))
		s.Equal(indexpb.JobState_JobStateInit, scheduler.tasks[buildID].GetState())

		scheduler.Start()
		waitTaskDoneFunc(scheduler)
		resetMetaFunc()
	})

	s.Run("enqueue valid", func() {
		for _, dataType := range []schemapb.DataType{
			schemapb.DataType_Int8,
			schemapb.DataType_Int16,
			schemapb.DataType_Int32,
			schemapb.DataType_Int64,
			schemapb.DataType_VarChar,
			schemapb.DataType_String,
		} {
			mt.collections[collID].Schema.Fields[1].DataType = dataType
			in.EXPECT().CreateJobV2(mock.Anything, mock.Anything).RunAndReturn(
				func(ctx context.Context, in *workerpb.CreateJobV2Request, opts ...grpc.CallOption) (*commonpb.Status, error) {
					s.NotZero(len(in.GetIndexRequest().OptionalScalarFields), "optional scalar field should be set")
					return merr.Success(), nil
				}).Once()
			t := &indexBuildTask{
				taskID: buildID,
				nodeID: nodeID,
				taskInfo: &workerpb.IndexTaskInfo{
					BuildID:    buildID,
					State:      commonpb.IndexState_Unissued,
					FailReason: "",
				},
			}
			scheduler.enqueue(t)
			waitTaskDoneFunc(scheduler)
			resetMetaFunc()
		}
	})

	// should still be able to build vec index when opt field is not set
	s.Run("enqueue returns empty optional field when cfg disable", func() {
		paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("false")
		in.EXPECT().CreateJobV2(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, in *workerpb.CreateJobV2Request, opts ...grpc.CallOption) (*commonpb.Status, error) {
				s.Zero(len(in.GetIndexRequest().OptionalScalarFields), "optional scalar field should be set")
				return merr.Success(), nil
			}).Once()
		t := &indexBuildTask{
			taskID: buildID,
			nodeID: nodeID,
			taskInfo: &workerpb.IndexTaskInfo{
				BuildID:    buildID,
				State:      commonpb.IndexState_Unissued,
				FailReason: "",
			},
		}
		scheduler.enqueue(t)
		waitTaskDoneFunc(scheduler)
		resetMetaFunc()
	})

	s.Run("enqueue returns empty when vector type is not dense vector", func() {
		paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("true")
		for _, dataType := range []schemapb.DataType{
			schemapb.DataType_SparseFloatVector,
		} {
			mt.collections[collID].Schema.Fields[0].DataType = dataType
			in.EXPECT().CreateJobV2(mock.Anything, mock.Anything).RunAndReturn(
				func(ctx context.Context, in *workerpb.CreateJobV2Request, opts ...grpc.CallOption) (*commonpb.Status, error) {
					s.Zero(len(in.GetIndexRequest().OptionalScalarFields), "optional scalar field should not be set")
					return merr.Success(), nil
				}).Once()
			t := &indexBuildTask{
				taskID: buildID,
				nodeID: nodeID,
				taskInfo: &workerpb.IndexTaskInfo{
					BuildID:    buildID,
					State:      commonpb.IndexState_Unissued,
					FailReason: "",
				},
			}
			scheduler.enqueue(t)
			waitTaskDoneFunc(scheduler)
			resetMetaFunc()
		}
	})

	s.Run("enqueue returns empty optional field when the data type is not STRING or VARCHAR or Integer", func() {
		paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("true")
		for _, dataType := range []schemapb.DataType{
			schemapb.DataType_Bool,
			schemapb.DataType_Float,
			schemapb.DataType_Double,
			schemapb.DataType_Array,
			schemapb.DataType_JSON,
		} {
			mt.collections[collID].Schema.Fields[1].DataType = dataType
			in.EXPECT().CreateJobV2(mock.Anything, mock.Anything).RunAndReturn(
				func(ctx context.Context, in *workerpb.CreateJobV2Request, opts ...grpc.CallOption) (*commonpb.Status, error) {
					s.Zero(len(in.GetIndexRequest().OptionalScalarFields), "optional scalar field should be set")
					return merr.Success(), nil
				}).Once()
			t := &indexBuildTask{
				taskID: buildID,
				nodeID: nodeID,
				taskInfo: &workerpb.IndexTaskInfo{
					BuildID:    buildID,
					State:      commonpb.IndexState_Unissued,
					FailReason: "",
				},
			}
			scheduler.enqueue(t)
			waitTaskDoneFunc(scheduler)
			resetMetaFunc()
		}
	})

	s.Run("enqueue returns empty optional field when no partition key", func() {
		paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("true")
		mt.collections[collID].Schema.Fields[1].IsPartitionKey = false
		in.EXPECT().CreateJobV2(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, in *workerpb.CreateJobV2Request, opts ...grpc.CallOption) (*commonpb.Status, error) {
				s.Zero(len(in.GetIndexRequest().OptionalScalarFields), "optional scalar field should be set")
				return merr.Success(), nil
			}).Once()
		t := &indexBuildTask{
			taskID: buildID,
			nodeID: nodeID,
			taskInfo: &workerpb.IndexTaskInfo{
				BuildID:    buildID,
				State:      commonpb.IndexState_Unissued,
				FailReason: "",
			},
		}
		scheduler.enqueue(t)
		waitTaskDoneFunc(scheduler)
		resetMetaFunc()
	})

	s.Run("enqueue partitionKeyIsolation is false when schema is not set", func() {
		paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("true")
		in.EXPECT().CreateJobV2(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, in *workerpb.CreateJobV2Request, opts ...grpc.CallOption) (*commonpb.Status, error) {
				s.Equal(in.GetIndexRequest().PartitionKeyIsolation, false)
				return merr.Success(), nil
			}).Once()
		t := &indexBuildTask{
			taskID: buildID,
			nodeID: nodeID,
			taskInfo: &workerpb.IndexTaskInfo{
				BuildID:    buildID,
				State:      commonpb.IndexState_Unissued,
				FailReason: "",
			},
		}
		scheduler.enqueue(t)
		waitTaskDoneFunc(scheduler)
		resetMetaFunc()
	})
	scheduler.Stop()

	isoCollInfo := &collectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Name:               "coll",
			Fields:             fieldsSchema,
			EnableDynamicField: false,
		},
		Properties: map[string]string{
			common.PartitionKeyIsolationKey: "false",
		},
	}
	handler_isolation := NewNMockHandler(s.T())
	handler_isolation.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(isoCollInfo, nil)

	scheduler_isolation := newTaskScheduler(ctx, &mt, workerManager, cm, newIndexEngineVersionManager(), handler_isolation, nil)
	scheduler_isolation.Start()

	s.Run("enqueue partitionKeyIsolation is false when MV not enabled", func() {
		paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("false")
		in.EXPECT().CreateJobV2(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, in *workerpb.CreateJobV2Request, opts ...grpc.CallOption) (*commonpb.Status, error) {
				s.Equal(in.GetIndexRequest().PartitionKeyIsolation, false)
				return merr.Success(), nil
			}).Once()
		t := &indexBuildTask{
			taskID: buildID,
			nodeID: nodeID,
			taskInfo: &workerpb.IndexTaskInfo{
				BuildID:    buildID,
				State:      commonpb.IndexState_Unissued,
				FailReason: "",
			},
		}
		scheduler_isolation.enqueue(t)
		waitTaskDoneFunc(scheduler_isolation)
		resetMetaFunc()
	})

	s.Run("enqueue partitionKeyIsolation is true when MV enabled", func() {
		paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("true")
		defer paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("false")
		isoCollInfo.Properties[common.PartitionKeyIsolationKey] = "true"
		in.EXPECT().CreateJobV2(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, in *workerpb.CreateJobV2Request, opts ...grpc.CallOption) (*commonpb.Status, error) {
				s.Equal(in.GetIndexRequest().PartitionKeyIsolation, true)
				return merr.Success(), nil
			}).Once()
		t := &indexBuildTask{
			taskID: buildID,
			nodeID: nodeID,
			taskInfo: &workerpb.IndexTaskInfo{
				BuildID:    buildID,
				State:      commonpb.IndexState_Unissued,
				FailReason: "",
			},
		}
		scheduler_isolation.enqueue(t)
		waitTaskDoneFunc(scheduler_isolation)
		resetMetaFunc()
	})

	s.Run("enqueue partitionKeyIsolation is invalid when MV is enabled", func() {
		paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("true")
		defer paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("false")
		isoCollInfo.Properties[common.PartitionKeyIsolationKey] = "invalid"
		in.EXPECT().CreateJobV2(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, in *workerpb.CreateJobV2Request, opts ...grpc.CallOption) (*commonpb.Status, error) {
				s.Equal(in.GetIndexRequest().PartitionKeyIsolation, false)
				return merr.Success(), nil
			}).Once()
		t := &indexBuildTask{
			taskID: buildID,
			nodeID: nodeID,
			taskInfo: &workerpb.IndexTaskInfo{
				BuildID:    buildID,
				State:      commonpb.IndexState_Unissued,
				FailReason: "",
			},
		}
		scheduler_isolation.enqueue(t)
		waitTaskDoneFunc(scheduler_isolation)
		resetMetaFunc()
	})
	scheduler_isolation.Stop()
}

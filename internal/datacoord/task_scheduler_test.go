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
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/metastore"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
	statsTaskID    = UniqueID(900)
)

func createIndexMeta(catalog metastore.DataCoordCatalog) *indexMeta {
	indexBuildInfo := newSegmentIndexBuildInfo()
	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1025,
		IndexID:             indexID,
		BuildID:             buildID,
		NodeID:              0,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      0,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 1,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 1,
		NodeID:              nodeID,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_InProgress,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 2,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 2,
		NodeID:              nodeID,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_InProgress,
		FailReason:          "",
		IsDeleted:           true,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 3,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             500,
		IndexID:             indexID,
		BuildID:             buildID + 3,
		NodeID:              0,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 4,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 4,
		NodeID:              nodeID,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 5,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 5,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 6,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 6,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 7,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 7,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Failed,
		FailReason:          "error",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 8,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 8,
		NodeID:              nodeID + 1,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_InProgress,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 9,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             500,
		IndexID:             indexID,
		BuildID:             buildID + 9,
		NodeID:              0,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 10,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             500,
		IndexID:             indexID,
		BuildID:             buildID + 10,
		NodeID:              nodeID,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIndexes := typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]]()
	segIdx0 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx0.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1025,
		IndexID:             indexID,
		BuildID:             buildID,
		NodeID:              0,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      0,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx1 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx1.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 1,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 1,
		NodeID:              nodeID,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_InProgress,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx2 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx2.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 2,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 2,
		NodeID:              nodeID,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_InProgress,
		FailReason:          "",
		IsDeleted:           true,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx3 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx3.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 3,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             500,
		IndexID:             indexID,
		BuildID:             buildID + 3,
		NodeID:              0,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx4 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx4.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 4,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 4,
		NodeID:              nodeID,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx5 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx5.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 5,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 5,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx6 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx6.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 6,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 6,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx7 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx7.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 7,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 7,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Failed,
		FailReason:          "error",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx8 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx8.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 8,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 8,
		NodeID:              nodeID + 1,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_InProgress,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx9 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx9.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 9,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             500,
		IndexID:             indexID,
		BuildID:             buildID + 9,
		NodeID:              0,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx10 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx10.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 10,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             500,
		IndexID:             indexID,
		BuildID:             buildID + 10,
		NodeID:              nodeID,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIndexes.Insert(segID, segIdx0)
	segIndexes.Insert(segID+1, segIdx1)
	segIndexes.Insert(segID+2, segIdx2)
	segIndexes.Insert(segID+3, segIdx3)
	segIndexes.Insert(segID+4, segIdx4)
	segIndexes.Insert(segID+5, segIdx5)
	segIndexes.Insert(segID+6, segIdx6)
	segIndexes.Insert(segID+7, segIdx7)
	segIndexes.Insert(segID+8, segIdx8)
	segIndexes.Insert(segID+9, segIdx9)
	segIndexes.Insert(segID+10, segIdx10)

	return &indexMeta{
		catalog: catalog,
		keyLock: lock.NewKeyLock[UniqueID](),
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
		segmentIndexes:   segIndexes,
		segmentBuildInfo: indexBuildInfo,
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
		catalog:     catalog,
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
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
				State:        indexpb.JobState_JobStateInit,
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
	catalog.EXPECT().DropSegmentIndex(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().DropStatsTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().DropAnalyzeTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil)
	// catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil)

	in := mocks.NewMockDataNodeClient(s.T())
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
	workerManager.EXPECT().QuerySlots().RunAndReturn(func() map[int64]int64 {
		return map[int64]int64{
			1: 1,
		}
	})
	workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true)

	mt := createMeta(catalog, withAnalyzeMeta(s.createAnalyzeMeta(catalog)), withIndexMeta(createIndexMeta(catalog)),
		withStatsTaskMeta(&statsTaskMeta{
			ctx:             ctx,
			catalog:         catalog,
			keyLock:         lock.NewKeyLock[UniqueID](),
			tasks:           typeutil.NewConcurrentMap[UniqueID, *indexpb.StatsTask](),
			segmentID2Tasks: typeutil.NewConcurrentMap[string, *indexpb.StatsTask](),
		}))

	cm := mocks.NewChunkManager(s.T())
	cm.EXPECT().RootPath().Return("root")

	scheduler := newTaskScheduler(ctx, mt, workerManager, cm, newIndexEngineVersionManager(), handler, nil, nil)
	s.Equal(6, scheduler.pendingTasks.TaskCount())
	s.Equal(3, scheduler.runningTasks.Len())
	s.Equal(indexpb.JobState_JobStateInit, scheduler.pendingTasks.Get(1).GetState())
	s.Equal(indexpb.JobState_JobStateInit, scheduler.pendingTasks.Get(2).GetState())
	t5, ok := scheduler.runningTasks.Get(5)
	s.True(ok)
	s.Equal(indexpb.JobState_JobStateRetry, t5.GetState())
	s.Equal(indexpb.JobState_JobStateInit, scheduler.pendingTasks.Get(buildID).GetState())
	t6, ok := scheduler.runningTasks.Get(buildID + 1)
	s.True(ok)
	s.Equal(indexpb.JobState_JobStateInProgress, t6.GetState())
	s.Equal(indexpb.JobState_JobStateInit, scheduler.pendingTasks.Get(buildID+3).GetState())
	t8, ok := scheduler.runningTasks.Get(buildID + 8)
	s.True(ok)
	s.Equal(indexpb.JobState_JobStateInProgress, t8.GetState())
	s.Equal(indexpb.JobState_JobStateInit, scheduler.pendingTasks.Get(buildID+9).GetState())
	s.Equal(indexpb.JobState_JobStateInit, scheduler.pendingTasks.Get(buildID+10).GetState())

	mt.segments.DropSegment(segID + 9)

	scheduler.scheduleDuration = time.Millisecond * 500
	scheduler.collectMetricsDuration = time.Millisecond * 200
	scheduler.Start()

	s.Run("Submit", func() {
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
		if scheduler.pendingTasks.TaskCount() == 0 {
			// maybe task is waiting for assigning, so sleep three seconds.
			time.Sleep(time.Second * 3)
			taskNum := scheduler.runningTasks.Len()
			if taskNum == 0 {
				break
			}
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
	_, exist = mt.indexMeta.GetIndexJob(buildID + 9)
	s.False(exist)
	// segment not healthy, remove task
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
		workerManager.EXPECT().QuerySlots().RunAndReturn(func() map[int64]int64 {
			return map[int64]int64{
				1: 1,
			}
		})

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
				ctx:              ctx,
				keyLock:          lock.NewKeyLock[UniqueID](),
				catalog:          catalog,
				segmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
				segmentBuildInfo: newSegmentIndexBuildInfo(),
			}),
			withStatsTaskMeta(&statsTaskMeta{
				ctx:             ctx,
				catalog:         catalog,
				keyLock:         lock.NewKeyLock[UniqueID](),
				tasks:           typeutil.NewConcurrentMap[UniqueID, *indexpb.StatsTask](),
				segmentID2Tasks: typeutil.NewConcurrentMap[string, *indexpb.StatsTask](),
			}))

		handler := NewNMockHandler(s.T())
		scheduler := newTaskScheduler(ctx, mt, workerManager, nil, nil, handler, nil, nil)

		mt.segments.DropSegment(1000)
		scheduler.scheduleDuration = s.duration
		scheduler.Start()

		// taskID 1 PreCheck failed --> state: Failed --> save
		catalog.EXPECT().SaveAnalyzeTask(mock.Anything, mock.Anything).Return(nil).Once()
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(nil, false).Once()

		for {
			if scheduler.pendingTasks.TaskCount() == 0 {
				taskNum := scheduler.runningTasks.Len()
				if taskNum == 0 {
					break
				}
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

		in := mocks.NewMockDataNodeClient(s.T())

		workerManager := session.NewMockWorkerManager(s.T())
		workerManager.EXPECT().QuerySlots().RunAndReturn(func() map[int64]int64 {
			return map[int64]int64{
				1: 1,
			}
		})

		mt := createMeta(catalog, withAnalyzeMeta(s.createAnalyzeMeta(catalog)),
			withIndexMeta(&indexMeta{
				ctx:              ctx,
				catalog:          catalog,
				keyLock:          lock.NewKeyLock[UniqueID](),
				segmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
				segmentBuildInfo: newSegmentIndexBuildInfo(),
			}), withStatsTaskMeta(&statsTaskMeta{
				ctx:             ctx,
				catalog:         catalog,
				keyLock:         lock.NewKeyLock[UniqueID](),
				tasks:           typeutil.NewConcurrentMap[UniqueID, *indexpb.StatsTask](),
				segmentID2Tasks: typeutil.NewConcurrentMap[string, *indexpb.StatsTask](),
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

		scheduler := newTaskScheduler(ctx, mt, workerManager, nil, nil, handler, nil, nil)

		// remove task in meta
		err := scheduler.meta.analyzeMeta.DropAnalyzeTask(context.TODO(), 1)
		s.NoError(err)
		err = scheduler.meta.analyzeMeta.DropAnalyzeTask(context.TODO(), 2)
		s.NoError(err)

		mt.segments.DropSegment(1000)
		scheduler.scheduleDuration = s.duration
		scheduler.Start()

		// taskID 5 state retry, drop task on worker --> state: Init
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
		in.EXPECT().DropJobsV2(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		// update version failed --> state: init
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
		catalog.EXPECT().SaveAnalyzeTask(mock.Anything, mock.Anything).Return(errors.New("catalog update version error")).Once()

		// assign task to indexNode fail --> state: retry
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
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
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
		catalog.EXPECT().SaveAnalyzeTask(mock.Anything, mock.Anything).Return(nil).Once()
		in.EXPECT().CreateJobV2(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()
		catalog.EXPECT().SaveAnalyzeTask(mock.Anything, mock.Anything).Return(errors.New("catalog update building state error")).Once()

		// retry --> state: init
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
		in.EXPECT().DropJobsV2(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		// assign success --> state: InProgress
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
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
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
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
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
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
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
		catalog.EXPECT().SaveAnalyzeTask(mock.Anything, mock.Anything).Return(nil).Twice()
		in.EXPECT().CreateJobV2(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		// node not exist --> state: retry
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(nil, false).Once()

		// retry --> state: init
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
		in.EXPECT().DropJobsV2(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		// init --> state: InProgress
		workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
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
			if scheduler.pendingTasks.TaskCount() == 0 {
				taskNum := scheduler.runningTasks.Len()
				if taskNum == 0 {
					break
				}
			}
			time.Sleep(time.Second)
		}

		scheduler.Stop()
	})
}

func (s *taskSchedulerSuite) Test_indexTaskFailCase() {
	s.Run("HNSW", func() {
		ctx := context.Background()
		indexNodeTasks := make(map[int64]int)

		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		in := mocks.NewMockDataNodeClient(s.T())
		workerManager := session.NewMockWorkerManager(s.T())
		workerManager.EXPECT().QuerySlots().RunAndReturn(func() map[int64]int64 {
			return map[int64]int64{
				1: 1,
			}
		})

		segIndexes := typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]]()
		segIdx := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		segIdx.Insert(indexID, &model.SegmentIndex{
			SegmentID:    segID,
			CollectionID: s.collectionID,
			PartitionID:  s.partitionID,
			NumRows:      1025,
			IndexID:      indexID,
			BuildID:      buildID,
			IndexState:   commonpb.IndexState_Unissued,
		})
		segIndexes.Insert(segID, segIdx)

		mt := createMeta(catalog,
			withAnalyzeMeta(&analyzeMeta{
				ctx:     context.Background(),
				catalog: catalog,
			}),
			withIndexMeta(&indexMeta{
				fieldIndexLock: sync.RWMutex{},
				ctx:            ctx,
				catalog:        catalog,
				keyLock:        lock.NewKeyLock[UniqueID](),
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
				segmentBuildInfo: newSegmentIndexBuildInfo(),
				segmentIndexes:   segIndexes,
			}),
			withStatsTaskMeta(&statsTaskMeta{
				ctx:             context.Background(),
				catalog:         catalog,
				keyLock:         lock.NewKeyLock[UniqueID](),
				tasks:           typeutil.NewConcurrentMap[UniqueID, *indexpb.StatsTask](),
				segmentID2Tasks: typeutil.NewConcurrentMap[string, *indexpb.StatsTask](),
			}))

		mt.indexMeta.segmentBuildInfo.Add(&model.SegmentIndex{
			SegmentID:    segID,
			CollectionID: s.collectionID,
			PartitionID:  s.partitionID,
			NumRows:      1025,
			IndexID:      indexID,
			BuildID:      buildID,
			IndexState:   commonpb.IndexState_Unissued,
		})
		cm := mocks.NewChunkManager(s.T())
		cm.EXPECT().RootPath().Return("ut-index")

		handler := NewNMockHandler(s.T())
		scheduler := newTaskScheduler(ctx, mt, workerManager, cm, newIndexEngineVersionManager(), handler, nil, nil)

		paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("True")
		defer paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("False")
		scheduler.Start()

		// get collection info failed --> init
		handler.EXPECT().GetCollection(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, collID int64) (*collectionInfo, error) {
			log.Debug("get collection info failed", zap.Int64("collectionID", collID))
			return nil, errors.New("mock error")
		}).Once()

		// get collection info success, get dim failed --> init
		handler.EXPECT().GetCollection(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, i int64) (*collectionInfo, error) {
			log.Debug("get collection info success", zap.Int64("collectionID", i))
			return &collectionInfo{
				ID: collID,
				Schema: &schemapb.CollectionSchema{
					Fields: []*schemapb.FieldSchema{
						{FieldID: 100, Name: "pk", IsPrimaryKey: true, IsPartitionKey: true, DataType: schemapb.DataType_Int64},
						{FieldID: s.fieldID, Name: "vec"},
					},
				},
			}, nil
		}).Once()

		// assign failed --> retry
		workerManager.EXPECT().GetClientByID(mock.Anything).RunAndReturn(func(nodeID int64) (types.DataNodeClient, bool) {
			log.Debug("get client success, but assign failed", zap.Int64("nodeID", nodeID))
			return in, true
		}).Once()
		catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, indices []*model.SegmentIndex) error {
			log.Debug("alter segment indexes success, but assign failed", zap.Int64("taskID", indices[0].BuildID))
			return nil
		}).Once()
		in.EXPECT().CreateJobV2(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, request *workerpb.CreateJobV2Request, option ...grpc.CallOption) (*commonpb.Status, error) {
			indexNodeTasks[request.GetTaskID()]++
			log.Debug("assign task failed", zap.Int64("taskID", request.GetTaskID()))
			return nil, errors.New("mock error")
		}).Once()

		// retry --> init
		workerManager.EXPECT().GetClientByID(mock.Anything).RunAndReturn(func(nodeID int64) (types.DataNodeClient, bool) {
			log.Debug("assign failed, drop task on worker", zap.Int64("nodeID", nodeID))
			return in, true
		}).Once()
		in.EXPECT().DropJobsV2(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, request *workerpb.DropJobsV2Request, option ...grpc.CallOption) (*commonpb.Status, error) {
			for _, taskID := range request.GetTaskIDs() {
				indexNodeTasks[taskID]--
			}
			log.Debug("drop task on worker, success", zap.Int64s("taskIDs", request.GetTaskIDs()))
			return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
		}).Once()

		// init --> inProgress
		workerManager.EXPECT().GetClientByID(mock.Anything).RunAndReturn(func(nodeID int64) (types.DataNodeClient, bool) {
			log.Debug("assign task success", zap.Int64("nodeID", nodeID))
			return in, true
		}).Once()
		catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, indices []*model.SegmentIndex) error {
			log.Debug("alter segment success twice and assign task success", zap.Int64("taskID", indices[0].BuildID))
			return nil
		}).Twice()
		handler.EXPECT().GetCollection(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, i int64) (*collectionInfo, error) {
			log.Debug("get collection success and assign task success", zap.Int64("collID", i))
			return &collectionInfo{
				ID: collID,
				Schema: &schemapb.CollectionSchema{
					Fields: []*schemapb.FieldSchema{
						{FieldID: 100, Name: "pk", IsPrimaryKey: true, IsPartitionKey: true, DataType: schemapb.DataType_Int64},
						{FieldID: s.fieldID, Name: "vec", TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "10"}}},
					},
				},
			}, nil
		}).Once()
		in.EXPECT().CreateJobV2(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, request *workerpb.CreateJobV2Request, option ...grpc.CallOption) (*commonpb.Status, error) {
			indexNodeTasks[request.GetTaskID()]++
			log.Debug("assign task success", zap.Int64("nodeID", nodeID))
			return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
		}).Once()

		// inProgress --> Finished
		workerManager.EXPECT().GetClientByID(mock.Anything).RunAndReturn(func(nodeID int64) (types.DataNodeClient, bool) {
			log.Debug("get task result success, task is finished", zap.Int64("nodeID", nodeID))
			return in, true
		}).Once()
		in.EXPECT().QueryJobsV2(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, request *workerpb.QueryJobsV2Request, option ...grpc.CallOption) (*workerpb.QueryJobsV2Response, error) {
			log.Debug("query task result success, task is finished", zap.Int64s("taskIDs", request.GetTaskIDs()))
			return &workerpb.QueryJobsV2Response{
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
			}, nil
		})

		// finished --> done
		finishCH := make(chan struct{})
		catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, indices []*model.SegmentIndex) error {
			log.Debug("task is finished, alter segment index success", zap.Int64("taskID", indices[0].BuildID))
			return nil
		}).Once()
		workerManager.EXPECT().GetClientByID(mock.Anything).RunAndReturn(func(nodeID int64) (types.DataNodeClient, bool) {
			log.Debug("task is finished, drop task on worker", zap.Int64("nodeID", nodeID))
			return in, true
		}).Once()
		in.EXPECT().DropJobsV2(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, request *workerpb.DropJobsV2Request, option ...grpc.CallOption) (*commonpb.Status, error) {
			for _, taskID := range request.GetTaskIDs() {
				indexNodeTasks[taskID]--
				finishCH <- struct{}{}
			}
			log.Debug("task is finished, drop task on worker success", zap.Int64("nodeID", nodeID))
			return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
		}).Once()

		<-finishCH
		for {
			if scheduler.pendingTasks.TaskCount() == 0 {
				taskNum := scheduler.runningTasks.Len()
				if taskNum == 0 {
					break
				}
			}
			time.Sleep(time.Second)
		}

		scheduler.Stop()

		indexJob, exist := mt.indexMeta.GetIndexJob(buildID)
		s.True(exist)
		s.Equal(commonpb.IndexState_Finished, indexJob.IndexState)

		for _, v := range indexNodeTasks {
			s.Zero(v)
		}
	})
}

func Test_taskSchedulerSuite(t *testing.T) {
	suite.Run(t, new(taskSchedulerSuite))
}

func (s *taskSchedulerSuite) Test_indexTaskWithMvOptionalScalarField() {
	ctx := context.Background()
	catalog := catalogmocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil)
	in := mocks.NewMockDataNodeClient(s.T())

	workerManager := session.NewMockWorkerManager(s.T())
	workerManager.EXPECT().QuerySlots().RunAndReturn(func() map[int64]int64 {
		return map[int64]int64{
			1: 1,
		}
	})
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
					Value: "HNSW",
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

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(collID, &collectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Fields: fieldsSchema,
		},
		CreatedAt: 0,
	})

	segIndexes := typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]]()
	segIdx := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             minNumberOfRowsToBuild,
		IndexID:             indexID,
		BuildID:             buildID,
		NodeID:              0,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      0,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
	})
	segIndexes.Insert(segID, segIdx)

	mt := meta{
		catalog:     catalog,
		collections: collections,

		analyzeMeta: &analyzeMeta{
			ctx:     context.Background(),
			catalog: catalog,
		},

		indexMeta: &indexMeta{
			catalog: catalog,
			keyLock: lock.NewKeyLock[UniqueID](),
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
								Value: "HNSW",
							},
						},
					},
				},
			},
			segmentIndexes:   segIndexes,
			segmentBuildInfo: newSegmentIndexBuildInfo(),
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
		statsTaskMeta: &statsTaskMeta{
			ctx:             context.Background(),
			catalog:         catalog,
			keyLock:         lock.NewKeyLock[UniqueID](),
			tasks:           typeutil.NewConcurrentMap[UniqueID, *indexpb.StatsTask](),
			segmentID2Tasks: typeutil.NewConcurrentMap[string, *indexpb.StatsTask](),
		},
	}

	mt.indexMeta.segmentBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             minNumberOfRowsToBuild,
		IndexID:             indexID,
		BuildID:             buildID,
		NodeID:              0,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      0,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
	})
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
	scheduler := newTaskScheduler(ctx, &mt, workerManager, cm, newIndexEngineVersionManager(), handler, nil, nil)

	waitTaskDoneFunc := func(sche *taskScheduler) {
		for {
			time.Sleep(time.Second * 3)
			if sche.pendingTasks.TaskCount() == 0 {
				taskNum := scheduler.runningTasks.Len()
				if taskNum == 0 {
					break
				}
			}
		}
	}

	resetMetaFunc := func() {
		mt.indexMeta.keyLock.Lock(buildID)
		t, ok := mt.indexMeta.segmentBuildInfo.Get(buildID)
		s.True(ok)
		t.IndexState = commonpb.IndexState_Unissued
		mt.indexMeta.segmentBuildInfo.Add(t)
		segIdxes, ok := mt.indexMeta.segmentIndexes.Get(segID)
		s.True(ok)
		t, ok = segIdxes.Get(indexID)
		s.True(ok)
		t.IndexState = commonpb.IndexState_Unissued
		segIdxes.Insert(indexID, t)
		mt.indexMeta.segmentIndexes.Insert(segID, segIdxes)
		mt.indexMeta.keyLock.Unlock(buildID)

		mt.indexMeta.fieldIndexLock.Lock()
		defer mt.indexMeta.fieldIndexLock.Unlock()
		mt.indexMeta.indexes[collID][indexID].IndexParams[1].Value = "HNSW"
		coll, ok := mt.collections.Get(collID)
		s.True(ok)
		coll.Schema.Fields[0].DataType = schemapb.DataType_FloatVector
		coll.Schema.Fields[1].IsPartitionKey = true
		coll.Schema.Fields[1].DataType = schemapb.DataType_VarChar
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
		s.Equal(1, scheduler.pendingTasks.TaskCount())
		s.Equal(indexpb.JobState_JobStateInit, scheduler.pendingTasks.Get(buildID).GetState())

		scheduler.Start()
		waitTaskDoneFunc(scheduler)
		resetMetaFunc()
	})

	s.Run("Submit valid", func() {
		for _, dataType := range []schemapb.DataType{
			schemapb.DataType_Int8,
			schemapb.DataType_Int16,
			schemapb.DataType_Int32,
			schemapb.DataType_Int64,
			schemapb.DataType_VarChar,
			schemapb.DataType_String,
		} {
			coll, ok := mt.collections.Get(collID)
			s.True(ok)
			coll.Schema.Fields[1].DataType = dataType
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
	s.Run("Submit returns empty optional field when cfg disable", func() {
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

	s.Run("Submit returns empty when vector type is not dense vector", func() {
		paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("true")
		for _, dataType := range []schemapb.DataType{
			schemapb.DataType_SparseFloatVector,
		} {
			coll, ok := mt.collections.Get(collID)
			s.True(ok)
			coll.Schema.Fields[0].DataType = dataType
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

	s.Run("Submit returns empty optional field when the data type is not STRING or VARCHAR or Integer", func() {
		paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("true")
		for _, dataType := range []schemapb.DataType{
			schemapb.DataType_Bool,
			schemapb.DataType_Float,
			schemapb.DataType_Double,
			schemapb.DataType_Array,
			schemapb.DataType_JSON,
		} {
			coll, ok := mt.collections.Get(collID)
			s.True(ok)
			coll.Schema.Fields[1].DataType = dataType
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

	s.Run("Submit returns empty optional field when no partition key", func() {
		paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("true")
		coll, ok := mt.collections.Get(collID)
		s.True(ok)
		coll.Schema.Fields[1].IsPartitionKey = false
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

	s.Run("Submit partitionKeyIsolation is false when schema is not set", func() {
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

	scheduler_isolation := newTaskScheduler(ctx, &mt, workerManager, cm, newIndexEngineVersionManager(), handler_isolation, nil, nil)
	scheduler_isolation.Start()

	s.Run("Submit partitionKeyIsolation is false when MV not enabled", func() {
		paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("false")
		defer paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("true")
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

	s.Run("Submit partitionKeyIsolation is true when MV enabled", func() {
		paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("true")
		defer paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("false")
		isoCollInfo.Properties[common.PartitionKeyIsolationKey] = "true"
		in.EXPECT().CreateJobV2(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, in *workerpb.CreateJobV2Request, opts ...grpc.CallOption) (*commonpb.Status, error) {
				s.True(in.GetIndexRequest().PartitionKeyIsolation)
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

	s.Run("Submit partitionKeyIsolation is invalid when MV is enabled", func() {
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

func (s *taskSchedulerSuite) Test_reload() {
	s.Run("normal case", func() {
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		workerManager := session.NewMockWorkerManager(s.T())
		handler := NewNMockHandler(s.T())
		tasks := typeutil.NewConcurrentMap[UniqueID, *indexpb.StatsTask]()
		statsTask := &indexpb.StatsTask{
			CollectionID:    10000,
			PartitionID:     10001,
			SegmentID:       1000,
			InsertChannel:   "",
			TaskID:          statsTaskID,
			Version:         1,
			NodeID:          1,
			State:           indexpb.JobState_JobStateInProgress,
			FailReason:      "",
			TargetSegmentID: 2000,
			SubJobType:      indexpb.StatsSubJob_Sort,
			CanRecycle:      false,
		}
		tasks.Insert(statsTaskID, statsTask)
		secondaryIndex := typeutil.NewConcurrentMap[string, *indexpb.StatsTask]()
		secondaryKey := createSecondaryIndexKey(statsTask.GetSegmentID(), statsTask.GetSubJobType().String())
		secondaryIndex.Insert(secondaryKey, statsTask)
		mt := createMeta(catalog, withAnalyzeMeta(s.createAnalyzeMeta(catalog)), withIndexMeta(createIndexMeta(catalog)),
			withStatsTaskMeta(&statsTaskMeta{
				ctx:             context.Background(),
				catalog:         catalog,
				tasks:           tasks,
				segmentID2Tasks: secondaryIndex,
			}))
		compactionHandler := NewMockCompactionPlanContext(s.T())
		compactionHandler.EXPECT().checkAndSetSegmentStating(mock.Anything, mock.Anything).Return(true).Maybe()
		scheduler := newTaskScheduler(context.Background(), mt, workerManager, nil, nil, handler, nil, compactionHandler)
		s.NotNil(scheduler)
		s.True(mt.segments.segments[1000].isCompacting)
		task, ok := scheduler.runningTasks.Get(statsTaskID)
		s.True(ok)
		s.NotNil(task)
	})

	s.Run("segment is compacting", func() {
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		catalog.EXPECT().DropStatsTask(mock.Anything, mock.Anything).Return(nil)
		workerManager := session.NewMockWorkerManager(s.T())
		handler := NewNMockHandler(s.T())
		tasks := typeutil.NewConcurrentMap[UniqueID, *indexpb.StatsTask]()
		statsTask := &indexpb.StatsTask{
			CollectionID:    10000,
			PartitionID:     10001,
			SegmentID:       1000,
			InsertChannel:   "",
			TaskID:          statsTaskID,
			Version:         1,
			NodeID:          1,
			State:           indexpb.JobState_JobStateInProgress,
			FailReason:      "",
			TargetSegmentID: 2000,
			SubJobType:      indexpb.StatsSubJob_Sort,
			CanRecycle:      false,
		}
		tasks.Insert(statsTaskID, statsTask)
		secondaryIndex := typeutil.NewConcurrentMap[string, *indexpb.StatsTask]()
		secondaryKey := createSecondaryIndexKey(statsTask.GetSegmentID(), statsTask.GetSubJobType().String())
		secondaryIndex.Insert(secondaryKey, statsTask)
		mt := createMeta(catalog, withAnalyzeMeta(s.createAnalyzeMeta(catalog)), withIndexMeta(createIndexMeta(catalog)),
			withStatsTaskMeta(&statsTaskMeta{
				ctx:             context.Background(),
				catalog:         catalog,
				tasks:           tasks,
				segmentID2Tasks: secondaryIndex,
				keyLock:         lock.NewKeyLock[UniqueID](),
			}))
		compactionHandler := NewMockCompactionPlanContext(s.T())
		compactionHandler.EXPECT().checkAndSetSegmentStating(mock.Anything, mock.Anything).Return(true).Maybe()
		mt.segments.segments[1000].isCompacting = true
		scheduler := newTaskScheduler(context.Background(), mt, workerManager, nil, nil, handler, nil, compactionHandler)
		s.NotNil(scheduler)
		s.True(mt.segments.segments[1000].isCompacting)
		task := scheduler.pendingTasks.Get(statsTaskID)
		s.Nil(task)
	})

	s.Run("drop task failed", func() {
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		catalog.EXPECT().DropStatsTask(mock.Anything, mock.Anything).Return(errors.New("mock error"))
		workerManager := session.NewMockWorkerManager(s.T())
		handler := NewNMockHandler(s.T())
		tasks := typeutil.NewConcurrentMap[UniqueID, *indexpb.StatsTask]()
		statsTask := &indexpb.StatsTask{
			CollectionID:    10000,
			PartitionID:     10001,
			SegmentID:       1000,
			InsertChannel:   "",
			TaskID:          statsTaskID,
			Version:         1,
			NodeID:          1,
			State:           indexpb.JobState_JobStateInProgress,
			FailReason:      "",
			TargetSegmentID: 2000,
			SubJobType:      indexpb.StatsSubJob_Sort,
			CanRecycle:      false,
		}
		tasks.Insert(statsTaskID, statsTask)
		secondaryIndex := typeutil.NewConcurrentMap[string, *indexpb.StatsTask]()
		secondaryKey := createSecondaryIndexKey(statsTask.GetSegmentID(), statsTask.GetSubJobType().String())
		secondaryIndex.Insert(secondaryKey, statsTask)
		mt := createMeta(catalog, withAnalyzeMeta(s.createAnalyzeMeta(catalog)), withIndexMeta(createIndexMeta(catalog)),
			withStatsTaskMeta(&statsTaskMeta{
				ctx:             context.Background(),
				catalog:         catalog,
				tasks:           tasks,
				segmentID2Tasks: secondaryIndex,
				keyLock:         lock.NewKeyLock[UniqueID](),
			}))
		compactionHandler := NewMockCompactionPlanContext(s.T())
		compactionHandler.EXPECT().checkAndSetSegmentStating(mock.Anything, mock.Anything).Return(true).Maybe()
		mt.segments.segments[1000].isCompacting = true
		scheduler := newTaskScheduler(context.Background(), mt, workerManager, nil, nil, handler, nil, compactionHandler)
		s.NotNil(scheduler)
		s.True(mt.segments.segments[1000].isCompacting)
		task, ok := scheduler.runningTasks.Get(statsTaskID)
		s.True(ok)
		s.Equal(indexpb.JobState_JobStateFailed, task.GetState())
	})

	s.Run("segment is in l0 compaction", func() {
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		catalog.EXPECT().DropStatsTask(mock.Anything, mock.Anything).Return(nil)
		workerManager := session.NewMockWorkerManager(s.T())
		handler := NewNMockHandler(s.T())
		tasks := typeutil.NewConcurrentMap[UniqueID, *indexpb.StatsTask]()
		statsTask := &indexpb.StatsTask{
			CollectionID:    10000,
			PartitionID:     10001,
			SegmentID:       1000,
			InsertChannel:   "",
			TaskID:          statsTaskID,
			Version:         1,
			NodeID:          1,
			State:           indexpb.JobState_JobStateInProgress,
			FailReason:      "",
			TargetSegmentID: 2000,
			SubJobType:      indexpb.StatsSubJob_Sort,
			CanRecycle:      false,
		}
		tasks.Insert(statsTaskID, statsTask)
		secondaryIndex := typeutil.NewConcurrentMap[string, *indexpb.StatsTask]()
		secondaryKey := createSecondaryIndexKey(statsTask.GetSegmentID(), statsTask.GetSubJobType().String())
		secondaryIndex.Insert(secondaryKey, statsTask)
		mt := createMeta(catalog, withAnalyzeMeta(s.createAnalyzeMeta(catalog)), withIndexMeta(createIndexMeta(catalog)),
			withStatsTaskMeta(&statsTaskMeta{
				ctx:             context.Background(),
				catalog:         catalog,
				tasks:           tasks,
				segmentID2Tasks: secondaryIndex,
				keyLock:         lock.NewKeyLock[UniqueID](),
			}))
		compactionHandler := NewMockCompactionPlanContext(s.T())
		compactionHandler.EXPECT().checkAndSetSegmentStating(mock.Anything, mock.Anything).Return(false).Maybe()
		mt.segments.segments[1000].isCompacting = false
		scheduler := newTaskScheduler(context.Background(), mt, workerManager, nil, nil, handler, nil, compactionHandler)
		s.NotNil(scheduler)
		s.False(mt.segments.segments[1000].isCompacting)
		task := scheduler.pendingTasks.Get(statsTaskID)
		s.Nil(task)
	})

	s.Run("drop task failed", func() {
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		catalog.EXPECT().DropStatsTask(mock.Anything, mock.Anything).Return(errors.New("mock error"))
		workerManager := session.NewMockWorkerManager(s.T())
		handler := NewNMockHandler(s.T())
		tasks := typeutil.NewConcurrentMap[UniqueID, *indexpb.StatsTask]()
		statsTask := &indexpb.StatsTask{
			CollectionID:    10000,
			PartitionID:     10001,
			SegmentID:       1000,
			InsertChannel:   "",
			TaskID:          statsTaskID,
			Version:         1,
			NodeID:          1,
			State:           indexpb.JobState_JobStateInProgress,
			FailReason:      "",
			TargetSegmentID: 2000,
			SubJobType:      indexpb.StatsSubJob_Sort,
			CanRecycle:      false,
		}
		tasks.Insert(statsTaskID, statsTask)
		secondaryIndex := typeutil.NewConcurrentMap[string, *indexpb.StatsTask]()
		secondaryKey := createSecondaryIndexKey(statsTask.GetSegmentID(), statsTask.GetSubJobType().String())
		secondaryIndex.Insert(secondaryKey, statsTask)

		mt := createMeta(catalog, withAnalyzeMeta(s.createAnalyzeMeta(catalog)), withIndexMeta(createIndexMeta(catalog)),
			withStatsTaskMeta(&statsTaskMeta{
				ctx:             context.Background(),
				catalog:         catalog,
				tasks:           tasks,
				segmentID2Tasks: secondaryIndex,
				keyLock:         lock.NewKeyLock[UniqueID](),
			}))
		compactionHandler := NewMockCompactionPlanContext(s.T())
		compactionHandler.EXPECT().checkAndSetSegmentStating(mock.Anything, mock.Anything).Return(false).Maybe()
		mt.segments.segments[1000].isCompacting = false
		scheduler := newTaskScheduler(context.Background(), mt, workerManager, nil, nil, handler, nil, compactionHandler)
		s.NotNil(scheduler)
		s.False(mt.segments.segments[1000].isCompacting)
		task, ok := scheduler.runningTasks.Get(statsTaskID)
		s.True(ok)
		s.Equal(indexpb.JobState_JobStateFailed, task.GetState())
	})
}

func (s *taskSchedulerSuite) Test_zeroSegmentStats() {
	ctx := context.Background()
	catalog := catalogmocks.NewDataCoordCatalog(s.T())
	taskID := UniqueID(111)
	segID := UniqueID(112)
	targetSegID := UniqueID(113)

	workerManager := session.NewMockWorkerManager(s.T())
	workerManager.EXPECT().QuerySlots().RunAndReturn(func() map[int64]int64 {
		return map[int64]int64{
			1: 1,
		}
	})

	mt := &meta{
		ctx:      ctx,
		catalog:  catalog,
		segments: NewSegmentsInfo(),
		statsTaskMeta: &statsTaskMeta{
			ctx:             ctx,
			catalog:         catalog,
			tasks:           typeutil.NewConcurrentMap[UniqueID, *indexpb.StatsTask](),
			segmentID2Tasks: typeutil.NewConcurrentMap[string, *indexpb.StatsTask](),
			keyLock:         lock.NewKeyLock[UniqueID](),
		},
	}
	mt.statsTaskMeta.tasks.Insert(taskID, &indexpb.StatsTask{
		CollectionID:    1,
		PartitionID:     2,
		SegmentID:       segID,
		InsertChannel:   "ch-1",
		TaskID:          taskID,
		Version:         0,
		NodeID:          0,
		State:           indexpb.JobState_JobStateInit,
		FailReason:      "",
		TargetSegmentID: targetSegID,
		SubJobType:      indexpb.StatsSubJob_Sort,
		CanRecycle:      false,
	})

	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	err := mt.AddSegment(ctx, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            segID,
			CollectionID:  1,
			PartitionID:   2,
			InsertChannel: "ch-1",
			State:         commonpb.SegmentState_Flushed,
			NumOfRows:     0,
		},
	})
	s.NoError(err)
	cm := mocks.NewChunkManager(s.T())
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil)
	in := mocks.NewMockDataNodeClient(s.T())
	in.EXPECT().DropJobsV2(mock.Anything, mock.Anything).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)
	workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true)

	handler := NewNMockHandler(s.T())
	ctx, cancel := context.WithCancel(ctx)
	scheduler := &taskScheduler{
		ctx:                       ctx,
		cancel:                    cancel,
		meta:                      mt,
		pendingTasks:              newFairQueuePolicy(),
		runningTasks:              typeutil.NewConcurrentMap[UniqueID, Task](),
		notifyChan:                make(chan struct{}, 1),
		taskLock:                  lock.NewKeyLock[int64](),
		scheduleDuration:          Params.DataCoordCfg.IndexTaskSchedulerInterval.GetAsDuration(time.Millisecond),
		collectMetricsDuration:    time.Minute,
		policy:                    defaultBuildIndexPolicy,
		nodeManager:               workerManager,
		chunkManager:              cm,
		handler:                   handler,
		indexEngineVersionManager: newIndexEngineVersionManager(),
		allocator:                 nil,
		taskStats:                 expirable.NewLRU[UniqueID, Task](512, nil, time.Minute*15),
		compactionHandler:         nil,
	}
	scheduler.Start()

	scheduler.enqueue(newStatsTask(taskID, segID, targetSegID, indexpb.StatsSubJob_Sort))
	for {
		time.Sleep(time.Second)
		if scheduler.pendingTasks.TaskCount() == 0 {
			taskNum := scheduler.runningTasks.Len()
			if taskNum == 0 {
				break
			}
		}
	}
	scheduler.Stop()
	segment := mt.GetSegment(ctx, targetSegID)
	s.Equal(int64(0), segment.NumOfRows)
	s.Equal(commonpb.SegmentState_Dropped, segment.State)
}

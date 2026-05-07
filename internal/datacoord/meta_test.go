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
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	mockkv "github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	mocks2 "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/metastore"
	"github.com/milvus-io/milvus/pkg/v3/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// MetaReloadSuite tests meta reload & meta creation related logic
type MetaReloadSuite struct {
	testutils.PromMetricsSuite

	catalog *mocks2.DataCoordCatalog
	meta    *meta
}

func (suite *MetaReloadSuite) SetupTest() {
	catalog := mocks2.NewDataCoordCatalog(suite.T())
	suite.catalog = catalog
}

func (suite *MetaReloadSuite) resetMock() {
	suite.catalog.ExpectedCalls = nil
}

func (suite *MetaReloadSuite) TestReloadFromKV() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	suite.Run("ListSegments_fail", func() {
		defer suite.resetMock()
		brk := broker.NewMockBroker(suite.T())
		brk.EXPECT().ShowCollectionIDs(mock.Anything).Return(&rootcoordpb.ShowCollectionIDsResponse{
			Status: merr.Success(),
			DbCollections: []*rootcoordpb.DBCollections{
				{
					DbName:        "db_1",
					CollectionIDs: []int64{100},
				},
			},
		}, nil)
		suite.catalog.EXPECT().ListSegments(mock.Anything, mock.Anything).Return(nil, errors.New("mock"))
		suite.catalog.EXPECT().ListIndexes(mock.Anything).Return([]*model.Index{}, nil)
		suite.catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return([]*model.SegmentIndex{}, nil).Maybe()
		suite.catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

		_, err := newMeta(ctx, suite.catalog, nil, brk)
		suite.Error(err)
	})

	suite.Run("ListChannelCheckpoint_fail", func() {
		defer suite.resetMock()
		brk := broker.NewMockBroker(suite.T())
		brk.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListSegments(mock.Anything, mock.Anything).Return([]*datapb.SegmentInfo{}, nil)
		suite.catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, errors.New("mock"))
		suite.catalog.EXPECT().ListIndexes(mock.Anything).Return([]*model.Index{}, nil)
		suite.catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return([]*model.SegmentIndex{}, nil).Maybe()
		suite.catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

		_, err := newMeta(ctx, suite.catalog, nil, brk)
		suite.Error(err)
	})

	suite.Run("ok", func() {
		defer suite.resetMock()
		brk := broker.NewMockBroker(suite.T())
		brk.EXPECT().ShowCollectionIDs(mock.Anything).Return(&rootcoordpb.ShowCollectionIDsResponse{
			Status: merr.Success(),
			DbCollections: []*rootcoordpb.DBCollections{
				{
					DbName:        "db_1",
					CollectionIDs: []int64{1},
				},
			},
		}, nil)

		suite.catalog.EXPECT().ListIndexes(mock.Anything).Return([]*model.Index{}, nil)
		suite.catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return([]*model.SegmentIndex{}, nil).Maybe()
		suite.catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListSegments(mock.Anything, mock.Anything).Return([]*datapb.SegmentInfo{
			{
				ID:           1,
				CollectionID: 1,
				PartitionID:  1,
				State:        commonpb.SegmentState_Flushed,
			},
		}, nil)
		suite.catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(map[string]*msgpb.MsgPosition{
			"ch": {
				ChannelName: "cn",
				MsgID:       []byte{},
				Timestamp:   1000,
			},
		}, nil)
		suite.catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

		_, err := newMeta(ctx, suite.catalog, nil, brk)
		suite.NoError(err)

		suite.MetricsEqual(metrics.DataCoordNumSegments.WithLabelValues(metrics.FlushedSegmentLabel, datapb.SegmentLevel_Legacy.String(), "unsorted", "0"), 1)
	})

	suite.Run("ListIndexes_fail", func() {
		defer suite.resetMock()
		brk := broker.NewMockBroker(suite.T())
		brk.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, errors.New("mock"))
		suite.catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return([]*model.SegmentIndex{}, nil).Maybe()
		suite.catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

		_, err := newMeta(ctx, suite.catalog, nil, brk)
		suite.Error(err)
	})

	suite.Run("ListAnalyzeTasks_fail", func() {
		defer suite.resetMock()
		brk := broker.NewMockBroker(suite.T())
		brk.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListIndexes(mock.Anything).Return([]*model.Index{}, nil)
		suite.catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return([]*model.SegmentIndex{}, nil).Maybe()
		suite.catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, errors.New("mock"))
		suite.catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

		_, err := newMeta(ctx, suite.catalog, nil, brk)
		suite.Error(err)
	})

	suite.Run("ListPartitionStatsInfos_fail", func() {
		defer suite.resetMock()
		brk := broker.NewMockBroker(suite.T())
		brk.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListIndexes(mock.Anything).Return([]*model.Index{}, nil)
		suite.catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return([]*model.SegmentIndex{}, nil).Maybe()
		suite.catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, errors.New("mock"))
		suite.catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

		_, err := newMeta(ctx, suite.catalog, nil, brk)
		suite.Error(err)
	})

	suite.Run("ListCompactionTask_fail", func() {
		defer suite.resetMock()
		brk := broker.NewMockBroker(suite.T())
		brk.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListIndexes(mock.Anything).Return([]*model.Index{}, nil)
		suite.catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return([]*model.SegmentIndex{}, nil).Maybe()
		suite.catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, errors.New("mock"))
		suite.catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

		_, err := newMeta(ctx, suite.catalog, nil, brk)
		suite.Error(err)
	})

	suite.Run("ListStatsTasks_fail", func() {
		defer suite.resetMock()
		brk := broker.NewMockBroker(suite.T())
		brk.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListIndexes(mock.Anything).Return([]*model.Index{}, nil)
		suite.catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return([]*model.SegmentIndex{}, nil).Maybe()
		suite.catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, errors.New("mock"))
		suite.catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

		_, err := newMeta(ctx, suite.catalog, nil, brk)
		suite.Error(err)
	})

	suite.Run("ListSnapshots_fail", func() {
		defer suite.resetMock()
		brk := broker.NewMockBroker(suite.T())
		brk.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListIndexes(mock.Anything).Return([]*model.Index{}, nil)
		suite.catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return([]*model.SegmentIndex{}, nil).Maybe()
		suite.catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, errors.New("mock"))
		suite.catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

		_, err := newMeta(ctx, suite.catalog, nil, brk)
		suite.Error(err)
	})

	suite.Run("test list segments", func() {
		defer suite.resetMock()
		brk := broker.NewMockBroker(suite.T())
		brk.EXPECT().ShowCollectionIDs(mock.Anything).Return(&rootcoordpb.ShowCollectionIDsResponse{
			Status: merr.Success(),
			DbCollections: []*rootcoordpb.DBCollections{
				{
					DbName:        "db_1",
					CollectionIDs: []int64{100, 101, 102},
				},
				{
					DbName:        "db_2",
					CollectionIDs: []int64{200, 201, 202},
				},
			},
		}, nil)

		suite.catalog.EXPECT().ListIndexes(mock.Anything).Return([]*model.Index{}, nil)
		suite.catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return([]*model.SegmentIndex{}, nil).Maybe()
		suite.catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

		suite.catalog.EXPECT().ListSegments(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, collectionID int64) ([]*datapb.SegmentInfo, error) {
				return []*datapb.SegmentInfo{
					{
						ID:           rand.Int63(),
						CollectionID: collectionID,
						State:        commonpb.SegmentState_Flushed,
					},
				}, nil
			})

		meta, err := newMeta(ctx, suite.catalog, nil, brk)
		suite.NoError(err)
		for _, collectionID := range []int64{100, 101, 102, 200, 201, 202} {
			segments := meta.GetSegmentsOfCollection(ctx, collectionID)
			suite.Len(segments, 1)
			suite.Equal(collectionID, segments[0].GetCollectionID())
		}
	})
}

type MetaBasicSuite struct {
	testutils.PromMetricsSuite

	collID      int64
	partIDs     []int64
	channelName string

	meta *meta
}

func (suite *MetaBasicSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *MetaBasicSuite) SetupTest() {
	suite.collID = 1
	suite.partIDs = []int64{100, 101}
	suite.channelName = "c1"

	meta, err := newMemoryMeta(suite.T())

	suite.Require().NoError(err)
	suite.meta = meta
}

func (suite *MetaBasicSuite) getCollectionInfo(partIDs ...int64) *collectionInfo {
	testSchema := newTestSchema()
	return &collectionInfo{
		ID:             suite.collID,
		Schema:         testSchema,
		Partitions:     partIDs,
		StartPositions: []*commonpb.KeyDataPair{},
	}
}

func (suite *MetaBasicSuite) TestCollection() {
	meta := suite.meta

	info := suite.getCollectionInfo(suite.partIDs...)
	meta.AddCollection(info)

	collInfo := meta.GetCollection(suite.collID)
	suite.Require().NotNil(collInfo)

	// check partition info
	suite.EqualValues(suite.collID, collInfo.ID)
	suite.EqualValues(info.Schema, collInfo.Schema)
	suite.EqualValues(len(suite.partIDs), len(collInfo.Partitions))
	suite.ElementsMatch(info.Partitions, collInfo.Partitions)

	suite.MetricsEqual(metrics.DataCoordNumCollections.WithLabelValues(), 1)
}

func (suite *MetaBasicSuite) TestCompleteCompactionMutation() {
	getLatestSegments := func() *SegmentsInfo {
		latestSegments := NewSegmentsInfo()
		for segID, segment := range map[UniqueID]*SegmentInfo{
			1: {SegmentInfo: &datapb.SegmentInfo{
				ID:           1,
				CollectionID: 100,
				PartitionID:  10,
				State:        commonpb.SegmentState_Flushed,
				Level:        datapb.SegmentLevel_L1,
				Binlogs:      []*datapb.FieldBinlog{getFieldBinlogIDs(0, 10000, 10001)},
				Statslogs:    []*datapb.FieldBinlog{getFieldBinlogIDs(0, 20000, 20001)},
				// latest segment has 2 deltalogs, one submit for compaction, one is appended before compaction done
				Deltalogs: []*datapb.FieldBinlog{getFieldBinlogIDs(0, 30000), getFieldBinlogIDs(0, 30001)},
				NumOfRows: 2,
			}},
			2: {SegmentInfo: &datapb.SegmentInfo{
				ID:           2,
				CollectionID: 100,
				PartitionID:  10,
				State:        commonpb.SegmentState_Flushed,
				Level:        datapb.SegmentLevel_L1,
				Binlogs:      []*datapb.FieldBinlog{getFieldBinlogIDs(0, 11000)},
				Statslogs:    []*datapb.FieldBinlog{getFieldBinlogIDs(0, 21000)},
				// latest segment has 2 deltalogs, one submit for compaction, one is appended before compaction done
				Deltalogs: []*datapb.FieldBinlog{getFieldBinlogIDs(0, 31000), getFieldBinlogIDs(0, 31001)},
				NumOfRows: 2,
			}},
		} {
			latestSegments.SetSegment(segID, segment)
		}

		return latestSegments
	}

	mockChMgr := mocks.NewChunkManager(suite.T())

	suite.Run("test complete with empty result segments", func() {
		// Test case: when all data is deleted, compaction result should have empty Segments slice
		// This verifies that completeMixCompactionMutation correctly handles the case
		// where no output segments are produced (all data deleted during compaction)
		latestSegments := getLatestSegments()

		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{}, // Empty - all data deleted
		}
		task := &datapb.CompactionTask{
			InputSegments: []UniqueID{1, 2},
			Type:          datapb.CompactionType_MixCompaction,
		}
		m := &meta{
			catalog:      &datacoord.Catalog{MetaKv: NewMetaMemoryKV()},
			segments:     latestSegments,
			chunkManager: mockChMgr,
		}

		infos, mutation, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		suite.NoError(err)
		suite.Empty(infos) // No output segments when all data deleted
		suite.NotNil(mutation)

		// check compactFrom segments are marked as Dropped
		for _, segID := range []int64{1, 2} {
			seg := m.GetSegment(context.TODO(), segID)
			suite.Equal(commonpb.SegmentState_Dropped, seg.GetState())
			suite.NotEmpty(seg.GetDroppedAt())
			suite.True(seg.GetCompacted())
		}

		// check mutation metrics - only input segments changed to Dropped
		suite.EqualValues(-4, mutation.rowCountChange)
		flushedUnsorted := mutation.stateChange[datapb.SegmentLevel_L1.String()][commonpb.SegmentState_Flushed.String()][getSortStatus(false)]["0"]
		suite.EqualValues(-2, flushedUnsorted)
		droppedUnsorted := mutation.stateChange[datapb.SegmentLevel_L1.String()][commonpb.SegmentState_Dropped.String()][getSortStatus(false)]["0"]
		suite.EqualValues(2, droppedUnsorted)
	})

	suite.Run("test complete with compactTo 0 num of rows", func() {
		latestSegments := getLatestSegments()
		compactToSeg := &datapb.CompactionSegment{
			SegmentID:           4,
			InsertLogs:          []*datapb.FieldBinlog{},
			Field2StatslogPaths: []*datapb.FieldBinlog{},
			NumOfRows:           0,
		}

		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{compactToSeg},
		}
		task := &datapb.CompactionTask{
			InputSegments: []UniqueID{1, 2},
			Type:          datapb.CompactionType_MixCompaction,
		}
		m := &meta{
			catalog:      &datacoord.Catalog{MetaKv: NewMetaMemoryKV()},
			segments:     latestSegments,
			chunkManager: mockChMgr,
		}

		infos, mutation, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		assert.NoError(suite.T(), err)
		suite.Equal(1, len(infos))
		info := infos[0]
		suite.NoError(err)
		suite.NotNil(info)
		suite.NotNil(mutation)

		// check compact to segments
		suite.EqualValues(4, info.GetID())
		suite.Equal(datapb.SegmentLevel_L1, info.GetLevel())
		suite.Equal(commonpb.SegmentState_Dropped, info.GetState())

		suite.Empty(info.GetBinlogs())
		suite.Empty(info.GetStatslogs())

		// check compactFrom segments
		for _, segID := range []int64{1, 2} {
			seg := m.GetSegment(context.TODO(), segID)
			suite.Equal(commonpb.SegmentState_Dropped, seg.GetState())
			suite.NotEmpty(seg.GetDroppedAt())

			suite.EqualValues(segID, seg.GetID())
			suite.ElementsMatch(latestSegments.segments[segID].GetBinlogs(), seg.GetBinlogs())
			suite.ElementsMatch(latestSegments.segments[segID].GetStatslogs(), seg.GetStatslogs())
			suite.ElementsMatch(latestSegments.segments[segID].GetDeltalogs(), seg.GetDeltalogs())
		}

		// check mutation metrics
		suite.EqualValues(2, len(mutation.stateChange[datapb.SegmentLevel_L1.String()]))
		suite.EqualValues(-4, mutation.rowCountChange)
		suite.EqualValues(0, mutation.rowCountAccChange)
		flushedUnsorted := mutation.stateChange[datapb.SegmentLevel_L1.String()][commonpb.SegmentState_Flushed.String()][getSortStatus(false)]["0"]
		suite.EqualValues(-2, flushedUnsorted)

		droppedUnsorted := mutation.stateChange[datapb.SegmentLevel_L1.String()][commonpb.SegmentState_Dropped.String()][getSortStatus(false)]["0"]
		suite.EqualValues(3, droppedUnsorted)
	})

	suite.Run("test complete compaction mutation", func() {
		latestSegments := getLatestSegments()
		compactToSeg := &datapb.CompactionSegment{
			SegmentID:           3,
			InsertLogs:          []*datapb.FieldBinlog{getFieldBinlogIDs(0, 50000)},
			Field2StatslogPaths: []*datapb.FieldBinlog{getFieldBinlogIDs(0, 50001)},
			NumOfRows:           2,
		}

		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{compactToSeg},
		}
		task := &datapb.CompactionTask{
			InputSegments: []UniqueID{1, 2},
			Type:          datapb.CompactionType_MixCompaction,
		}
		m := &meta{
			catalog:      &datacoord.Catalog{MetaKv: NewMetaMemoryKV()},
			segments:     latestSegments,
			chunkManager: mockChMgr,
		}

		infos, mutation, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		assert.NoError(suite.T(), err)
		suite.Equal(1, len(infos))
		info := infos[0]
		suite.NoError(err)
		suite.NotNil(info)
		suite.NotNil(mutation)

		// check newSegment
		suite.EqualValues(3, info.GetID())
		suite.Equal(datapb.SegmentLevel_L1, info.GetLevel())
		suite.Equal(commonpb.SegmentState_Flushed, info.GetState())

		binlogs := info.GetBinlogs()
		for _, fbinlog := range binlogs {
			for _, blog := range fbinlog.GetBinlogs() {
				suite.Empty(blog.GetLogPath())
				suite.EqualValues(50000, blog.GetLogID())
			}
		}

		statslogs := info.GetStatslogs()
		for _, fbinlog := range statslogs {
			for _, blog := range fbinlog.GetBinlogs() {
				suite.Empty(blog.GetLogPath())
				suite.EqualValues(50001, blog.GetLogID())
			}
		}

		// check compactFrom segments
		for _, segID := range []int64{1, 2} {
			seg := m.GetSegment(context.TODO(), segID)
			suite.Equal(commonpb.SegmentState_Dropped, seg.GetState())
			suite.NotEmpty(seg.GetDroppedAt())

			suite.EqualValues(segID, seg.GetID())
			suite.ElementsMatch(latestSegments.segments[segID].GetBinlogs(), seg.GetBinlogs())
			suite.ElementsMatch(latestSegments.segments[segID].GetStatslogs(), seg.GetStatslogs())
			suite.ElementsMatch(latestSegments.segments[segID].GetDeltalogs(), seg.GetDeltalogs())
		}

		// check mutation metrics
		suite.EqualValues(2, len(mutation.stateChange[datapb.SegmentLevel_L1.String()]))
		suite.EqualValues(-2, mutation.rowCountChange)
		suite.EqualValues(2, mutation.rowCountAccChange)
		flushedCount := mutation.stateChange[datapb.SegmentLevel_L1.String()][commonpb.SegmentState_Flushed.String()][getSortStatus(false)]["0"]
		suite.EqualValues(-1, flushedCount)

		droppedCount := mutation.stateChange[datapb.SegmentLevel_L1.String()][commonpb.SegmentState_Dropped.String()][getSortStatus(false)]["0"]
		suite.EqualValues(2, droppedCount)
	})

	suite.Run("test L2 sort", func() {
		getLatestSegments := func() *SegmentsInfo {
			latestSegments := NewSegmentsInfo()
			for segID, segment := range map[UniqueID]*SegmentInfo{
				1: {SegmentInfo: &datapb.SegmentInfo{
					ID:           1,
					CollectionID: 100,
					PartitionID:  10,
					State:        commonpb.SegmentState_Flushed,
					Level:        datapb.SegmentLevel_L2,
					Binlogs:      []*datapb.FieldBinlog{getFieldBinlogIDs(0, 10000, 10001)},
					Statslogs:    []*datapb.FieldBinlog{getFieldBinlogIDs(0, 20000, 20001)},
					// latest segment has 2 deltalogs, one submit for compaction, one is appended before compaction done
					Deltalogs:      []*datapb.FieldBinlog{getFieldBinlogIDs(0, 30000), getFieldBinlogIDs(0, 30001)},
					NumOfRows:      2,
					StorageVersion: storage.StorageV1,
				}},
			} {
				latestSegments.SetSegment(segID, segment)
			}

			return latestSegments
		}

		latestSegments := getLatestSegments()
		compactToSeg := &datapb.CompactionSegment{
			SegmentID:           2,
			InsertLogs:          []*datapb.FieldBinlog{getFieldBinlogIDs(0, 50000)},
			Field2StatslogPaths: []*datapb.FieldBinlog{getFieldBinlogIDs(0, 50001)},
			NumOfRows:           2,
			StorageVersion:      storage.StorageV2,
		}

		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{compactToSeg},
		}
		task := &datapb.CompactionTask{
			InputSegments: []UniqueID{1},
			Type:          datapb.CompactionType_SortCompaction,
		}
		m := &meta{
			catalog:      &datacoord.Catalog{MetaKv: NewMetaMemoryKV()},
			segments:     latestSegments,
			chunkManager: mockChMgr,
		}

		infos, mutation, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		assert.NoError(suite.T(), err)
		suite.Equal(1, len(infos))
		info := infos[0]
		suite.NoError(err)
		suite.NotNil(info)
		suite.NotNil(mutation)

		// check newSegment
		suite.EqualValues(2, info.GetID())
		suite.Equal(datapb.SegmentLevel_L2, info.GetLevel())
		suite.Equal(commonpb.SegmentState_Flushed, info.GetState())
		suite.Equal(storage.StorageV2, info.GetStorageVersion())

		binlogs := info.GetBinlogs()
		for _, fbinlog := range binlogs {
			for _, blog := range fbinlog.GetBinlogs() {
				suite.Empty(blog.GetLogPath())
				suite.EqualValues(50000, blog.GetLogID())
			}
		}

		statslogs := info.GetStatslogs()
		for _, fbinlog := range statslogs {
			for _, blog := range fbinlog.GetBinlogs() {
				suite.Empty(blog.GetLogPath())
				suite.EqualValues(50001, blog.GetLogID())
			}
		}

		// check compactFrom segments
		for _, segID := range []int64{1} {
			seg := m.GetSegment(context.TODO(), segID)
			suite.Equal(commonpb.SegmentState_Dropped, seg.GetState())
			suite.NotEmpty(seg.GetDroppedAt())
		}
	})
}

func (suite *MetaBasicSuite) TestValidateSegmentState_BlockedBySnapshot() {
	latestSegments := NewSegmentsInfo()
	for segID, segment := range map[UniqueID]*SegmentInfo{
		1: {SegmentInfo: &datapb.SegmentInfo{
			ID:           1,
			CollectionID: 100,
			PartitionID:  10,
			State:        commonpb.SegmentState_Flushed,
		}},
	} {
		latestSegments.SetSegment(segID, segment)
	}

	task := &datapb.CompactionTask{
		PlanID:        999,
		InputSegments: []UniqueID{1},
		CollectionID:  100,
		Type:          datapb.CompactionType_MixCompaction,
	}

	suite.Run("rejected by snapshot pending collection", func() {
		sm := createTestSnapshotMetaLoaded(suite.T())
		sm.SetSnapshotPending(100)

		m := &meta{
			segments:     latestSegments,
			snapshotMeta: sm,
		}

		err := m.ValidateSegmentStateBeforeCompleteCompactionMutation(task)
		suite.Error(err)
		suite.Contains(err.Error(), "compaction blocked")
	})

	suite.Run("rejected by segment protection", func() {
		sm := createTestSnapshotMetaLoaded(suite.T())
		futureTs := uint64(time.Now().Unix()) + 3600
		sm.segmentProtectionMu.Lock()
		sm.segmentProtectionUntil[1] = futureTs
		sm.segmentProtectionMu.Unlock()

		m := &meta{
			segments:     latestSegments,
			snapshotMeta: sm,
		}

		err := m.ValidateSegmentStateBeforeCompleteCompactionMutation(task)
		suite.Error(err)
		suite.Contains(err.Error(), "segment 1")
	})

	suite.Run("passes when no snapshot", func() {
		sm := createTestSnapshotMetaLoaded(suite.T())

		m := &meta{
			segments:     latestSegments,
			snapshotMeta: sm,
		}

		err := m.ValidateSegmentStateBeforeCompleteCompactionMutation(task)
		suite.NoError(err)
	})

	suite.Run("passes when snapshotMeta is nil", func() {
		m := &meta{
			segments: latestSegments,
		}

		err := m.ValidateSegmentStateBeforeCompleteCompactionMutation(task)
		suite.NoError(err)
	})

	suite.Run("rejected when only middle segment is protected in multi-segment task", func() {
		multiSegments := NewSegmentsInfo()
		for segID, segment := range map[UniqueID]*SegmentInfo{
			1: {SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 100, PartitionID: 10, State: commonpb.SegmentState_Flushed}},
			2: {SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 100, PartitionID: 10, State: commonpb.SegmentState_Flushed}},
			3: {SegmentInfo: &datapb.SegmentInfo{ID: 3, CollectionID: 100, PartitionID: 10, State: commonpb.SegmentState_Flushed}},
		} {
			multiSegments.SetSegment(segID, segment)
		}

		multiTask := &datapb.CompactionTask{
			PlanID:        998,
			InputSegments: []UniqueID{1, 2, 3},
			CollectionID:  100,
			Type:          datapb.CompactionType_MixCompaction,
		}

		sm := createTestSnapshotMetaLoaded(suite.T())
		futureTs := uint64(time.Now().Unix()) + 3600
		// Only protect segment 2
		sm.segmentProtectionMu.Lock()
		sm.segmentProtectionUntil[2] = futureTs
		sm.segmentProtectionMu.Unlock()

		m := &meta{
			segments:     multiSegments,
			snapshotMeta: sm,
		}

		err := m.ValidateSegmentStateBeforeCompleteCompactionMutation(multiTask)
		suite.Error(err)
		suite.Contains(err.Error(), "segment 2")
	})

	suite.Run("passes when protection expired", func() {
		sm := createTestSnapshotMetaLoaded(suite.T())
		pastTs := uint64(time.Now().Unix()) - 100
		sm.segmentProtectionMu.Lock()
		sm.segmentProtectionUntil[1] = pastTs
		sm.segmentProtectionMu.Unlock()

		m := &meta{
			segments:     latestSegments,
			snapshotMeta: sm,
		}

		err := m.ValidateSegmentStateBeforeCompleteCompactionMutation(task)
		suite.NoError(err)
	})

	// Regression: L0 delete compaction must bypass snapshot protection checks entirely.
	// Snapshots only reference sealed L1/L2 segments; blocking L0 here would cause
	// delta log accumulation, query latency spikes, and write stalls.
	suite.Run("L0 compaction passes even when collection is snapshot-pending", func() {
		sm := createTestSnapshotMetaLoaded(suite.T())
		sm.SetSnapshotPending(100)

		m := &meta{
			segments:     latestSegments,
			snapshotMeta: sm,
		}

		l0Task := &datapb.CompactionTask{
			PlanID:        8001,
			InputSegments: []UniqueID{1},
			CollectionID:  100,
			Type:          datapb.CompactionType_Level0DeleteCompaction,
		}
		err := m.ValidateSegmentStateBeforeCompleteCompactionMutation(l0Task)
		suite.NoError(err)
	})

	suite.Run("L0 compaction passes even when input segment is snapshot-protected", func() {
		sm := createTestSnapshotMetaLoaded(suite.T())
		futureTs := uint64(time.Now().Unix()) + 3600
		sm.segmentProtectionMu.Lock()
		sm.segmentProtectionUntil[1] = futureTs
		sm.segmentProtectionMu.Unlock()

		m := &meta{
			segments:     latestSegments,
			snapshotMeta: sm,
		}

		l0Task := &datapb.CompactionTask{
			PlanID:        8002,
			InputSegments: []UniqueID{1},
			CollectionID:  100,
			Type:          datapb.CompactionType_Level0DeleteCompaction,
		}
		err := m.ValidateSegmentStateBeforeCompleteCompactionMutation(l0Task)
		suite.NoError(err)
	})

	// Regression: collection-level block should produce ErrCompactionBlocked,
	// not ErrServiceInternal, so SRE alerting does not treat it as a P0 fault.
	suite.Run("rejection uses ErrCompactionBlocked, not ErrServiceInternal", func() {
		sm := createTestSnapshotMetaLoaded(suite.T())
		sm.SetSnapshotPending(100)

		m := &meta{
			segments:     latestSegments,
			snapshotMeta: sm,
		}

		err := m.ValidateSegmentStateBeforeCompleteCompactionMutation(task)
		suite.Error(err)
		suite.True(errors.Is(err, merr.ErrCompactionBlocked))
		suite.False(errors.Is(err, merr.ErrServiceInternal))
	})
}

func (suite *MetaBasicSuite) TestGetMaxPosition() {
	suite.Run("nil_positions", func() {
		pos := getMaxPosition(nil)
		suite.Nil(pos)
	})

	suite.Run("single_position", func() {
		p := &msgpb.MsgPosition{Timestamp: 100}
		pos := getMaxPosition([]*msgpb.MsgPosition{p})
		suite.Equal(uint64(100), pos.GetTimestamp())
	})

	suite.Run("multiple_positions", func() {
		pos := getMaxPosition([]*msgpb.MsgPosition{
			{Timestamp: 100},
			{Timestamp: 300},
			{Timestamp: 200},
		})
		suite.Equal(uint64(300), pos.GetTimestamp())
	})

	suite.Run("with_nil_entries", func() {
		pos := getMaxPosition([]*msgpb.MsgPosition{
			nil,
			{Timestamp: 50},
			nil,
			{Timestamp: 200},
		})
		suite.Equal(uint64(200), pos.GetTimestamp())
	})
}

func (suite *MetaBasicSuite) TestRecalculateSegmentPosition() {
	channel := "ch-1"
	fallbackStart := &msgpb.MsgPosition{ChannelName: channel, Timestamp: 10}
	fallbackDml := &msgpb.MsgPosition{ChannelName: channel, Timestamp: 90}

	suite.Run("recalculates_from_binlog_timestamps", func() {
		binlogs := []*datapb.FieldBinlog{
			{
				FieldID: 0,
				Binlogs: []*datapb.Binlog{
					{LogID: 1, TimestampFrom: 100, TimestampTo: 200},
					{LogID: 2, TimestampFrom: 150, TimestampTo: 300},
				},
			},
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{
					{LogID: 3, TimestampFrom: 80, TimestampTo: 250},
				},
			},
		}
		startPos, dmlPos := recalculateSegmentPosition(binlogs, channel, fallbackStart, fallbackDml)
		suite.Equal(uint64(80), startPos.GetTimestamp())
		suite.Equal(uint64(300), dmlPos.GetTimestamp())
		suite.Equal(channel, startPos.GetChannelName())
		suite.Equal(channel, dmlPos.GetChannelName())
	})

	suite.Run("fallback_when_timestamps_zero", func() {
		binlogs := []*datapb.FieldBinlog{
			{
				FieldID: 0,
				Binlogs: []*datapb.Binlog{
					{LogID: 1, TimestampFrom: 0, TimestampTo: 0},
				},
			},
		}
		startPos, dmlPos := recalculateSegmentPosition(binlogs, channel, fallbackStart, fallbackDml)
		suite.Equal(fallbackStart, startPos)
		suite.Equal(fallbackDml, dmlPos)
	})

	suite.Run("fallback_when_no_binlogs", func() {
		startPos, dmlPos := recalculateSegmentPosition(nil, channel, fallbackStart, fallbackDml)
		suite.Equal(fallbackStart, startPos)
		suite.Equal(fallbackDml, dmlPos)
	})

	suite.Run("fallback_when_empty_binlogs", func() {
		startPos, dmlPos := recalculateSegmentPosition([]*datapb.FieldBinlog{}, channel, fallbackStart, fallbackDml)
		suite.Equal(fallbackStart, startPos)
		suite.Equal(fallbackDml, dmlPos)
	})
}

func (suite *MetaBasicSuite) TestCompleteCompactionMutation_RecalculatePositions() {
	mockChMgr := mocks.NewChunkManager(suite.T())

	// Helper to build FieldBinlog with timestamps
	fieldBinlogWithTimestamps := func(fieldID int64, logID int64, tsFrom, tsTo uint64) *datapb.FieldBinlog {
		return &datapb.FieldBinlog{
			FieldID: fieldID,
			Binlogs: []*datapb.Binlog{
				{LogID: logID, TimestampFrom: tsFrom, TimestampTo: tsTo},
			},
		}
	}

	suite.Run("mix_compaction_recalculates_positions_from_binlogs", func() {
		latestSegments := NewSegmentsInfo()
		for segID, segment := range map[UniqueID]*SegmentInfo{
			1: {SegmentInfo: &datapb.SegmentInfo{
				ID:            1,
				CollectionID:  100,
				PartitionID:   10,
				State:         commonpb.SegmentState_Flushed,
				Level:         datapb.SegmentLevel_L1,
				Binlogs:       []*datapb.FieldBinlog{getFieldBinlogIDs(0, 10000)},
				Statslogs:     []*datapb.FieldBinlog{getFieldBinlogIDs(0, 20000)},
				NumOfRows:     2,
				StartPosition: &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 999},
				DmlPosition:   &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 1},
			}},
			2: {SegmentInfo: &datapb.SegmentInfo{
				ID:            2,
				CollectionID:  100,
				PartitionID:   10,
				State:         commonpb.SegmentState_Flushed,
				Level:         datapb.SegmentLevel_L1,
				Binlogs:       []*datapb.FieldBinlog{getFieldBinlogIDs(0, 11000)},
				Statslogs:     []*datapb.FieldBinlog{getFieldBinlogIDs(0, 21000)},
				NumOfRows:     2,
				StartPosition: &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 888},
				DmlPosition:   &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 2},
			}},
		} {
			latestSegments.SetSegment(segID, segment)
		}

		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{{
				SegmentID:           3,
				InsertLogs:          []*datapb.FieldBinlog{fieldBinlogWithTimestamps(0, 50000, 100, 500)},
				Field2StatslogPaths: []*datapb.FieldBinlog{getFieldBinlogIDs(0, 50001)},
				NumOfRows:           4,
			}},
		}
		task := &datapb.CompactionTask{
			InputSegments: []UniqueID{1, 2},
			Type:          datapb.CompactionType_MixCompaction,
			Channel:       "ch-1",
		}
		m := &meta{
			catalog:      &datacoord.Catalog{MetaKv: NewMetaMemoryKV()},
			segments:     latestSegments,
			chunkManager: mockChMgr,
		}

		infos, _, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		suite.NoError(err)
		suite.Require().Equal(1, len(infos))

		// Should use binlog timestamps, NOT inherited positions
		suite.Equal(uint64(100), infos[0].GetStartPosition().GetTimestamp())
		suite.Equal(uint64(500), infos[0].GetDmlPosition().GetTimestamp())
		suite.Equal("ch-1", infos[0].GetStartPosition().GetChannelName())
	})

	suite.Run("mix_compaction_fallback_when_no_timestamps", func() {
		latestSegments := NewSegmentsInfo()
		for segID, segment := range map[UniqueID]*SegmentInfo{
			1: {SegmentInfo: &datapb.SegmentInfo{
				ID:            1,
				CollectionID:  100,
				PartitionID:   10,
				State:         commonpb.SegmentState_Flushed,
				Level:         datapb.SegmentLevel_L1,
				Binlogs:       []*datapb.FieldBinlog{getFieldBinlogIDs(0, 10000)},
				Statslogs:     []*datapb.FieldBinlog{getFieldBinlogIDs(0, 20000)},
				NumOfRows:     2,
				StartPosition: &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 50},
				DmlPosition:   &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 200},
			}},
			2: {SegmentInfo: &datapb.SegmentInfo{
				ID:            2,
				CollectionID:  100,
				PartitionID:   10,
				State:         commonpb.SegmentState_Flushed,
				Level:         datapb.SegmentLevel_L1,
				Binlogs:       []*datapb.FieldBinlog{getFieldBinlogIDs(0, 11000)},
				Statslogs:     []*datapb.FieldBinlog{getFieldBinlogIDs(0, 21000)},
				NumOfRows:     2,
				StartPosition: &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 100},
				DmlPosition:   &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 300},
			}},
		} {
			latestSegments.SetSegment(segID, segment)
		}

		// Result binlogs have no timestamps (legacy)
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{{
				SegmentID:           3,
				InsertLogs:          []*datapb.FieldBinlog{getFieldBinlogIDs(0, 50000)},
				Field2StatslogPaths: []*datapb.FieldBinlog{getFieldBinlogIDs(0, 50001)},
				NumOfRows:           4,
			}},
		}
		task := &datapb.CompactionTask{
			InputSegments: []UniqueID{1, 2},
			Type:          datapb.CompactionType_MixCompaction,
			Channel:       "ch-1",
		}
		m := &meta{
			catalog:      &datacoord.Catalog{MetaKv: NewMetaMemoryKV()},
			segments:     latestSegments,
			chunkManager: mockChMgr,
		}

		infos, _, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		suite.NoError(err)
		suite.Require().Equal(1, len(infos))

		// Fallback: StartPosition = min, DmlPosition = max of source segments
		suite.Equal(uint64(50), infos[0].GetStartPosition().GetTimestamp())
		suite.Equal(uint64(300), infos[0].GetDmlPosition().GetTimestamp())
	})

	suite.Run("cluster_compaction_recalculates_positions", func() {
		latestSegments := NewSegmentsInfo()
		for segID, segment := range map[UniqueID]*SegmentInfo{
			1: {SegmentInfo: &datapb.SegmentInfo{
				ID:            1,
				CollectionID:  100,
				PartitionID:   10,
				State:         commonpb.SegmentState_Flushed,
				Level:         datapb.SegmentLevel_L1,
				Binlogs:       []*datapb.FieldBinlog{getFieldBinlogIDs(0, 10000)},
				Statslogs:     []*datapb.FieldBinlog{getFieldBinlogIDs(0, 20000)},
				NumOfRows:     2,
				StartPosition: &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 999},
				DmlPosition:   &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 1},
			}},
			2: {SegmentInfo: &datapb.SegmentInfo{
				ID:            2,
				CollectionID:  100,
				PartitionID:   10,
				State:         commonpb.SegmentState_Flushed,
				Level:         datapb.SegmentLevel_L1,
				Binlogs:       []*datapb.FieldBinlog{getFieldBinlogIDs(0, 11000)},
				Statslogs:     []*datapb.FieldBinlog{getFieldBinlogIDs(0, 21000)},
				NumOfRows:     2,
				StartPosition: &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 888},
				DmlPosition:   &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 2},
			}},
		} {
			latestSegments.SetSegment(segID, segment)
		}

		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{{
				SegmentID:           3,
				InsertLogs:          []*datapb.FieldBinlog{fieldBinlogWithTimestamps(0, 50000, 150, 600)},
				Field2StatslogPaths: []*datapb.FieldBinlog{getFieldBinlogIDs(0, 50001)},
				NumOfRows:           4,
			}},
		}
		task := &datapb.CompactionTask{
			InputSegments: []UniqueID{1, 2},
			Type:          datapb.CompactionType_ClusteringCompaction,
			Channel:       "ch-1",
		}
		m := &meta{
			catalog:      &datacoord.Catalog{MetaKv: NewMetaMemoryKV()},
			segments:     latestSegments,
			chunkManager: mockChMgr,
		}

		infos, _, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		suite.NoError(err)
		suite.Require().Equal(1, len(infos))

		suite.Equal(uint64(150), infos[0].GetStartPosition().GetTimestamp())
		suite.Equal(uint64(600), infos[0].GetDmlPosition().GetTimestamp())
		suite.Equal(datapb.SegmentLevel_L2, infos[0].GetLevel())
	})

	suite.Run("cluster_compaction_fallback_when_no_timestamps", func() {
		latestSegments := NewSegmentsInfo()
		for segID, segment := range map[UniqueID]*SegmentInfo{
			1: {SegmentInfo: &datapb.SegmentInfo{
				ID:            1,
				CollectionID:  100,
				PartitionID:   10,
				State:         commonpb.SegmentState_Flushed,
				Level:         datapb.SegmentLevel_L1,
				Binlogs:       []*datapb.FieldBinlog{getFieldBinlogIDs(0, 10000)},
				Statslogs:     []*datapb.FieldBinlog{getFieldBinlogIDs(0, 20000)},
				NumOfRows:     2,
				StartPosition: &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 50},
				DmlPosition:   &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 400},
			}},
			2: {SegmentInfo: &datapb.SegmentInfo{
				ID:            2,
				CollectionID:  100,
				PartitionID:   10,
				State:         commonpb.SegmentState_Flushed,
				Level:         datapb.SegmentLevel_L1,
				Binlogs:       []*datapb.FieldBinlog{getFieldBinlogIDs(0, 11000)},
				Statslogs:     []*datapb.FieldBinlog{getFieldBinlogIDs(0, 21000)},
				NumOfRows:     2,
				StartPosition: &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 80},
				DmlPosition:   &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 500},
			}},
		} {
			latestSegments.SetSegment(segID, segment)
		}

		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{{
				SegmentID:           3,
				InsertLogs:          []*datapb.FieldBinlog{getFieldBinlogIDs(0, 50000)},
				Field2StatslogPaths: []*datapb.FieldBinlog{getFieldBinlogIDs(0, 50001)},
				NumOfRows:           4,
			}},
		}
		task := &datapb.CompactionTask{
			InputSegments: []UniqueID{1, 2},
			Type:          datapb.CompactionType_ClusteringCompaction,
			Channel:       "ch-1",
		}
		m := &meta{
			catalog:      &datacoord.Catalog{MetaKv: NewMetaMemoryKV()},
			segments:     latestSegments,
			chunkManager: mockChMgr,
		}

		infos, _, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		suite.NoError(err)
		suite.Require().Equal(1, len(infos))

		suite.Equal(uint64(50), infos[0].GetStartPosition().GetTimestamp())
		suite.Equal(uint64(500), infos[0].GetDmlPosition().GetTimestamp())
	})

	suite.Run("sort_compaction_recalculates_positions", func() {
		latestSegments := NewSegmentsInfo()
		latestSegments.SetSegment(1, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:             1,
			CollectionID:   100,
			PartitionID:    10,
			State:          commonpb.SegmentState_Flushed,
			Level:          datapb.SegmentLevel_L2,
			Binlogs:        []*datapb.FieldBinlog{getFieldBinlogIDs(0, 10000)},
			Statslogs:      []*datapb.FieldBinlog{getFieldBinlogIDs(0, 20000)},
			NumOfRows:      2,
			StartPosition:  &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 999},
			DmlPosition:    &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 1},
			InsertChannel:  "ch-1",
			StorageVersion: storage.StorageV1,
		}})

		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{{
				SegmentID:           2,
				InsertLogs:          []*datapb.FieldBinlog{fieldBinlogWithTimestamps(0, 50000, 200, 800)},
				Field2StatslogPaths: []*datapb.FieldBinlog{getFieldBinlogIDs(0, 50001)},
				NumOfRows:           2,
				StorageVersion:      storage.StorageV2,
			}},
		}
		task := &datapb.CompactionTask{
			InputSegments: []UniqueID{1},
			Type:          datapb.CompactionType_SortCompaction,
		}
		m := &meta{
			catalog:      &datacoord.Catalog{MetaKv: NewMetaMemoryKV()},
			segments:     latestSegments,
			chunkManager: mockChMgr,
		}

		infos, _, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		suite.NoError(err)
		suite.Require().Equal(1, len(infos))

		suite.Equal(uint64(200), infos[0].GetStartPosition().GetTimestamp())
		suite.Equal(uint64(800), infos[0].GetDmlPosition().GetTimestamp())
	})

	suite.Run("sort_compaction_fallback_when_no_timestamps", func() {
		latestSegments := NewSegmentsInfo()
		latestSegments.SetSegment(1, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:             1,
			CollectionID:   100,
			PartitionID:    10,
			State:          commonpb.SegmentState_Flushed,
			Level:          datapb.SegmentLevel_L2,
			Binlogs:        []*datapb.FieldBinlog{getFieldBinlogIDs(0, 10000)},
			Statslogs:      []*datapb.FieldBinlog{getFieldBinlogIDs(0, 20000)},
			NumOfRows:      2,
			StartPosition:  &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 77},
			DmlPosition:    &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 333},
			InsertChannel:  "ch-1",
			StorageVersion: storage.StorageV1,
		}})

		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{{
				SegmentID:           2,
				InsertLogs:          []*datapb.FieldBinlog{getFieldBinlogIDs(0, 50000)},
				Field2StatslogPaths: []*datapb.FieldBinlog{getFieldBinlogIDs(0, 50001)},
				NumOfRows:           2,
				StorageVersion:      storage.StorageV2,
			}},
		}
		task := &datapb.CompactionTask{
			InputSegments: []UniqueID{1},
			Type:          datapb.CompactionType_SortCompaction,
		}
		m := &meta{
			catalog:      &datacoord.Catalog{MetaKv: NewMetaMemoryKV()},
			segments:     latestSegments,
			chunkManager: mockChMgr,
		}

		infos, _, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		suite.NoError(err)
		suite.Require().Equal(1, len(infos))

		suite.Equal(uint64(77), infos[0].GetStartPosition().GetTimestamp())
		suite.Equal(uint64(333), infos[0].GetDmlPosition().GetTimestamp())
	})
}

func (suite *MetaBasicSuite) TestSetSegment() {
	meta := suite.meta
	catalog := mocks2.NewDataCoordCatalog(suite.T())
	meta.catalog = catalog
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	suite.Run("normal", func() {
		segmentID := int64(1000)
		catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Once()
		segment := NewSegmentInfo(&datapb.SegmentInfo{
			ID:            segmentID,
			MaxRowNum:     30000,
			CollectionID:  suite.collID,
			InsertChannel: suite.channelName,
			State:         commonpb.SegmentState_Flushed,
		})
		err := meta.AddSegment(ctx, segment)
		suite.Require().NoError(err)

		noOp := func(segment *SegmentInfo) bool {
			return true
		}

		catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Once()

		err = meta.UpdateSegment(segmentID, noOp)
		suite.NoError(err)
	})

	suite.Run("not_updated", func() {
		segmentID := int64(1001)
		catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Once()
		segment := NewSegmentInfo(&datapb.SegmentInfo{
			ID:            segmentID,
			MaxRowNum:     30000,
			CollectionID:  suite.collID,
			InsertChannel: suite.channelName,
			State:         commonpb.SegmentState_Flushed,
		})
		err := meta.AddSegment(ctx, segment)
		suite.Require().NoError(err)

		noOp := func(segment *SegmentInfo) bool {
			return false
		}

		err = meta.UpdateSegment(segmentID, noOp)
		suite.NoError(err)
	})

	suite.Run("catalog_error", func() {
		segmentID := int64(1002)
		catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Once()
		segment := NewSegmentInfo(&datapb.SegmentInfo{
			ID:            segmentID,
			MaxRowNum:     30000,
			CollectionID:  suite.collID,
			InsertChannel: suite.channelName,
			State:         commonpb.SegmentState_Flushed,
		})
		err := meta.AddSegment(ctx, segment)
		suite.Require().NoError(err)

		noOp := func(segment *SegmentInfo) bool {
			return true
		}

		catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(errors.New("mocked")).Once()

		err = meta.UpdateSegment(segmentID, noOp)
		suite.Error(err)
	})

	suite.Run("segment_not_found", func() {
		segmentID := int64(1003)

		noOp := func(segment *SegmentInfo) bool {
			return true
		}

		err := meta.UpdateSegment(segmentID, noOp)
		suite.Error(err)
		suite.ErrorIs(err, merr.ErrSegmentNotFound)
	})
}

func (suite *MetaBasicSuite) TestCompleteBackfillCompactionMutation() {
	// Helper to build a SegmentsInfo containing a single healthy Flushed segment with the given ID.
	makeSegments := func(segID int64, state commonpb.SegmentState) *SegmentsInfo {
		segs := NewSegmentsInfo()
		segs.SetSegment(segID, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            segID,
			CollectionID:  100,
			PartitionID:   10,
			State:         state,
			Level:         datapb.SegmentLevel_L1,
			Binlogs:       []*datapb.FieldBinlog{getFieldBinlogIDs(0, 10000)},
			Statslogs:     []*datapb.FieldBinlog{getFieldBinlogIDs(0, 20000)},
			NumOfRows:     5,
			SchemaVersion: 1,
		}})
		return segs
	}

	suite.Run("too many input segments", func() {
		m := &meta{
			catalog:  &datacoord.Catalog{MetaKv: NewMetaMemoryKV()},
			segments: makeSegments(1, commonpb.SegmentState_Flushed),
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1, 2}, // two inputs — should error
			Type:          datapb.CompactionType_BackfillCompaction,
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{SegmentID: 1},
			},
		}
		infos, mutation, err := m.completeBackfillCompactionMutation(task, result)
		suite.Error(err)
		suite.Nil(infos)
		suite.Nil(mutation)
	})

	suite.Run("too many result segments", func() {
		m := &meta{
			catalog:  &datacoord.Catalog{MetaKv: NewMetaMemoryKV()},
			segments: makeSegments(1, commonpb.SegmentState_Flushed),
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BackfillCompaction,
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{SegmentID: 1},
				{SegmentID: 2}, // two results — should error
			},
		}
		infos, mutation, err := m.completeBackfillCompactionMutation(task, result)
		suite.Error(err)
		suite.Nil(infos)
		suite.Nil(mutation)
	})

	suite.Run("segment not found", func() {
		// Segment 99 is not in meta.
		m := &meta{
			catalog:  &datacoord.Catalog{MetaKv: NewMetaMemoryKV()},
			segments: NewSegmentsInfo(),
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{99},
			Type:          datapb.CompactionType_BackfillCompaction,
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{SegmentID: 99},
			},
		}
		infos, mutation, err := m.completeBackfillCompactionMutation(task, result)
		suite.Error(err)
		suite.ErrorIs(err, merr.ErrSegmentNotFound)
		suite.Nil(infos)
		suite.Nil(mutation)
	})

	suite.Run("segment dropped", func() {
		m := &meta{
			catalog:  &datacoord.Catalog{MetaKv: NewMetaMemoryKV()},
			segments: makeSegments(1, commonpb.SegmentState_Dropped),
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BackfillCompaction,
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{SegmentID: 1},
			},
		}
		infos, mutation, err := m.completeBackfillCompactionMutation(task, result)
		suite.Error(err)
		suite.ErrorIs(err, merr.ErrSegmentNotFound)
		suite.Nil(infos)
		suite.Nil(mutation)
	})

	suite.Run("segment ID mismatch", func() {
		m := &meta{
			catalog:  &datacoord.Catalog{MetaKv: NewMetaMemoryKV()},
			segments: makeSegments(1, commonpb.SegmentState_Flushed),
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BackfillCompaction,
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{SegmentID: 999}, // ID mismatch
			},
		}
		infos, mutation, err := m.completeBackfillCompactionMutation(task, result)
		suite.Error(err)
		suite.Nil(infos)
		suite.Nil(mutation)
	})

	suite.Run("v2 success - schema version updated", func() {
		// Task schema version (3) > old segment schema version (1) → cloned.SchemaVersion should become 3.
		segs := NewSegmentsInfo()
		segs.SetSegment(1, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            1,
			CollectionID:  100,
			PartitionID:   10,
			State:         commonpb.SegmentState_Flushed,
			Level:         datapb.SegmentLevel_L1,
			Binlogs:       []*datapb.FieldBinlog{getFieldBinlogIDs(0, 10000)},
			NumOfRows:     5,
			SchemaVersion: 1,
		}})
		m := &meta{
			catalog:  &datacoord.Catalog{MetaKv: NewMetaMemoryKV()},
			segments: segs,
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BackfillCompaction,
			Schema: &schemapb.CollectionSchema{
				Version: 3,
			},
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:  1,
					InsertLogs: []*datapb.FieldBinlog{getFieldBinlogIDs(0, 10001)},
					// No manifest → V2 path
				},
			},
		}
		infos, mutation, err := m.completeBackfillCompactionMutation(task, result)
		suite.NoError(err)
		suite.NotNil(mutation)
		suite.Require().Len(infos, 1)
		suite.EqualValues(3, infos[0].GetSchemaVersion())
		// In-memory segment should also be updated.
		updated := m.segments.GetSegment(1)
		suite.EqualValues(3, updated.GetSchemaVersion())
	})

	suite.Run("v2 success - bm25 stats merged", func() {
		// Old segment already has BM25 stats for field 101.
		// Result adds BM25 stats for field 102.
		// Both should be present after mutation.
		segs := NewSegmentsInfo()
		segs.SetSegment(1, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            1,
			CollectionID:  100,
			PartitionID:   10,
			State:         commonpb.SegmentState_Flushed,
			Level:         datapb.SegmentLevel_L1,
			Binlogs:       []*datapb.FieldBinlog{getFieldBinlogIDs(0, 10000)},
			Bm25Statslogs: []*datapb.FieldBinlog{getFieldBinlogIDs(101, 50001)},
			NumOfRows:     5,
			SchemaVersion: 1,
		}})
		m := &meta{
			catalog:  &datacoord.Catalog{MetaKv: NewMetaMemoryKV()},
			segments: segs,
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BackfillCompaction,
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:  1,
					InsertLogs: []*datapb.FieldBinlog{getFieldBinlogIDs(0, 10001)},
					Bm25Logs:   []*datapb.FieldBinlog{getFieldBinlogIDs(102, 50002)},
					// No manifest → V2 path, BM25 stats should be merged
				},
			},
		}
		infos, mutation, err := m.completeBackfillCompactionMutation(task, result)
		suite.NoError(err)
		suite.NotNil(mutation)
		suite.Require().Len(infos, 1)

		// Collect all field IDs present in Bm25Statslogs of the result.
		fieldIDs := make(map[int64]bool)
		for _, fl := range infos[0].GetBm25Statslogs() {
			fieldIDs[fl.GetFieldID()] = true
		}
		suite.True(fieldIDs[101], "field 101 bm25 stats should be preserved")
		suite.True(fieldIDs[102], "field 102 bm25 stats should be added from result")
	})

	suite.Run("v2 crash-replay idempotent - no duplicate bm25 stats", func() {
		// Simulate crash-replay: datacoord applies the same backfill result twice
		// (crash between etcd write and task state transition). Without the dedup
		// filter, the second application would append duplicate logID entries to
		// Bm25Statslogs. Verify that applying the same result twice is a no-op.
		segs := NewSegmentsInfo()
		segs.SetSegment(1, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            1,
			CollectionID:  100,
			PartitionID:   10,
			State:         commonpb.SegmentState_Flushed,
			Level:         datapb.SegmentLevel_L1,
			Binlogs:       []*datapb.FieldBinlog{getFieldBinlogIDs(0, 10000)},
			NumOfRows:     5,
			SchemaVersion: 1,
		}})
		m := &meta{
			catalog:  &datacoord.Catalog{MetaKv: NewMetaMemoryKV()},
			segments: segs,
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BackfillCompaction,
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:  1,
					InsertLogs: []*datapb.FieldBinlog{getFieldBinlogIDs(0, 10001)},
					Bm25Logs:   []*datapb.FieldBinlog{getFieldBinlogIDs(102, 50002)},
				},
			},
		}

		// First application
		infos, _, err := m.completeBackfillCompactionMutation(task, result)
		suite.NoError(err)
		suite.Require().Len(infos, 1)
		suite.Require().Len(infos[0].GetBm25Statslogs(), 1)
		firstFieldBinlog := infos[0].GetBm25Statslogs()[0]
		suite.Equal(int64(102), firstFieldBinlog.GetFieldID())
		suite.Require().Len(firstFieldBinlog.GetBinlogs(), 1)
		firstLogID := firstFieldBinlog.GetBinlogs()[0].GetLogID()

		// Second application (crash-replay) — same result, must be idempotent
		infos2, _, err := m.completeBackfillCompactionMutation(task, result)
		suite.NoError(err)
		suite.Require().Len(infos2, 1)
		suite.Require().Len(infos2[0].GetBm25Statslogs(), 1,
			"replay must not add a second FieldBinlog entry")
		secondFieldBinlog := infos2[0].GetBm25Statslogs()[0]
		suite.Equal(int64(102), secondFieldBinlog.GetFieldID())
		suite.Require().Len(secondFieldBinlog.GetBinlogs(), 1,
			"replay must not append a duplicate logID entry")
		suite.Equal(firstLogID, secondFieldBinlog.GetBinlogs()[0].GetLogID())

		// Third application — still idempotent
		infos3, _, err := m.completeBackfillCompactionMutation(task, result)
		suite.NoError(err)
		suite.Require().Len(infos3[0].GetBm25Statslogs(), 1)
		suite.Require().Len(infos3[0].GetBm25Statslogs()[0].GetBinlogs(), 1)
	})

	suite.Run("v3 success - manifest and storage version updated", func() {
		// Result segment has a non-empty manifest → V3 path.
		// ManifestPath and StorageVersion should be set; Bm25Statslogs should NOT change.
		segs := NewSegmentsInfo()
		segs.SetSegment(1, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            1,
			CollectionID:  100,
			PartitionID:   10,
			State:         commonpb.SegmentState_Flushed,
			Level:         datapb.SegmentLevel_L1,
			Binlogs:       []*datapb.FieldBinlog{getFieldBinlogIDs(0, 10000)},
			Bm25Statslogs: []*datapb.FieldBinlog{getFieldBinlogIDs(101, 50001)},
			NumOfRows:     5,
			SchemaVersion: 1,
		}})
		m := &meta{
			catalog:  &datacoord.Catalog{MetaKv: NewMetaMemoryKV()},
			segments: segs,
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BackfillCompaction,
		}
		const manifestPath = "collection/100/partition/10/segment/1/v3_manifest.json"
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:      1,
					InsertLogs:     []*datapb.FieldBinlog{getFieldBinlogIDs(0, 10001)},
					Bm25Logs:       []*datapb.FieldBinlog{getFieldBinlogIDs(102, 50002)},
					Manifest:       manifestPath,
					StorageVersion: 3,
				},
			},
		}
		infos, mutation, err := m.completeBackfillCompactionMutation(task, result)
		suite.NoError(err)
		suite.NotNil(mutation)
		suite.Require().Len(infos, 1)

		// ManifestPath and StorageVersion should reflect the V3 result.
		suite.Equal(manifestPath, infos[0].GetManifestPath())
		suite.EqualValues(3, infos[0].GetStorageVersion())

		// BM25 stats should NOT be updated for V3 path — still only field 101.
		fieldIDs := make(map[int64]bool)
		for _, fl := range infos[0].GetBm25Statslogs() {
			fieldIDs[fl.GetFieldID()] = true
		}
		suite.True(fieldIDs[101], "field 101 bm25 stats should be preserved")
		suite.False(fieldIDs[102], "field 102 bm25 stats should NOT be added for V3 path")
	})
}

func TestMeta(t *testing.T) {
	suite.Run(t, new(MetaBasicSuite))
	suite.Run(t, new(MetaReloadSuite))
}

func TestMeta_Basic(t *testing.T) {
	const collID = UniqueID(0)
	const partID0 = UniqueID(100)
	const partID1 = UniqueID(101)
	const channelName = "c1"

	// mockAllocator := newMockAllocator(t)
	meta, err := newMemoryMeta(t)
	assert.NoError(t, err)

	testSchema := newTestSchema()

	collInfo := &collectionInfo{
		ID:             collID,
		Schema:         testSchema,
		Partitions:     []UniqueID{partID0, partID1},
		StartPositions: []*commonpb.KeyDataPair{},
		DatabaseName:   util.DefaultDBName,
	}
	collInfoWoPartition := &collectionInfo{
		ID:         collID,
		Schema:     testSchema,
		Partitions: []UniqueID{},
	}

	count := atomic.Int64{}
	AllocID := func() int64 {
		return count.Add(1)
	}

	t.Run("Test Segment", func(t *testing.T) {
		meta.AddCollection(collInfoWoPartition)
		// create seg0 for partition0, seg0/seg1 for partition1
		segID0_0 := AllocID()
		segInfo0_0 := buildSegment(collID, partID0, segID0_0, channelName)
		segID1_0 := AllocID()
		segInfo1_0 := buildSegment(collID, partID1, segID1_0, channelName)
		segID1_1 := AllocID()
		segInfo1_1 := buildSegment(collID, partID1, segID1_1, channelName)

		// check AddSegment
		err = meta.AddSegment(context.TODO(), segInfo0_0)
		assert.NoError(t, err)
		err = meta.AddSegment(context.TODO(), segInfo1_0)
		assert.NoError(t, err)
		err = meta.AddSegment(context.TODO(), segInfo1_1)
		assert.NoError(t, err)

		// check GetSegment
		info0_0 := meta.GetHealthySegment(context.TODO(), segID0_0)
		assert.NotNil(t, info0_0)
		assert.True(t, proto.Equal(info0_0, segInfo0_0))
		info1_0 := meta.GetHealthySegment(context.TODO(), segID1_0)
		assert.NotNil(t, info1_0)
		assert.True(t, proto.Equal(info1_0, segInfo1_0))

		// check GetSegmentsOfCollection
		segIDs := meta.GetSegmentsIDOfCollection(context.TODO(), collID)
		assert.EqualValues(t, 3, len(segIDs))
		assert.Contains(t, segIDs, segID0_0)
		assert.Contains(t, segIDs, segID1_0)
		assert.Contains(t, segIDs, segID1_1)

		// check GetSegmentsOfPartition
		segIDs = meta.GetSegmentsIDOfPartition(context.TODO(), collID, partID0)
		assert.EqualValues(t, 1, len(segIDs))
		assert.Contains(t, segIDs, segID0_0)
		segIDs = meta.GetSegmentsIDOfPartition(context.TODO(), collID, partID1)
		assert.EqualValues(t, 2, len(segIDs))
		assert.Contains(t, segIDs, segID1_0)
		assert.Contains(t, segIDs, segID1_1)

		// check DropSegment
		err = meta.DropSegment(context.TODO(), segID1_0)
		assert.NoError(t, err)
		segIDs = meta.GetSegmentsIDOfPartition(context.TODO(), collID, partID1)
		assert.EqualValues(t, 1, len(segIDs))
		assert.Contains(t, segIDs, segID1_1)

		err = meta.SetState(context.TODO(), segID0_0, commonpb.SegmentState_Sealed)
		assert.NoError(t, err)
		err = meta.SetState(context.TODO(), segID0_0, commonpb.SegmentState_Flushed)
		assert.NoError(t, err)

		info0_0 = meta.GetHealthySegment(context.TODO(), segID0_0)
		assert.NotNil(t, info0_0)
		assert.EqualValues(t, commonpb.SegmentState_Flushed, info0_0.State)
	})

	t.Run("Test segment with kv fails", func(t *testing.T) {
		// inject error for `Save`
		metakv := mockkv.NewMetaKv(t)
		metakv.EXPECT().Save(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed")).Maybe()
		metakv.EXPECT().MultiSave(mock.Anything, mock.Anything).Return(errors.New("failed")).Maybe()
		metakv.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		metakv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).Return(nil, nil, nil).Maybe()
		metakv.EXPECT().Has(mock.Anything, datacoord.FileResourceVersionKey).Return(false, nil).Maybe()
		catalog := datacoord.NewCatalog(metakv, "", "")
		broker := broker.NewMockBroker(t)
		broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
		meta, err := newMeta(context.TODO(), catalog, nil, broker)
		assert.NoError(t, err)

		err = meta.AddSegment(context.TODO(), NewSegmentInfo(&datapb.SegmentInfo{}))
		assert.Error(t, err)

		metakv2 := mockkv.NewMetaKv(t)
		metakv2.EXPECT().Save(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		metakv2.EXPECT().Has(mock.Anything, datacoord.FileResourceVersionKey).Return(false, nil).Maybe()
		metakv2.EXPECT().MultiSave(mock.Anything, mock.Anything).Return(nil).Maybe()
		metakv2.EXPECT().Remove(mock.Anything, mock.Anything).Return(errors.New("failed")).Maybe()
		metakv2.EXPECT().MultiRemove(mock.Anything, mock.Anything).Return(errors.New("failed")).Maybe()
		metakv2.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		metakv2.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).Return(nil, nil, nil).Maybe()
		metakv2.EXPECT().MultiSaveAndRemoveWithPrefix(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed"))
		catalog = datacoord.NewCatalog(metakv2, "", "")
		meta, err = newMeta(context.TODO(), catalog, nil, broker)
		assert.NoError(t, err)
		// nil, since no segment yet
		err = meta.DropSegment(context.TODO(), 0)
		assert.NoError(t, err)
		// nil, since Save error not injected
		err = meta.AddSegment(context.TODO(), NewSegmentInfo(&datapb.SegmentInfo{}))
		assert.NoError(t, err)
		// error injected
		err = meta.DropSegment(context.TODO(), 0)
		assert.Error(t, err)

		catalog = datacoord.NewCatalog(metakv, "", "")
		meta, err = newMeta(context.TODO(), catalog, nil, broker)
		assert.NoError(t, err)
		assert.NotNil(t, meta)
	})

	t.Run("Test GetCount", func(t *testing.T) {
		const rowCount0 = 100
		const rowCount1 = 300

		// no segment
		nums := meta.GetNumRowsOfCollection(context.Background(), collID)
		assert.EqualValues(t, 0, nums)

		// add seg1 with 100 rows
		segID0 := AllocID()
		segInfo0 := buildSegment(collID, partID0, segID0, channelName)
		segInfo0.NumOfRows = rowCount0
		err = meta.AddSegment(context.TODO(), segInfo0)
		assert.NoError(t, err)

		// add seg2 with 300 rows
		segID1 := AllocID()
		segInfo1 := buildSegment(collID, partID0, segID1, channelName)
		segInfo1.NumOfRows = rowCount1
		err = meta.AddSegment(context.TODO(), segInfo1)
		assert.NoError(t, err)

		// check partition/collection statistics
		nums = meta.GetNumRowsOfPartition(context.TODO(), collID, partID0)
		assert.EqualValues(t, (rowCount0 + rowCount1), nums)
		nums = meta.GetNumRowsOfCollection(context.Background(), collID)
		assert.EqualValues(t, (rowCount0 + rowCount1), nums)
	})

	t.Run("Test GetSegmentsChanPart", func(t *testing.T) {
		result := GetSegmentsChanPart(meta, collID, SegmentFilterFunc(func(segment *SegmentInfo) bool { return true }))
		assert.Equal(t, 2, len(result))
		for _, entry := range result {
			assert.Equal(t, "c1", entry.channelName)
			if entry.partitionID == UniqueID(100) {
				assert.Equal(t, 3, len(entry.segments))
			}
			if entry.partitionID == UniqueID(101) {
				assert.Equal(t, 1, len(entry.segments))
			}
		}
		result = GetSegmentsChanPart(meta, 10)
		assert.Equal(t, 0, len(result))
	})

	t.Run("GetClonedCollectionInfo", func(t *testing.T) {
		// collection does not exist
		ret := meta.GetClonedCollectionInfo(-1)
		assert.Nil(t, ret)

		collInfo.Properties = map[string]string{
			common.CollectionTTLConfigKey: "3600",
		}
		meta.AddCollection(collInfo)
		ret = meta.GetClonedCollectionInfo(collInfo.ID)
		equalCollectionInfo(t, collInfo, ret)

		collInfo.StartPositions = []*commonpb.KeyDataPair{
			{
				Key:  "k",
				Data: []byte("v"),
			},
		}
		meta.AddCollection(collInfo)
		ret = meta.GetClonedCollectionInfo(collInfo.ID)
		equalCollectionInfo(t, collInfo, ret)
	})

	t.Run("Test GetCollectionBinlogSize", func(t *testing.T) {
		const size0 = 1024
		const size1 = 2048

		// add seg0 with size0
		segID0 := AllocID()
		segInfo0 := buildSegment(collID, partID0, segID0, channelName)
		segInfo0.size.Store(size0)
		err = meta.AddSegment(context.TODO(), segInfo0)
		assert.NoError(t, err)

		// add seg1 with size1
		segID1 := AllocID()
		segInfo1 := buildSegment(collID, partID0, segID1, channelName)
		segInfo1.size.Store(size1)
		err = meta.AddSegment(context.TODO(), segInfo1)
		assert.NoError(t, err)

		// check TotalBinlogSize
		quotaInfo := meta.GetQuotaInfo()
		assert.Len(t, quotaInfo.CollectionBinlogSize, 1)
		assert.Equal(t, int64(size0+size1), quotaInfo.CollectionBinlogSize[collID])
		assert.Equal(t, int64(size0+size1), quotaInfo.TotalBinlogSize)

		meta.collections.Insert(collID, collInfo)
		quotaInfo = meta.GetQuotaInfo()
		assert.Len(t, quotaInfo.CollectionBinlogSize, 1)
		assert.Equal(t, int64(size0+size1), quotaInfo.CollectionBinlogSize[collID])
		assert.Equal(t, int64(size0+size1), quotaInfo.TotalBinlogSize)
	})

	t.Run("Test AddAllocation", func(t *testing.T) {
		meta, _ := newMemoryMeta(t)
		err := meta.AddAllocation(1, &Allocation{
			SegmentID:  1,
			NumOfRows:  1,
			ExpireTime: 0,
		})
		assert.Error(t, err)
	})
}

func TestGetUnFlushedSegments(t *testing.T) {
	meta, err := newMemoryMeta(t)
	assert.NoError(t, err)
	s1 := &datapb.SegmentInfo{
		ID:           0,
		CollectionID: 0,
		PartitionID:  0,
		State:        commonpb.SegmentState_Growing,
	}
	err = meta.AddSegment(context.TODO(), NewSegmentInfo(s1))
	assert.NoError(t, err)
	s2 := &datapb.SegmentInfo{
		ID:           1,
		CollectionID: 0,
		PartitionID:  0,
		State:        commonpb.SegmentState_Flushed,
	}
	err = meta.AddSegment(context.TODO(), NewSegmentInfo(s2))
	assert.NoError(t, err)

	segments := meta.GetUnFlushedSegments()
	assert.NoError(t, err)

	assert.EqualValues(t, 1, len(segments))
	assert.EqualValues(t, 0, segments[0].ID)
	assert.NotEqualValues(t, commonpb.SegmentState_Flushed, segments[0].State)
}

func TestAlterSegmentsWithRecovery(t *testing.T) {
	rootPath := "AlterSegmentsWithRecovery-" + funcutil.RandomString(10)
	meta, _ := newMetaWithEtcd(t, rootPath)
	etcdCli, _ := kvfactory.GetEtcdAndPath()

	collectionID := int64(1)
	partitionID := int64(1)
	segmentID := int64(1)
	fieldID := int64(1)
	segment1 := NewSegmentInfo(&datapb.SegmentInfo{
		CollectionID:  collectionID,
		PartitionID:   partitionID,
		ID:            segmentID,
		State:         commonpb.SegmentState_Growing,
		Binlogs:       []*datapb.FieldBinlog{getFieldBinlogIDs(1, 10, 333)},
		Statslogs:     []*datapb.FieldBinlog{getFieldBinlogIDs(1, 334)},
		Deltalogs:     []*datapb.FieldBinlog{getFieldBinlogIDs(1, 100)},
		Bm25Statslogs: []*datapb.FieldBinlog{getFieldBinlogIDs(1, 20)},
	})
	err := meta.AddSegment(context.TODO(), segment1)
	require.NoError(t, err)

	segmentPath := fmt.Sprintf("%s/%s/%d/%d/%d", rootPath, datacoord.SegmentPrefix, collectionID, partitionID, segmentID)
	binlogsPath := fmt.Sprintf("%s/%s/%d/%d/%d/%d", rootPath, datacoord.SegmentBinlogPathPrefix, collectionID, partitionID, segmentID, fieldID)
	statslogsPath := fmt.Sprintf("%s/%s/%d/%d/%d/%d", rootPath, datacoord.SegmentStatslogPathPrefix, collectionID, partitionID, segmentID, fieldID)
	deltalogsPath := fmt.Sprintf("%s/%s/%d/%d/%d/%d", rootPath, datacoord.SegmentDeltalogPathPrefix, collectionID, partitionID, segmentID, fieldID)
	bm25StatslogsPath := fmt.Sprintf("%s/%s/%d/%d/%d/%d", rootPath, datacoord.SegmentBM25logPathPrefix, collectionID, partitionID, segmentID, fieldID)

	checkVersion := func(sVersion int64, bVersion int64, stVersion int64, dVersion int64, bm25Version int64) {
		kv, err := etcdCli.Get(context.TODO(), segmentPath)
		require.NoError(t, err)
		require.Equal(t, kv.Kvs[0].Version, sVersion)
		kv, err = etcdCli.Get(context.TODO(), binlogsPath)
		require.NoError(t, err)
		require.Equal(t, kv.Kvs[0].Version, bVersion)
		kv, err = etcdCli.Get(context.TODO(), statslogsPath)
		require.NoError(t, err)
		require.Equal(t, kv.Kvs[0].Version, stVersion)
		kv, err = etcdCli.Get(context.TODO(), deltalogsPath)
		require.NoError(t, err)
		require.Equal(t, kv.Kvs[0].Version, dVersion)
		kv, err = etcdCli.Get(context.TODO(), bm25StatslogsPath)
		require.NoError(t, err)
		require.Equal(t, kv.Kvs[0].Version, bm25Version)
	}
	checkVersion(1, 1, 1, 1, 1)

	err = meta.UpdateSegmentsInfo(context.TODO(), AddBinlogsOperator(1,
		[]*datapb.FieldBinlog{getFieldBinlogIDs(1, 10, 333)},
		nil,
		nil,
		nil,
	))
	require.NoError(t, err)
	checkVersion(2, 2, 1, 1, 1)

	err = meta.UpdateSegmentsInfo(context.TODO(), AddBinlogsOperator(1,
		nil,
		[]*datapb.FieldBinlog{getFieldBinlogIDs(1, 10, 333)},
		nil,
		nil,
	))
	require.NoError(t, err)
	checkVersion(3, 2, 2, 1, 1)

	err = meta.UpdateSegmentsInfo(context.TODO(), AddBinlogsOperator(1,
		nil,
		nil,
		[]*datapb.FieldBinlog{getFieldBinlogIDs(1, 10, 333)},
		nil,
	))
	require.NoError(t, err)
	checkVersion(4, 2, 2, 2, 1)

	err = meta.UpdateSegmentsInfo(context.TODO(), AddBinlogsOperator(1,
		nil,
		nil,
		nil,
		[]*datapb.FieldBinlog{getFieldBinlogIDs(1, 10, 333)},
	))
	require.NoError(t, err)
	checkVersion(5, 2, 2, 2, 2)

	err = meta.UpdateSegmentsInfo(context.TODO(), AddBinlogsOperator(1,
		[]*datapb.FieldBinlog{getFieldBinlogIDs(1, 10, 333)},
		[]*datapb.FieldBinlog{getFieldBinlogIDs(1, 10, 333)},
		[]*datapb.FieldBinlog{getFieldBinlogIDs(1, 10, 333)},
		[]*datapb.FieldBinlog{getFieldBinlogIDs(1, 10, 333)},
	))
	require.NoError(t, err)
	checkVersion(6, 3, 3, 3, 3)
}

func TestUpdateSegmentsInfo(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)

		segment1 := NewSegmentInfo(&datapb.SegmentInfo{
			ID: 1, State: commonpb.SegmentState_Growing,
			Binlogs:   []*datapb.FieldBinlog{getFieldBinlogIDsWithEntry(1, 1, 222)},
			Statslogs: []*datapb.FieldBinlog{getFieldBinlogIDs(1, 2)},
		})
		err = meta.AddSegment(context.TODO(), segment1)
		assert.NoError(t, err)
		require.EqualValues(t, -1, segment1.deltaRowcount.Load())
		assert.EqualValues(t, 0, segment1.getDeltaCount())

		err = meta.UpdateSegmentsInfo(
			context.TODO(),
			UpdateStatusOperator(1, commonpb.SegmentState_Growing),
			AddBinlogsOperator(1,
				[]*datapb.FieldBinlog{getFieldBinlogIDsWithEntry(1, 10, 333)},
				[]*datapb.FieldBinlog{getFieldBinlogIDs(1, 334)},
				[]*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{EntriesNum: 1, TimestampFrom: 100, TimestampTo: 200, LogSize: 1000, LogID: 335}}}},
				[]*datapb.FieldBinlog{},
			),
			UpdateStartPosition([]*datapb.SegmentStartPosition{{SegmentID: 1, StartPosition: &msgpb.MsgPosition{MsgID: []byte{1, 2, 3}}}}),
			UpdateCheckPointOperator(1, []*datapb.CheckPoint{{SegmentID: 1, NumOfRows: 10}}),
		)
		assert.NoError(t, err)

		updated := meta.GetHealthySegment(context.TODO(), 1)
		assert.EqualValues(t, -1, updated.deltaRowcount.Load())
		assert.EqualValues(t, 1, updated.getDeltaCount())

		expected := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 1, State: commonpb.SegmentState_Growing, NumOfRows: 11,
			StartPosition: &msgpb.MsgPosition{MsgID: []byte{1, 2, 3}},
			Binlogs:       []*datapb.FieldBinlog{getFieldBinlogIDs(1, 222, 333)},
			Statslogs:     []*datapb.FieldBinlog{getFieldBinlogIDs(1, 0, 1)},
			Deltalogs:     []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{EntriesNum: 1, TimestampFrom: 100, TimestampTo: 200, LogSize: 1000}}}},
		}}

		assert.Equal(t, updated.StartPosition, expected.StartPosition)
		assert.Equal(t, updated.DmlPosition, expected.DmlPosition)
		assert.Equal(t, updated.DmlPosition, expected.DmlPosition)
		assert.Equal(t, len(updated.Binlogs[0].Binlogs), len(expected.Binlogs[0].Binlogs))
		assert.Equal(t, len(updated.Statslogs[0].Binlogs), len(expected.Statslogs[0].Binlogs))
		assert.Equal(t, len(updated.Deltalogs[0].Binlogs), len(expected.Deltalogs[0].Binlogs))
		assert.Equal(t, updated.State, expected.State)
		assert.Equal(t, updated.size.Load(), expected.size.Load())
		assert.Equal(t, updated.NumOfRows, expected.NumOfRows)
	})

	t.Run("update binlogs from save binlog paths", func(t *testing.T) {
		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)

		segment1 := NewSegmentInfo(&datapb.SegmentInfo{
			ID: 1, State: commonpb.SegmentState_Growing,
			Binlogs:   []*datapb.FieldBinlog{},
			Statslogs: []*datapb.FieldBinlog{},
		})
		err = meta.AddSegment(context.TODO(), segment1)
		assert.NoError(t, err)
		require.EqualValues(t, -1, segment1.deltaRowcount.Load())
		assert.EqualValues(t, 0, segment1.getDeltaCount())

		err = meta.UpdateSegmentsInfo(
			context.TODO(),
			UpdateStatusOperator(1, commonpb.SegmentState_Growing),
			UpdateBinlogsFromSaveBinlogPathsOperator(1,
				[]*datapb.FieldBinlog{getFieldBinlogIDsWithEntry(1, 10, 333)},
				[]*datapb.FieldBinlog{getFieldBinlogIDs(1, 334)},
				[]*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{EntriesNum: 1, TimestampFrom: 100, TimestampTo: 200, LogSize: 1000, LogID: 335}}}},
				[]*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{EntriesNum: 1, TimestampFrom: 100, TimestampTo: 200, LogSize: 1000, LogID: 335}}}},
			),
			UpdateStartPosition([]*datapb.SegmentStartPosition{{SegmentID: 1, StartPosition: &msgpb.MsgPosition{MsgID: []byte{1, 2, 3}}}}),
			UpdateCheckPointOperator(1, []*datapb.CheckPoint{{SegmentID: 1, NumOfRows: 10, Position: &msgpb.MsgPosition{MsgID: []byte{1, 2, 3}, Timestamp: 100}}}, true),
			UpdateManifest(1, "files/binlogs/1/2/1000/manifest_0"),
		)
		assert.NoError(t, err)

		updated := meta.GetHealthySegment(context.TODO(), 1)
		assert.EqualValues(t, -1, updated.deltaRowcount.Load())
		assert.EqualValues(t, 1, updated.getDeltaCount())

		assert.Equal(t, updated.StartPosition, &msgpb.MsgPosition{MsgID: []byte{1, 2, 3}})
		assert.Equal(t, updated.DmlPosition, &msgpb.MsgPosition{MsgID: []byte{1, 2, 3}, Timestamp: 100})
		assert.Equal(t, len(updated.Binlogs[0].Binlogs), 1)
		assert.Equal(t, len(updated.Statslogs[0].Binlogs), 1)
		assert.Equal(t, len(updated.Deltalogs[0].Binlogs), 1)
		assert.Equal(t, len(updated.Bm25Statslogs[0].Binlogs), 1)
		assert.Equal(t, updated.State, commonpb.SegmentState_Growing)
		assert.Equal(t, updated.NumOfRows, int64(10))
		assert.Equal(t, updated.ManifestPath, "files/binlogs/1/2/1000/manifest_0")

		err = meta.UpdateSegmentsInfo(
			context.TODO(),
			UpdateStatusOperator(1, commonpb.SegmentState_Growing),
			UpdateBinlogsFromSaveBinlogPathsOperator(1, nil, nil, nil, nil),
			UpdateStartPosition([]*datapb.SegmentStartPosition{{SegmentID: 1, StartPosition: &msgpb.MsgPosition{MsgID: []byte{1, 2, 3}}}}),
			UpdateCheckPointOperator(1, []*datapb.CheckPoint{{SegmentID: 1, NumOfRows: 10, Position: &msgpb.MsgPosition{MsgID: []byte{1, 2, 3}, Timestamp: 99}}}, true),
		)
		assert.True(t, errors.Is(err, ErrIgnoredSegmentMetaOperation))

		err = meta.UpdateSegmentsInfo(
			context.TODO(),
			UpdateStatusOperator(1, commonpb.SegmentState_Growing),
			UpdateBinlogsFromSaveBinlogPathsOperator(1,
				[]*datapb.FieldBinlog{getFieldBinlogIDsWithEntry(1, 10, 335, 337)},
				[]*datapb.FieldBinlog{getFieldBinlogIDs(1, 336)},
				[]*datapb.FieldBinlog{},
				[]*datapb.FieldBinlog{},
			),
			UpdateStatusOperator(1, commonpb.SegmentState_Flushed),
			UpdateStartPosition([]*datapb.SegmentStartPosition{{SegmentID: 1, StartPosition: &msgpb.MsgPosition{MsgID: []byte{1, 2, 3}}}}),
			UpdateCheckPointOperator(1, []*datapb.CheckPoint{{SegmentID: 1, NumOfRows: 12, Position: &msgpb.MsgPosition{MsgID: []byte{1, 2, 3}, Timestamp: 101}}}, true),
			UpdateManifest(1, "files/binlogs/1/2/1000/manifest_2"),
		)
		assert.NoError(t, err)

		updated = meta.GetHealthySegment(context.TODO(), 1)
		assert.Equal(t, updated.NumOfRows, int64(20))
		assert.Equal(t, updated.DmlPosition, &msgpb.MsgPosition{MsgID: []byte{1, 2, 3}, Timestamp: 101})
		assert.Equal(t, len(updated.Binlogs[0].Binlogs), 2)
		assert.Equal(t, len(updated.Statslogs[0].Binlogs), 1)
		assert.Equal(t, len(updated.Deltalogs), 0)
		assert.Equal(t, len(updated.Bm25Statslogs), 0)
		assert.Equal(t, updated.State, commonpb.SegmentState_Flushed)
		assert.Equal(t, updated.ManifestPath, "files/binlogs/1/2/1000/manifest_2")

		err = meta.UpdateSegmentsInfo(
			context.TODO(),
			UpdateStatusOperator(1, commonpb.SegmentState_Flushed),
			UpdateBinlogsFromSaveBinlogPathsOperator(1,
				[]*datapb.FieldBinlog{},
				[]*datapb.FieldBinlog{},
				[]*datapb.FieldBinlog{},
				[]*datapb.FieldBinlog{}),
			UpdateCheckPointOperator(1, []*datapb.CheckPoint{{SegmentID: 1, NumOfRows: 12, Position: &msgpb.MsgPosition{MsgID: []byte{1, 2, 3}, Timestamp: 101}}}, true),
		)
		assert.True(t, errors.Is(err, ErrIgnoredSegmentMetaOperation))

		updated = meta.GetHealthySegment(context.TODO(), 1)
		assert.Equal(t, updated.NumOfRows, int64(20))
		assert.Equal(t, updated.DmlPosition, &msgpb.MsgPosition{MsgID: []byte{1, 2, 3}, Timestamp: 101})
		assert.Equal(t, len(updated.Binlogs[0].Binlogs), 2)
		assert.Equal(t, len(updated.Statslogs[0].Binlogs), 1)
		assert.Equal(t, len(updated.Deltalogs), 0)
		assert.Equal(t, len(updated.Bm25Statslogs), 0)
		assert.Equal(t, updated.State, commonpb.SegmentState_Flushed)

		err = meta.UpdateSegmentsInfo(
			context.TODO(),
			UpdateStatusOperator(1, commonpb.SegmentState_Dropped),
			UpdateBinlogsFromSaveBinlogPathsOperator(1,
				[]*datapb.FieldBinlog{},
				[]*datapb.FieldBinlog{},
				[]*datapb.FieldBinlog{},
				[]*datapb.FieldBinlog{}),
			UpdateCheckPointOperator(1, []*datapb.CheckPoint{{SegmentID: 1, NumOfRows: 12, Position: &msgpb.MsgPosition{MsgID: []byte{1, 2, 3}, Timestamp: 101}}}, true),
		)
		assert.NoError(t, err)

		updated = meta.GetSegment(context.TODO(), 1)
		assert.Equal(t, updated.State, commonpb.SegmentState_Dropped)
	})

	t.Run("v3 storage segment with empty binlogs uses checkpoint NumOfRows", func(t *testing.T) {
		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)

		// Create a V3 segment with no binlogs (V3 storage uses ManifestPath instead)
		segment1 := NewSegmentInfo(&datapb.SegmentInfo{
			ID:           1,
			State:        commonpb.SegmentState_Growing,
			Binlogs:      []*datapb.FieldBinlog{},
			Statslogs:    []*datapb.FieldBinlog{},
			ManifestPath: "files/binlogs/1/2/1000/manifest_0",
		})
		err = meta.AddSegment(context.TODO(), segment1)
		assert.NoError(t, err)
		assert.EqualValues(t, 0, segment1.NumOfRows)

		// UpdateCheckPointOperator with cpNumRows=100, segment has no binlogs
		// CalcRowCountFromBinLog will return 0, so should fall back to cpNumRows
		err = meta.UpdateSegmentsInfo(
			context.TODO(),
			UpdateCheckPointOperator(1, []*datapb.CheckPoint{{
				SegmentID: 1,
				NumOfRows: 100,
				Position:  &msgpb.MsgPosition{MsgID: []byte{1, 2, 3}, Timestamp: 100},
			}}, true),
		)
		assert.NoError(t, err)

		updated := meta.GetHealthySegment(context.TODO(), 1)
		// NumOfRows should be set from checkpoint, not left at 0
		assert.EqualValues(t, 100, updated.NumOfRows)
	})

	t.Run("update compacted segment", func(t *testing.T) {
		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)

		// segment not found
		err = meta.UpdateSegmentsInfo(
			context.TODO(),
			UpdateCompactedOperator(1),
		)
		assert.NoError(t, err)

		// normal
		segment1 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 1, State: commonpb.SegmentState_Flushed,
			Binlogs:   []*datapb.FieldBinlog{getFieldBinlogIDs(1, 2)},
			Statslogs: []*datapb.FieldBinlog{getFieldBinlogIDs(1, 2)},
		}}
		err = meta.AddSegment(context.TODO(), segment1)
		assert.NoError(t, err)

		err = meta.UpdateSegmentsInfo(
			context.TODO(),
			UpdateCompactedOperator(1),
		)
		assert.NoError(t, err)
	})
	t.Run("update non-existed segment", func(t *testing.T) {
		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)

		err = meta.UpdateSegmentsInfo(
			context.TODO(),
			UpdateStatusOperator(1, commonpb.SegmentState_Flushing),
		)
		assert.NoError(t, err)

		err = meta.UpdateSegmentsInfo(
			context.TODO(),
			AddBinlogsOperator(1, nil, nil, nil, nil),
		)
		assert.NoError(t, err)

		err = meta.UpdateSegmentsInfo(
			context.TODO(),
			UpdateStartPosition([]*datapb.SegmentStartPosition{{SegmentID: 1, StartPosition: &msgpb.MsgPosition{MsgID: []byte{1, 2, 3}}}}),
		)
		assert.NoError(t, err)

		err = meta.UpdateSegmentsInfo(
			context.TODO(),
			UpdateCheckPointOperator(1, []*datapb.CheckPoint{{SegmentID: 1, NumOfRows: 10}}),
		)
		assert.NoError(t, err)

		err = meta.UpdateSegmentsInfo(
			context.TODO(),
			UpdateBinlogsOperator(1, nil, nil, nil, nil),
		)
		assert.NoError(t, err)

		err = meta.UpdateSegmentsInfo(
			context.TODO(),
			UpdateDmlPosition(1, nil),
		)
		assert.NoError(t, err)

		err = meta.UpdateSegmentsInfo(
			context.TODO(),
			UpdateDmlPosition(1, &msgpb.MsgPosition{MsgID: []byte{1}}),
		)
		assert.NoError(t, err)

		err = meta.UpdateSegmentsInfo(
			context.TODO(),
			UpdateImportedRows(1, 0),
		)
		assert.NoError(t, err)

		err = meta.UpdateSegmentsInfo(
			context.TODO(),
			UpdateIsImporting(1, true),
		)
		assert.NoError(t, err)

		err = meta.UpdateSegmentsInfo(
			context.TODO(),
			UpdateManifest(1, "files/binlogs/1/2/1000/manifest_0"),
		)
		assert.NoError(t, err)

		err = meta.UpdateSegmentsInfo(context.TODO(), UpdateAsDroppedIfEmptyWhenFlushing(1))
		assert.NoError(t, err)
	})

	t.Run("update empty segment into flush", func(t *testing.T) {
		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)
		meta.AddSegment(context.Background(), &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 1, State: commonpb.SegmentState_Growing}})
		err = meta.UpdateSegmentsInfo(
			context.TODO(),
			UpdateStatusOperator(1, commonpb.SegmentState_Flushing),
			UpdateAsDroppedIfEmptyWhenFlushing(1),
		)
		assert.NoError(t, err)
	})

	t.Run("update checkpoints and start position of non existed segment", func(t *testing.T) {
		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)

		segment1 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 1, State: commonpb.SegmentState_Growing}}
		err = meta.AddSegment(context.TODO(), segment1)
		assert.NoError(t, err)

		err = meta.UpdateSegmentsInfo(
			context.TODO(),
			UpdateCheckPointOperator(1, []*datapb.CheckPoint{{SegmentID: 2, NumOfRows: 10}}),
		)

		assert.NoError(t, err)
		assert.Nil(t, meta.GetHealthySegment(context.TODO(), 2))
	})

	t.Run("test save etcd failed", func(t *testing.T) {
		metakv := mockkv.NewMetaKv(t)
		metakv.EXPECT().Save(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("mocked fail")).Maybe()
		metakv.EXPECT().MultiSave(mock.Anything, mock.Anything).Return(errors.New("mocked fail")).Maybe()
		metakv.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		metakv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).Return(nil, nil, nil).Maybe()
		metakv.EXPECT().Has(mock.Anything, mock.Anything).Return(false, nil).Maybe()
		catalog := datacoord.NewCatalog(metakv, "", "")
		broker := broker.NewMockBroker(t)
		broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
		meta, err := newMeta(context.TODO(), catalog, nil, broker)
		assert.NoError(t, err)

		segmentInfo := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:        1,
				NumOfRows: 0,
				State:     commonpb.SegmentState_Growing,
			},
		}
		meta.segments.SetSegment(1, segmentInfo)

		err = meta.UpdateSegmentsInfo(
			context.TODO(),
			UpdateStatusOperator(1, commonpb.SegmentState_Flushing),
			AddBinlogsOperator(1,
				[]*datapb.FieldBinlog{getFieldBinlogIDs(1, 2)},
				[]*datapb.FieldBinlog{getFieldBinlogIDs(1, 2)},
				[]*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{EntriesNum: 1, TimestampFrom: 100, TimestampTo: 200, LogSize: 1000, LogPath: "", LogID: 2}}}},
				[]*datapb.FieldBinlog{},
			),
			UpdateStartPosition([]*datapb.SegmentStartPosition{{SegmentID: 1, StartPosition: &msgpb.MsgPosition{MsgID: []byte{1, 2, 3}}}}),
			UpdateCheckPointOperator(1, []*datapb.CheckPoint{{SegmentID: 1, NumOfRows: 10}}),
		)

		assert.Error(t, err)
		assert.Equal(t, "mocked fail", err.Error())
		segmentInfo = meta.GetHealthySegment(context.TODO(), 1)
		assert.EqualValues(t, 0, segmentInfo.NumOfRows)
		assert.Equal(t, commonpb.SegmentState_Growing, segmentInfo.State)
		assert.Nil(t, segmentInfo.Binlogs)
		assert.Nil(t, segmentInfo.StartPosition)
	})
}

func TestUpdateManifestVersion(t *testing.T) {
	t.Run("segment not found", func(t *testing.T) {
		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)

		operator := UpdateManifestVersion(999, 10)
		pack := &updateSegmentPack{
			meta:     meta,
			segments: make(map[int64]*SegmentInfo),
		}
		assert.False(t, operator(pack))
	})

	t.Run("empty manifest path", func(t *testing.T) {
		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)

		meta.AddSegment(context.Background(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           1,
				State:        commonpb.SegmentState_Flushed,
				ManifestPath: "",
			},
		})

		operator := UpdateManifestVersion(1, 10)
		pack := &updateSegmentPack{
			meta:     meta,
			segments: make(map[int64]*SegmentInfo),
		}
		assert.False(t, operator(pack))
	})

	t.Run("invalid manifest path - unmarshal error", func(t *testing.T) {
		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)

		meta.AddSegment(context.Background(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           1,
				State:        commonpb.SegmentState_Flushed,
				ManifestPath: "not-json",
			},
		})

		operator := UpdateManifestVersion(1, 10)
		pack := &updateSegmentPack{
			meta:     meta,
			segments: make(map[int64]*SegmentInfo),
		}
		assert.False(t, operator(pack))
	})

	t.Run("same version - no update", func(t *testing.T) {
		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)

		manifestPath := packed.MarshalManifestPath("/data/segments/1", 10)
		meta.AddSegment(context.Background(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           1,
				State:        commonpb.SegmentState_Flushed,
				ManifestPath: manifestPath,
			},
		})

		operator := UpdateManifestVersion(1, 10)
		pack := &updateSegmentPack{
			meta:     meta,
			segments: make(map[int64]*SegmentInfo),
		}
		assert.False(t, operator(pack))
	})

	t.Run("success - version updated", func(t *testing.T) {
		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)

		manifestPath := packed.MarshalManifestPath("/data/segments/1", 5)
		meta.AddSegment(context.Background(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           1,
				State:        commonpb.SegmentState_Flushed,
				ManifestPath: manifestPath,
			},
		})

		operator := UpdateManifestVersion(1, 10)
		pack := &updateSegmentPack{
			meta:     meta,
			segments: make(map[int64]*SegmentInfo),
		}
		assert.True(t, operator(pack))

		// Verify the manifest path was updated
		seg := pack.Get(1)
		assert.NotNil(t, seg)
		basePath, version, err := packed.UnmarshalManifestPath(seg.ManifestPath)
		assert.NoError(t, err)
		assert.Equal(t, "/data/segments/1", basePath)
		assert.Equal(t, int64(10), version)
	})

	t.Run("success - via UpdateSegmentsInfo", func(t *testing.T) {
		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)

		manifestPath := packed.MarshalManifestPath("/data/segments/1", 1)
		meta.AddSegment(context.Background(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           1,
				State:        commonpb.SegmentState_Flushed,
				ManifestPath: manifestPath,
			},
		})

		err = meta.UpdateSegmentsInfo(
			context.TODO(),
			UpdateManifestVersion(1, 5),
		)
		assert.NoError(t, err)

		updated := meta.GetHealthySegment(context.TODO(), 1)
		assert.NotNil(t, updated)
		basePath, version, err := packed.UnmarshalManifestPath(updated.ManifestPath)
		assert.NoError(t, err)
		assert.Equal(t, "/data/segments/1", basePath)
		assert.Equal(t, int64(5), version)
	})

	t.Run("rollback rejected - currentVer > incomingVer is a no-op", func(t *testing.T) {
		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)

		// Current version = 10. A stale broadcast carrying version = 5 must
		// not regress the pointer. classifyBackfillSegments pre-checks
		// monotonicity at broadcast time, but concurrent compaction may have
		// advanced ManifestPath between pre-check and this apply -- the
		// operator-level guard is the last line of defense.
		manifestPath := packed.MarshalManifestPath("/data/segments/1", 10)
		meta.AddSegment(context.Background(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           1,
				State:        commonpb.SegmentState_Flushed,
				ManifestPath: manifestPath,
			},
		})

		operator := UpdateManifestVersion(1, 5)
		pack := &updateSegmentPack{
			meta:     meta,
			segments: make(map[int64]*SegmentInfo),
		}
		assert.False(t, operator(pack))

		// Confirm the stored manifest path was not mutated in the pack.
		got := pack.Get(1)
		_, currentVer, err := packed.UnmarshalManifestPath(got.ManifestPath)
		assert.NoError(t, err)
		assert.Equal(t, int64(10), currentVer)
	})
}

func TestUpdateSegmentColumnGroupsOperator(t *testing.T) {
	// Helper: build a segment with two pre-existing column groups, where the
	// first group owns top-level fieldID=100 and child_fields=[200,201], and
	// the second group owns top-level fieldID=300.
	newSegmentWithExistingGroups := func() *SegmentInfo {
		return &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:             1,
				CollectionID:   1000,
				State:          commonpb.SegmentState_Flushed,
				StorageVersion: storage.StorageV2,
				DataVersion:    int32(5),
				Binlogs: []*datapb.FieldBinlog{
					{
						FieldID:     100,
						ChildFields: []int64{200, 201},
						Binlogs:     []*datapb.Binlog{{LogID: 1}},
					},
					{
						FieldID: 300,
						Binlogs: []*datapb.Binlog{{LogID: 2}},
					},
				},
			},
		}
	}

	t.Run("segment not found returns false", func(t *testing.T) {
		m, err := newMemoryMeta(t)
		assert.NoError(t, err)
		op := UpdateSegmentColumnGroupsOperator(999, map[int64]*datapb.FieldBinlog{
			400: {FieldID: 400},
		})
		pack := &updateSegmentPack{
			meta:       m,
			segments:   make(map[int64]*SegmentInfo),
			increments: make(map[int64]metastore.BinlogsIncrement),
		}
		assert.False(t, op(pack))
	})

	t.Run("append new group bumps DataVersion", func(t *testing.T) {
		m, err := newMemoryMeta(t)
		assert.NoError(t, err)
		seg := newSegmentWithExistingGroups()
		m.AddSegment(context.Background(), seg)

		op := UpdateSegmentColumnGroupsOperator(1, map[int64]*datapb.FieldBinlog{
			400: {FieldID: 400, Binlogs: []*datapb.Binlog{{LogID: 10, EntriesNum: 100}}},
		})
		pack := &updateSegmentPack{
			meta:       m,
			segments:   make(map[int64]*SegmentInfo),
			increments: make(map[int64]metastore.BinlogsIncrement),
		}
		assert.True(t, op(pack))

		got := pack.Get(1)
		assert.NotNil(t, got)
		assert.Equal(t, int32(6), got.DataVersion)
		// Two pre-existing + one new.
		assert.Len(t, got.Binlogs, 3)
		var fids []int64
		for _, fb := range got.Binlogs {
			fids = append(fids, fb.GetFieldID())
		}
		assert.ElementsMatch(t, []int64{100, 300, 400}, fids)
		// child_fields on 100 untouched because no child collision.
		for _, fb := range got.Binlogs {
			if fb.GetFieldID() == 100 {
				assert.ElementsMatch(t, []int64{200, 201}, fb.GetChildFields())
			}
		}
		_, ok := pack.increments[1]
		assert.True(t, ok)
	})

	t.Run("strips child fields from existing group", func(t *testing.T) {
		m, err := newMemoryMeta(t)
		assert.NoError(t, err)
		seg := newSegmentWithExistingGroups()
		m.AddSegment(context.Background(), seg)

		// new group 500 owns child 200 which was held by group 100.
		op := UpdateSegmentColumnGroupsOperator(1, map[int64]*datapb.FieldBinlog{
			500: {FieldID: 500, ChildFields: []int64{200}},
		})
		pack := &updateSegmentPack{
			meta:       m,
			segments:   make(map[int64]*SegmentInfo),
			increments: make(map[int64]metastore.BinlogsIncrement),
		}
		assert.True(t, op(pack))

		got := pack.Get(1)
		for _, fb := range got.Binlogs {
			if fb.GetFieldID() == 100 {
				assert.ElementsMatch(t, []int64{201}, fb.GetChildFields(),
					"child 200 should have been stripped from old group")
			}
		}
	})

	t.Run("replace same fieldID in place", func(t *testing.T) {
		m, err := newMemoryMeta(t)
		assert.NoError(t, err)
		seg := newSegmentWithExistingGroups()
		m.AddSegment(context.Background(), seg)

		op := UpdateSegmentColumnGroupsOperator(1, map[int64]*datapb.FieldBinlog{
			100: {FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 999, EntriesNum: 7}}},
		})
		pack := &updateSegmentPack{
			meta:       m,
			segments:   make(map[int64]*SegmentInfo),
			increments: make(map[int64]metastore.BinlogsIncrement),
		}
		assert.True(t, op(pack))

		got := pack.Get(1)
		// 100 replaced, 300 preserved => still 2 groups.
		assert.Len(t, got.Binlogs, 2)
		for _, fb := range got.Binlogs {
			if fb.GetFieldID() == 100 {
				assert.Len(t, fb.GetBinlogs(), 1)
				assert.Equal(t, int64(999), fb.GetBinlogs()[0].GetLogID())
			}
		}
	})

	t.Run("drops empty-children existing group and records DroppedBinlogFieldIDs", func(t *testing.T) {
		// Pre-existing single-child group (fieldID=100 owns child 200) whose
		// only child is claimed by a new backfill group (fieldID=200). After
		// stripping, group 100's ChildFields is empty -- the operator must
		// drop it from segment.Binlogs AND record 100 in DroppedBinlogFieldIDs
		// so the catalog removes the orphan etcd KV (without it, listBinlogs'
		// prefix scan would resurrect the zombie on restart).
		m, err := newMemoryMeta(t)
		assert.NoError(t, err)
		m.AddSegment(context.Background(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:             1,
				CollectionID:   1000,
				State:          commonpb.SegmentState_Flushed,
				StorageVersion: storage.StorageV2,
				Binlogs: []*datapb.FieldBinlog{
					{
						FieldID:     100,
						ChildFields: []int64{200}, // single child
						Binlogs:     []*datapb.Binlog{{LogID: 1}},
					},
					{
						FieldID:     300,
						ChildFields: []int64{301, 302},
						Binlogs:     []*datapb.Binlog{{LogID: 2}},
					},
				},
			},
		})

		op := UpdateSegmentColumnGroupsOperator(1, map[int64]*datapb.FieldBinlog{
			200: {FieldID: 200, ChildFields: []int64{200}, Binlogs: []*datapb.Binlog{{LogID: 99}}},
		})
		pack := &updateSegmentPack{
			meta:       m,
			segments:   make(map[int64]*SegmentInfo),
			increments: make(map[int64]metastore.BinlogsIncrement),
		}
		assert.True(t, op(pack))

		got := pack.Get(1)
		// Group 100 must be gone from in-memory binlogs; 300 (unaffected) plus
		// new 200 remain.
		assert.Len(t, got.Binlogs, 2)
		fids := lo.Map(got.Binlogs, func(fb *datapb.FieldBinlog, _ int) int64 { return fb.GetFieldID() })
		assert.ElementsMatch(t, []int64{200, 300}, fids)

		// Increment carries the orphan FieldID so AlterSegments can remove
		// the persisted KV.
		inc, ok := pack.increments[1]
		assert.True(t, ok)
		assert.ElementsMatch(t, []int64{100}, inc.DroppedBinlogFieldIDs)
	})

	t.Run("DataVersion monotonic across reruns", func(t *testing.T) {
		m, err := newMemoryMeta(t)
		assert.NoError(t, err)
		seg := newSegmentWithExistingGroups()
		m.AddSegment(context.Background(), seg)

		err = m.UpdateSegmentsInfo(context.Background(),
			UpdateSegmentColumnGroupsOperator(1, map[int64]*datapb.FieldBinlog{
				400: {FieldID: 400, Binlogs: []*datapb.Binlog{{LogID: 10}}},
			}),
		)
		assert.NoError(t, err)
		err = m.UpdateSegmentsInfo(context.Background(),
			UpdateSegmentColumnGroupsOperator(1, map[int64]*datapb.FieldBinlog{
				400: {FieldID: 400, Binlogs: []*datapb.Binlog{{LogID: 11}}},
			}),
		)
		assert.NoError(t, err)

		got := m.GetHealthySegment(context.Background(), 1)
		assert.NotNil(t, got)
		// Started at 5, two bumps => 7.
		assert.Equal(t, int32(7), got.DataVersion)
	})
}

func Test_meta_SetSegmentsCompacting(t *testing.T) {
	type fields struct {
		client   kv.MetaKv
		segments *SegmentsInfo
	}
	type args struct {
		segmentID  UniqueID
		compacting bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			"test set segment compacting",
			fields{
				NewMetaMemoryKV(),
				func() *SegmentsInfo {
					s := NewSegmentsInfo()
					s.SetSegment(1, &SegmentInfo{
						SegmentInfo: &datapb.SegmentInfo{
							ID:    1,
							State: commonpb.SegmentState_Flushed,
						},
						isCompacting: false,
					})
					return s
				}(),
			},
			args{
				segmentID:  1,
				compacting: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &meta{
				catalog:  &datacoord.Catalog{MetaKv: tt.fields.client},
				segments: tt.fields.segments,
			}
			m.SetSegmentsCompacting(context.TODO(), []UniqueID{tt.args.segmentID}, tt.args.compacting)
			segment := m.GetHealthySegment(context.TODO(), tt.args.segmentID)
			assert.Equal(t, tt.args.compacting, segment.isCompacting)
		})
	}
}

func Test_meta_GetSegmentsOfCollection(t *testing.T) {
	storedSegments := NewSegmentsInfo()

	for segID, segment := range map[int64]*SegmentInfo{
		1: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:           1,
				CollectionID: 1,
				State:        commonpb.SegmentState_Flushed,
			},
		},
		2: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:           2,
				CollectionID: 1,
				State:        commonpb.SegmentState_Growing,
			},
		},
		3: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:           3,
				CollectionID: 2,
				State:        commonpb.SegmentState_Flushed,
			},
		},
	} {
		storedSegments.SetSegment(segID, segment)
	}
	expectedSeg := map[int64]commonpb.SegmentState{1: commonpb.SegmentState_Flushed, 2: commonpb.SegmentState_Growing}
	m := &meta{segments: storedSegments}
	got := m.GetSegmentsOfCollection(context.TODO(), 1)
	assert.Equal(t, len(expectedSeg), len(got))
	for _, gotInfo := range got {
		expected, ok := expectedSeg[gotInfo.ID]
		assert.True(t, ok)
		assert.Equal(t, expected, gotInfo.GetState())
	}

	got = m.GetSegmentsOfCollection(context.TODO(), -1)
	assert.Equal(t, 3, len(got))

	got = m.GetSegmentsOfCollection(context.TODO(), 10)
	assert.Equal(t, 0, len(got))
}

func Test_meta_GetSegmentsWithChannel(t *testing.T) {
	storedSegments := NewSegmentsInfo()
	for segID, segment := range map[int64]*SegmentInfo{
		1: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            1,
				CollectionID:  1,
				InsertChannel: "h1",
				State:         commonpb.SegmentState_Flushed,
			},
		},
		2: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            2,
				CollectionID:  1,
				InsertChannel: "h2",
				State:         commonpb.SegmentState_Growing,
			},
		},
		3: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            3,
				CollectionID:  2,
				State:         commonpb.SegmentState_Flushed,
				InsertChannel: "h1",
			},
		},
	} {
		storedSegments.SetSegment(segID, segment)
	}
	m := &meta{segments: storedSegments}
	got := m.GetSegmentsByChannel("h1")
	assert.Equal(t, 2, len(got))
	assert.ElementsMatch(t, []int64{1, 3}, lo.Map(
		got,
		func(s *SegmentInfo, i int) int64 {
			return s.ID
		},
	))

	got = m.GetSegmentsByChannel("h3")
	assert.Equal(t, 0, len(got))

	got = m.SelectSegments(context.TODO(), WithCollection(1), WithChannel("h1"), SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return segment != nil && segment.GetState() == commonpb.SegmentState_Flushed
	}))
	assert.Equal(t, 1, len(got))
	assert.ElementsMatch(t, []int64{1}, lo.Map(
		got,
		func(s *SegmentInfo, i int) int64 {
			return s.ID
		},
	))

	m.segments.DropSegment(3)
	_, ok := m.segments.secondaryIndexes.coll2Segments[2]
	assert.False(t, ok)
	assert.Equal(t, 1, len(m.segments.secondaryIndexes.coll2Segments))
	assert.Equal(t, 2, len(m.segments.secondaryIndexes.channel2Segments))

	segments, ok := m.segments.secondaryIndexes.channel2Segments["h1"]
	assert.True(t, ok)
	assert.Equal(t, 1, len(segments))
	assert.Equal(t, int64(1), segments[1].ID)
	segments, ok = m.segments.secondaryIndexes.channel2Segments["h2"]
	assert.True(t, ok)
	assert.Equal(t, 1, len(segments))
	assert.Equal(t, int64(2), segments[2].ID)

	m.segments.DropSegment(2)
	segments, ok = m.segments.secondaryIndexes.coll2Segments[1]
	assert.True(t, ok)
	assert.Equal(t, 1, len(segments))
	assert.Equal(t, int64(1), segments[1].ID)
	assert.Equal(t, 1, len(m.segments.secondaryIndexes.coll2Segments))
	assert.Equal(t, 1, len(m.segments.secondaryIndexes.channel2Segments))

	segments, ok = m.segments.secondaryIndexes.channel2Segments["h1"]
	assert.True(t, ok)
	assert.Equal(t, 1, len(segments))
	assert.Equal(t, int64(1), segments[1].ID)
	_, ok = m.segments.secondaryIndexes.channel2Segments["h2"]
	assert.False(t, ok)
}

func TestMeta_HasSegments(t *testing.T) {
	m := &meta{
		segments: &SegmentsInfo{
			segments: map[UniqueID]*SegmentInfo{
				1: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:        1,
						NumOfRows: 100,
					},
				},
			},
		},
	}

	has, err := m.HasSegments([]UniqueID{1})
	assert.Equal(t, true, has)
	assert.NoError(t, err)

	has, err = m.HasSegments([]UniqueID{2})
	assert.Equal(t, false, has)
	assert.Error(t, err)
}

func TestMeta_GetAllSegments(t *testing.T) {
	m := &meta{
		segments: &SegmentsInfo{
			segments: map[UniqueID]*SegmentInfo{
				1: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:    1,
						State: commonpb.SegmentState_Growing,
					},
				},
				2: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:    2,
						State: commonpb.SegmentState_Dropped,
					},
				},
			},
		},
	}

	seg1 := m.GetHealthySegment(context.TODO(), 1)
	seg1All := m.GetSegment(context.TODO(), 1)
	seg2 := m.GetHealthySegment(context.TODO(), 2)
	seg2All := m.GetSegment(context.TODO(), 2)
	assert.NotNil(t, seg1)
	assert.NotNil(t, seg1All)
	assert.Nil(t, seg2)
	assert.NotNil(t, seg2All)
}

func TestMeta_isSegmentHealthy_issue17823_panic(t *testing.T) {
	var seg *SegmentInfo

	assert.False(t, isSegmentHealthy(seg))
}

func equalCollectionInfo(t *testing.T, a *collectionInfo, b *collectionInfo) {
	assert.Equal(t, a.ID, b.ID)
	assert.Equal(t, a.Partitions, b.Partitions)
	assert.Equal(t, a.Schema, b.Schema)
	assert.Equal(t, a.Properties, b.Properties)
	assert.Equal(t, a.StartPositions, b.StartPositions)
}

func TestChannelCP(t *testing.T) {
	mockVChannel := "fake-by-dev-rootcoord-dml-1-testchannelcp-v0"
	mockPChannel := "fake-by-dev-rootcoord-dml-1"

	pos := &msgpb.MsgPosition{
		ChannelName: mockPChannel,
		MsgID:       []byte{0, 0, 0, 0, 0, 0, 0, 0},
		Timestamp:   1000,
	}

	t.Run("UpdateChannelCheckpoint", func(t *testing.T) {
		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)

		// nil position
		err = meta.UpdateChannelCheckpoint(context.TODO(), mockVChannel, nil)
		assert.Error(t, err)

		err = meta.UpdateChannelCheckpoint(context.TODO(), mockVChannel, pos)
		assert.NoError(t, err)
	})

	t.Run("UpdateChannelCheckpoints", func(t *testing.T) {
		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(meta.channelCPs.checkpoints))

		err = meta.UpdateChannelCheckpoints(context.TODO(), nil)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(meta.channelCPs.checkpoints))

		err = meta.UpdateChannelCheckpoints(context.TODO(), []*msgpb.MsgPosition{pos, {
			ChannelName: "",
		}})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(meta.channelCPs.checkpoints))
	})

	t.Run("GetChannelCheckpoint", func(t *testing.T) {
		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)

		position := meta.GetChannelCheckpoint(mockVChannel)
		assert.Nil(t, position)

		err = meta.UpdateChannelCheckpoint(context.TODO(), mockVChannel, pos)
		assert.NoError(t, err)
		position = meta.GetChannelCheckpoint(mockVChannel)
		assert.NotNil(t, position)
		assert.True(t, position.ChannelName == pos.ChannelName)
		assert.True(t, position.Timestamp == pos.Timestamp)
	})

	t.Run("DropChannelCheckpoint", func(t *testing.T) {
		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)

		err = meta.DropChannelCheckpoint(mockVChannel)
		assert.NoError(t, err)

		err = meta.UpdateChannelCheckpoint(context.TODO(), mockVChannel, pos)
		assert.NoError(t, err)
		err = meta.DropChannelCheckpoint(mockVChannel)
		assert.NoError(t, err)
	})

	t.Run("WatchChannelCheckpoint", func(t *testing.T) {
		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)

		err = meta.UpdateChannelCheckpoint(context.TODO(), mockVChannel, pos)
		assert.NoError(t, err)
		err = meta.WatchChannelCheckpoint(context.TODO(), mockVChannel, pos.Timestamp-1)
		assert.NoError(t, err)
	})

	t.Run("TruncateChannelByTime", func(t *testing.T) {
		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)

		flushTs := uint64(2000)
		channelName := mockVChannel

		// Test case 1: No segments to drop
		err = meta.TruncateChannelByTime(context.TODO(), channelName, flushTs)
		assert.NoError(t, err)

		// Test case 2: Add segments that should be dropped (timestamp <= flushTs)
		seg1 := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            1,
				CollectionID:  1,
				PartitionID:   1,
				InsertChannel: channelName,
				State:         commonpb.SegmentState_Flushed,
				DmlPosition: &msgpb.MsgPosition{
					ChannelName: channelName,
					Timestamp:   flushTs - 100, // less than flushTs
				},
			},
		}
		err = meta.AddSegment(context.TODO(), seg1)
		assert.NoError(t, err)

		// Test case 3: Add segment that should NOT be dropped (timestamp > flushTs)
		seg2 := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            2,
				CollectionID:  1,
				PartitionID:   1,
				InsertChannel: channelName,
				State:         commonpb.SegmentState_Flushed,
				DmlPosition: &msgpb.MsgPosition{
					ChannelName: channelName,
					Timestamp:   flushTs + 100, // greater than flushTs
				},
			},
		}
		err = meta.AddSegment(context.TODO(), seg2)
		assert.NoError(t, err)

		// Test case 4: Add segment that is already dropped (should be skipped)
		seg3 := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            3,
				CollectionID:  1,
				PartitionID:   1,
				InsertChannel: channelName,
				State:         commonpb.SegmentState_Dropped,
				DmlPosition: &msgpb.MsgPosition{
					ChannelName: channelName,
					Timestamp:   flushTs - 100, // less than flushTs but already dropped
				},
			},
		}
		err = meta.AddSegment(context.TODO(), seg3)
		assert.NoError(t, err)

		// Test case 5: TruncateChannelByTime should drop seg1 but not seg2 or seg3
		err = meta.TruncateChannelByTime(context.TODO(), channelName, flushTs)
		assert.NoError(t, err)

		// Verify seg1 is dropped
		seg1After := meta.GetSegment(context.TODO(), seg1.ID)
		assert.NotNil(t, seg1After)
		assert.Equal(t, commonpb.SegmentState_Dropped, seg1After.GetState())

		// Verify seg2 is not dropped
		seg2After := meta.GetSegment(context.TODO(), seg2.ID)
		assert.NotNil(t, seg2After)
		assert.NotEqual(t, commonpb.SegmentState_Dropped, seg2After.GetState())

		// Verify seg3 remains dropped
		seg3After := meta.GetSegment(context.TODO(), seg3.ID)
		assert.NotNil(t, seg3After)
		assert.Equal(t, commonpb.SegmentState_Dropped, seg3After.GetState())

		// Test case 6: Call again with same flushTs, should return nil (no segments to drop)
		err = meta.TruncateChannelByTime(context.TODO(), channelName, flushTs)
		assert.NoError(t, err)
	})
}

func Test_meta_GcConfirm(t *testing.T) {
	m := &meta{}
	catalog := mocks2.NewDataCoordCatalog(t)
	m.catalog = catalog

	catalog.On("GcConfirm",
		mock.Anything,
		mock.AnythingOfType("int64"),
		mock.AnythingOfType("int64")).
		Return(false)

	assert.False(t, m.GcConfirm(context.TODO(), 100, 10000))
}

func Test_meta_ReloadCollectionsFromRootcoords(t *testing.T) {
	t.Run("fail to list database", func(t *testing.T) {
		m := &meta{
			collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		}
		mockBroker := broker.NewMockBroker(t)
		mockBroker.EXPECT().ListDatabases(mock.Anything).Return(nil, errors.New("list database failed, mocked"))
		err := m.reloadCollectionsFromRootcoord(context.TODO(), mockBroker)
		assert.Error(t, err)
	})

	t.Run("fail to show collections", func(t *testing.T) {
		m := &meta{
			collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		}
		mockBroker := broker.NewMockBroker(t)

		mockBroker.EXPECT().ListDatabases(mock.Anything).Return(&milvuspb.ListDatabasesResponse{
			DbNames: []string{"db1"},
		}, nil)
		mockBroker.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(nil, errors.New("show collections failed, mocked"))
		err := m.reloadCollectionsFromRootcoord(context.TODO(), mockBroker)
		assert.Error(t, err)
	})

	t.Run("fail to describe collection", func(t *testing.T) {
		m := &meta{
			collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		}
		mockBroker := broker.NewMockBroker(t)

		mockBroker.EXPECT().ListDatabases(mock.Anything).Return(&milvuspb.ListDatabasesResponse{
			DbNames: []string{"db1"},
		}, nil)
		mockBroker.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&milvuspb.ShowCollectionsResponse{
			CollectionNames: []string{"coll1"},
			CollectionIds:   []int64{1000},
		}, nil)
		mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).Return(nil, errors.New("describe collection failed, mocked"))
		err := m.reloadCollectionsFromRootcoord(context.TODO(), mockBroker)
		assert.Error(t, err)
	})

	t.Run("fail to show partitions", func(t *testing.T) {
		m := &meta{
			collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		}
		mockBroker := broker.NewMockBroker(t)

		mockBroker.EXPECT().ListDatabases(mock.Anything).Return(&milvuspb.ListDatabasesResponse{
			DbNames: []string{"db1"},
		}, nil)
		mockBroker.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&milvuspb.ShowCollectionsResponse{
			CollectionNames: []string{"coll1"},
			CollectionIds:   []int64{1000},
		}, nil)
		mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{}, nil)
		mockBroker.EXPECT().ShowPartitionsInternal(mock.Anything, mock.Anything).Return(nil, errors.New("show partitions failed, mocked"))
		err := m.reloadCollectionsFromRootcoord(context.TODO(), mockBroker)
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		m := &meta{
			collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		}
		mockBroker := broker.NewMockBroker(t)

		mockBroker.EXPECT().ListDatabases(mock.Anything).Return(&milvuspb.ListDatabasesResponse{
			DbNames: []string{"db1"},
		}, nil)
		mockBroker.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&milvuspb.ShowCollectionsResponse{
			CollectionNames: []string{"coll1"},
			CollectionIds:   []int64{1000},
		}, nil)
		mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
			CollectionID: 1000,
		}, nil)
		mockBroker.EXPECT().ShowPartitionsInternal(mock.Anything, mock.Anything).Return([]int64{2000}, nil)
		err := m.reloadCollectionsFromRootcoord(context.TODO(), mockBroker)
		assert.NoError(t, err)
		c := m.GetCollection(UniqueID(1000))
		assert.NotNil(t, c)
	})
}

func TestMeta_GetSegmentsJSON(t *testing.T) {
	// Create a mock meta object
	m := &meta{
		segments: &SegmentsInfo{
			segments: map[int64]*SegmentInfo{
				1: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:            1,
						CollectionID:  1,
						PartitionID:   1,
						InsertChannel: "channel1",
						NumOfRows:     100,
						State:         commonpb.SegmentState_Growing,
						MaxRowNum:     1000,
						Compacted:     false,
					},
				},
				2: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:            2,
						CollectionID:  2,
						PartitionID:   2,
						InsertChannel: "channel2",
						NumOfRows:     200,
						State:         commonpb.SegmentState_Sealed,
						MaxRowNum:     2000,
						Compacted:     true,
					},
				},
			},
		},
	}

	segments := m.getSegmentsMetrics(0)

	// Check the length of the segments
	assert.Equal(t, 2, len(segments))

	slices.SortFunc(segments, func(i, j *metricsinfo.Segment) int { return int(i.SegmentID - j.SegmentID) })

	// Check the first segment
	assert.Equal(t, int64(1), segments[0].SegmentID)
	assert.Equal(t, int64(1), segments[0].CollectionID)
	assert.Equal(t, int64(1), segments[0].PartitionID)
	assert.Equal(t, "channel1", segments[0].Channel)
	assert.Equal(t, int64(100), segments[0].NumOfRows)
	assert.Equal(t, "Growing", segments[0].State)
	assert.False(t, segments[0].Compacted)

	// Check the second segment
	assert.Equal(t, int64(2), segments[1].SegmentID)
	assert.Equal(t, int64(2), segments[1].CollectionID)
	assert.Equal(t, int64(2), segments[1].PartitionID)
	assert.Equal(t, "channel2", segments[1].Channel)
	assert.Equal(t, int64(200), segments[1].NumOfRows)
	assert.Equal(t, "Sealed", segments[1].State)
	assert.True(t, segments[1].Compacted)
}

func Test_meta_DropSegmentsOfPartition(t *testing.T) {
	meta, err := newMemoryMeta(t)
	assert.NoError(t, err)

	err = meta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:           1,
		PartitionID:  1,
		CollectionID: 1,
	}))
	assert.NoError(t, err)
	err = meta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:           2,
		PartitionID:  1,
		CollectionID: 1,
	}))
	assert.NoError(t, err)
	err = meta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:           3,
		PartitionID:  2,
		CollectionID: 1,
	}))
	assert.NoError(t, err)

	err = meta.DropSegmentsOfPartition(context.Background(), []int64{1})
	assert.NoError(t, err)

	segment := meta.GetSegment(context.Background(), 1)
	assert.Equal(t, commonpb.SegmentState_Dropped, segment.GetState())
	segment = meta.GetSegment(context.Background(), 2)
	assert.Equal(t, commonpb.SegmentState_Dropped, segment.GetState())
	segment = meta.GetSegment(context.Background(), 3)
	assert.NotEqual(t, commonpb.SegmentState_Dropped, segment.GetState())
}

func TestGetMinGrowingSegmentCheckpoint(t *testing.T) {
	mockVChannel := "fake-by-dev-rootcoord-dml-1-testgrowing-v0"

	t.Run("empty returns nil", func(t *testing.T) {
		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)

		pos := meta.GetMinGrowingSegmentCheckpoint(mockVChannel)
		assert.Nil(t, pos)
	})

	t.Run("growing L1 segments return min DmlPosition", func(t *testing.T) {
		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)

		// Register a TEXT collection so the checkpoint logic applies
		meta.collections.Insert(1, &collectionInfo{
			ID: 1,
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64},
					{FieldID: 101, Name: "text_field", DataType: schemapb.DataType_Text},
				},
			},
		})

		// Growing L1 segment with T300
		seg1 := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            1,
				CollectionID:  1,
				PartitionID:   1,
				InsertChannel: mockVChannel,
				State:         commonpb.SegmentState_Growing,
				Level:         datapb.SegmentLevel_L1,
				DmlPosition: &msgpb.MsgPosition{
					ChannelName: mockVChannel,
					MsgID:       []byte{1},
					Timestamp:   300,
				},
			},
		}
		err = meta.AddSegment(context.TODO(), seg1)
		assert.NoError(t, err)

		// Growing L1 segment with T500
		seg2 := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            2,
				CollectionID:  1,
				PartitionID:   1,
				InsertChannel: mockVChannel,
				State:         commonpb.SegmentState_Growing,
				Level:         datapb.SegmentLevel_L1,
				DmlPosition: &msgpb.MsgPosition{
					ChannelName: mockVChannel,
					MsgID:       []byte{2},
					Timestamp:   500,
				},
			},
		}
		err = meta.AddSegment(context.TODO(), seg2)
		assert.NoError(t, err)

		pos := meta.GetMinGrowingSegmentCheckpoint(mockVChannel)
		assert.NotNil(t, pos)
		assert.Equal(t, uint64(300), pos.GetTimestamp())
	})

	t.Run("L0 segments excluded", func(t *testing.T) {
		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)

		// Growing L0 segment with T100
		seg := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            1,
				CollectionID:  1,
				PartitionID:   1,
				InsertChannel: mockVChannel,
				State:         commonpb.SegmentState_Growing,
				Level:         datapb.SegmentLevel_L0,
				DmlPosition: &msgpb.MsgPosition{
					ChannelName: mockVChannel,
					MsgID:       []byte{1},
					Timestamp:   100,
				},
			},
		}
		err = meta.AddSegment(context.TODO(), seg)
		assert.NoError(t, err)

		pos := meta.GetMinGrowingSegmentCheckpoint(mockVChannel)
		assert.Nil(t, pos)
	})

	t.Run("non-Growing segments excluded", func(t *testing.T) {
		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)

		// Flushed L1 segment with T100
		seg := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            1,
				CollectionID:  1,
				PartitionID:   1,
				InsertChannel: mockVChannel,
				State:         commonpb.SegmentState_Flushed,
				Level:         datapb.SegmentLevel_L1,
				DmlPosition: &msgpb.MsgPosition{
					ChannelName: mockVChannel,
					MsgID:       []byte{1},
					Timestamp:   100,
				},
			},
		}
		err = meta.AddSegment(context.TODO(), seg)
		assert.NoError(t, err)

		pos := meta.GetMinGrowingSegmentCheckpoint(mockVChannel)
		assert.Nil(t, pos)
	})
}

func TestUpdateChannelCheckpoint_ClampedByGrowing(t *testing.T) {
	mockVChannel := "fake-by-dev-rootcoord-dml-1-testclamp-v0"

	meta, err := newMemoryMeta(t)
	assert.NoError(t, err)

	// Register a TEXT collection so clamping is enabled
	meta.collections.Insert(1, &collectionInfo{
		ID: 1,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "text_field", DataType: schemapb.DataType_Text},
			},
		},
	})

	// Add a growing L1 segment at T300
	seg := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            1,
			CollectionID:  1,
			PartitionID:   1,
			InsertChannel: mockVChannel,
			State:         commonpb.SegmentState_Growing,
			Level:         datapb.SegmentLevel_L1,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: mockVChannel,
				MsgID:       []byte{1},
				Timestamp:   300,
			},
		},
	}
	err = meta.AddSegment(context.TODO(), seg)
	assert.NoError(t, err)

	// Update checkpoint with T500, should be clamped to T300 (TEXT collection)
	err = meta.UpdateChannelCheckpoint(context.TODO(), mockVChannel, &msgpb.MsgPosition{
		ChannelName: mockVChannel,
		MsgID:       []byte{0, 0, 0, 0, 0, 0, 0, 0},
		Timestamp:   500,
	})
	assert.NoError(t, err)

	cp := meta.GetChannelCheckpoint(mockVChannel)
	assert.NotNil(t, cp)
	assert.Equal(t, uint64(300), cp.GetTimestamp())
}

func TestUpdateChannelCheckpoint_NotClampedForNonTextCollection(t *testing.T) {
	mockVChannel := "fake-by-dev-rootcoord-dml-1-testclamp-notext-v0"

	meta, err := newMemoryMeta(t)
	assert.NoError(t, err)

	// Register a non-TEXT collection — clamping should NOT apply
	meta.collections.Insert(2, &collectionInfo{
		ID: 2,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "varchar_field", DataType: schemapb.DataType_VarChar},
			},
		},
	})

	// Add a growing L1 segment at T300 for the non-TEXT collection
	seg := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            10,
			CollectionID:  2,
			PartitionID:   1,
			InsertChannel: mockVChannel,
			State:         commonpb.SegmentState_Growing,
			Level:         datapb.SegmentLevel_L1,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: mockVChannel,
				MsgID:       []byte{1},
				Timestamp:   300,
			},
		},
	}
	err = meta.AddSegment(context.TODO(), seg)
	assert.NoError(t, err)

	// Update checkpoint with T500 — should NOT be clamped (non-TEXT collection)
	err = meta.UpdateChannelCheckpoint(context.TODO(), mockVChannel, &msgpb.MsgPosition{
		ChannelName: mockVChannel,
		MsgID:       []byte{0, 0, 0, 0, 0, 0, 0, 0},
		Timestamp:   500,
	})
	assert.NoError(t, err)

	cp := meta.GetChannelCheckpoint(mockVChannel)
	assert.NotNil(t, cp)
	assert.Equal(t, uint64(500), cp.GetTimestamp()) // NOT clamped
}

func TestUpdateChannelCheckpoints_ClampedByGrowing(t *testing.T) {
	mockVChannel := "fake-by-dev-rootcoord-dml-1-testclampbatch-v0"

	meta, err := newMemoryMeta(t)
	assert.NoError(t, err)

	// Register a TEXT collection so clamping is enabled
	meta.collections.Insert(1, &collectionInfo{
		ID: 1,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "text_field", DataType: schemapb.DataType_Text},
			},
		},
	})

	// Add a growing L1 segment at T300
	seg := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            1,
			CollectionID:  1,
			PartitionID:   1,
			InsertChannel: mockVChannel,
			State:         commonpb.SegmentState_Growing,
			Level:         datapb.SegmentLevel_L1,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: mockVChannel,
				MsgID:       []byte{1},
				Timestamp:   300,
			},
		},
	}
	err = meta.AddSegment(context.TODO(), seg)
	assert.NoError(t, err)

	// Batch update checkpoint with T500, should be clamped to T300 (TEXT collection)
	err = meta.UpdateChannelCheckpoints(context.TODO(), []*msgpb.MsgPosition{
		{
			ChannelName: mockVChannel,
			MsgID:       []byte{0, 0, 0, 0, 0, 0, 0, 0},
			Timestamp:   500,
		},
	})
	assert.NoError(t, err)

	cp := meta.GetChannelCheckpoint(mockVChannel)
	assert.NotNil(t, cp)
	assert.Equal(t, uint64(300), cp.GetTimestamp())
}

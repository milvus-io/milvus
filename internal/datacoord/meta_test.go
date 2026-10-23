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
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	prometheustestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/snapshot"
	mockkv "github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	mocks2 "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
)

// MetaReloadSuite tests meta reload & meta creation related logic
type MetaReloadSuite struct {
	testutils.PromMetricsSuite

	catalog *mocks2.DataCoordCatalog
	meta    *meta.Meta
}

func (suite *MetaReloadSuite) SetupTest() {
	catalog := mocks2.NewDataCoordCatalog(suite.T())
	catalog.EXPECT().ListSegmentChangeGroups(mock.Anything).Return(nil, nil).Maybe()
	suite.catalog = catalog
}

func (suite *MetaReloadSuite) resetMock() {
	suite.catalog.ExpectedCalls = nil
	// newMeta calls ListSegmentChangeGroups (group recovery) on every
	// construction; SetupTest runs once per test method, not per subtest, so
	// resetMock must re-register this default or the subtests that reach
	// loadSegmentChangeGroups fail on an unexpected call (C12).
	suite.catalog.EXPECT().ListSegmentChangeGroups(mock.Anything).Return(nil, nil).Maybe()
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
		suite.catalog.EXPECT().ListCompactionTargets(mock.Anything).Return(nil, nil).Maybe()
		suite.catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

		_, err := meta.NewMeta(ctx, suite.catalog, nil, metacache.NewMetaStore(suite.catalog), brk, loadDomainMetas(suite.catalog, nil, &domainMetas{}))
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
		suite.catalog.EXPECT().ListCompactionTargets(mock.Anything).Return(nil, nil).Maybe()
		suite.catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

		_, err := meta.NewMeta(ctx, suite.catalog, nil, metacache.NewMetaStore(suite.catalog), brk, loadDomainMetas(suite.catalog, nil, &domainMetas{}))
		suite.Error(err)
	})

	suite.Run("ok", func() {
		defer suite.resetMock()
		paramtable.Get().Save(Params.DataCoordCfg.EnableTargetBasedCompaction.Key, "true")
		defer paramtable.Get().Reset(Params.DataCoordCfg.EnableTargetBasedCompaction.Key)

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
		suite.catalog.EXPECT().ListCompactionTargets(mock.Anything).Return([]*datapb.CompactionTarget{
			{
				TargetID: 10,
				Intent:   datapb.TargetIntent_INTENT_REWRITE,
				State:    datapb.TargetState_TARGET_STATE_ACTIVE,
			},
		}, nil)
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

		dm := &domainMetas{}
		_, err := meta.NewMeta(ctx, suite.catalog, nil, metacache.NewMetaStore(suite.catalog), brk, loadDomainMetas(suite.catalog, nil, dm))
		suite.NoError(err)
		suite.NotNil(dm.compactionTargetMeta)
		suite.Equal(datapb.TargetState_TARGET_STATE_ACTIVE, dm.compactionTargetMeta.GetCompactionTarget(10).GetState())

		suite.MetricsEqual(metrics.DataCoordNumSegments.WithLabelValues(metrics.FlushedSegmentLabel, datapb.SegmentLevel_Legacy.String(), "unsorted", "0", "legacy"), 1)
	})

	suite.Run("ListIndexes_fail", func() {
		defer suite.resetMock()
		brk := broker.NewMockBroker(suite.T())
		brk.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, errors.New("mock"))
		suite.catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return([]*model.SegmentIndex{}, nil).Maybe()
		suite.catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListCompactionTargets(mock.Anything).Return(nil, nil).Maybe()
		suite.catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

		_, err := meta.NewMeta(ctx, suite.catalog, nil, metacache.NewMetaStore(suite.catalog), brk, loadDomainMetas(suite.catalog, nil, &domainMetas{}))
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
		suite.catalog.EXPECT().ListCompactionTargets(mock.Anything).Return(nil, nil).Maybe()
		suite.catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

		_, err := meta.NewMeta(ctx, suite.catalog, nil, metacache.NewMetaStore(suite.catalog), brk, loadDomainMetas(suite.catalog, nil, &domainMetas{}))
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
		suite.catalog.EXPECT().ListCompactionTargets(mock.Anything).Return(nil, nil).Maybe()
		suite.catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, errors.New("mock"))
		suite.catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

		_, err := meta.NewMeta(ctx, suite.catalog, nil, metacache.NewMetaStore(suite.catalog), brk, loadDomainMetas(suite.catalog, nil, &domainMetas{}))
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
		suite.catalog.EXPECT().ListCompactionTargets(mock.Anything).Return(nil, nil).Maybe()
		suite.catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

		_, err := meta.NewMeta(ctx, suite.catalog, nil, metacache.NewMetaStore(suite.catalog), brk, loadDomainMetas(suite.catalog, nil, &domainMetas{}))
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
		suite.catalog.EXPECT().ListCompactionTargets(mock.Anything).Return(nil, nil).Maybe()
		suite.catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, errors.New("mock"))
		suite.catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

		_, err := meta.NewMeta(ctx, suite.catalog, nil, metacache.NewMetaStore(suite.catalog), brk, loadDomainMetas(suite.catalog, nil, &domainMetas{}))
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
		suite.catalog.EXPECT().ListCompactionTargets(mock.Anything).Return(nil, nil).Maybe()
		suite.catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, errors.New("mock"))
		suite.catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

		_, err := meta.NewMeta(ctx, suite.catalog, nil, metacache.NewMetaStore(suite.catalog), brk, loadDomainMetas(suite.catalog, nil, &domainMetas{}))
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
		suite.catalog.EXPECT().ListCompactionTargets(mock.Anything).Return(nil, nil).Maybe()
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

		mt, err := meta.NewMeta(ctx, suite.catalog, nil, metacache.NewMetaStore(suite.catalog), brk, loadDomainMetas(suite.catalog, nil, &domainMetas{}))
		suite.NoError(err)
		for _, collectionID := range []int64{100, 101, 102, 200, 201, 202} {
			segments := mt.GetSegmentsOfCollection(ctx, collectionID)
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

	meta *meta.Meta
}

func (suite *MetaBasicSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *MetaBasicSuite) SetupTest() {
	suite.collID = 1
	suite.partIDs = []int64{100, 101}
	suite.channelName = "c1"

	mt, err := meta.NewMemoryMeta(suite.T())

	suite.Require().NoError(err)
	suite.meta = mt
}

func (suite *MetaBasicSuite) getCollectionInfo(partIDs ...int64) *meta.CollectionInfo {
	testSchema := meta.NewTestSchema()
	return &meta.CollectionInfo{
		ID:             suite.collID,
		Schema:         testSchema,
		Partitions:     partIDs,
		StartPositions: []*commonpb.KeyDataPair{},
	}
}

func (suite *MetaBasicSuite) TestCollection() {
	mt := suite.meta

	info := suite.getCollectionInfo(suite.partIDs...)
	mt.AddCollection(info)

	collInfo := mt.GetCollection(suite.collID)
	suite.Require().NotNil(collInfo)

	// check partition info
	suite.EqualValues(suite.collID, collInfo.ID)
	suite.EqualValues(info.Schema, collInfo.Schema)
	suite.EqualValues(len(suite.partIDs), len(collInfo.Partitions))
	suite.ElementsMatch(info.Partitions, collInfo.Partitions)

	suite.MetricsEqual(metrics.DataCoordNumCollections.WithLabelValues(), 1)
}

func (suite *MetaBasicSuite) TestCompleteCompactionMutation() {
	ms := metacache.NewMetaStore(nil)
	getLatestSegments := func() *meta.SegmentsInfo {
		latestSegments := meta.NewSegmentsInfo(ms)
		for segID, segment := range map[UniqueID]*meta.SegmentInfo{
			1: {SegmentInfo: &datapb.SegmentInfo{
				ID:           1,
				CollectionID: 100,
				PartitionID:  10,
				State:        commonpb.SegmentState_Flushed,
				Level:        datapb.SegmentLevel_L1,
				Binlogs:      []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10000, 10001)},
				Statslogs:    []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 20000, 20001)},
				// latest segment has 2 deltalogs, one submit for compaction, one is appended before compaction done
				Deltalogs: []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 30000), meta.GetFieldBinlogIDs(0, 30001)},
				NumOfRows: 2,
			}},
			2: {SegmentInfo: &datapb.SegmentInfo{
				ID:           2,
				CollectionID: 100,
				PartitionID:  10,
				State:        commonpb.SegmentState_Flushed,
				Level:        datapb.SegmentLevel_L1,
				Binlogs:      []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 11000)},
				Statslogs:    []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 21000)},
				// latest segment has 2 deltalogs, one submit for compaction, one is appended before compaction done
				Deltalogs: []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 31000), meta.GetFieldBinlogIDs(0, 31001)},
				NumOfRows: 2,
			}},
		} {
			latestSegments.SetSegment(segID, segment)
		}

		return latestSegments
	}

	mockChMgr := mocks.NewChunkManager(suite.T())

	// numL1Legacy reads DataCoordNumSegments for the fixture segments' labels
	// (L1, unsorted, storage version 0, legacy format).
	numL1Legacy := func(state commonpb.SegmentState) float64 {
		return prometheustestutil.ToFloat64(metrics.DataCoordNumSegments.WithLabelValues(
			state.String(), datapb.SegmentLevel_L1.String(), "unsorted", "0", "legacy"))
	}

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
			Schema:        &schemapb.CollectionSchema{Version: 1},
		}
		m := &meta.Meta{
			Catalog:      &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments:     latestSegments,
			ChunkManager: mockChMgr,
		}

		flushedBefore := numL1Legacy(commonpb.SegmentState_Flushed)
		droppedBefore := numL1Legacy(commonpb.SegmentState_Dropped)
		infos, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		suite.NoError(err)
		suite.Empty(infos) // No output segments when all data deleted

		// check compactFrom segments are marked as Dropped
		for _, segID := range []int64{1, 2} {
			seg := m.GetSegment(context.TODO(), segID)
			suite.Equal(commonpb.SegmentState_Dropped, seg.GetState())
			suite.NotEmpty(seg.GetDroppedAt())
			suite.True(seg.GetCompacted())
		}

		// segment metrics - only input segments changed to Dropped
		suite.EqualValues(-2, numL1Legacy(commonpb.SegmentState_Flushed)-flushedBefore)
		suite.EqualValues(2, numL1Legacy(commonpb.SegmentState_Dropped)-droppedBefore)
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
			Schema:        &schemapb.CollectionSchema{Version: 1},
		}
		m := &meta.Meta{
			Catalog:      &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments:     latestSegments,
			ChunkManager: mockChMgr,
		}

		flushedBefore := numL1Legacy(commonpb.SegmentState_Flushed)
		droppedBefore := numL1Legacy(commonpb.SegmentState_Dropped)
		infos, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		assert.NoError(suite.T(), err)
		suite.Equal(1, len(infos))
		info := infos[0]
		suite.NoError(err)
		suite.NotNil(info)

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
			suite.ElementsMatch(latestSegments.GetSegment(segID).GetBinlogs(), seg.GetBinlogs())
			suite.ElementsMatch(latestSegments.GetSegment(segID).GetStatslogs(), seg.GetStatslogs())
			suite.ElementsMatch(latestSegments.GetSegment(segID).GetDeltalogs(), seg.GetDeltalogs())
		}

		// segment metrics - inputs dropped, empty compactTo segment added as Dropped
		suite.EqualValues(-2, numL1Legacy(commonpb.SegmentState_Flushed)-flushedBefore)
		suite.EqualValues(3, numL1Legacy(commonpb.SegmentState_Dropped)-droppedBefore)
	})

	suite.Run("test complete compaction mutation", func() {
		latestSegments := getLatestSegments()
		compactToSeg := &datapb.CompactionSegment{
			SegmentID:           3,
			InsertLogs:          []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 50000)},
			Field2StatslogPaths: []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 50001)},
			NumOfRows:           2,
		}

		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{compactToSeg},
		}
		task := &datapb.CompactionTask{
			InputSegments: []UniqueID{1, 2},
			Type:          datapb.CompactionType_MixCompaction,
			Schema:        &schemapb.CollectionSchema{Version: 1},
		}
		m := &meta.Meta{
			Catalog:      &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments:     latestSegments,
			ChunkManager: mockChMgr,
		}

		flushedBefore := numL1Legacy(commonpb.SegmentState_Flushed)
		droppedBefore := numL1Legacy(commonpb.SegmentState_Dropped)
		infos, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		assert.NoError(suite.T(), err)
		suite.Equal(1, len(infos))
		info := infos[0]
		suite.NoError(err)
		suite.NotNil(info)

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
			suite.ElementsMatch(latestSegments.GetSegment(segID).GetBinlogs(), seg.GetBinlogs())
			suite.ElementsMatch(latestSegments.GetSegment(segID).GetStatslogs(), seg.GetStatslogs())
			suite.ElementsMatch(latestSegments.GetSegment(segID).GetDeltalogs(), seg.GetDeltalogs())
		}

		// segment metrics - inputs dropped, compactTo segment added as Flushed
		suite.EqualValues(-1, numL1Legacy(commonpb.SegmentState_Flushed)-flushedBefore)
		suite.EqualValues(2, numL1Legacy(commonpb.SegmentState_Dropped)-droppedBefore)
	})

	suite.Run("mixed schema version mix compaction uses task schema version", func() {
		latestSegments := getLatestSegments()
		latestSegments.GetSegment(1).SchemaVersion = 2
		latestSegments.GetSegment(2).SchemaVersion = 3
		compactToSeg := &datapb.CompactionSegment{
			SegmentID:           5,
			InsertLogs:          []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 50000)},
			Field2StatslogPaths: []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 50001)},
			NumOfRows:           2,
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{compactToSeg},
		}
		task := &datapb.CompactionTask{
			InputSegments: []UniqueID{1, 2},
			Type:          datapb.CompactionType_MixCompaction,
			Schema:        &schemapb.CollectionSchema{Version: 9},
		}
		m := &meta.Meta{
			Catalog:      &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments:     latestSegments,
			ChunkManager: mockChMgr,
		}

		infos, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		suite.NoError(err)
		suite.Require().Len(infos, 1)
		suite.EqualValues(9, infos[0].GetSchemaVersion())
	})

	suite.Run("mixed schema version clustering compaction uses task schema version", func() {
		latestSegments := meta.NewSegmentsInfo(ms)
		for segID, segment := range map[UniqueID]*meta.SegmentInfo{
			1: {SegmentInfo: &datapb.SegmentInfo{
				ID:            1,
				CollectionID:  100,
				PartitionID:   10,
				State:         commonpb.SegmentState_Flushed,
				Level:         datapb.SegmentLevel_L1,
				Binlogs:       []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10000)},
				Statslogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 20000)},
				NumOfRows:     2,
				SchemaVersion: 1,
			}},
			2: {SegmentInfo: &datapb.SegmentInfo{
				ID:            2,
				CollectionID:  100,
				PartitionID:   10,
				State:         commonpb.SegmentState_Flushed,
				Level:         datapb.SegmentLevel_L1,
				Binlogs:       []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 11000)},
				Statslogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 21000)},
				NumOfRows:     2,
				SchemaVersion: 4,
			}},
		} {
			latestSegments.SetSegment(segID, segment)
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{{
				SegmentID:           3,
				InsertLogs:          []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 50000)},
				Field2StatslogPaths: []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 50001)},
				NumOfRows:           4,
			}},
		}
		task := &datapb.CompactionTask{
			InputSegments: []UniqueID{1, 2},
			Type:          datapb.CompactionType_ClusteringCompaction,
			Schema:        &schemapb.CollectionSchema{Version: 9},
		}
		m := &meta.Meta{
			Catalog:      &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments:     latestSegments,
			ChunkManager: mockChMgr,
		}

		infos, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		suite.NoError(err)
		suite.Require().Len(infos, 1)
		suite.EqualValues(9, infos[0].GetSchemaVersion())
	})

	suite.Run("test mix complete rejects nil task schema", func() {
		latestSegments := getLatestSegments()
		compactToSeg := &datapb.CompactionSegment{
			SegmentID:           7,
			InsertLogs:          []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 50000)},
			Field2StatslogPaths: []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 50001)},
			NumOfRows:           2,
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{compactToSeg},
		}
		task := &datapb.CompactionTask{
			InputSegments: []UniqueID{1, 2},
			Type:          datapb.CompactionType_MixCompaction,
		}
		m := &meta.Meta{
			Catalog:      &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments:     latestSegments,
			ChunkManager: mockChMgr,
		}

		infos, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		suite.Error(err)
		suite.True(errors.Is(err, merr.ErrIllegalCompactionPlan))
		suite.Nil(infos)
	})

	suite.Run("test L2 sort", func() {
		getLatestSegments := func() *meta.SegmentsInfo {
			latestSegments := meta.NewSegmentsInfo(ms)
			for segID, segment := range map[UniqueID]*meta.SegmentInfo{
				1: {SegmentInfo: &datapb.SegmentInfo{
					ID:           1,
					CollectionID: 100,
					PartitionID:  10,
					State:        commonpb.SegmentState_Flushed,
					Level:        datapb.SegmentLevel_L2,
					Binlogs:      []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10000, 10001)},
					Statslogs:    []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 20000, 20001)},
					// latest segment has 2 deltalogs, one submit for compaction, one is appended before compaction done
					Deltalogs:      []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 30000), meta.GetFieldBinlogIDs(0, 30001)},
					NumOfRows:      2,
					StorageVersion: storage.StorageV1,
					SchemaVersion:  2,
				}},
			} {
				latestSegments.SetSegment(segID, segment)
			}

			return latestSegments
		}

		latestSegments := getLatestSegments()
		compactToSeg := &datapb.CompactionSegment{
			SegmentID:           2,
			InsertLogs:          []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 50000)},
			Field2StatslogPaths: []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 50001)},
			NumOfRows:           2,
			StorageVersion:      storage.StorageV2,
		}

		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{compactToSeg},
		}
		task := &datapb.CompactionTask{
			InputSegments: []UniqueID{1},
			Type:          datapb.CompactionType_SortCompaction,
			Schema:        &schemapb.CollectionSchema{Version: 3},
		}
		m := &meta.Meta{
			Catalog:      &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments:     latestSegments,
			ChunkManager: mockChMgr,
		}

		infos, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		assert.NoError(suite.T(), err)
		suite.Equal(1, len(infos))
		info := infos[0]
		suite.NoError(err)
		suite.NotNil(info)

		// check newSegment
		suite.EqualValues(2, info.GetID())
		suite.Equal(datapb.SegmentLevel_L2, info.GetLevel())
		suite.Equal(commonpb.SegmentState_Flushed, info.GetState())
		suite.Equal(storage.StorageV2, info.GetStorageVersion())
		suite.EqualValues(3, info.GetSchemaVersion())

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

		latestSegments = getLatestSegments()
		compactToSeg = &datapb.CompactionSegment{
			SegmentID:           3,
			InsertLogs:          []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 50000)},
			Field2StatslogPaths: []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 50001)},
			NumOfRows:           2,
			StorageVersion:      storage.StorageV2,
		}
		result = &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{compactToSeg},
		}
		task = &datapb.CompactionTask{
			InputSegments: []UniqueID{1},
			Type:          datapb.CompactionType_SortCompaction,
		}
		m = &meta.Meta{
			Catalog:      &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments:     latestSegments,
			ChunkManager: mockChMgr,
		}

		infos, err = m.CompleteCompactionMutation(context.TODO(), task, result)
		suite.Error(err)
		suite.True(errors.Is(err, merr.ErrIllegalCompactionPlan))
		suite.Nil(infos)
		suite.Equal(commonpb.SegmentState_Flushed, m.GetSegment(context.TODO(), 1).GetState())
	})

	suite.Run("mix compaction normalizes commit_timestamp to zero", func() {
		// Input: two import segments with different commit_timestamps.
		// After compaction, row timestamps are already rewritten to commit_ts
		// by the compactor, so the output segment is normalized (CommitTimestamp = 0).
		latestSegments := meta.NewSegmentsInfo(ms)
		latestSegments.SetSegment(1, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 1, CollectionID: 100, PartitionID: 10,
			State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
			NumOfRows: 2, CommitTimestamp: 5000,
		}})
		latestSegments.SetSegment(2, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 2, CollectionID: 100, PartitionID: 10,
			State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
			NumOfRows: 3, CommitTimestamp: 8000,
		}})

		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{{SegmentID: 10, NumOfRows: 5}},
		}
		task := &datapb.CompactionTask{
			InputSegments: []UniqueID{1, 2},
			Type:          datapb.CompactionType_MixCompaction,
			Schema:        &schemapb.CollectionSchema{Version: 1},
		}
		m := &meta.Meta{
			Catalog:      &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments:     latestSegments,
			ChunkManager: mockChMgr,
		}
		infos, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		suite.NoError(err)
		suite.Require().Equal(1, len(infos))
		suite.EqualValues(0, infos[0].GetCommitTimestamp(), "compaction normalizes commit_timestamp: row timestamps already rewritten")
	})

	suite.Run("sort compaction normalizes commit_timestamp to zero", func() {
		latestSegments := meta.NewSegmentsInfo(ms)
		latestSegments.SetSegment(1, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 1, CollectionID: 100, PartitionID: 10,
			State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L2,
			NumOfRows: 2, CommitTimestamp: 7777,
		}})

		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{{SegmentID: 2, NumOfRows: 2}},
		}
		task := &datapb.CompactionTask{
			InputSegments: []UniqueID{1},
			Type:          datapb.CompactionType_SortCompaction,
			Schema:        &schemapb.CollectionSchema{Version: 1},
		}
		m := &meta.Meta{
			Catalog:      &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments:     latestSegments,
			ChunkManager: mockChMgr,
		}
		infos, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		suite.NoError(err)
		suite.Require().Equal(1, len(infos))
		suite.EqualValues(0, infos[0].GetCommitTimestamp(), "sort compaction normalizes commit_timestamp: row timestamps already rewritten")
	})

	suite.Run("clustering compaction normalizes commit_timestamp to zero", func() {
		latestSegments := meta.NewSegmentsInfo(ms)
		latestSegments.SetSegment(1, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 1, CollectionID: 100, PartitionID: 10,
			State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
			NumOfRows: 2, CommitTimestamp: 6000,
		}})
		latestSegments.SetSegment(2, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 2, CollectionID: 100, PartitionID: 10,
			State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
			NumOfRows: 3, CommitTimestamp: 4000,
		}})

		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{{SegmentID: 10, NumOfRows: 5}},
		}
		task := &datapb.CompactionTask{
			InputSegments: []UniqueID{1, 2},
			Type:          datapb.CompactionType_ClusteringCompaction,
		}
		m := &meta.Meta{
			Catalog:      &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments:     latestSegments,
			ChunkManager: mockChMgr,
		}
		infos, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		suite.NoError(err)
		suite.Require().Equal(1, len(infos))
		suite.EqualValues(0, infos[0].GetCommitTimestamp(), "clustering compaction normalizes commit_timestamp: row timestamps already rewritten")
	})

	suite.Run("mix compaction with mixed import and normal segments normalizes to zero", func() {
		// One import segment (commitTs=5000) + one normal segment (commitTs=0).
		// After compaction, row timestamps are rewritten, so output is normalized.
		latestSegments := meta.NewSegmentsInfo(ms)
		latestSegments.SetSegment(1, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 1, CollectionID: 100, PartitionID: 10,
			State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
			NumOfRows: 2, CommitTimestamp: 5000,
		}})
		latestSegments.SetSegment(2, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 2, CollectionID: 100, PartitionID: 10,
			State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
			NumOfRows: 3, CommitTimestamp: 0,
		}})

		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{{SegmentID: 10, NumOfRows: 5}},
		}
		task := &datapb.CompactionTask{
			InputSegments: []UniqueID{1, 2},
			Type:          datapb.CompactionType_MixCompaction,
			Schema:        &schemapb.CollectionSchema{Version: 1},
		}
		m := &meta.Meta{
			Catalog:      &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments:     latestSegments,
			ChunkManager: mockChMgr,
		}
		infos, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		suite.NoError(err)
		suite.Require().Equal(1, len(infos))
		suite.EqualValues(0, infos[0].GetCommitTimestamp(), "compaction normalizes commit_timestamp: row timestamps already rewritten")
	})

	suite.Run("mix compaction with no import segments sets commit_timestamp to 0", func() {
		latestSegments := meta.NewSegmentsInfo(ms)
		latestSegments.SetSegment(1, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 1, CollectionID: 100, PartitionID: 10,
			State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
			NumOfRows: 2, CommitTimestamp: 0,
		}})
		latestSegments.SetSegment(2, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 2, CollectionID: 100, PartitionID: 10,
			State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
			NumOfRows: 3, CommitTimestamp: 0,
		}})

		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{{SegmentID: 10, NumOfRows: 5}},
		}
		task := &datapb.CompactionTask{
			InputSegments: []UniqueID{1, 2},
			Type:          datapb.CompactionType_MixCompaction,
			Schema:        &schemapb.CollectionSchema{Version: 1},
		}
		m := &meta.Meta{
			Catalog:      &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments:     latestSegments,
			ChunkManager: mockChMgr,
		}
		infos, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		suite.NoError(err)
		suite.Require().Equal(1, len(infos))
		suite.EqualValues(0, infos[0].GetCommitTimestamp(), "normal segment compaction must not set commit_timestamp")
	})

	suite.Run("sort compaction normalizes stale import fallback start position", func() {
		latestSegments := meta.NewSegmentsInfo(ms)
		latestSegments.SetSegment(1, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 1, CollectionID: 100, PartitionID: 10,
			State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L2,
			NumOfRows: 2, CommitTimestamp: 5000,
			StartPosition: &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 1000},
		}})

		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{{SegmentID: 2, NumOfRows: 2}},
		}
		task := &datapb.CompactionTask{
			InputSegments: []UniqueID{1},
			Type:          datapb.CompactionType_SortCompaction,
			Channel:       "ch-1",
			Schema:        &schemapb.CollectionSchema{Version: 1},
		}
		m := &meta.Meta{
			Catalog:      &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments:     latestSegments,
			ChunkManager: mockChMgr,
		}
		infos, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		suite.NoError(err)
		suite.Require().Equal(1, len(infos))
		suite.EqualValues(5000, infos[0].GetStartPosition().GetTimestamp())
		suite.EqualValues(0, infos[0].GetCommitTimestamp(), "sort compaction normalizes commit_timestamp after rewriting row timestamps")
	})

	suite.Run("mix compaction preserves fallback start while normalizing fallback dml", func() {
		latestSegments := meta.NewSegmentsInfo(ms)
		latestSegments.SetSegment(1, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 1, CollectionID: 100, PartitionID: 10,
			State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
			NumOfRows: 2, CommitTimestamp: 5000,
			StartPosition: &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 1000},
			DmlPosition:   &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 1500},
		}})
		latestSegments.SetSegment(2, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 2, CollectionID: 100, PartitionID: 10,
			State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
			NumOfRows: 3, CommitTimestamp: 0,
			StartPosition: &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 2000},
			DmlPosition:   &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 3000},
		}})

		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{{SegmentID: 10, NumOfRows: 5}},
		}
		task := &datapb.CompactionTask{
			InputSegments: []UniqueID{1, 2},
			Type:          datapb.CompactionType_MixCompaction,
			Channel:       "ch-1",
			Schema:        &schemapb.CollectionSchema{Version: 1},
		}
		m := &meta.Meta{
			Catalog:      &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments:     latestSegments,
			ChunkManager: mockChMgr,
		}
		infos, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		suite.NoError(err)
		suite.Require().Equal(1, len(infos))
		suite.EqualValues(1000, infos[0].GetStartPosition().GetTimestamp())
		suite.EqualValues(5000, infos[0].GetDmlPosition().GetTimestamp())
		suite.EqualValues(0, infos[0].GetCommitTimestamp())
	})
}

func (suite *MetaBasicSuite) TestCompleteMixCompactionMutation_UsesCompositeUpdate() {
	ms := metacache.NewMetaStore(nil)
	mockChMgr := mocks.NewChunkManager(suite.T())

	latestSegments := meta.NewSegmentsInfo(ms)
	latestSegments.SetSegment(1, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:           1,
		CollectionID: 100,
		PartitionID:  10,
		State:        commonpb.SegmentState_Flushed,
		Level:        datapb.SegmentLevel_L1,
		Binlogs:      []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10000)},
		NumOfRows:    2,
	}})
	latestSegments.SetSegment(2, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:           2,
		CollectionID: 100,
		PartitionID:  10,
		State:        commonpb.SegmentState_Flushed,
		Level:        datapb.SegmentLevel_L1,
		Binlogs:      []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 11000)},
		NumOfRows:    2,
	}})

	compactToSeg := &datapb.CompactionSegment{
		SegmentID:           3,
		InsertLogs:          []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 50000)},
		Field2StatslogPaths: []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 50001)},
		NumOfRows:           4,
	}
	result := &datapb.CompactionPlanResult{
		Segments: []*datapb.CompactionSegment{compactToSeg},
	}
	task := &datapb.CompactionTask{
		InputSegments: []UniqueID{1, 2},
		Type:          datapb.CompactionType_MixCompaction,
		Schema:        &schemapb.CollectionSchema{Version: 1},
	}

	// catalog has no AlterSegments expectation: if completeMixCompactionMutation
	// still called AlterSegments directly (the old two-call path), mockery
	// would fail the test with an unexpected-call panic.
	catalog := mocks2.NewDataCoordCatalog(suite.T())
	catalog.EXPECT().ListSegmentChangeGroups(mock.Anything).Return(nil, nil).Maybe()
	var gotActions []metastore.UpdateAction
	catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, actions ...metastore.UpdateAction) error {
			gotActions = actions
			return nil
		}).Once()

	m := &meta.Meta{
		Catalog:      catalog,
		Segments:     latestSegments,
		ChunkManager: mockChMgr,
	}

	_, err := m.CompleteCompactionMutation(context.TODO(), task, result)
	suite.NoError(err)

	// One AddSegment for the compactTo segment, then one UpdateSegment per
	// compactFrom segment (2). catalog.Update called exactly once (.Once()).
	suite.Require().Len(gotActions, 3, "expected one add + two update actions")

	// The compactTo add comes first, so it is published before the compactFrom
	// segments are retired in the fallback (chunked) ordering.
	compactToEntry, ok := gotActions[0].Entry.(metastore.SegmentEntry)
	suite.Require().True(ok)
	suite.Equal(metastore.ActionAdd, gotActions[0].Type)
	suite.EqualValues(3, compactToEntry.Segment.GetID())

	var compactFromIDs []int64
	for _, a := range gotActions[1:] {
		se, ok := a.Entry.(metastore.SegmentEntry)
		suite.Require().True(ok)
		suite.Equal(metastore.ActionUpdate, a.Type)
		compactFromIDs = append(compactFromIDs, se.Segment.GetID())
	}
	suite.ElementsMatch([]int64{1, 2}, compactFromIDs)
}

func (suite *MetaBasicSuite) TestBatchSaveDropSegments_UsesCompositeUpdate() {
	ms := metacache.NewMetaStore(nil)
	modSegments := map[int64]*meta.SegmentInfo{
		1: {SegmentInfo: &datapb.SegmentInfo{
			ID:           1,
			CollectionID: 100,
			PartitionID:  10,
			State:        commonpb.SegmentState_Dropped,
		}},
		2: {SegmentInfo: &datapb.SegmentInfo{
			ID:           2,
			CollectionID: 100,
			PartitionID:  10,
			State:        commonpb.SegmentState_Dropped,
		}},
	}

	// catalog has no SaveDroppedSegmentsInBatch/MarkChannelDeleted
	// expectation: if batchSaveDropSegments still called those directly (the
	// old two-call path), mockery would fail the test with an
	// unexpected-call panic.
	catalog := mocks2.NewDataCoordCatalog(suite.T())
	catalog.EXPECT().ListSegmentChangeGroups(mock.Anything).Return(nil, nil).Maybe()
	var gotActions []metastore.UpdateAction
	catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, actions ...metastore.UpdateAction) error {
			gotActions = actions
			return nil
		}).Once()

	m := &meta.Meta{
		Catalog:  catalog,
		Segments: meta.NewSegmentsInfo(ms),
	}

	err := m.BatchSaveDropSegments(context.TODO(), "ch-1", modSegments)
	suite.NoError(err)

	// One UpdateSegment per dropped segment (2), then the channel tombstone.
	// catalog.Update called exactly once (.Once()).
	suite.Require().Len(gotActions, 3, "expected two segment updates + one channel action")

	// The channel tombstone is last (the visibility marker).
	channelEntry, ok := gotActions[2].Entry.(metastore.ChannelEntry)
	suite.Require().True(ok)
	suite.Equal(metastore.ActionUpdate, gotActions[2].Type)
	suite.Require().Equal("ch-1", channelEntry.Channel)

	var droppedIDs []int64
	for _, a := range gotActions[:2] {
		se, ok := a.Entry.(metastore.SegmentEntry)
		suite.Require().True(ok)
		suite.Equal(metastore.ActionUpdate, a.Type)
		droppedIDs = append(droppedIDs, se.Segment.GetID())
	}
	suite.ElementsMatch([]int64{1, 2}, droppedIDs)

	// memory info updated
	suite.NotNil(m.Segments.GetSegment(1))
	suite.NotNil(m.Segments.GetSegment(2))
}

func (suite *MetaBasicSuite) TestValidateSegmentState_BlockedBySnapshot() {
	ms := metacache.NewMetaStore(nil)
	latestSegments := meta.NewSegmentsInfo(ms)
	for segID, segment := range map[UniqueID]*meta.SegmentInfo{
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
		sm := snapshot.CreateTestSnapshotMetaLoaded(suite.T())
		sm.SetSnapshotPending(100)

		m := &meta.Meta{
			Segments: latestSegments,
		}
		m.CompactionProtection = sm

		err := m.ValidateSegmentStateBeforeCompleteCompactionMutation(task)
		suite.Error(err)
		suite.Contains(err.Error(), "compaction blocked")
	})

	suite.Run("rejected by segment protection", func() {
		sm := snapshot.CreateTestSnapshotMetaLoaded(suite.T())
		futureTs := uint64(time.Now().Unix()) + 3600
		sm.SegmentProtectionMu.Lock()
		sm.SegmentProtectionUntil[1] = futureTs
		sm.SegmentProtectionMu.Unlock()

		m := &meta.Meta{
			Segments: latestSegments,
		}
		m.CompactionProtection = sm

		err := m.ValidateSegmentStateBeforeCompleteCompactionMutation(task)
		suite.Error(err)
		suite.Contains(err.Error(), "segment 1")
	})

	suite.Run("passes when no snapshot", func() {
		sm := snapshot.CreateTestSnapshotMetaLoaded(suite.T())

		m := &meta.Meta{
			Segments: latestSegments,
		}
		m.CompactionProtection = sm

		err := m.ValidateSegmentStateBeforeCompleteCompactionMutation(task)
		suite.NoError(err)
	})

	suite.Run("passes when snapshotMeta is nil", func() {
		m := &meta.Meta{
			Segments: latestSegments,
		}

		err := m.ValidateSegmentStateBeforeCompleteCompactionMutation(task)
		suite.NoError(err)
	})

	suite.Run("rejected when only middle segment is protected in multi-segment task", func() {
		multiSegments := meta.NewSegmentsInfo(ms)
		for segID, segment := range map[UniqueID]*meta.SegmentInfo{
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

		sm := snapshot.CreateTestSnapshotMetaLoaded(suite.T())
		futureTs := uint64(time.Now().Unix()) + 3600
		// Only protect segment 2
		sm.SegmentProtectionMu.Lock()
		sm.SegmentProtectionUntil[2] = futureTs
		sm.SegmentProtectionMu.Unlock()

		m := &meta.Meta{
			Segments: multiSegments,
		}
		m.CompactionProtection = sm

		err := m.ValidateSegmentStateBeforeCompleteCompactionMutation(multiTask)
		suite.Error(err)
		suite.Contains(err.Error(), "segment 2")
	})

	suite.Run("passes when protection expired", func() {
		sm := snapshot.CreateTestSnapshotMetaLoaded(suite.T())
		pastTs := uint64(time.Now().Unix()) - 100
		sm.SegmentProtectionMu.Lock()
		sm.SegmentProtectionUntil[1] = pastTs
		sm.SegmentProtectionMu.Unlock()

		m := &meta.Meta{
			Segments: latestSegments,
		}
		m.CompactionProtection = sm

		err := m.ValidateSegmentStateBeforeCompleteCompactionMutation(task)
		suite.NoError(err)
	})

	// Regression: L0 delete compaction must bypass snapshot protection checks entirely.
	// Snapshots only reference sealed L1/L2 segments; blocking L0 here would cause
	// delta log accumulation, query latency spikes, and write stalls.
	suite.Run("L0 compaction passes even when collection is snapshot-pending", func() {
		sm := snapshot.CreateTestSnapshotMetaLoaded(suite.T())
		sm.SetSnapshotPending(100)

		m := &meta.Meta{
			Segments: latestSegments,
		}
		m.CompactionProtection = sm

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
		sm := snapshot.CreateTestSnapshotMetaLoaded(suite.T())
		futureTs := uint64(time.Now().Unix()) + 3600
		sm.SegmentProtectionMu.Lock()
		sm.SegmentProtectionUntil[1] = futureTs
		sm.SegmentProtectionMu.Unlock()

		m := &meta.Meta{
			Segments: latestSegments,
		}
		m.CompactionProtection = sm

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
		sm := snapshot.CreateTestSnapshotMetaLoaded(suite.T())
		sm.SetSnapshotPending(100)

		m := &meta.Meta{
			Segments: latestSegments,
		}
		m.CompactionProtection = sm

		err := m.ValidateSegmentStateBeforeCompleteCompactionMutation(task)
		suite.Error(err)
		suite.True(errors.Is(err, merr.ErrCompactionBlocked))
		suite.False(errors.Is(err, merr.ErrServiceInternal))
	})
}

func (suite *MetaBasicSuite) TestGetMaxPosition() {
	suite.Run("nil_positions", func() {
		pos := meta.GetMaxPosition(nil)
		suite.Nil(pos)
	})

	suite.Run("single_position", func() {
		p := &msgpb.MsgPosition{Timestamp: 100}
		pos := meta.GetMaxPosition([]*msgpb.MsgPosition{p})
		suite.Equal(uint64(100), pos.GetTimestamp())
	})

	suite.Run("multiple_positions", func() {
		pos := meta.GetMaxPosition([]*msgpb.MsgPosition{
			{Timestamp: 100},
			{Timestamp: 300},
			{Timestamp: 200},
		})
		suite.Equal(uint64(300), pos.GetTimestamp())
	})

	suite.Run("with_nil_entries", func() {
		pos := meta.GetMaxPosition([]*msgpb.MsgPosition{
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
		startPos, dmlPos := meta.RecalculateSegmentPosition(binlogs, channel, fallbackStart, fallbackDml)
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
		startPos, dmlPos := meta.RecalculateSegmentPosition(binlogs, channel, fallbackStart, fallbackDml)
		suite.Equal(fallbackStart, startPos)
		suite.Equal(fallbackDml, dmlPos)
	})

	suite.Run("fallback_when_no_binlogs", func() {
		startPos, dmlPos := meta.RecalculateSegmentPosition(nil, channel, fallbackStart, fallbackDml)
		suite.Equal(fallbackStart, startPos)
		suite.Equal(fallbackDml, dmlPos)
	})

	suite.Run("fallback_when_empty_binlogs", func() {
		startPos, dmlPos := meta.RecalculateSegmentPosition([]*datapb.FieldBinlog{}, channel, fallbackStart, fallbackDml)
		suite.Equal(fallbackStart, startPos)
		suite.Equal(fallbackDml, dmlPos)
	})
}

func (suite *MetaBasicSuite) TestCompleteCompactionMutation_RecalculatePositions() {
	ms := metacache.NewMetaStore(nil)
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
		latestSegments := meta.NewSegmentsInfo(ms)
		for segID, segment := range map[UniqueID]*meta.SegmentInfo{
			1: {SegmentInfo: &datapb.SegmentInfo{
				ID:            1,
				CollectionID:  100,
				PartitionID:   10,
				State:         commonpb.SegmentState_Flushed,
				Level:         datapb.SegmentLevel_L1,
				Binlogs:       []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10000)},
				Statslogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 20000)},
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
				Binlogs:       []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 11000)},
				Statslogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 21000)},
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
				Field2StatslogPaths: []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 50001)},
				NumOfRows:           4,
			}},
		}
		task := &datapb.CompactionTask{
			InputSegments: []UniqueID{1, 2},
			Type:          datapb.CompactionType_MixCompaction,
			Channel:       "ch-1",
			Schema:        &schemapb.CollectionSchema{Version: 1},
		}
		m := &meta.Meta{
			Catalog:      &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments:     latestSegments,
			ChunkManager: mockChMgr,
		}

		infos, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		suite.NoError(err)
		suite.Require().Equal(1, len(infos))

		// Should use binlog timestamps, NOT inherited positions
		suite.Equal(uint64(100), infos[0].GetStartPosition().GetTimestamp())
		suite.Equal(uint64(500), infos[0].GetDmlPosition().GetTimestamp())
		suite.Equal("ch-1", infos[0].GetStartPosition().GetChannelName())
	})

	suite.Run("mix_compaction_uses_output_timestamps_when_normal_segment_precedes_import_commit", func() {
		latestSegments := meta.NewSegmentsInfo(ms)
		for segID, segment := range map[UniqueID]*meta.SegmentInfo{
			1: {SegmentInfo: &datapb.SegmentInfo{
				ID:              1,
				CollectionID:    100,
				PartitionID:     10,
				State:           commonpb.SegmentState_Flushed,
				Level:           datapb.SegmentLevel_L1,
				Binlogs:         []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10000)},
				Statslogs:       []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 20000)},
				NumOfRows:       2,
				CommitTimestamp: 0,
				StartPosition:   &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 1000},
				DmlPosition:     &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 1500},
			}},
			2: {SegmentInfo: &datapb.SegmentInfo{
				ID:              2,
				CollectionID:    100,
				PartitionID:     10,
				State:           commonpb.SegmentState_Flushed,
				Level:           datapb.SegmentLevel_L1,
				Binlogs:         []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 11000)},
				Statslogs:       []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 21000)},
				NumOfRows:       3,
				CommitTimestamp: 5000,
				StartPosition:   &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 2000},
				DmlPosition:     &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 2500},
			}},
		} {
			latestSegments.SetSegment(segID, segment)
		}

		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{{
				SegmentID:           3,
				InsertLogs:          []*datapb.FieldBinlog{fieldBinlogWithTimestamps(0, 50000, 1000, 5000)},
				Field2StatslogPaths: []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 50001)},
				NumOfRows:           5,
			}},
		}
		task := &datapb.CompactionTask{
			InputSegments: []UniqueID{1, 2},
			Type:          datapb.CompactionType_MixCompaction,
			Channel:       "ch-1",
			Schema:        &schemapb.CollectionSchema{Version: 1},
		}
		m := &meta.Meta{
			Catalog:      &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments:     latestSegments,
			ChunkManager: mockChMgr,
		}

		infos, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		suite.NoError(err)
		suite.Require().Equal(1, len(infos))

		suite.EqualValues(0, infos[0].GetCommitTimestamp())
		suite.Equal(uint64(1000), infos[0].GetStartPosition().GetTimestamp())
		suite.Equal(uint64(5000), infos[0].GetDmlPosition().GetTimestamp())
	})

	suite.Run("mix_compaction_fallback_when_no_timestamps", func() {
		latestSegments := meta.NewSegmentsInfo(ms)
		for segID, segment := range map[UniqueID]*meta.SegmentInfo{
			1: {SegmentInfo: &datapb.SegmentInfo{
				ID:            1,
				CollectionID:  100,
				PartitionID:   10,
				State:         commonpb.SegmentState_Flushed,
				Level:         datapb.SegmentLevel_L1,
				Binlogs:       []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10000)},
				Statslogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 20000)},
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
				Binlogs:       []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 11000)},
				Statslogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 21000)},
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
				InsertLogs:          []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 50000)},
				Field2StatslogPaths: []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 50001)},
				NumOfRows:           4,
			}},
		}
		task := &datapb.CompactionTask{
			InputSegments: []UniqueID{1, 2},
			Type:          datapb.CompactionType_MixCompaction,
			Channel:       "ch-1",
			Schema:        &schemapb.CollectionSchema{Version: 1},
		}
		m := &meta.Meta{
			Catalog:      &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments:     latestSegments,
			ChunkManager: mockChMgr,
		}

		infos, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		suite.NoError(err)
		suite.Require().Equal(1, len(infos))

		// Fallback: StartPosition = min, DmlPosition = max of source segments
		suite.Equal(uint64(50), infos[0].GetStartPosition().GetTimestamp())
		suite.Equal(uint64(300), infos[0].GetDmlPosition().GetTimestamp())
	})

	suite.Run("cluster_compaction_recalculates_positions", func() {
		latestSegments := meta.NewSegmentsInfo(ms)
		for segID, segment := range map[UniqueID]*meta.SegmentInfo{
			1: {SegmentInfo: &datapb.SegmentInfo{
				ID:            1,
				CollectionID:  100,
				PartitionID:   10,
				State:         commonpb.SegmentState_Flushed,
				Level:         datapb.SegmentLevel_L1,
				Binlogs:       []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10000)},
				Statslogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 20000)},
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
				Binlogs:       []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 11000)},
				Statslogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 21000)},
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
				Field2StatslogPaths: []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 50001)},
				NumOfRows:           4,
			}},
		}
		task := &datapb.CompactionTask{
			InputSegments: []UniqueID{1, 2},
			Type:          datapb.CompactionType_ClusteringCompaction,
			Channel:       "ch-1",
		}
		m := &meta.Meta{
			Catalog:      &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments:     latestSegments,
			ChunkManager: mockChMgr,
		}

		infos, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		suite.NoError(err)
		suite.Require().Equal(1, len(infos))

		suite.Equal(uint64(150), infos[0].GetStartPosition().GetTimestamp())
		suite.Equal(uint64(600), infos[0].GetDmlPosition().GetTimestamp())
		suite.Equal(datapb.SegmentLevel_L2, infos[0].GetLevel())
	})

	suite.Run("cluster_compaction_fallback_when_no_timestamps", func() {
		latestSegments := meta.NewSegmentsInfo(ms)
		for segID, segment := range map[UniqueID]*meta.SegmentInfo{
			1: {SegmentInfo: &datapb.SegmentInfo{
				ID:            1,
				CollectionID:  100,
				PartitionID:   10,
				State:         commonpb.SegmentState_Flushed,
				Level:         datapb.SegmentLevel_L1,
				Binlogs:       []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10000)},
				Statslogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 20000)},
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
				Binlogs:       []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 11000)},
				Statslogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 21000)},
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
				InsertLogs:          []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 50000)},
				Field2StatslogPaths: []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 50001)},
				NumOfRows:           4,
			}},
		}
		task := &datapb.CompactionTask{
			InputSegments: []UniqueID{1, 2},
			Type:          datapb.CompactionType_ClusteringCompaction,
			Channel:       "ch-1",
		}
		m := &meta.Meta{
			Catalog:      &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments:     latestSegments,
			ChunkManager: mockChMgr,
		}

		infos, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		suite.NoError(err)
		suite.Require().Equal(1, len(infos))

		suite.Equal(uint64(50), infos[0].GetStartPosition().GetTimestamp())
		suite.Equal(uint64(500), infos[0].GetDmlPosition().GetTimestamp())
	})

	suite.Run("sort_compaction_recalculates_positions", func() {
		latestSegments := meta.NewSegmentsInfo(ms)
		latestSegments.SetSegment(1, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:             1,
			CollectionID:   100,
			PartitionID:    10,
			State:          commonpb.SegmentState_Flushed,
			Level:          datapb.SegmentLevel_L2,
			Binlogs:        []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10000)},
			Statslogs:      []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 20000)},
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
				Field2StatslogPaths: []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 50001)},
				NumOfRows:           2,
				StorageVersion:      storage.StorageV2,
			}},
		}
		task := &datapb.CompactionTask{
			InputSegments: []UniqueID{1},
			Type:          datapb.CompactionType_SortCompaction,
			Schema:        &schemapb.CollectionSchema{Version: 1},
		}
		m := &meta.Meta{
			Catalog:      &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments:     latestSegments,
			ChunkManager: mockChMgr,
		}

		infos, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		suite.NoError(err)
		suite.Require().Equal(1, len(infos))

		suite.Equal(uint64(200), infos[0].GetStartPosition().GetTimestamp())
		suite.Equal(uint64(800), infos[0].GetDmlPosition().GetTimestamp())
	})

	suite.Run("sort_compaction_fallback_when_no_timestamps", func() {
		latestSegments := meta.NewSegmentsInfo(ms)
		latestSegments.SetSegment(1, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:             1,
			CollectionID:   100,
			PartitionID:    10,
			State:          commonpb.SegmentState_Flushed,
			Level:          datapb.SegmentLevel_L2,
			Binlogs:        []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10000)},
			Statslogs:      []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 20000)},
			NumOfRows:      2,
			StartPosition:  &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 77},
			DmlPosition:    &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 333},
			InsertChannel:  "ch-1",
			StorageVersion: storage.StorageV1,
		}})

		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{{
				SegmentID:           2,
				InsertLogs:          []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 50000)},
				Field2StatslogPaths: []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 50001)},
				NumOfRows:           2,
				StorageVersion:      storage.StorageV2,
			}},
		}
		task := &datapb.CompactionTask{
			InputSegments: []UniqueID{1},
			Type:          datapb.CompactionType_SortCompaction,
			Schema:        &schemapb.CollectionSchema{Version: 1},
		}
		m := &meta.Meta{
			Catalog:      &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments:     latestSegments,
			ChunkManager: mockChMgr,
		}

		infos, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		suite.NoError(err)
		suite.Require().Equal(1, len(infos))

		suite.Equal(uint64(77), infos[0].GetStartPosition().GetTimestamp())
		suite.Equal(uint64(333), infos[0].GetDmlPosition().GetTimestamp())
	})
}

func (suite *MetaBasicSuite) TestSetSegment() {
	mt := suite.meta
	catalog := mocks2.NewDataCoordCatalog(suite.T())
	catalog.EXPECT().ListSegmentChangeGroups(mock.Anything).Return(nil, nil).Maybe()
	mt.Catalog = catalog
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	suite.Run("normal", func() {
		segmentID := int64(1000)
		catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Once()
		segment := meta.NewSegmentInfo(&datapb.SegmentInfo{
			ID:            segmentID,
			MaxRowNum:     30000,
			CollectionID:  suite.collID,
			InsertChannel: suite.channelName,
			State:         commonpb.SegmentState_Flushed,
		})
		err := mt.AddSegment(ctx, segment)
		suite.Require().NoError(err)

		noOp := func(segment *meta.SegmentInfo) bool {
			return true
		}

		catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Once()

		err = mt.UpdateSegment(segmentID, noOp)
		suite.NoError(err)
	})

	suite.Run("not_updated", func() {
		segmentID := int64(1001)
		catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Once()
		segment := meta.NewSegmentInfo(&datapb.SegmentInfo{
			ID:            segmentID,
			MaxRowNum:     30000,
			CollectionID:  suite.collID,
			InsertChannel: suite.channelName,
			State:         commonpb.SegmentState_Flushed,
		})
		err := mt.AddSegment(ctx, segment)
		suite.Require().NoError(err)

		noOp := func(segment *meta.SegmentInfo) bool {
			return false
		}

		err = mt.UpdateSegment(segmentID, noOp)
		suite.NoError(err)
	})

	suite.Run("catalog_error", func() {
		segmentID := int64(1002)
		catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Once()
		segment := meta.NewSegmentInfo(&datapb.SegmentInfo{
			ID:            segmentID,
			MaxRowNum:     30000,
			CollectionID:  suite.collID,
			InsertChannel: suite.channelName,
			State:         commonpb.SegmentState_Flushed,
		})
		err := mt.AddSegment(ctx, segment)
		suite.Require().NoError(err)

		noOp := func(segment *meta.SegmentInfo) bool {
			return true
		}

		catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(errors.New("mocked")).Once()

		err = mt.UpdateSegment(segmentID, noOp)
		suite.Error(err)
	})

	suite.Run("segment_not_found", func() {
		segmentID := int64(1003)

		noOp := func(segment *meta.SegmentInfo) bool {
			return true
		}

		err := mt.UpdateSegment(segmentID, noOp)
		suite.Error(err)
		suite.ErrorIs(err, merr.ErrSegmentNotFound)
	})
}

func (suite *MetaBasicSuite) TestCompleteBumpSchemaVersionCompactionMutation() {
	ms := metacache.NewMetaStore(nil)
	// Helper to build a SegmentsInfo containing a single healthy Flushed segment with the given ID.
	makeSegments := func(segID int64, state commonpb.SegmentState) *meta.SegmentsInfo {
		segs := meta.NewSegmentsInfo(metacache.NewMetaStore(nil))
		segs.SetSegment(segID, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            segID,
			CollectionID:  100,
			PartitionID:   10,
			State:         state,
			Level:         datapb.SegmentLevel_L1,
			Binlogs:       []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10000)},
			Statslogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 20000)},
			NumOfRows:     5,
			SchemaVersion: 1,
		}})
		return segs
	}

	suite.Run("too many input segments", func() {
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: makeSegments(1, commonpb.SegmentState_Flushed),
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1, 2}, // two inputs — should error
			Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{SegmentID: 1},
			},
		}
		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.Error(err)
		suite.Nil(infos)
	})

	suite.Run("too many result segments", func() {
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: makeSegments(1, commonpb.SegmentState_Flushed),
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{SegmentID: 1},
				{SegmentID: 2}, // two results — should error
			},
		}
		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.Error(err)
		suite.Nil(infos)
	})

	suite.Run("segment not found", func() {
		// Segment 99 is not in meta.
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: meta.NewSegmentsInfo(ms),
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{99},
			Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{SegmentID: 99},
			},
		}
		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.Error(err)
		suite.ErrorIs(err, merr.ErrSegmentNotFound)
		suite.Nil(infos)
	})

	suite.Run("segment dropped", func() {
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: makeSegments(1, commonpb.SegmentState_Dropped),
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{SegmentID: 1},
			},
		}
		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.Error(err)
		suite.ErrorIs(err, merr.ErrSegmentNotFound)
		suite.Nil(infos)
	})

	suite.Run("in-place result with matching base manifest is adopted", func() {
		currentManifest := packed.MarshalManifestPath("/data/segments/1", 10)
		resultManifest := packed.MarshalManifestPath("/data/segments/1", 12)
		segs := makeSegments(1, commonpb.SegmentState_Flushed)
		old := segs.GetSegment(1)
		old.StorageVersion = storage.StorageV3
		old.ManifestPath = currentManifest
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: segs,
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
			Schema:        &schemapb.CollectionSchema{Version: 3},
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:      1,
					NumOfRows:      5,
					InsertLogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10001)},
					Manifest:       resultManifest,
					BaseManifest:   currentManifest, // == current pointer and advances it, adopt
					StorageVersion: storage.StorageV3,
				},
			},
		}
		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.NoError(err)
		suite.Require().Len(infos, 1)
		suite.EqualValues(1, infos[0].GetID())
		suite.Equal(resultManifest, infos[0].GetManifestPath())
	})

	suite.Run("schema-bump-only nil result stats preserves old stats", func() {
		// runSchemaVersionBumpOnly ships Stats=nil by design (the receiver must
		// preserve oldSegment.Stats). For V3 the per-field KVs are skipped, so
		// clobbering with nil yields an all-zero durable summary after restart.
		currentManifest := packed.MarshalManifestPath("/data/segments/1", 10)
		resultManifest := packed.MarshalManifestPath("/data/segments/1", 12)
		segs := makeSegments(1, commonpb.SegmentState_Flushed)
		old := segs.GetSegment(1)
		old.StorageVersion = storage.StorageV3
		old.ManifestPath = currentManifest
		oldStats := &datapb.Statistics{
			InsertBinlogSize:  1234,
			InsertBinlogCount: 7,
			DeleteNumRows:     3,
			TimestampFrom:     10,
			TimestampTo:       50,
		}
		old.Stats = oldStats
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: segs,
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
			Schema:        &schemapb.CollectionSchema{Version: 3},
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:      1,
					NumOfRows:      5,
					InsertLogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10001)},
					Manifest:       resultManifest,
					BaseManifest:   currentManifest,
					StorageVersion: storage.StorageV3,
					// Stats intentionally nil (bump-only)
				},
			},
		}
		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.NoError(err)
		suite.Require().Len(infos, 1)
		suite.Require().NotNil(infos[0].GetStats())
		suite.EqualValues(1234, infos[0].GetStats().GetInsertBinlogSize())
		suite.EqualValues(7, infos[0].GetStats().GetInsertBinlogCount())
		suite.EqualValues(3, infos[0].GetStats().GetDeleteNumRows())
	})

	suite.Run("in-place result stats is added onto existing stats", func() {
		currentManifest := packed.MarshalManifestPath("/data/segments/1", 10)
		resultManifest := packed.MarshalManifestPath("/data/segments/1", 12)
		segs := makeSegments(1, commonpb.SegmentState_Flushed)
		old := segs.GetSegment(1)
		old.StorageVersion = storage.StorageV3
		old.ManifestPath = currentManifest
		old.Stats = &datapb.Statistics{
			InsertBinlogSize:  1000,
			InsertBinlogCount: 4,
			StatsBinlogSize:   20,
			DeleteNumRows:     3,
			DeltaBinlogSize:   77,
			TimestampFrom:     10,
			TimestampTo:       50,
			NullCounts:        map[int64]int64{100: 0},
		}
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: segs,
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
			Schema:        &schemapb.CollectionSchema{Version: 3},
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:      1,
					NumOfRows:      5,
					InsertLogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10001)},
					Manifest:       resultManifest,
					BaseManifest:   currentManifest,
					StorageVersion: storage.StorageV3,
					Stats: &datapb.Statistics{
						InsertBinlogSize:  300,
						InsertBinlogCount: 1,
						StatsBinlogSize:   5,
						NullCounts:        map[int64]int64{102: 0},
					},
				},
			},
		}
		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.NoError(err)
		suite.Require().Len(infos, 1)

		got := infos[0].GetStats()
		suite.EqualValues(1300, got.GetInsertBinlogSize())
		suite.EqualValues(5, got.GetInsertBinlogCount())
		suite.EqualValues(25, got.GetStatsBinlogSize())
		// Untouched by the increment.
		suite.EqualValues(3, got.GetDeleteNumRows())
		suite.EqualValues(77, got.GetDeltaBinlogSize())
		suite.EqualValues(10, got.GetTimestampFrom())
		suite.EqualValues(50, got.GetTimestampTo())
		suite.Equal(map[int64]int64{100: 0, 102: 0}, got.GetNullCounts())
	})

	suite.Run("replayed in-place result does not accumulate twice", func() {
		// result == current means a prior adoption already persisted this
		// manifest, so the increment is already in Stats. Accumulating again
		// would double-count. This test guards the coupling between the
		// accumulation and the forward-commit branch of the manifest CAS.
		manifest := packed.MarshalManifestPath("/data/segments/1", 12)
		segs := makeSegments(1, commonpb.SegmentState_Flushed)
		old := segs.GetSegment(1)
		old.StorageVersion = storage.StorageV3
		old.ManifestPath = manifest
		old.Stats = &datapb.Statistics{InsertBinlogSize: 1300, InsertBinlogCount: 5}
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: segs,
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
			Schema:        &schemapb.CollectionSchema{Version: 3},
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:      1,
					NumOfRows:      5,
					Manifest:       manifest, // == current pointer: replay
					BaseManifest:   packed.MarshalManifestPath("/data/segments/1", 10),
					StorageVersion: storage.StorageV3,
					Stats: &datapb.Statistics{
						InsertBinlogSize:  300,
						InsertBinlogCount: 1,
					},
				},
			},
		}
		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.NoError(err)
		suite.Require().Len(infos, 1)

		got := infos[0].GetStats()
		suite.EqualValues(1300, got.GetInsertBinlogSize())
		suite.EqualValues(5, got.GetInsertBinlogCount())
	})

	suite.Run("in-place result atomically merges ordinary and function-output column groups", func() {
		// The compactor ships only the groups this run wrote, so the receiver
		// must upsert them: the live Binlogs keeps the complete column set and
		// a not-yet-restarted QueryNode estimate stays correct. Replaying the
		// result must neither duplicate the new groups nor re-apply the
		// manifest-gated statistics delta.
		currentManifest := packed.MarshalManifestPath("/data/segments/1", 10)
		resultManifest := packed.MarshalManifestPath("/data/segments/1", 12)
		segs := makeSegments(1, commonpb.SegmentState_Flushed)
		old := segs.GetSegment(1)
		old.StorageVersion = storage.StorageV3
		old.ManifestPath = currentManifest
		old.Stats = &datapb.Statistics{
			InsertBinlogSize:  1024,
			InsertBinlogCount: 1,
			NullCounts:        map[int64]int64{100: 0},
		}
		old.Binlogs = []*datapb.FieldBinlog{
			{FieldID: 0, ChildFields: []int64{0, 1, 100}, Binlogs: []*datapb.Binlog{{LogID: 10000}}},
		}
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: segs,
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
			Schema:        &schemapb.CollectionSchema{Version: 3},
		}
		ordinaryGroup := &datapb.FieldBinlog{
			FieldID:     101,
			ChildFields: []int64{101},
			Binlogs:     []*datapb.Binlog{{LogID: 10001, EntriesNum: 5, MemorySize: 256}},
		}
		functionOutputGroup := &datapb.FieldBinlog{
			FieldID:     102,
			ChildFields: []int64{102},
			Binlogs:     []*datapb.Binlog{{LogID: 10002, EntriesNum: 5, MemorySize: 512}},
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:      1,
					NumOfRows:      5,
					InsertLogs:     []*datapb.FieldBinlog{ordinaryGroup, functionOutputGroup},
					Manifest:       resultManifest,
					BaseManifest:   currentManifest,
					StorageVersion: storage.StorageV3,
					Stats: &datapb.Statistics{
						InsertBinlogSize:  768,
						InsertBinlogCount: 2,
						NullCounts:        map[int64]int64{101: 5, 102: 0},
					},
				},
			},
		}
		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.NoError(err)
		suite.Require().Len(infos, 1)
		// The pre-existing group survives and both newly appended groups are
		// visible through the same adopted manifest.
		suite.ElementsMatch([]int64{0, 101, 102},
			lo.Map(infos[0].GetBinlogs(), func(fb *datapb.FieldBinlog, _ int) int64 { return fb.GetFieldID() }))
		for _, fb := range infos[0].GetBinlogs() {
			if fb.GetFieldID() == 0 {
				suite.ElementsMatch([]int64{0, 1, 100}, fb.GetChildFields(),
					"no child collision, so the pre-existing group keeps its fields")
			}
		}
		firstStats := infos[0].GetStats()
		suite.EqualValues(1792, firstStats.GetInsertBinlogSize())
		suite.EqualValues(3, firstStats.GetInsertBinlogCount())
		suite.Equal(map[int64]int64{100: 0, 101: 5, 102: 0}, firstStats.GetNullCounts())

		// Replay: the segment now sits at resultManifest, so this is the
		// idempotent-adoption branch. The merge must not duplicate either new
		// group, and the manifest-gated increment must not be accumulated twice.
		replayed, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.NoError(err)
		suite.Require().Len(replayed, 1)
		suite.Require().Len(replayed[0].GetBinlogs(), 3)
		suite.ElementsMatch([]int64{0, 101, 102},
			lo.Map(replayed[0].GetBinlogs(), func(fb *datapb.FieldBinlog, _ int) int64 { return fb.GetFieldID() }))
		replayedStats := replayed[0].GetStats()
		suite.EqualValues(1792, replayedStats.GetInsertBinlogSize())
		suite.EqualValues(3, replayedStats.GetInsertBinlogCount())
		suite.Equal(map[int64]int64{100: 0, 101: 5, 102: 0}, replayedStats.GetNullCounts())
	})

	suite.Run("materialized column group is visible on the live segment", func() {
		// Regression pin: the merged column group keeps the live Binlogs
		// complete (pre-restart QueryNode estimation reads it), while index
		// eligibility is gated by the schema-version advance — the inspector
		// does not read Binlogs and the build worker reads the manifest.
		currentManifest := packed.MarshalManifestPath("/data/segments/1", 10)
		resultManifest := packed.MarshalManifestPath("/data/segments/1", 12)
		segs := makeSegments(1, commonpb.SegmentState_Flushed)
		old := segs.GetSegment(1)
		old.StorageVersion = storage.StorageV3
		old.ManifestPath = currentManifest
		old.Binlogs = []*datapb.FieldBinlog{
			{FieldID: 0, ChildFields: []int64{0, 1, 100}, Binlogs: []*datapb.Binlog{{LogID: 10000}}},
		}
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: segs,
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
			Schema:        &schemapb.CollectionSchema{Version: 3},
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:      1,
					NumOfRows:      5,
					InsertLogs:     []*datapb.FieldBinlog{{FieldID: 102, ChildFields: []int64{102}, Binlogs: []*datapb.Binlog{{LogID: 10002}}}},
					Manifest:       resultManifest,
					BaseManifest:   currentManifest,
					StorageVersion: storage.StorageV3,
					Stats:          &datapb.Statistics{InsertBinlogSize: 1},
				},
			},
		}
		_, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.NoError(err)

		updated := m.Segments.GetSegment(1)
		fields := make(map[int64]struct{})
		for _, binlog := range updated.GetBinlogs() {
			for _, childFieldID := range binlog.GetChildFields() {
				fields[childFieldID] = struct{}{}
			}
		}
		suite.Contains(fields, int64(102), "materialized function-output field must be visible on live Binlogs")
		suite.Contains(fields, int64(100), "pre-existing fields keep their data")

		handler := meta.NewNMockHandler(suite.T())
		handler.EXPECT().GetCollection(mock.Anything, updated.GetCollectionID()).Return(&meta.CollectionInfo{
			ID: updated.GetCollectionID(),
			Schema: &schemapb.CollectionSchema{
				Version: 3,
				Fields: []*schemapb.FieldSchema{
					{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
				},
				Functions: []*schemapb.FunctionSchema{
					{OutputFieldIds: []int64{102}},
				},
			},
		}, nil).Once()
		inspector := &index.IndexInspector{Handler: handler}
		suite.True(inspector.CanCreateIndexForSegment(context.Background(), updated, &model.Index{
			CollectionID: updated.GetCollectionID(),
			FieldID:      102,
			IndexID:      1,
		}), "materialized function-output field must be index-eligible")
	})

	suite.Run("in-place result with stale base manifest is rejected", func() {
		segs := makeSegments(1, commonpb.SegmentState_Flushed)
		old := segs.GetSegment(1)
		old.StorageVersion = storage.StorageV3
		// pointer advanced past the result's base
		old.ManifestPath = packed.MarshalManifestPath("/data/segments/1", 11)
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: segs,
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
			Schema:        &schemapb.CollectionSchema{Version: 3},
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:      1,
					NumOfRows:      5,
					Manifest:       packed.MarshalManifestPath("/data/segments/1", 12),
					BaseManifest:   packed.MarshalManifestPath("/data/segments/1", 10), // stale, != current v11
					StorageVersion: storage.StorageV3,
				},
			},
		}
		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.Error(err)
		suite.ErrorIs(err, merr.ErrIllegalCompactionPlan)
		suite.Nil(infos)
	})

	// Defensive: base == current only claims the pointer has not drifted; it does
	// not independently validate the worker's result. A buggy worker that reports
	// the matching base but produces a result that does not advance the current
	// manifest on the same base path (rollback / equal version / different base
	// path / unparsable) must be rejected, not adopted.
	suite.Run("in-place result on current base that does not advance is rejected", func() {
		currentManifest := packed.MarshalManifestPath("/data/segments/1", 10)
		cases := map[string]string{
			"rollback":       packed.MarshalManifestPath("/data/segments/1", 9),
			"different base": packed.MarshalManifestPath("/data/segments/2", 12),
			"unparsable":     "not-a-manifest",
		}
		for name, resultManifest := range cases {
			suite.Run(name, func() {
				segs := makeSegments(1, commonpb.SegmentState_Flushed)
				old := segs.GetSegment(1)
				old.StorageVersion = storage.StorageV3
				old.ManifestPath = currentManifest
				m := &meta.Meta{
					Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
					Segments: segs,
				}
				task := &datapb.CompactionTask{
					InputSegments: []int64{1},
					Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
					Schema:        &schemapb.CollectionSchema{Version: 3},
				}
				result := &datapb.CompactionPlanResult{
					Segments: []*datapb.CompactionSegment{
						{
							SegmentID:      1,
							NumOfRows:      5,
							Manifest:       resultManifest,
							BaseManifest:   currentManifest, // == current pointer, but result does not advance it
							StorageVersion: storage.StorageV3,
						},
					},
				}
				infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
				suite.Error(err)
				suite.ErrorIs(err, merr.ErrIllegalCompactionPlan)
				suite.Nil(infos)
			})
		}
	})

	suite.Run("in-place result already at current manifest is idempotently adopted", func() {
		// Crash-replay: an adoption already succeeded (segment is at the
		// materialized manifest) but the task state was lost before meta_saved.
		// Replaying the same result — stale base, but manifest already == current
		// — must be accepted, not rejected, so the already-succeeded task is not
		// spuriously failed.
		segs := makeSegments(1, commonpb.SegmentState_Flushed)
		old := segs.GetSegment(1)
		old.StorageVersion = storage.StorageV3
		old.ManifestPath = "base/manifest-12" // already adopted the materialized manifest
		old.SchemaVersion = 3
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: segs,
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
			Schema:        &schemapb.CollectionSchema{Version: 3},
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:      1,
					NumOfRows:      5,
					InsertLogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10000)},
					Manifest:       "base/manifest-12", // == current pointer
					BaseManifest:   "base/manifest-10", // stale base, but result already current
					StorageVersion: storage.StorageV3,
				},
			},
		}
		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.NoError(err)
		suite.Require().Len(infos, 1)
		suite.Equal("base/manifest-12", infos[0].GetManifestPath())
	})

	suite.Run("replacement result without preallocated ID rejected", func() {
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: makeSegments(1, commonpb.SegmentState_Flushed),
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
			Schema:        &schemapb.CollectionSchema{Version: 3},
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:      999,
					Manifest:       "manifest-new",
					StorageVersion: storage.StorageV3,
				},
			},
		}
		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.Error(err)
		suite.Nil(infos)
	})

	suite.Run("replacement result accepts expanded preallocated segment ID range", func() {
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: makeSegments(1, commonpb.SegmentState_Flushed),
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
			Schema: &schemapb.CollectionSchema{
				Version: 3,
			},
			PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 2, End: 4},
		}
		oldBefore := m.Segments.GetSegment(1)
		oldBefore.InsertChannel = "test-channel"
		oldBefore.StartPosition = &msgpb.MsgPosition{ChannelName: "test-channel", Timestamp: 10}
		oldBefore.DmlPosition = &msgpb.MsgPosition{ChannelName: "test-channel", Timestamp: 20}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID: 2,
					NumOfRows: 5,
					InsertLogs: []*datapb.FieldBinlog{
						{
							FieldID: 0,
							Binlogs: []*datapb.Binlog{
								{LogID: 10001, TimestampFrom: 100, TimestampTo: 300},
								{LogID: 10002, TimestampFrom: 80, TimestampTo: 250},
							},
						},
					},
					Manifest:       "manifest-new",
					StorageVersion: storage.StorageV3,
				},
			},
		}

		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.NoError(err)
		suite.Require().Len(infos, 1)
		suite.EqualValues(2, infos[0].GetID())
		suite.Equal(commonpb.SegmentState_Flushed, infos[0].GetState())
		suite.EqualValues(3, infos[0].GetSchemaVersion())
		suite.Equal([]int64{int64(1)}, infos[0].GetCompactionFrom())
		suite.True(infos[0].GetCreatedByCompaction())

		old := m.Segments.GetSegment(1)
		suite.Require().NotNil(old)
		suite.Equal(commonpb.SegmentState_Dropped, old.GetState())
		suite.True(old.GetCompacted())
		suite.NotZero(old.GetDroppedAt())

		created := m.Segments.GetSegment(2)
		suite.Require().NotNil(created)
		suite.Equal(commonpb.SegmentState_Flushed, created.GetState())
		suite.EqualValues(3, created.GetSchemaVersion())
		suite.Equal("manifest-new", created.GetManifestPath())
		suite.False(created.GetIsInvisible())
		suite.Equal("test-channel", created.GetStartPosition().GetChannelName())
		suite.EqualValues(80, created.GetStartPosition().GetTimestamp())
		suite.Equal("test-channel", created.GetDmlPosition().GetChannelName())
		suite.EqualValues(300, created.GetDmlPosition().GetTimestamp())
	})

	suite.Run("zero-row replacement drops old segment and creates dropped new segment without manifest", func() {
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: makeSegments(1, commonpb.SegmentState_Flushed),
		}
		task := &datapb.CompactionTask{
			InputSegments:          []int64{1},
			Type:                   datapb.CompactionType_BumpSchemaVersionCompaction,
			Schema:                 &schemapb.CollectionSchema{Version: 3},
			PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 2, End: 3},
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:      2,
					NumOfRows:      0,
					StorageVersion: storage.StorageV3,
				},
			},
		}

		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.NoError(err)
		suite.Require().Len(infos, 1)
		suite.EqualValues(2, infos[0].GetID())
		suite.Equal(commonpb.SegmentState_Dropped, infos[0].GetState())
		suite.EqualValues(0, infos[0].GetNumOfRows())
		suite.Empty(infos[0].GetManifestPath())

		old := m.Segments.GetSegment(1)
		suite.Require().NotNil(old)
		suite.Equal(commonpb.SegmentState_Dropped, old.GetState())
		suite.True(old.GetCompacted())

		created := m.Segments.GetSegment(2)
		suite.Require().NotNil(created)
		suite.Equal(commonpb.SegmentState_Dropped, created.GetState())
		suite.Empty(created.GetManifestPath())
	})

	suite.Run("replacement preserves input sort flags", func() {
		for _, tc := range []struct {
			name                      string
			inputIsSorted             bool
			inputIsSortedByNamespace  bool
			resultIsSorted            bool
			resultIsSortedByNamespace bool
		}{
			{name: "sorted by pk", inputIsSorted: true},
			{name: "sorted by namespace", inputIsSortedByNamespace: true},
			{name: "ignores result-only flags", resultIsSorted: true, resultIsSortedByNamespace: true},
		} {
			suite.Run(tc.name, func() {
				segs := makeSegments(1, commonpb.SegmentState_Flushed)
				oldSegment := segs.GetSegment(1)
				oldSegment.IsSorted = tc.inputIsSorted
				oldSegment.IsSortedByNamespace = tc.inputIsSortedByNamespace
				m := &meta.Meta{
					Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
					Segments: segs,
				}
				task := &datapb.CompactionTask{
					InputSegments:          []int64{1},
					Type:                   datapb.CompactionType_BumpSchemaVersionCompaction,
					Schema:                 &schemapb.CollectionSchema{Version: 3},
					PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 2, End: 3},
				}
				result := &datapb.CompactionPlanResult{
					Segments: []*datapb.CompactionSegment{
						{
							SegmentID:           2,
							NumOfRows:           5,
							InsertLogs:          []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10001)},
							Manifest:            "manifest-new",
							StorageVersion:      storage.StorageV3,
							IsSorted:            tc.resultIsSorted,
							IsSortedByNamespace: tc.resultIsSortedByNamespace,
						},
					},
				}

				infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
				suite.NoError(err)
				suite.Require().Len(infos, 1)
				suite.Equal(tc.inputIsSorted, infos[0].GetIsSorted())
				suite.Equal(tc.inputIsSortedByNamespace, infos[0].GetIsSortedByNamespace())

				created := m.Segments.GetSegment(2)
				suite.Require().NotNil(created)
				suite.Equal(tc.inputIsSorted, created.GetIsSorted())
				suite.Equal(tc.inputIsSortedByNamespace, created.GetIsSortedByNamespace())
			})
		}
	})

	suite.Run("replacement result accepts same task schema", func() {
		segs := makeSegments(1, commonpb.SegmentState_Flushed)
		segs.GetSegment(1).SchemaVersion = 3
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: segs,
		}
		task := &datapb.CompactionTask{
			InputSegments:          []int64{1},
			Type:                   datapb.CompactionType_BumpSchemaVersionCompaction,
			Schema:                 &schemapb.CollectionSchema{Version: 3},
			PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 2, End: 3},
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:      2,
					NumOfRows:      5,
					InsertLogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10001)},
					Manifest:       "manifest-new",
					StorageVersion: storage.StorageV3,
				},
			},
		}

		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.NoError(err)
		suite.Require().Len(infos, 1)
		suite.EqualValues(2, infos[0].GetID())
		suite.EqualValues(3, infos[0].GetSchemaVersion())
		suite.False(infos[0].GetIsInvisible())
	})

	suite.Run("replacement result rejects stale task schema", func() {
		segs := makeSegments(1, commonpb.SegmentState_Flushed)
		segs.GetSegment(1).SchemaVersion = 3
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: segs,
		}
		task := &datapb.CompactionTask{
			InputSegments:          []int64{1},
			Type:                   datapb.CompactionType_BumpSchemaVersionCompaction,
			Schema:                 &schemapb.CollectionSchema{Version: 2},
			PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 2, End: 3},
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:      2,
					NumOfRows:      5,
					InsertLogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10001)},
					Manifest:       "manifest-new",
					StorageVersion: storage.StorageV3,
				},
			},
		}

		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.ErrorIs(err, merr.ErrIllegalCompactionPlan)
		suite.Nil(infos)
	})

	suite.Run("replacement result rejects invisible input", func() {
		segs := makeSegments(1, commonpb.SegmentState_Flushed)
		segs.GetSegment(1).IsInvisible = true
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: segs,
		}
		task := &datapb.CompactionTask{
			InputSegments:          []int64{1},
			Type:                   datapb.CompactionType_BumpSchemaVersionCompaction,
			Schema:                 &schemapb.CollectionSchema{Version: 3},
			PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 2, End: 3},
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:      2,
					NumOfRows:      5,
					InsertLogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10001)},
					Manifest:       "manifest-new",
					StorageVersion: storage.StorageV3,
				},
			},
		}

		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.ErrorIs(err, merr.ErrIllegalCompactionPlan)
		suite.Nil(infos)
	})

	suite.Run("replacement result rejects nil task schema", func() {
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: makeSegments(1, commonpb.SegmentState_Flushed),
		}
		task := &datapb.CompactionTask{
			InputSegments:          []int64{1},
			Type:                   datapb.CompactionType_BumpSchemaVersionCompaction,
			PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 2, End: 3},
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:      2,
					NumOfRows:      5,
					InsertLogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10001)},
					Manifest:       "manifest-new",
					StorageVersion: storage.StorageV3,
				},
			},
		}

		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.Error(err)
		suite.Nil(infos)
	})

	suite.Run("replacement result rejects non-begin preallocated segment ID", func() {
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: makeSegments(1, commonpb.SegmentState_Flushed),
		}
		task := &datapb.CompactionTask{
			InputSegments:          []int64{1},
			Type:                   datapb.CompactionType_BumpSchemaVersionCompaction,
			Schema:                 &schemapb.CollectionSchema{Version: 3},
			PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 2, End: 4},
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:      3,
					NumOfRows:      5,
					InsertLogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10001)},
					Manifest:       "manifest-new",
					StorageVersion: storage.StorageV3,
				},
			},
		}

		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.Error(err)
		suite.Nil(infos)
	})

	suite.Run("missing manifest rejected", func() {
		segs := meta.NewSegmentsInfo(ms)
		segs.SetSegment(1, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            1,
			CollectionID:  100,
			PartitionID:   10,
			State:         commonpb.SegmentState_Flushed,
			Level:         datapb.SegmentLevel_L1,
			Binlogs:       []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10000)},
			NumOfRows:     5,
			SchemaVersion: 1,
		}})
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: segs,
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
			Schema: &schemapb.CollectionSchema{
				Version: 3,
			},
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:  1,
					InsertLogs: []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10001)},
				},
			},
		}
		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.Error(err)
		suite.Nil(infos)
	})

	suite.Run("non-v3 result rejected", func() {
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: makeSegments(1, commonpb.SegmentState_Flushed),
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:      1,
					InsertLogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10001)},
					Manifest:       "manifest",
					StorageVersion: storage.StorageV2,
				},
			},
		}
		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.Error(err)
		suite.Nil(infos)
	})

	suite.Run("v3 success - forward manifest updated", func() {
		segs := meta.NewSegmentsInfo(ms)
		currentManifest := packed.MarshalManifestPath("/data/segments/1", 1)
		resultManifest := packed.MarshalManifestPath("/data/segments/1", 2)
		segs.SetSegment(1, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:             1,
			CollectionID:   100,
			PartitionID:    10,
			State:          commonpb.SegmentState_Flushed,
			Level:          datapb.SegmentLevel_L1,
			Binlogs:        []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10000)},
			Bm25Statslogs:  []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(101, 50001)},
			NumOfRows:      5,
			SchemaVersion:  1,
			StorageVersion: storage.StorageV3,
			ManifestPath:   currentManifest,
		}})
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: segs,
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
			Schema:        &schemapb.CollectionSchema{Version: 3},
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:      1,
					InsertLogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10001)},
					Manifest:       resultManifest,
					BaseManifest:   currentManifest,
					StorageVersion: storage.StorageV3,
				},
			},
		}
		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.NoError(err)
		suite.Require().Len(infos, 1)

		suite.Equal(resultManifest, infos[0].GetManifestPath())
		suite.EqualValues(3, infos[0].GetStorageVersion())
		suite.EqualValues(3, infos[0].GetSchemaVersion())
		suite.EqualValues(1, infos[0].GetDataVersion())

		fieldIDs := make(map[int64]bool)
		for _, fl := range infos[0].GetBm25Statslogs() {
			fieldIDs[fl.GetFieldID()] = true
		}
		suite.True(fieldIDs[101], "field 101 bm25 stats should be preserved")
		suite.False(fieldIDs[102], "field 102 bm25 stats should NOT be added for V3 path")
	})

	suite.Run("v3 same manifest and newer task schema accepted", func() {
		manifestPath := packed.MarshalManifestPath("/data/segments/1", 10)
		segs := makeSegments(1, commonpb.SegmentState_Flushed)
		segment := segs.GetSegment(1)
		segment.SchemaVersion = 1
		segment.StorageVersion = storage.StorageV3
		segment.ManifestPath = manifestPath
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: segs,
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
			Schema: &schemapb.CollectionSchema{
				Version: 3,
			},
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:      1,
					InsertLogs:     segment.GetBinlogs(),
					Manifest:       manifestPath,
					BaseManifest:   manifestPath,
					StorageVersion: storage.StorageV3,
				},
			},
		}

		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.NoError(err)
		suite.Require().Len(infos, 1)
		suite.Equal(manifestPath, infos[0].GetManifestPath())
		suite.EqualValues(3, infos[0].GetSchemaVersion())
	})

	suite.Run("v3 in-place adds shipped stats increment", func() {
		currentManifest := packed.MarshalManifestPath("/data/segments/1", 10)
		newerManifest := packed.MarshalManifestPath("/data/segments/1", 11)
		segs := makeSegments(1, commonpb.SegmentState_Flushed)
		segment := segs.GetSegment(1)
		segment.SchemaVersion = 1
		segment.StorageVersion = storage.StorageV3
		segment.ManifestPath = currentManifest
		// Footprint of the segment before materialization appended columns.
		segment.Stats = &datapb.Statistics{InsertBinlogSize: 100, InsertBinlogCount: 1}
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: segs,
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
			Schema:        &schemapb.CollectionSchema{Version: 3},
		}
		// Materialization adds a function-output column and ships the
		// footprint of just that column; the receiver adds it onto the
		// segment's current Stats.
		shippedStats := &datapb.Statistics{InsertBinlogSize: 250, InsertBinlogCount: 2}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:      1,
					InsertLogs:     segment.GetBinlogs(),
					Manifest:       newerManifest,
					BaseManifest:   currentManifest,
					StorageVersion: storage.StorageV3,
					Stats:          shippedStats,
				},
			},
		}

		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.NoError(err)
		suite.Require().Len(infos, 1)
		suite.EqualValues(350, infos[0].GetStats().GetInsertBinlogSize())
		suite.EqualValues(3, infos[0].GetStats().GetInsertBinlogCount())
		suite.EqualValues(350, m.Segments.GetSegment(1).GetSegmentSize())
	})

	suite.Run("v3 same manifest and same task schema accepted", func() {
		manifestPath := packed.MarshalManifestPath("/data/segments/1", 10)
		segs := makeSegments(1, commonpb.SegmentState_Flushed)
		segment := segs.GetSegment(1)
		segment.SchemaVersion = 3
		segment.StorageVersion = storage.StorageV3
		segment.ManifestPath = manifestPath
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: segs,
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
			Schema:        &schemapb.CollectionSchema{Version: 3},
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:      1,
					InsertLogs:     segment.GetBinlogs(),
					Manifest:       manifestPath,
					BaseManifest:   manifestPath,
					StorageVersion: storage.StorageV3,
				},
			},
		}

		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.NoError(err)
		suite.Require().Len(infos, 1)
		suite.Equal(manifestPath, infos[0].GetManifestPath())
		suite.EqualValues(3, infos[0].GetSchemaVersion())
		suite.EqualValues(0, infos[0].GetDataVersion())
	})

	suite.Run("input manifest missing rejected", func() {
		segs := makeSegments(1, commonpb.SegmentState_Flushed)
		segment := segs.GetSegment(1)
		segment.StorageVersion = storage.StorageV3
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: segs,
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
			Schema:        &schemapb.CollectionSchema{Version: 3},
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:      1,
					InsertLogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10001)},
					Manifest:       packed.MarshalManifestPath("/data/segments/1", 11),
					StorageVersion: storage.StorageV3,
				},
			},
		}

		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.ErrorIs(err, merr.ErrIllegalCompactionPlan)
		suite.Nil(infos)
		suite.Empty(m.Segments.GetSegment(1).GetManifestPath())
	})

	suite.Run("replacement zero row result is dropped", func() {
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: makeSegments(1, commonpb.SegmentState_Flushed),
		}
		task := &datapb.CompactionTask{
			InputSegments:          []int64{1},
			Type:                   datapb.CompactionType_BumpSchemaVersionCompaction,
			Schema:                 &schemapb.CollectionSchema{Version: 3},
			PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 2, End: 3},
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:      2,
					NumOfRows:      0,
					Manifest:       "manifest-new",
					StorageVersion: storage.StorageV3,
				},
			},
		}

		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.NoError(err)
		suite.Require().Len(infos, 1)
		suite.Equal(commonpb.SegmentState_Dropped, infos[0].GetState())
		suite.Equal(commonpb.SegmentState_Dropped, m.Segments.GetSegment(2).GetState())
		suite.Equal(commonpb.SegmentState_Dropped, m.Segments.GetSegment(1).GetState())
	})

	suite.Run("catalog error in place does not update memory", func() {
		currentManifest := packed.MarshalManifestPath("/data/segments/1", 10)
		resultManifest := packed.MarshalManifestPath("/data/segments/1", 11)
		segs := makeSegments(1, commonpb.SegmentState_Flushed)
		segment := segs.GetSegment(1)
		segment.StorageVersion = storage.StorageV3
		segment.ManifestPath = currentManifest
		catalogErr := errors.New("catalog error")
		metakv := mockkv.NewMetaKv(suite.T())
		metakv.EXPECT().MultiSave(mock.Anything, mock.Anything).Return(catalogErr).Once()
		m := &meta.Meta{
			Catalog:  datacoord.NewCatalog(metakv, "", ""),
			Segments: segs,
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
			Schema:        &schemapb.CollectionSchema{Version: 3},
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:      1,
					InsertLogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10001)},
					Manifest:       resultManifest,
					BaseManifest:   currentManifest,
					StorageVersion: storage.StorageV3,
				},
			},
		}

		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.ErrorIs(err, catalogErr)
		suite.Nil(infos)
		unchanged := m.Segments.GetSegment(1)
		suite.Equal(currentManifest, unchanged.GetManifestPath())
		suite.EqualValues(1, unchanged.GetSchemaVersion())
		suite.EqualValues(0, unchanged.GetDataVersion())
	})

	suite.Run("catalog error replacement does not update memory", func() {
		catalogErr := errors.New("catalog error")
		metakv := mockkv.NewMetaKv(suite.T())
		metakv.EXPECT().HasPrefix(mock.Anything, mock.Anything).Return(false, nil).Times(3)
		metakv.EXPECT().MultiSave(mock.Anything, mock.Anything).Return(catalogErr).Once()
		m := &meta.Meta{
			Catalog:  datacoord.NewCatalog(metakv, "", ""),
			Segments: makeSegments(1, commonpb.SegmentState_Flushed),
		}
		task := &datapb.CompactionTask{
			InputSegments:          []int64{1},
			Type:                   datapb.CompactionType_BumpSchemaVersionCompaction,
			Schema:                 &schemapb.CollectionSchema{Version: 3},
			PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 2, End: 3},
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:      2,
					NumOfRows:      5,
					InsertLogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10001)},
					Manifest:       "manifest-new",
					StorageVersion: storage.StorageV3,
				},
			},
		}

		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.ErrorIs(err, catalogErr)
		suite.Nil(infos)
		suite.Equal(commonpb.SegmentState_Flushed, m.Segments.GetSegment(1).GetState())
		suite.Nil(m.Segments.GetSegment(2))
	})

	suite.Run("v3 nil task schema rejected", func() {
		currentManifest := packed.MarshalManifestPath("/data/segments/1", 10)
		resultManifest := packed.MarshalManifestPath("/data/segments/1", 11)
		segs := makeSegments(1, commonpb.SegmentState_Flushed)
		segment := segs.GetSegment(1)
		segment.StorageVersion = storage.StorageV3
		segment.ManifestPath = currentManifest
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: segs,
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:      1,
					InsertLogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10001)},
					Manifest:       resultManifest,
					StorageVersion: storage.StorageV3,
				},
			},
		}

		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.ErrorIs(err, merr.ErrIllegalCompactionPlan)
		suite.Nil(infos)
		suite.Equal(currentManifest, m.Segments.GetSegment(1).GetManifestPath())
	})

	suite.Run("v3 stale task schema rejected", func() {
		currentManifest := packed.MarshalManifestPath("/data/segments/1", 10)
		resultManifest := packed.MarshalManifestPath("/data/segments/1", 11)
		segs := makeSegments(1, commonpb.SegmentState_Flushed)
		segment := segs.GetSegment(1)
		segment.StorageVersion = storage.StorageV3
		segment.ManifestPath = currentManifest
		segment.SchemaVersion = 3
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: segs,
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
			Schema:        &schemapb.CollectionSchema{Version: 2},
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:      1,
					InsertLogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10001)},
					Manifest:       resultManifest,
					StorageVersion: storage.StorageV3,
				},
			},
		}

		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.ErrorIs(err, merr.ErrIllegalCompactionPlan)
		suite.Nil(infos)
		suite.Equal(currentManifest, m.Segments.GetSegment(1).GetManifestPath())
	})

	suite.Run("v3 invisible input rejected", func() {
		currentManifest := packed.MarshalManifestPath("/data/segments/1", 10)
		resultManifest := packed.MarshalManifestPath("/data/segments/1", 11)
		segs := makeSegments(1, commonpb.SegmentState_Flushed)
		segment := segs.GetSegment(1)
		segment.StorageVersion = storage.StorageV3
		segment.ManifestPath = currentManifest
		segment.IsInvisible = true
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: segs,
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
			Schema:        &schemapb.CollectionSchema{Version: 3},
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:      1,
					InsertLogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10001)},
					Manifest:       resultManifest,
					StorageVersion: storage.StorageV3,
				},
			},
		}

		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.ErrorIs(err, merr.ErrIllegalCompactionPlan)
		suite.Nil(infos)
		suite.Equal(currentManifest, m.Segments.GetSegment(1).GetManifestPath())
	})

	suite.Run("v3 stale manifest rejected", func() {
		currentManifest := packed.MarshalManifestPath("/data/segments/1", 10)
		resultManifest := packed.MarshalManifestPath("/data/segments/1", 5)
		segs := makeSegments(1, commonpb.SegmentState_Flushed)
		segment := segs.GetSegment(1)
		segment.StorageVersion = storage.StorageV3
		segment.ManifestPath = currentManifest
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: segs,
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
			Schema:        &schemapb.CollectionSchema{Version: 3},
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:      1,
					InsertLogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10001)},
					Manifest:       resultManifest,
					StorageVersion: storage.StorageV3,
				},
			},
		}

		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.ErrorIs(err, merr.ErrIllegalCompactionPlan)
		suite.Nil(infos)
		suite.Equal(currentManifest, m.Segments.GetSegment(1).GetManifestPath())
	})

	suite.Run("v3 different manifest base rejected", func() {
		currentManifest := packed.MarshalManifestPath("/data/segments/1", 10)
		resultManifest := packed.MarshalManifestPath("/data/segments/2", 11)
		segs := makeSegments(1, commonpb.SegmentState_Flushed)
		segment := segs.GetSegment(1)
		segment.StorageVersion = storage.StorageV3
		segment.ManifestPath = currentManifest
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: segs,
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
			Schema:        &schemapb.CollectionSchema{Version: 3},
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:      1,
					InsertLogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10001)},
					Manifest:       resultManifest,
					StorageVersion: storage.StorageV3,
				},
			},
		}

		infos, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
		suite.ErrorIs(err, merr.ErrIllegalCompactionPlan)
		suite.Nil(infos)
		suite.Equal(currentManifest, m.Segments.GetSegment(1).GetManifestPath())
	})

	suite.Run("post-restart segment accumulates successive increments", func() {
		// The recovered StorageV3 shape: per-field binlog KVs were skipped, so
		// Binlogs/Deltalogs come back empty, and Stats is the only durable
		// record of the segment's footprint. Two successive materializations
		// must both land on top of it.
		segs := meta.NewSegmentsInfo(ms)
		segs.SetSegment(1, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:             1,
			CollectionID:   100,
			PartitionID:    10,
			State:          commonpb.SegmentState_Flushed,
			Level:          datapb.SegmentLevel_L1,
			NumOfRows:      5,
			SchemaVersion:  1,
			StorageVersion: storage.StorageV3,
			ManifestPath:   packed.MarshalManifestPath("/data/segments/1", 10),
			Binlogs:        nil, // recovered V3: arrays are gone
			Deltalogs:      nil,
			Stats: &datapb.Statistics{
				InsertBinlogSize:  10_000,
				InsertBinlogCount: 8,
				StatsBinlogSize:   100,
				DeleteNumRows:     42,
				NullCounts:        map[int64]int64{100: 0},
			},
		}})
		m := &meta.Meta{
			Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
			Segments: segs,
		}
		task := &datapb.CompactionTask{
			InputSegments: []int64{1},
			Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
			Schema:        &schemapb.CollectionSchema{Version: 3},
		}

		apply := func(baseVer, resultVer int64, fieldID int64, size int64) {
			result := &datapb.CompactionPlanResult{
				Segments: []*datapb.CompactionSegment{
					{
						SegmentID:      1,
						NumOfRows:      5,
						Manifest:       packed.MarshalManifestPath("/data/segments/1", resultVer),
						BaseManifest:   packed.MarshalManifestPath("/data/segments/1", baseVer),
						StorageVersion: storage.StorageV3,
						Stats: &datapb.Statistics{
							InsertBinlogSize:  size,
							InsertBinlogCount: 1,
							StatsBinlogSize:   10,
							NullCounts:        map[int64]int64{fieldID: 0},
						},
					},
				},
			}
			_, err := m.CompleteBumpSchemaVersionCompactionMutation(task, result)
			suite.NoError(err)
		}

		apply(10, 11, 102, 2_000)
		apply(11, 12, 103, 3_000)

		got := m.Segments.GetSegment(1).GetStats()
		suite.EqualValues(15_000, got.GetInsertBinlogSize())
		suite.EqualValues(10, got.GetInsertBinlogCount())
		suite.EqualValues(120, got.GetStatsBinlogSize())
		suite.EqualValues(42, got.GetDeleteNumRows())
		suite.Equal(map[int64]int64{100: 0, 102: 0, 103: 0}, got.GetNullCounts())
	})
}

func (suite *MetaBasicSuite) TestCompleteCompactionMutation_DispatchesBumpSchemaVersion() {
	ms := metacache.NewMetaStore(nil)
	manifestPath := packed.MarshalManifestPath("/data/segments/1", 10)
	segs := meta.NewSegmentsInfo(ms)
	segs.SetSegment(1, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:             1,
		CollectionID:   100,
		PartitionID:    10,
		State:          commonpb.SegmentState_Flushed,
		Level:          datapb.SegmentLevel_L1,
		Binlogs:        []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10000)},
		NumOfRows:      5,
		SchemaVersion:  1,
		StorageVersion: storage.StorageV3,
		ManifestPath:   manifestPath,
	}})
	m := &meta.Meta{
		Catalog:  &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()},
		Segments: segs,
	}
	task := &datapb.CompactionTask{
		InputSegments: []int64{1},
		Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
		Schema:        &schemapb.CollectionSchema{Version: 3},
	}
	result := &datapb.CompactionPlanResult{
		Segments: []*datapb.CompactionSegment{
			{
				SegmentID:      1,
				InsertLogs:     []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10001)},
				Manifest:       manifestPath,
				BaseManifest:   manifestPath,
				StorageVersion: storage.StorageV3,
			},
		},
	}

	infos, err := m.CompleteCompactionMutation(context.TODO(), task, result)
	suite.NoError(err)
	suite.Require().Len(infos, 1)
	suite.EqualValues(task.GetSchema().GetVersion(), infos[0].GetSchemaVersion())
}

func TestMeta(t *testing.T) {
	suite.Run(t, new(MetaBasicSuite))
	suite.Run(t, new(MetaReloadSuite))
}

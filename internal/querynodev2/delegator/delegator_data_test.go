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

package delegator

import (
	"context"
	"fmt"
	"path"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/pingcap/log"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/bloomfilter"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type DelegatorDataSuite struct {
	suite.Suite

	collectionID  int64
	replicaID     int64
	vchannelName  string
	version       int64
	workerManager *cluster.MockManager
	manager       *segments.Manager
	loader        *segments.MockLoader
	mq            *msgstream.MockMsgStream
	channel       metautil.Channel
	mapper        metautil.ChannelMapper

	delegator    *shardDelegator
	rootPath     string
	chunkManager storage.ChunkManager
}

func (s *DelegatorDataSuite) SetupSuite() {
	paramtable.Init()
	paramtable.SetNodeID(1)
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.CleanExcludeSegInterval.Key, "1")
	localDataRootPath := filepath.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), typeutil.QueryNodeRole)
	initcore.InitLocalChunkManager(localDataRootPath)
	initcore.InitMmapManager(paramtable.Get())

	s.collectionID = 1000
	s.replicaID = 65535
	s.vchannelName = "rootcoord-dml_1000v0"
	s.version = 2000
	var err error
	s.mapper = metautil.NewDynChannelMapper()
	s.channel, err = metautil.ParseChannel(s.vchannelName, s.mapper)
	s.Require().NoError(err)
}

func (s *DelegatorDataSuite) TearDownSuite() {
	paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.CleanExcludeSegInterval.Key)
}

func (s *DelegatorDataSuite) genNormalCollection() {
	s.manager.Collection.PutOrRef(s.collectionID, &schemapb.CollectionSchema{
		Name: "TestCollection",
		Fields: []*schemapb.FieldSchema{
			{
				Name:         "id",
				FieldID:      100,
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
				AutoID:       true,
			},
			{
				Name:         "vector",
				FieldID:      101,
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_BinaryVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "128",
					},
				},
			},
		},
	}, &segcorepb.CollectionIndexMeta{
		MaxIndexRowCount: 100,
		IndexMetas: []*segcorepb.FieldIndexMeta{
			{
				FieldID:      101,
				CollectionID: s.collectionID,
				IndexName:    "binary_index",
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "128",
					},
				},
				IndexParams: []*commonpb.KeyValuePair{
					{
						Key:   common.IndexTypeKey,
						Value: "BIN_IVF_FLAT",
					},
					{
						Key:   common.MetricTypeKey,
						Value: metric.JACCARD,
					},
				},
			},
		},
	}, &querypb.LoadMetaInfo{
		LoadType:     querypb.LoadType_LoadCollection,
		PartitionIDs: []int64{1001, 1002},
	})
}

func (s *DelegatorDataSuite) genCollectionWithFunction() {
	s.manager.Collection.PutOrRef(s.collectionID, &schemapb.CollectionSchema{
		Name: "TestCollection",
		Fields: []*schemapb.FieldSchema{
			{
				Name:         "id",
				FieldID:      100,
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
				AutoID:       true,
			}, {
				Name:         "vector",
				FieldID:      101,
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_SparseFloatVector,
			}, {
				Name:     "text",
				FieldID:  102,
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.MaxLengthKey,
						Value: "256",
					},
				},
			},
		},
		Functions: []*schemapb.FunctionSchema{{
			Type:           schemapb.FunctionType_BM25,
			InputFieldIds:  []int64{102},
			OutputFieldIds: []int64{101},
		}},
	}, nil, nil)

	delegator, err := NewShardDelegator(context.Background(), s.collectionID, s.replicaID, s.vchannelName, s.version, s.workerManager, s.manager, s.loader, &msgstream.MockMqFactory{
		NewMsgStreamFunc: func(_ context.Context) (msgstream.MsgStream, error) {
			return s.mq, nil
		},
	}, 10000, nil, s.chunkManager)
	s.NoError(err)
	s.delegator = delegator.(*shardDelegator)
}

func (s *DelegatorDataSuite) SetupTest() {
	s.workerManager = &cluster.MockManager{}
	s.manager = segments.NewManager()
	s.loader = &segments.MockLoader{}

	// init schema
	s.genNormalCollection()
	s.mq = &msgstream.MockMsgStream{}
	s.rootPath = s.Suite.T().Name()
	chunkManagerFactory := storage.NewTestChunkManagerFactory(paramtable.Get(), s.rootPath)
	s.chunkManager, _ = chunkManagerFactory.NewPersistentStorageChunkManager(context.Background())
	delegator, err := NewShardDelegator(context.Background(), s.collectionID, s.replicaID, s.vchannelName, s.version, s.workerManager, s.manager, s.loader, &msgstream.MockMqFactory{
		NewMsgStreamFunc: func(_ context.Context) (msgstream.MsgStream, error) {
			return s.mq, nil
		},
	}, 10000, nil, s.chunkManager)
	s.Require().NoError(err)
	sd, ok := delegator.(*shardDelegator)
	s.Require().True(ok)
	s.delegator = sd
}

func (s *DelegatorDataSuite) TestProcessInsert() {
	s.Run("normal_insert", func() {
		s.delegator.ProcessInsert(map[int64]*InsertData{
			100: {
				RowIDs:        []int64{0, 1},
				PrimaryKeys:   []storage.PrimaryKey{storage.NewInt64PrimaryKey(1), storage.NewInt64PrimaryKey(2)},
				Timestamps:    []uint64{10, 10},
				PartitionID:   500,
				StartPosition: &msgpb.MsgPosition{},
				InsertRecord: &segcorepb.InsertRecord{
					FieldsData: []*schemapb.FieldData{
						{
							Type:      schemapb.DataType_Int64,
							FieldName: "id",
							Field: &schemapb.FieldData_Scalars{
								Scalars: &schemapb.ScalarField{
									Data: &schemapb.ScalarField_LongData{
										LongData: &schemapb.LongArray{
											Data: []int64{1, 2},
										},
									},
								},
							},
							FieldId: 100,
						},
						{
							Type:      schemapb.DataType_FloatVector,
							FieldName: "vector",
							Field: &schemapb.FieldData_Vectors{
								Vectors: &schemapb.VectorField{
									Dim: 128,
									Data: &schemapb.VectorField_FloatVector{
										FloatVector: &schemapb.FloatArray{Data: make([]float32, 128*2)},
									},
								},
							},
							FieldId: 101,
						},
					},
					NumRows: 2,
				},
			},
		})

		s.NotNil(s.manager.Segment.GetGrowing(100))
	})

	s.Run("insert_bad_data", func() {
		s.Panics(func() {
			s.delegator.ProcessInsert(map[int64]*InsertData{
				100: {
					RowIDs:        []int64{0, 1},
					PrimaryKeys:   []storage.PrimaryKey{storage.NewInt64PrimaryKey(1), storage.NewInt64PrimaryKey(2)},
					Timestamps:    []uint64{10, 10},
					PartitionID:   500,
					StartPosition: &msgpb.MsgPosition{},
					InsertRecord: &segcorepb.InsertRecord{
						FieldsData: []*schemapb.FieldData{
							{
								Type:      schemapb.DataType_Int64,
								FieldName: "id",
								Field: &schemapb.FieldData_Scalars{
									Scalars: &schemapb.ScalarField{
										Data: &schemapb.ScalarField_LongData{
											LongData: &schemapb.LongArray{
												Data: []int64{1, 2},
											},
										},
									},
								},
								FieldId: 100,
							},
						},
						NumRows: 2,
					},
				},
			})
		})
	})
}

func (s *DelegatorDataSuite) TestProcessDelete() {
	s.loader.EXPECT().
		Load(mock.Anything, s.collectionID, segments.SegmentTypeGrowing, int64(0), mock.Anything).
		Call.Return(func(ctx context.Context, collectionID int64, segmentType segments.SegmentType, version int64, infos ...*querypb.SegmentLoadInfo) []segments.Segment {
		return lo.Map(infos, func(info *querypb.SegmentLoadInfo, _ int) segments.Segment {
			ms := &segments.MockSegment{}
			ms.EXPECT().ID().Return(info.GetSegmentID())
			ms.EXPECT().Type().Return(segments.SegmentTypeGrowing)
			ms.EXPECT().Collection().Return(info.GetCollectionID())
			ms.EXPECT().Partition().Return(info.GetPartitionID())
			ms.EXPECT().Indexes().Return(nil)
			ms.EXPECT().RowNum().Return(info.GetNumOfRows())
			ms.EXPECT().Delete(mock.Anything, mock.Anything, mock.Anything).Return(nil)
			ms.EXPECT().MayPkExist(mock.Anything).RunAndReturn(func(lc *storage.LocationsCache) bool {
				return lc.GetPk().EQ(storage.NewInt64PrimaryKey(10))
			})
			ms.EXPECT().BatchPkExist(mock.Anything).RunAndReturn(func(lc *storage.BatchLocationsCache) []bool {
				hits := make([]bool, lc.Size())
				for i, pk := range lc.PKs() {
					hits[i] = pk.EQ(storage.NewInt64PrimaryKey(10))
				}
				return hits
			})
			return ms
		})
	}, nil)
	s.loader.EXPECT().LoadBloomFilterSet(mock.Anything, s.collectionID, mock.AnythingOfType("int64"), mock.Anything).
		Call.Return(func(ctx context.Context, collectionID int64, version int64, infos ...*querypb.SegmentLoadInfo) []*pkoracle.BloomFilterSet {
		return lo.Map(infos, func(info *querypb.SegmentLoadInfo, _ int) *pkoracle.BloomFilterSet {
			bfs := pkoracle.NewBloomFilterSet(info.GetSegmentID(), info.GetPartitionID(), commonpb.SegmentState_Sealed)
			bf := bloomfilter.NewBloomFilterWithType(paramtable.Get().CommonCfg.BloomFilterSize.GetAsUint(),
				paramtable.Get().CommonCfg.MaxBloomFalsePositive.GetAsFloat(),
				paramtable.Get().CommonCfg.BloomFilterType.GetValue())
			pks := &storage.PkStatistics{
				PkFilter: bf,
			}
			pks.UpdatePKRange(&storage.Int64FieldData{
				Data: []int64{10, 20, 30},
			})
			bfs.AddHistoricalStats(pks)
			return bfs
		})
	}, func(ctx context.Context, collectionID int64, version int64, infos ...*querypb.SegmentLoadInfo) error {
		return nil
	})

	workers := make(map[int64]*cluster.MockWorker)
	worker1 := &cluster.MockWorker{}
	workers[1] = worker1

	worker1.EXPECT().LoadSegments(mock.Anything, mock.AnythingOfType("*querypb.LoadSegmentsRequest")).
		Return(nil)
	worker1.EXPECT().Delete(mock.Anything, mock.AnythingOfType("*querypb.DeleteRequest")).Return(nil)
	s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
		return workers[nodeID]
	}, nil)
	// load growing
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := s.delegator.LoadGrowing(ctx, []*querypb.SegmentLoadInfo{
		{
			SegmentID:     1001,
			CollectionID:  s.collectionID,
			PartitionID:   500,
			InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", s.collectionID),
		},
	}, 0)
	s.Require().NoError(err)
	// load sealed
	s.delegator.LoadSegments(ctx, &querypb.LoadSegmentsRequest{
		Base:         commonpbutil.NewMsgBase(),
		DstNodeID:    1,
		CollectionID: s.collectionID,
		Infos: []*querypb.SegmentLoadInfo{
			{
				SegmentID:     1000,
				CollectionID:  s.collectionID,
				PartitionID:   500,
				StartPosition: &msgpb.MsgPosition{Timestamp: 20000},
				DeltaPosition: &msgpb.MsgPosition{Timestamp: 20000},
				InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", s.collectionID),
			},
		},
	})
	s.Require().NoError(err)

	s.delegator.ProcessDelete([]*DeleteData{
		{
			PartitionID: 500,
			PrimaryKeys: []storage.PrimaryKey{storage.NewInt64PrimaryKey(10)},
			Timestamps:  []uint64{10},
			RowCount:    1,
		},
	}, 10)

	// load sealed
	s.delegator.LoadSegments(ctx, &querypb.LoadSegmentsRequest{
		Base:         commonpbutil.NewMsgBase(),
		DstNodeID:    1,
		CollectionID: s.collectionID,
		Infos: []*querypb.SegmentLoadInfo{
			{
				SegmentID:     1000,
				CollectionID:  s.collectionID,
				PartitionID:   500,
				StartPosition: &msgpb.MsgPosition{Timestamp: 5000},
				DeltaPosition: &msgpb.MsgPosition{Timestamp: 5000},
				InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", s.collectionID),
			},
		},
	})
	s.Require().NoError(err)

	s.delegator.ProcessDelete([]*DeleteData{
		{
			PartitionID: 500,
			PrimaryKeys: []storage.PrimaryKey{storage.NewInt64PrimaryKey(10)},
			Timestamps:  []uint64{10},
			RowCount:    1,
		},
	}, 10)
	s.True(s.delegator.distribution.Serviceable())

	// test worker return segment not loaded
	worker1.ExpectedCalls = nil
	worker1.EXPECT().Delete(mock.Anything, mock.Anything).Return(merr.ErrSegmentNotLoaded)
	s.delegator.ProcessDelete([]*DeleteData{
		{
			PartitionID: 500,
			PrimaryKeys: []storage.PrimaryKey{storage.NewInt64PrimaryKey(10)},
			Timestamps:  []uint64{10},
			RowCount:    1,
		},
	}, 10)
	s.True(s.delegator.distribution.Serviceable(), "segment not loaded shall not trigger offline")

	// test worker offline
	worker1.ExpectedCalls = nil
	worker1.EXPECT().Delete(mock.Anything, mock.Anything).Return(merr.ErrNodeNotFound)
	s.delegator.ProcessDelete([]*DeleteData{
		{
			PartitionID: 500,
			PrimaryKeys: []storage.PrimaryKey{storage.NewInt64PrimaryKey(10)},
			Timestamps:  []uint64{10},
			RowCount:    1,
		},
	}, 10)

	s.False(s.delegator.distribution.Serviceable())

	worker1.EXPECT().LoadSegments(mock.Anything, mock.AnythingOfType("*querypb.LoadSegmentsRequest")).
		Return(nil)
	// reload, refresh the state
	s.delegator.LoadSegments(ctx, &querypb.LoadSegmentsRequest{
		Base:         commonpbutil.NewMsgBase(),
		DstNodeID:    1,
		CollectionID: s.collectionID,
		Infos: []*querypb.SegmentLoadInfo{
			{
				SegmentID:     1000,
				CollectionID:  s.collectionID,
				PartitionID:   500,
				StartPosition: &msgpb.MsgPosition{Timestamp: 20000},
				DeltaPosition: &msgpb.MsgPosition{Timestamp: 20000},
				InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", s.collectionID),
			},
		},
		Version: 1,
	})
	s.Require().NoError(err)
	s.True(s.delegator.distribution.Serviceable())
	// Test normal errors with retry and fail
	worker1.ExpectedCalls = nil
	worker1.EXPECT().Delete(mock.Anything, mock.Anything).Return(merr.ErrSegcore)
	s.delegator.ProcessDelete([]*DeleteData{
		{
			PartitionID: 500,
			PrimaryKeys: []storage.PrimaryKey{storage.NewInt64PrimaryKey(10)},
			Timestamps:  []uint64{10},
			RowCount:    1,
		},
	}, 10)
	s.False(s.delegator.distribution.Serviceable(), "should retry and failed")

	// refresh
	worker1.EXPECT().LoadSegments(mock.Anything, mock.AnythingOfType("*querypb.LoadSegmentsRequest")).
		Return(nil)
	// reload, refresh the state
	s.delegator.LoadSegments(ctx, &querypb.LoadSegmentsRequest{
		Base:         commonpbutil.NewMsgBase(),
		DstNodeID:    1,
		CollectionID: s.collectionID,
		Infos: []*querypb.SegmentLoadInfo{
			{
				SegmentID:     1000,
				CollectionID:  s.collectionID,
				PartitionID:   500,
				StartPosition: &msgpb.MsgPosition{Timestamp: 20000},
				DeltaPosition: &msgpb.MsgPosition{Timestamp: 20000},
				InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", s.collectionID),
			},
		},
		Version: 2,
	})
	s.Require().NoError(err)
	s.True(s.delegator.distribution.Serviceable())

	s.delegator.Close()
	s.delegator.ProcessDelete([]*DeleteData{
		{
			PartitionID: 500,
			PrimaryKeys: []storage.PrimaryKey{storage.NewInt64PrimaryKey(10)},
			Timestamps:  []uint64{10},
			RowCount:    1,
		},
	}, 10)
	s.Require().NoError(err)
	s.False(s.delegator.distribution.Serviceable())
}

func (s *DelegatorDataSuite) TestLoadGrowingWithBM25() {
	s.genCollectionWithFunction()
	mockSegment := segments.NewMockSegment(s.T())
	s.loader.EXPECT().Load(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]segments.Segment{mockSegment}, nil)

	mockSegment.EXPECT().Partition().Return(111)
	mockSegment.EXPECT().ID().Return(111)
	mockSegment.EXPECT().Type().Return(commonpb.SegmentState_Growing)
	mockSegment.EXPECT().GetBM25Stats().Return(map[int64]*storage.BM25Stats{})

	err := s.delegator.LoadGrowing(context.Background(), []*querypb.SegmentLoadInfo{{SegmentID: 1}}, 1)
	s.NoError(err)
}

func (s *DelegatorDataSuite) TestLoadSegmentsWithBm25() {
	s.genCollectionWithFunction()
	s.Run("normal_run", func() {
		defer func() {
			s.workerManager.ExpectedCalls = nil
			s.loader.ExpectedCalls = nil
		}()

		statsMap := typeutil.NewConcurrentMap[int64, map[int64]*storage.BM25Stats]()
		stats := storage.NewBM25Stats()
		stats.Append(map[uint32]float32{1: 1})

		statsMap.Insert(1, map[int64]*storage.BM25Stats{101: stats})

		s.loader.EXPECT().LoadBM25Stats(mock.Anything, s.collectionID, mock.Anything).Return(statsMap, nil)
		s.loader.EXPECT().LoadBloomFilterSet(mock.Anything, s.collectionID, mock.AnythingOfType("int64"), mock.Anything).
			Call.Return(func(ctx context.Context, collectionID int64, version int64, infos ...*querypb.SegmentLoadInfo) []*pkoracle.BloomFilterSet {
			return lo.Map(infos, func(info *querypb.SegmentLoadInfo, _ int) *pkoracle.BloomFilterSet {
				return pkoracle.NewBloomFilterSet(info.GetSegmentID(), info.GetPartitionID(), commonpb.SegmentState_Sealed)
			})
		}, func(ctx context.Context, collectionID int64, version int64, infos ...*querypb.SegmentLoadInfo) error {
			return nil
		})

		workers := make(map[int64]*cluster.MockWorker)
		worker1 := &cluster.MockWorker{}
		workers[1] = worker1

		worker1.EXPECT().LoadSegments(mock.Anything, mock.AnythingOfType("*querypb.LoadSegmentsRequest")).
			Return(nil)
		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := s.delegator.LoadSegments(ctx, &querypb.LoadSegmentsRequest{
			Base:         commonpbutil.NewMsgBase(),
			DstNodeID:    1,
			CollectionID: s.collectionID,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:     100,
					PartitionID:   500,
					StartPosition: &msgpb.MsgPosition{Timestamp: 20000},
					DeltaPosition: &msgpb.MsgPosition{Timestamp: 20000},
					Level:         datapb.SegmentLevel_L1,
					InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", s.collectionID),
				},
			},
		})

		s.NoError(err)
		sealed, _ := s.delegator.GetSegmentInfo(false)
		s.Require().Equal(1, len(sealed))
		s.Equal(int64(1), sealed[0].NodeID)
		s.ElementsMatch([]SegmentEntry{
			{
				SegmentID:     100,
				NodeID:        1,
				PartitionID:   500,
				TargetVersion: unreadableTargetVersion,
				Level:         datapb.SegmentLevel_L1,
			},
		}, sealed[0].Segments)
	})

	s.Run("loadBM25_failed", func() {
		defer func() {
			s.workerManager.ExpectedCalls = nil
			s.loader.ExpectedCalls = nil
		}()

		s.loader.EXPECT().LoadBM25Stats(mock.Anything, s.collectionID, mock.Anything).Return(nil, fmt.Errorf("mock error"))

		workers := make(map[int64]*cluster.MockWorker)
		worker1 := &cluster.MockWorker{}
		workers[1] = worker1

		worker1.EXPECT().LoadSegments(mock.Anything, mock.AnythingOfType("*querypb.LoadSegmentsRequest")).
			Return(nil)
		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := s.delegator.LoadSegments(ctx, &querypb.LoadSegmentsRequest{
			Base:         commonpbutil.NewMsgBase(),
			DstNodeID:    1,
			CollectionID: s.collectionID,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:     100,
					PartitionID:   500,
					StartPosition: &msgpb.MsgPosition{Timestamp: 20000},
					DeltaPosition: &msgpb.MsgPosition{Timestamp: 20000},
					Level:         datapb.SegmentLevel_L1,
					InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", s.collectionID),
				},
			},
		})

		s.Error(err)
	})
}

func (s *DelegatorDataSuite) TestLoadSegments() {
	s.Run("normal_run", func() {
		defer func() {
			s.workerManager.ExpectedCalls = nil
			s.loader.ExpectedCalls = nil
		}()

		s.loader.EXPECT().LoadBloomFilterSet(mock.Anything, s.collectionID, mock.AnythingOfType("int64"), mock.Anything).
			Call.Return(func(ctx context.Context, collectionID int64, version int64, infos ...*querypb.SegmentLoadInfo) []*pkoracle.BloomFilterSet {
			return lo.Map(infos, func(info *querypb.SegmentLoadInfo, _ int) *pkoracle.BloomFilterSet {
				return pkoracle.NewBloomFilterSet(info.GetSegmentID(), info.GetPartitionID(), commonpb.SegmentState_Sealed)
			})
		}, func(ctx context.Context, collectionID int64, version int64, infos ...*querypb.SegmentLoadInfo) error {
			return nil
		})

		workers := make(map[int64]*cluster.MockWorker)
		worker1 := &cluster.MockWorker{}
		workers[1] = worker1

		worker1.EXPECT().LoadSegments(mock.Anything, mock.AnythingOfType("*querypb.LoadSegmentsRequest")).
			Return(nil)
		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := s.delegator.LoadSegments(ctx, &querypb.LoadSegmentsRequest{
			Base:         commonpbutil.NewMsgBase(),
			DstNodeID:    1,
			CollectionID: s.collectionID,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:     100,
					PartitionID:   500,
					StartPosition: &msgpb.MsgPosition{Timestamp: 20000},
					DeltaPosition: &msgpb.MsgPosition{Timestamp: 20000},
					Level:         datapb.SegmentLevel_L1,
					InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", s.collectionID),
				},
			},
		})

		s.NoError(err)
		sealed, _ := s.delegator.GetSegmentInfo(false)
		s.Require().Equal(1, len(sealed))
		s.Equal(int64(1), sealed[0].NodeID)
		s.ElementsMatch([]SegmentEntry{
			{
				SegmentID:     100,
				NodeID:        1,
				PartitionID:   500,
				TargetVersion: unreadableTargetVersion,
				Level:         datapb.SegmentLevel_L1,
			},
		}, sealed[0].Segments)
	})

	s.Run("load_segments_with_delete", func() {
		defer func() {
			s.workerManager.ExpectedCalls = nil
			s.loader.ExpectedCalls = nil
		}()

		s.loader.EXPECT().LoadBloomFilterSet(mock.Anything, s.collectionID, mock.AnythingOfType("int64"), mock.Anything).
			Call.Return(func(ctx context.Context, collectionID int64, version int64, infos ...*querypb.SegmentLoadInfo) []*pkoracle.BloomFilterSet {
			return lo.Map(infos, func(info *querypb.SegmentLoadInfo, _ int) *pkoracle.BloomFilterSet {
				bfs := pkoracle.NewBloomFilterSet(info.GetSegmentID(), info.GetPartitionID(), commonpb.SegmentState_Sealed)
				bf := bloomfilter.NewBloomFilterWithType(
					paramtable.Get().CommonCfg.BloomFilterSize.GetAsUint(),
					paramtable.Get().CommonCfg.MaxBloomFalsePositive.GetAsFloat(),
					paramtable.Get().CommonCfg.BloomFilterType.GetValue())
				pks := &storage.PkStatistics{
					PkFilter: bf,
				}
				pks.UpdatePKRange(&storage.Int64FieldData{
					Data: []int64{10, 20, 30},
				})
				bfs.AddHistoricalStats(pks)
				return bfs
			})
		}, func(ctx context.Context, collectionID int64, version int64, infos ...*querypb.SegmentLoadInfo) error {
			return nil
		})

		workers := make(map[int64]*cluster.MockWorker)
		worker1 := &cluster.MockWorker{}
		workers[1] = worker1

		worker1.EXPECT().LoadSegments(mock.Anything, mock.AnythingOfType("*querypb.LoadSegmentsRequest")).
			Return(nil)
		worker1.EXPECT().Delete(mock.Anything, mock.AnythingOfType("*querypb.DeleteRequest")).Return(nil)
		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil)

		s.delegator.ProcessDelete([]*DeleteData{
			{
				PartitionID: 500,
				PrimaryKeys: []storage.PrimaryKey{
					storage.NewInt64PrimaryKey(1),
					storage.NewInt64PrimaryKey(10),
				},
				Timestamps: []uint64{10, 10},
				RowCount:   2,
			},
		}, 10)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err := s.delegator.LoadSegments(ctx, &querypb.LoadSegmentsRequest{
			Base:         commonpbutil.NewMsgBase(),
			DstNodeID:    1,
			CollectionID: s.collectionID,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:     200,
					PartitionID:   500,
					StartPosition: &msgpb.MsgPosition{Timestamp: 20000},
					DeltaPosition: &msgpb.MsgPosition{Timestamp: 20000},
					Deltalogs:     []*datapb.FieldBinlog{},
					Level:         datapb.SegmentLevel_L0,
					InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", s.collectionID),
				},
			},
		})
		s.NoError(err)

		// err = s.delegator.LoadSegments(ctx, &querypb.LoadSegmentsRequest{
		// 	Base:         commonpbutil.NewMsgBase(),
		// 	DstNodeID:    1,
		// 	CollectionID: s.collectionID,
		// 	Infos: []*querypb.SegmentLoadInfo{
		// 		{
		// 			SegmentID:     200,
		// 			PartitionID:   500,
		// 			StartPosition: &msgpb.MsgPosition{Timestamp: 20000},
		// 			DeltaPosition: &msgpb.MsgPosition{Timestamp: 20000},
		// 			Level:         datapb.SegmentLevel_L1,
		// 			InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", s.collectionID),
		// 		},
		// 	},
		// })

		s.NoError(err)
		sealed, _ := s.delegator.GetSegmentInfo(false)
		s.Require().Equal(1, len(sealed))
		s.Equal(int64(1), sealed[0].NodeID)
		s.ElementsMatch([]SegmentEntry{
			{
				SegmentID:     100,
				NodeID:        1,
				PartitionID:   500,
				TargetVersion: unreadableTargetVersion,
				Level:         datapb.SegmentLevel_L1,
			},
			{
				SegmentID:     200,
				NodeID:        1,
				PartitionID:   500,
				TargetVersion: unreadableTargetVersion,
				Level:         datapb.SegmentLevel_L0,
			},
		}, sealed[0].Segments)
	})

	s.Run("load_segments_with_l0_delete_failed", func() {
		s.T().Skip("skip this test for now")
		defer func() {
			s.workerManager.ExpectedCalls = nil
			s.loader.ExpectedCalls = nil
		}()

		delegator, err := NewShardDelegator(
			context.Background(),
			s.collectionID,
			s.replicaID,
			s.vchannelName,
			s.version,
			s.workerManager,
			s.manager,
			s.loader,
			&msgstream.MockMqFactory{
				NewMsgStreamFunc: func(_ context.Context) (msgstream.MsgStream, error) {
					return s.mq, nil
				},
			}, 10000, nil, nil)
		s.NoError(err)

		growing0 := segments.NewMockSegment(s.T())
		growing0.EXPECT().ID().Return(1)
		growing0.EXPECT().Partition().Return(10)
		growing0.EXPECT().Type().Return(segments.SegmentTypeGrowing)
		growing0.EXPECT().Release(context.Background())

		growing1 := segments.NewMockSegment(s.T())
		growing1.EXPECT().ID().Return(2)
		growing1.EXPECT().Partition().Return(10)
		growing1.EXPECT().Type().Return(segments.SegmentTypeGrowing)
		growing1.EXPECT().Release(context.Background())

		mockErr := merr.WrapErrServiceInternal("mock")

		growing0.EXPECT().Delete(mock.Anything, mock.Anything, mock.Anything).Return(nil)
		growing1.EXPECT().Delete(mock.Anything, mock.Anything, mock.Anything).Return(mockErr)

		s.loader.EXPECT().Load(
			mock.Anything,
			mock.Anything,
			segments.SegmentTypeGrowing,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return([]segments.Segment{growing0, growing1}, nil)

		err = delegator.LoadGrowing(context.Background(), []*querypb.SegmentLoadInfo{{}, {}}, 100)
		s.ErrorIs(err, mockErr)
	})

	s.Run("load_segments_with_streaming_delete_failed", func() {
		defer func() {
			s.workerManager.ExpectedCalls = nil
			s.loader.ExpectedCalls = nil
		}()

		s.loader.EXPECT().LoadBloomFilterSet(mock.Anything, s.collectionID, mock.AnythingOfType("int64"), mock.Anything).
			Call.Return(func(ctx context.Context, collectionID int64, version int64, infos ...*querypb.SegmentLoadInfo) []*pkoracle.BloomFilterSet {
			return lo.Map(infos, func(info *querypb.SegmentLoadInfo, _ int) *pkoracle.BloomFilterSet {
				bfs := pkoracle.NewBloomFilterSet(info.GetSegmentID(), info.GetPartitionID(), commonpb.SegmentState_Sealed)
				bf := bloomfilter.NewBloomFilterWithType(
					paramtable.Get().CommonCfg.BloomFilterSize.GetAsUint(),
					paramtable.Get().CommonCfg.MaxBloomFalsePositive.GetAsFloat(),
					paramtable.Get().CommonCfg.BloomFilterType.GetValue())
				pks := &storage.PkStatistics{
					PkFilter: bf,
				}
				pks.UpdatePKRange(&storage.Int64FieldData{
					Data: []int64{10, 20, 30},
				})
				bfs.AddHistoricalStats(pks)
				return bfs
			})
		}, func(ctx context.Context, collectionID int64, version int64, infos ...*querypb.SegmentLoadInfo) error {
			return nil
		})

		workers := make(map[int64]*cluster.MockWorker)
		worker1 := &cluster.MockWorker{}
		workers[1] = worker1

		worker1.EXPECT().LoadSegments(mock.Anything, mock.AnythingOfType("*querypb.LoadSegmentsRequest")).
			Return(nil)
		worker1.EXPECT().Delete(mock.Anything, mock.AnythingOfType("*querypb.DeleteRequest")).Return(errors.New("mocked"))
		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil)

		s.delegator.ProcessDelete([]*DeleteData{
			{
				PartitionID: 500,
				PrimaryKeys: []storage.PrimaryKey{
					storage.NewInt64PrimaryKey(1),
					storage.NewInt64PrimaryKey(10),
				},
				Timestamps: []uint64{10, 10},
				RowCount:   2,
			},
		}, 10)

		s.mq.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		s.mq.EXPECT().Seek(mock.Anything, mock.Anything, mock.Anything).Return(nil)
		s.mq.EXPECT().Close()
		ch := make(chan *msgstream.MsgPack, 10)
		close(ch)

		s.mq.EXPECT().Chan().Return(ch)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := s.delegator.LoadSegments(ctx, &querypb.LoadSegmentsRequest{
			Base:         commonpbutil.NewMsgBase(),
			DstNodeID:    1,
			CollectionID: s.collectionID,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:     300,
					PartitionID:   500,
					StartPosition: &msgpb.MsgPosition{Timestamp: 2},
					DeltaPosition: &msgpb.MsgPosition{Timestamp: 2},
					InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", s.collectionID),
				},
			},
		})
		s.Error(err)
	})

	s.Run("get_worker_fail", func() {
		defer func() {
			s.workerManager.ExpectedCalls = nil
			s.loader.ExpectedCalls = nil
		}()

		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Return(nil, errors.New("mock error"))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := s.delegator.LoadSegments(ctx, &querypb.LoadSegmentsRequest{
			Base:         commonpbutil.NewMsgBase(),
			DstNodeID:    1,
			CollectionID: s.collectionID,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:     100,
					PartitionID:   500,
					StartPosition: &msgpb.MsgPosition{Timestamp: 20000},
					DeltaPosition: &msgpb.MsgPosition{Timestamp: 20000},
					InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", s.collectionID),
				},
			},
		})

		s.Error(err)
	})

	s.Run("loader_bfs_fail", func() {
		defer func() {
			s.workerManager.ExpectedCalls = nil
			s.loader.ExpectedCalls = nil
		}()

		s.loader.EXPECT().LoadBloomFilterSet(mock.Anything, s.collectionID, mock.AnythingOfType("int64"), mock.Anything).
			Return(nil, errors.New("mocked error"))

		workers := make(map[int64]*cluster.MockWorker)
		worker1 := &cluster.MockWorker{}
		workers[1] = worker1

		worker1.EXPECT().LoadSegments(mock.Anything, mock.AnythingOfType("*querypb.LoadSegmentsRequest")).
			Return(nil)
		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := s.delegator.LoadSegments(ctx, &querypb.LoadSegmentsRequest{
			Base:         commonpbutil.NewMsgBase(),
			DstNodeID:    1,
			CollectionID: s.collectionID,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:     100,
					PartitionID:   500,
					StartPosition: &msgpb.MsgPosition{Timestamp: 20000},
					DeltaPosition: &msgpb.MsgPosition{Timestamp: 20000},
					InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", s.collectionID),
				},
			},
		})

		s.Error(err)
	})

	s.Run("worker_load_fail", func() {
		defer func() {
			s.workerManager.ExpectedCalls = nil
			s.loader.ExpectedCalls = nil
		}()

		s.loader.EXPECT().LoadBloomFilterSet(mock.Anything, s.collectionID, mock.AnythingOfType("int64"), mock.Anything).
			Call.Return(func(ctx context.Context, collectionID int64, version int64, infos ...*querypb.SegmentLoadInfo) []*pkoracle.BloomFilterSet {
			return lo.Map(infos, func(info *querypb.SegmentLoadInfo, _ int) *pkoracle.BloomFilterSet {
				return pkoracle.NewBloomFilterSet(info.GetSegmentID(), info.GetPartitionID(), commonpb.SegmentState_Sealed)
			})
		}, func(ctx context.Context, collectionID int64, version int64, infos ...*querypb.SegmentLoadInfo) error {
			return nil
		})

		workers := make(map[int64]*cluster.MockWorker)
		worker1 := &cluster.MockWorker{}
		workers[1] = worker1

		worker1.EXPECT().LoadSegments(mock.Anything, mock.AnythingOfType("*querypb.LoadSegmentsRequest")).
			Return(errors.New("mocked error"))
		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := s.delegator.LoadSegments(ctx, &querypb.LoadSegmentsRequest{
			Base:         commonpbutil.NewMsgBase(),
			DstNodeID:    1,
			CollectionID: s.collectionID,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:     100,
					PartitionID:   500,
					StartPosition: &msgpb.MsgPosition{Timestamp: 20000},
					DeltaPosition: &msgpb.MsgPosition{Timestamp: 20000},
					InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", s.collectionID),
				},
			},
		})

		s.Error(err)
	})
}

func (s *DelegatorDataSuite) TestBuildBM25IDF() {
	s.genCollectionWithFunction()

	genBM25Stats := func(start uint32, end uint32) map[int64]*storage.BM25Stats {
		result := make(map[int64]*storage.BM25Stats)
		result[101] = storage.NewBM25Stats()
		for i := start; i < end; i++ {
			row := map[uint32]float32{i: 1}
			result[101].Append(row)
		}
		return result
	}

	genSnapShot := func(seals, grows []int64, targetVersion int64) *snapshot {
		snapshot := &snapshot{
			dist:          []SnapshotItem{{1, make([]SegmentEntry, 0)}},
			targetVersion: targetVersion,
		}

		newSeal := []SegmentEntry{}
		for _, seg := range seals {
			newSeal = append(newSeal, SegmentEntry{NodeID: 1, SegmentID: seg, TargetVersion: targetVersion})
		}

		newGrow := []SegmentEntry{}
		for _, seg := range grows {
			newGrow = append(newGrow, SegmentEntry{NodeID: 1, SegmentID: seg, TargetVersion: targetVersion})
		}

		log.Info("Test-", zap.Any("shanshot", snapshot), zap.Any("seg", newSeal))
		snapshot.dist[0].Segments = newSeal
		snapshot.growing = newGrow
		return snapshot
	}

	genStringFieldData := func(strs ...string) *schemapb.FieldData {
		return &schemapb.FieldData{
			Type:    schemapb.DataType_VarChar,
			FieldId: 102,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: strs,
						},
					},
				},
			},
		}
	}

	s.Run("normal case", func() {
		// register sealed
		sealedSegs := []int64{1, 2, 3, 4}
		for _, segID := range sealedSegs {
			// every segment stats only has one token, avgdl = 1
			s.delegator.idfOracle.Register(segID, genBM25Stats(uint32(segID), uint32(segID)+1), commonpb.SegmentState_Sealed)
		}
		snapshot := genSnapShot([]int64{1, 2, 3, 4}, []int64{}, 100)

		s.delegator.idfOracle.SyncDistribution(snapshot)
		placeholderGroupBytes, err := funcutil.FieldDataToPlaceholderGroupBytes(genStringFieldData("test bm25 data"))
		s.NoError(err)

		plan, err := proto.Marshal(&planpb.PlanNode{
			Node: &planpb.PlanNode_VectorAnns{
				VectorAnns: &planpb.VectorANNS{
					QueryInfo: &planpb.QueryInfo{},
				},
			},
		})
		s.NoError(err)

		req := &internalpb.SearchRequest{
			PlaceholderGroup:   placeholderGroupBytes,
			SerializedExprPlan: plan,
			FieldId:            101,
		}
		avgdl, err := s.delegator.buildBM25IDF(req)
		s.NoError(err)
		s.Equal(float64(1), avgdl)

		// check avgdl in plan
		newplan := &planpb.PlanNode{}
		err = proto.Unmarshal(req.GetSerializedExprPlan(), newplan)
		s.NoError(err)

		annplan, ok := newplan.GetNode().(*planpb.PlanNode_VectorAnns)
		s.Require().True(ok)
		s.Equal(avgdl, annplan.VectorAnns.QueryInfo.Bm25Avgdl)

		// check idf in placeholder
		placeholder := &commonpb.PlaceholderGroup{}
		err = proto.Unmarshal(req.GetPlaceholderGroup(), placeholder)
		s.Require().NoError(err)
		s.Equal(placeholder.GetPlaceholders()[0].GetType(), commonpb.PlaceholderType_SparseFloatVector)
	})

	s.Run("invalid place holder type error", func() {
		placeholderGroupBytes, err := proto.Marshal(&commonpb.PlaceholderGroup{
			Placeholders: []*commonpb.PlaceholderValue{{Type: commonpb.PlaceholderType_SparseFloatVector}},
		})
		s.NoError(err)

		req := &internalpb.SearchRequest{
			PlaceholderGroup: placeholderGroupBytes,
			FieldId:          101,
		}
		_, err = s.delegator.buildBM25IDF(req)
		s.Error(err)
	})

	s.Run("no function runner error", func() {
		placeholderGroupBytes, err := funcutil.FieldDataToPlaceholderGroupBytes(genStringFieldData("test bm25 data"))
		s.NoError(err)

		req := &internalpb.SearchRequest{
			PlaceholderGroup: placeholderGroupBytes,
			FieldId:          103, // invalid field id
		}

		_, err = s.delegator.buildBM25IDF(req)
		s.Error(err)
	})

	s.Run("function runner run failed error", func() {
		placeholderGroupBytes, err := funcutil.FieldDataToPlaceholderGroupBytes(genStringFieldData("test bm25 data"))
		s.NoError(err)

		oldRunner := s.delegator.functionRunners
		mockRunner := function.NewMockFunctionRunner(s.T())
		s.delegator.functionRunners = map[int64]function.FunctionRunner{101: mockRunner}
		mockRunner.EXPECT().GetInputFields().Return([]*schemapb.FieldSchema{
			{
				FieldID:  101,
				Name:     "text",
				DataType: schemapb.DataType_VarChar,
			},
		})
		mockRunner.EXPECT().BatchRun(mock.Anything).Return(nil, fmt.Errorf("mock err"))
		defer func() {
			s.delegator.functionRunners = oldRunner
		}()

		req := &internalpb.SearchRequest{
			PlaceholderGroup: placeholderGroupBytes,
			FieldId:          101,
		}
		_, err = s.delegator.buildBM25IDF(req)
		s.Error(err)
	})

	s.Run("function runner output type error", func() {
		placeholderGroupBytes, err := funcutil.FieldDataToPlaceholderGroupBytes(genStringFieldData("test bm25 data"))
		s.NoError(err)

		oldRunner := s.delegator.functionRunners
		mockRunner := function.NewMockFunctionRunner(s.T())
		s.delegator.functionRunners = map[int64]function.FunctionRunner{101: mockRunner}
		mockRunner.EXPECT().GetInputFields().Return([]*schemapb.FieldSchema{
			{
				FieldID:  101,
				Name:     "text",
				DataType: schemapb.DataType_VarChar,
			},
		})
		mockRunner.EXPECT().BatchRun(mock.Anything).Return([]interface{}{1}, nil)
		defer func() {
			s.delegator.functionRunners = oldRunner
		}()

		req := &internalpb.SearchRequest{
			PlaceholderGroup: placeholderGroupBytes,
			FieldId:          101,
		}
		_, err = s.delegator.buildBM25IDF(req)
		s.Error(err)
	})

	s.Run("idf oracle build idf error", func() {
		placeholderGroupBytes, err := funcutil.FieldDataToPlaceholderGroupBytes(genStringFieldData("test bm25 data"))
		s.NoError(err)

		oldRunner := s.delegator.functionRunners
		mockRunner := function.NewMockFunctionRunner(s.T())
		s.delegator.functionRunners = map[int64]function.FunctionRunner{103: mockRunner}
		mockRunner.EXPECT().GetInputFields().Return([]*schemapb.FieldSchema{
			{
				FieldID:  101,
				Name:     "text",
				DataType: schemapb.DataType_VarChar,
			},
		})
		mockRunner.EXPECT().BatchRun(mock.Anything).Return([]interface{}{&schemapb.SparseFloatArray{Contents: [][]byte{typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{1: 1})}}}, nil)
		defer func() {
			s.delegator.functionRunners = oldRunner
		}()

		req := &internalpb.SearchRequest{
			PlaceholderGroup: placeholderGroupBytes,
			FieldId:          103, // invalid field
		}
		_, err = s.delegator.buildBM25IDF(req)
		s.Error(err)
		log.Info("test", zap.Error(err))
	})

	s.Run("set avgdl failed", func() {
		// register sealed
		sealedSegs := []int64{1, 2, 3, 4}
		for _, segID := range sealedSegs {
			// every segment stats only has one token, avgdl = 1
			s.delegator.idfOracle.Register(segID, genBM25Stats(uint32(segID), uint32(segID)+1), commonpb.SegmentState_Sealed)
		}
		snapshot := genSnapShot([]int64{1, 2, 3, 4}, []int64{}, 100)

		s.delegator.idfOracle.SyncDistribution(snapshot)
		placeholderGroupBytes, err := funcutil.FieldDataToPlaceholderGroupBytes(genStringFieldData("test bm25 data"))
		s.NoError(err)

		req := &internalpb.SearchRequest{
			PlaceholderGroup: placeholderGroupBytes,
			FieldId:          101,
		}
		_, err = s.delegator.buildBM25IDF(req)
		s.Error(err)
	})
}

func (s *DelegatorDataSuite) TestReleaseSegment() {
	s.loader.EXPECT().
		Load(mock.Anything, s.collectionID, segments.SegmentTypeGrowing, int64(0), mock.Anything).
		Call.Return(func(ctx context.Context, collectionID int64, segmentType segments.SegmentType, version int64, infos ...*querypb.SegmentLoadInfo) []segments.Segment {
		return lo.Map(infos, func(info *querypb.SegmentLoadInfo, _ int) segments.Segment {
			ms := &segments.MockSegment{}
			ms.EXPECT().ID().Return(info.GetSegmentID())
			ms.EXPECT().Type().Return(segments.SegmentTypeGrowing)
			ms.EXPECT().Partition().Return(info.GetPartitionID())
			ms.EXPECT().Collection().Return(info.GetCollectionID())
			ms.EXPECT().Indexes().Return(nil)
			ms.EXPECT().RowNum().Return(info.GetNumOfRows())
			ms.EXPECT().Delete(mock.Anything, mock.Anything, mock.Anything).Return(nil)
			ms.EXPECT().MayPkExist(mock.Anything).Call.Return(func(pk storage.PrimaryKey) bool {
				return pk.EQ(storage.NewInt64PrimaryKey(10))
			})
			return ms
		})
	}, nil)
	s.loader.EXPECT().LoadBloomFilterSet(mock.Anything, s.collectionID, mock.AnythingOfType("int64"), mock.Anything).
		Call.Return(func(ctx context.Context, collectionID int64, version int64, infos ...*querypb.SegmentLoadInfo) []*pkoracle.BloomFilterSet {
		return lo.Map(infos, func(info *querypb.SegmentLoadInfo, _ int) *pkoracle.BloomFilterSet {
			bfs := pkoracle.NewBloomFilterSet(info.GetSegmentID(), info.GetPartitionID(), commonpb.SegmentState_Sealed)
			bf := bloomfilter.NewBloomFilterWithType(
				paramtable.Get().CommonCfg.BloomFilterSize.GetAsUint(),
				paramtable.Get().CommonCfg.MaxBloomFalsePositive.GetAsFloat(),
				paramtable.Get().CommonCfg.BloomFilterType.GetValue())
			pks := &storage.PkStatistics{
				PkFilter: bf,
			}
			pks.UpdatePKRange(&storage.Int64FieldData{
				Data: []int64{10, 20, 30},
			})
			bfs.AddHistoricalStats(pks)
			return bfs
		})
	}, func(ctx context.Context, collectionID int64, version int64, infos ...*querypb.SegmentLoadInfo) error {
		return nil
	})

	workers := make(map[int64]*cluster.MockWorker)
	worker1 := &cluster.MockWorker{}
	workers[1] = worker1
	worker2 := &cluster.MockWorker{}
	workers[2] = worker2

	worker1.EXPECT().LoadSegments(mock.Anything, mock.AnythingOfType("*querypb.LoadSegmentsRequest")).
		Return(nil)
	worker1.EXPECT().ReleaseSegments(mock.Anything, mock.AnythingOfType("*querypb.ReleaseSegmentsRequest")).Return(nil)
	worker2.EXPECT().ReleaseSegments(mock.Anything, mock.AnythingOfType("*querypb.ReleaseSegmentsRequest")).Return(nil)
	s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
		return workers[nodeID]
	}, nil)
	// load growing
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := s.delegator.LoadGrowing(ctx, []*querypb.SegmentLoadInfo{
		{
			SegmentID:     1001,
			CollectionID:  s.collectionID,
			PartitionID:   500,
			InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", s.collectionID),
		},
	}, 0)
	s.Require().NoError(err)
	// load sealed
	s.delegator.LoadSegments(ctx, &querypb.LoadSegmentsRequest{
		Base:         commonpbutil.NewMsgBase(),
		DstNodeID:    1,
		CollectionID: s.collectionID,
		Infos: []*querypb.SegmentLoadInfo{
			{
				SegmentID:     1000,
				CollectionID:  s.collectionID,
				PartitionID:   500,
				StartPosition: &msgpb.MsgPosition{Timestamp: 20000},
				DeltaPosition: &msgpb.MsgPosition{Timestamp: 20000},
				InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", s.collectionID),
			},
		},
	})
	s.Require().NoError(err)

	sealed, growing := s.delegator.GetSegmentInfo(false)
	s.Require().Equal(1, len(sealed))
	s.Equal(int64(1), sealed[0].NodeID)
	s.ElementsMatch([]SegmentEntry{
		{
			SegmentID:     1000,
			NodeID:        1,
			PartitionID:   500,
			TargetVersion: unreadableTargetVersion,
		},
	}, sealed[0].Segments)

	s.ElementsMatch([]SegmentEntry{
		{
			SegmentID:     1001,
			NodeID:        1,
			PartitionID:   500,
			TargetVersion: unreadableTargetVersion,
		},
	}, growing)

	err = s.delegator.ReleaseSegments(ctx, &querypb.ReleaseSegmentsRequest{
		Base:       commonpbutil.NewMsgBase(),
		NodeID:     1,
		SegmentIDs: []int64{1000},
		Scope:      querypb.DataScope_Historical,
	}, false)

	s.NoError(err)
	sealed, _ = s.delegator.GetSegmentInfo(false)
	s.Equal(0, len(sealed))

	err = s.delegator.ReleaseSegments(ctx, &querypb.ReleaseSegmentsRequest{
		Base:       commonpbutil.NewMsgBase(),
		NodeID:     1,
		SegmentIDs: []int64{1001},
		Scope:      querypb.DataScope_Streaming,
	}, false)

	s.NoError(err)
	_, growing = s.delegator.GetSegmentInfo(false)
	s.Equal(0, len(growing))

	err = s.delegator.ReleaseSegments(ctx, &querypb.ReleaseSegmentsRequest{
		Base:       commonpbutil.NewMsgBase(),
		NodeID:     1,
		SegmentIDs: []int64{1000},
		Scope:      querypb.DataScope_All,
	}, true)
	s.NoError(err)

	// test transfer
	req := &querypb.ReleaseSegmentsRequest{
		Base:         commonpbutil.NewMsgBase(),
		NodeID:       2,
		SegmentIDs:   []int64{1000},
		Scope:        querypb.DataScope_All,
		NeedTransfer: true,
	}
	req.Base.TargetID = 1
	err = s.delegator.ReleaseSegments(ctx, req, false)
	s.NoError(err)
}

func (s *DelegatorDataSuite) TestLoadPartitionStats() {
	segStats := make(map[UniqueID]storage.SegmentStats)
	centroid := []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0}
	var segID int64 = 1
	rows := 1990
	{
		// p1 stats
		fieldStats := make([]storage.FieldStats, 0)
		fieldStat1 := storage.FieldStats{
			FieldID: 1,
			Type:    schemapb.DataType_Int64,
			Max:     storage.NewInt64FieldValue(200),
			Min:     storage.NewInt64FieldValue(100),
		}
		fieldStat2 := storage.FieldStats{
			FieldID: 2,
			Type:    schemapb.DataType_Int64,
			Max:     storage.NewInt64FieldValue(400),
			Min:     storage.NewInt64FieldValue(300),
		}
		fieldStat3 := storage.FieldStats{
			FieldID: 3,
			Type:    schemapb.DataType_FloatVector,
			Centroids: []storage.VectorFieldValue{
				&storage.FloatVectorFieldValue{
					Value: centroid,
				},
				&storage.FloatVectorFieldValue{
					Value: centroid,
				},
			},
		}
		fieldStats = append(fieldStats, fieldStat1)
		fieldStats = append(fieldStats, fieldStat2)
		fieldStats = append(fieldStats, fieldStat3)
		segStats[segID] = *storage.NewSegmentStats(fieldStats, rows)
	}
	partitionStats1 := &storage.PartitionStatsSnapshot{
		SegmentStats: segStats,
	}
	statsData1, err := storage.SerializePartitionStatsSnapshot(partitionStats1)
	s.NoError(err)
	partitionID1 := int64(1001)
	idPath1 := metautil.JoinIDPath(s.collectionID, partitionID1)
	idPath1 = path.Join(idPath1, s.delegator.vchannelName)
	statsPath1 := path.Join(s.chunkManager.RootPath(), common.PartitionStatsPath, idPath1, strconv.Itoa(1))
	s.chunkManager.Write(context.Background(), statsPath1, statsData1)
	defer s.chunkManager.Remove(context.Background(), statsPath1)

	// reload and check partition stats
	partVersions := make(map[int64]int64)
	partVersions[partitionID1] = 1
	s.delegator.loadPartitionStats(context.Background(), partVersions)
	s.Equal(1, len(s.delegator.partitionStats))
	s.NotNil(s.delegator.partitionStats[partitionID1])
	p1Stats := s.delegator.partitionStats[partitionID1]
	s.Equal(int64(1), p1Stats.GetVersion())
	s.Equal(rows, p1Stats.SegmentStats[segID].NumRows)
	s.Equal(3, len(p1Stats.SegmentStats[segID].FieldStats))

	// judge vector stats
	vecFieldStats := p1Stats.SegmentStats[segID].FieldStats[2]
	s.Equal(2, len(vecFieldStats.Centroids))
	s.Equal(8, len(vecFieldStats.Centroids[0].GetValue().([]float32)))

	// judge scalar stats
	fieldStats1 := p1Stats.SegmentStats[segID].FieldStats[0]
	s.Equal(int64(100), fieldStats1.Min.GetValue().(int64))
	s.Equal(int64(200), fieldStats1.Max.GetValue().(int64))
	fieldStats2 := p1Stats.SegmentStats[segID].FieldStats[1]
	s.Equal(int64(300), fieldStats2.Min.GetValue().(int64))
	s.Equal(int64(400), fieldStats2.Max.GetValue().(int64))
}

func (s *DelegatorDataSuite) TestSyncTargetVersion() {
	for i := int64(0); i < 5; i++ {
		ms := &segments.MockSegment{}
		ms.EXPECT().ID().Return(i)
		ms.EXPECT().StartPosition().Return(&msgpb.MsgPosition{
			Timestamp: uint64(i),
		})
		ms.EXPECT().Type().Return(segments.SegmentTypeGrowing)
		ms.EXPECT().Collection().Return(1)
		ms.EXPECT().Partition().Return(1)
		ms.EXPECT().InsertCount().Return(0)
		ms.EXPECT().Indexes().Return(nil)
		ms.EXPECT().Shard().Return(s.channel)
		ms.EXPECT().Level().Return(datapb.SegmentLevel_L1)
		s.manager.Segment.Put(context.Background(), segments.SegmentTypeGrowing, ms)
	}

	s.delegator.SyncTargetVersion(int64(5), []int64{1}, []int64{1}, []int64{2}, []int64{3, 4}, &msgpb.MsgPosition{})
	s.Equal(int64(5), s.delegator.GetTargetVersion())
}

func (s *DelegatorDataSuite) TestLevel0Deletions() {
	delegator := s.delegator
	partitionID := int64(10)
	partitionDeleteData, err := storage.NewDeltaDataWithPkType(1, schemapb.DataType_Int64)
	s.Require().NoError(err)
	err = partitionDeleteData.Append(storage.NewInt64PrimaryKey(1), 100)
	s.Require().NoError(err)

	allPartitionDeleteData, err := storage.NewDeltaDataWithPkType(1, schemapb.DataType_Int64)
	s.Require().NoError(err)
	err = allPartitionDeleteData.Append(storage.NewInt64PrimaryKey(2), 101)
	s.Require().NoError(err)

	schema := mock_segcore.GenTestCollectionSchema("test_stop", schemapb.DataType_Int64, true)
	collection, err := segments.NewCollection(1, schema, nil, &querypb.LoadMetaInfo{
		LoadType: querypb.LoadType_LoadCollection,
	})
	s.NoError(err)

	l0, _ := segments.NewL0Segment(collection, segments.SegmentTypeSealed, 1, &querypb.SegmentLoadInfo{
		CollectionID:  1,
		SegmentID:     2,
		PartitionID:   partitionID,
		InsertChannel: delegator.vchannelName,
		Level:         datapb.SegmentLevel_L0,
		NumOfRows:     1,
	})
	l0.LoadDeltaData(context.TODO(), partitionDeleteData)
	delegator.segmentManager.Put(context.TODO(), segments.SegmentTypeSealed, l0)

	l0Global, _ := segments.NewL0Segment(collection, segments.SegmentTypeSealed, 2, &querypb.SegmentLoadInfo{
		CollectionID:  1,
		SegmentID:     3,
		PartitionID:   common.AllPartitionsID,
		InsertChannel: delegator.vchannelName,
		Level:         datapb.SegmentLevel_L0,
		NumOfRows:     int64(1),
	})
	l0Global.LoadDeltaData(context.TODO(), allPartitionDeleteData)

	pks, _ := delegator.GetLevel0Deletions(partitionID, pkoracle.NewCandidateKey(l0.ID(), l0.Partition(), segments.SegmentTypeGrowing))
	s.True(pks.Get(0).EQ(partitionDeleteData.DeletePks().Get(0)))

	pks, _ = delegator.GetLevel0Deletions(partitionID+1, pkoracle.NewCandidateKey(l0.ID(), l0.Partition(), segments.SegmentTypeGrowing))
	s.Empty(pks)

	delegator.segmentManager.Put(context.TODO(), segments.SegmentTypeSealed, l0Global)
	pks, _ = delegator.GetLevel0Deletions(partitionID, pkoracle.NewCandidateKey(l0.ID(), l0.Partition(), segments.SegmentTypeGrowing))
	rawPks := make([]storage.PrimaryKey, 0, pks.Len())
	for i := 0; i < pks.Len(); i++ {
		rawPks = append(rawPks, pks.Get(i))
	}
	s.ElementsMatch(rawPks, []storage.PrimaryKey{partitionDeleteData.DeletePks().Get(0), allPartitionDeleteData.DeletePks().Get(0)})

	bfs := pkoracle.NewBloomFilterSet(3, l0.Partition(), commonpb.SegmentState_Sealed)
	bfs.UpdateBloomFilter([]storage.PrimaryKey{allPartitionDeleteData.DeletePks().Get(0)})

	pks, _ = delegator.GetLevel0Deletions(partitionID, bfs)
	// bf filtered segment
	s.Equal(pks.Len(), 1)
	s.True(pks.Get(0).EQ(allPartitionDeleteData.DeletePks().Get(0)))

	delegator.segmentManager.Remove(context.TODO(), l0.ID(), querypb.DataScope_All)
	pks, _ = delegator.GetLevel0Deletions(partitionID, pkoracle.NewCandidateKey(l0.ID(), l0.Partition(), segments.SegmentTypeGrowing))
	s.True(pks.Get(0).EQ(allPartitionDeleteData.DeletePks().Get(0)))

	pks, _ = delegator.GetLevel0Deletions(partitionID+1, pkoracle.NewCandidateKey(l0.ID(), l0.Partition(), segments.SegmentTypeGrowing))
	s.True(pks.Get(0).EQ(allPartitionDeleteData.DeletePks().Get(0)))

	delegator.segmentManager.Remove(context.TODO(), l0Global.ID(), querypb.DataScope_All)
	pks, _ = delegator.GetLevel0Deletions(partitionID+1, pkoracle.NewCandidateKey(l0.ID(), l0.Partition(), segments.SegmentTypeGrowing))
	s.Empty(pks)
}

func (s *DelegatorDataSuite) TestReadDeleteFromMsgstream() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.mq.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.mq.EXPECT().Seek(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.mq.EXPECT().Close()
	ch := make(chan *msgstream.MsgPack, 10)
	s.mq.EXPECT().Chan().Return(ch)

	oracle := pkoracle.NewBloomFilterSet(1, 1, commonpb.SegmentState_Sealed)
	oracle.UpdateBloomFilter([]storage.PrimaryKey{storage.NewInt64PrimaryKey(1), storage.NewInt64PrimaryKey(2)})

	baseMsg := &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete}

	datas := []*msgstream.MsgPack{
		{EndTs: 10, EndPositions: []*msgpb.MsgPosition{{Timestamp: 10}}, Msgs: []msgstream.TsMsg{
			&msgstream.DeleteMsg{DeleteRequest: &msgpb.DeleteRequest{Base: baseMsg, CollectionID: s.collectionID, PartitionID: 1, PrimaryKeys: storage.ParseInt64s2IDs(1), Timestamps: []uint64{1}}},
			&msgstream.DeleteMsg{DeleteRequest: &msgpb.DeleteRequest{Base: baseMsg, CollectionID: s.collectionID, PartitionID: -1, PrimaryKeys: storage.ParseInt64s2IDs(2), Timestamps: []uint64{5}}},
			// invalid msg because partition wrong
			&msgstream.DeleteMsg{DeleteRequest: &msgpb.DeleteRequest{Base: baseMsg, CollectionID: s.collectionID, PartitionID: 2, PrimaryKeys: storage.ParseInt64s2IDs(1), Timestamps: []uint64{10}}},
		}},
	}

	for _, data := range datas {
		ch <- data
	}

	result, err := s.delegator.readDeleteFromMsgstream(ctx, &msgpb.MsgPosition{Timestamp: 0}, 10, oracle)
	s.NoError(err)
	s.Equal(2, len(result.Pks))
}

func (s *DelegatorDataSuite) TestDelegatorData_ExcludeSegments() {
	s.delegator.AddExcludedSegments(map[int64]uint64{
		1: 3,
	})

	s.False(s.delegator.VerifyExcludedSegments(1, 1))
	s.True(s.delegator.VerifyExcludedSegments(1, 5))

	time.Sleep(time.Second * 1)
	s.delegator.TryCleanExcludedSegments(4)
	s.True(s.delegator.VerifyExcludedSegments(1, 1))
	s.True(s.delegator.VerifyExcludedSegments(1, 5))
}

func TestDelegatorDataSuite(t *testing.T) {
	suite.Run(t, new(DelegatorDataSuite))
}

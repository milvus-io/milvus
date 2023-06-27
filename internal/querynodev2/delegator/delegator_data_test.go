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
	"testing"

	bloom "github.com/bits-and-blooms/bloom/v3"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/querynodev2/tsafe"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type DelegatorDataSuite struct {
	suite.Suite

	collectionID  int64
	replicaID     int64
	vchannelName  string
	version       int64
	workerManager *cluster.MockManager
	manager       *segments.Manager
	tsafeManager  tsafe.Manager
	loader        *segments.MockLoader
	mq            *msgstream.MockMsgStream

	delegator ShardDelegator
}

func (s *DelegatorDataSuite) SetupSuite() {
	paramtable.Init()
	paramtable.SetNodeID(1)
}

func (s *DelegatorDataSuite) SetupTest() {
	s.collectionID = 1000
	s.replicaID = 65535
	s.vchannelName = "rootcoord-dml_1000_v0"
	s.version = 2000
	s.workerManager = &cluster.MockManager{}
	s.manager = segments.NewManager()
	s.tsafeManager = tsafe.NewTSafeReplica()
	s.loader = &segments.MockLoader{}

	// init schema
	s.manager.Collection.Put(s.collectionID, &schemapb.CollectionSchema{
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
						Value: "TANIMOTO",
					},
				},
			},
		},
	}, &querypb.LoadMetaInfo{
		LoadType: querypb.LoadType_LoadCollection,
	})

	s.mq = &msgstream.MockMsgStream{}

	var err error
	s.delegator, err = NewShardDelegator(s.collectionID, s.replicaID, s.vchannelName, s.version, s.workerManager, s.manager, s.tsafeManager, s.loader, &msgstream.MockMqFactory{
		NewMsgStreamFunc: func(_ context.Context) (msgstream.MsgStream, error) {
			return s.mq, nil
		},
	}, 10000)
	s.Require().NoError(err)
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
			bf := bloom.NewWithEstimates(storage.BloomFilterSize, storage.MaxBloomFalsePositive)
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
	s.workerManager.EXPECT().GetWorker(mock.AnythingOfType("int64")).Call.Return(func(nodeID int64) cluster.Worker {
		return workers[nodeID]
	}, nil)
	// load growing
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := s.delegator.LoadGrowing(ctx, []*querypb.SegmentLoadInfo{
		{
			SegmentID:    1001,
			CollectionID: s.collectionID,
			PartitionID:  500,
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
		s.workerManager.EXPECT().GetWorker(mock.AnythingOfType("int64")).Call.Return(func(nodeID int64) cluster.Worker {
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
				},
			},
		})

		s.NoError(err)
		sealed, _ := s.delegator.GetSegmentInfo(false)
		s.Require().Equal(1, len(sealed))
		s.Equal(int64(1), sealed[0].NodeID)
		s.ElementsMatch([]SegmentEntry{
			{
				SegmentID:   100,
				NodeID:      1,
				PartitionID: 500,
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
				bf := bloom.NewWithEstimates(storage.BloomFilterSize, storage.MaxBloomFalsePositive)
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
		s.workerManager.EXPECT().GetWorker(mock.AnythingOfType("int64")).Call.Return(func(nodeID int64) cluster.Worker {
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
				},
			},
		})

		s.NoError(err)
		sealed, _ := s.delegator.GetSegmentInfo(false)
		s.Require().Equal(1, len(sealed))
		s.Equal(int64(1), sealed[0].NodeID)
		s.ElementsMatch([]SegmentEntry{
			{
				SegmentID:   100,
				NodeID:      1,
				PartitionID: 500,
			},
			{
				SegmentID:   200,
				NodeID:      1,
				PartitionID: 500,
			},
		}, sealed[0].Segments)
	})

	s.Run("get_worker_fail", func() {
		defer func() {
			s.workerManager.ExpectedCalls = nil
			s.loader.ExpectedCalls = nil
		}()

		s.workerManager.EXPECT().GetWorker(mock.AnythingOfType("int64")).Return(nil, errors.New("mock error"))

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
		s.workerManager.EXPECT().GetWorker(mock.AnythingOfType("int64")).Call.Return(func(nodeID int64) cluster.Worker {
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
		s.workerManager.EXPECT().GetWorker(mock.AnythingOfType("int64")).Call.Return(func(nodeID int64) cluster.Worker {
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
				},
			},
		})

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
			bf := bloom.NewWithEstimates(storage.BloomFilterSize, storage.MaxBloomFalsePositive)
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
	s.workerManager.EXPECT().GetWorker(mock.AnythingOfType("int64")).Call.Return(func(nodeID int64) cluster.Worker {
		return workers[nodeID]
	}, nil)
	// load growing
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := s.delegator.LoadGrowing(ctx, []*querypb.SegmentLoadInfo{
		{
			SegmentID:    1001,
			CollectionID: s.collectionID,
			PartitionID:  500,
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
			},
		},
	})
	s.Require().NoError(err)

	sealed, growing := s.delegator.GetSegmentInfo(false)
	s.Require().Equal(1, len(sealed))
	s.Equal(int64(1), sealed[0].NodeID)
	s.ElementsMatch([]SegmentEntry{
		{
			SegmentID:   1000,
			NodeID:      1,
			PartitionID: 500,
		},
	}, sealed[0].Segments)

	s.ElementsMatch([]SegmentEntry{
		{
			SegmentID:   1001,
			NodeID:      1,
			PartitionID: 500,
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

func (s *DelegatorSuite) TestSyncTargetVersion() {
	s.delegator.SyncTargetVersion(int64(5), []int64{}, []int64{})
	s.Equal(int64(5), s.delegator.GetTargetVersion())
}

func TestDelegatorDataSuite(t *testing.T) {
	suite.Run(t, new(DelegatorDataSuite))
}

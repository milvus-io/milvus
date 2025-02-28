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
	"math/rand"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type StreamingForwardSuite struct {
	suite.Suite

	collectionID  int64
	partitionIDs  []int64
	replicaID     int64
	vchannelName  string
	version       int64
	workerManager *cluster.MockManager
	manager       *segments.Manager
	loader        *segments.MockLoader
	mq            *msgstream.MockMsgStream

	delegator    *shardDelegator
	chunkManager storage.ChunkManager
	rootPath     string
}

func (s *StreamingForwardSuite) SetupSuite() {
	paramtable.Init()
	paramtable.SetNodeID(1)
}

func (s *StreamingForwardSuite) SetupTest() {
	s.collectionID = 1000
	s.partitionIDs = []int64{500, 501}
	s.replicaID = 65535
	s.vchannelName = "rootcoord-dml_1000_v0"
	s.version = 2000
	s.workerManager = &cluster.MockManager{}
	s.manager = segments.NewManager()
	s.loader = &segments.MockLoader{}
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
			return ms
		})
	}, nil)

	// init schema
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
		PartitionIDs: s.partitionIDs,
	})

	s.mq = &msgstream.MockMsgStream{}
	s.rootPath = "delegator_test"

	// init chunkManager
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

func (s *StreamingForwardSuite) TestBFStreamingForward() {
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.StreamingDeltaForwardPolicy.Key, StreamingForwardPolicyBF)
	defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.StreamingDeltaForwardPolicy.Key)

	delegator := s.delegator

	// Setup distribution
	delegator.distribution.AddGrowing(SegmentEntry{
		NodeID:      1,
		PartitionID: 1,
		SegmentID:   100,
	})
	delegator.distribution.AddDistributions(SegmentEntry{
		NodeID:      1,
		PartitionID: 1,
		SegmentID:   101,
	})
	delegator.distribution.AddDistributions(SegmentEntry{
		NodeID:      1,
		PartitionID: 1,
		SegmentID:   102,
	})
	delegator.distribution.SyncTargetVersion(1, []int64{1}, []int64{100}, []int64{101, 102}, nil)

	// Setup pk oracle
	// empty bfs will not match
	delegator.pkOracle.Register(pkoracle.NewBloomFilterSet(100, 10, commonpb.SegmentState_Growing), 1)
	delegator.pkOracle.Register(pkoracle.NewBloomFilterSet(102, 10, commonpb.SegmentState_Sealed), 1)
	// candidate key alway match
	delegator.pkOracle.Register(pkoracle.NewCandidateKey(101, 10, commonpb.SegmentState_Sealed), 1)

	deletedSegment := typeutil.NewConcurrentSet[int64]()
	mockWorker := cluster.NewMockWorker(s.T())
	s.workerManager.EXPECT().GetWorker(mock.Anything, int64(1)).Return(mockWorker, nil)
	mockWorker.EXPECT().Delete(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, dr *querypb.DeleteRequest) error {
		s.T().Log(dr.GetSegmentId())
		deletedSegment.Insert(dr.SegmentId)
		s.ElementsMatch([]int64{10}, dr.GetPrimaryKeys().GetIntId().GetData())
		s.ElementsMatch([]uint64{10}, dr.GetTimestamps())
		return nil
	}).Maybe()

	delegator.ProcessDelete([]*DeleteData{
		{
			PartitionID: -1,
			PrimaryKeys: []storage.PrimaryKey{storage.NewInt64PrimaryKey(10)},
			Timestamps:  []uint64{10},
			RowCount:    1,
		},
	}, 100)
	s.ElementsMatch([]int64{101}, deletedSegment.Collect())
}

func (s *StreamingForwardSuite) TestDirectStreamingForward() {
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.StreamingDeltaForwardPolicy.Key, StreamingForwardPolicyDirect)
	defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.StreamingDeltaForwardPolicy.Key)

	delegator := s.delegator

	// Setup distribution
	delegator.distribution.AddGrowing(SegmentEntry{
		NodeID:      1,
		PartitionID: 1,
		SegmentID:   100,
	})
	delegator.distribution.AddDistributions(SegmentEntry{
		NodeID:      1,
		PartitionID: 1,
		SegmentID:   101,
	})
	delegator.distribution.AddDistributions(SegmentEntry{
		NodeID:      1,
		PartitionID: 1,
		SegmentID:   102,
	})
	delegator.distribution.SyncTargetVersion(1, []int64{1}, []int64{100}, []int64{101, 102}, nil)

	// Setup pk oracle
	// empty bfs will not match
	delegator.pkOracle.Register(pkoracle.NewBloomFilterSet(100, 10, commonpb.SegmentState_Growing), 1)
	delegator.pkOracle.Register(pkoracle.NewBloomFilterSet(102, 10, commonpb.SegmentState_Sealed), 1)
	// candidate key alway match
	delegator.pkOracle.Register(pkoracle.NewCandidateKey(101, 10, commonpb.SegmentState_Sealed), 1)

	deletedSegment := typeutil.NewConcurrentSet[int64]()
	mockWorker := cluster.NewMockWorker(s.T())
	s.workerManager.EXPECT().GetWorker(mock.Anything, int64(1)).Return(mockWorker, nil)
	mockWorker.EXPECT().DeleteBatch(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, dr *querypb.DeleteBatchRequest) (*querypb.DeleteBatchResponse, error) {
		deletedSegment.Upsert(dr.GetSegmentIds()...)
		s.ElementsMatch([]int64{10}, dr.GetPrimaryKeys().GetIntId().GetData())
		s.ElementsMatch([]uint64{10}, dr.GetTimestamps())
		return &querypb.DeleteBatchResponse{Status: merr.Success()}, nil
	})

	delegator.ProcessDelete([]*DeleteData{
		{
			PartitionID: -1,
			PrimaryKeys: []storage.PrimaryKey{storage.NewInt64PrimaryKey(10)},
			Timestamps:  []uint64{10},
			RowCount:    1,
		},
	}, 100)
	s.ElementsMatch([]int64{100, 101, 102}, deletedSegment.Collect())
}

func TestStreamingForward(t *testing.T) {
	suite.Run(t, new(StreamingForwardSuite))
}

type GrowingMergeL0Suite struct {
	suite.Suite

	collectionID  int64
	partitionIDs  []int64
	replicaID     int64
	vchannelName  string
	version       int64
	schema        *schemapb.CollectionSchema
	workerManager *cluster.MockManager
	manager       *segments.Manager
	loader        *segments.MockLoader
	mq            *msgstream.MockMsgStream

	delegator    *shardDelegator
	chunkManager storage.ChunkManager
	rootPath     string
}

func (s *GrowingMergeL0Suite) SetupSuite() {
	paramtable.Init()
	paramtable.SetNodeID(1)
}

func (s *GrowingMergeL0Suite) SetupTest() {
	s.collectionID = 1000
	s.partitionIDs = []int64{500, 501}
	s.replicaID = 65535
	s.vchannelName = "rootcoord-dml_1000v0"
	s.version = 2000
	s.workerManager = &cluster.MockManager{}
	s.manager = segments.NewManager()
	s.loader = &segments.MockLoader{}
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
			return ms
		})
	}, nil)

	// init schema
	s.schema = &schemapb.CollectionSchema{
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
	}
	s.manager.Collection.PutOrRef(s.collectionID, s.schema, &segcorepb.CollectionIndexMeta{
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
		PartitionIDs: s.partitionIDs,
	})

	s.mq = &msgstream.MockMsgStream{}
	s.rootPath = "delegator_test"

	// init chunkManager
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

func (s *GrowingMergeL0Suite) TestAddL0ForGrowingBF() {
	sd := s.delegator
	sd.l0ForwardPolicy = L0ForwardPolicyBF

	seg := segments.NewMockSegment(s.T())
	coll := s.manager.Collection.Get(s.collectionID)
	l0Segment, err := segments.NewL0Segment(coll, segments.SegmentTypeSealed, s.version, &querypb.SegmentLoadInfo{
		SegmentID:     10001,
		CollectionID:  s.collectionID,
		PartitionID:   common.AllPartitionsID,
		InsertChannel: s.vchannelName,
	})
	s.Require().NoError(err)

	n := 10
	deltaData := storage.NewDeltaData(int64(n))

	for i := 0; i < n; i++ {
		deltaData.Append(storage.NewInt64PrimaryKey(rand.Int63()), 0)
	}
	err = l0Segment.LoadDeltaData(context.Background(), deltaData)
	s.Require().NoError(err)
	s.delegator.deleteBuffer.RegisterL0(l0Segment)

	seg.EXPECT().ID().Return(10000)
	seg.EXPECT().Partition().Return(100)
	seg.EXPECT().Delete(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pk storage.PrimaryKeys, u []uint64) error {
		s.Equal(deltaData.DeletePks(), pk)
		s.Equal(deltaData.DeleteTimestamps(), u)
		return nil
	}).Once()

	err = sd.addL0ForGrowing(context.Background(), seg)
	s.NoError(err)

	seg.EXPECT().Delete(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pk storage.PrimaryKeys, u []uint64) error {
		return errors.New("mocked")
	}).Once()
	err = sd.addL0ForGrowing(context.Background(), seg)
	s.Error(err)
}

func (s *GrowingMergeL0Suite) TestAddL0ForGrowingLoad() {
	sd := s.delegator
	sd.l0ForwardPolicy = L0ForwardPolicyRemoteLoad

	seg := segments.NewMockSegment(s.T())
	coll := s.manager.Collection.Get(s.collectionID)
	l0Segment, err := segments.NewL0Segment(coll, segments.SegmentTypeSealed, s.version, &querypb.SegmentLoadInfo{
		SegmentID:     10001,
		CollectionID:  s.collectionID,
		PartitionID:   common.AllPartitionsID,
		InsertChannel: s.vchannelName,
		Deltalogs: []*datapb.FieldBinlog{
			{Binlogs: []*datapb.Binlog{
				{LogPath: "mocked_log_path"},
			}},
		},
	})
	s.Require().NoError(err)

	n := 10
	deltaData := storage.NewDeltaData(int64(n))

	for i := 0; i < n; i++ {
		deltaData.Append(storage.NewInt64PrimaryKey(rand.Int63()), 0)
	}
	err = l0Segment.LoadDeltaData(context.Background(), deltaData)
	s.Require().NoError(err)
	s.delegator.deleteBuffer.RegisterL0(l0Segment)

	seg.EXPECT().ID().Return(10000)
	seg.EXPECT().Partition().Return(100)
	s.loader.EXPECT().LoadDeltaLogs(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, seg segments.Segment, fb []*datapb.FieldBinlog) error {
		s.ElementsMatch([]string{"mocked_log_path"}, lo.Flatten(lo.Map(fb, func(fbl *datapb.FieldBinlog, _ int) []string {
			return lo.Map(fbl.Binlogs, func(bl *datapb.Binlog, _ int) string { return bl.LogPath })
		})))
		return nil
	}).Once()

	err = sd.addL0ForGrowing(context.Background(), seg)
	s.NoError(err)

	s.loader.EXPECT().LoadDeltaLogs(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, seg segments.Segment, fb []*datapb.FieldBinlog) error {
		return errors.New("mocked")
	}).Once()
	err = sd.addL0ForGrowing(context.Background(), seg)
	s.Error(err)
}

func (s *GrowingMergeL0Suite) TestAddL0ForGrowingInvalid() {
	sd := s.delegator
	sd.l0ForwardPolicy = "invalid_policy"

	seg := segments.NewMockSegment(s.T())
	s.Panics(func() {
		sd.addL0ForGrowing(context.Background(), seg)
	})
}

func TestGrowingMergeL0(t *testing.T) {
	suite.Run(t, new(GrowingMergeL0Suite))
}

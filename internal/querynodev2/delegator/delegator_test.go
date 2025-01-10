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
	"io"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metric"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type DelegatorSuite struct {
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

	delegator    ShardDelegator
	chunkManager storage.ChunkManager
	rootPath     string
}

func (s *DelegatorSuite) SetupSuite() {
	paramtable.Init()
}

func (s *DelegatorSuite) TearDownSuite() {
}

func (s *DelegatorSuite) SetupTest() {
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

	var err error
	//	s.delegator, err = NewShardDelegator(s.collectionID, s.replicaID, s.vchannelName, s.version, s.workerManager, s.manager, s.tsafeManager, s.loader)
	s.delegator, err = NewShardDelegator(context.Background(), s.collectionID, s.replicaID, s.vchannelName, s.version, s.workerManager, s.manager, s.loader, &msgstream.MockMqFactory{
		NewMsgStreamFunc: func(_ context.Context) (msgstream.MsgStream, error) {
			return s.mq, nil
		},
	}, 10000, nil, s.chunkManager)
	s.Require().NoError(err)
}

func (s *DelegatorSuite) TearDownTest() {
	s.delegator.Close()
	s.delegator = nil
}

func (s *DelegatorSuite) TestCreateDelegatorWithFunction() {
	s.Run("init function failed", func() {
		manager := segments.NewManager()
		manager.Collection.PutOrRef(s.collectionID, &schemapb.CollectionSchema{
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
				},
			},
			Functions: []*schemapb.FunctionSchema{{
				Type:           schemapb.FunctionType_BM25,
				InputFieldIds:  []int64{102},
				OutputFieldIds: []int64{101, 103}, // invalid output field
			}},
		}, nil, nil)

		_, err := NewShardDelegator(context.Background(), s.collectionID, s.replicaID, s.vchannelName, s.version, s.workerManager, manager, s.loader, &msgstream.MockMqFactory{
			NewMsgStreamFunc: func(_ context.Context) (msgstream.MsgStream, error) {
				return s.mq, nil
			},
		}, 10000, nil, s.chunkManager)
		s.Error(err)
	})

	s.Run("init function failed", func() {
		manager := segments.NewManager()
		manager.Collection.PutOrRef(s.collectionID, &schemapb.CollectionSchema{
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

		_, err := NewShardDelegator(context.Background(), s.collectionID, s.replicaID, s.vchannelName, s.version, s.workerManager, manager, s.loader, &msgstream.MockMqFactory{
			NewMsgStreamFunc: func(_ context.Context) (msgstream.MsgStream, error) {
				return s.mq, nil
			},
		}, 10000, nil, s.chunkManager)
		s.NoError(err)
	})
}

func (s *DelegatorSuite) TestBasicInfo() {
	s.Equal(s.collectionID, s.delegator.Collection())
	s.Equal(s.version, s.delegator.Version())
	s.False(s.delegator.Serviceable())
	s.delegator.Start()
	s.True(s.delegator.Serviceable())
}

func (s *DelegatorSuite) TestGetSegmentInfo() {
	sealed, growing := s.delegator.GetSegmentInfo(false)
	s.Equal(0, len(sealed))
	s.Equal(0, len(growing))

	s.delegator.SyncDistribution(context.Background(), SegmentEntry{
		NodeID:      1,
		SegmentID:   1001,
		PartitionID: 500,
		Version:     2001,
	})

	sealed, growing = s.delegator.GetSegmentInfo(false)
	s.EqualValues([]SnapshotItem{
		{
			NodeID: 1,
			Segments: []SegmentEntry{
				{
					NodeID:        1,
					SegmentID:     1001,
					PartitionID:   500,
					Version:       2001,
					TargetVersion: unreadableTargetVersion,
				},
			},
		},
	}, sealed)
	s.Equal(0, len(growing))
}

// nodeID 1 => sealed segment 1000, 1001
// nodeID 1 => growing segment 1004
// nodeID 2 => sealed segment 1002, 1003
func (s *DelegatorSuite) initSegments() {
	s.delegator.LoadGrowing(context.Background(), []*querypb.SegmentLoadInfo{
		{
			SegmentID:    1004,
			CollectionID: s.collectionID,
			PartitionID:  500,
		},
	}, 0)
	s.delegator.SyncDistribution(context.Background(),
		SegmentEntry{
			NodeID:      1,
			SegmentID:   1000,
			PartitionID: 500,
			Version:     2001,
		},
		SegmentEntry{
			NodeID:      1,
			SegmentID:   1001,
			PartitionID: 501,
			Version:     2001,
		},
		SegmentEntry{
			NodeID:      2,
			SegmentID:   1002,
			PartitionID: 500,
			Version:     2001,
		},
		SegmentEntry{
			NodeID:      2,
			SegmentID:   1003,
			PartitionID: 501,
			Version:     2001,
		},
	)
	s.delegator.SyncTargetVersion(2001, []int64{500, 501}, []int64{1004}, []int64{1000, 1001, 1002, 1003}, []int64{}, &msgpb.MsgPosition{})
}

func (s *DelegatorSuite) TestSearch() {
	s.delegator.Start()
	paramtable.SetNodeID(1)
	s.initSegments()
	s.Run("normal", func() {
		defer func() {
			s.workerManager.ExpectedCalls = nil
		}()
		workers := make(map[int64]*cluster.MockWorker)
		worker1 := &cluster.MockWorker{}
		worker2 := &cluster.MockWorker{}

		workers[1] = worker1
		workers[2] = worker2

		worker1.EXPECT().SearchSegments(mock.Anything, mock.AnythingOfType("*querypb.SearchRequest")).
			Run(func(_ context.Context, req *querypb.SearchRequest) {
				s.EqualValues(1, req.Req.GetBase().GetTargetID())

				if req.GetScope() == querypb.DataScope_Streaming {
					s.EqualValues([]string{s.vchannelName}, req.GetDmlChannels())
					s.ElementsMatch([]int64{1004}, req.GetSegmentIDs())
				}
				if req.GetScope() == querypb.DataScope_Historical {
					s.EqualValues([]string{s.vchannelName}, req.GetDmlChannels())
					s.ElementsMatch([]int64{1000, 1001}, req.GetSegmentIDs())
				}
			}).Return(&internalpb.SearchResults{}, nil)
		worker2.EXPECT().SearchSegments(mock.Anything, mock.AnythingOfType("*querypb.SearchRequest")).
			Run(func(_ context.Context, req *querypb.SearchRequest) {
				s.EqualValues(2, req.Req.GetBase().GetTargetID())

				s.Equal(querypb.DataScope_Historical, req.GetScope())
				s.EqualValues([]string{s.vchannelName}, req.GetDmlChannels())
				s.ElementsMatch([]int64{1002, 1003}, req.GetSegmentIDs())
			}).Return(&internalpb.SearchResults{}, nil)

		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		results, err := s.delegator.Search(ctx, &querypb.SearchRequest{
			Req:         &internalpb.SearchRequest{Base: commonpbutil.NewMsgBase()},
			DmlChannels: []string{s.vchannelName},
		})

		s.NoError(err)
		s.Equal(3, len(results))
	})

	s.Run("partition_not_loaded", func() {
		defer func() {
			s.workerManager.ExpectedCalls = nil
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := s.delegator.Search(ctx, &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				Base: commonpbutil.NewMsgBase(),
				// not load partation -1,will return error
				PartitionIDs: []int64{-1},
			},
			DmlChannels: []string{s.vchannelName},
		})

		s.True(errors.Is(err, merr.ErrPartitionNotLoaded))
	})

	s.Run("worker_return_error", func() {
		defer func() {
			s.workerManager.ExpectedCalls = nil
		}()
		workers := make(map[int64]*cluster.MockWorker)
		worker1 := &cluster.MockWorker{}
		worker2 := &cluster.MockWorker{}

		workers[1] = worker1
		workers[2] = worker2

		worker1.EXPECT().SearchSegments(mock.Anything, mock.AnythingOfType("*querypb.SearchRequest")).Return(nil, errors.New("mock error"))
		worker2.EXPECT().SearchSegments(mock.Anything, mock.AnythingOfType("*querypb.SearchRequest")).
			Run(func(_ context.Context, req *querypb.SearchRequest) {
				s.EqualValues(2, req.Req.GetBase().GetTargetID())

				s.Equal(querypb.DataScope_Historical, req.GetScope())
				s.EqualValues([]string{s.vchannelName}, req.GetDmlChannels())
				s.ElementsMatch([]int64{1002, 1003}, req.GetSegmentIDs())
			}).Return(&internalpb.SearchResults{}, nil)

		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := s.delegator.Search(ctx, &querypb.SearchRequest{
			Req:         &internalpb.SearchRequest{Base: commonpbutil.NewMsgBase()},
			DmlChannels: []string{s.vchannelName},
		})

		s.Error(err)
	})

	s.Run("worker_return_failure_code", func() {
		defer func() {
			s.workerManager.ExpectedCalls = nil
		}()
		workers := make(map[int64]*cluster.MockWorker)
		worker1 := &cluster.MockWorker{}
		worker2 := &cluster.MockWorker{}

		workers[1] = worker1
		workers[2] = worker2

		worker1.EXPECT().SearchSegments(mock.Anything, mock.AnythingOfType("*querypb.SearchRequest")).Return(&internalpb.SearchResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "mocked error",
			},
		}, nil)
		worker2.EXPECT().SearchSegments(mock.Anything, mock.AnythingOfType("*querypb.SearchRequest")).
			Run(func(_ context.Context, req *querypb.SearchRequest) {
				s.EqualValues(2, req.Req.GetBase().GetTargetID())

				s.Equal(querypb.DataScope_Historical, req.GetScope())
				s.EqualValues([]string{s.vchannelName}, req.GetDmlChannels())
				s.ElementsMatch([]int64{1002, 1003}, req.GetSegmentIDs())
			}).Return(&internalpb.SearchResults{}, nil)

		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := s.delegator.Search(ctx, &querypb.SearchRequest{
			Req:         &internalpb.SearchRequest{Base: commonpbutil.NewMsgBase()},
			DmlChannels: []string{s.vchannelName},
		})

		s.Error(err)
	})

	s.Run("wrong_channel", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, err := s.delegator.Search(ctx, &querypb.SearchRequest{
			Req:         &internalpb.SearchRequest{Base: commonpbutil.NewMsgBase()},
			DmlChannels: []string{"non_exist_channel"},
		})

		s.Error(err)
	})

	s.Run("wait_tsafe_timeout", func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
		defer cancel()

		_, err := s.delegator.Search(ctx, &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				Base:               commonpbutil.NewMsgBase(),
				GuaranteeTimestamp: 10100,
			},
			DmlChannels: []string{s.vchannelName},
		})

		s.Error(err)
	})

	s.Run("tsafe_behind_max_lag", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, err := s.delegator.Search(ctx, &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				Base:               commonpbutil.NewMsgBase(),
				GuaranteeTimestamp: uint64(paramtable.Get().QueryNodeCfg.MaxTimestampLag.GetAsDuration(time.Second)) + 10001,
			},
			DmlChannels: []string{s.vchannelName},
		})

		s.Error(err)
	})

	s.Run("distribution_not_serviceable", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sd, ok := s.delegator.(*shardDelegator)
		s.Require().True(ok)
		sd.distribution.AddOfflines(1001)

		_, err := s.delegator.Search(ctx, &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				Base: commonpbutil.NewMsgBase(),
			},
			DmlChannels: []string{s.vchannelName},
		})

		s.Error(err)
	})

	s.Run("cluster_not_serviceable", func() {
		s.delegator.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, err := s.delegator.Search(ctx, &querypb.SearchRequest{
			Req:         &internalpb.SearchRequest{Base: commonpbutil.NewMsgBase()},
			DmlChannels: []string{s.vchannelName},
		})

		s.Error(err)
	})
}

func (s *DelegatorSuite) TestQuery() {
	s.delegator.Start()
	paramtable.SetNodeID(1)
	s.initSegments()
	s.Run("normal", func() {
		defer func() {
			s.workerManager.ExpectedCalls = nil
		}()
		workers := make(map[int64]*cluster.MockWorker)
		worker1 := &cluster.MockWorker{}
		worker2 := &cluster.MockWorker{}

		workers[1] = worker1
		workers[2] = worker2

		worker1.EXPECT().QuerySegments(mock.Anything, mock.AnythingOfType("*querypb.QueryRequest")).
			Run(func(_ context.Context, req *querypb.QueryRequest) {
				s.EqualValues(1, req.Req.GetBase().GetTargetID())

				if req.GetScope() == querypb.DataScope_Streaming {
					s.EqualValues([]string{s.vchannelName}, req.GetDmlChannels())
					s.ElementsMatch([]int64{1004}, req.GetSegmentIDs())
				}
				if req.GetScope() == querypb.DataScope_Historical {
					s.EqualValues([]string{s.vchannelName}, req.GetDmlChannels())
					s.ElementsMatch([]int64{1000, 1001}, req.GetSegmentIDs())
				}
			}).Return(&internalpb.RetrieveResults{}, nil)
		worker2.EXPECT().QuerySegments(mock.Anything, mock.AnythingOfType("*querypb.QueryRequest")).
			Run(func(_ context.Context, req *querypb.QueryRequest) {
				s.EqualValues(2, req.Req.GetBase().GetTargetID())

				s.Equal(querypb.DataScope_Historical, req.GetScope())
				s.EqualValues([]string{s.vchannelName}, req.GetDmlChannels())
				s.ElementsMatch([]int64{1002, 1003}, req.GetSegmentIDs())
			}).Return(&internalpb.RetrieveResults{}, nil)

		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		results, err := s.delegator.Query(ctx, &querypb.QueryRequest{
			Req:         &internalpb.RetrieveRequest{Base: commonpbutil.NewMsgBase()},
			DmlChannels: []string{s.vchannelName},
		})

		s.NoError(err)
		s.Equal(3, len(results))
	})

	s.Run("partition_not_loaded", func() {
		defer func() {
			s.workerManager.ExpectedCalls = nil
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := s.delegator.Query(ctx, &querypb.QueryRequest{
			Req: &internalpb.RetrieveRequest{
				Base: commonpbutil.NewMsgBase(),
				// not load partation -1,will return error
				PartitionIDs: []int64{-1},
			},
			DmlChannels: []string{s.vchannelName},
		})
		s.True(errors.Is(err, merr.ErrPartitionNotLoaded))
	})

	s.Run("worker_return_error", func() {
		defer func() {
			s.workerManager.ExpectedCalls = nil
		}()
		workers := make(map[int64]*cluster.MockWorker)
		worker1 := &cluster.MockWorker{}
		worker2 := &cluster.MockWorker{}

		workers[1] = worker1
		workers[2] = worker2

		worker1.EXPECT().QuerySegments(mock.Anything, mock.AnythingOfType("*querypb.QueryRequest")).Return(nil, errors.New("mock error"))
		worker2.EXPECT().QuerySegments(mock.Anything, mock.AnythingOfType("*querypb.QueryRequest")).
			Run(func(_ context.Context, req *querypb.QueryRequest) {
				s.EqualValues(2, req.Req.GetBase().GetTargetID())

				s.Equal(querypb.DataScope_Historical, req.GetScope())
				s.EqualValues([]string{s.vchannelName}, req.GetDmlChannels())
				s.ElementsMatch([]int64{1002, 1003}, req.GetSegmentIDs())
			}).Return(&internalpb.RetrieveResults{}, nil)

		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := s.delegator.Query(ctx, &querypb.QueryRequest{
			Req:         &internalpb.RetrieveRequest{Base: commonpbutil.NewMsgBase()},
			DmlChannels: []string{s.vchannelName},
		})

		s.Error(err)
	})

	s.Run("worker_return_failure_code", func() {
		workers := make(map[int64]*cluster.MockWorker)
		worker1 := &cluster.MockWorker{}
		worker2 := &cluster.MockWorker{}

		workers[1] = worker1
		workers[2] = worker2

		worker1.EXPECT().QuerySegments(mock.Anything, mock.AnythingOfType("*querypb.QueryRequest")).Return(&internalpb.RetrieveResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "mocked error",
			},
		}, nil)
		worker2.EXPECT().QuerySegments(mock.Anything, mock.AnythingOfType("*querypb.QueryRequest")).
			Run(func(_ context.Context, req *querypb.QueryRequest) {
				s.EqualValues(2, req.Req.GetBase().GetTargetID())

				s.Equal(querypb.DataScope_Historical, req.GetScope())
				s.EqualValues([]string{s.vchannelName}, req.GetDmlChannels())
				s.ElementsMatch([]int64{1002, 1003}, req.GetSegmentIDs())
			}).Return(&internalpb.RetrieveResults{}, nil)

		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := s.delegator.Query(ctx, &querypb.QueryRequest{
			Req:         &internalpb.RetrieveRequest{Base: commonpbutil.NewMsgBase()},
			DmlChannels: []string{s.vchannelName},
		})

		s.Error(err)
	})

	s.Run("wrong_channel", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, err := s.delegator.Query(ctx, &querypb.QueryRequest{
			Req:         &internalpb.RetrieveRequest{Base: commonpbutil.NewMsgBase()},
			DmlChannels: []string{"non_exist_channel"},
		})

		s.Error(err)
	})

	s.Run("distribution_not_serviceable", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sd, ok := s.delegator.(*shardDelegator)
		s.Require().True(ok)
		sd.distribution.AddOfflines(1001)

		_, err := s.delegator.Query(ctx, &querypb.QueryRequest{
			Req:         &internalpb.RetrieveRequest{Base: commonpbutil.NewMsgBase()},
			DmlChannels: []string{s.vchannelName},
		})

		s.Error(err)
	})

	s.Run("cluster_not_serviceable", func() {
		s.delegator.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, err := s.delegator.Query(ctx, &querypb.QueryRequest{
			Req:         &internalpb.RetrieveRequest{Base: commonpbutil.NewMsgBase()},
			DmlChannels: []string{s.vchannelName},
		})

		s.Error(err)
	})
}

func (s *DelegatorSuite) TestQueryStream() {
	s.delegator.Start()
	paramtable.SetNodeID(1)
	s.initSegments()

	s.Run("normal", func() {
		defer func() {
			s.workerManager.AssertExpectations(s.T())
			s.workerManager.ExpectedCalls = nil
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client := streamrpc.NewLocalQueryClient(ctx)
		server := client.CreateServer()

		workers := make(map[int64]*cluster.MockWorker)
		worker1 := &cluster.MockWorker{}
		worker2 := &cluster.MockWorker{}

		workers[1] = worker1
		workers[2] = worker2
		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil)

		worker1.EXPECT().QueryStreamSegments(mock.Anything, mock.AnythingOfType("*querypb.QueryRequest"), mock.Anything).
			Run(func(ctx context.Context, req *querypb.QueryRequest, srv streamrpc.QueryStreamServer) {
				s.EqualValues(1, req.Req.GetBase().GetTargetID())

				if req.GetScope() == querypb.DataScope_Streaming {
					s.EqualValues([]string{s.vchannelName}, req.GetDmlChannels())
					s.ElementsMatch([]int64{1004}, req.GetSegmentIDs())
				}
				if req.GetScope() == querypb.DataScope_Historical {
					s.EqualValues([]string{s.vchannelName}, req.GetDmlChannels())
					s.ElementsMatch([]int64{1000, 1001}, req.GetSegmentIDs())
				}

				srv.Send(&internalpb.RetrieveResults{
					Status: merr.Success(),
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{
							IntId: &schemapb.LongArray{Data: req.GetSegmentIDs()},
						},
					},
				})
			}).Return(nil)

		worker2.EXPECT().QueryStreamSegments(mock.Anything, mock.AnythingOfType("*querypb.QueryRequest"), mock.Anything).
			Run(func(ctx context.Context, req *querypb.QueryRequest, srv streamrpc.QueryStreamServer) {
				s.EqualValues(2, req.Req.GetBase().GetTargetID())

				s.Equal(querypb.DataScope_Historical, req.GetScope())
				s.EqualValues([]string{s.vchannelName}, req.GetDmlChannels())
				s.ElementsMatch([]int64{1002, 1003}, req.GetSegmentIDs())
				srv.Send(&internalpb.RetrieveResults{
					Status: merr.Success(),
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{
							IntId: &schemapb.LongArray{Data: req.GetSegmentIDs()},
						},
					},
				})
			}).Return(nil)

		// run stream function
		go func() {
			err := s.delegator.QueryStream(ctx, &querypb.QueryRequest{
				Req:         &internalpb.RetrieveRequest{Base: commonpbutil.NewMsgBase()},
				DmlChannels: []string{s.vchannelName},
			}, server)
			s.NoError(err)
			server.FinishSend(err)
		}()

		resultIDs := []int64{1000, 1001, 1002, 1003, 1004}
		recNum := 0
		for {
			result, err := client.Recv()
			if err == io.EOF {
				s.Equal(recNum, len(resultIDs))
				break
			}
			s.NoError(err)

			err = merr.Error(result.GetStatus())
			s.NoError(err)

			for _, segmentID := range result.Ids.GetIntId().Data {
				s.Less(recNum, len(resultIDs))
				lo.Contains[int64](resultIDs, segmentID)
				recNum++
			}
		}
	})

	s.Run("partition_not_loaded", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client := streamrpc.NewLocalQueryClient(ctx)
		server := client.CreateServer()

		err := s.delegator.QueryStream(ctx, &querypb.QueryRequest{
			Req: &internalpb.RetrieveRequest{
				Base: commonpbutil.NewMsgBase(),
				// not load partation -1,will return error
				PartitionIDs: []int64{-1},
			},
			DmlChannels: []string{s.vchannelName},
		}, server)
		s.True(errors.Is(err, merr.ErrPartitionNotLoaded))
	})

	s.Run("tsafe_behind_max_lag", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client := streamrpc.NewLocalQueryClient(ctx)
		server := client.CreateServer()

		err := s.delegator.QueryStream(ctx, &querypb.QueryRequest{
			Req: &internalpb.RetrieveRequest{
				Base:               commonpbutil.NewMsgBase(),
				GuaranteeTimestamp: uint64(paramtable.Get().QueryNodeCfg.MaxTimestampLag.GetAsDuration(time.Second)),
			},
			DmlChannels: []string{s.vchannelName},
		}, server)
		s.Error(err)
	})

	s.Run("get_worker_failed", func() {
		defer func() {
			s.workerManager.AssertExpectations(s.T())
			s.workerManager.ExpectedCalls = nil
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client := streamrpc.NewLocalQueryClient(ctx)
		server := client.CreateServer()

		mockErr := errors.New("mock error")

		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(nil, mockErr)

		// run stream function
		err := s.delegator.QueryStream(ctx, &querypb.QueryRequest{
			Req:         &internalpb.RetrieveRequest{Base: commonpbutil.NewMsgBase()},
			DmlChannels: []string{s.vchannelName},
		}, server)
		s.True(errors.Is(err, mockErr))
	})

	s.Run("worker_return_error", func() {
		defer func() {
			s.workerManager.AssertExpectations(s.T())
			s.workerManager.ExpectedCalls = nil
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client := streamrpc.NewLocalQueryClient(ctx)
		server := client.CreateServer()

		workers := make(map[int64]*cluster.MockWorker)
		worker1 := &cluster.MockWorker{}
		worker2 := &cluster.MockWorker{}
		mockErr := errors.New("mock error")

		workers[1] = worker1
		workers[2] = worker2
		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil)

		worker1.EXPECT().QueryStreamSegments(mock.Anything, mock.AnythingOfType("*querypb.QueryRequest"), mock.Anything).
			Return(mockErr)

		worker2.EXPECT().QueryStreamSegments(mock.Anything, mock.AnythingOfType("*querypb.QueryRequest"), mock.Anything).
			Run(func(ctx context.Context, req *querypb.QueryRequest, srv streamrpc.QueryStreamServer) {
				s.EqualValues(2, req.Req.GetBase().GetTargetID())

				s.Equal(querypb.DataScope_Historical, req.GetScope())
				s.EqualValues([]string{s.vchannelName}, req.GetDmlChannels())
				s.ElementsMatch([]int64{1002, 1003}, req.GetSegmentIDs())
				srv.Send(&internalpb.RetrieveResults{
					Status: merr.Success(),
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{
							IntId: &schemapb.LongArray{Data: req.GetSegmentIDs()},
						},
					},
				})
			}).Return(nil)

		// run stream function
		go func() {
			err := s.delegator.QueryStream(ctx, &querypb.QueryRequest{
				Req:         &internalpb.RetrieveRequest{Base: commonpbutil.NewMsgBase()},
				DmlChannels: []string{s.vchannelName},
			}, server)
			server.Send(&internalpb.RetrieveResults{
				Status: merr.Status(err),
			})
		}()

		resultIDs := []int64{1002, 1003}
		recNum := 0
		for {
			result, err := client.Recv()
			s.NoError(err)

			err = merr.Error(result.GetStatus())
			if err != nil {
				s.Equal(recNum, len(resultIDs))
				s.Equal(err.Error(), mockErr.Error())
				break
			}

			for _, segmentID := range result.Ids.GetIntId().GetData() {
				s.Less(recNum, len(resultIDs))
				lo.Contains[int64](resultIDs, segmentID)
				recNum++
			}
		}
	})

	s.Run("wrong_channel", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client := streamrpc.NewLocalQueryClient(ctx)
		server := client.CreateServer()

		err := s.delegator.QueryStream(ctx, &querypb.QueryRequest{
			Req:         &internalpb.RetrieveRequest{Base: commonpbutil.NewMsgBase()},
			DmlChannels: []string{"non_exist_channel"},
		}, server)

		s.Error(err)
	})

	s.Run("distribution_not_serviceable", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sd, ok := s.delegator.(*shardDelegator)
		s.Require().True(ok)
		sd.distribution.AddOfflines(1001)

		client := streamrpc.NewLocalQueryClient(ctx)
		server := client.CreateServer()

		// run stream function
		err := s.delegator.QueryStream(ctx, &querypb.QueryRequest{
			Req:         &internalpb.RetrieveRequest{Base: commonpbutil.NewMsgBase()},
			DmlChannels: []string{s.vchannelName},
		}, server)
		s.Error(err)
	})

	s.Run("cluster_not_serviceable", func() {
		s.delegator.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client := streamrpc.NewLocalQueryClient(ctx)
		server := client.CreateServer()

		err := s.delegator.QueryStream(ctx, &querypb.QueryRequest{
			Req:         &internalpb.RetrieveRequest{Base: commonpbutil.NewMsgBase()},
			DmlChannels: []string{s.vchannelName},
		}, server)

		s.Error(err)
	})
}

func (s *DelegatorSuite) TestGetStats() {
	s.delegator.Start()
	// 1 => sealed segment 1000, 1001
	// 1 => growing segment 1004
	// 2 => sealed segment 1002, 1003
	paramtable.SetNodeID(1)
	s.initSegments()

	s.Run("normal", func() {
		defer func() {
			s.workerManager.ExpectedCalls = nil
		}()
		workers := make(map[int64]*cluster.MockWorker)
		worker1 := &cluster.MockWorker{}
		worker2 := &cluster.MockWorker{}

		workers[1] = worker1
		workers[2] = worker2

		worker1.EXPECT().GetStatistics(mock.Anything, mock.AnythingOfType("*querypb.GetStatisticsRequest")).
			Run(func(_ context.Context, req *querypb.GetStatisticsRequest) {
				s.EqualValues(1, req.Req.GetBase().GetTargetID())
				s.True(req.GetFromShardLeader())
				if req.GetScope() == querypb.DataScope_Streaming {
					s.EqualValues([]string{s.vchannelName}, req.GetDmlChannels())
					s.ElementsMatch([]int64{1004}, req.GetSegmentIDs())
				}
				if req.GetScope() == querypb.DataScope_Historical {
					s.EqualValues([]string{s.vchannelName}, req.GetDmlChannels())
					s.ElementsMatch([]int64{1000, 1001}, req.GetSegmentIDs())
				}
			}).Return(&internalpb.GetStatisticsResponse{}, nil)
		worker2.EXPECT().GetStatistics(mock.Anything, mock.AnythingOfType("*querypb.GetStatisticsRequest")).
			Run(func(_ context.Context, req *querypb.GetStatisticsRequest) {
				s.EqualValues(2, req.Req.GetBase().GetTargetID())
				s.True(req.GetFromShardLeader())
				s.Equal(querypb.DataScope_Historical, req.GetScope())
				s.EqualValues([]string{s.vchannelName}, req.GetDmlChannels())
				s.ElementsMatch([]int64{1002, 1003}, req.GetSegmentIDs())
			}).Return(&internalpb.GetStatisticsResponse{}, nil)

		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		results, err := s.delegator.GetStatistics(ctx, &querypb.GetStatisticsRequest{
			Req:         &internalpb.GetStatisticsRequest{Base: commonpbutil.NewMsgBase()},
			DmlChannels: []string{s.vchannelName},
		})

		s.NoError(err)
		s.Equal(3, len(results))
	})

	s.Run("worker_return_error", func() {
		defer func() {
			s.workerManager.ExpectedCalls = nil
		}()
		workers := make(map[int64]*cluster.MockWorker)
		worker1 := &cluster.MockWorker{}
		worker2 := &cluster.MockWorker{}

		workers[1] = worker1
		workers[2] = worker2

		worker1.EXPECT().GetStatistics(mock.Anything, mock.AnythingOfType("*querypb.GetStatisticsRequest")).Return(nil, errors.New("mock error"))
		worker2.EXPECT().GetStatistics(mock.Anything, mock.AnythingOfType("*querypb.GetStatisticsRequest")).
			Run(func(_ context.Context, req *querypb.GetStatisticsRequest) {
				s.EqualValues(2, req.Req.GetBase().GetTargetID())
				s.True(req.GetFromShardLeader())
				s.Equal(querypb.DataScope_Historical, req.GetScope())
				s.EqualValues([]string{s.vchannelName}, req.GetDmlChannels())
				s.ElementsMatch([]int64{1002, 1003}, req.GetSegmentIDs())
			}).Return(&internalpb.GetStatisticsResponse{}, nil)

		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := s.delegator.GetStatistics(ctx, &querypb.GetStatisticsRequest{
			Req:         &internalpb.GetStatisticsRequest{Base: commonpbutil.NewMsgBase()},
			DmlChannels: []string{s.vchannelName},
		})

		s.Error(err)
	})

	s.Run("worker_return_failure_code", func() {
		workers := make(map[int64]*cluster.MockWorker)
		worker1 := &cluster.MockWorker{}
		worker2 := &cluster.MockWorker{}

		workers[1] = worker1
		workers[2] = worker2

		worker1.EXPECT().GetStatistics(mock.Anything, mock.AnythingOfType("*querypb.GetStatisticsRequest")).Return(&internalpb.GetStatisticsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "mocked error",
			},
		}, nil)
		worker2.EXPECT().GetStatistics(mock.Anything, mock.AnythingOfType("*querypb.GetStatisticsRequest")).
			Run(func(_ context.Context, req *querypb.GetStatisticsRequest) {
				s.EqualValues(2, req.Req.GetBase().GetTargetID())
				s.True(req.GetFromShardLeader())
				s.Equal(querypb.DataScope_Historical, req.GetScope())
				s.EqualValues([]string{s.vchannelName}, req.GetDmlChannels())
				s.ElementsMatch([]int64{1002, 1003}, req.GetSegmentIDs())
			}).Return(&internalpb.GetStatisticsResponse{}, nil)

		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := s.delegator.GetStatistics(ctx, &querypb.GetStatisticsRequest{
			Req:         &internalpb.GetStatisticsRequest{Base: commonpbutil.NewMsgBase()},
			DmlChannels: []string{s.vchannelName},
		})

		s.Error(err)
	})

	s.Run("wrong_channel", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, err := s.delegator.GetStatistics(ctx, &querypb.GetStatisticsRequest{
			Req:         &internalpb.GetStatisticsRequest{Base: commonpbutil.NewMsgBase()},
			DmlChannels: []string{"non_exist_channel"},
		})

		s.Error(err)
	})

	s.Run("distribution_not_serviceable", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sd, ok := s.delegator.(*shardDelegator)
		s.Require().True(ok)
		sd.distribution.AddOfflines(1001)

		_, err := s.delegator.GetStatistics(ctx, &querypb.GetStatisticsRequest{
			Req:         &internalpb.GetStatisticsRequest{Base: commonpbutil.NewMsgBase()},
			DmlChannels: []string{s.vchannelName},
		})

		s.Error(err)
	})

	s.Run("cluster_not_serviceable", func() {
		s.delegator.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, err := s.delegator.GetStatistics(ctx, &querypb.GetStatisticsRequest{
			Req:         &internalpb.GetStatisticsRequest{Base: commonpbutil.NewMsgBase()},
			DmlChannels: []string{s.vchannelName},
		})

		s.Error(err)
	})
}

func TestDelegatorSuite(t *testing.T) {
	suite.Run(t, new(DelegatorSuite))
}

func TestDelegatorSearchBM25InvalidMetricType(t *testing.T) {
	paramtable.Init()
	searchReq := &querypb.SearchRequest{
		Req: &internalpb.SearchRequest{
			Base: commonpbutil.NewMsgBase(),
		},
	}

	searchReq.Req.FieldId = 101
	searchReq.Req.MetricType = metric.IP

	sd := &shardDelegator{
		isBM25Field: map[int64]bool{101: true},
	}

	_, err := sd.search(context.Background(), searchReq, []SnapshotItem{}, []SegmentEntry{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must use BM25 metric type when searching against BM25 Function output field")
}

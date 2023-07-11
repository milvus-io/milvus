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
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/querynodev2/tsafe"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
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
	tsafeManager  tsafe.Manager
	loader        *segments.MockLoader
	mq            *msgstream.MockMsgStream

	delegator ShardDelegator
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
	s.tsafeManager = tsafe.NewTSafeReplica()
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
						Value: "TANIMOTO",
					},
				},
			},
		},
	}, &querypb.LoadMetaInfo{
		PartitionIDs: s.partitionIDs,
	})

	s.mq = &msgstream.MockMsgStream{}

	var err error
	//	s.delegator, err = NewShardDelegator(s.collectionID, s.replicaID, s.vchannelName, s.version, s.workerManager, s.manager, s.tsafeManager, s.loader)
	s.delegator, err = NewShardDelegator(s.collectionID, s.replicaID, s.vchannelName, s.version, s.workerManager, s.manager, s.tsafeManager, s.loader, &msgstream.MockMqFactory{
		NewMsgStreamFunc: func(_ context.Context) (msgstream.MsgStream, error) {
			return s.mq, nil
		},
	}, 10000)
	s.Require().NoError(err)
}

func (s *DelegatorSuite) TearDownTest() {
	s.delegator.Close()
	s.delegator = nil
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
					NodeID:      1,
					SegmentID:   1001,
					PartitionID: 500,
					Version:     2001,
				},
			},
		},
	}, sealed)
	s.Equal(0, len(growing))
}

func (s *DelegatorSuite) TestSearch() {
	s.delegator.Start()
	// 1 => sealed segment 1000, 1001
	// 1 => growing segment 1004
	// 2 => sealed segment 1002, 1003
	paramtable.SetNodeID(1)
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
	s.delegator.SyncTargetVersion(2001, []int64{1004}, []int64{1000, 1001, 1002, 1003})
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
				s.True(req.GetFromShardLeader())
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
				s.True(req.GetFromShardLeader())
				s.Equal(querypb.DataScope_Historical, req.GetScope())
				s.EqualValues([]string{s.vchannelName}, req.GetDmlChannels())
				s.ElementsMatch([]int64{1002, 1003}, req.GetSegmentIDs())
			}).Return(&internalpb.SearchResults{}, nil)

		s.workerManager.EXPECT().GetWorker(mock.AnythingOfType("int64")).Call.Return(func(nodeID int64) cluster.Worker {
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
		workers := make(map[int64]*cluster.MockWorker)
		worker1 := &cluster.MockWorker{}
		worker2 := &cluster.MockWorker{}

		workers[1] = worker1
		workers[2] = worker2

		worker1.EXPECT().SearchSegments(mock.Anything, mock.AnythingOfType("*querypb.SearchRequest")).
			Return(&internalpb.SearchResults{}, nil)
		worker2.EXPECT().SearchSegments(mock.Anything, mock.AnythingOfType("*querypb.SearchRequest")).
			Return(&internalpb.SearchResults{}, nil)

		s.workerManager.EXPECT().GetWorker(mock.AnythingOfType("int64")).Call.Return(func(nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := s.delegator.Search(ctx, &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				Base:         commonpbutil.NewMsgBase(),
				PartitionIDs: []int64{500},
			},
			DmlChannels: []string{s.vchannelName},
		})

		errors.Is(err, merr.ErrPartitionNotLoaded)
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
				s.True(req.GetFromShardLeader())
				s.Equal(querypb.DataScope_Historical, req.GetScope())
				s.EqualValues([]string{s.vchannelName}, req.GetDmlChannels())
				s.ElementsMatch([]int64{1002, 1003}, req.GetSegmentIDs())
			}).Return(&internalpb.SearchResults{}, nil)

		s.workerManager.EXPECT().GetWorker(mock.AnythingOfType("int64")).Call.Return(func(nodeID int64) cluster.Worker {
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
				s.True(req.GetFromShardLeader())
				s.Equal(querypb.DataScope_Historical, req.GetScope())
				s.EqualValues([]string{s.vchannelName}, req.GetDmlChannels())
				s.ElementsMatch([]int64{1002, 1003}, req.GetSegmentIDs())
			}).Return(&internalpb.SearchResults{}, nil)

		s.workerManager.EXPECT().GetWorker(mock.AnythingOfType("int64")).Call.Return(func(nodeID int64) cluster.Worker {
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
				GuaranteeTimestamp: 100,
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
				GuaranteeTimestamp: uint64(paramtable.Get().QueryNodeCfg.MaxTimestampLag.GetAsDuration(time.Second)) + 1,
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
	// 1 => sealed segment 1000, 1001
	// 1 => growing segment 1004
	// 2 => sealed segment 1002, 1003
	paramtable.SetNodeID(1)
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
	s.delegator.SyncTargetVersion(2001, []int64{1004}, []int64{1000, 1001, 1002, 1003})
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
				s.True(req.GetFromShardLeader())
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
				s.True(req.GetFromShardLeader())
				s.Equal(querypb.DataScope_Historical, req.GetScope())
				s.EqualValues([]string{s.vchannelName}, req.GetDmlChannels())
				s.ElementsMatch([]int64{1002, 1003}, req.GetSegmentIDs())
			}).Return(&internalpb.RetrieveResults{}, nil)

		s.workerManager.EXPECT().GetWorker(mock.AnythingOfType("int64")).Call.Return(func(nodeID int64) cluster.Worker {
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
		workers := make(map[int64]*cluster.MockWorker)
		worker1 := &cluster.MockWorker{}
		worker2 := &cluster.MockWorker{}

		workers[1] = worker1
		workers[2] = worker2

		worker1.EXPECT().QuerySegments(mock.Anything, mock.AnythingOfType("*querypb.QueryRequest")).
			Return(&internalpb.RetrieveResults{}, nil)
		worker2.EXPECT().QuerySegments(mock.Anything, mock.AnythingOfType("*querypb.QueryRequest")).
			Return(&internalpb.RetrieveResults{}, nil)

		s.workerManager.EXPECT().GetWorker(mock.AnythingOfType("int64")).Call.Return(func(nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := s.delegator.Query(ctx, &querypb.QueryRequest{
			Req: &internalpb.RetrieveRequest{
				Base:         commonpbutil.NewMsgBase(),
				PartitionIDs: []int64{500},
			},
			DmlChannels: []string{s.vchannelName},
		})

		errors.Is(err, merr.ErrPartitionNotLoaded)
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
				s.True(req.GetFromShardLeader())
				s.Equal(querypb.DataScope_Historical, req.GetScope())
				s.EqualValues([]string{s.vchannelName}, req.GetDmlChannels())
				s.ElementsMatch([]int64{1002, 1003}, req.GetSegmentIDs())
			}).Return(&internalpb.RetrieveResults{}, nil)

		s.workerManager.EXPECT().GetWorker(mock.AnythingOfType("int64")).Call.Return(func(nodeID int64) cluster.Worker {
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
				s.True(req.GetFromShardLeader())
				s.Equal(querypb.DataScope_Historical, req.GetScope())
				s.EqualValues([]string{s.vchannelName}, req.GetDmlChannels())
				s.ElementsMatch([]int64{1002, 1003}, req.GetSegmentIDs())
			}).Return(&internalpb.RetrieveResults{}, nil)

		s.workerManager.EXPECT().GetWorker(mock.AnythingOfType("int64")).Call.Return(func(nodeID int64) cluster.Worker {
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

func (s *DelegatorSuite) TestGetStats() {
	s.delegator.Start()
	// 1 => sealed segment 1000, 1001
	// 1 => growing segment 1004
	// 2 => sealed segment 1002, 1003
	paramtable.SetNodeID(1)
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

	s.delegator.SyncTargetVersion(2001, []int64{1004}, []int64{1000, 1001, 1002, 1003})
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

		s.workerManager.EXPECT().GetWorker(mock.AnythingOfType("int64")).Call.Return(func(nodeID int64) cluster.Worker {
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

		s.workerManager.EXPECT().GetWorker(mock.AnythingOfType("int64")).Call.Return(func(nodeID int64) cluster.Worker {
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

		s.workerManager.EXPECT().GetWorker(mock.AnythingOfType("int64")).Call.Return(func(nodeID int64) cluster.Worker {
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

func TestDelegatorWatchTsafe(t *testing.T) {
	channelName := "default_dml_channel"

	tsafeManager := tsafe.NewTSafeReplica()
	tsafeManager.Add(channelName, 100)
	sd := &shardDelegator{
		tsafeManager: tsafeManager,
		vchannelName: channelName,
		lifetime:     newLifetime(),
		latestTsafe:  atomic.NewUint64(0),
	}
	defer sd.Close()

	m := sync.Mutex{}
	sd.tsCond = sync.NewCond(&m)
	sd.wg.Add(1)
	go sd.watchTSafe()

	err := tsafeManager.Set(channelName, 200)
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		return sd.latestTsafe.Load() == 200
	}, time.Second*10, time.Millisecond*10)
}

func TestDelegatorTSafeListenerClosed(t *testing.T) {
	channelName := "default_dml_channel"

	tsafeManager := tsafe.NewTSafeReplica()
	tsafeManager.Add(channelName, 100)
	sd := &shardDelegator{
		tsafeManager: tsafeManager,
		vchannelName: channelName,
		lifetime:     newLifetime(),
		latestTsafe:  atomic.NewUint64(0),
	}
	defer sd.Close()

	m := sync.Mutex{}
	sd.tsCond = sync.NewCond(&m)
	sd.wg.Add(1)
	signal := make(chan struct{})
	go func() {
		sd.watchTSafe()
		close(signal)
	}()

	select {
	case <-signal:
		assert.FailNow(t, "watchTsafe quit unexpectedly")
	case <-time.After(time.Millisecond * 10):
	}

	tsafeManager.Remove(channelName)

	select {
	case <-signal:
	case <-time.After(time.Second):
		assert.FailNow(t, "watchTsafe still working after listener closed")
	}
}

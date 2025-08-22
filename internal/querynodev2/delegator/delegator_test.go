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
	"os"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestMain(m *testing.M) {
	streaming.SetupNoopWALForTest()

	os.Exit(m.Run())
}

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
		PartitionIDs:  s.partitionIDs,
		SchemaVersion: tsoutil.ComposeTSByTime(time.Now(), 0),
	})

	s.mq = &msgstream.MockMsgStream{}
	s.rootPath = "delegator_test"

	// init chunkManager
	chunkManagerFactory := storage.NewTestChunkManagerFactory(paramtable.Get(), s.rootPath)
	s.chunkManager, _ = chunkManagerFactory.NewPersistentStorageChunkManager(context.Background())

	var err error
	//	s.delegator, err = NewShardDelegator(s.collectionID, s.replicaID, s.vchannelName, s.version, s.workerManager, s.manager, s.tsafeManager, s.loader)
	s.delegator, err = NewShardDelegator(context.Background(), s.collectionID, s.replicaID, s.vchannelName, s.version, s.workerManager, s.manager, s.loader, 10000, nil, s.chunkManager, NewChannelQueryView(nil, nil, nil, initialTargetVersion))
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
		}, nil, &querypb.LoadMetaInfo{SchemaVersion: tsoutil.ComposeTSByTime(time.Now(), 0)})

		_, err := NewShardDelegator(context.Background(), s.collectionID, s.replicaID, s.vchannelName, s.version, s.workerManager, manager, s.loader, 10000, nil, s.chunkManager, NewChannelQueryView(nil, nil, nil, initialTargetVersion))
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
		}, nil, &querypb.LoadMetaInfo{SchemaVersion: tsoutil.ComposeTSByTime(time.Now(), 0)})

		_, err := NewShardDelegator(context.Background(), s.collectionID, s.replicaID, s.vchannelName, s.version, s.workerManager, manager, s.loader, 10000, nil, s.chunkManager, NewChannelQueryView(nil, nil, nil, initialTargetVersion))
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

// TestDelegatorStateChecks tests the state checking methods added/modified in the delegator
func (s *DelegatorSuite) TestDelegatorStateChecks() {
	sd := s.delegator.(*shardDelegator)

	s.Run("test_state_methods_with_different_states", func() {
		// Test Initializing state
		sd.lifetime.SetState(lifetime.Initializing)

		// NotStopped should return nil for non-stopped states
		err := sd.NotStopped(sd.lifetime.GetState())
		s.NoError(err)

		// IsWorking should return error for non-working states
		err = sd.IsWorking(sd.lifetime.GetState())
		s.Error(err)
		s.Contains(err.Error(), "delegator is not ready")
		s.Contains(err.Error(), "Initializing")

		// Serviceable should return false for non-working states
		s.False(sd.Serviceable())

		// Stopped should return false for non-stopped states
		s.False(sd.Stopped())

		// Test Working state
		sd.lifetime.SetState(lifetime.Working)

		// NotStopped should return nil for non-stopped states
		err = sd.NotStopped(sd.lifetime.GetState())
		s.NoError(err)

		// IsWorking should return nil for working state
		err = sd.IsWorking(sd.lifetime.GetState())
		s.NoError(err)

		// Serviceable should return true for working state
		s.True(sd.Serviceable())

		// Stopped should return false for non-stopped states
		s.False(sd.Stopped())

		// Test Stopped state
		sd.lifetime.SetState(lifetime.Stopped)

		// NotStopped should return error for stopped state
		err = sd.NotStopped(sd.lifetime.GetState())
		s.Error(err)
		s.Contains(err.Error(), "delegator is not ready")
		s.Contains(err.Error(), "Stopped")

		// IsWorking should return error for stopped state
		err = sd.IsWorking(sd.lifetime.GetState())
		s.Error(err)
		s.Contains(err.Error(), "delegator is not ready")
		s.Contains(err.Error(), "Stopped")

		// Serviceable should return false for stopped state
		s.False(sd.Serviceable())

		// Stopped should return true for stopped state
		s.True(sd.Stopped())
	})

	s.Run("test_state_methods_with_direct_state_parameter", func() {
		// Test NotStopped with different states
		err := sd.NotStopped(lifetime.Initializing)
		s.NoError(err)

		err = sd.NotStopped(lifetime.Working)
		s.NoError(err)

		err = sd.NotStopped(lifetime.Stopped)
		s.Error(err)
		s.Contains(err.Error(), "delegator is not ready")
		s.Contains(err.Error(), sd.vchannelName)

		// Test IsWorking with different states
		err = sd.IsWorking(lifetime.Initializing)
		s.Error(err)
		s.Contains(err.Error(), "delegator is not ready")
		s.Contains(err.Error(), sd.vchannelName)

		err = sd.IsWorking(lifetime.Working)
		s.NoError(err)

		err = sd.IsWorking(lifetime.Stopped)
		s.Error(err)
		s.Contains(err.Error(), "delegator is not ready")
		s.Contains(err.Error(), sd.vchannelName)
	})

	s.Run("test_error_messages_contain_channel_name", func() {
		// Verify error messages contain the channel name for better debugging
		err := sd.NotStopped(lifetime.Stopped)
		s.Error(err)
		s.Contains(err.Error(), sd.vchannelName)

		err = sd.IsWorking(lifetime.Initializing)
		s.Error(err)
		s.Contains(err.Error(), sd.vchannelName)

		err = sd.IsWorking(lifetime.Stopped)
		s.Error(err)
		s.Contains(err.Error(), sd.vchannelName)
	})
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
	s.delegator.SyncTargetVersion(&querypb.SyncAction{
		TargetVersion:   2001,
		GrowingInTarget: []int64{1004},
		SealedSegmentRowCount: map[int64]int64{
			1000: 100,
			1001: 100,
			1002: 100,
			1003: 100,
		},
		DroppedInTarget: []int64{},
		Checkpoint:      &msgpb.MsgPosition{},
		DeleteCP:        &msgpb.MsgPosition{},
	}, []int64{500, 501})
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

	s.Run("downgrade_tsafe", func() {
		defer func() {
			s.workerManager.ExpectedCalls = nil
		}()
		pt := paramtable.Get()
		pt.Save(pt.QueryNodeCfg.DowngradeTsafe.Key, "true")
		defer pt.Reset(pt.QueryNodeCfg.DowngradeTsafe.Key)

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
			Req: &internalpb.SearchRequest{
				Base:               commonpbutil.NewMsgBase(),
				GuaranteeTimestamp: uint64(paramtable.Get().QueryNodeCfg.MaxTimestampLag.GetAsDuration(time.Second)) + 10001,
			},
			DmlChannels: []string{s.vchannelName},
		})

		s.NoError(err)
		s.Equal(3, len(results))
	})

	s.Run("distribution_not_serviceable", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sd, ok := s.delegator.(*shardDelegator)
		s.Require().True(ok)
		sd.distribution.MarkOfflineSegments(1001)

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
		sd.distribution.MarkOfflineSegments(1001)

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
		s.Error(err)
		s.ErrorContains(err, "segments not loaded in any worker")
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
		sd.distribution.MarkOfflineSegments(1001)

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
		sd.distribution.MarkOfflineSegments(1001)

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

func (s *DelegatorSuite) TestUpdateSchema() {
	s.delegator.Start()
	paramtable.SetNodeID(1)
	s.initSegments()

	s.Run("normal", func() {
		workers := make(map[int64]*cluster.MockWorker)
		worker1 := cluster.NewMockWorker(s.T())
		worker2 := cluster.NewMockWorker(s.T())

		workers[1] = worker1
		workers[2] = worker2

		worker1.EXPECT().UpdateSchema(mock.Anything, mock.AnythingOfType("*querypb.UpdateSchemaRequest")).RunAndReturn(func(ctx context.Context, usr *querypb.UpdateSchemaRequest) (*commonpb.Status, error) {
			return merr.Success(), nil
		}).Twice()

		worker2.EXPECT().UpdateSchema(mock.Anything, mock.AnythingOfType("*querypb.UpdateSchemaRequest")).RunAndReturn(func(ctx context.Context, usr *querypb.UpdateSchemaRequest) (*commonpb.Status, error) {
			return merr.Success(), nil
		}).Once()

		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil).Times(3) // currently node 1 will be called twice for growing & sealed

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err := s.delegator.UpdateSchema(ctx, &schemapb.CollectionSchema{}, 100)
		s.NoError(err)
	})

	s.Run("worker_return_error", func() {
		workers := make(map[int64]*cluster.MockWorker)
		worker1 := cluster.NewMockWorker(s.T())
		worker2 := cluster.NewMockWorker(s.T())

		workers[1] = worker1
		workers[2] = worker2

		worker1.EXPECT().UpdateSchema(mock.Anything, mock.AnythingOfType("*querypb.UpdateSchemaRequest")).RunAndReturn(func(ctx context.Context, usr *querypb.UpdateSchemaRequest) (*commonpb.Status, error) {
			return merr.Status(merr.WrapErrServiceInternal("mocked")), merr.WrapErrServiceInternal("mocked")
		}).Maybe()

		worker2.EXPECT().UpdateSchema(mock.Anything, mock.AnythingOfType("*querypb.UpdateSchemaRequest")).RunAndReturn(func(ctx context.Context, usr *querypb.UpdateSchemaRequest) (*commonpb.Status, error) {
			return merr.Success(), nil
		}).Maybe()

		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil).Times(3)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err := s.delegator.UpdateSchema(ctx, &schemapb.CollectionSchema{}, 100)
		s.Error(err)
	})

	s.Run("worker_manager_error", func() {
		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).RunAndReturn(func(ctx context.Context, i int64) (cluster.Worker, error) {
			return nil, merr.WrapErrServiceInternal("mocked")
		})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err := s.delegator.UpdateSchema(ctx, &schemapb.CollectionSchema{}, 100)
		s.Error(err)
	})

	s.Run("distribution_not_serviceable", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sd, ok := s.delegator.(*shardDelegator)
		s.Require().True(ok)
		sd.distribution.MarkOfflineSegments(1001)

		err := s.delegator.UpdateSchema(ctx, &schemapb.CollectionSchema{}, 100)
		s.Error(err)
	})

	s.Run("cluster_not_serviceable", func() {
		s.delegator.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err := s.delegator.UpdateSchema(ctx, &schemapb.CollectionSchema{}, 100)
		s.Error(err)
	})
}

func (s *DelegatorSuite) ResetDelegator() {
	var err error
	s.delegator.Close()
	s.delegator, err = NewShardDelegator(context.Background(), s.collectionID, s.replicaID, s.vchannelName, s.version, s.workerManager, s.manager, s.loader, 10000, nil, s.chunkManager, NewChannelQueryView(nil, nil, nil, initialTargetVersion))
	s.Require().NoError(err)
}

func (s *DelegatorSuite) TestRunAnalyzer() {
	ctx := context.Background()
	s.TestCreateDelegatorWithFunction()
	s.Run("field analyzer not exist", func() {
		_, err := s.delegator.RunAnalyzer(ctx, &querypb.RunAnalyzerRequest{
			FieldId: 100,
		})
		s.Require().Error(err)
	})

	s.Run("normal analyer", func() {
		s.manager.Collection.PutOrRef(s.collectionID, &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:    100,
					Name:       "text",
					DataType:   schemapb.DataType_VarChar,
					TypeParams: []*commonpb.KeyValuePair{{Key: "analyzer_params", Value: "{}"}},
				},
				{
					FieldID:  101,
					Name:     "sparse",
					DataType: schemapb.DataType_SparseFloatVector,
				},
			},
			Functions: []*schemapb.FunctionSchema{{
				Type:             schemapb.FunctionType_BM25,
				InputFieldNames:  []string{"text"},
				InputFieldIds:    []int64{100},
				OutputFieldNames: []string{"sparse"},
				OutputFieldIds:   []int64{101},
			}},
		}, nil, &querypb.LoadMetaInfo{SchemaVersion: tsoutil.ComposeTSByTime(time.Now(), 0)})
		s.ResetDelegator()

		result, err := s.delegator.RunAnalyzer(ctx, &querypb.RunAnalyzerRequest{
			FieldId:     100,
			Placeholder: [][]byte{[]byte("test doc")},
		})
		s.Require().NoError(err)
		s.Equal(2, len(result[0].GetTokens()))
	})

	s.Run("multi analyzer", func() {
		s.manager.Collection.PutOrRef(s.collectionID, &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:  100,
					Name:     "text",
					DataType: schemapb.DataType_VarChar,
					TypeParams: []*commonpb.KeyValuePair{{Key: "multi_analyzer_params", Value: `{
						"by_field": "analyzer",
    					"analyzers": {
							"default": {}
						}
					}`}},
				},
				{
					FieldID:  101,
					Name:     "sparse",
					DataType: schemapb.DataType_SparseFloatVector,
				},
				{
					FieldID:  102,
					Name:     "analyzer",
					DataType: schemapb.DataType_VarChar,
				},
			},
			Functions: []*schemapb.FunctionSchema{{
				Type:             schemapb.FunctionType_BM25,
				InputFieldNames:  []string{"text"},
				InputFieldIds:    []int64{100},
				OutputFieldNames: []string{"sparse"},
				OutputFieldIds:   []int64{101},
			}},
		}, nil, &querypb.LoadMetaInfo{SchemaVersion: tsoutil.ComposeTSByTime(time.Now(), 0)})
		s.ResetDelegator()

		result, err := s.delegator.RunAnalyzer(ctx, &querypb.RunAnalyzerRequest{
			FieldId:       100,
			Placeholder:   [][]byte{[]byte("test doc"), []byte("test doc2")},
			AnalyzerNames: []string{"default"},
		})
		s.Require().NoError(err)
		s.Equal(2, len(result[0].GetTokens()))
		s.Equal(2, len(result[1].GetTokens()))
	})

	s.Run("error multi analyzer but no analyzer name", func() {
		s.manager.Collection.PutOrRef(s.collectionID, &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:  100,
					Name:     "text",
					DataType: schemapb.DataType_VarChar,
					TypeParams: []*commonpb.KeyValuePair{{Key: "multi_analyzer_params", Value: `{
						"by_field": "analyzer",
    					"analyzers": {
							"default": {}
						}
					}`}},
				},
				{
					FieldID:  101,
					Name:     "sparse",
					DataType: schemapb.DataType_SparseFloatVector,
				},
				{
					FieldID:  102,
					Name:     "analyzer",
					DataType: schemapb.DataType_VarChar,
				},
			},
			Functions: []*schemapb.FunctionSchema{{
				Type:             schemapb.FunctionType_BM25,
				InputFieldNames:  []string{"text"},
				InputFieldIds:    []int64{100},
				OutputFieldNames: []string{"sparse"},
				OutputFieldIds:   []int64{101},
			}},
		}, nil, &querypb.LoadMetaInfo{SchemaVersion: tsoutil.ComposeTSByTime(time.Now(), 0)})
		s.ResetDelegator()

		_, err := s.delegator.RunAnalyzer(ctx, &querypb.RunAnalyzerRequest{
			FieldId:     100,
			Placeholder: [][]byte{[]byte("test doc")},
		})
		s.Require().Error(err)
	})
}

// TestDelegatorLifetimeIntegration tests the integration of lifetime state checks with main delegator methods
func (s *DelegatorSuite) TestDelegatorLifetimeIntegration() {
	sd := s.delegator.(*shardDelegator)
	ctx := context.Background()

	s.Run("test_methods_fail_when_not_working", func() {
		// Set delegator to Initializing state (not ready)
		sd.lifetime.SetState(lifetime.Initializing)

		// Search should fail when not ready
		_, err := sd.Search(ctx, &querypb.SearchRequest{
			Req:         &internalpb.SearchRequest{Base: commonpbutil.NewMsgBase()},
			DmlChannels: []string{s.vchannelName},
		})
		s.Error(err)
		s.Contains(err.Error(), "delegator is not ready")

		// Query should fail when not ready
		_, err = sd.Query(ctx, &querypb.QueryRequest{
			Req:         &internalpb.RetrieveRequest{Base: commonpbutil.NewMsgBase()},
			DmlChannels: []string{s.vchannelName},
		})
		s.Error(err)
		s.Contains(err.Error(), "delegator is not ready")

		// GetStatistics should fail when not ready
		_, err = sd.GetStatistics(ctx, &querypb.GetStatisticsRequest{
			Req:         &internalpb.GetStatisticsRequest{Base: commonpbutil.NewMsgBase()},
			DmlChannels: []string{s.vchannelName},
		})
		s.Error(err)
		s.Contains(err.Error(), "delegator is not ready")

		// UpdateSchema should fail when not ready
		err = sd.UpdateSchema(ctx, &schemapb.CollectionSchema{Name: "test"}, 1)
		s.Error(err)
		s.Contains(err.Error(), "delegator is not ready")
	})

	s.Run("test_methods_fail_when_stopped", func() {
		// Set delegator to Stopped state
		sd.lifetime.SetState(lifetime.Stopped)

		// Search should fail when stopped
		_, err := sd.Search(ctx, &querypb.SearchRequest{
			Req:         &internalpb.SearchRequest{Base: commonpbutil.NewMsgBase()},
			DmlChannels: []string{s.vchannelName},
		})
		s.Error(err)
		s.Contains(err.Error(), "delegator is not ready")

		// Query should fail when stopped
		_, err = sd.Query(ctx, &querypb.QueryRequest{
			Req:         &internalpb.RetrieveRequest{Base: commonpbutil.NewMsgBase()},
			DmlChannels: []string{s.vchannelName},
		})
		s.Error(err)
		s.Contains(err.Error(), "delegator is not ready")

		// GetStatistics should fail when stopped
		_, err = sd.GetStatistics(ctx, &querypb.GetStatisticsRequest{
			Req:         &internalpb.GetStatisticsRequest{Base: commonpbutil.NewMsgBase()},
			DmlChannels: []string{s.vchannelName},
		})
		s.Error(err)
		s.Contains(err.Error(), "delegator is not ready")

		// UpdateSchema should fail when stopped
		err = sd.UpdateSchema(ctx, &schemapb.CollectionSchema{Name: "test"}, 1)
		s.Error(err)
		s.Contains(err.Error(), "delegator is not ready")
	})
}

// TestDelegatorStateTransitions tests state transitions and edge cases
func (s *DelegatorSuite) TestDelegatorStateTransitions() {
	sd := s.delegator.(*shardDelegator)

	s.Run("test_state_transition_sequence", func() {
		// Test normal state transition sequence

		// Start from Initializing
		sd.lifetime.SetState(lifetime.Initializing)
		s.False(sd.Serviceable())
		s.False(sd.Stopped())

		// Transition to Working
		sd.Start() // This calls lifetime.SetState(lifetime.Working)
		s.True(sd.Serviceable())
		s.False(sd.Stopped())

		// Transition to Stopped
		sd.lifetime.SetState(lifetime.Stopped)
		s.False(sd.Serviceable())
		s.True(sd.Stopped())
	})

	s.Run("test_multiple_start_calls", func() {
		// Test that multiple Start() calls don't cause issues
		sd.lifetime.SetState(lifetime.Initializing)
		s.False(sd.Serviceable())

		// Call Start multiple times
		sd.Start()
		s.True(sd.Serviceable())

		sd.Start()
		sd.Start()
		s.True(sd.Serviceable()) // Should remain serviceable
	})

	s.Run("test_start_after_stopped", func() {
		// Test starting after being stopped
		sd.lifetime.SetState(lifetime.Stopped)
		s.True(sd.Stopped())
		s.False(sd.Serviceable())

		// Start again
		sd.Start()
		s.False(sd.Stopped())
		s.True(sd.Serviceable())
	})

	s.Run("test_consistency_between_methods", func() {
		// Test consistency between Serviceable() and Stopped() methods

		// In Initializing state
		sd.lifetime.SetState(lifetime.Initializing)
		serviceable := sd.Serviceable()
		stopped := sd.Stopped()
		s.False(serviceable)
		s.False(stopped)

		// In Working state
		sd.lifetime.SetState(lifetime.Working)
		serviceable = sd.Serviceable()
		stopped = sd.Stopped()
		s.True(serviceable)
		s.False(stopped)

		// In Stopped state
		sd.lifetime.SetState(lifetime.Stopped)
		serviceable = sd.Serviceable()
		stopped = sd.Stopped()
		s.False(serviceable)
		s.True(stopped)
	})

	s.Run("test_error_types_and_wrapping", func() {
		// Test that errors are properly wrapped with channel information

		// Test NotStopped error
		err := sd.NotStopped(lifetime.Stopped)
		s.Error(err)
		s.True(errors.Is(err, merr.ErrChannelNotAvailable))

		// Test IsWorking error
		err = sd.IsWorking(lifetime.Initializing)
		s.Error(err)
		s.True(errors.Is(err, merr.ErrChannelNotAvailable))

		err = sd.IsWorking(lifetime.Stopped)
		s.Error(err)
		s.True(errors.Is(err, merr.ErrChannelNotAvailable))
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

	_, err := sd.search(context.Background(), searchReq, []SnapshotItem{}, []SegmentEntry{}, map[int64]int64{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must use BM25 metric type when searching against BM25 Function output field")
}

func TestNewRowCountBasedEvaluator_SearchAndQueryTasks(t *testing.T) {
	mockey.PatchConvey("TestNewRowCountBasedEvaluator_SearchAndQueryTasks", t, func() {
		sealedRowCount := map[int64]int64{
			1: 1000,
			2: 2000,
			3: 3000,
		}

		// Mock paramtable configuration
		mockParamTable := mockey.Mock(mockey.GetMethod(&paramtable.ParamItem{}, "GetAsFloat")).Return(0.8).Build()
		defer mockParamTable.UnPatch()

		evaluator := NewRowCountBasedEvaluator(sealedRowCount)

		// Test Search task - success segments meet required ratio
		successSegments := typeutil.NewSet[int64]()
		successSegments.Insert(1, 2) // 3000 rows out of 6000 total = 0.5
		failureSegments := []int64{3}
		errors := []error{errors.New("segment 3 failed")}

		shouldReturn, accessedRatio := evaluator("Search", successSegments, failureSegments, errors)
		assert.False(t, shouldReturn) // 0.5 < 0.8, should not return partial
		assert.Equal(t, 0.5, accessedRatio)

		// Test Query task - success segments meet required ratio
		successSegments = typeutil.NewSet[int64]()
		successSegments.Insert(1, 2, 3) // 6000 rows out of 6000 total = 1.0
		failureSegments = []int64{}
		errors = []error{}

		shouldReturn, accessedRatio = evaluator("Query", successSegments, failureSegments, errors)
		assert.True(t, shouldReturn)
		assert.True(t, accessedRatio >= 0.8) // All segments succeeded
		assert.Equal(t, 1.0, accessedRatio)
	})
}

func TestNewRowCountBasedEvaluator_RequiredRatioConfiguration(t *testing.T) {
	mockey.PatchConvey("TestNewRowCountBasedEvaluator_RequiredRatioConfiguration", t, func() {
		sealedRowCount := map[int64]int64{
			1: 1000,
			2: 2000,
		}

		// Test with required ratio >= 1.0 (should always return false for partial results)
		mockParamTable := mockey.Mock(mockey.GetMethod(&paramtable.ParamItem{}, "GetAsFloat")).Return(1.0).Build()
		defer mockParamTable.UnPatch()

		evaluator := NewRowCountBasedEvaluator(sealedRowCount)
		successSegments := typeutil.NewSet[int64]()
		successSegments.Insert(1) // Only partial data available

		shouldReturn, accessedRatio := evaluator("Search", successSegments, []int64{2}, []error{errors.New("test error")})
		assert.False(t, shouldReturn) // Should not return partial when ratio >= 1.0
		assert.Equal(t, 0.0, accessedRatio)

		// Test non-Search/Query task (should use ratio 1.0)
		shouldReturn, accessedRatio = evaluator("GetStatistics", successSegments, []int64{2}, []error{errors.New("test error")})
		assert.False(t, shouldReturn)
		assert.Equal(t, 0.0, accessedRatio)
	})
}

func TestNewRowCountBasedEvaluator_PartialResultAcceptance(t *testing.T) {
	mockey.PatchConvey("TestNewRowCountBasedEvaluator_PartialResultAcceptance", t, func() {
		sealedRowCount := map[int64]int64{
			1: 1000, // 1000 rows
			2: 2000, // 2000 rows
			3: 3000, // 3000 rows
			4: 4000, // 4000 rows
		}
		// Total: 10000 rows

		// Mock paramtable to require 70% data availability
		mockParamTable := mockey.Mock(mockey.GetMethod(&paramtable.ParamItem{}, "GetAsFloat")).Return(0.7).Build()
		defer mockParamTable.UnPatch()

		evaluator := NewRowCountBasedEvaluator(sealedRowCount)

		// Test case: 80% data available (should accept partial result)
		successSegments := typeutil.NewSet[int64]()
		successSegments.Insert(1, 2, 3) // 6000 rows out of 10000 = 0.6
		failureSegments := []int64{4}
		testErrors := []error{errors.New("segment 4 failed")}

		shouldReturn, accessedRatio := evaluator("Search", successSegments, failureSegments, testErrors)
		assert.False(t, shouldReturn) // 0.6 < 0.7, should not return partial
		assert.Equal(t, 0.6, accessedRatio)

		// Test case: 90% data available (should accept partial result)
		successSegments = typeutil.NewSet[int64]()
		successSegments.Insert(2, 3, 4) // 9000 rows out of 10000 = 0.9
		failureSegments = []int64{1}
		testErrors = []error{errors.New("segment 1 failed")}

		shouldReturn, accessedRatio = evaluator("Query", successSegments, failureSegments, testErrors)
		assert.True(t, shouldReturn) // 0.9 > 0.7, should return partial
		assert.Equal(t, 0.9, accessedRatio)
	})
}

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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/tidwall/gjson"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type mockMetricDataNodeClient struct {
	types.DataNodeClient
	mock func() (*milvuspb.GetMetricsResponse, error)
}

func (c *mockMetricDataNodeClient) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	if c.mock == nil {
		return c.DataNodeClient.GetMetrics(ctx, req)
	}
	return c.mock()
}

type mockMetricIndexNodeClient struct {
	types.IndexNodeClient
	mock func() (*milvuspb.GetMetricsResponse, error)
}

func (m *mockMetricIndexNodeClient) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	if m.mock == nil {
		return m.IndexNodeClient.GetMetrics(ctx, req)
	}
	return m.mock()
}

func TestGetDataNodeMetrics(t *testing.T) {
	svr := newTestServer(t)
	defer closeTestServer(t, svr)

	ctx := context.Background()
	req := &milvuspb.GetMetricsRequest{}
	// nil node
	_, err := svr.getDataNodeMetrics(ctx, req, nil)
	assert.Error(t, err)

	// nil client node
	_, err = svr.getDataNodeMetrics(ctx, req, session.NewSession(&session.NodeInfo{}, nil))
	assert.Error(t, err)

	creator := func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
		return newMockDataNodeClient(100, nil)
	}

	// mock datanode client
	sess := session.NewSession(&session.NodeInfo{}, creator)
	info, err := svr.getDataNodeMetrics(ctx, req, sess)
	assert.NoError(t, err)
	assert.False(t, info.HasError)
	assert.Equal(t, metricsinfo.ConstructComponentName(typeutil.DataNodeRole, 100), info.BaseComponentInfos.Name)

	getMockFailedClientCreator := func(mockFunc func() (*milvuspb.GetMetricsResponse, error)) session.DataNodeCreatorFunc {
		return func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
			cli, err := creator(ctx, addr, nodeID)
			assert.NoError(t, err)
			return &mockMetricDataNodeClient{DataNodeClient: cli, mock: mockFunc}, nil
		}
	}

	mockFailClientCreator := getMockFailedClientCreator(func() (*milvuspb.GetMetricsResponse, error) {
		return nil, errors.New("mocked fail")
	})

	info, err = svr.getDataNodeMetrics(ctx, req, session.NewSession(&session.NodeInfo{}, mockFailClientCreator))
	assert.NoError(t, err)
	assert.True(t, info.HasError)

	mockErr := errors.New("mocked error")
	// mock status not success
	mockFailClientCreator = getMockFailedClientCreator(func() (*milvuspb.GetMetricsResponse, error) {
		return &milvuspb.GetMetricsResponse{
			Status: merr.Status(mockErr),
		}, nil
	})

	info, err = svr.getDataNodeMetrics(ctx, req, session.NewSession(&session.NodeInfo{}, mockFailClientCreator))
	assert.NoError(t, err)
	assert.True(t, info.HasError)
	assert.Equal(t, "mocked error", info.ErrorReason)

	// mock parse error
	mockFailClientCreator = getMockFailedClientCreator(func() (*milvuspb.GetMetricsResponse, error) {
		return &milvuspb.GetMetricsResponse{
			Status:   merr.Success(),
			Response: `{"error_reason": 1}`,
		}, nil
	})

	info, err = svr.getDataNodeMetrics(ctx, req, session.NewSession(&session.NodeInfo{}, mockFailClientCreator))
	assert.NoError(t, err)
	assert.True(t, info.HasError)
}

func TestGetIndexNodeMetrics(t *testing.T) {
	svr := newTestServer(t)
	defer closeTestServer(t, svr)

	ctx := context.Background()
	req := &milvuspb.GetMetricsRequest{}
	// nil node
	_, err := svr.getIndexNodeMetrics(ctx, req, nil)
	assert.Error(t, err)

	// return error
	info, err := svr.getIndexNodeMetrics(ctx, req, &mockMetricIndexNodeClient{mock: func() (*milvuspb.GetMetricsResponse, error) {
		return nil, errors.New("mock error")
	}})
	assert.NoError(t, err)
	assert.True(t, info.HasError)

	// failed
	mockErr := errors.New("mocked error")
	info, err = svr.getIndexNodeMetrics(ctx, req, &mockMetricIndexNodeClient{
		mock: func() (*milvuspb.GetMetricsResponse, error) {
			return &milvuspb.GetMetricsResponse{
				Status:        merr.Status(mockErr),
				ComponentName: "indexnode100",
			}, nil
		},
	})
	assert.NoError(t, err)
	assert.True(t, info.HasError)
	assert.Equal(t, metricsinfo.ConstructComponentName(typeutil.IndexNodeRole, 100), info.BaseComponentInfos.Name)

	// return unexpected
	info, err = svr.getIndexNodeMetrics(ctx, req, &mockMetricIndexNodeClient{
		mock: func() (*milvuspb.GetMetricsResponse, error) {
			return &milvuspb.GetMetricsResponse{
				Status:        merr.Success(),
				Response:      "XXXXXXXXXXXXX",
				ComponentName: "indexnode100",
			}, nil
		},
	})
	assert.NoError(t, err)
	assert.True(t, info.HasError)
	assert.Equal(t, metricsinfo.ConstructComponentName(typeutil.IndexNodeRole, 100), info.BaseComponentInfos.Name)

	// success
	info, err = svr.getIndexNodeMetrics(ctx, req, &mockMetricIndexNodeClient{
		mock: func() (*milvuspb.GetMetricsResponse, error) {
			nodeID = UniqueID(100)

			nodeInfos := metricsinfo.DataNodeInfos{
				BaseComponentInfos: metricsinfo.BaseComponentInfos{
					Name: metricsinfo.ConstructComponentName(typeutil.IndexNodeRole, nodeID),
					ID:   nodeID,
				},
			}
			resp, err := metricsinfo.MarshalComponentInfos(nodeInfos)
			if err != nil {
				return &milvuspb.GetMetricsResponse{
					Status:        merr.Status(err),
					ComponentName: metricsinfo.ConstructComponentName(typeutil.IndexNodeRole, nodeID),
				}, nil
			}

			return &milvuspb.GetMetricsResponse{
				Status:        merr.Success(),
				Response:      resp,
				ComponentName: metricsinfo.ConstructComponentName(typeutil.IndexNodeRole, nodeID),
			}, nil
		},
	})

	assert.NoError(t, err)
	assert.False(t, info.HasError)
	assert.Equal(t, metricsinfo.ConstructComponentName(typeutil.IndexNodeRole, 100), info.BaseComponentInfos.Name)
}

func TestGetSyncTaskMetrics(t *testing.T) {
	svr := Server{}
	t.Run("ReturnsCorrectJSON", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{}
		ctx := context.Background()

		tasks := []metricsinfo.SyncTask{
			{
				SegmentID:     1,
				BatchRows:     100,
				SegmentLevel:  "L0",
				TSFrom:        "t1",
				TSTo:          "t2",
				DeltaRowCount: 10,
				FlushSize:     1024,
				RunningTime:   "2h",
			},
		}
		tasksBytes, err := json.Marshal(tasks)
		assert.NoError(t, err)
		expectedJSON := string(tasksBytes)

		mockResp := &milvuspb.GetMetricsResponse{
			Status:   merr.Success(),
			Response: expectedJSON,
		}

		mockClient := &mockMetricDataNodeClient{
			mock: func() (*milvuspb.GetMetricsResponse, error) {
				return mockResp, nil
			},
		}

		dataNodeCreator := func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
			return mockClient, nil
		}

		mockCluster := NewMockCluster(t)
		mockCluster.EXPECT().GetSessions().Return([]*session.Session{session.NewSession(&session.NodeInfo{NodeID: 1}, dataNodeCreator)})
		svr.cluster = mockCluster

		actualJSON, err := svr.getSyncTaskJSON(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, expectedJSON, actualJSON)
	})

	t.Run("ReturnsErrorOnRequestFailure", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{}
		ctx := context.Background()

		mockClient := &mockMetricDataNodeClient{
			mock: func() (*milvuspb.GetMetricsResponse, error) {
				return nil, errors.New("request failed")
			},
		}

		dataNodeCreator := func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
			return mockClient, nil
		}

		mockCluster := NewMockCluster(t)
		mockCluster.EXPECT().GetSessions().Return([]*session.Session{session.NewSession(&session.NodeInfo{NodeID: 1}, dataNodeCreator)})
		svr.cluster = mockCluster

		actualJSON, err := svr.getSyncTaskJSON(ctx, req)
		assert.Error(t, err)
		assert.Equal(t, "", actualJSON)
	})

	t.Run("ReturnsErrorOnUnmarshalFailure", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{}
		ctx := context.Background()

		mockResp := &milvuspb.GetMetricsResponse{
			Status:   merr.Success(),
			Response: `invalid json`,
		}

		mockClient := &mockMetricDataNodeClient{
			mock: func() (*milvuspb.GetMetricsResponse, error) {
				return mockResp, nil
			},
		}

		dataNodeCreator := func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
			return mockClient, nil
		}

		mockCluster := NewMockCluster(t)
		mockCluster.EXPECT().GetSessions().Return([]*session.Session{session.NewSession(&session.NodeInfo{NodeID: 1}, dataNodeCreator)})
		svr.cluster = mockCluster

		actualJSON, err := svr.getSyncTaskJSON(ctx, req)
		assert.Error(t, err)
		assert.Equal(t, "", actualJSON)
	})

	t.Run("ReturnsEmptyJSONWhenNoTasks", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{}
		ctx := context.Background()

		mockResp := &milvuspb.GetMetricsResponse{
			Status:   merr.Success(),
			Response: "",
		}

		mockClient := &mockMetricDataNodeClient{
			mock: func() (*milvuspb.GetMetricsResponse, error) {
				return mockResp, nil
			},
		}

		dataNodeCreator := func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
			return mockClient, nil
		}

		mockCluster := NewMockCluster(t)
		mockCluster.EXPECT().GetSessions().Return([]*session.Session{session.NewSession(&session.NodeInfo{NodeID: 1}, dataNodeCreator)})
		svr.cluster = mockCluster

		expectedJSON := "null"
		actualJSON, err := svr.getSyncTaskJSON(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, expectedJSON, actualJSON)
	})
}

func TestGetSegmentsJSON(t *testing.T) {
	svr := Server{}
	t.Run("ReturnsCorrectJSON", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{}
		ctx := context.Background()

		segments := []*metricsinfo.Segment{
			{
				SegmentID:    1,
				CollectionID: 100,
				PartitionID:  10,
				NumOfRows:    1000,
				State:        "Flushed",
			},
		}
		segmentsBytes, err := json.Marshal(segments)
		assert.NoError(t, err)
		expectedJSON := string(segmentsBytes)

		mockResp := &milvuspb.GetMetricsResponse{
			Status:   merr.Success(),
			Response: expectedJSON,
		}

		mockClient := &mockMetricDataNodeClient{
			mock: func() (*milvuspb.GetMetricsResponse, error) {
				return mockResp, nil
			},
		}

		dataNodeCreator := func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
			return mockClient, nil
		}

		mockCluster := NewMockCluster(t)
		mockCluster.EXPECT().GetSessions().Return([]*session.Session{session.NewSession(&session.NodeInfo{NodeID: 1}, dataNodeCreator)})
		svr.cluster = mockCluster

		actualJSON, err := svr.getDataNodeSegmentsJSON(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, expectedJSON, actualJSON)
	})

	t.Run("ReturnsErrorOnRequestFailure", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{}
		ctx := context.Background()

		mockClient := &mockMetricDataNodeClient{
			mock: func() (*milvuspb.GetMetricsResponse, error) {
				return nil, errors.New("request failed")
			},
		}

		dataNodeCreator := func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
			return mockClient, nil
		}

		mockCluster := NewMockCluster(t)
		mockCluster.EXPECT().GetSessions().Return([]*session.Session{session.NewSession(&session.NodeInfo{NodeID: 1}, dataNodeCreator)})
		svr.cluster = mockCluster

		actualJSON, err := svr.getDataNodeSegmentsJSON(ctx, req)
		assert.Error(t, err)
		assert.Equal(t, "", actualJSON)
	})

	t.Run("ReturnsErrorOnUnmarshalFailure", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{}
		ctx := context.Background()

		mockResp := &milvuspb.GetMetricsResponse{
			Status:   merr.Success(),
			Response: `invalid json`,
		}

		mockClient := &mockMetricDataNodeClient{
			mock: func() (*milvuspb.GetMetricsResponse, error) {
				return mockResp, nil
			},
		}

		dataNodeCreator := func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
			return mockClient, nil
		}

		mockCluster := NewMockCluster(t)
		mockCluster.EXPECT().GetSessions().Return([]*session.Session{session.NewSession(&session.NodeInfo{NodeID: 1}, dataNodeCreator)})
		svr.cluster = mockCluster

		actualJSON, err := svr.getDataNodeSegmentsJSON(ctx, req)
		assert.Error(t, err)
		assert.Equal(t, "", actualJSON)
	})

	t.Run("ReturnsEmptyJSONWhenNoSegments", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{}
		ctx := context.Background()

		mockResp := &milvuspb.GetMetricsResponse{
			Status:   merr.Success(),
			Response: "",
		}

		mockClient := &mockMetricDataNodeClient{
			mock: func() (*milvuspb.GetMetricsResponse, error) {
				return mockResp, nil
			},
		}

		dataNodeCreator := func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
			return mockClient, nil
		}

		mockCluster := NewMockCluster(t)
		mockCluster.EXPECT().GetSessions().Return([]*session.Session{session.NewSession(&session.NodeInfo{NodeID: 1}, dataNodeCreator)})
		svr.cluster = mockCluster

		expectedJSON := "null"
		actualJSON, err := svr.getDataNodeSegmentsJSON(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, expectedJSON, actualJSON)
	})
}

func TestGetChannelsJSON(t *testing.T) {
	svr := Server{}
	t.Run("ReturnsCorrectJSON", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{}

		channels := []*metricsinfo.Channel{
			{
				Name:         "channel1",
				CollectionID: 100,
				NodeID:       1,
			},
		}
		channelsBytes, err := json.Marshal(channels)
		assert.NoError(t, err)
		channelJSON := string(channelsBytes)

		mockResp := &milvuspb.GetMetricsResponse{
			Status:   merr.Success(),
			Response: channelJSON,
		}

		mockClient := &mockMetricDataNodeClient{
			mock: func() (*milvuspb.GetMetricsResponse, error) {
				return mockResp, nil
			},
		}

		dataNodeCreator := func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
			return mockClient, nil
		}

		mockCluster := NewMockCluster(t)
		mockCluster.EXPECT().GetSessions().Return([]*session.Session{session.NewSession(&session.NodeInfo{NodeID: 1}, dataNodeCreator)})
		svr.cluster = mockCluster

		svr.meta = &meta{channelCPs: newChannelCps()}
		svr.meta.channelCPs.checkpoints["channel1"] = &msgpb.MsgPosition{Timestamp: 1000}

		actualJSON, err := svr.getChannelsJSON(context.TODO(), req)
		assert.NoError(t, err)

		channels = []*metricsinfo.Channel{
			{
				Name:         "channel1",
				CollectionID: 100,
				NodeID:       1,
				CheckpointTS: tsoutil.PhysicalTimeFormat(1000),
			},
		}
		channelsBytes, err = json.Marshal(channels)
		assert.NoError(t, err)
		expectedJSON := string(channelsBytes)

		assert.Equal(t, expectedJSON, actualJSON)
	})

	t.Run("ReturnsErrorOnRequestFailure", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{}
		ctx := context.Background()

		mockClient := &mockMetricDataNodeClient{
			mock: func() (*milvuspb.GetMetricsResponse, error) {
				return nil, errors.New("request failed")
			},
		}

		dataNodeCreator := func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
			return mockClient, nil
		}

		mockCluster := NewMockCluster(t)
		mockCluster.EXPECT().GetSessions().Return([]*session.Session{session.NewSession(&session.NodeInfo{NodeID: 1}, dataNodeCreator)})
		svr.cluster = mockCluster

		svr.meta = &meta{channelCPs: newChannelCps()}

		actualJSON, err := svr.getChannelsJSON(ctx, req)
		assert.Error(t, err)
		assert.Equal(t, "", actualJSON)
	})

	t.Run("ReturnsErrorOnUnmarshalFailure", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{}
		ctx := context.Background()

		mockResp := &milvuspb.GetMetricsResponse{
			Status:   merr.Success(),
			Response: `invalid json`,
		}

		mockClient := &mockMetricDataNodeClient{
			mock: func() (*milvuspb.GetMetricsResponse, error) {
				return mockResp, nil
			},
		}

		dataNodeCreator := func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
			return mockClient, nil
		}

		mockCluster := NewMockCluster(t)
		mockCluster.EXPECT().GetSessions().Return([]*session.Session{session.NewSession(&session.NodeInfo{NodeID: 1}, dataNodeCreator)})
		svr.cluster = mockCluster

		svr.meta = &meta{channelCPs: newChannelCps()}

		actualJSON, err := svr.getChannelsJSON(ctx, req)
		assert.Error(t, err)
		assert.Equal(t, "", actualJSON)
	})

	t.Run("ReturnsEmptyJSONWhenNoChannels", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{}
		ctx := context.Background()

		mockResp := &milvuspb.GetMetricsResponse{
			Status:   merr.Success(),
			Response: "",
		}

		mockClient := &mockMetricDataNodeClient{
			mock: func() (*milvuspb.GetMetricsResponse, error) {
				return mockResp, nil
			},
		}

		dataNodeCreator := func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
			return mockClient, nil
		}

		mockCluster := NewMockCluster(t)
		mockCluster.EXPECT().GetSessions().Return([]*session.Session{session.NewSession(&session.NodeInfo{NodeID: 1}, dataNodeCreator)})
		svr.cluster = mockCluster
		svr.meta = &meta{channelCPs: newChannelCps()}

		expectedJSON := "null"
		actualJSON, err := svr.getChannelsJSON(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, expectedJSON, actualJSON)
	})
}

func TestGetDistJSON(t *testing.T) {
	svr := Server{}
	nodeID := paramtable.GetNodeID()
	paramtable.SetNodeID(1)
	defer paramtable.SetNodeID(nodeID)

	t.Run("ReturnsCorrectJSON", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{}
		ctx := context.Background()

		svr.meta = &meta{
			segments: &SegmentsInfo{
				segments: map[int64]*SegmentInfo{
					1: {
						SegmentInfo: &datapb.SegmentInfo{
							ID:            1,
							CollectionID:  1,
							PartitionID:   1,
							InsertChannel: "channel1",
							Level:         datapb.SegmentLevel_L1,
							State:         commonpb.SegmentState_Flushed,
						},
					},
				},
			},
		}

		cm := NewMockChannelManager(t)
		cm.EXPECT().GetChannelWatchInfos().Return(map[int64]map[string]*datapb.ChannelWatchInfo{
			1: {
				"channel1": {
					State: datapb.ChannelWatchState_ToWatch,
					Vchan: &datapb.VchannelInfo{
						ChannelName: "channel1",
					},
				},
			},
		})

		svr.channelManager = cm

		segments := []*metricsinfo.Segment{
			{
				SegmentID:    1,
				State:        commonpb.SegmentState_Flushed.String(),
				CollectionID: 1,
				PartitionID:  1,
				Channel:      "channel1",
				Level:        datapb.SegmentLevel_L1.String(),
				NodeID:       1,
			},
		}
		channels := []*metricsinfo.DmChannel{
			{
				ChannelName: "channel1",
				NodeID:      1,
				WatchState:  datapb.ChannelWatchState_ToWatch.String(),
			},
		}
		dist := &metricsinfo.DataCoordDist{
			Segments:   segments,
			DMChannels: channels,
		}
		distBytes, err := json.Marshal(dist)
		assert.NoError(t, err)
		expectedJSON := string(distBytes)

		actualJSON := svr.getDistJSON(ctx, req)
		assert.Equal(t, expectedJSON, actualJSON)
	})

	t.Run("ReturnsEmptyJSONWhenNoDist", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{}
		ctx := context.Background()

		svr.meta = &meta{segments: &SegmentsInfo{segments: map[int64]*SegmentInfo{}}}
		cm := NewMockChannelManager(t)
		cm.EXPECT().GetChannelWatchInfos().Return(map[int64]map[string]*datapb.ChannelWatchInfo{})

		svr.channelManager = cm
		expectedJSON := "{}"
		actualJSON := svr.getDistJSON(ctx, req)
		assert.Equal(t, expectedJSON, actualJSON)
	})
}

func TestServer_getSegmentsJSON(t *testing.T) {
	s := &Server{
		meta: &meta{
			segments: &SegmentsInfo{
				segments: map[int64]*SegmentInfo{
					1: {
						SegmentInfo: &datapb.SegmentInfo{
							ID:            1,
							CollectionID:  1,
							PartitionID:   2,
							InsertChannel: "channel1",
						},
					},
				},
			},
			indexMeta: &indexMeta{
				segmentIndexes: map[UniqueID]map[UniqueID]*model.SegmentIndex{
					1000: {
						10: &model.SegmentIndex{
							SegmentID:           1000,
							CollectionID:        1,
							PartitionID:         2,
							NumRows:             10250,
							IndexID:             10,
							BuildID:             10000,
							NodeID:              1,
							IndexVersion:        0,
							IndexState:          commonpb.IndexState_Finished,
							FailReason:          "",
							IsDeleted:           false,
							CreatedUTCTime:      12,
							IndexFileKeys:       nil,
							IndexSerializedSize: 0,
						},
					},
				},
				indexes: map[UniqueID]map[UniqueID]*model.Index{
					1: {
						10: &model.Index{
							CollectionID: 1,
							FieldID:      100,
							IndexID:      10,
							IndexName:    "test_index",
							IsDeleted:    false,
						},
					},
				},
			},
		},
	}

	ctx := context.TODO()
	req := &milvuspb.GetMetricsRequest{}
	t.Run("valid request in dc", func(t *testing.T) {
		jsonReq := gjson.Parse(`{"in": "dc", "collection_id": 1}`)
		result, err := s.getSegmentsJSON(ctx, req, jsonReq)
		assert.NoError(t, err)
		assert.NotEmpty(t, result)
	})

	t.Run("invalid request", func(t *testing.T) {
		jsonReq := gjson.Parse(`{"in": "invalid"}`)
		result, err := s.getSegmentsJSON(ctx, req, jsonReq)
		assert.Error(t, err)
		assert.Empty(t, result)
	})

	t.Run("vaild request in dn", func(t *testing.T) {
		segments := []*metricsinfo.Segment{
			{
				SegmentID:    1,
				CollectionID: 100,
				PartitionID:  10,
				NumOfRows:    1000,
				State:        "Flushed",
			},
		}
		segmentsBytes, err := json.Marshal(segments)
		assert.NoError(t, err)
		expectedJSON := string(segmentsBytes)

		mockResp := &milvuspb.GetMetricsResponse{
			Status:   merr.Success(),
			Response: expectedJSON,
		}

		mockClient := &mockMetricDataNodeClient{
			mock: func() (*milvuspb.GetMetricsResponse, error) {
				return mockResp, nil
			},
		}

		dataNodeCreator := func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
			return mockClient, nil
		}

		mockCluster := NewMockCluster(t)
		mockCluster.EXPECT().GetSessions().Return([]*session.Session{session.NewSession(&session.NodeInfo{NodeID: 1}, dataNodeCreator)})
		s.cluster = mockCluster

		jsonReq := gjson.Parse(`{"in": "dn"}`)
		result, err := s.getSegmentsJSON(ctx, req, jsonReq)
		assert.NoError(t, err)
		assert.NotEmpty(t, result)

		jsonReq = gjson.Parse(`{}`)
		result, err = s.getSegmentsJSON(ctx, req, jsonReq)
		assert.NoError(t, err)
		assert.NotEmpty(t, result)
	})
}

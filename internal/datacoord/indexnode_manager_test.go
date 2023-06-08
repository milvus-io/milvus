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
	"sync"
	"testing"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/indexnode"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/stretchr/testify/assert"
)

func TestIndexNodeManager_AddNode(t *testing.T) {
	nm := NewNodeManager(context.Background(), defaultIndexNodeCreatorFunc)
	nodeID, client := nm.PeekClient(&model.SegmentIndex{})
	assert.Equal(t, int64(-1), nodeID)
	assert.Nil(t, client)

	t.Run("success", func(t *testing.T) {
		err := nm.AddNode(1, "indexnode-1")
		assert.NoError(t, err)
	})

	t.Run("fail", func(t *testing.T) {
		err := nm.AddNode(2, "")
		assert.Error(t, err)
	})
}

func TestIndexNodeManager_PeekClient(t *testing.T) {
	t.Run("multiple unavailable IndexNode", func(t *testing.T) {
		nm := &IndexNodeManager{
			ctx: context.TODO(),
			nodeClients: map[UniqueID]types.IndexNode{
				1: &indexnode.Mock{
					CallGetJobStats: func(ctx context.Context, req *indexpb.GetJobStatsRequest) (*indexpb.GetJobStatsResponse, error) {
						return &indexpb.GetJobStatsResponse{
							Status: &commonpb.Status{
								ErrorCode: commonpb.ErrorCode_UnexpectedError,
							},
						}, errors.New("error")
					},
				},
				2: &indexnode.Mock{
					CallGetJobStats: func(ctx context.Context, req *indexpb.GetJobStatsRequest) (*indexpb.GetJobStatsResponse, error) {
						return &indexpb.GetJobStatsResponse{
							Status: &commonpb.Status{
								ErrorCode: commonpb.ErrorCode_UnexpectedError,
							},
						}, errors.New("error")
					},
				},
				3: &indexnode.Mock{
					CallGetJobStats: func(ctx context.Context, req *indexpb.GetJobStatsRequest) (*indexpb.GetJobStatsResponse, error) {
						return &indexpb.GetJobStatsResponse{
							Status: &commonpb.Status{
								ErrorCode: commonpb.ErrorCode_UnexpectedError,
							},
						}, errors.New("error")
					},
				},
				4: &indexnode.Mock{
					CallGetJobStats: func(ctx context.Context, req *indexpb.GetJobStatsRequest) (*indexpb.GetJobStatsResponse, error) {
						return &indexpb.GetJobStatsResponse{
							Status: &commonpb.Status{
								ErrorCode: commonpb.ErrorCode_UnexpectedError,
							},
						}, errors.New("error")
					},
				},
				5: &indexnode.Mock{
					CallGetJobStats: func(ctx context.Context, req *indexpb.GetJobStatsRequest) (*indexpb.GetJobStatsResponse, error) {
						return &indexpb.GetJobStatsResponse{
							Status: &commonpb.Status{
								ErrorCode: commonpb.ErrorCode_UnexpectedError,
								Reason:    "fail reason",
							},
						}, nil
					},
				},
				6: &indexnode.Mock{
					CallGetJobStats: func(ctx context.Context, req *indexpb.GetJobStatsRequest) (*indexpb.GetJobStatsResponse, error) {
						return &indexpb.GetJobStatsResponse{
							Status: &commonpb.Status{
								ErrorCode: commonpb.ErrorCode_UnexpectedError,
								Reason:    "fail reason",
							},
						}, nil
					},
				},
				7: &indexnode.Mock{
					CallGetJobStats: func(ctx context.Context, req *indexpb.GetJobStatsRequest) (*indexpb.GetJobStatsResponse, error) {
						return &indexpb.GetJobStatsResponse{
							Status: &commonpb.Status{
								ErrorCode: commonpb.ErrorCode_UnexpectedError,
								Reason:    "fail reason",
							},
						}, nil
					},
				},
				8: &indexnode.Mock{
					CallGetJobStats: func(ctx context.Context, req *indexpb.GetJobStatsRequest) (*indexpb.GetJobStatsResponse, error) {
						return &indexpb.GetJobStatsResponse{
							TaskSlots: 1,
							Status: &commonpb.Status{
								ErrorCode: commonpb.ErrorCode_Success,
								Reason:    "",
							},
						}, nil
					},
				},
				9: &indexnode.Mock{
					CallGetJobStats: func(ctx context.Context, req *indexpb.GetJobStatsRequest) (*indexpb.GetJobStatsResponse, error) {
						return &indexpb.GetJobStatsResponse{
							TaskSlots: 10,
							Status: &commonpb.Status{
								ErrorCode: commonpb.ErrorCode_Success,
								Reason:    "",
							},
						}, nil
					},
				},
			},
		}

		nodeID, client := nm.PeekClient(&model.SegmentIndex{})
		assert.NotNil(t, client)
		assert.Contains(t, []UniqueID{8, 9}, nodeID)
	})
}

func TestIndexNodeManager_ClientSupportDisk(t *testing.T) {
	t.Run("support", func(t *testing.T) {
		nm := &IndexNodeManager{
			ctx:  context.Background(),
			lock: sync.RWMutex{},
			nodeClients: map[UniqueID]types.IndexNode{
				1: &indexnode.Mock{
					CallGetJobStats: func(ctx context.Context, in *indexpb.GetJobStatsRequest) (*indexpb.GetJobStatsResponse, error) {
						return &indexpb.GetJobStatsResponse{
							Status: &commonpb.Status{
								ErrorCode: commonpb.ErrorCode_Success,
								Reason:    "",
							},
							TaskSlots:  1,
							JobInfos:   nil,
							EnableDisk: true,
						}, nil
					},
				},
			},
		}

		support := nm.ClientSupportDisk()
		assert.True(t, support)
	})

	t.Run("not support", func(t *testing.T) {
		nm := &IndexNodeManager{
			ctx:  context.Background(),
			lock: sync.RWMutex{},
			nodeClients: map[UniqueID]types.IndexNode{
				1: &indexnode.Mock{
					CallGetJobStats: func(ctx context.Context, in *indexpb.GetJobStatsRequest) (*indexpb.GetJobStatsResponse, error) {
						return &indexpb.GetJobStatsResponse{
							Status: &commonpb.Status{
								ErrorCode: commonpb.ErrorCode_Success,
								Reason:    "",
							},
							TaskSlots:  1,
							JobInfos:   nil,
							EnableDisk: false,
						}, nil
					},
				},
			},
		}

		support := nm.ClientSupportDisk()
		assert.False(t, support)
	})

	t.Run("no indexnode", func(t *testing.T) {
		nm := &IndexNodeManager{
			ctx:         context.Background(),
			lock:        sync.RWMutex{},
			nodeClients: map[UniqueID]types.IndexNode{},
		}

		support := nm.ClientSupportDisk()
		assert.False(t, support)
	})

	t.Run("error", func(t *testing.T) {
		nm := &IndexNodeManager{
			ctx:  context.Background(),
			lock: sync.RWMutex{},
			nodeClients: map[UniqueID]types.IndexNode{
				1: &indexnode.Mock{
					CallGetJobStats: func(ctx context.Context, in *indexpb.GetJobStatsRequest) (*indexpb.GetJobStatsResponse, error) {
						return nil, errors.New("error")
					},
				},
			},
		}

		support := nm.ClientSupportDisk()
		assert.False(t, support)
	})

	t.Run("fail reason", func(t *testing.T) {
		nm := &IndexNodeManager{
			ctx:  context.Background(),
			lock: sync.RWMutex{},
			nodeClients: map[UniqueID]types.IndexNode{
				1: &indexnode.Mock{
					CallGetJobStats: func(ctx context.Context, in *indexpb.GetJobStatsRequest) (*indexpb.GetJobStatsResponse, error) {
						return &indexpb.GetJobStatsResponse{
							Status: &commonpb.Status{
								ErrorCode: commonpb.ErrorCode_UnexpectedError,
								Reason:    "fail reason",
							},
							TaskSlots:  0,
							JobInfos:   nil,
							EnableDisk: false,
						}, nil
					},
				},
			},
		}

		support := nm.ClientSupportDisk()
		assert.False(t, support)
	})
}

func TestNodeManager_StoppingNode(t *testing.T) {
	nm := NewNodeManager(context.Background(), defaultIndexNodeCreatorFunc)
	err := nm.AddNode(1, "indexnode-1")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(nm.GetAllClients()))

	nm.StoppingNode(1)
	assert.Equal(t, 0, len(nm.GetAllClients()))
	assert.Equal(t, 1, len(nm.stoppingNodes))

	nm.RemoveNode(1)
	assert.Equal(t, 0, len(nm.GetAllClients()))
	assert.Equal(t, 0, len(nm.stoppingNodes))
}

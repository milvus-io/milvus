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

package rootcoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
)

func Test_hasPartitionTask_Prepare(t *testing.T) {
	t.Run("invalid msg type", func(t *testing.T) {
		task := &hasPartitionTask{
			Req: &milvuspb.HasPartitionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Undefined,
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		task := &hasPartitionTask{
			Req: &milvuspb.HasPartitionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_HasPartition,
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func Test_hasPartitionTask_Execute(t *testing.T) {
	t.Run("fail to get collection", func(t *testing.T) {
		core := newTestCore(withInvalidMeta())
		task := &hasPartitionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.HasPartitionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_HasPartition,
				},
				CollectionName: "test coll",
			},
			Rsp: &milvuspb.BoolResponse{},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
		assert.Equal(t, task.Rsp.GetStatus().GetErrorCode(), commonpb.ErrorCode_CollectionNotExists)
		assert.False(t, task.Rsp.GetValue())
	})

	t.Run("failed", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(&model.Collection{
			Partitions: []*model.Partition{
				{
					PartitionName: "invalid test partition",
				},
			},
		}, nil)

		core := newTestCore(withMeta(meta))
		task := &hasPartitionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.HasPartitionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_HasCollection,
				},
				CollectionName: "test coll",
				PartitionName:  "test partition",
			},
			Rsp: &milvuspb.BoolResponse{},
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, task.Rsp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
		assert.False(t, task.Rsp.GetValue())
	})

	t.Run("success", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(&model.Collection{
			Partitions: []*model.Partition{
				{
					PartitionName: "invalid test partition",
				},
				{
					PartitionName: "test partition",
				},
			},
		}, nil)

		core := newTestCore(withMeta(meta))
		task := &hasPartitionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.HasPartitionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_HasCollection,
				},
				CollectionName: "test coll",
				PartitionName:  "test partition",
			},
			Rsp: &milvuspb.BoolResponse{},
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, task.Rsp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
		assert.True(t, task.Rsp.GetValue())
	})
}

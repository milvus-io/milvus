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
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func Test_showPartitionTask_Prepare(t *testing.T) {
	t.Run("invalid msg type", func(t *testing.T) {
		task := &showPartitionTask{
			Req: &milvuspb.ShowPartitionsRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Undefined,
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		task := &showPartitionTask{
			Req: &milvuspb.ShowPartitionsRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_ShowPartitions,
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func Test_showPartitionTask_Execute(t *testing.T) {
	t.Run("failed to list collections by name", func(t *testing.T) {
		metaTable := mockrootcoord.NewIMetaTable(t)
		metaTable.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, "test coll", mock.Anything).Return(nil, merr.WrapErrCollectionNotFound("test coll"))
		core := newTestCore(withMeta(metaTable))
		task := &showPartitionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.ShowPartitionsRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_ShowPartitions,
				},
				CollectionName: "test coll",
			},
			Rsp: &milvuspb.ShowPartitionsResponse{},
		}
		err := task.Execute(context.Background())
		assert.ErrorIs(t, err, merr.ErrCollectionNotFound)
		assert.ErrorIs(t, merr.Error(task.Rsp.GetStatus()), merr.ErrCollectionNotFound)
	})

	t.Run("failed to list collections by id", func(t *testing.T) {
		metaTable := mockrootcoord.NewIMetaTable(t)
		metaTable.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, merr.WrapErrCollectionNotFound(1))
		core := newTestCore(withMeta(metaTable))
		task := &showPartitionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.ShowPartitionsRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_ShowPartitions,
				},
				CollectionID: 1,
			},
			Rsp: &milvuspb.ShowPartitionsResponse{},
		}
		err := task.Execute(context.Background())
		assert.ErrorIs(t, err, merr.ErrCollectionNotFound)
		assert.ErrorIs(t, merr.Error(task.Rsp.GetStatus()), merr.ErrCollectionNotFound)
	})

	t.Run("success", func(t *testing.T) {
		meta := newMockMetaTable()
		meta.GetCollectionByIDFunc = func(ctx context.Context, collectionID typeutil.UniqueID, ts Timestamp, allowUnavailable bool) (*model.Collection, error) {
			return &model.Collection{
				CollectionID: collectionID,
				Name:         "test coll",
				Partitions: []*model.Partition{
					{
						PartitionID:   1,
						PartitionName: "test partition1",
					},
					{
						PartitionID:   2,
						PartitionName: "test partition2",
					},
				},
			}, nil
		}
		core := newTestCore(withMeta(meta))
		task := &showPartitionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.ShowPartitionsRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_ShowPartitions,
				},
				CollectionID: 1,
			},
			Rsp: &milvuspb.ShowPartitionsResponse{},
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, task.Rsp.GetStatus().GetErrorCode())
		assert.Equal(t, 2, len(task.Rsp.GetPartitionNames()))
	})
}

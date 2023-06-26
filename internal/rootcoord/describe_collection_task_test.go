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
	"github.com/milvus-io/milvus/pkg/util/funcutil"
)

func Test_describeCollectionTask_Prepare(t *testing.T) {
	t.Run("invalid msg type", func(t *testing.T) {
		task := &describeCollectionTask{
			Req: &milvuspb.DescribeCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_DropCollection,
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		task := &describeCollectionTask{
			Req: &milvuspb.DescribeCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_DescribeCollection,
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func Test_describeCollectionTask_Execute(t *testing.T) {
	t.Run("failed to get collection by name", func(t *testing.T) {
		core := newTestCore(withInvalidMeta())
		task := &describeCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.DescribeCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_DescribeCollection,
				},
				CollectionName: "test coll",
			},
			Rsp: &milvuspb.DescribeCollectionResponse{},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("failed to get collection by id", func(t *testing.T) {
		core := newTestCore(withInvalidMeta())
		task := &describeCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.DescribeCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_DescribeCollection,
				},
				CollectionID: 1,
			},
			Rsp: &milvuspb.DescribeCollectionResponse{},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		alias1, alias2 := funcutil.GenRandomStr(), funcutil.GenRandomStr()
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("GetCollectionByID",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(&model.Collection{
			CollectionID: 1,
			Name:         "test coll",
		}, nil)
		meta.On("ListAliasesByID",
			mock.Anything,
		).Return([]string{alias1, alias2})

		core := newTestCore(withMeta(meta))
		task := &describeCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.DescribeCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_DescribeCollection,
				},
				CollectionID: 1,
			},
			Rsp: &milvuspb.DescribeCollectionResponse{},
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, task.Rsp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
		assert.ElementsMatch(t, []string{alias1, alias2}, task.Rsp.GetAliases())
	})
}

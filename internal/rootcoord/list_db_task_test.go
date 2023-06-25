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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func Test_ListDBTask(t *testing.T) {
	t.Run("list db fails", func(t *testing.T) {
		core := newTestCore(withInvalidMeta())
		task := &listDatabaseTask{
			baseTask: newBaseTask(context.TODO(), core),
			Req: &milvuspb.ListDatabasesRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_ListDatabases,
				},
			},
			Resp: &milvuspb.ListDatabasesResponse{},
		}

		err := task.Prepare(context.Background())
		assert.NoError(t, err)

		err = task.Execute(context.Background())
		assert.Error(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, task.Resp.Status.ErrorCode)
	})

	t.Run("ok", func(t *testing.T) {
		ret := []*model.Database{model.NewDefaultDatabase()}
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("ListDatabases",
			mock.Anything,
			mock.Anything).
			Return(ret, nil)

		core := newTestCore(withMeta(meta))
		task := &listDatabaseTask{
			baseTask: newBaseTask(context.TODO(), core),
			Req: &milvuspb.ListDatabasesRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_ListDatabases,
				},
			},
			Resp: &milvuspb.ListDatabasesResponse{},
		}

		err := task.Prepare(context.Background())
		assert.NoError(t, err)

		err = task.Execute(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 1, len(task.Resp.GetDbNames()))
		assert.Equal(t, ret[0].Name, task.Resp.GetDbNames()[0])
		assert.Equal(t, commonpb.ErrorCode_Success, task.Resp.Status.ErrorCode)
	})
}

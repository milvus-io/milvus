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
	"github.com/milvus-io/milvus/internal/metastore/model"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util"
)

func Test_describeDatabaseTask_Execute(t *testing.T) {
	t.Run("failed to get database by name", func(t *testing.T) {
		core := newTestCore(withInvalidMeta())
		task := &describeDBTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &rootcoordpb.DescribeDatabaseRequest{
				DbName: "testDB",
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
		assert.NotNil(t, task.Rsp)
		assert.NotNil(t, task.Rsp.Status)
	})

	t.Run("describe with empty database name", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetDatabaseByName(mock.Anything, mock.Anything, mock.Anything).
			Return(model.NewDefaultDatabase(nil), nil)
		core := newTestCore(withMeta(meta))

		task := &describeDBTask{
			baseTask: newBaseTask(context.Background(), core),
			Req:      &rootcoordpb.DescribeDatabaseRequest{},
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
		assert.NotNil(t, task.Rsp)
		assert.Equal(t, task.Rsp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
		assert.Equal(t, util.DefaultDBName, task.Rsp.GetDbName())
		assert.Equal(t, util.DefaultDBID, task.Rsp.GetDbID())
	})

	t.Run("describe with specified database name", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetDatabaseByName(mock.Anything, mock.Anything, mock.Anything).
			Return(&model.Database{
				Name:        "db1",
				ID:          100,
				CreatedTime: 1,
			}, nil)
		core := newTestCore(withMeta(meta))

		task := &describeDBTask{
			baseTask: newBaseTask(context.Background(), core),
			Req:      &rootcoordpb.DescribeDatabaseRequest{DbName: "db1"},
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
		assert.NotNil(t, task.Rsp)
		assert.Equal(t, task.Rsp.GetStatus().GetCode(), int32(commonpb.ErrorCode_Success))
		assert.Equal(t, "db1", task.Rsp.GetDbName())
		assert.Equal(t, int64(100), task.Rsp.GetDbID())
		assert.Equal(t, uint64(1), task.Rsp.GetCreatedTimestamp())
	})
}

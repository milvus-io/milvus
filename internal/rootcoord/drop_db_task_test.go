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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/v2/util"
)

func Test_DropDBTask(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("DropDatabase",
			mock.Anything,
			mock.Anything,
			mock.Anything).
			Return(nil)

		core := newTestCore(withMeta(meta), withValidProxyManager())
		task := &dropDatabaseTask{
			baseTask: newBaseTask(context.TODO(), core),
			Req: &milvuspb.DropDatabaseRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_DropDatabase,
				},
				DbName: "db",
			},
		}

		err := task.Prepare(context.Background())
		assert.NoError(t, err)

		err = task.Execute(context.Background())
		assert.NoError(t, err)
	})

	t.Run("default db", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		core := newTestCore(withMeta(meta))
		task := &dropDatabaseTask{
			baseTask: newBaseTask(context.TODO(), core),
			Req: &milvuspb.DropDatabaseRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_DropDatabase,
				},
				DbName: util.DefaultDBName,
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("drop db fail", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().DropDatabase(
			mock.Anything,
			mock.Anything,
			mock.Anything).
			Return(errors.New("mock drop db error"))

		core := newTestCore(withMeta(meta))
		task := &dropDatabaseTask{
			baseTask: newBaseTask(context.TODO(), core),
			Req: &milvuspb.DropDatabaseRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_DropDatabase,
				},
				DbName: "db",
			},
		}

		err := task.Execute(context.Background())
		assert.Error(t, err)
	})
}

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
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func Test_CreateDBTask_Prepare(t *testing.T) {
	paramtable.Init()
	t.Run("list database fail", func(t *testing.T) {
		core := newTestCore(withInvalidMeta())
		task := &createDatabaseTask{
			baseTask: newBaseTask(context.TODO(), core),
			Req: &milvuspb.CreateDatabaseRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_CreateDatabase,
				},
				DbName: "db",
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("check database number fail", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		cfgMaxDatabaseNum := Params.RootCoordCfg.MaxDatabaseNum.GetAsInt()
		len := cfgMaxDatabaseNum + 1
		dbs := make([]*model.Database, 0, len)
		for i := 0; i < len; i++ {
			dbs = append(dbs, model.NewDefaultDatabase())
		}
		meta.On("ListDatabases",
			mock.Anything,
			mock.Anything).
			Return(dbs, nil)

		core := newTestCore(withMeta(meta))

		task := &createDatabaseTask{
			baseTask: newBaseTask(context.TODO(), core),
			Req: &milvuspb.CreateDatabaseRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_CreateDatabase,
				},
				DbName: "db",
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("ListDatabases",
			mock.Anything,
			mock.Anything).
			Return([]*model.Database{model.NewDefaultDatabase()}, nil)

		core := newTestCore(withMeta(meta), withValidIDAllocator())
		paramtable.Get().Save(Params.RootCoordCfg.MaxDatabaseNum.Key, strconv.Itoa(10))
		defer paramtable.Get().Reset(Params.RootCoordCfg.MaxDatabaseNum.Key)

		task := &createDatabaseTask{
			baseTask: newBaseTask(context.TODO(), core),
			Req: &milvuspb.CreateDatabaseRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_CreateDatabase,
				},
				DbName: "db",
			},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func Test_CreateDBTask_Execute(t *testing.T) {
	meta := mockrootcoord.NewIMetaTable(t)
	meta.On("CreateDatabase",
		mock.Anything,
		mock.Anything,
		mock.Anything).
		Return(nil)

	core := newTestCore(withMeta(meta))
	task := &createDatabaseTask{
		baseTask: newBaseTask(context.TODO(), core),
		Req: &milvuspb.CreateDatabaseRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_CreateDatabase,
			},
			DbName: "db",
		},
	}
	err := task.Execute(context.Background())
	assert.NoError(t, err)
}

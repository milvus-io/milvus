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
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/common"
)

func Test_alterDatabaseTask_Prepare(t *testing.T) {
	t.Run("invalid collectionID", func(t *testing.T) {
		task := &alterDatabaseTask{Req: &rootcoordpb.AlterDatabaseRequest{}}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		task := &alterDatabaseTask{
			Req: &rootcoordpb.AlterDatabaseRequest{
				DbName: "cn",
			},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func Test_alterDatabaseTask_Execute(t *testing.T) {
	properties := []*commonpb.KeyValuePair{
		{
			Key:   common.CollectionTTLConfigKey,
			Value: "3600",
		},
	}

	t.Run("properties is empty", func(t *testing.T) {
		task := &alterDatabaseTask{Req: &rootcoordpb.AlterDatabaseRequest{}}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("failed to create alias", func(t *testing.T) {
		core := newTestCore(withInvalidMeta())
		task := &alterDatabaseTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &rootcoordpb.AlterDatabaseRequest{
				DbName:     "cn",
				Properties: properties,
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	meta := mockrootcoord.NewIMetaTable(t)
	properties = append(properties, &commonpb.KeyValuePair{Key: common.DatabaseForceDenyReadingKey, Value: "true"})

	meta.On("GetDatabaseByName",
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(&model.Database{ID: int64(1), Properties: properties}, nil).Maybe()
	meta.On("AlterDatabase",
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(nil).Maybe()

	t.Run("alter skip due to no change", func(t *testing.T) {
		core := newTestCore(withMeta(meta))
		task := &alterDatabaseTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &rootcoordpb.AlterDatabaseRequest{
				DbName: "cn",
				Properties: []*commonpb.KeyValuePair{
					{
						Key:   common.DatabaseForceDenyReadingKey,
						Value: "true",
					},
				},
			},
		}

		err := task.Execute(context.Background())
		assert.NoError(t, err)
	})

	t.Run("alter step failed", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("GetDatabaseByName",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(&model.Database{ID: int64(1)}, nil)
		meta.On("AlterDatabase",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(errors.New("err"))

		core := newTestCore(withMeta(meta))
		task := &alterDatabaseTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &rootcoordpb.AlterDatabaseRequest{
				DbName:     "cn",
				Properties: properties,
			},
		}

		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("alter successfully", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("GetDatabaseByName",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(&model.Database{ID: int64(1)}, nil)
		meta.On("AlterDatabase",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)

		core := newTestCore(withMeta(meta))
		task := &alterDatabaseTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &rootcoordpb.AlterDatabaseRequest{
				DbName:     "cn",
				Properties: properties,
			},
		}

		err := task.Execute(context.Background())
		assert.NoError(t, err)
	})

	t.Run("test update collection props", func(t *testing.T) {
		oldProps := []*commonpb.KeyValuePair{
			{
				Key:   common.CollectionTTLConfigKey,
				Value: "1",
			},
		}

		updateProps1 := []*commonpb.KeyValuePair{
			{
				Key:   common.CollectionAutoCompactionKey,
				Value: "true",
			},
		}

		ret := MergeProperties(oldProps, updateProps1)

		assert.Contains(t, ret, &commonpb.KeyValuePair{
			Key:   common.CollectionTTLConfigKey,
			Value: "1",
		})

		assert.Contains(t, ret, &commonpb.KeyValuePair{
			Key:   common.CollectionAutoCompactionKey,
			Value: "true",
		})

		updateProps2 := []*commonpb.KeyValuePair{
			{
				Key:   common.CollectionTTLConfigKey,
				Value: "2",
			},
		}
		ret2 := MergeProperties(ret, updateProps2)

		assert.Contains(t, ret2, &commonpb.KeyValuePair{
			Key:   common.CollectionTTLConfigKey,
			Value: "2",
		})

		assert.Contains(t, ret2, &commonpb.KeyValuePair{
			Key:   common.CollectionAutoCompactionKey,
			Value: "true",
		})
	})
}

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

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type AlterDynamicSchemaTaskSuite struct {
	suite.Suite
	meta *mockrootcoord.IMetaTable
}

func (s *AlterDynamicSchemaTaskSuite) getDisabledCollection() *model.Collection {
	return &model.Collection{
		CollectionID: 1,
		Name:         "coll_disabled",
		Fields: []*model.Field{
			{
				Name:         "pk",
				FieldID:      100,
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				Name:     "vec",
				FieldID:  101,
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "768",
					},
				},
			},
		},
		EnableDynamicField:   false,
		PhysicalChannelNames: []string{"dml_ch_01", "dml_ch_02"},
		VirtualChannelNames:  []string{"dml_ch_01", "dml_ch_02"},
	}
}

func (s *AlterDynamicSchemaTaskSuite) SetupTest() {
	s.meta = mockrootcoord.NewIMetaTable(s.T())
	s.meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, "not_existed_coll", mock.Anything).Return(nil, merr.WrapErrCollectionNotFound("not_existed_coll")).Maybe()
	s.meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, "coll_disabled", mock.Anything).Return(s.getDisabledCollection(), nil).Maybe()
	s.meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, "coll_enabled", mock.Anything).Return(&model.Collection{
		CollectionID: 1,
		Name:         "coll_enabled",
		Fields: []*model.Field{
			{
				Name:         "pk",
				FieldID:      100,
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				Name:     "vec",
				FieldID:  101,
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "768",
					},
				},
			},
			{
				Name:      "$meta",
				IsDynamic: true,
				DataType:  schemapb.DataType_JSON,
			},
		},
		EnableDynamicField: true,
	}, nil).Maybe()
}

func (s *AlterDynamicSchemaTaskSuite) TestPrepare() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("invalid_msg_type", func() {
		task := &alterDynamicFieldTask{Req: &milvuspb.AlterCollectionRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection}}}
		err := task.Prepare(ctx)
		s.Error(err)
	})

	s.Run("alter_with_other_properties", func() {
		task := &alterDynamicFieldTask{Req: &milvuspb.AlterCollectionRequest{
			Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
			Properties: []*commonpb.KeyValuePair{
				{
					Key:   common.EnableDynamicSchemaKey,
					Value: "true",
				},
				{
					Key:   "other_keys",
					Value: "other_values",
				},
			},
		}}
		err := task.Prepare(ctx)
		s.Error(err)
	})

	s.Run("disable_dynamic_field_for_disabled_coll", func() {
		core := newTestCore(withMeta(s.meta))
		task := &alterDynamicFieldTask{
			baseTask:    newBaseTask(ctx, core),
			Req:         &milvuspb.AlterCollectionRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterCollection}, CollectionName: "coll_disabled"},
			targetValue: false,
		}
		err := task.Prepare(ctx)
		s.NoError(err, "disabling dynamic schema on diabled collection shall be a no-op")
	})

	s.Run("disable_dynamic_field_for_enabled_coll", func() {
		core := newTestCore(withMeta(s.meta))
		task := &alterDynamicFieldTask{
			baseTask:    newBaseTask(ctx, core),
			Req:         &milvuspb.AlterCollectionRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterCollection}, CollectionName: "coll_enabled"},
			targetValue: false,
		}
		err := task.Prepare(ctx)
		s.Error(err, "disabling dynamic schema on enabled collection not supported yet")
	})

	s.Run("enable_dynamic_field_for_enabled_coll", func() {
		core := newTestCore(withMeta(s.meta))
		task := &alterDynamicFieldTask{
			baseTask:    newBaseTask(ctx, core),
			Req:         &milvuspb.AlterCollectionRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterCollection}, CollectionName: "coll_enabled"},
			targetValue: true,
		}
		err := task.Prepare(ctx)
		s.NoError(err)
	})

	s.Run("collection_not_exist", func() {
		core := newTestCore(withMeta(s.meta))
		task := &alterDynamicFieldTask{
			baseTask:    newBaseTask(ctx, core),
			Req:         &milvuspb.AlterCollectionRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterCollection}, CollectionName: "not_existed_coll"},
			targetValue: true,
		}
		err := task.Prepare(ctx)
		s.Error(err)
	})

	s.Run("normal_case", func() {
		core := newTestCore(withMeta(s.meta))
		task := &alterDynamicFieldTask{
			baseTask:    newBaseTask(ctx, core),
			Req:         &milvuspb.AlterCollectionRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterCollection}, CollectionName: "coll_disabled"},
			targetValue: true,
		}
		err := task.Prepare(ctx)
		s.NoError(err)
		s.NotNil(task.fieldSchema)
	})
}

func (s *AlterDynamicSchemaTaskSuite) TestExecute() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("no_op", func() {
		core := newTestCore(withMeta(s.meta))
		task := &alterDynamicFieldTask{
			baseTask:    newBaseTask(ctx, core),
			Req:         &milvuspb.AlterCollectionRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterCollection}, CollectionName: "coll_disabled"},
			oldColl:     s.getDisabledCollection(),
			targetValue: false,
		}
		err := task.Execute(ctx)
		s.NoError(err)
	})

	s.Run("normal_case", func() {
		b := mock_streaming.NewMockBroadcast(s.T())
		wal := mock_streaming.NewMockWALAccesser(s.T())
		wal.EXPECT().Broadcast().Return(b).Maybe()
		streaming.SetWALForTest(wal)

		b.EXPECT().Append(mock.Anything, mock.Anything).Return(&types.BroadcastAppendResult{
			AppendResults: map[string]*types.AppendResult{
				"dml_ch_01": {TimeTick: 100},
				"dml_ch_02": {TimeTick: 101},
			},
		}, nil).Times(1)

		s.meta.EXPECT().AlterCollection(
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		s.meta.EXPECT().ListAliasesByID(mock.Anything, mock.Anything).Return([]string{})

		broker := newMockBroker()
		broker.BroadcastAlteredCollectionFunc = func(ctx context.Context, req *milvuspb.AlterCollectionRequest) error {
			return nil
		}
		alloc := newMockIDAllocator()
		core := newTestCore(withValidProxyManager(), withMeta(s.meta), withBroker(broker), withIDAllocator(alloc))

		task := &alterDynamicFieldTask{
			baseTask:    newBaseTask(ctx, core),
			Req:         &milvuspb.AlterCollectionRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterCollection}, CollectionName: "coll_disabled"},
			targetValue: true,
		}
		err := task.Prepare(ctx)
		s.NoError(err)
		err = task.Execute(ctx)
		s.NoError(err)
	})
}

func TestAlterDynamicSchemaTask(t *testing.T) {
	suite.Run(t, new(AlterDynamicSchemaTaskSuite))
}

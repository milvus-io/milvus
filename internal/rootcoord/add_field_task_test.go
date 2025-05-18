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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func Test_AddCollectionFieldTask_Prepare(t *testing.T) {
	t.Run("invalid msg type", func(t *testing.T) {
		task := &addCollectionFieldTask{Req: &milvuspb.AddCollectionFieldRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection}}}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("check field failed", func(t *testing.T) {
		fieldSchema := &schemapb.FieldSchema{
			DataType: schemapb.DataType_Int64,
			DefaultValue: &schemapb.ValueField{
				Data: &schemapb.ValueField_BoolData{
					BoolData: false,
				},
			},
		}
		bytes, err := proto.Marshal(fieldSchema)
		assert.NoError(t, err)
		task := &addCollectionFieldTask{Req: &milvuspb.AddCollectionFieldRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_AddCollectionField}, Schema: bytes}}
		err = task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		fieldSchema := &schemapb.FieldSchema{
			DataType: schemapb.DataType_Bool,
			DefaultValue: &schemapb.ValueField{
				Data: &schemapb.ValueField_BoolData{
					BoolData: false,
				},
			},
		}
		bytes, err := proto.Marshal(fieldSchema)
		assert.NoError(t, err)
		task := &addCollectionFieldTask{Req: &milvuspb.AddCollectionFieldRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_AddCollectionField}, Schema: bytes}}
		err = task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func Test_AddCollectionFieldTask_Execute(t *testing.T) {
	b := mock_streaming.NewMockBroadcast(t)
	wal := mock_streaming.NewMockWALAccesser(t)
	wal.EXPECT().Broadcast().Return(b).Maybe()
	streaming.SetWALForTest(wal)

	testCollection := &model.Collection{
		CollectionID: int64(1),
		Fields: []*model.Field{
			{
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				Name:     "vec",
				DataType: schemapb.DataType_FloatVector,
			},
		},
		PhysicalChannelNames: []string{"dml_ch_01", "dml_ch_02"},
		VirtualChannelNames:  []string{"dml_ch_01", "dml_ch_02"},
	}

	t.Run("failed_to_get_collection", func(t *testing.T) {
		metaTable := mockrootcoord.NewIMetaTable(t)
		metaTable.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, "not_existed_coll", mock.Anything).Return(nil, merr.WrapErrCollectionNotFound("not_existed_coll"))
		core := newTestCore(withMeta(metaTable))
		task := &addCollectionFieldTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.AddCollectionFieldRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterAlias},
				CollectionName: "not_existed_coll",
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err, "error shall be return when get collection failed")
	})

	t.Run("write_wal_fail", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByName(
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(testCollection, nil)
		meta.EXPECT().ListAliasesByID(mock.Anything, mock.Anything).Return([]string{})

		broker := newMockBroker()
		broker.BroadcastAlteredCollectionFunc = func(ctx context.Context, req *milvuspb.AlterCollectionRequest) error {
			return errors.New("mock")
		}
		alloc := newMockIDAllocator()
		core := newTestCore(withValidProxyManager(), withMeta(meta), withBroker(broker), withIDAllocator(alloc))
		task := &addCollectionFieldTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.AddCollectionFieldRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterAlias},
				CollectionName: "coll",
			},
			fieldSchema: &schemapb.FieldSchema{
				Name:     "fid",
				DataType: schemapb.DataType_Bool,
				Nullable: true,
			},
		}

		t.Run("write_schema_change_fail", func(t *testing.T) {
			b.EXPECT().Append(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

			err := task.Execute(context.Background())
			assert.Error(t, err)
		})
	})

	t.Run("add field step failed", func(t *testing.T) {
		b.EXPECT().Append(mock.Anything, mock.Anything).Return(&types.BroadcastAppendResult{
			AppendResults: map[string]*types.AppendResult{
				"dml_ch_01": {TimeTick: 100},
				"dml_ch_02": {TimeTick: 101},
			},
		}, nil).Times(1)
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByName(
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(testCollection, nil)
		meta.EXPECT().AlterCollection(
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(errors.New("mock"))
		meta.EXPECT().ListAliasesByID(mock.Anything, mock.Anything).Return([]string{})
		alloc := newMockIDAllocator()
		core := newTestCore(withValidProxyManager(), withMeta(meta), withIDAllocator(alloc))
		task := &addCollectionFieldTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.AddCollectionFieldRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterAlias},
				CollectionName: "coll",
			},
			fieldSchema: &schemapb.FieldSchema{
				Name:     "fid",
				DataType: schemapb.DataType_Bool,
				Nullable: true,
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("broadcast add field step failed", func(t *testing.T) {
		b.EXPECT().Append(mock.Anything, mock.Anything).Return(&types.BroadcastAppendResult{
			AppendResults: map[string]*types.AppendResult{
				"dml_ch_01": {TimeTick: 100},
				"dml_ch_02": {TimeTick: 101},
			},
		}, nil).Times(1)

		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByName(
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(testCollection, nil)
		meta.EXPECT().AlterCollection(
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		meta.EXPECT().ListAliasesByID(mock.Anything, mock.Anything).Return([]string{})

		broker := newMockBroker()
		broker.BroadcastAlteredCollectionFunc = func(ctx context.Context, req *milvuspb.AlterCollectionRequest) error {
			return errors.New("mock")
		}
		alloc := newMockIDAllocator()
		core := newTestCore(withValidProxyManager(), withMeta(meta), withBroker(broker), withIDAllocator(alloc))
		task := &addCollectionFieldTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.AddCollectionFieldRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterAlias},
				CollectionName: "coll",
			},
			fieldSchema: &schemapb.FieldSchema{
				Name:     "fid",
				DataType: schemapb.DataType_Bool,
				Nullable: true,
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("expire cache failed", func(t *testing.T) {
		b.EXPECT().Append(mock.Anything, mock.Anything).Return(&types.BroadcastAppendResult{
			AppendResults: map[string]*types.AppendResult{
				"dml_ch_01": {TimeTick: 100},
				"dml_ch_02": {TimeTick: 101},
			},
		}, nil).Times(1)

		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByName(
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(testCollection, nil)
		meta.EXPECT().AlterCollection(
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		meta.EXPECT().ListAliasesByID(mock.Anything, mock.Anything).Return([]string{})

		broker := newMockBroker()
		broker.BroadcastAlteredCollectionFunc = func(ctx context.Context, req *milvuspb.AlterCollectionRequest) error {
			return nil
		}

		alloc := newMockIDAllocator()
		core := newTestCore(withInvalidProxyManager(), withMeta(meta), withBroker(broker), withIDAllocator(alloc))
		task := &addCollectionFieldTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.AddCollectionFieldRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterAlias},
				CollectionName: "coll",
			},
			fieldSchema: &schemapb.FieldSchema{
				Name:     "fid",
				DataType: schemapb.DataType_Bool,
				Nullable: true,
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		b.EXPECT().Append(mock.Anything, mock.Anything).Return(&types.BroadcastAppendResult{
			AppendResults: map[string]*types.AppendResult{
				"dml_ch_01": {TimeTick: 100},
				"dml_ch_02": {TimeTick: 101},
			},
		}, nil).Times(1)

		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByName(
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(testCollection, nil)
		meta.EXPECT().AlterCollection(
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		meta.EXPECT().ListAliasesByID(mock.Anything, mock.Anything).Return([]string{})

		broker := newMockBroker()
		broker.BroadcastAlteredCollectionFunc = func(ctx context.Context, req *milvuspb.AlterCollectionRequest) error {
			return nil
		}
		alloc := newMockIDAllocator()
		core := newTestCore(withValidProxyManager(), withMeta(meta), withBroker(broker), withIDAllocator(alloc))
		task := &addCollectionFieldTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.AddCollectionFieldRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_AddCollectionField},
				CollectionName: "coll",
			},
			fieldSchema: &schemapb.FieldSchema{
				Name:     "fid",
				DataType: schemapb.DataType_Bool,
				Nullable: true,
			},
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
	})
}

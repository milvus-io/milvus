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
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func Test_createCollectionTask_validate(t *testing.T) {
	t.Run("empty request", func(t *testing.T) {
		task := createCollectionTask{
			Req: nil,
		}
		err := task.validate()
		assert.Error(t, err)
	})

	t.Run("invalid msg type", func(t *testing.T) {
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
			},
		}
		err := task.validate()
		assert.Error(t, err)
	})

	t.Run("shard num exceeds max configuration", func(t *testing.T) {
		// TODO: better to have a `Set` method for ParamItem.
		cfgMaxShardNum := Params.RootCoordCfg.DmlChannelNum.GetAsInt32()
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:      &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				ShardsNum: cfgMaxShardNum + 1,
			},
		}
		err := task.validate()
		assert.Error(t, err)
	})

	t.Run("shard num exceeds limit", func(t *testing.T) {
		// TODO: better to have a `Set` method for ParamItem.
		cfgShardLimit := Params.ProxyCfg.MaxShardNum.GetAsInt32()
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:      &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				ShardsNum: cfgShardLimit + 1,
			},
		}
		err := task.validate()
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
			},
		}
		err := task.validate()
		assert.NoError(t, err)
	})
}

func Test_createCollectionTask_validateSchema(t *testing.T) {
	t.Run("name mismatch", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		otherName := collectionName + "_other"
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
		}
		schema := &schemapb.CollectionSchema{
			Name: otherName,
		}
		err := task.validateSchema(schema)
		assert.Error(t, err)
	})

	t.Run("has system fields", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
		}
		schema := &schemapb.CollectionSchema{
			Name: collectionName,
			Fields: []*schemapb.FieldSchema{
				{Name: RowIDFieldName},
			},
		}
		err := task.validateSchema(schema)
		assert.Error(t, err)
	})

	t.Run("default value type mismatch", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
		}
		schema1 := &schemapb.CollectionSchema{
			Name: collectionName,
			Fields: []*schemapb.FieldSchema{
				{
					DataType: schemapb.DataType_BinaryVector,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_BoolData{
							BoolData: false,
						},
					},
				},
			},
		}
		err1 := task.validateSchema(schema1)
		assert.ErrorIs(t, err1, merr.ErrParameterInvalid)

		schema2 := &schemapb.CollectionSchema{
			Name: collectionName,
			Fields: []*schemapb.FieldSchema{
				{
					DataType: schemapb.DataType_BinaryVector,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_IntData{
							IntData: 1,
						},
					},
				},
			},
		}
		err2 := task.validateSchema(schema2)
		assert.ErrorIs(t, err2, merr.ErrParameterInvalid)

		schema3 := &schemapb.CollectionSchema{
			Name: collectionName,
			Fields: []*schemapb.FieldSchema{
				{
					DataType: schemapb.DataType_BinaryVector,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_LongData{
							LongData: 1,
						},
					},
				},
			},
		}
		err3 := task.validateSchema(schema3)
		assert.ErrorIs(t, err3, merr.ErrParameterInvalid)

		schema4 := &schemapb.CollectionSchema{
			Name: collectionName,
			Fields: []*schemapb.FieldSchema{
				{
					DataType: schemapb.DataType_BinaryVector,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_FloatData{
							FloatData: 1,
						},
					},
				},
			},
		}
		err4 := task.validateSchema(schema4)
		assert.ErrorIs(t, err4, merr.ErrParameterInvalid)

		schema5 := &schemapb.CollectionSchema{
			Name: collectionName,
			Fields: []*schemapb.FieldSchema{
				{
					DataType: schemapb.DataType_BinaryVector,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_DoubleData{
							DoubleData: 1,
						},
					},
				},
			},
		}
		err5 := task.validateSchema(schema5)
		assert.ErrorIs(t, err5, merr.ErrParameterInvalid)

		schema6 := &schemapb.CollectionSchema{
			Name: collectionName,
			Fields: []*schemapb.FieldSchema{
				{
					DataType: schemapb.DataType_BinaryVector,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_StringData{
							StringData: "a",
						},
					},
				},
			},
		}
		err6 := task.validateSchema(schema6)
		assert.ErrorIs(t, err6, merr.ErrParameterInvalid)

		schema7 := &schemapb.CollectionSchema{
			Name: collectionName,
			Fields: []*schemapb.FieldSchema{
				{
					DataType: schemapb.DataType_Int16,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_IntData{
							IntData: math.MaxInt32,
						},
					},
				},
			},
		}
		err7 := task.validateSchema(schema7)
		assert.ErrorIs(t, err7, merr.ErrParameterInvalid)

		schema8 := &schemapb.CollectionSchema{
			Name: collectionName,
			Fields: []*schemapb.FieldSchema{
				{
					DataType: schemapb.DataType_Int8,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_IntData{
							IntData: math.MaxInt32,
						},
					},
				},
			},
		}
		err8 := task.validateSchema(schema8)
		assert.ErrorIs(t, err8, merr.ErrParameterInvalid)
	})

	t.Run("default value length exceeds", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
		}
		schema := &schemapb.CollectionSchema{
			Name: collectionName,
			Fields: []*schemapb.FieldSchema{
				{
					DataType: schemapb.DataType_VarChar,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxLengthKey,
							Value: "2",
						},
					},
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_StringData{
							StringData: "abc",
						},
					},
				},
			},
		}
		err := task.validateSchema(schema)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("normal case", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
		}
		schema := &schemapb.CollectionSchema{
			Name:   collectionName,
			Fields: []*schemapb.FieldSchema{},
		}
		err := task.validateSchema(schema)
		assert.NoError(t, err)
	})
}

func Test_createCollectionTask_prepareSchema(t *testing.T) {
	t.Run("failed to unmarshal", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
				Schema:         []byte("invalid schema"),
			},
		}
		err := task.prepareSchema()
		assert.Error(t, err)
	})

	t.Run("contain system fields", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		schema := &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{Name: TimeStampFieldName},
			},
		}
		marshaledSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
				Schema:         marshaledSchema,
			},
		}
		err = task.prepareSchema()
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		field1 := funcutil.GenRandomStr()
		schema := &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{Name: field1},
			},
		}
		marshaledSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
				Schema:         marshaledSchema,
			},
		}
		err = task.prepareSchema()
		assert.NoError(t, err)
	})
}

func Test_createCollectionTask_Prepare(t *testing.T) {
	t.Run("invalid msg type", func(t *testing.T) {
		task := &createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("invalid schema", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		task := &createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
				Schema:         []byte("invalid schema"),
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("failed to assign id", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		field1 := funcutil.GenRandomStr()
		schema := &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{Name: field1},
			},
		}
		marshaledSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)

		core := newTestCore(withInvalidIDAllocator())

		task := createCollectionTask{
			baseTask: baseTask{core: core},
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
				Schema:         marshaledSchema,
			},
		}
		err = task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		defer cleanTestEnv()

		collectionName := funcutil.GenRandomStr()
		field1 := funcutil.GenRandomStr()

		ticker := newRocksMqTtSynchronizer()

		core := newTestCore(withValidIDAllocator(), withTtSynchronizer(ticker))

		schema := &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{Name: field1},
			},
		}
		marshaledSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)

		task := createCollectionTask{
			baseTask: baseTask{core: core},
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
				Schema:         marshaledSchema,
			},
		}
		task.Req.ShardsNum = int32(Params.RootCoordCfg.DmlChannelNum.GetAsInt() + 1) // no enough channels.
		err = task.Prepare(context.Background())
		assert.Error(t, err)
		task.Req.ShardsNum = common.DefaultShardsNum
		err = task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func Test_createCollectionTask_Execute(t *testing.T) {
	t.Run("add same collection with different parameters", func(t *testing.T) {
		defer cleanTestEnv()
		ticker := newRocksMqTtSynchronizer()

		collectionName := funcutil.GenRandomStr()
		field1 := funcutil.GenRandomStr()
		coll := &model.Collection{Name: collectionName}

		meta := newMockMetaTable()
		meta.GetCollectionByNameFunc = func(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error) {
			return coll, nil
		}
		meta.ListCollectionsFunc = func(ctx context.Context, ts Timestamp) ([]*model.Collection, error) {
			return []*model.Collection{}, nil
		}

		core := newTestCore(withMeta(meta), withTtSynchronizer(ticker))

		task := &createCollectionTask{
			baseTask: baseTask{core: core},
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
			schema: &schemapb.CollectionSchema{Name: collectionName, Fields: []*schemapb.FieldSchema{{Name: field1}}},
		}

		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("add duplicate collection", func(t *testing.T) {
		defer cleanTestEnv()
		ticker := newRocksMqTtSynchronizer()
		shardNum := 2
		pchans := ticker.getDmlChannelNames(shardNum)

		collectionName := funcutil.GenRandomStr()
		field1 := funcutil.GenRandomStr()
		collID := UniqueID(1)
		schema := &schemapb.CollectionSchema{Name: collectionName, Fields: []*schemapb.FieldSchema{{Name: field1}}}
		channels := collectionChannels{
			virtualChannels:  []string{funcutil.GenRandomStr(), funcutil.GenRandomStr()},
			physicalChannels: pchans,
		}
		coll := &model.Collection{
			CollectionID:         collID,
			Name:                 schema.Name,
			Description:          schema.Description,
			AutoID:               schema.AutoID,
			Fields:               model.UnmarshalFieldModels(schema.GetFields()),
			VirtualChannelNames:  channels.virtualChannels,
			PhysicalChannelNames: channels.physicalChannels,
		}

		meta := newMockMetaTable()
		meta.GetCollectionByNameFunc = func(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error) {
			return coll, nil
		}
		meta.ListCollectionsFunc = func(ctx context.Context, ts Timestamp) ([]*model.Collection, error) {
			return []*model.Collection{}, nil
		}

		core := newTestCore(withMeta(meta), withTtSynchronizer(ticker))

		task := &createCollectionTask{
			baseTask: baseTask{core: core},
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
			collID:   collID,
			schema:   schema,
			channels: channels,
		}

		err := task.Execute(context.Background())
		assert.NoError(t, err)
	})

	t.Run("failed to get start positions", func(t *testing.T) {
		ticker := newTickerWithMockFailStream()
		shardNum := 2
		pchans := ticker.getDmlChannelNames(shardNum)
		core := newTestCore(withTtSynchronizer(ticker))
		task := &createCollectionTask{
			baseTask: baseTask{core: core},
			channels: collectionChannels{
				physicalChannels: pchans,
				virtualChannels:  []string{funcutil.GenRandomStr(), funcutil.GenRandomStr()},
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		defer cleanTestEnv()

		collectionName := funcutil.GenRandomStr()
		field1 := funcutil.GenRandomStr()
		shardNum := 2

		ticker := newRocksMqTtSynchronizer()
		pchans := ticker.getDmlChannelNames(shardNum)

		meta := newMockMetaTable()
		meta.GetCollectionByNameFunc = func(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error) {
			return nil, errors.New("error mock GetCollectionByName")
		}
		meta.AddCollectionFunc = func(ctx context.Context, coll *model.Collection) error {
			return nil
		}
		meta.ChangeCollectionStateFunc = func(ctx context.Context, collectionID UniqueID, state etcdpb.CollectionState, ts Timestamp) error {
			return nil
		}

		dc := newMockDataCoord()
		dc.GetComponentStatesFunc = func(ctx context.Context) (*milvuspb.ComponentStates, error) {
			return &milvuspb.ComponentStates{
				State: &milvuspb.ComponentInfo{
					NodeID:    TestRootCoordID,
					StateCode: commonpb.StateCode_Healthy,
				},
				SubcomponentStates: nil,
				Status:             succStatus(),
			}, nil
		}
		dc.WatchChannelsFunc = func(ctx context.Context, req *datapb.WatchChannelsRequest) (*datapb.WatchChannelsResponse, error) {
			return &datapb.WatchChannelsResponse{Status: succStatus()}, nil
		}

		core := newTestCore(withValidIDAllocator(),
			withMeta(meta),
			withTtSynchronizer(ticker),
			withValidProxyManager(),
			withDataCoord(dc))
		core.broker = newServerBroker(core)

		schema := &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{Name: field1},
			},
		}
		marshaledSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)

		task := createCollectionTask{
			baseTask: baseTask{core: core},
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				DbName:         "mock-db",
				CollectionName: collectionName,
				Schema:         marshaledSchema,
				ShardsNum:      int32(shardNum),
			},
			channels: collectionChannels{physicalChannels: pchans},
			schema:   schema,
		}

		meta.ListCollectionsFunc = func(ctx context.Context, ts Timestamp) ([]*model.Collection, error) {
			return nil, errors.New("mock error")
		}
		err = task.Execute(context.Background())
		assert.Error(t, err)

		originFormatter := Params.QuotaConfig.MaxCollectionNum.Formatter
		Params.QuotaConfig.MaxCollectionNum.Formatter = func(originValue string) string {
			return "10"
		}
		meta.ListCollectionsFunc = func(ctx context.Context, ts Timestamp) ([]*model.Collection, error) {
			maxNum := Params.QuotaConfig.MaxCollectionNum.GetAsInt()
			return make([]*model.Collection, maxNum), nil
		}
		err = task.Execute(context.Background())
		assert.Error(t, err)
		Params.QuotaConfig.MaxCollectionNum.Formatter = originFormatter

		meta.ListCollectionsFunc = func(ctx context.Context, ts Timestamp) ([]*model.Collection, error) {
			maxNum := Params.QuotaConfig.MaxCollectionNumPerDB.GetAsInt()
			collections := make([]*model.Collection, 0, maxNum)
			for i := 0; i < maxNum; i++ {
				collections = append(collections, &model.Collection{DBName: task.Req.GetDbName()})
			}
			return collections, nil
		}
		err = task.Execute(context.Background())
		assert.Error(t, err)
		assert.True(t, errors.Is(merr.ErrCollectionNumLimitExceeded, err))

		meta.ListCollectionsFunc = func(ctx context.Context, ts Timestamp) ([]*model.Collection, error) {
			return []*model.Collection{}, nil
		}

		err = task.Execute(context.Background())
		assert.NoError(t, err)
	})

	t.Run("partial error, check if undo worked", func(t *testing.T) {
		defer cleanTestEnv()

		collectionName := funcutil.GenRandomStr()
		field1 := funcutil.GenRandomStr()
		shardNum := 2

		ticker := newRocksMqTtSynchronizer()
		pchans := ticker.getDmlChannelNames(shardNum)

		meta := newMockMetaTable()
		meta.GetCollectionByNameFunc = func(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error) {
			return nil, errors.New("error mock GetCollectionByName")
		}
		meta.AddCollectionFunc = func(ctx context.Context, coll *model.Collection) error {
			return nil
		}
		meta.ListCollectionsFunc = func(ctx context.Context, ts Timestamp) ([]*model.Collection, error) {
			return []*model.Collection{}, nil
		}
		// inject error here.
		meta.ChangeCollectionStateFunc = func(ctx context.Context, collectionID UniqueID, state etcdpb.CollectionState, ts Timestamp) error {
			return errors.New("error mock ChangeCollectionState")
		}
		removeCollectionCalled := false
		removeCollectionChan := make(chan struct{}, 1)
		meta.RemoveCollectionFunc = func(ctx context.Context, collectionID UniqueID, ts Timestamp) error {
			removeCollectionCalled = true
			removeCollectionChan <- struct{}{}
			return nil
		}

		broker := newMockBroker()
		broker.WatchChannelsFunc = func(ctx context.Context, info *watchInfo) error {
			return nil
		}

		unwatchChannelsCalled := false
		unwatchChannelsChan := make(chan struct{}, 1)
		gc := mockrootcoord.NewGarbageCollector(t)
		gc.On("GcCollectionData",
			mock.Anything, // context.Context
			mock.Anything, // *model.Collection
		).Return(func(ctx context.Context, collection *model.Collection) (ddlTs Timestamp) {
			for _, pchan := range pchans {
				ticker.syncedTtHistogram.update(pchan, 101)
			}
			unwatchChannelsCalled = true
			unwatchChannelsChan <- struct{}{}
			return 100
		}, nil)

		core := newTestCore(withValidIDAllocator(),
			withMeta(meta),
			withTtSynchronizer(ticker),
			withGarbageCollector(gc),
			withValidProxyManager(),
			withBroker(broker))

		schema := &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{Name: field1},
			},
		}
		marshaledSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)

		task := createCollectionTask{
			baseTask: baseTask{core: core},
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
				Schema:         marshaledSchema,
				ShardsNum:      int32(shardNum),
			},
			channels: collectionChannels{physicalChannels: pchans},
			schema:   schema,
		}

		err = task.Execute(context.Background())
		assert.Error(t, err)

		// check if undo worked.

		// undo watch.
		<-unwatchChannelsChan
		assert.True(t, unwatchChannelsCalled)

		// undo adding collection.
		<-removeCollectionChan
		assert.True(t, removeCollectionCalled)

		time.Sleep(time.Second * 2) // wait for asynchronous step done.
		// undo add channels.
		assert.Zero(t, len(ticker.listDmlChannels()))
	})
}

func Test_createCollectionTask_PartitionKey(t *testing.T) {
	defer cleanTestEnv()

	collectionName := funcutil.GenRandomStr()
	field1 := funcutil.GenRandomStr()
	ticker := newRocksMqTtSynchronizer()

	meta := mockrootcoord.NewIMetaTable(t)
	core := newTestCore(withValidIDAllocator(), withTtSynchronizer(ticker), withMeta(meta))

	partitionKeyField := &schemapb.FieldSchema{
		Name:           field1,
		DataType:       schemapb.DataType_Int64,
		IsPartitionKey: true,
	}

	schema := &schemapb.CollectionSchema{
		Name:        collectionName,
		Description: "",
		AutoID:      false,
		Fields:      []*schemapb.FieldSchema{partitionKeyField},
	}
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	task := createCollectionTask{
		baseTask: newBaseTask(context.TODO(), core),
		Req: &milvuspb.CreateCollectionRequest{
			Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
			CollectionName: collectionName,
			Schema:         marshaledSchema,
			ShardsNum:      common.DefaultShardsNum,
		},
	}

	t.Run("without num partition", func(t *testing.T) {
		task.Req.NumPartitions = 0
		err = task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("num partition too large", func(t *testing.T) {
		task.Req.NumPartitions = Params.RootCoordCfg.MaxPartitionNum.GetAsInt64() + 1
		err = task.Prepare(context.Background())
		assert.Error(t, err)
	})

	task.Req.NumPartitions = common.DefaultPartitionsWithPartitionKey

	t.Run("normal case", func(t *testing.T) {
		err = task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

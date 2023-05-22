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
	"errors"
	"math"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/funcutil"
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

	t.Run("shard num exceeds configuration", func(t *testing.T) {
		cfgMaxShardNum := Params.RootCoordCfg.DmlChannelNum
		restoreCfg := func() { Params.RootCoordCfg.DmlChannelNum = cfgMaxShardNum }
		defer restoreCfg()

		Params.RootCoordCfg.DmlChannelNum = 1

		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:      &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				ShardsNum: 2,
			},
		}
		err := task.validate()
		assert.Error(t, err)
	})

	t.Run("shard num exceeds limit", func(t *testing.T) {
		cfgMaxShardNum := Params.RootCoordCfg.DmlChannelNum
		cfgShardLimit := Params.ProxyCfg.MaxShardNum
		restoreCfg := func() {
			Params.RootCoordCfg.DmlChannelNum = cfgMaxShardNum
			Params.ProxyCfg.MaxShardNum = cfgShardLimit
		}
		defer restoreCfg()

		Params.RootCoordCfg.DmlChannelNum = 100
		Params.ProxyCfg.MaxShardNum = 4

		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:      &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				ShardsNum: 8,
			},
		}
		err := task.validate()
		assert.Error(t, err)
	})

	t.Run("total collection num exceeds limit", func(t *testing.T) {
		cfgCollectionLimit := Params.QuotaConfig.MaxCollectionNum
		restoreCfg := func() {
			Params.QuotaConfig.MaxCollectionNum = cfgCollectionLimit
		}
		defer restoreCfg()

		Params.QuotaConfig.MaxCollectionNum = 1
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("ListAllAvailCollections",
			mock.Anything,
		).Return(map[int64][]int64{
			1: {1, 2},
		}, nil)
		core := newTestCore(withMeta(meta))
		task := createCollectionTask{
			baseTask: newBaseTask(context.TODO(), core),
			Req: &milvuspb.CreateCollectionRequest{
				Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
			},
		}
		err := task.validate()
		assert.Error(t, err)
	})

	t.Run("collection num per db exceeds limit", func(t *testing.T) {
		cfgCollectionLimit := Params.QuotaConfig.MaxCollectionNumPerDB
		restoreCfg := func() {
			Params.QuotaConfig.MaxCollectionNumPerDB = cfgCollectionLimit
		}
		defer restoreCfg()

		Params.QuotaConfig.MaxCollectionNumPerDB = 1
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("ListAllAvailCollections",
			mock.Anything,
		).Return(map[int64][]int64{
			1: {1, 2},
		}, nil)
		core := newTestCore(withMeta(meta))
		task := createCollectionTask{
			baseTask: newBaseTask(context.TODO(), core),
			Req: &milvuspb.CreateCollectionRequest{
				Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
			},
		}
		err := task.validate()
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("ListAllAvailCollections",
			mock.Anything,
		).Return(map[int64][]int64{
			1: {1, 2},
		}, nil)

		core := newTestCore(withMeta(meta))
		task := createCollectionTask{
			baseTask: newBaseTask(context.TODO(), core),
			Req: &milvuspb.CreateCollectionRequest{
				Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
			},
			dbID: 1,
		}

		Params.QuotaConfig.MaxCollectionNum = math.MaxInt64
		Params.QuotaConfig.MaxCollectionNumPerDB = math.MaxInt64

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
			EnableDynamicField: true,
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
	meta := mockrootcoord.NewIMetaTable(t)
	meta.On("GetDatabaseByName",
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(model.NewDefaultDatabase(), nil)
	meta.On("ListAllAvailCollections",
		mock.Anything,
	).Return(map[int64][]int64{
		util.DefaultDBID: {1, 2},
	}, nil)
	Params.QuotaConfig.MaxCollectionNum = math.MaxInt64
	Params.QuotaConfig.MaxCollectionNumPerDB = math.MaxInt64

	t.Run("invalid msg type", func(t *testing.T) {
		core := newTestCore(withMeta(meta))
		task := &createCollectionTask{
			baseTask: newBaseTask(context.TODO(), core),
			Req: &milvuspb.CreateCollectionRequest{
				Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("invalid schema", func(t *testing.T) {
		core := newTestCore(withMeta(meta))
		collectionName := funcutil.GenRandomStr()
		task := &createCollectionTask{
			baseTask: newBaseTask(context.TODO(), core),
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
				Schema:         []byte("invalid schema"),
			},
			dbID: 1,
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

		core := newTestCore(withInvalidIDAllocator(), withMeta(meta))

		task := createCollectionTask{
			baseTask: newBaseTask(context.TODO(), core),
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
				Schema:         marshaledSchema,
			},
			dbID: 1,
		}
		err = task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		defer cleanTestEnv()

		collectionName := funcutil.GenRandomStr()
		field1 := funcutil.GenRandomStr()

		ticker := newRocksMqTtSynchronizer()

		core := newTestCore(withValidIDAllocator(), withTtSynchronizer(ticker), withMeta(meta))

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
			baseTask: newBaseTask(context.TODO(), core),
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
				Schema:         marshaledSchema,
			},
			dbID: 1,
		}
		task.Req.ShardsNum = int32(Params.RootCoordCfg.DmlChannelNum + 1) // no enough channels.
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

		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(coll, nil)

		core := newTestCore(withMeta(meta), withTtSynchronizer(ticker))

		task := &createCollectionTask{
			baseTask: newBaseTask(context.TODO(), core),
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

		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(coll, nil)

		core := newTestCore(withMeta(meta), withTtSynchronizer(ticker))

		task := &createCollectionTask{
			baseTask: newBaseTask(context.TODO(), core),
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
			baseTask: newBaseTask(context.TODO(), core),
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

		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil, errors.New("error mock GetCollectionByName"))
		meta.On("AddCollection",
			mock.Anything,
			mock.Anything,
		).Return(nil)
		meta.On("ChangeCollectionState",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)

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
			baseTask: newBaseTask(context.TODO(), core),
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

		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil, errors.New("error mock GetCollectionByName"))
		meta.On("AddCollection",
			mock.Anything,
			mock.Anything,
		).Return(nil)
		meta.On("ChangeCollectionState",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(errors.New("error mock ChangeCollectionState"))

		removeCollectionCalled := false
		removeCollectionChan := make(chan struct{}, 1)
		meta.On("RemoveCollection",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(func(ctx context.Context, collID UniqueID, ts Timestamp) error {
			removeCollectionCalled = true
			removeCollectionChan <- struct{}{}
			return nil
		})

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
			baseTask: newBaseTask(context.TODO(), core),
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
	meta.On("GetDatabaseByName",
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(model.NewDefaultDatabase(), nil)
	meta.On("ListAllAvailCollections",
		mock.Anything,
	).Return(map[int64][]int64{
		util.DefaultDBID: {1, 2},
	}, nil)
	Params.QuotaConfig.MaxCollectionNum = math.MaxInt64
	Params.QuotaConfig.MaxCollectionNumPerDB = math.MaxInt64
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
		task.Req.NumPartitions = Params.RootCoordCfg.MaxPartitionNum + 1
		err = task.Prepare(context.Background())
		assert.Error(t, err)
	})

	task.Req.NumPartitions = common.DefaultPartitionsWithPartitionKey

	t.Run("normal case", func(t *testing.T) {
		err = task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

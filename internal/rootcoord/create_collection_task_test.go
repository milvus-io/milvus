package rootcoord

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/stretchr/testify/assert"
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
			baseTaskV2: baseTaskV2{core: core},
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
				Schema:         marshaledSchema,
			},
		}
		err = task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("failed to assign channels", func(t *testing.T) {
		// TODO: error won't happen here.
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
			baseTaskV2: baseTaskV2{core: core},
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
				Schema:         marshaledSchema,
			},
		}
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

		core := newTestCore(withMeta(meta), withTtSynchronizer(ticker))

		task := &createCollectionTask{
			baseTaskV2: baseTaskV2{core: core},
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
			Partitions:           []*model.Partition{{PartitionName: Params.CommonCfg.DefaultPartitionName}},
		}

		meta := newMockMetaTable()
		meta.GetCollectionByNameFunc = func(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error) {
			return coll, nil
		}

		core := newTestCore(withMeta(meta), withTtSynchronizer(ticker))

		task := &createCollectionTask{
			baseTaskV2: baseTaskV2{core: core},
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
			baseTaskV2: baseTaskV2{core: core},
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
		dc.GetComponentStatesFunc = func(ctx context.Context) (*internalpb.ComponentStates, error) {
			return &internalpb.ComponentStates{
				State: &internalpb.ComponentInfo{
					NodeID:    TestRootCoordID,
					StateCode: internalpb.StateCode_Healthy,
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
			baseTaskV2: baseTaskV2{core: core},
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
		broker.UnwatchChannelsFunc = func(ctx context.Context, info *watchInfo) error {
			unwatchChannelsCalled = true
			unwatchChannelsChan <- struct{}{}
			return nil
		}

		core := newTestCore(withValidIDAllocator(),
			withMeta(meta),
			withTtSynchronizer(ticker),
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
			baseTaskV2: baseTaskV2{core: core},
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

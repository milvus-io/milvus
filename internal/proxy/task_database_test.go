package proxy

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/crypto"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestCreateDatabaseTask(t *testing.T) {
	paramtable.Init()
	rc := NewMixCoordMock()
	defer rc.Close()

	ctx := context.Background()
	task := &createDatabaseTask{
		Condition: NewTaskCondition(ctx),
		CreateDatabaseRequest: &milvuspb.CreateDatabaseRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreateDatabase,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName: "db",
		},
		ctx:      ctx,
		mixCoord: rc,
		result:   nil,
	}

	t.Run("ok", func(t *testing.T) {
		err := task.PreExecute(ctx)
		assert.NoError(t, err)

		assert.Equal(t, commonpb.MsgType_CreateDatabase, task.Type())
		assert.Equal(t, UniqueID(100), task.ID())
		assert.Equal(t, Timestamp(100), task.BeginTs())
		assert.Equal(t, Timestamp(100), task.EndTs())
		assert.Equal(t, "db", task.GetDbName())
		err = task.Execute(ctx)
		assert.NoError(t, err)

		task.Base = nil
		err = task.OnEnqueue()
		assert.NoError(t, err)
		assert.Equal(t, paramtable.GetNodeID(), task.GetBase().GetSourceID())
		assert.Equal(t, UniqueID(0), task.ID())
	})

	t.Run("pre execute fail", func(t *testing.T) {
		task.DbName = "#0xc0de"
		err := task.PreExecute(ctx)
		assert.Error(t, err)
	})
}

func TestDropDatabaseTask(t *testing.T) {
	paramtable.Init()
	rc := NewMixCoordMock()
	defer rc.Close()

	ctx := context.Background()
	task := &dropDatabaseTask{
		Condition: NewTaskCondition(ctx),
		DropDatabaseRequest: &milvuspb.DropDatabaseRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropDatabase,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName: "db",
		},
		ctx:      ctx,
		mixCoord: rc,
		result:   nil,
	}

	cache := NewMockCache(t)
	cache.On("RemoveDatabase",
		mock.Anything, // context.Context
		mock.AnythingOfType("string"),
	).Maybe()
	globalMetaCache = cache

	t.Run("ok", func(t *testing.T) {
		err := task.PreExecute(ctx)
		assert.NoError(t, err)

		assert.Equal(t, commonpb.MsgType_DropDatabase, task.Type())
		assert.Equal(t, UniqueID(100), task.ID())
		assert.Equal(t, Timestamp(100), task.BeginTs())
		assert.Equal(t, Timestamp(100), task.EndTs())
		assert.Equal(t, "db", task.GetDbName())
		err = task.Execute(ctx)
		assert.NoError(t, err)

		task.Base = nil
		err = task.OnEnqueue()
		assert.NoError(t, err)
		assert.Equal(t, paramtable.GetNodeID(), task.GetBase().GetSourceID())
		assert.Equal(t, UniqueID(0), task.ID())
	})

	t.Run("pre execute fail", func(t *testing.T) {
		task.DbName = "#0xc0de"
		err := task.PreExecute(ctx)
		assert.Error(t, err)
	})
}

func TestListDatabaseTask(t *testing.T) {
	paramtable.Init()
	rc := NewMixCoordMock()
	defer rc.Close()

	ctx := GetContext(context.Background(), "root:123456")
	task := &listDatabaseTask{
		Condition: NewTaskCondition(ctx),
		ListDatabasesRequest: &milvuspb.ListDatabasesRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ListDatabases,
				MsgID:     100,
				Timestamp: 100,
			},
		},
		ctx:      ctx,
		mixCoord: rc,
		result:   nil,
	}

	t.Run("ok", func(t *testing.T) {
		err := task.PreExecute(ctx)
		assert.NoError(t, err)

		assert.Equal(t, commonpb.MsgType_ListDatabases, task.Type())
		assert.Equal(t, UniqueID(100), task.ID())
		assert.Equal(t, Timestamp(100), task.BeginTs())
		assert.Equal(t, Timestamp(100), task.EndTs())
		err = task.Execute(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, task.result)

		err = task.OnEnqueue()
		assert.NoError(t, err)
		assert.Equal(t, paramtable.GetNodeID(), task.GetBase().GetSourceID())
		assert.Equal(t, UniqueID(0), task.ID())

		taskCtx := AppendUserInfoForRPC(ctx)
		md, ok := metadata.FromOutgoingContext(taskCtx)
		assert.True(t, ok)
		authorization, ok := md[strings.ToLower(util.HeaderAuthorize)]
		assert.True(t, ok)
		expectAuth := crypto.Base64Encode("root:root")
		assert.Equal(t, expectAuth, authorization[0])
	})
}

func TestAlterDatabase(t *testing.T) {
	rc := mocks.NewMockMixCoordClient(t)

	rc.EXPECT().AlterDatabase(mock.Anything, mock.Anything).Return(merr.Success(), nil)
	task := &alterDatabaseTask{
		AlterDatabaseRequest: &milvuspb.AlterDatabaseRequest{
			Base:       &commonpb.MsgBase{},
			DbName:     "test_alter_database",
			Properties: []*commonpb.KeyValuePair{{Key: common.MmapEnabledKey, Value: "true"}},
		},
		mixCoord: rc,
	}
	err := task.PreExecute(context.Background())
	assert.Nil(t, err)

	err = task.Execute(context.Background())
	assert.Nil(t, err)

	task1 := &alterDatabaseTask{
		AlterDatabaseRequest: &milvuspb.AlterDatabaseRequest{
			Base:       &commonpb.MsgBase{},
			DbName:     "test_alter_database",
			DeleteKeys: []string{common.MmapEnabledKey},
		},
		mixCoord: rc,
	}
	err1 := task1.PreExecute(context.Background())
	assert.Nil(t, err1)

	err1 = task1.Execute(context.Background())
	assert.Nil(t, err1)
}

func TestAlterDatabaseTaskForReplicateProperty(t *testing.T) {
	rc := mocks.NewMockMixCoordClient(t)
	cache := globalMetaCache
	defer func() { globalMetaCache = cache }()
	mockCache := NewMockCache(t)
	globalMetaCache = mockCache

	t.Run("replicate id", func(t *testing.T) {
		task := &alterDatabaseTask{
			AlterDatabaseRequest: &milvuspb.AlterDatabaseRequest{
				Base:   &commonpb.MsgBase{},
				DbName: "test_alter_database",
				Properties: []*commonpb.KeyValuePair{
					{
						Key:   common.MmapEnabledKey,
						Value: "true",
					},
					{
						Key:   common.ReplicateIDKey,
						Value: "local-test",
					},
				},
			},
			mixCoord: rc,
		}
		err := task.PreExecute(context.Background())
		assert.Error(t, err)
	})

	t.Run("fail to get database info", func(t *testing.T) {
		mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(nil, errors.New("err")).Once()
		task := &alterDatabaseTask{
			AlterDatabaseRequest: &milvuspb.AlterDatabaseRequest{
				Base:   &commonpb.MsgBase{},
				DbName: "test_alter_database",
				Properties: []*commonpb.KeyValuePair{
					{
						Key:   common.ReplicateEndTSKey,
						Value: "1000",
					},
				},
			},
			mixCoord: rc,
		}
		err := task.PreExecute(context.Background())
		assert.Error(t, err)
	})

	t.Run("not enable replicate", func(t *testing.T) {
		mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{
			properties: []*commonpb.KeyValuePair{},
		}, nil).Once()
		task := &alterDatabaseTask{
			AlterDatabaseRequest: &milvuspb.AlterDatabaseRequest{
				Base:   &commonpb.MsgBase{},
				DbName: "test_alter_database",
				Properties: []*commonpb.KeyValuePair{
					{
						Key:   common.ReplicateEndTSKey,
						Value: "1000",
					},
				},
			},
			mixCoord: rc,
		}
		err := task.PreExecute(context.Background())
		assert.Error(t, err)
	})

	t.Run("fail to alloc ts", func(t *testing.T) {
		mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{
			properties: []*commonpb.KeyValuePair{
				{
					Key:   common.ReplicateIDKey,
					Value: "local-test",
				},
			},
		}, nil).Once()
		rc.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).Return(nil, errors.New("err")).Once()
		task := &alterDatabaseTask{
			AlterDatabaseRequest: &milvuspb.AlterDatabaseRequest{
				Base:   &commonpb.MsgBase{},
				DbName: "test_alter_database",
				Properties: []*commonpb.KeyValuePair{
					{
						Key:   common.ReplicateEndTSKey,
						Value: "1000",
					},
				},
			},
			mixCoord: rc,
		}
		err := task.PreExecute(context.Background())
		assert.Error(t, err)
	})

	t.Run("alloc wrong ts", func(t *testing.T) {
		mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{
			properties: []*commonpb.KeyValuePair{
				{
					Key:   common.ReplicateIDKey,
					Value: "local-test",
				},
			},
		}, nil).Once()
		rc.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocTimestampResponse{
			Status:    merr.Success(),
			Timestamp: 999,
		}, nil).Once()
		task := &alterDatabaseTask{
			AlterDatabaseRequest: &milvuspb.AlterDatabaseRequest{
				Base:   &commonpb.MsgBase{},
				DbName: "test_alter_database",
				Properties: []*commonpb.KeyValuePair{
					{
						Key:   common.ReplicateEndTSKey,
						Value: "1000",
					},
				},
			},
			mixCoord: rc,
		}
		err := task.PreExecute(context.Background())
		assert.Error(t, err)
	})

	t.Run("alloc wrong ts", func(t *testing.T) {
		mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{
			properties: []*commonpb.KeyValuePair{
				{
					Key:   common.ReplicateIDKey,
					Value: "local-test",
				},
			},
		}, nil).Once()
		rc.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocTimestampResponse{
			Status:    merr.Success(),
			Timestamp: 1001,
		}, nil).Once()
		task := &alterDatabaseTask{
			AlterDatabaseRequest: &milvuspb.AlterDatabaseRequest{
				Base:   &commonpb.MsgBase{},
				DbName: "test_alter_database",
				Properties: []*commonpb.KeyValuePair{
					{
						Key:   common.ReplicateEndTSKey,
						Value: "1000",
					},
				},
			},
			mixCoord: rc,
		}
		err := task.PreExecute(context.Background())
		assert.NoError(t, err)
	})
}

func TestDescribeDatabaseTask(t *testing.T) {
	rc := mocks.NewMockMixCoordClient(t)

	rc.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).Return(&rootcoordpb.DescribeDatabaseResponse{}, nil)
	task := &describeDatabaseTask{
		DescribeDatabaseRequest: &milvuspb.DescribeDatabaseRequest{
			Base:   &commonpb.MsgBase{},
			DbName: "test_describe_database",
		},
		mixCoord: rc,
	}

	err := task.PreExecute(context.Background())
	assert.Nil(t, err)

	err = task.Execute(context.Background())
	assert.Nil(t, err)
}

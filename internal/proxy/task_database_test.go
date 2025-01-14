package proxy

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/crypto"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestCreateDatabaseTask(t *testing.T) {
	paramtable.Init()
	rc := NewRootCoordMock()
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
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
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
	rc := NewRootCoordMock()
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
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
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
	rc := NewRootCoordMock()
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
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
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
	rc := mocks.NewMockRootCoordClient(t)

	rc.EXPECT().AlterDatabase(mock.Anything, mock.Anything).Return(merr.Success(), nil)
	task := &alterDatabaseTask{
		AlterDatabaseRequest: &milvuspb.AlterDatabaseRequest{
			Base:       &commonpb.MsgBase{},
			DbName:     "test_alter_database",
			Properties: []*commonpb.KeyValuePair{{Key: common.MmapEnabledKey, Value: "true"}},
		},
		rootCoord: rc,
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
		rootCoord: rc,
	}
	err1 := task1.PreExecute(context.Background())
	assert.Nil(t, err1)

	err1 = task1.Execute(context.Background())
	assert.Nil(t, err1)
}

func TestDescribeDatabase(t *testing.T) {
	rc := mocks.NewMockRootCoordClient(t)

	rc.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).Return(&rootcoordpb.DescribeDatabaseResponse{}, nil)
	task := &describeDatabaseTask{
		DescribeDatabaseRequest: &milvuspb.DescribeDatabaseRequest{
			Base:   &commonpb.MsgBase{},
			DbName: "test_describe_database",
		},
		rootCoord: rc,
	}

	err := task.PreExecute(context.Background())
	assert.Nil(t, err)

	err = task.Execute(context.Background())
	assert.Nil(t, err)
}

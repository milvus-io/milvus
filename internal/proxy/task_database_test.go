package proxy

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/stretchr/testify/assert"
)

func TestCreateDatabaseTask(t *testing.T) {
	Params.InitOnce()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()

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

		err = task.OnEnqueue()
		assert.NoError(t, err)
		assert.Equal(t, Params.ProxyCfg.GetNodeID(), task.GetBase().GetSourceID())
		assert.Equal(t, UniqueID(0), task.ID())
	})

	t.Run("pre execute fail", func(t *testing.T) {
		task.DbName = "#0xc0de"
		err := task.PreExecute(ctx)
		assert.Error(t, err)
	})
}

func TestDropDatabaseTask(t *testing.T) {
	Params.InitOnce()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()

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

		err = task.OnEnqueue()
		assert.NoError(t, err)
		assert.Equal(t, Params.ProxyCfg.GetNodeID(), task.GetBase().GetSourceID())
		assert.Equal(t, UniqueID(0), task.ID())
	})

	t.Run("pre execute fail", func(t *testing.T) {
		task.DbName = "#0xc0de"
		err := task.PreExecute(ctx)
		assert.Error(t, err)
	})
}

func TestListDatabaseTask(t *testing.T) {
	Params.InitOnce()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()

	ctx := context.Background()
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
		assert.Equal(t, Params.ProxyCfg.GetNodeID(), task.GetBase().GetSourceID())
		assert.Equal(t, UniqueID(0), task.ID())
	})
}

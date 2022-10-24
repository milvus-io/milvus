package rootcoord

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
)

func Test_createAliasTask_Prepare(t *testing.T) {
	t.Run("invalid msg type", func(t *testing.T) {
		task := &createAliasTask{Req: &milvuspb.CreateAliasRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection}}}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		task := &createAliasTask{Req: &milvuspb.CreateAliasRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateAlias}}}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func Test_createAliasTask_Execute(t *testing.T) {
	t.Run("failed to expire cache", func(t *testing.T) {
		core := newTestCore(withInvalidProxyManager())
		task := &createAliasTask{
			baseTask: baseTask{core: core},
			Req: &milvuspb.CreateAliasRequest{
				Base:  &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateAlias},
				Alias: "test",
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("failed to create alias", func(t *testing.T) {
		core := newTestCore(withInvalidMeta(), withValidProxyManager())
		task := &createAliasTask{
			baseTask: baseTask{core: core},
			Req: &milvuspb.CreateAliasRequest{
				Base:  &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateAlias},
				Alias: "test",
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})
}

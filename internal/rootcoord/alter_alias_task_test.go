package rootcoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/commonpb"

	"github.com/milvus-io/milvus/internal/proto/milvuspb"
)

func Test_alterAliasTask_Prepare(t *testing.T) {
	t.Run("invalid msg type", func(t *testing.T) {
		task := &alterAliasTask{Req: &milvuspb.AlterAliasRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection}}}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		task := &alterAliasTask{Req: &milvuspb.AlterAliasRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterAlias}}}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func Test_alterAliasTask_Execute(t *testing.T) {
	t.Run("failed to expire cache", func(t *testing.T) {
		core := newTestCore(withInvalidProxyManager())
		task := &alterAliasTask{
			baseTaskV2: baseTaskV2{core: core},
			Req: &milvuspb.AlterAliasRequest{
				Base:  &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterAlias},
				Alias: "test",
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("failed to alter alias", func(t *testing.T) {
		core := newTestCore(withValidProxyManager(), withInvalidMeta())
		task := &alterAliasTask{
			baseTaskV2: baseTaskV2{core: core},
			Req: &milvuspb.AlterAliasRequest{
				Base:  &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterAlias},
				Alias: "test",
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})
}

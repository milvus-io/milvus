package rootcoord

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/util/funcutil"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
)

func Test_dropAliasTask_Prepare(t *testing.T) {
	t.Run("invalid msg type", func(t *testing.T) {
		task := &dropAliasTask{
			Req: &milvuspb.DropAliasRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection}},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		task := &dropAliasTask{
			Req: &milvuspb.DropAliasRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DropAlias}},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func Test_dropAliasTask_Execute(t *testing.T) {
	t.Run("failed to expire cache", func(t *testing.T) {
		core := newTestCore(withInvalidProxyManager())
		alias := funcutil.GenRandomStr()
		task := &dropAliasTask{
			baseTaskV2: baseTaskV2{core: core},
			Req: &milvuspb.DropAliasRequest{

				Base:  &commonpb.MsgBase{MsgType: commonpb.MsgType_DropAlias},
				Alias: alias,
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("failed to drop alias", func(t *testing.T) {
		core := newTestCore(withValidProxyManager(), withInvalidMeta())
		alias := funcutil.GenRandomStr()
		task := &dropAliasTask{
			baseTaskV2: baseTaskV2{core: core},
			Req: &milvuspb.DropAliasRequest{

				Base:  &commonpb.MsgBase{MsgType: commonpb.MsgType_DropAlias},
				Alias: alias,
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		meta := newMockMetaTable()
		meta.DropAliasFunc = func(ctx context.Context, alias string, ts Timestamp) error {
			return nil
		}
		core := newTestCore(withValidProxyManager(), withMeta(meta))
		alias := funcutil.GenRandomStr()
		task := &dropAliasTask{
			baseTaskV2: baseTaskV2{core: core},
			Req: &milvuspb.DropAliasRequest{

				Base:  &commonpb.MsgBase{MsgType: commonpb.MsgType_DropAlias},
				Alias: alias,
			},
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
	})
}

package rootcoord

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"

	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
)

func Test_renameCollectionTask_Prepare(t *testing.T) {
	t.Run("invalid msg type", func(t *testing.T) {
		task := &renameCollectionTask{
			Req: &milvuspb.RenameCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Undefined,
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		task := &renameCollectionTask{
			Req: &milvuspb.RenameCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_RenameCollection,
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func Test_renameCollectionTask_Execute(t *testing.T) {
	t.Run("failed to expire cache", func(t *testing.T) {
		core := newTestCore(withInvalidProxyManager())
		task := &renameCollectionTask{
			baseTask: baseTask{core: core},
			Req: &milvuspb.RenameCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_RenameCollection,
				},
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("failed to rename collection", func(t *testing.T) {
		meta := newMockMetaTable()
		meta.RenameCollectionFunc = func(ctx context.Context, oldName string, newName string, ts Timestamp) error {
			return errors.New("fail")
		}

		core := newTestCore(withValidProxyManager(), withMeta(meta))
		task := &renameCollectionTask{
			baseTask: baseTask{core: core},
			Req: &milvuspb.RenameCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_RenameCollection,
				},
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})
}

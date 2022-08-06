package rootcoord

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/util/funcutil"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
)

func Test_dropFunctionTask_Prepare(t *testing.T) {
	t.Run("invalid msg type", func(t *testing.T) {
		task := &dropFunctionTask{
			Req: &milvuspb.DropFunctionRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection}},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		task := &dropFunctionTask{
			Req: &milvuspb.DropFunctionRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DropFunction}},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func Test_dropFunctionTask_Execute(t *testing.T) {
	t.Run("failed to drop function", func(t *testing.T) {
		core := newTestCore(withInvalidMeta())
		funcName := funcutil.GenRandomStr()
		task := &dropFunctionTask{
			baseTask: baseTask{core: core},
			Req: &milvuspb.DropFunctionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_DropFunction,
				},
				FunctionName: funcName,
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})
}

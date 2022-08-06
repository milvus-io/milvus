package rootcoord

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/util/funcutil"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/stretchr/testify/assert"
)

func Test_getFunctionInfoTask_Prepare(t *testing.T) {
	t.Run("invalid msg type", func(t *testing.T) {
		task := &getFunctionInfoTask{
			Req: &rootcoordpb.GetFunctionInfoRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_CreateFunction,
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		task := &getFunctionInfoTask{
			Req: &rootcoordpb.GetFunctionInfoRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_GetFunctionInfo,
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func Test_getFunctionInfoTask_Execute(t *testing.T) {
	t.Run("fail to get function", func(t *testing.T) {
		core := newTestCore(withInvalidMeta())
		funcName := funcutil.GenRandomStr()
		task := &getFunctionInfoTask{
			baseTask: baseTask{core: core},
			Req: &rootcoordpb.GetFunctionInfoRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_GetFunctionInfo,
				},
				FunctionName: funcName,
			},
			Rsp: &rootcoordpb.GetFunctionInfoResponse{},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})
}

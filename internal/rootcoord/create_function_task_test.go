package rootcoord

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/api/schemapb"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/api/milvuspb"
)

func Test_createFunctionTask_Prepare(t *testing.T) {
	t.Run("invalid msg type", func(t *testing.T) {
		task := &createFunctionTask{Req: &milvuspb.CreateFunctionRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection}}}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		task := &createFunctionTask{Req: &milvuspb.CreateFunctionRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateFunction}}}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func Test_createFunctionTask_Execute(t *testing.T) {
	t.Run("failed to create function", func(t *testing.T) {
		core := newTestCore(withInvalidMeta())
		task := &createFunctionTask{
			baseTask: baseTask{core: core},
			Req: &milvuspb.CreateFunctionRequest{
				Base:          &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateFunction},
				FunctionName:  "test",
				WatBodyBase64: "test",
				ArgTypes:      []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_Int64},
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})
}

package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/api/commonpb"

	"github.com/milvus-io/milvus/api/milvuspb"
)

type dropFunctionTask struct {
	baseTask
	Req *milvuspb.DropFunctionRequest
}

func (t *dropFunctionTask) Prepare(ctx context.Context) error {
	if err := CheckMsgType(t.Req.GetBase().GetMsgType(), commonpb.MsgType_DropFunction); err != nil {
		return err
	}
	return nil
}

func (t *dropFunctionTask) Execute(ctx context.Context) error {
	return t.core.meta.DropFunction(ctx, t.Req.GetFunctionName(), t.GetTs())
}

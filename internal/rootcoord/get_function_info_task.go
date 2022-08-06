package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
)

type getFunctionInfoTask struct {
	baseTask
	Req *rootcoordpb.GetFunctionInfoRequest
	Rsp *rootcoordpb.GetFunctionInfoResponse
}

func (t *getFunctionInfoTask) Prepare(ctx context.Context) error {
	if err := CheckMsgType(t.Req.GetBase().GetMsgType(), commonpb.MsgType_GetFunctionInfo); err != nil {
		return err
	}
	return nil
}

func (t *getFunctionInfoTask) Execute(ctx context.Context) error {
	t.Rsp.Status = succStatus()
	functionInfo, err := t.core.meta.GetFunctionInfo(ctx, t.Req.GetFunctionName(), t.GetTs())
	if err != nil {
		t.Rsp.Status = failStatus(commonpb.ErrorCode_UnexpectedError, err.Error())
		return err
	}
	t.Rsp.Info = functionInfo
	return nil
}

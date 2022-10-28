package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
)

// hasCollectionTask has collection request task
type hasCollectionTask struct {
	baseTask
	Req *milvuspb.HasCollectionRequest
	Rsp *milvuspb.BoolResponse
}

func (t *hasCollectionTask) Prepare(ctx context.Context) error {
	if err := CheckMsgType(t.Req.Base.MsgType, commonpb.MsgType_HasCollection); err != nil {
		return err
	}
	return nil
}

// Execute task execution
func (t *hasCollectionTask) Execute(ctx context.Context) error {
	t.Rsp.Status = succStatus()
	ts := getTravelTs(t.Req)
	// TODO: what if err != nil && common.IsCollectionNotExistError == false, should we consider this RPC as failure?
	_, err := t.core.meta.GetCollectionByName(ctx, t.Req.GetCollectionName(), ts)
	t.Rsp.Value = err == nil
	return nil
}

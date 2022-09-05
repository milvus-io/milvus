package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
)

// hasCollectionTask has collection request task
type hasCollectionTask struct {
	baseTaskV2
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
	if t.Req.GetTimeStamp() == 0 {
		t.Req.TimeStamp = typeutil.MaxTimestamp
	}
	_, err := t.core.meta.GetCollectionByName(ctx, t.Req.GetCollectionName(), t.Req.GetTimeStamp())
	t.Rsp.Value = err == nil
	return nil
}

package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type alterAliasTask struct {
	baseTask
	Req *milvuspb.AlterAliasRequest
}

func (t *alterAliasTask) Prepare(ctx context.Context) error {
	t.SetStep(typeutil.TaskStepPreExecute)
	if err := CheckMsgType(t.Req.GetBase().GetMsgType(), commonpb.MsgType_AlterAlias); err != nil {
		return err
	}
	return nil
}

func (t *alterAliasTask) Execute(ctx context.Context) error {
	t.SetStep(typeutil.TaskStepExecute)
	if err := t.core.ExpireMetaCache(ctx, []string{t.Req.GetAlias()}, InvalidCollectionID, t.GetTs()); err != nil {
		return err
	}
	// alter alias is atomic enough.
	return t.core.meta.AlterAlias(ctx, t.Req.GetAlias(), t.Req.GetCollectionName(), t.GetTs())
}

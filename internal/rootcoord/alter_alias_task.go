package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
)

type alterAliasTask struct {
	baseTaskV2
	Req *milvuspb.AlterAliasRequest
}

func (t *alterAliasTask) Prepare(ctx context.Context) error {
	if err := CheckMsgType(t.Req.GetBase().GetMsgType(), commonpb.MsgType_AlterAlias); err != nil {
		return err
	}
	return nil
}

func (t *alterAliasTask) Execute(ctx context.Context) error {
	if err := t.core.ExpireMetaCache(ctx, []string{t.Req.GetAlias()}, InvalidCollectionID, t.GetTs()); err != nil {
		return err
	}
	// alter alias is atomic enough.
	return t.core.meta.AlterAlias(ctx, t.Req.GetAlias(), t.Req.GetCollectionName(), t.GetTs())
}

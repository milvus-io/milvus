package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/api/commonpb"

	"github.com/milvus-io/milvus/api/milvuspb"
)

type dropAliasTask struct {
	baseTaskV2
	Req *milvuspb.DropAliasRequest
}

func (t *dropAliasTask) Prepare(ctx context.Context) error {
	if err := CheckMsgType(t.Req.GetBase().GetMsgType(), commonpb.MsgType_DropAlias); err != nil {
		return err
	}
	return nil
}

func (t *dropAliasTask) Execute(ctx context.Context) error {
	// drop alias is atomic enough.
	if err := t.core.ExpireMetaCache(ctx, []string{t.Req.GetAlias()}, InvalidCollectionID, t.GetTs()); err != nil {
		return err
	}
	return t.core.meta.DropAlias(ctx, t.Req.GetAlias(), t.GetTs())
}

package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"

	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
)

type createAliasTask struct {
	baseTask
	Req *milvuspb.CreateAliasRequest
}

func (t *createAliasTask) Prepare(ctx context.Context) error {
	if err := CheckMsgType(t.Req.GetBase().GetMsgType(), commonpb.MsgType_CreateAlias); err != nil {
		return err
	}
	return nil
}

func (t *createAliasTask) Execute(ctx context.Context) error {
	if err := t.core.ExpireMetaCache(ctx, []string{t.Req.GetAlias(), t.Req.GetCollectionName()}, InvalidCollectionID, t.GetTs()); err != nil {
		return err
	}
	// create alias is atomic enough.
	return t.core.meta.CreateAlias(ctx, t.Req.GetAlias(), t.Req.GetCollectionName(), t.GetTs())
}

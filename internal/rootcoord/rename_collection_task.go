package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
)

type renameCollectionTask struct {
	baseTask
	Req *milvuspb.RenameCollectionRequest
}

func (t *renameCollectionTask) Prepare(ctx context.Context) error {
	if err := CheckMsgType(t.Req.GetBase().GetMsgType(), commonpb.MsgType_RenameCollection); err != nil {
		return err
	}
	return nil
}

func (t *renameCollectionTask) Execute(ctx context.Context) error {
	if err := t.core.ExpireMetaCache(ctx, []string{t.Req.GetOldName()}, InvalidCollectionID, t.GetTs()); err != nil {
		return err
	}
	return t.core.meta.RenameCollection(ctx, t.Req.GetOldName(), t.Req.GetNewName(), t.GetTs())
}

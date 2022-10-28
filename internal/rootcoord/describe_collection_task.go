package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
)

// describeCollectionTask describe collection request task
type describeCollectionTask struct {
	baseTask
	Req *milvuspb.DescribeCollectionRequest
	Rsp *milvuspb.DescribeCollectionResponse
}

func (t *describeCollectionTask) Prepare(ctx context.Context) error {
	if err := CheckMsgType(t.Req.Base.MsgType, commonpb.MsgType_DescribeCollection); err != nil {
		return err
	}
	return nil
}

// Execute task execution
func (t *describeCollectionTask) Execute(ctx context.Context) (err error) {
	coll, err := t.core.describeCollection(ctx, t.Req)
	if err != nil {
		return err
	}
	aliases := t.core.meta.ListAliasesByID(coll.CollectionID)
	t.Rsp = convertModelToDesc(coll, aliases)
	return nil
}

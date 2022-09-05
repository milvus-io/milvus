package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
)

// showCollectionTask show collection request task
type showCollectionTask struct {
	baseTaskV2
	Req *milvuspb.ShowCollectionsRequest
	Rsp *milvuspb.ShowCollectionsResponse
}

func (t *showCollectionTask) Prepare(ctx context.Context) error {
	if err := CheckMsgType(t.Req.Base.MsgType, commonpb.MsgType_ShowCollections); err != nil {
		return err
	}
	return nil
}

// Execute task execution
func (t *showCollectionTask) Execute(ctx context.Context) error {
	t.Rsp.Status = succStatus()
	ts := t.Req.GetTimeStamp()
	if ts == 0 {
		ts = typeutil.MaxTimestamp
	}
	colls, err := t.core.meta.ListCollections(ctx, ts)
	if err != nil {
		t.Rsp.Status = failStatus(commonpb.ErrorCode_UnexpectedError, err.Error())
		return err
	}
	for _, meta := range colls {
		t.Rsp.CollectionNames = append(t.Rsp.CollectionNames, meta.Name)
		t.Rsp.CollectionIds = append(t.Rsp.CollectionIds, meta.CollectionID)
		t.Rsp.CreatedTimestamps = append(t.Rsp.CreatedTimestamps, meta.CreateTime)
		physical, _ := tsoutil.ParseHybridTs(meta.CreateTime)
		t.Rsp.CreatedUtcTimestamps = append(t.Rsp.CreatedUtcTimestamps, uint64(physical))
	}
	return nil
}

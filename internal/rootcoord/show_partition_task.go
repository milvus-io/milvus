package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// showPartitionTask show partition request task
type showPartitionTask struct {
	baseTaskV2
	Req *milvuspb.ShowPartitionsRequest
	Rsp *milvuspb.ShowPartitionsResponse
}

func (t *showPartitionTask) Prepare(ctx context.Context) error {
	if err := CheckMsgType(t.Req.Base.MsgType, commonpb.MsgType_ShowPartitions); err != nil {
		return err
	}
	return nil
}

// Execute task execution
func (t *showPartitionTask) Execute(ctx context.Context) error {
	var coll *model.Collection
	var err error
	t.Rsp.Status = succStatus()
	if t.Req.GetCollectionName() == "" {
		coll, err = t.core.meta.GetCollectionByID(ctx, t.Req.GetCollectionID(), typeutil.MaxTimestamp)
	} else {
		coll, err = t.core.meta.GetCollectionByName(ctx, t.Req.GetCollectionName(), typeutil.MaxTimestamp)
	}
	if err != nil {
		t.Rsp.Status = failStatus(commonpb.ErrorCode_CollectionNotExists, err.Error())
		return err
	}

	for _, part := range coll.Partitions {
		t.Rsp.PartitionIDs = append(t.Rsp.PartitionIDs, part.PartitionID)
		t.Rsp.PartitionNames = append(t.Rsp.PartitionNames, part.PartitionName)
		t.Rsp.CreatedTimestamps = append(t.Rsp.CreatedTimestamps, part.PartitionCreatedTimestamp)
		physical, _ := tsoutil.ParseHybridTs(part.PartitionCreatedTimestamp)
		t.Rsp.CreatedUtcTimestamps = append(t.Rsp.CreatedUtcTimestamps, uint64(physical))
	}

	return nil
}

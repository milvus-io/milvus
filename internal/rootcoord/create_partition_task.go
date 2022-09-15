package rootcoord

import (
	"context"

	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"

	"github.com/milvus-io/milvus/api/commonpb"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/api/milvuspb"
)

type createPartitionTask struct {
	baseTaskV2
	Req      *milvuspb.CreatePartitionRequest
	collMeta *model.Collection
}

func (t *createPartitionTask) Prepare(ctx context.Context) error {
	if err := CheckMsgType(t.Req.GetBase().GetMsgType(), commonpb.MsgType_CreatePartition); err != nil {
		return err
	}
	collMeta, err := t.core.meta.GetCollectionByName(ctx, t.Req.GetCollectionName(), t.GetTs())
	if err != nil {
		return err
	}
	t.collMeta = collMeta
	return nil
}

func (t *createPartitionTask) Execute(ctx context.Context) error {
	for _, partition := range t.collMeta.Partitions {
		if partition.PartitionName == t.Req.GetPartitionName() {
			log.Warn("add duplicate partition", zap.String("collection", t.Req.GetCollectionName()), zap.String("partition", t.Req.GetPartitionName()), zap.Uint64("ts", t.GetTs()))
			return nil
		}
	}

	partID, err := t.core.idAllocator.AllocOne()
	if err != nil {
		return err
	}
	partition := &model.Partition{
		PartitionID:               partID,
		PartitionName:             t.Req.GetPartitionName(),
		PartitionCreatedTimestamp: t.GetTs(),
		Extra:                     nil,
		CollectionID:              t.collMeta.CollectionID,
		State:                     pb.PartitionState_PartitionCreated,
	}

	undoTask := newBaseUndoTask()
	undoTask.AddStep(&ExpireCacheStep{
		baseStep:        baseStep{core: t.core},
		collectionNames: []string{t.Req.GetCollectionName()},
		collectionID:    t.collMeta.CollectionID,
		ts:              t.GetTs(),
	}, &NullStep{})
	undoTask.AddStep(&AddPartitionMetaStep{
		baseStep:  baseStep{core: t.core},
		partition: partition,
	}, &NullStep{}) // adding partition is atomic enough.

	return undoTask.Execute(ctx)
}

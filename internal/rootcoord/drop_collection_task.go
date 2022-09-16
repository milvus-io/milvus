package rootcoord

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"

	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"

	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/milvus-io/milvus/api/commonpb"

	"github.com/milvus-io/milvus/api/milvuspb"
)

type dropCollectionTask struct {
	baseTaskV2
	Req *milvuspb.DropCollectionRequest
}

func (t *dropCollectionTask) validate() error {
	if err := CheckMsgType(t.Req.GetBase().GetMsgType(), commonpb.MsgType_DropCollection); err != nil {
		return err
	}
	if t.core.meta.IsAlias(t.Req.GetCollectionName()) {
		return fmt.Errorf("cannot drop the collection via alias = %s", t.Req.CollectionName)
	}
	return nil
}

func (t *dropCollectionTask) Prepare(ctx context.Context) error {
	return t.validate()
}

func (t *dropCollectionTask) Execute(ctx context.Context) error {
	// use max ts to check if latest collection exists.
	// we cannot handle case that
	// dropping collection with `ts1` but a collection exists in catalog with newer ts which is bigger than `ts1`.
	// fortunately, if ddls are promised to execute in sequence, then everything is OK. The `ts1` will always be latest.
	collMeta, err := t.core.meta.GetCollectionByName(ctx, t.Req.GetCollectionName(), typeutil.MaxTimestamp)
	if err != nil {
		// make dropping collection idempotent.
		log.Warn("drop non-existent collection", zap.String("collection", t.Req.GetCollectionName()))
		return nil
	}

	// meta cache of all aliases should also be cleaned.
	aliases := t.core.meta.ListAliasesByID(collMeta.CollectionID)

	ts := t.GetTs()

	redoTask := newBaseRedoTask()

	redoTask.AddSyncStep(&ExpireCacheStep{
		baseStep:        baseStep{core: t.core},
		collectionNames: append(aliases, collMeta.Name),
		collectionID:    collMeta.CollectionID,
		ts:              ts,
	})
	redoTask.AddSyncStep(&ChangeCollectionStateStep{
		baseStep:     baseStep{core: t.core},
		collectionID: collMeta.CollectionID,
		state:        pb.CollectionState_CollectionDropping,
		ts:           ts,
	})

	redoTask.AddAsyncStep(&ReleaseCollectionStep{
		baseStep:     baseStep{core: t.core},
		collectionID: collMeta.CollectionID,
	})
	redoTask.AddAsyncStep(&DropIndexStep{
		baseStep: baseStep{core: t.core},
		collID:   collMeta.CollectionID,
	})
	redoTask.AddAsyncStep(&DeleteCollectionDataStep{
		baseStep: baseStep{core: t.core},
		coll:     collMeta,
		ts:       ts,
	})
	redoTask.AddAsyncStep(&RemoveDmlChannelsStep{
		baseStep:  baseStep{core: t.core},
		pchannels: collMeta.PhysicalChannelNames,
	})
	redoTask.AddAsyncStep(&DeleteCollectionMetaStep{
		baseStep:     baseStep{core: t.core},
		collectionID: collMeta.CollectionID,
		ts:           ts,
	})

	return redoTask.Execute(ctx)
}

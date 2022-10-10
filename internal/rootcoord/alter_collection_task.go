package rootcoord

import (
	"context"
	"errors"
	"fmt"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/api/milvuspb"
)

type alterCollectionTask struct {
	baseTask
	Req *milvuspb.AlterCollectionRequest
}

func (a *alterCollectionTask) Prepare(ctx context.Context) error {
	if a.Req.GetCollectionName() == "" {
		return fmt.Errorf("alter collection failed, collection name does not exists")
	}

	return nil
}

func (a *alterCollectionTask) Execute(ctx context.Context) error {
	// Now we only support alter properties of collection
	if a.Req.GetProperties() == nil {
		return errors.New("only support alter collection properties, but collection properties is empty")
	}

	oldColl, err := a.core.meta.GetCollectionByName(ctx, a.Req.GetCollectionName(), a.ts)
	if err != nil {
		log.Warn("get collection failed during changing collection state",
			zap.String("collectionName", a.Req.GetCollectionName()), zap.Uint64("ts", a.ts))
		return err
	}

	newColl := oldColl.Clone()
	newColl.Properties = a.Req.GetProperties()

	ts := a.GetTs()
	redoTask := newBaseRedoTask(a.core.stepExecutor)
	redoTask.AddSyncStep(&AlterCollectionStep{
		baseStep: baseStep{core: a.core},
		oldColl:  oldColl,
		newColl:  newColl,
		ts:       ts,
	})

	redoTask.AddSyncStep(&expireCacheStep{
		baseStep:     baseStep{core: a.core},
		collectionID: oldColl.CollectionID,
		ts:           ts,
	})

	a.Req.CollectionID = oldColl.CollectionID
	redoTask.AddSyncStep(&BroadcastAlteredCollectionStep{
		baseStep: baseStep{core: a.core},
		req:      a.Req,
		core:     a.core,
	})

	return redoTask.Execute(ctx)
}

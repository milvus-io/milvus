// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rootcoord

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type dropCollectionTask struct {
	baseTask
	Req *milvuspb.DropCollectionRequest
}

func (t *dropCollectionTask) validate() error {
	if err := CheckMsgType(t.Req.GetBase().GetMsgType(), commonpb.MsgType_DropCollection); err != nil {
		return err
	}
	if t.core.meta.IsAlias(t.Req.GetDbName(), t.Req.GetCollectionName()) {
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
	collMeta, err := t.core.meta.GetCollectionByName(ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), typeutil.MaxTimestamp)
	if errors.Is(err, merr.ErrCollectionNotFound) {
		// make dropping collection idempotent.
		log.Warn("drop non-existent collection", zap.String("collection", t.Req.GetCollectionName()))
		return nil
	}

	if err != nil {
		return err
	}

	// meta cache of all aliases should also be cleaned.
	aliases := t.core.meta.ListAliasesByID(collMeta.CollectionID)
	ts := t.GetTs()

	task := newDropCollectionRedoTask(t.core, collMeta, t.Req.GetDbName(), aliases, ts, false)
	return task.Execute(ctx)
}

// newDropCollectionRedoTask creates a redo task for dropping collection.
func newDropCollectionRedoTask(
	core *Core,
	collMeta *model.Collection,
	dbName string,
	aliases []string,
	ts uint64,
	isRecover bool,
) *baseRedoTask {
	redoTask := newBaseRedoTask(core.stepExecutor)
	if !isRecover {
		// if the task is recoverred from meta, the state has been changed to dropping, so skip it.
		redoTask.AddSyncStep(&changeCollectionStateStep{
			baseStep:     baseStep{core: core},
			collectionID: collMeta.CollectionID,
			state:        pb.CollectionState_CollectionDropping,
			ts:           ts,
		})
	}

	redoTask.AddSyncStep(&expireCacheStep{
		baseStep:        baseStep{core: core},
		dbName:          dbName,
		collectionNames: append(aliases, collMeta.Name),
		collectionID:    collMeta.CollectionID,
		ts:              ts,
		opts:            []proxyutil.ExpireCacheOpt{proxyutil.SetMsgType(commonpb.MsgType_DropCollection)},
	})
	redoTask.AddAsyncStep(&releaseCollectionStep{
		baseStep:     baseStep{core: core},
		collectionID: collMeta.CollectionID,
	})
	redoTask.AddAsyncStep(&dropIndexStep{
		baseStep: baseStep{core: core},
		collID:   collMeta.CollectionID,
		partIDs:  nil,
	})
	redoTask.AddAsyncStep(&deleteCollectionDataStep{
		baseStep: baseStep{core: core},
		coll:     collMeta,
		isSkip:   !Params.CommonCfg.TTMsgEnabled.GetAsBool(),
	})
	redoTask.AddAsyncStep(&removeDmlChannelsStep{
		baseStep:  baseStep{core: core},
		pChannels: collMeta.PhysicalChannelNames,
	})
	redoTask.AddAsyncStep(newDropCollectionAtDataCoordStep(core, collMeta.CollectionID, collMeta.VirtualChannelNames))
	redoTask.AddAsyncStep(newConfirmGCStep(core, collMeta.CollectionID, allPartition))
	redoTask.AddAsyncStep(&deleteCollectionMetaStep{
		baseStep:     baseStep{core: core},
		collectionID: collMeta.CollectionID,
		// This ts is less than the ts when we notify data nodes to drop collection, but it's OK since we have already
		// marked this collection as deleted. If we want to make this ts greater than the notification's ts, we should
		// wrap a step who will have these three children and connect them with ts.
		ts: ts,
	})
	return redoTask
}

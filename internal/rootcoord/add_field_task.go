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

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
)

type addCollectionFieldTask struct {
	baseTask
	Req         *milvuspb.AddCollectionFieldRequest
	fieldSchema *schemapb.FieldSchema
}

func (t *addCollectionFieldTask) Prepare(ctx context.Context) error {
	if err := CheckMsgType(t.Req.GetBase().GetMsgType(), commonpb.MsgType_AddCollectionField); err != nil {
		return err
	}
	t.fieldSchema = &schemapb.FieldSchema{}
	err := proto.Unmarshal(t.Req.Schema, t.fieldSchema)
	if err != nil {
		return err
	}
	if err := checkFieldSchema([]*schemapb.FieldSchema{t.fieldSchema}); err != nil {
		return err
	}
	return nil
}

func (t *addCollectionFieldTask) Execute(ctx context.Context) error {
	oldColl, err := t.core.meta.GetCollectionByName(ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), t.ts)
	if err != nil {
		log.Ctx(ctx).Warn("get collection failed during add field",
			zap.String("collectionName", t.Req.GetCollectionName()), zap.Uint64("ts", t.ts))
		return err
	}

	id, err := t.core.idAllocator.AllocOne()
	if err != nil {
		return err
	}
	// assign field id
	t.fieldSchema.FieldID = id

	newField := model.UnmarshalFieldModel(t.fieldSchema)

	ts := t.GetTs()
	return executeAddCollectionFieldTaskSteps(ctx, t.core, oldColl, newField, t.Req, ts)
}

func (t *addCollectionFieldTask) GetLockerKey() LockerKey {
	collection := t.core.getCollectionIDStr(t.ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), 0)
	return NewLockerKeyChain(
		NewClusterLockerKey(false),
		NewDatabaseLockerKey(t.Req.GetDbName(), false),
		NewCollectionLockerKey(collection, true),
	)
}

func executeAddCollectionFieldTaskSteps(ctx context.Context,
	core *Core,
	col *model.Collection,
	newField *model.Field,
	req *milvuspb.AddCollectionFieldRequest,
	ts Timestamp,
) error {
	redoTask := newBaseRedoTask(core.stepExecutor)

	oldColl := col.Clone()
	redoTask.AddSyncStep(&AddCollectionFieldStep{
		baseStep: baseStep{core: core},
		oldColl:  oldColl,
		newField: newField,
		ts:       ts,
	})

	updatedCollection := col.Clone()
	updatedCollection.Fields = append(updatedCollection.Fields, newField)
	redoTask.AddSyncStep(&WriteSchemaChangeWALStep{
		baseStep:   baseStep{core: core},
		collection: updatedCollection,
		ts:         ts,
	})

	req.CollectionID = oldColl.CollectionID
	redoTask.AddSyncStep(&BroadcastAlteredCollectionStep{
		baseStep: baseStep{core: core},
		req: &milvuspb.AlterCollectionRequest{
			DbName:         req.GetDbName(),
			CollectionName: req.GetCollectionName(),
			CollectionID:   req.GetCollectionID(),
		},
		core: core,
	})

	// field needs to be refreshed in the cache
	aliases := core.meta.ListAliasesByID(ctx, oldColl.CollectionID)
	redoTask.AddSyncStep(&expireCacheStep{
		baseStep:        baseStep{core: core},
		dbName:          req.GetDbName(),
		collectionNames: append(aliases, req.GetCollectionName()),
		collectionID:    oldColl.CollectionID,
		opts:            []proxyutil.ExpireCacheOpt{proxyutil.SetMsgType(commonpb.MsgType_AddCollectionField)},
	})

	return redoTask.Execute(ctx)
}

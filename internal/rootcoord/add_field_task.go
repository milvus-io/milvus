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
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"go.uber.org/zap"
)

type addFieldTask struct {
	baseTask
	Req *milvuspb.AddFieldRequest
}

func (t *addFieldTask) Prepare(ctx context.Context) error {
	if t.Req == nil {
		return errors.New("empty requests")
	}
	if t.Req.GetCollectionName() == "" {
		return fmt.Errorf("add field failed, collection name does not exists")
	}
	// change type,to do lxg
	if err := CheckMsgType(t.Req.GetBase().GetMsgType(), commonpb.MsgType_CreateCollection); err != nil {
		return err
	}

	if err := checkFieldSchema(t.Req.GetFieldSchema()); err != nil {
		return err
	}

	if err := validateFieldDataType(t.Req.GetFieldSchema()); err != nil {
		return err
	}
	return nil
}

func (t *addFieldTask) Execute(ctx context.Context) error {
	oldColl, err := t.core.meta.GetCollectionByName(ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), t.ts)
	if err != nil {
		log.Ctx(ctx).Warn("get collection failed during add field",
			zap.String("collectionName", t.Req.GetCollectionName()), zap.Uint64("ts", t.ts))
		return err
	}

	start := len(oldColl.Fields)
	// assign field id
	for idx, field := range t.Req.GetFieldSchema() {
		field.FieldID = int64(idx + StartOfUserFieldID + start)
	}

	newField := model.UnmarshalFieldModels(t.Req.FieldSchema)

	ts := t.GetTs()
	return executeAddFieldTaskSteps(ctx, t.core, oldColl, newField, t.Req, ts)
}

func (t *addFieldTask) GetLockerKey() LockerKey {
	collection := t.core.getCollectionIDStr(t.ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), 0)
	return NewLockerKeyChain(
		NewClusterLockerKey(false),
		NewDatabaseLockerKey(t.Req.GetDbName(), false),
		NewCollectionLockerKey(collection, true),
	)
}

func executeAddFieldTaskSteps(ctx context.Context,
	core *Core,
	col *model.Collection,
	newField []*model.Field,
	req *milvuspb.AddFieldRequest,
	ts Timestamp,
) error {
	oldColl := col.Clone()
	redoTask := newBaseRedoTask(core.stepExecutor)
	redoTask.AddSyncStep(&AddFieldStep{
		baseStep: baseStep{core: core},
		oldColl:  oldColl,
		newField: newField,
		ts:       ts,
	})

	req.CollectionID = oldColl.CollectionID
	redoTask.AddSyncStep(&BroadcastAddFieldStep{
		baseStep: baseStep{core: core},
		req:      req,
		core:     core,
	})

	// field needs to be refreshed in the cache
	aliases := core.meta.ListAliasesByID(ctx, oldColl.CollectionID)
	redoTask.AddSyncStep(&expireCacheStep{
		baseStep:        baseStep{core: core},
		dbName:          req.GetDbName(),
		collectionNames: append(aliases, req.GetCollectionName()),
		collectionID:    oldColl.CollectionID,
		opts:            []proxyutil.ExpireCacheOpt{proxyutil.SetMsgType(commonpb.MsgType_AddField)},
	})

	return redoTask.Execute(ctx)
}

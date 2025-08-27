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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
)

type createAliasTask struct {
	baseTask
	Req *milvuspb.CreateAliasRequest
}

func (t *createAliasTask) Prepare(ctx context.Context) error {
	if err := CheckMsgType(t.Req.GetBase().GetMsgType(), commonpb.MsgType_CreateAlias); err != nil {
		return err
	}
	return nil
}

func (t *createAliasTask) Execute(ctx context.Context) error {
	oldColl, err := t.core.meta.GetCollectionByName(ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), t.ts)
	if err != nil {
		log.Ctx(ctx).Warn("get collection failed during create alias",
			zap.String("collectionName", t.Req.GetCollectionName()), zap.Uint64("ts", t.ts))
		return err
	}

	return executeCreateAliasTaskSteps(ctx, t.core, t.Req, oldColl, t.GetTs())
}

func (t *createAliasTask) GetLockerKey() LockerKey {
	return NewLockerKeyChain(
		NewClusterLockerKey(false),
		NewDatabaseLockerKey(t.Req.GetDbName(), false),
		NewCollectionLockerKey(t.Req.GetCollectionName(), true),
	)
}

func executeCreateAliasTaskSteps(ctx context.Context, core *Core, req *milvuspb.CreateAliasRequest, oldColl *model.Collection, ts Timestamp) error {
	redoTask := newBaseRedoTask(core.stepExecutor)

	// properties needs to be refreshed in the cache
	aliases := core.meta.ListAliasesByID(ctx, oldColl.CollectionID)
	redoTask.AddSyncStep(&expireCacheStep{
		baseStep:        baseStep{core: core},
		dbName:          req.GetDbName(),
		collectionNames: append(aliases, req.GetCollectionName()),
		collectionID:    oldColl.CollectionID,
		opts:            []proxyutil.ExpireCacheOpt{proxyutil.SetMsgType(commonpb.MsgType_CreateAlias)},
	})

	redoTask.AddSyncStep(&CreateAliasStep{
		baseStep: baseStep{core: core},
		Req:      req,
		ts:       ts,
	})

	return redoTask.Execute(ctx)
}

type CreateAliasStep struct {
	baseStep
	Req *milvuspb.CreateAliasRequest
	ts  Timestamp
}

func (c *CreateAliasStep) Execute(ctx context.Context) ([]nestedStep, error) {
	err := c.core.meta.CreateAlias(ctx, c.Req.GetDbName(), c.Req.GetAlias(), c.Req.GetCollectionName(), c.ts)
	return nil, err
}

func (c *CreateAliasStep) Desc() string {
	return fmt.Sprintf("create alias %s, for collection %s", c.Req.GetAlias(), c.Req.GetCollectionName())
}

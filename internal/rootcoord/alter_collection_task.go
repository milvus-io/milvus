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
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
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

	oldColl, err := a.core.meta.GetCollectionByName(ctx, a.Req.GetDbName(), a.Req.GetCollectionName(), a.ts)
	if err != nil {
		log.Warn("get collection failed during changing collection state",
			zap.String("collectionName", a.Req.GetCollectionName()), zap.Uint64("ts", a.ts))
		return err
	}

	newColl := oldColl.Clone()
	newColl.Properties = MergeProperties(oldColl.Properties, a.Req.GetProperties())

	ts := a.GetTs()
	redoTask := newBaseRedoTask(a.core.stepExecutor)
	redoTask.AddSyncStep(&AlterCollectionStep{
		baseStep: baseStep{core: a.core},
		oldColl:  oldColl,
		newColl:  newColl,
		ts:       ts,
	})

	a.Req.CollectionID = oldColl.CollectionID
	redoTask.AddSyncStep(&BroadcastAlteredCollectionStep{
		baseStep: baseStep{core: a.core},
		req:      a.Req,
		core:     a.core,
	})

	// properties needs to be refreshed in the cache
	aliases := a.core.meta.ListAliasesByID(oldColl.CollectionID)
	redoTask.AddSyncStep(&expireCacheStep{
		baseStep:        baseStep{core: a.core},
		dbName:          a.Req.GetDbName(),
		collectionNames: append(aliases, a.Req.GetCollectionName()),
		collectionID:    oldColl.CollectionID,
		opts:            []proxyutil.ExpireCacheOpt{proxyutil.SetMsgType(commonpb.MsgType_AlterCollection)},
	})

	oldReplicaNumber, _ := common.DatabaseLevelReplicaNumber(oldColl.Properties)
	oldResourceGroups, _ := common.DatabaseLevelResourceGroups(oldColl.Properties)
	newReplicaNumber, _ := common.DatabaseLevelReplicaNumber(newColl.Properties)
	newResourceGroups, _ := common.DatabaseLevelResourceGroups(newColl.Properties)
	left, right := lo.Difference(oldResourceGroups, newResourceGroups)
	rgChanged := len(left) > 0 || len(right) > 0
	replicaChanged := oldReplicaNumber != newReplicaNumber
	if rgChanged || replicaChanged {
		log.Ctx(ctx).Warn("alter collection trigger update load config",
			zap.Int64("collectionID", oldColl.CollectionID),
			zap.Int64("oldReplicaNumber", oldReplicaNumber),
			zap.Int64("newReplicaNumber", newReplicaNumber),
			zap.Strings("oldResourceGroups", oldResourceGroups),
			zap.Strings("newResourceGroups", newResourceGroups),
		)
		redoTask.AddAsyncStep(NewSimpleStep("", func(ctx context.Context) ([]nestedStep, error) {
			resp, err := a.core.queryCoord.UpdateLoadConfig(ctx, &querypb.UpdateLoadConfigRequest{
				CollectionIDs:  []int64{oldColl.CollectionID},
				ReplicaNumber:  int32(newReplicaNumber),
				ResourceGroups: newResourceGroups,
			})
			if err := merr.CheckRPCCall(resp, err); err != nil {
				log.Warn("failed to trigger update load config for collection", zap.Int64("collectionID", newColl.CollectionID), zap.Error(err))
				return nil, err
			}
			return nil, nil
		}))
	}

	return redoTask.Execute(ctx)
}

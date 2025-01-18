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
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type alterDatabaseTask struct {
	baseTask
	Req *rootcoordpb.AlterDatabaseRequest
}

func (a *alterDatabaseTask) Prepare(ctx context.Context) error {
	if a.Req.GetDbName() == "" {
		return fmt.Errorf("alter database failed, database name does not exists")
	}

	return nil
}

func (a *alterDatabaseTask) Execute(ctx context.Context) error {
	// Now we support alter and delete properties of database
	if a.Req.GetProperties() == nil && a.Req.GetDeleteKeys() == nil {
		return errors.New("alter database requires either properties or deletekeys to modify or delete keys, both cannot be empty")
	}

	if len(a.Req.GetProperties()) > 0 && len(a.Req.GetDeleteKeys()) > 0 {
		return errors.New("alter database operation cannot modify properties and delete keys at the same time")
	}

	oldDB, err := a.core.meta.GetDatabaseByName(ctx, a.Req.GetDbName(), a.ts)
	if err != nil {
		log.Ctx(ctx).Warn("get database failed during changing database props",
			zap.String("databaseName", a.Req.GetDbName()), zap.Uint64("ts", a.ts))
		return err
	}

	newDB := oldDB.Clone()
	if (len(a.Req.GetProperties())) > 0 {
		if ContainsKeyPairArray(a.Req.GetProperties(), oldDB.Properties) {
			log.Info("skip to alter database due to no changes were detected in the properties", zap.String("databaseName", a.Req.GetDbName()))
			return nil
		}
		ret := MergeProperties(oldDB.Properties, a.Req.GetProperties())
		newDB.Properties = ret
	} else if (len(a.Req.GetDeleteKeys())) > 0 {
		ret := DeleteProperties(oldDB.Properties, a.Req.GetDeleteKeys())
		newDB.Properties = ret
	}

	ts := a.GetTs()
	redoTask := newBaseRedoTask(a.core.stepExecutor)
	redoTask.AddSyncStep(&AlterDatabaseStep{
		baseStep: baseStep{core: a.core},
		oldDB:    oldDB,
		newDB:    newDB,
		ts:       ts,
	})

	oldReplicaNumber, _ := common.DatabaseLevelReplicaNumber(oldDB.Properties)
	oldResourceGroups, _ := common.DatabaseLevelResourceGroups(oldDB.Properties)
	newReplicaNumber, _ := common.DatabaseLevelReplicaNumber(newDB.Properties)
	newResourceGroups, _ := common.DatabaseLevelResourceGroups(newDB.Properties)
	left, right := lo.Difference(oldResourceGroups, newResourceGroups)
	rgChanged := len(left) > 0 || len(right) > 0
	replicaChanged := oldReplicaNumber != newReplicaNumber
	if rgChanged || replicaChanged {
		log.Ctx(ctx).Warn("alter database trigger update load config",
			zap.Int64("dbID", oldDB.ID),
			zap.Int64("oldReplicaNumber", oldReplicaNumber),
			zap.Int64("newReplicaNumber", newReplicaNumber),
			zap.Strings("oldResourceGroups", oldResourceGroups),
			zap.Strings("newResourceGroups", newResourceGroups),
		)
		redoTask.AddAsyncStep(NewSimpleStep("", func(ctx context.Context) ([]nestedStep, error) {
			colls, err := a.core.meta.ListCollections(ctx, oldDB.Name, typeutil.MaxTimestamp, true)
			if err != nil {
				log.Warn("failed to trigger update load config for database", zap.Int64("dbID", oldDB.ID), zap.Error(err))
				return nil, err
			}
			if len(colls) == 0 {
				return nil, nil
			}

			resp, err := a.core.queryCoord.UpdateLoadConfig(ctx, &querypb.UpdateLoadConfigRequest{
				CollectionIDs:  lo.Map(colls, func(coll *model.Collection, _ int) int64 { return coll.CollectionID }),
				ReplicaNumber:  int32(newReplicaNumber),
				ResourceGroups: newResourceGroups,
			})
			if err := merr.CheckRPCCall(resp, err); err != nil {
				log.Warn("failed to trigger update load config for database", zap.Int64("dbID", oldDB.ID), zap.Error(err))
				return nil, err
			}
			return nil, nil
		}))
	}

	return redoTask.Execute(ctx)
}

func (a *alterDatabaseTask) GetLockerKey() LockerKey {
	return NewLockerKeyChain(
		NewClusterLockerKey(false),
		NewDatabaseLockerKey(a.Req.GetDbName(), true),
	)
}

func MergeProperties(oldProps []*commonpb.KeyValuePair, updatedProps []*commonpb.KeyValuePair) []*commonpb.KeyValuePair {
	props := make(map[string]string)
	for _, prop := range oldProps {
		props[prop.Key] = prop.Value
	}

	for _, prop := range updatedProps {
		props[prop.Key] = prop.Value
	}

	propKV := make([]*commonpb.KeyValuePair, 0)

	for key, value := range props {
		propKV = append(propKV, &commonpb.KeyValuePair{
			Key:   key,
			Value: value,
		})
	}

	return propKV
}

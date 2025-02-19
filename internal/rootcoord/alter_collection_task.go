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
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
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

func getConsistencyLevel(props ...*commonpb.KeyValuePair) (bool, commonpb.ConsistencyLevel) {
	for _, p := range props {
		if p.GetKey() == common.ConsistencyLevel {
			value := p.GetValue()
			if level, err := strconv.ParseInt(value, 10, 32); err == nil {
				if _, ok := commonpb.ConsistencyLevel_name[int32(level)]; ok {
					return true, commonpb.ConsistencyLevel(level)
				}
			} else {
				if level, ok := commonpb.ConsistencyLevel_value[value]; ok {
					return true, commonpb.ConsistencyLevel(level)
				}
			}
		}
	}
	return false, commonpb.ConsistencyLevel(0)
}

func (a *alterCollectionTask) Execute(ctx context.Context) error {
	// Now we only support alter properties of collection
	if a.Req.GetProperties() == nil && a.Req.GetDeleteKeys() == nil {
		return errors.New("The collection properties to alter and keys to delete must not be empty at the same time")
	}

	if len(a.Req.GetProperties()) > 0 && len(a.Req.GetDeleteKeys()) > 0 {
		return errors.New("can not provide properties and deletekeys at the same time")
	}

	oldColl, err := a.core.meta.GetCollectionByName(ctx, a.Req.GetDbName(), a.Req.GetCollectionName(), a.ts)
	if err != nil {
		log.Warn("get collection failed during changing collection state",
			zap.String("collectionName", a.Req.GetCollectionName()), zap.Uint64("ts", a.ts))
		return err
	}

	newColl := oldColl.Clone()
	if len(a.Req.Properties) > 0 {
		if ContainsKeyPairArray(a.Req.GetProperties(), oldColl.Properties) {
			log.Info("skip to alter collection due to no changes were detected in the properties", zap.Int64("collectionID", oldColl.CollectionID))
			return nil
		}
		newColl.Properties = MergeProperties(oldColl.Properties, a.Req.GetProperties())
	} else if len(a.Req.DeleteKeys) > 0 {
		newColl.Properties = DeleteProperties(oldColl.Properties, a.Req.GetDeleteKeys())
	}

	ts := a.GetTs()

	tso, err := a.core.tsoAllocator.GenerateTSO(1)
	if err == nil {
		newColl.UpdateTimestamp = tso
	}
	if ok, level := getConsistencyLevel(a.Req.GetProperties()...); ok {
		newColl.ConsistencyLevel = level
	}
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
		collectionID:    InvalidCollectionID,
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

func DeleteProperties(oldProps []*commonpb.KeyValuePair, deleteKeys []string) []*commonpb.KeyValuePair {
	propsMap := make(map[string]string)
	for _, prop := range oldProps {
		propsMap[prop.Key] = prop.Value
	}
	for _, key := range deleteKeys {
		delete(propsMap, key)
	}
	propKV := make([]*commonpb.KeyValuePair, 0, len(propsMap))
	for key, value := range propsMap {
		propKV = append(propKV, &commonpb.KeyValuePair{Key: key, Value: value})
	}
	return propKV
}

type alterCollectionFieldTask struct {
	baseTask
	Req *milvuspb.AlterCollectionFieldRequest
}

func (a *alterCollectionFieldTask) Prepare(ctx context.Context) error {
	if a.Req.GetCollectionName() == "" {
		return fmt.Errorf("alter collection field failed, collection name does not exists")
	}

	if a.Req.GetFieldName() == "" {
		return fmt.Errorf("alter collection field failed, filed name does not exists")
	}

	return nil
}

func (a *alterCollectionFieldTask) Execute(ctx context.Context) error {
	if a.Req.GetProperties() == nil {
		return errors.New("only support alter collection properties, but collection field properties is empty")
	}

	oldColl, err := a.core.meta.GetCollectionByName(ctx, a.Req.GetDbName(), a.Req.GetCollectionName(), a.ts)
	if err != nil {
		log.Warn("get collection failed during changing collection state",
			zap.String("collectionName", a.Req.GetCollectionName()),
			zap.String("fieldName", a.Req.GetFieldName()),
			zap.Uint64("ts", a.ts))
		return err
	}

	newColl := oldColl.Clone()
	err = UpdateFieldProperties(newColl, a.Req.GetFieldName(), a.Req.GetProperties())
	if err != nil {
		return err
	}
	ts := a.GetTs()

	tso, err := a.core.tsoAllocator.GenerateTSO(1)
	if err == nil {
		newColl.UpdateTimestamp = tso
	}

	redoTask := newBaseRedoTask(a.core.stepExecutor)
	redoTask.AddSyncStep(&AlterCollectionStep{
		baseStep: baseStep{core: a.core},
		oldColl:  oldColl,
		newColl:  newColl,
		ts:       ts,
	})

	redoTask.AddSyncStep(&BroadcastAlteredCollectionStep{
		baseStep: baseStep{core: a.core},
		req: &milvuspb.AlterCollectionRequest{
			Base:           a.Req.Base,
			DbName:         a.Req.DbName,
			CollectionName: a.Req.CollectionName,
			CollectionID:   oldColl.CollectionID,
		},
		core: a.core,
	})
	collectionNames := []string{}
	redoTask.AddSyncStep(&expireCacheStep{
		baseStep:        baseStep{core: a.core},
		dbName:          a.Req.GetDbName(),
		collectionNames: append(collectionNames, a.Req.GetCollectionName()),
		collectionID:    InvalidCollectionID,
		opts:            []proxyutil.ExpireCacheOpt{proxyutil.SetMsgType(commonpb.MsgType_AlterCollectionField)},
	})

	return redoTask.Execute(ctx)
}

func UpdateFieldProperties(coll *model.Collection, fieldName string, updatedProps []*commonpb.KeyValuePair) error {
	for i, field := range coll.Fields {
		if field.Name == fieldName {
			coll.Fields[i].TypeParams = UpdateFieldPropertyParams(field.TypeParams, updatedProps)
			return nil
		}
	}
	return merr.WrapErrParameterInvalidMsg("field %s does not exist in collection", fieldName)
}

func UpdateFieldPropertyParams(oldProps, updatedProps []*commonpb.KeyValuePair) []*commonpb.KeyValuePair {
	props := make(map[string]string)
	for _, prop := range oldProps {
		props[prop.Key] = prop.Value
	}
	log.Info("UpdateFieldPropertyParams", zap.Any("oldprops", props), zap.Any("newprops", updatedProps))
	for _, prop := range updatedProps {
		props[prop.Key] = prop.Value
	}
	log.Info("UpdateFieldPropertyParams", zap.Any("newprops", props))
	propKV := make([]*commonpb.KeyValuePair, 0)
	for key, value := range props {
		propKV = append(propKV, &commonpb.KeyValuePair{
			Key:   key,
			Value: value,
		})
	}

	return propKV
}

func (a *alterCollectionTask) GetLockerKey() LockerKey {
	collection := a.core.getCollectionIDStr(a.ctx, a.Req.GetDbName(), a.Req.GetCollectionName(), a.Req.GetCollectionID())
	return NewLockerKeyChain(
		NewClusterLockerKey(false),
		NewDatabaseLockerKey(a.Req.GetDbName(), false),
		NewCollectionLockerKey(collection, true),
	)
}

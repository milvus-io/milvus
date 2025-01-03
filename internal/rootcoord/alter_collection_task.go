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
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
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
	if a.Req.GetProperties() == nil && a.Req.GetDeleteKeys() == nil {
		return errors.New("The collection properties to alter and keys to delete must not be empty at the same time")
	}

	if len(a.Req.GetProperties()) > 0 && len(a.Req.GetDeleteKeys()) > 0 {
		return errors.New("can not provide properties and deletekeys at the same time")
	}

	oldColl, err := a.core.meta.GetCollectionByName(ctx, a.Req.GetDbName(), a.Req.GetCollectionName(), a.ts)
	if err != nil {
		log.Ctx(ctx).Warn("get collection failed during changing collection state",
			zap.String("collectionName", a.Req.GetCollectionName()), zap.Uint64("ts", a.ts))
		return err
	}

	var newProperties []*commonpb.KeyValuePair
	if len(a.Req.Properties) > 0 {
		if ContainsKeyPairArray(a.Req.GetProperties(), oldColl.Properties) {
			log.Info("skip to alter collection due to no changes were detected in the properties", zap.Int64("collectionID", oldColl.CollectionID))
			return nil
		}
		newProperties = MergeProperties(oldColl.Properties, a.Req.GetProperties())
	} else if len(a.Req.DeleteKeys) > 0 {
		newProperties = DeleteProperties(oldColl.Properties, a.Req.GetDeleteKeys())
	}

	ts := a.GetTs()
	return executeAlterCollectionTaskSteps(ctx, a.core, oldColl, oldColl.Properties, newProperties, a.Req, ts)
}

func (a *alterCollectionTask) GetLockerKey() LockerKey {
	collection := a.core.getCollectionIDStr(a.ctx, a.Req.GetDbName(), a.Req.GetCollectionName(), a.Req.GetCollectionID())
	return NewLockerKeyChain(
		NewClusterLockerKey(false),
		NewDatabaseLockerKey(a.Req.GetDbName(), false),
		NewCollectionLockerKey(collection, true),
	)
}

func executeAlterCollectionTaskSteps(ctx context.Context,
	core *Core,
	col *model.Collection,
	oldProperties []*commonpb.KeyValuePair,
	newProperties []*commonpb.KeyValuePair,
	request *milvuspb.AlterCollectionRequest,
	ts Timestamp,
) error {
	oldColl := col.Clone()
	oldColl.Properties = oldProperties
	newColl := col.Clone()
	newColl.Properties = newProperties
	redoTask := newBaseRedoTask(core.stepExecutor)
	redoTask.AddSyncStep(&AlterCollectionStep{
		baseStep: baseStep{core: core},
		oldColl:  oldColl,
		newColl:  newColl,
		ts:       ts,
	})

	request.CollectionID = oldColl.CollectionID
	redoTask.AddSyncStep(&BroadcastAlteredCollectionStep{
		baseStep: baseStep{core: core},
		req:      request,
		core:     core,
	})

	// properties needs to be refreshed in the cache
	aliases := core.meta.ListAliasesByID(ctx, oldColl.CollectionID)
	redoTask.AddSyncStep(&expireCacheStep{
		baseStep:        baseStep{core: core},
		dbName:          request.GetDbName(),
		collectionNames: append(aliases, request.GetCollectionName()),
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
			resp, err := core.queryCoord.UpdateLoadConfig(ctx, &querypb.UpdateLoadConfigRequest{
				CollectionIDs:  []int64{oldColl.CollectionID},
				ReplicaNumber:  int32(newReplicaNumber),
				ResourceGroups: newResourceGroups,
			})
			if err := merr.CheckRPCCall(resp, err); err != nil {
				log.Ctx(ctx).Warn("failed to trigger update load config for collection", zap.Int64("collectionID", newColl.CollectionID), zap.Error(err))
				return nil, err
			}
			return nil, nil
		}))
	}

	oldReplicateEnable, _ := common.IsReplicateEnabled(oldColl.Properties)
	replicateEnable, ok := common.IsReplicateEnabled(newColl.Properties)
	if ok && !replicateEnable && oldReplicateEnable {
		replicateID, _ := common.GetReplicateID(oldColl.Properties)
		redoTask.AddAsyncStep(NewSimpleStep("send replicate end msg for collection", func(ctx context.Context) ([]nestedStep, error) {
			msgPack := &msgstream.MsgPack{}
			msg := &msgstream.ReplicateMsg{
				BaseMsg: msgstream.BaseMsg{
					Ctx:            ctx,
					BeginTimestamp: ts,
					EndTimestamp:   ts,
					HashValues:     []uint32{0},
				},
				ReplicateMsg: &msgpb.ReplicateMsg{
					Base: &commonpb.MsgBase{
						MsgType:   commonpb.MsgType_Replicate,
						Timestamp: ts,
						ReplicateInfo: &commonpb.ReplicateInfo{
							IsReplicate: true,
							ReplicateID: replicateID,
						},
					},
					IsEnd:      true,
					Database:   newColl.DBName,
					Collection: newColl.Name,
				},
			}
			msgPack.Msgs = append(msgPack.Msgs, msg)
			log.Info("send replicate end msg",
				zap.String("collection", newColl.Name),
				zap.String("database", newColl.DBName),
				zap.String("replicateID", replicateID),
			)
			return nil, core.chanTimeTick.broadcastDmlChannels(newColl.PhysicalChannelNames, msgPack)
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

	oldFieldProperties, err := GetFieldProperties(oldColl, a.Req.GetFieldName())
	if err != nil {
		log.Warn("get field properties failed during changing collection state", zap.Error(err))
		return err
	}
	ts := a.GetTs()
	return executeAlterCollectionFieldTaskSteps(ctx, a.core, oldColl, oldFieldProperties, a.Req, ts)
}

func (a *alterCollectionFieldTask) GetLockerKey() LockerKey {
	collection := a.core.getCollectionIDStr(a.ctx, a.Req.GetDbName(), a.Req.GetCollectionName(), 0)
	return NewLockerKeyChain(
		NewClusterLockerKey(false),
		NewDatabaseLockerKey(a.Req.GetDbName(), false),
		NewCollectionLockerKey(collection, true),
	)
}

func executeAlterCollectionFieldTaskSteps(ctx context.Context,
	core *Core,
	col *model.Collection,
	oldFieldProperties []*commonpb.KeyValuePair,
	request *milvuspb.AlterCollectionFieldRequest,
	ts Timestamp,
) error {
	var err error
	filedName := request.GetFieldName()
	newFieldProperties := UpdateFieldPropertyParams(oldFieldProperties, request.GetProperties())
	oldColl := col.Clone()
	err = ResetFieldProperties(oldColl, filedName, oldFieldProperties)
	if err != nil {
		return err
	}
	newColl := col.Clone()
	err = ResetFieldProperties(newColl, filedName, newFieldProperties)
	if err != nil {
		return err
	}
	redoTask := newBaseRedoTask(core.stepExecutor)
	redoTask.AddSyncStep(&AlterCollectionStep{
		baseStep: baseStep{core: core},
		oldColl:  oldColl,
		newColl:  newColl,
		ts:       ts,
	})

	redoTask.AddSyncStep(&BroadcastAlteredCollectionStep{
		baseStep: baseStep{core: core},
		req: &milvuspb.AlterCollectionRequest{
			Base:           request.Base,
			DbName:         request.DbName,
			CollectionName: request.CollectionName,
			CollectionID:   oldColl.CollectionID,
		},
		core: core,
	})
	redoTask.AddSyncStep(&expireCacheStep{
		baseStep:        baseStep{core: core},
		dbName:          request.GetDbName(),
		collectionNames: []string{request.GetCollectionName()},
		collectionID:    oldColl.CollectionID,
		opts:            []proxyutil.ExpireCacheOpt{proxyutil.SetMsgType(commonpb.MsgType_AlterCollectionField)},
	})

	return redoTask.Execute(ctx)
}

func ResetFieldProperties(coll *model.Collection, fieldName string, newProps []*commonpb.KeyValuePair) error {
	for i, field := range coll.Fields {
		if field.Name == fieldName {
			coll.Fields[i].TypeParams = newProps
			return nil
		}
	}
	return merr.WrapErrParameterInvalidMsg("field %s does not exist in collection", fieldName)
}

func GetFieldProperties(coll *model.Collection, fieldName string) ([]*commonpb.KeyValuePair, error) {
	for _, field := range coll.Fields {
		if field.Name == fieldName {
			return field.TypeParams, nil
		}
	}
	return nil, merr.WrapErrParameterInvalidMsg("field %s does not exist in collection", fieldName)
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

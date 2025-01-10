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
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type alterDatabaseTask struct {
	baseTask
	Req *rootcoordpb.AlterDatabaseRequest
}

func (a *alterDatabaseTask) Prepare(ctx context.Context) error {
	if a.Req.GetDbName() == "" {
		return fmt.Errorf("alter database failed, database name does not exists")
	}

	// TODO SimFG maybe it will support to alter the replica.id properties in the future when the database has no collections
	// now it can't be because the latest database properties can't be notified to the querycoord and datacoord
	replicateID, _ := common.GetReplicateID(a.Req.Properties)
	if replicateID != "" {
		colls, err := a.core.meta.ListCollections(ctx, a.Req.DbName, a.ts, true)
		if err != nil {
			return err
		}
		if len(colls) > 0 {
			return errors.New("can't set replicate id on database with collections")
		}
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

	redoTask.AddSyncStep(&expireCacheStep{
		baseStep: baseStep{core: a.core},
		dbName:   newDB.Name,
		ts:       ts,
		// make sure to send the "expire cache" request
		// because it won't send this request when the length of collection names array is zero
		collectionNames: []string{""},
		opts: []proxyutil.ExpireCacheOpt{
			proxyutil.SetMsgType(commonpb.MsgType_AlterDatabase),
		},
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
			colls, err := a.core.meta.ListCollections(ctx, oldDB.Name, a.ts, true)
			if err != nil {
				log.Ctx(ctx).Warn("failed to trigger update load config for database", zap.Int64("dbID", oldDB.ID), zap.Error(err))
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
				log.Ctx(ctx).Warn("failed to trigger update load config for database", zap.Int64("dbID", oldDB.ID), zap.Error(err))
				return nil, err
			}
			return nil, nil
		}))
	}

	oldReplicateEnable, _ := common.IsReplicateEnabled(oldDB.Properties)
	newReplicateEnable, ok := common.IsReplicateEnabled(newDB.Properties)
	if ok && !newReplicateEnable && oldReplicateEnable {
		replicateID, _ := common.GetReplicateID(oldDB.Properties)
		redoTask.AddAsyncStep(NewSimpleStep("send replicate end msg for db", func(ctx context.Context) ([]nestedStep, error) {
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
					Database:   newDB.Name,
					Collection: "",
				},
			}
			msgPack.Msgs = append(msgPack.Msgs, msg)
			log.Info("send replicate end msg for db", zap.String("db", newDB.Name), zap.String("replicateID", replicateID))
			return nil, a.core.chanTimeTick.broadcastDmlChannels(a.core.chanTimeTick.listDmlChannels(), msgPack)
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
	_, existEndTS := common.GetReplicateEndTS(updatedProps)
	if existEndTS {
		updatedProps = append(updatedProps, &commonpb.KeyValuePair{
			Key:   common.ReplicateIDKey,
			Value: "",
		})
	}

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

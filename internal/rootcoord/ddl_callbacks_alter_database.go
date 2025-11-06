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

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/ce"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func (c *Core) broadcastAlterDatabase(ctx context.Context, req *rootcoordpb.AlterDatabaseRequest) error {
	if req.GetDbName() == "" {
		return merr.WrapErrParameterInvalidMsg("alter database failed, database name does not exists")
	}
	if req.GetProperties() == nil && req.GetDeleteKeys() == nil {
		return merr.WrapErrParameterInvalidMsg("alter database with empty properties and delete keys, expected to set either properties or delete keys")
	}

	if len(req.GetProperties()) > 0 && len(req.GetDeleteKeys()) > 0 {
		return merr.WrapErrParameterInvalidMsg("alter database cannot modify properties and delete keys at the same time")
	}

	if hookutil.ContainsCipherProperties(req.GetProperties(), req.GetDeleteKeys()) {
		return merr.WrapErrParameterInvalidMsg("can not alter cipher related properties")
	}

	// Validate timezone
	tz, exist := funcutil.TryGetAttrByKeyFromRepeatedKV(common.TimezoneKey, req.GetProperties())
	if exist && !funcutil.IsTimezoneValid(tz) {
		return merr.WrapErrParameterInvalidMsg("unknown or invalid IANA Time Zone ID: %s", tz)
	}

	broadcaster, err := startBroadcastWithDatabaseLock(ctx, req.GetDbName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	oldDB, err := c.meta.GetDatabaseByName(ctx, req.GetDbName(), typeutil.MaxTimestamp)
	if err != nil {
		return errors.Wrap(err, "failed to get database by name")
	}
	alterLoadConfig, err := c.getAlterLoadConfigOfAlterDatabase(ctx, req.GetDbName(), oldDB.Properties, req.GetProperties())
	if err != nil {
		return errors.Wrap(err, "failed to get alter load config of alter database")
	}

	// We only allow to alter or delete properties, not both.
	var newProperties []*commonpb.KeyValuePair
	if (len(req.GetProperties())) > 0 {
		if IsSubsetOfProperties(req.GetProperties(), oldDB.Properties) {
			// no changes were detected in the properties
			return errIgnoredAlterDatabase
		}
		newProperties = MergeProperties(oldDB.Properties, req.GetProperties())
	} else if (len(req.GetDeleteKeys())) > 0 {
		newProperties = DeleteProperties(oldDB.Properties, req.GetDeleteKeys())
		if len(newProperties) == len(oldDB.Properties) {
			// no changes were detected in the properties
			return errIgnoredAlterDatabase
		}
	}

	msg := message.NewAlterDatabaseMessageBuilderV2().
		WithHeader(&message.AlterDatabaseMessageHeader{
			DbName: req.GetDbName(),
			DbId:   oldDB.ID,
		}).
		WithBody(&message.AlterDatabaseMessageBody{
			Properties:      newProperties,
			AlterLoadConfig: alterLoadConfig,
		}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

// getAlterLoadConfigOfAlterDatabase gets the alter load config of alter database.
func (c *Core) getAlterLoadConfigOfAlterDatabase(ctx context.Context, dbName string, oldProps []*commonpb.KeyValuePair, newProps []*commonpb.KeyValuePair) (*message.AlterLoadConfigOfAlterDatabase, error) {
	oldReplicaNumber, _ := common.DatabaseLevelReplicaNumber(oldProps)
	oldResourceGroups, _ := common.DatabaseLevelResourceGroups(oldProps)
	newReplicaNumber, _ := common.DatabaseLevelReplicaNumber(newProps)
	newResourceGroups, _ := common.DatabaseLevelResourceGroups(newProps)
	left, right := lo.Difference(oldResourceGroups, newResourceGroups)
	rgChanged := len(left) > 0 || len(right) > 0
	replicaChanged := oldReplicaNumber != newReplicaNumber
	if !rgChanged && !replicaChanged {
		return nil, nil
	}

	colls, err := c.meta.ListCollections(ctx, dbName, typeutil.MaxTimestamp, true)
	if err != nil {
		return nil, err
	}
	if len(colls) == 0 {
		return nil, nil
	}
	return &message.AlterLoadConfigOfAlterDatabase{
		CollectionIds:  lo.Map(colls, func(coll *model.Collection, _ int) int64 { return coll.CollectionID }),
		ReplicaNumber:  int32(newReplicaNumber),
		ResourceGroups: newResourceGroups,
	}, nil
}

func (c *DDLCallback) alterDatabaseV1AckCallback(ctx context.Context, result message.BroadcastResultAlterDatabaseMessageV2) error {
	header := result.Message.Header()
	body := result.Message.MustBody()

	db := model.NewDatabase(header.DbId, header.DbName, etcdpb.DatabaseState_DatabaseCreated, result.Message.MustBody().Properties)
	if err := c.meta.AlterDatabase(ctx, db, result.GetControlChannelResult().TimeTick); err != nil {
		return errors.Wrap(err, "failed to alter database")
	}
	if body.AlterLoadConfig != nil {
		resp, err := c.mixCoord.UpdateLoadConfig(ctx, &querypb.UpdateLoadConfigRequest{
			CollectionIDs:  body.AlterLoadConfig.CollectionIds,
			ReplicaNumber:  body.AlterLoadConfig.ReplicaNumber,
			ResourceGroups: body.AlterLoadConfig.ResourceGroups,
		})
		if err := merr.CheckRPCCall(resp, err); err != nil {
			return errors.Wrap(err, "failed to update load config")
		}
	}
	return c.ExpireCaches(ctx, ce.NewBuilder().
		WithLegacyProxyCollectionMetaCache(
			ce.OptLPCMDBName(header.DbName),
			ce.OptLPCMMsgType(commonpb.MsgType_AlterDatabase),
		),
		result.GetControlChannelResult().TimeTick)
}

func MergeProperties(oldProps, updatedProps []*commonpb.KeyValuePair) []*commonpb.KeyValuePair {
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

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
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/ce"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func (c *Core) broadcastCreateDatabase(ctx context.Context, req *milvuspb.CreateDatabaseRequest) error {
	req.DbName = strings.TrimSpace(req.DbName)
	broadcaster, err := startBroadcastWithDatabaseLock(ctx, req.DbName)
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	if err := c.meta.CheckIfDatabaseCreatable(ctx, req); err != nil {
		return err
	}

	dbID, err := c.idAllocator.AllocOne()
	if err != nil {
		return errors.Wrap(err, "failed to allocate database id")
	}

	// Use dbID as ezID because the dbID is unqiue
	properties, err := hookutil.TidyDBCipherProperties(dbID, req.GetProperties())
	if err != nil {
		return errors.Wrap(err, "failed to tidy database cipher properties")
	}
	tz, exist := funcutil.TryGetAttrByKeyFromRepeatedKV(common.TimezoneKey, properties)
	if exist && !funcutil.IsTimezoneValid(tz) {
		return merr.WrapErrParameterInvalidMsg("unknown or invalid IANA Time Zone ID: %s", tz)
	}
	msg := message.NewCreateDatabaseMessageBuilderV2().
		WithHeader(&message.CreateDatabaseMessageHeader{
			DbName: req.GetDbName(),
			DbId:   dbID,
		}).
		WithBody(&message.CreateDatabaseMessageBody{
			Properties: properties,
		}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (c *DDLCallback) createDatabaseV1AckCallback(ctx context.Context, result message.BroadcastResultCreateDatabaseMessageV2) error {
	header := result.Message.Header()
	db := model.NewDatabase(header.DbId, header.DbName, etcdpb.DatabaseState_DatabaseCreated, result.Message.MustBody().Properties)
	if err := c.meta.CreateDatabase(ctx, db, result.GetControlChannelResult().TimeTick); err != nil {
		return errors.Wrap(err, "failed to create database")
	}
	return c.ExpireCaches(ctx, ce.NewBuilder().
		WithLegacyProxyCollectionMetaCache(
			ce.OptLPCMDBName(header.DbName),
			ce.OptLPCMMsgType(commonpb.MsgType_DropDatabase),
		),
		result.GetControlChannelResult().TimeTick)
}

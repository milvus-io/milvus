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
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type dropCollectionTask struct {
	*Core
	Req       *milvuspb.DropCollectionRequest
	header    *message.DropCollectionMessageHeader
	body      *message.DropCollectionRequest
	vchannels []string
}

func (t *dropCollectionTask) validate(ctx context.Context) error {
	// Critical promise here, also see comment of startBroadcastWithCollectionLock.
	if t.meta.IsAlias(ctx, t.Req.GetDbName(), t.Req.GetCollectionName()) {
		return fmt.Errorf("cannot drop the collection via alias = %s", t.Req.CollectionName)
	}

	// use max ts to check if latest collection exists.
	// we cannot handle case that
	// dropping collection with `ts1` but a collection exists in catalog with newer ts which is bigger than `ts1`.
	// fortunately, if ddls are promised to execute in sequence, then everything is OK. The `ts1` will always be latest.
	collMeta, err := t.meta.GetCollectionByName(ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), typeutil.MaxTimestamp)
	if err != nil {
		if errors.Is(err, merr.ErrCollectionNotFound) || errors.Is(err, merr.ErrDatabaseNotFound) {
			return errIgnoredDropCollection
		}
		return err
	}

	// meta cache of all aliases should also be cleaned.
	aliases := t.meta.ListAliasesByID(ctx, collMeta.CollectionID)

	// Check if all aliases have been dropped.
	if len(aliases) > 0 {
		err = fmt.Errorf("unable to drop the collection [%s] because it has associated aliases %v, please remove all aliases before dropping the collection", t.Req.GetCollectionName(), aliases)
		log.Ctx(ctx).Warn("drop collection failed", zap.String("database", t.Req.GetDbName()), zap.Error(err))
		return err
	}

	// fill the message body and header
	// TODO: cleanupMetricsStep
	t.header = &message.DropCollectionMessageHeader{
		CollectionId: collMeta.CollectionID,
		DbId:         collMeta.DBID,
	}
	t.body = &message.DropCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_DropCollection,
		},
		CollectionID:   collMeta.CollectionID,
		DbID:           collMeta.DBID,
		CollectionName: t.Req.CollectionName,
		DbName:         collMeta.DBName,
	}
	t.vchannels = collMeta.VirtualChannelNames
	return nil
}

func (t *dropCollectionTask) Prepare(ctx context.Context) error {
	return t.validate(ctx)
}

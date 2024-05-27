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
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type truncateCollectionTask struct {
	baseTask
	Req              *rootcoordpb.TruncateCollectionRequest
	collectionID     int64
	tempCollectionID int64
}

func (t *truncateCollectionTask) validate() error {
	// todo update to MsgType_TruncateCollection
	if err := CheckMsgType(t.Req.GetBase().GetMsgType(), commonpb.MsgType_DropCollection); err != nil {
		return err
	}
	if t.core.meta.IsAlias(t.Req.GetDbName(), t.Req.GetCollectionName()) {
		return fmt.Errorf("cannot drop the collection via alias = %s", t.Req.CollectionName)
	}
	return nil
}

func (t *truncateCollectionTask) Prepare(ctx context.Context) error {
	return t.validate()
}

func (t *truncateCollectionTask) Execute(ctx context.Context) error {
	collMeta, err := t.core.meta.GetCollectionByName(ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}
	aliases := t.core.meta.ListAliasesByID(collMeta.CollectionID)
	log.Ctx(ctx).Debug("get target collection meta", zap.Any("meta", collMeta), zap.Any("aliases", aliases))
	if err := t.core.ExpireMetaCache(ctx, t.Req.GetDbName(), append(aliases, collMeta.Name), collMeta.CollectionID, "", t.GetTs(), proxyutil.SetMsgType(commonpb.MsgType_DropCollection)); err != nil {
		return err
	}
	t.collectionID, t.tempCollectionID, err = t.core.meta.ReplaceCollectionWithTemp(ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), t.GetTs())
	return err
}

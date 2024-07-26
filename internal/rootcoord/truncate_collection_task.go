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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
)

type truncateCollectionTask struct {
	baseTask
	mt *markCollectionTask
	ct *createCollectionTask
}

func (t *truncateCollectionTask) Prepare(ctx context.Context) error {
	return nil
}

func (t *truncateCollectionTask) Execute(ctx context.Context) error {
	// todo MsgType_TruncateCollection
	// no need of collectionName
	err := t.core.ExpireMetaCache(t.ctx, t.mt.Req.GetDbName(), []string{t.mt.Req.GetCollectionName()}, t.mt.collection.CollectionID, "", t.GetTs(), proxyutil.SetMsgType(commonpb.MsgType_DropCollection))
	if err != nil {
		return err
	}
	return t.core.meta.ExchangeCollectionIDs(t.ctx, t.mt.Req.GetDbName(), t.mt.Req.GetCollectionName(), t.GetTs())
}

type markCollectionTask struct {
	baseTask
	Req        *milvuspb.DropCollectionRequest
	fromState  pb.CollectionState
	toState    pb.CollectionState
	collection *model.Collection
}

func (b *markCollectionTask) Prepare(ctx context.Context) error {
	return nil
}

func (t *markCollectionTask) Execute(ctx context.Context) error {
	var err error
	t.collection, err = t.core.meta.GetCollectionByName(t.ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), t.GetTs())
	if err != nil {
		return err
	}
	if t.collection.State != t.fromState {
		return fmt.Errorf("collection's state is unexcepted")
	}
	// todo MsgType_TruncateCollection
	// no need of collectionName
	err = t.core.ExpireMetaCache(t.ctx, t.Req.GetDbName(), []string{t.Req.GetCollectionName()}, t.collection.CollectionID, "", t.GetTs(), proxyutil.SetMsgType(commonpb.MsgType_DropCollection))
	if err != nil {
		return err
	}
	return t.core.meta.ChangeCollectionState(t.ctx, t.collection.CollectionID, t.toState, t.GetTs())
}

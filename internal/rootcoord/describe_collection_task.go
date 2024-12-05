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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

// describeCollectionTask describe collection request task
type describeCollectionTask struct {
	baseTask
	Req              *milvuspb.DescribeCollectionRequest
	Rsp              *milvuspb.DescribeCollectionResponse
	allowUnavailable bool
}

func (t *describeCollectionTask) Prepare(ctx context.Context) error {
	if err := CheckMsgType(t.Req.Base.MsgType, commonpb.MsgType_DescribeCollection); err != nil {
		return err
	}
	return nil
}

// Execute task execution
func (t *describeCollectionTask) Execute(ctx context.Context) (err error) {
	coll, err := t.core.describeCollection(ctx, t.Req, t.allowUnavailable)
	if err != nil {
		return err
	}

	aliases := t.core.meta.ListAliasesByID(ctx, coll.CollectionID)
	db, err := t.core.meta.GetDatabaseByID(ctx, coll.DBID, t.GetTs())
	if err != nil {
		return err
	}
	t.Rsp = convertModelToDesc(coll, aliases, db.Name)
	t.Rsp.RequestTime = t.ts
	return nil
}

func (t *describeCollectionTask) GetLockerKey() LockerKey {
	collection := t.core.getCollectionIDStr(t.ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), t.Req.GetCollectionID())
	return NewLockerKeyChain(
		NewClusterLockerKey(false),
		NewDatabaseLockerKey(t.Req.GetDbName(), false),
		NewCollectionLockerKey(collection, false),
	)
}

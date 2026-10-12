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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
	// Component metadata lookups deliberately carry no user. All other calls
	// must retain the identity forwarded by Proxy, even when describing by ID.
	if Params.CommonCfg.AuthorizationEnabled.GetAsBool() && !contextutil.IsIntraClusterRequest(ctx) {
		user, userErr := contextutil.GetCurUserFromContext(ctx)
		if userErr != nil || user == "" {
			err = merr.WrapErrPrivilegeNotPermitted("describe collection requires an authenticated user")
			t.Rsp.Status = merr.Status(err)
			return err
		}
	}
	coll, err := t.core.describeCollection(ctx, t.Req, t.allowUnavailable)
	if err != nil {
		return err
	}

	// ID lookups are cluster-wide. Authorize the resolved collection in its
	// actual database, never the database/name supplied as lookup hints.
	db, err := t.core.meta.GetDatabaseByID(ctx, coll.DBID, t.GetTs())
	if err != nil {
		return err
	}
	visibleCollections, err := t.core.getCurrentUserVisibleCollections(ctx, db.Name)
	if err != nil {
		t.Rsp.Status = merr.Status(err)
		return err
	}
	if !isVisibleCollectionForCurUser(coll.Name, visibleCollections) {
		err = merr.WrapErrPrivilegeNotPermitted("not allowed to access collection")
		t.Rsp.Status = merr.Status(err)
		return err
	}

	aliases := t.core.meta.ListAliasesByID(ctx, coll.CollectionID)
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

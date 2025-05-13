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

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type listDatabaseTask struct {
	baseTask
	Req  *milvuspb.ListDatabasesRequest
	Resp *milvuspb.ListDatabasesResponse
}

func (t *listDatabaseTask) Prepare(ctx context.Context) error {
	return nil
}

func (t *listDatabaseTask) Execute(ctx context.Context) error {
	t.Resp.Status = merr.Success()

	visibleDBs, err := t.core.getCurrentUserVisibleDatabases(ctx)
	if err != nil {
		t.Resp.Status = merr.Status(err)
		return err
	}
	if len(visibleDBs) == 0 {
		return nil
	}

	ret, err := t.core.meta.ListDatabases(ctx, t.GetTs())
	if err != nil {
		t.Resp.Status = merr.Status(err)
		return err
	}

	dbNames := make([]string, 0, len(ret))
	dbIDs := make([]int64, 0, len(ret))
	createdTimes := make([]uint64, 0, len(ret))
	for _, db := range ret {
		if !isVisibleDatabaseForCurUser(db.Name, visibleDBs) {
			continue
		}
		dbNames = append(dbNames, db.Name)
		dbIDs = append(dbIDs, db.ID)
		createdTimes = append(createdTimes, db.CreatedTime)
	}
	t.Resp.DbNames = dbNames
	t.Resp.DbIds = dbIDs
	t.Resp.CreatedTimestamp = createdTimes
	return nil
}

func (t *listDatabaseTask) GetLockerKey() LockerKey {
	return NewLockerKeyChain(NewClusterLockerKey(false))
}

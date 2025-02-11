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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/contextutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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

	getVisibleDBs := func() (typeutil.Set[string], error) {
		enableAuth := Params.CommonCfg.AuthorizationEnabled.GetAsBool()
		privilegeDBs := typeutil.NewSet[string]()
		if !enableAuth {
			privilegeDBs.Insert(util.AnyWord)
			return privilegeDBs, nil
		}
		curUser, err := contextutil.GetCurUserFromContext(ctx)
		// it will fail if the inner node server use the list database API
		if err != nil || (curUser == util.UserRoot && !Params.CommonCfg.RootShouldBindRole.GetAsBool()) {
			if err != nil {
				log.Warn("get current user from context failed", zap.Error(err))
			}
			privilegeDBs.Insert(util.AnyWord)
			return privilegeDBs, nil
		}
		userRoles, err := t.core.meta.SelectUser("", &milvuspb.UserEntity{
			Name: curUser,
		}, true)
		if err != nil {
			return nil, err
		}
		if len(userRoles) == 0 {
			return privilegeDBs, nil
		}
		for _, role := range userRoles[0].Roles {
			if role.GetName() == util.RoleAdmin {
				privilegeDBs.Insert(util.AnyWord)
				return privilegeDBs, nil
			}
			if role.GetName() == util.RolePublic {
				continue
			}
			entities, err := t.core.meta.SelectGrant("", &milvuspb.GrantEntity{
				Role:   role,
				DbName: util.AnyWord,
			})
			if err != nil {
				return nil, err
			}
			for _, entity := range entities {
				privilegeDBs.Insert(entity.GetDbName())
				if entity.GetDbName() == util.AnyWord {
					return privilegeDBs, nil
				}
			}
		}
		return privilegeDBs, nil
	}

	isVisibleDBForCurUser := func(dbName string, visibleDBs typeutil.Set[string]) bool {
		if visibleDBs.Contain(util.AnyWord) {
			return true
		}
		return visibleDBs.Contain(dbName)
	}

	visibleDBs, err := getVisibleDBs()
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
	createdTimes := make([]uint64, 0, len(ret))
	for _, db := range ret {
		if !isVisibleDBForCurUser(db.Name, visibleDBs) {
			continue
		}
		dbNames = append(dbNames, db.Name)
		createdTimes = append(createdTimes, db.CreatedTime)
	}
	t.Resp.DbNames = dbNames
	t.Resp.CreatedTimestamp = createdTimes
	return nil
}

func (t *listDatabaseTask) GetLockerKey() LockerKey {
	return NewLockerKeyChain(NewClusterLockerKey(false))
}

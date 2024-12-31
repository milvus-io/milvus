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

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/contextutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// showCollectionTask show collection request task
type showCollectionTask struct {
	baseTask
	Req *milvuspb.ShowCollectionsRequest
	Rsp *milvuspb.ShowCollectionsResponse
}

func (t *showCollectionTask) Prepare(ctx context.Context) error {
	if err := CheckMsgType(t.Req.Base.MsgType, commonpb.MsgType_ShowCollections); err != nil {
		return err
	}
	return nil
}

// Execute task execution
func (t *showCollectionTask) Execute(ctx context.Context) error {
	t.Rsp.Status = merr.Success()

	getVisibleCollections := func() (typeutil.Set[string], error) {
		enableAuth := Params.CommonCfg.AuthorizationEnabled.GetAsBool()
		privilegeColls := typeutil.NewSet[string]()
		if !enableAuth {
			privilegeColls.Insert(util.AnyWord)
			return privilegeColls, nil
		}
		curUser, err := contextutil.GetCurUserFromContext(ctx)
		if err != nil || curUser == util.UserRoot {
			if err != nil {
				log.Ctx(ctx).Warn("get current user from context failed", zap.Error(err))
			}
			privilegeColls.Insert(util.AnyWord)
			return privilegeColls, nil
		}
		userRoles, err := t.core.meta.SelectUser(ctx, "", &milvuspb.UserEntity{
			Name: curUser,
		}, true)
		if err != nil {
			return nil, err
		}
		if len(userRoles) == 0 {
			return privilegeColls, nil
		}
		for _, role := range userRoles[0].Roles {
			if role.GetName() == util.RoleAdmin {
				privilegeColls.Insert(util.AnyWord)
				return privilegeColls, nil
			}
			entities, err := t.core.meta.SelectGrant(ctx, "", &milvuspb.GrantEntity{
				Role:   role,
				DbName: t.Req.GetDbName(),
			})
			if err != nil {
				return nil, err
			}
			for _, entity := range entities {
				objectType := entity.GetObject().GetName()
				priv := entity.GetGrantor().GetPrivilege().GetName()
				if objectType == commonpb.ObjectType_Global.String() &&
					priv == util.PrivilegeNameForAPI(commonpb.ObjectPrivilege_PrivilegeAll.String()) {
					privilegeColls.Insert(util.AnyWord)
					return privilegeColls, nil
				}
				// should list collection level built-in privilege group objects
				if objectType != commonpb.ObjectType_Collection.String() &&
					!Params.RbacConfig.IsCollectionPrivilegeGroup(priv) {
					continue
				}
				collectionName := entity.GetObjectName()
				privilegeColls.Insert(collectionName)
				if collectionName == util.AnyWord {
					return privilegeColls, nil
				}
			}
		}
		return privilegeColls, nil
	}

	isVisibleCollectionForCurUser := func(collectionName string, visibleCollections typeutil.Set[string]) bool {
		if visibleCollections.Contain(util.AnyWord) {
			return true
		}
		return visibleCollections.Contain(collectionName)
	}

	visibleCollections, err := getVisibleCollections()
	if err != nil {
		t.Rsp.Status = merr.Status(err)
		return err
	}
	if len(visibleCollections) == 0 {
		return nil
	}

	ts := t.Req.GetTimeStamp()
	if ts == 0 {
		ts = typeutil.MaxTimestamp
	}
	colls, err := t.core.meta.ListCollections(ctx, t.Req.GetDbName(), ts, true)
	if err != nil {
		t.Rsp.Status = merr.Status(err)
		return err
	}
	for _, coll := range colls {
		if len(t.Req.GetCollectionNames()) > 0 && !lo.Contains(t.Req.GetCollectionNames(), coll.Name) {
			continue
		}
		if !isVisibleCollectionForCurUser(coll.Name, visibleCollections) {
			continue
		}

		t.Rsp.CollectionNames = append(t.Rsp.CollectionNames, coll.Name)
		t.Rsp.CollectionIds = append(t.Rsp.CollectionIds, coll.CollectionID)
		t.Rsp.CreatedTimestamps = append(t.Rsp.CreatedTimestamps, coll.CreateTime)
		physical, _ := tsoutil.ParseHybridTs(coll.CreateTime)
		t.Rsp.CreatedUtcTimestamps = append(t.Rsp.CreatedUtcTimestamps, uint64(physical))
	}
	return nil
}

func (t *showCollectionTask) GetLockerKey() LockerKey {
	return NewLockerKeyChain(NewClusterLockerKey(false), NewDatabaseLockerKey(t.Req.GetDbName(), false))
}

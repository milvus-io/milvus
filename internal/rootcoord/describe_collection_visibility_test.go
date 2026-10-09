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
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/interceptor"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestDescribeCollectionResolvedVisibility(t *testing.T) {
	paramtable.Init()
	require.NoError(t, Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true"))
	t.Cleanup(func() { Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key) })
	for _, lookup := range []string{"name", "alias", "id with another database", "id without database"} {
		t.Run(lookup, func(t *testing.T) {
			const collectionID int64 = 77
			meta := mockrootcoord.NewIMetaTable(t)
			core := newTestCore(withMeta(meta))
			coll := &model.Collection{CollectionID: collectionID, Name: "orders", DBID: 9}
			req := &milvuspb.DescribeCollectionRequest{DbName: "tenant_b", CollectionName: "orders"}
			switch lookup {
			case "alias":
				req.CollectionName = "orders_alias"
			case "id with another database":
				req = &milvuspb.DescribeCollectionRequest{DbName: "tenant_a", CollectionID: collectionID}
			case "id without database":
				req = &milvuspb.DescribeCollectionRequest{CollectionID: collectionID}
			}
			if req.CollectionName == "" {
				meta.EXPECT().GetCollectionByID(mock.Anything, req.DbName, collectionID, mock.Anything, false).Return(coll, nil)
			} else {
				meta.EXPECT().GetCollectionByName(mock.Anything, req.DbName, req.CollectionName, mock.Anything, false).Return(coll, nil)
			}
			meta.EXPECT().GetDatabaseByID(mock.Anything, int64(9), mock.Anything).
				Return(&model.Database{ID: 9, Name: "tenant_b"}, nil)
			meta.EXPECT().SelectUser(mock.Anything, "", mock.MatchedBy(func(user *milvuspb.UserEntity) bool {
				return user.GetName() == "alice"
			}), true).Return([]*milvuspb.UserResult{{
				Roles: []*milvuspb.RoleEntity{{Name: util.RolePublic}, {Name: "alice_role"}},
			}}, nil)
			meta.EXPECT().ListPrivilegeGroups(mock.Anything).Return(nil, nil)
			granted := false
			meta.EXPECT().SelectGrant(mock.Anything, "", mock.Anything).RunAndReturn(
				func(_ context.Context, _ string, grant *milvuspb.GrantEntity) ([]*milvuspb.GrantEntity, error) {
					require.Equal(t, "alice_role", grant.GetRole().GetName(), "public does not establish object visibility")
					require.Equal(t, "tenant_b", grant.GetDbName(), "an ID's actual database must govern authorization")
					if !granted {
						return nil, nil
					}
					return []*milvuspb.GrantEntity{{
						Object: &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Collection.String()}, ObjectName: "orders",
					}}, nil
				})
			meta.EXPECT().ListAliasesByID(mock.Anything, collectionID).Return([]string{"orders_alias"}).Once()
			ctx := GetContext(context.Background(), "alice:password")
			for _, allow := range []bool{false, true, false} {
				granted = allow
				task := &describeCollectionTask{baseTask: newBaseTask(ctx, core), Req: req, Rsp: &milvuspb.DescribeCollectionResponse{}}
				err := task.Execute(ctx)
				if allow {
					require.NoError(t, err)
					require.Equal(t, "tenant_b", task.Rsp.GetDbName())
					require.Equal(t, "orders", task.Rsp.GetCollectionName())
					require.Equal(t, collectionID, task.Rsp.GetCollectionID())
				} else {
					require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
					require.ErrorIs(t, merr.Error(task.Rsp.GetStatus()), merr.ErrPrivilegeNotPermitted)
					require.Nil(t, task.Rsp.GetSchema())
				}
			}
		})
	}
}

func TestDescribeCollectionIdentityBoundary(t *testing.T) {
	paramtable.Init()
	require.NoError(t, Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true"))
	t.Cleanup(func() { Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key) })
	for _, tc := range []struct {
		name     string
		ctx      context.Context
		internal bool
	}{
		{"in process component", context.Background(), true},
		{"component RPC", metadata.NewIncomingContext(context.Background(), metadata.Pairs(interceptor.ServerIDKey, "2")), true},
		{"missing forwarded identity", metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-user", "root")), false},
		{"malformed identity", metadata.NewIncomingContext(context.Background(), metadata.Pairs(util.HeaderAuthorize, "invalid")), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			meta := mockrootcoord.NewIMetaTable(t)
			core := newTestCore(withMeta(meta))
			if tc.internal {
				meta.EXPECT().GetCollectionByID(mock.Anything, "", int64(77), mock.Anything, false).
					Return(&model.Collection{CollectionID: 77, Name: "orders", DBID: 9}, nil).Once()
				meta.EXPECT().GetDatabaseByID(mock.Anything, int64(9), mock.Anything).
					Return(&model.Database{ID: 9, Name: "tenant_b"}, nil).Once()
				meta.EXPECT().ListAliasesByID(mock.Anything, int64(77)).Return(nil).Once()
			}
			task := &describeCollectionTask{
				baseTask: newBaseTask(tc.ctx, core), Req: &milvuspb.DescribeCollectionRequest{CollectionID: 77},
				Rsp: &milvuspb.DescribeCollectionResponse{},
			}
			err := task.Execute(tc.ctx)
			if tc.internal {
				require.NoError(t, err)
				require.Equal(t, int64(77), task.Rsp.GetCollectionID())
			} else {
				require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
				require.Nil(t, task.Rsp.GetSchema())
			}
		})
	}
}

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

package rbac

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/crypto"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

type describeCollectionVisibilitySuite struct {
	integration.MiniClusterSuite
	cached bool
}

func (s *describeCollectionVisibilitySuite) SetupSuite() {
	paramtable.Init()
	params := paramtable.Get()
	s.WithMilvusConfig(params.CommonCfg.AuthorizationEnabled.Key, "true")
	s.WithMilvusConfig(params.ProxyCfg.EnableCachedServiceProvider.Key, strconv.FormatBool(s.cached))
	s.MiniClusterSuite.SetupSuite()
}

// Exercise the public gRPC authentication, Proxy provider, coordinator transport
// and stored RBAC grants together. The metadata cache must never cache the
// administrator's authorization or keep a user's authorization after revoke.
func (s *describeCollectionVisibilitySuite) TestVisibility() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	rootCtx := GetContext(ctx, defaultAuth)
	userCtx := GetContext(ctx, "describe_user:describe_password")
	client := s.Cluster.MilvusClient
	const (
		ownDB      = "describe_tenant_a"
		otherDB    = "describe_tenant_b"
		collection = "orders"
		alias      = "orders_alias"
		role       = "describe_role"
	)
	mustStatus := func(status *commonpb.Status, err error) {
		s.Require().NoError(err)
		s.Require().NoError(merr.Error(status))
	}
	for _, db := range []string{ownDB, otherDB} {
		mustStatus(client.CreateDatabase(rootCtx, &milvuspb.CreateDatabaseRequest{DbName: db}))
		schema, err := proto.Marshal(integration.ConstructSchema(collection, dim, true))
		s.Require().NoError(err)
		mustStatus(client.CreateCollection(rootCtx, &milvuspb.CreateCollectionRequest{
			DbName: db, CollectionName: collection, Schema: schema,
		}))
	}
	mustStatus(client.CreateAlias(rootCtx, &milvuspb.CreateAliasRequest{
		DbName: otherDB, CollectionName: collection, Alias: alias,
	}))
	mustStatus(client.CreateRole(rootCtx, &milvuspb.CreateRoleRequest{Entity: &milvuspb.RoleEntity{Name: role}}))
	mustStatus(client.CreateCredential(rootCtx, &milvuspb.CreateCredentialRequest{
		Username: "describe_user", Password: crypto.Base64Encode("describe_password"),
	}))
	mustStatus(client.OperateUserRole(rootCtx, &milvuspb.OperateUserRoleRequest{
		Username: "describe_user", RoleName: role, Type: milvuspb.OperateUserRoleType_AddUserToRole,
	}))
	grant := func(db string, operation milvuspb.OperatePrivilegeType) {
		mustStatus(client.OperatePrivilegeV2(rootCtx, &milvuspb.OperatePrivilegeV2Request{
			Role: &milvuspb.RoleEntity{Name: role},
			Grantor: &milvuspb.GrantorEntity{
				User: &milvuspb.UserEntity{Name: util.UserRoot}, Privilege: &milvuspb.PrivilegeEntity{Name: "Search"},
			},
			Type: operation, DbName: db, CollectionName: collection,
		}))
	}
	grant(ownDB, milvuspb.OperatePrivilegeType_Grant)

	// Obtain the ID without warming DescribeCollection's metadata cache.
	listed, err := client.ShowCollections(rootCtx, &milvuspb.ShowCollectionsRequest{DbName: otherDB})
	s.Require().NoError(err)
	s.Require().NoError(merr.Error(listed.GetStatus()))
	s.Require().Equal([]string{collection}, listed.GetCollectionNames())
	s.Require().Len(listed.GetCollectionIds(), 1)
	collectionID := listed.GetCollectionIds()[0]
	lookups := []struct {
		name string
		req  *milvuspb.DescribeCollectionRequest
	}{
		{"name", &milvuspb.DescribeCollectionRequest{DbName: otherDB, CollectionName: collection}},
		{"alias", &milvuspb.DescribeCollectionRequest{DbName: otherDB, CollectionName: alias}},
		{"ID with another database", &milvuspb.DescribeCollectionRequest{DbName: ownDB, CollectionID: collectionID}},
		{"ID without database", &milvuspb.DescribeCollectionRequest{CollectionID: collectionID}},
	}
	check := func(allowed bool) {
		for _, lookup := range lookups {
			resp, err := client.DescribeCollection(userCtx, lookup.req)
			s.Require().NoError(err, lookup.name)
			if allowed {
				s.Require().NoError(merr.Error(resp.GetStatus()), lookup.name)
				s.Equal(collectionID, resp.GetCollectionID(), lookup.name)
				s.Equal(otherDB, resp.GetDbName(), lookup.name)
				s.Equal(collection, resp.GetSchema().GetName(), lookup.name)
			} else {
				s.ErrorIs(merr.Error(resp.GetStatus()), merr.ErrPrivilegeNotPermitted, lookup.name)
				s.Nil(resp.GetSchema(), lookup.name)
				s.Zero(resp.GetCollectionID(), lookup.name)
				s.Empty(resp.GetAliases(), lookup.name)
			}
		}
	}
	check(false)
	// The identical name in the user's database must remain accessible.
	own, err := client.DescribeCollection(userCtx, &milvuspb.DescribeCollectionRequest{DbName: ownDB, CollectionName: collection})
	s.Require().NoError(err)
	s.Require().NoError(merr.Error(own.GetStatus()))
	s.Equal(ownDB, own.GetDbName())
	s.NotEqual(collectionID, own.GetCollectionID())

	warm, err := client.DescribeCollection(rootCtx, lookups[0].req)
	s.Require().NoError(err)
	s.Require().NoError(merr.Error(warm.GetStatus()))
	s.Equal(collectionID, warm.GetCollectionID())
	check(false)
	grant(otherDB, milvuspb.OperatePrivilegeType_Grant)
	check(true)
	grant(otherDB, milvuspb.OperatePrivilegeType_Revoke)
	check(false)
}

func TestDescribeCollectionVisibility(t *testing.T) {
	for _, cached := range []bool{true, false} {
		t.Run("cached="+strconv.FormatBool(cached), func(t *testing.T) {
			suite.Run(t, &describeCollectionVisibilitySuite{cached: cached})
		})
	}
}

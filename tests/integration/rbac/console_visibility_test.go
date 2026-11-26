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
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	mhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/crypto"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
	"github.com/milvus-io/milvus/tests/integration/cluster"
)

// consoleVisibilitySuite drives the web console routes on the proxy's metrics
// port over HTTP, through the real authentication middleware, Proxy, coordinator
// transport and stored RBAC grants.
type consoleVisibilitySuite struct {
	integration.MiniClusterSuite
	adminAuth bool
}

func (s *consoleVisibilitySuite) SetupSuite() {
	paramtable.Init()
	params := paramtable.Get()
	s.WithMilvusConfig(params.CommonCfg.AuthorizationEnabled.Key, "true")
	s.WithMilvusConfig(params.CommonCfg.AdminAuthEnabled.Key, strconv.FormatBool(s.adminAuth))
	s.WithClusterOptions(cluster.WithMetricsPorts())
	s.MiniClusterSuite.SetupSuite()
}

type consoleResponse struct {
	status int
	body   []byte
}

func (s *consoleVisibilitySuite) get(user, password, path string, query url.Values) consoleResponse {
	addr := s.Cluster.DefaultProxy().GetMetricsAddr()
	s.Require().NotEmpty(addr, "proxy has no metrics port")
	target := url.URL{Scheme: "http", Host: addr, Path: "/api/v1" + path, RawQuery: query.Encode()}
	req, err := http.NewRequest(http.MethodGet, target.String(), nil)
	s.Require().NoError(err)
	req.SetBasicAuth(user, password)
	// A non-browser client declares a management request explicitly.
	req.Header.Set(mhttp.AdminRequestHeader, "true")
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	s.Require().NoError(err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	s.Require().NoError(err)
	return consoleResponse{status: resp.StatusCode, body: body}
}

func (s *consoleVisibilitySuite) TestVisibility() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	rootCtx := GetContext(ctx, defaultAuth)
	client := s.Cluster.MilvusClient
	const (
		ownDB    = "console_tenant_a"
		otherDB  = "console_tenant_b"
		shared   = "orders"
		secret   = "secret_orders"
		role     = "console_role"
		user     = "console_user"
		password = "console_password"
		rootPwd  = "Milvus"
	)
	mustStatus := func(status *commonpb.Status, err error) {
		s.Require().NoError(err)
		s.Require().NoError(merr.Error(status))
	}
	createCollection := func(db, name string) {
		schema, err := proto.Marshal(integration.ConstructSchema(name, dim, true))
		s.Require().NoError(err)
		mustStatus(client.CreateCollection(rootCtx, &milvuspb.CreateCollectionRequest{
			DbName: db, CollectionName: name, Schema: schema,
		}))
	}
	for _, db := range []string{ownDB, otherDB} {
		mustStatus(client.CreateDatabase(rootCtx, &milvuspb.CreateDatabaseRequest{DbName: db}))
		createCollection(db, shared)
	}
	createCollection(otherDB, secret)
	mustStatus(client.CreateRole(rootCtx, &milvuspb.CreateRoleRequest{Entity: &milvuspb.RoleEntity{Name: role}}))
	mustStatus(client.CreateCredential(rootCtx, &milvuspb.CreateCredentialRequest{
		Username: user, Password: crypto.Base64Encode(password),
	}))
	mustStatus(client.OperateUserRole(rootCtx, &milvuspb.OperateUserRoleRequest{
		Username: user, RoleName: role, Type: milvuspb.OperateUserRoleType_AddUserToRole,
	}))
	mustStatus(client.OperatePrivilegeV2(rootCtx, &milvuspb.OperatePrivilegeV2Request{
		Role: &milvuspb.RoleEntity{Name: role},
		Grantor: &milvuspb.GrantorEntity{
			User: &milvuspb.UserEntity{Name: util.UserRoot}, Privilege: &milvuspb.PrivilegeEntity{Name: "Search"},
		},
		Type: milvuspb.OperatePrivilegeType_Grant, DbName: ownDB, CollectionName: shared,
	}))

	listDatabases := func(user, password string) []string {
		resp := s.get(user, password, mhttp.DatabaseListPath, nil)
		s.Require().Equal(http.StatusOK, resp.status, string(resp.body))
		var dbs metricsinfo.Databases
		s.Require().NoError(json.Unmarshal(resp.body, &dbs))
		return dbs.Names
	}
	listCollections := func(user, password, db string) []string {
		resp := s.get(user, password, mhttp.CollectionListPath, url.Values{"db_name": {db}})
		s.Require().Equal(http.StatusOK, resp.status, string(resp.body))
		var colls metricsinfo.Collections
		s.Require().NoError(json.Unmarshal(resp.body, &colls))
		s.Require().Len(colls.CollectionIDs, len(colls.CollectionNames))
		s.Require().Len(colls.InMemoryPercentages, len(colls.CollectionNames))
		return colls.CollectionNames
	}
	describeDatabase := func(user, password, db string) consoleResponse {
		return s.get(user, password, mhttp.DatabaseDescPath, url.Values{"db_name": {db}})
	}
	describeCollection := func(user, password, db, collection string) consoleResponse {
		return s.get(user, password, mhttp.CollectionDescPath, url.Values{"db_name": {db}, "collection_name": {collection}})
	}

	// root is the operator view on every route, whether or not the management
	// gate is enabled.
	s.Subset(listDatabases(util.UserRoot, rootPwd), []string{util.DefaultDBName, ownDB, otherDB})
	s.ElementsMatch([]string{shared, secret}, listCollections(util.UserRoot, rootPwd, otherDB))
	s.Equal(http.StatusOK, describeDatabase(util.UserRoot, rootPwd, otherDB).status)
	s.Equal(http.StatusOK, describeCollection(util.UserRoot, rootPwd, otherDB, secret).status)

	if s.adminAuth {
		// The management gate admits only root; an ordinary user is refused
		// before any route runs.
		for _, resp := range []consoleResponse{
			s.get(user, password, mhttp.DatabaseListPath, nil),
			s.get(user, password, mhttp.CollectionListPath, url.Values{"db_name": {ownDB}}),
			describeDatabase(user, password, ownDB),
			describeCollection(user, password, ownDB, shared),
		} {
			s.Equal(http.StatusForbidden, resp.status, string(resp.body))
			s.NotContains(string(resp.body), shared)
		}
		return
	}

	// With the gate disabled the console accepts ordinary users, who see only
	// the objects their grants make visible, as on the data-plane port.
	s.Equal([]string{ownDB}, listDatabases(user, password))
	s.Equal([]string{shared}, listCollections(user, password, ownDB))
	s.Empty(listCollections(user, password, otherDB))
	s.Empty(listCollections(user, password, util.DefaultDBName))
	s.Equal(http.StatusOK, describeDatabase(user, password, ownDB).status)
	s.Equal(http.StatusOK, describeCollection(user, password, ownDB, shared).status)
	for _, resp := range []consoleResponse{
		describeDatabase(user, password, otherDB),
		describeCollection(user, password, otherDB, secret),
		describeCollection(user, password, otherDB, shared),
	} {
		s.NotEqual(http.StatusOK, resp.status, string(resp.body))
		s.NotContains(string(resp.body), "dbID")
		s.NotContains(string(resp.body), "collection_id")
	}
}

func TestConsoleVisibility(t *testing.T) {
	for _, adminAuth := range []bool{false, true} {
		t.Run("adminAuth="+strconv.FormatBool(adminAuth), func(t *testing.T) {
			suite.Run(t, &consoleVisibilitySuite{adminAuth: adminAuth})
		})
	}
}

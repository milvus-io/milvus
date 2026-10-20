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
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/util/crypto"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

type importMetadataSuite struct {
	integration.MiniClusterSuite
	cached bool
}

func (s *importMetadataSuite) SetupSuite() {
	paramtable.Init()
	p := paramtable.Get()
	s.WithMilvusConfig(p.CommonCfg.AuthorizationEnabled.Key, "true")
	s.WithMilvusConfig(p.ProxyCfg.EnableCachedServiceProvider.Key, strconv.FormatBool(s.cached))
	s.WithMilvusConfig(p.DataCoordCfg.ImportCheckIntervalHigh.Key, "0.1")
	s.WithMilvusConfig(p.DataCoordCfg.ImportScheduleInterval.Key, "0.1")
	s.MiniClusterSuite.SetupSuite()
}

// Use external gRPC authentication and persisted jobs, including a failed job
// whose diagnostic must be visible only with Import on the owning collection.
func (s *importMetadataSuite) TestResourceAuthorization() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	root := GetContext(ctx, defaultAuth)
	user := metadata.AppendToOutgoingContext(GetContext(ctx, "import_reader:import_password"), "dbname", "import_a")
	client := s.Cluster.MilvusClient
	mustStatus := func(status *commonpb.Status, err error) {
		s.Require().NoError(err)
		s.Require().NoError(merr.Error(status))
	}
	jobs := make(map[string]int64)
	for _, db := range []string{"import_a", "import_b"} {
		mustStatus(client.CreateDatabase(root, &milvuspb.CreateDatabaseRequest{DbName: db}))
		schema, err := proto.Marshal(integration.ConstructSchema("records", dim, true))
		s.Require().NoError(err)
		mustStatus(client.CreateCollection(root, &milvuspb.CreateCollectionRequest{DbName: db, CollectionName: "records", Schema: schema}))
		imported, err := client.Import(root, &milvuspb.ImportRequest{
			DbName: db, CollectionName: "records", Files: []string{"missing_import_metadata.json"},
		})
		s.Require().NoError(err)
		s.Require().NoError(merr.Error(imported.GetStatus()))
		s.Require().Len(imported.GetTasks(), 1)
		jobs[db] = imported.GetTasks()[0]
	}
	mustStatus(client.CreateCredential(root, &milvuspb.CreateCredentialRequest{Username: "import_reader", Password: crypto.Base64Encode("import_password")}))
	mustStatus(client.CreateRole(root, &milvuspb.CreateRoleRequest{Entity: &milvuspb.RoleEntity{Name: "import_role"}}))
	mustStatus(client.OperateUserRole(root, &milvuspb.OperateUserRoleRequest{Username: "import_reader", RoleName: "import_role", Type: milvuspb.OperateUserRoleType_AddUserToRole}))
	grant := func(db string, operation milvuspb.OperatePrivilegeType) {
		mustStatus(client.OperatePrivilegeV2(root, &milvuspb.OperatePrivilegeV2Request{
			Role:    &milvuspb.RoleEntity{Name: "import_role"},
			Grantor: &milvuspb.GrantorEntity{User: &milvuspb.UserEntity{Name: "root"}, Privilege: &milvuspb.PrivilegeEntity{Name: "Import"}},
			Type:    operation, DbName: db, CollectionName: "records",
		}))
	}
	checkProgress := func(job int64, allowed bool) {
		resp, err := client.GetImportState(user, &milvuspb.GetImportStateRequest{Task: job})
		s.Require().NoError(err)
		if allowed {
			s.Require().NoError(merr.Error(resp.GetStatus()))
		} else {
			s.Require().ErrorIs(merr.Error(resp.GetStatus()), merr.ErrPrivilegeNotPermitted)
			s.Empty(resp.GetInfos())
			s.Zero(resp.GetRowCount())
		}
	}
	checkList := func(db string, count int) {
		resp, err := client.ListImportTasks(user, &milvuspb.ListImportTasksRequest{DbName: db})
		s.Require().NoError(err)
		s.Require().NoError(merr.Error(resp.GetStatus()))
		s.Len(resp.GetTasks(), count)
	}
	checkProgress(jobs["import_b"], false)
	checkList("import_a", 0)
	grant("import_a", milvuspb.OperatePrivilegeType_Grant)
	checkProgress(jobs["import_a"], true)
	checkProgress(jobs["import_b"], false)
	checkProgress(123456789, false)
	checkList("import_a", 1)
	checkList("import_b", 0)
	// The actual resource grant works even though outgoing metadata selects A.
	grant("import_b", milvuspb.OperatePrivilegeType_Grant)
	checkProgress(jobs["import_b"], true)
	s.Require().Eventually(func() bool {
		resp, err := client.GetImportState(user, &milvuspb.GetImportStateRequest{Task: jobs["import_b"]})
		return err == nil && merr.Ok(resp.GetStatus()) && resp.GetState() == commonpb.ImportState_ImportFailed && len(resp.GetInfos()) > 0
	}, time.Minute, 100*time.Millisecond)
	grant("import_b", milvuspb.OperatePrivilegeType_Revoke)
	checkProgress(jobs["import_b"], false)
	checkList("import_b", 0)

	// A newly created collection with the same name cannot own the old job.
	mustStatus(client.DropCollection(root, &milvuspb.DropCollectionRequest{DbName: "import_b", CollectionName: "records"}))
	schema, err := proto.Marshal(integration.ConstructSchema("records", dim, true))
	s.Require().NoError(err)
	mustStatus(client.CreateCollection(root, &milvuspb.CreateCollectionRequest{DbName: "import_b", CollectionName: "records", Schema: schema}))
	grant("import_b", milvuspb.OperatePrivilegeType_Grant)
	checkProgress(jobs["import_b"], false)
	checkList("import_b", 0)
}

func TestImportMetadataAuthorization(t *testing.T) {
	for _, cached := range []bool{true, false} {
		t.Run("cached="+strconv.FormatBool(cached), func(t *testing.T) {
			suite.Run(t, &importMetadataSuite{cached: cached})
		})
	}
}

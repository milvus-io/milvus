// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package rbac

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/crypto"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/tests/integration"
)

// TestGrantMigrationOnUpgrade verifies that name-based grants are migrated to
// ID-based grants when rootcoord restarts after an upgrade. It simulates the
// pre-upgrade state by directly removing ID-based grant keys from etcd, then
// restarts the coordinator to trigger the migration, and finally verifies that
// RBAC enforcement still works.
func (s *RBACBasicTestSuite) TestGrantMigrationOnUpgrade() {
	rootCtx := GetContext(context.Background(), defaultAuth)

	colName := "migration_e2e_col"
	roleName := "migration_e2e_role"
	userName := "migration_e2e_user"
	userPassword := "migration_e2e_passwd"

	// ---- Setup: collection, role, user, grant ----

	// Wait for cluster to be fully ready (streaming service needs time)
	time.Sleep(5 * time.Second)

	schema := integration.ConstructSchema(colName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.Require().NoError(err)

	// CreateCollection may need retries due to streaming WAL ack delays
	var createResp *commonpb.Status
	for i := 0; i < 3; i++ {
		createResp, err = s.Cluster.MilvusClient.CreateCollection(rootCtx, &milvuspb.CreateCollectionRequest{
			CollectionName: colName,
			Schema:         marshaledSchema,
		})
		if err == nil && merr.Ok(createResp) {
			break
		}
		log.Info("CreateCollection retry", zap.Int("attempt", i+1), zap.Error(err))
		time.Sleep(5 * time.Second)
	}
	s.Require().NoError(err)
	s.Require().True(merr.Ok(createResp))

	resp, err := s.Cluster.MilvusClient.CreateRole(rootCtx, &milvuspb.CreateRoleRequest{
		Entity: &milvuspb.RoleEntity{Name: roleName},
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(resp))

	resp, err = s.Cluster.MilvusClient.CreateCredential(rootCtx, &milvuspb.CreateCredentialRequest{
		Username: userName,
		Password: crypto.Base64Encode(userPassword),
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(resp))

	resp, err = s.Cluster.MilvusClient.OperateUserRole(rootCtx, &milvuspb.OperateUserRoleRequest{
		Username: userName,
		RoleName: roleName,
		Type:     milvuspb.OperateUserRoleType_AddUserToRole,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(resp))

	defer func() {
		s.Cluster.MilvusClient.DropCollection(rootCtx, &milvuspb.DropCollectionRequest{CollectionName: colName}) //nolint
		s.Cluster.MilvusClient.DropRole(rootCtx, &milvuspb.DropRoleRequest{RoleName: roleName, ForceDrop: true}) //nolint
		s.Cluster.MilvusClient.DeleteCredential(rootCtx, &milvuspb.DeleteCredentialRequest{Username: userName})  //nolint
	}()

	// Grant GetStatistics on the collection (dual-write: both name-based and ID-based keys)
	grantResp, err := s.Cluster.MilvusClient.OperatePrivilegeV2(rootCtx, &milvuspb.OperatePrivilegeV2Request{
		Role: &milvuspb.RoleEntity{Name: roleName},
		Grantor: &milvuspb.GrantorEntity{
			User:      &milvuspb.UserEntity{Name: util.UserRoot},
			Privilege: &milvuspb.PrivilegeEntity{Name: "GetStatistics"},
		},
		Type:           milvuspb.OperatePrivilegeType_Grant,
		DbName:         util.AnyWord,
		CollectionName: colName,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(grantResp), "OperatePrivilegeV2 failed: %v", grantResp)

	// Wait for privilege cache to refresh (Casbin cache interval = 10s)
	time.Sleep(10 * time.Second)

	// ---- Pre-migration: verify enforcement works ----

	userCtx := GetContext(context.Background(), fmt.Sprintf("%s:%s", userName, userPassword))

	statsResp, err := s.Cluster.MilvusClient.GetCollectionStatistics(userCtx, &milvuspb.GetCollectionStatisticsRequest{
		CollectionName: colName,
	})
	s.Require().NoError(err, "GetCollectionStatistics should succeed before migration")
	s.Require().True(merr.Ok(statsResp.GetStatus()), "user should have GetStatistics permission before migration")

	// ---- Simulate pre-upgrade state: delete ID-based keys from etcd ----

	metaRoot := path.Join(s.Cluster.RootPath(), "meta")
	granteePrefix := metaRoot + "/root-coord/credential/grantee-privileges/"
	granteeIDPrefix := metaRoot + "/root-coord/credential/grantee-id/"

	etcdCli := s.Cluster.EtcdCli
	etcdCtx := context.Background()

	// List all grantee-privileges keys
	getResp, err := etcdCli.Get(etcdCtx, granteePrefix, clientv3.WithPrefix())
	s.Require().NoError(err)

	for _, kv := range getResp.Kvs {
		log.Info("etcd grantee key", zap.String("key", string(kv.Key)), zap.String("value", string(kv.Value)))
	}

	// Delete ID-based grantee-privileges keys and their grantee-id sub-keys
	deletedPointers := 0
	deletedSubKeys := 0
	for _, kv := range getResp.Kvs {
		key := string(kv.Key)
		// ID-based keys contain "dbID:" or "colID:" patterns
		if strings.Contains(key, "dbID:") || strings.Contains(key, "colID:") {
			// The value is the grantee ID — delete its sub-keys too
			granteeID := string(kv.Value)
			subKeyPrefix := granteeIDPrefix + granteeID + "/"
			delResp, err := etcdCli.Delete(etcdCtx, subKeyPrefix, clientv3.WithPrefix())
			s.Require().NoError(err)
			deletedSubKeys += int(delResp.Deleted)

			// Delete the pointer key itself
			_, err = etcdCli.Delete(etcdCtx, key)
			s.Require().NoError(err)
			deletedPointers++
			log.Info("deleted ID-based grant key", zap.String("key", key), zap.String("granteeID", granteeID))
		}
	}
	s.Require().Greater(deletedPointers, 0, "should have found and deleted ID-based grant keys")
	log.Info("cleaned up ID-based keys", zap.Int("pointers", deletedPointers), zap.Int("subKeys", deletedSubKeys))

	// Delete the migration flag so rootcoord re-runs migration on restart
	migrationFlagKey := metaRoot + "/root-coord/credential/grant-migration-to-id-done"
	_, err = etcdCli.Delete(etcdCtx, migrationFlagKey)
	s.Require().NoError(err)
	log.Info("deleted migration flag", zap.String("key", migrationFlagKey))

	// Verify: ID-based keys are gone, name-based keys remain
	getResp, err = etcdCli.Get(etcdCtx, granteePrefix, clientv3.WithPrefix())
	s.Require().NoError(err)
	for _, kv := range getResp.Kvs {
		key := string(kv.Key)
		s.Require().False(strings.Contains(key, "dbID:") || strings.Contains(key, "colID:"),
			"ID-based keys should have been deleted: %s", key)
	}
	s.Require().NotEmpty(getResp.Kvs, "name-based keys should still exist")

	// ---- Restart rootcoord to trigger migration ----

	log.Info("stopping MixCoord to trigger migration on restart...")
	s.Cluster.DefaultMixCoord().Stop()

	log.Info("starting new MixCoord...")
	s.Cluster.AddMixCoord()
	log.Info("MixCoord restarted, waiting for cluster to stabilize...")

	// Wait for cluster to be ready and privilege cache to refresh
	time.Sleep(15 * time.Second)

	// ---- Post-migration: verify ID-based keys were recreated ----

	getResp, err = etcdCli.Get(etcdCtx, granteePrefix, clientv3.WithPrefix())
	s.Require().NoError(err)

	hasIDBased := false
	hasNameBased := false
	for _, kv := range getResp.Kvs {
		key := string(kv.Key)
		log.Info("post-migration etcd key", zap.String("key", key))
		if strings.Contains(key, "dbID:") || strings.Contains(key, "colID:") {
			hasIDBased = true
		} else if strings.Contains(key, "/Collection/") {
			hasNameBased = true
		}
	}
	s.True(hasIDBased, "migration should have recreated ID-based grant keys")
	s.True(hasNameBased, "name-based grant keys should still exist")

	// Verify the migration flag was set
	flagResp, err := etcdCli.Get(etcdCtx, migrationFlagKey)
	s.Require().NoError(err)
	s.True(len(flagResp.Kvs) > 0, "migration flag should be set after migration")

	// ---- Post-migration: verify RBAC enforcement still works ----

	statsResp, err = s.Cluster.MilvusClient.GetCollectionStatistics(userCtx, &milvuspb.GetCollectionStatisticsRequest{
		CollectionName: colName,
	})
	s.NoError(err, "GetCollectionStatistics should succeed after migration")
	s.True(merr.Ok(statsResp.GetStatus()), "user should still have GetStatistics permission after migration")

	// Verify that unprivileged operations are still denied
	_, err = s.Cluster.MilvusClient.Insert(userCtx, &milvuspb.InsertRequest{
		CollectionName: colName,
	})
	s.Error(err, "user should NOT have Insert permission")
}

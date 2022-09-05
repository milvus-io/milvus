package proxy

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/util/funcutil"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/stretchr/testify/assert"
)

func TestUnaryServerInterceptor(t *testing.T) {
	interceptor := UnaryServerInterceptor(PrivilegeInterceptor)
	assert.NotNil(t, interceptor)
}

func TestPrivilegeInterceptor(t *testing.T) {
	ctx := context.Background()
	t.Run("Authorization Disabled", func(t *testing.T) {
		Params.CommonCfg.AuthorizationEnabled = false
		_, err := PrivilegeInterceptor(ctx, &milvuspb.LoadCollectionRequest{
			DbName:         "db_test",
			CollectionName: "col1",
		})
		assert.Nil(t, err)
	})

	t.Run("Authorization Enabled", func(t *testing.T) {
		Params.CommonCfg.AuthorizationEnabled = true

		_, err := PrivilegeInterceptor(ctx, &milvuspb.HasCollectionRequest{})
		assert.Nil(t, err)

		_, err = PrivilegeInterceptor(ctx, &milvuspb.LoadCollectionRequest{
			DbName:         "db_test",
			CollectionName: "col1",
		})
		assert.NotNil(t, err)

		ctx = GetContext(context.Background(), "alice:123456")
		client := &MockRootCoordClientInterface{}
		queryCoord := &MockQueryCoordClientInterface{}
		mgr := newShardClientMgr()

		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				PolicyInfos: []string{
					funcutil.PolicyForPrivilege("role1", commonpb.ObjectType_Collection.String(), "col1", commonpb.ObjectPrivilege_PrivilegeLoad.String()),
					funcutil.PolicyForPrivilege("role1", commonpb.ObjectType_Collection.String(), "col1", commonpb.ObjectPrivilege_PrivilegeFlush.String()),
					funcutil.PolicyForPrivilege("role2", commonpb.ObjectType_Global.String(), "*", commonpb.ObjectPrivilege_PrivilegeAll.String()),
				},
				UserRoles: []string{
					funcutil.EncodeUserRoleCache("alice", "role1"),
					funcutil.EncodeUserRoleCache("fooo", "role2"),
				},
			}, nil
		}

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "foo:123456"), &milvuspb.LoadCollectionRequest{
			DbName:         "db_test",
			CollectionName: "col1",
		})
		assert.NotNil(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "root:123456"), &milvuspb.LoadCollectionRequest{
			DbName:         "db_test",
			CollectionName: "col1",
		})
		assert.Nil(t, err)

		err = InitMetaCache(ctx, client, queryCoord, mgr)
		assert.Nil(t, err)
		_, err = PrivilegeInterceptor(ctx, &milvuspb.HasCollectionRequest{
			DbName:         "db_test",
			CollectionName: "col1",
		})
		assert.Nil(t, err)
		_, err = PrivilegeInterceptor(ctx, &milvuspb.LoadCollectionRequest{
			DbName:         "db_test",
			CollectionName: "col1",
		})
		assert.Nil(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "foo:123456"), &milvuspb.LoadCollectionRequest{
			DbName:         "db_test",
			CollectionName: "col1",
		})
		assert.NotNil(t, err)
		_, err = PrivilegeInterceptor(ctx, &milvuspb.InsertRequest{
			DbName:         "db_test",
			CollectionName: "col1",
		})
		assert.NotNil(t, err)

		_, err = PrivilegeInterceptor(ctx, &milvuspb.FlushRequest{
			DbName:          "db_test",
			CollectionNames: []string{"col1"},
		})
		assert.Nil(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.LoadCollectionRequest{
			DbName:         "db_test",
			CollectionName: "col1",
		})
		assert.Nil(t, err)
	})

}

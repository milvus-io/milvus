package proxy

import (
	"context"
	"fmt"
	"testing"

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
		_, err := PrivilegeInterceptor(ctx, &milvuspb.HasCollectionRequest{
			DbName:         "db_test",
			CollectionName: "col1",
		})
		assert.Nil(t, err)
	})

	t.Run("Authorization Enabled", func(t *testing.T) {
		Params.CommonCfg.AuthorizationEnabled = true

		_, err := PrivilegeInterceptor(ctx, &milvuspb.ListCredUsersRequest{})
		assert.Nil(t, err)

		_, err = PrivilegeInterceptor(ctx, &milvuspb.HasCollectionRequest{
			DbName:         "db_test",
			CollectionName: "col1",
		})
		assert.NotNil(t, err)

		ctx = GetContext(context.Background(), "alice:123456")
		client := &MockRootCoordClientInterface{}
		queryCoord := &MockQueryCoordClientInterface{}
		mgr := newShardClientMgr()

		client.policyList = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				PolicyInfos: []string{
					fmt.Sprintf(`{"PType":"p","V0":"alice","V1":"%s-col1","V2":"%s"}`, commonpb.ResourceType_Collection, commonpb.ResourcePrivilege_PrivilegeRead),
				},
			}, nil
		}
		err = InitMetaCache(ctx, client, queryCoord, mgr)
		assert.Nil(t, err)
		_, err = PrivilegeInterceptor(ctx, &milvuspb.HasCollectionRequest{
			DbName:         "db_test",
			CollectionName: "col1",
		})
		assert.Nil(t, err)

		client.policyList = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				PolicyInfos: []string{
					fmt.Sprintf(`{"PType":"p","V0":"alice","V1":"%s-col1","V2":"%s"}`, commonpb.ResourceType_Collection, commonpb.ResourcePrivilege_PrivilegeLoad),
				},
			}, nil
		}
		err = InitMetaCache(ctx, client, queryCoord, mgr)
		assert.Nil(t, err)
		_, err = PrivilegeInterceptor(ctx, &milvuspb.HasCollectionRequest{
			DbName:         "db_test",
			CollectionName: "col1",
		})
		assert.NotNil(t, err)

	})

}

//go:build rbac

package advcases

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

func TestFileResourceRBAC(t *testing.T) {
	ctx := hp.CreateContext(t, 2*time.Minute)
	rootClient := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{
		Address:  hp.GetAddr(),
		Username: hp.GetUser(),
		Password: hp.GetPassword(),
	})

	userName := common.GenRandomString("file_resource_user", 6)
	roleName := common.GenRandomString("file_resource_role", 6)
	password := common.GenRandomString("pwd", 8)
	require.NoError(t, rootClient.CreateUser(ctx, client.NewCreateUserOption(userName, password)))
	require.NoError(t, rootClient.CreateRole(ctx, client.NewCreateRoleOption(roleName)))
	require.NoError(t, rootClient.GrantRole(ctx, client.NewGrantRoleOption(userName, roleName)))

	grantedPrivileges := make([]string, 0, 3)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, privilege := range grantedPrivileges {
			_ = rootClient.RevokePrivilegeV2(cleanupCtx,
				client.NewRevokePrivilegeV2Option(roleName, privilege, AllObjectName).WithDbName(AllObjectName))
		}
		_ = rootClient.RevokeRole(cleanupCtx, client.NewRevokeRoleOption(userName, roleName))
		_ = rootClient.DropUser(cleanupCtx, client.NewDropUserOption(userName))
		_ = rootClient.DropRole(cleanupCtx, client.NewDropRoleOption(roleName))
	})

	userClient := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{
		Address:  hp.GetAddr(),
		Username: userName,
		Password: password,
	})
	resourceName := common.GenRandomString("file_resource_rbac", 6)
	missingPath := "file-resource-rbac/not-exist.txt"

	// The three cluster privileges are independent. Without explicit grants,
	// authorization must reject each operation before its business validation.
	err := userClient.AddFileResource(ctx, client.NewAddFileResourceOption(resourceName, missingPath))
	require.ErrorContains(t, err, "permission deny")
	err = userClient.RemoveFileResource(ctx, client.NewRemoveFileResourceOption(resourceName))
	require.ErrorContains(t, err, "permission deny")
	_, err = userClient.ListFileResources(ctx, client.NewListFileResourcesOption())
	require.ErrorContains(t, err, "permission deny")

	for _, privilege := range []string{"ListFileResources", "AddFileResource", "RemoveFileResource"} {
		require.NoError(t, rootClient.GrantPrivilegeV2(ctx,
			client.NewGrantPrivilegeV2Option(roleName, privilege, AllObjectName).WithDbName(AllObjectName)))
		grantedPrivileges = append(grantedPrivileges, privilege)
	}

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		_, listErr := userClient.ListFileResources(ctx, client.NewListFileResourcesOption())
		require.NoError(collect, listErr)

		// Reaching path validation proves AddFileResource authorization passed.
		addErr := userClient.AddFileResource(ctx, client.NewAddFileResourceOption(resourceName, missingPath))
		require.ErrorContains(collect, addErr, "path not exist")

		// Removing a missing name is idempotent once RemoveFileResource is granted.
		require.NoError(collect,
			userClient.RemoveFileResource(ctx, client.NewRemoveFileResourceOption(resourceName)))
	}, 20*time.Second, time.Second)
}

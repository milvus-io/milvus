//go:build rbac

package advcases

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

const (
	CollectionObjectType = "Collection"
	GlobalObjectType     = "Global"
	AllObjectName        = "*"
)

func resetRbac(t *testing.T, ctx context.Context, mc *base.MilvusClient) {
	t.Helper()
	userNames, _ := mc.ListUsers(ctx, client.NewListUserOption())
	for _, userName := range userNames {
		if userName != common.RootUser {
			err := mc.DropUser(ctx, client.NewDropUserOption(userName))
			common.CheckErr(t, err, true)
		}
	}
	roleNames, _ := mc.ListRoles(ctx, client.NewListRoleOption())
	for _, roleName := range roleNames {
		if lo.Contains([]string{common.AdminRole, common.PublicRole}, roleName) {
			continue
		}
		role, err := mc.DescribeRole(ctx, client.NewDescribeRoleOption(roleName).WithDbName("*"))
		common.CheckErr(t, err, true)
		if role.Privileges != nil {
			for _, grantItem := range role.Privileges {
				err := mc.RevokePrivilegeV2(ctx, client.NewRevokePrivilegeV2Option(roleName, grantItem.Privilege, grantItem.ObjectName).WithDbName(grantItem.DbName))
				common.CheckErr(t, err, true)
			}
		}
		err = mc.DropRole(ctx, client.NewDropRoleOption(roleName))
		common.CheckErr(t, err, true)
	}
}

func setupTest(t *testing.T, ctx context.Context, mc *base.MilvusClient) {
	resetRbac(t, ctx, mc)
	t.Cleanup(func() {
		resetRbac(t, ctx, mc)
	})
}

func TestRbacDefault(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{Address: hp.GetAddr(), Username: hp.GetUser(), Password: hp.GetPassword()})
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	setupTest(t, ctx, mc)

	// create user & list user
	userName := common.GenRandomString("user", 6)
	pwd := common.GenRandomString("pwd", 6)
	log.Info(t.Name(), zap.String("pwd", pwd))
	err := mc.CreateUser(ctx, client.NewCreateUserOption(userName, pwd))
	common.CheckErr(t, err, true)
	users, err := mc.ListUsers(ctx, client.NewListUserOption())
	common.CheckErr(t, err, true)
	require.Contains(t, users, userName)

	// create role and list role
	roleName := common.GenRandomString("role", 6)
	errRole := mc.CreateRole(ctx, client.NewCreateRoleOption(roleName))
	common.CheckErr(t, errRole, true)
	roles, _ := mc.ListRoles(ctx, client.NewListRoleOption())
	require.Contains(t, roles, roleName)

	// grant role to a user
	errGrant := mc.GrantRole(ctx, client.NewGrantRoleOption(userName, roleName))
	common.CheckErr(t, errGrant, true)

	// create index not permission deny
	mcUser := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{Address: hp.GetAddr(), Username: userName, Password: pwd})
	_, errIndex := mcUser.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName, index.NewAutoIndex(entity.COSINE)))
	log.Info("TestRbacDefault", zap.Error(errIndex))
	common.CheckErr(t, errIndex, false, fmt.Sprintf("permission deny to %s in the `default` database", userName))

	// grant privilege to role
	errGrant = mc.GrantPrivilege(ctx, client.NewGrantPrivilegeOption(roleName, CollectionObjectType, "CreateIndex", AllObjectName))
	common.CheckErr(t, errGrant, true)
	log.Info("TestRbacDefault", zap.Error(errGrant))

	// describe role
	role, _ := mc.DescribeRole(ctx, client.NewDescribeRoleOption(roleName))
	require.Equal(t, roleName, role.RoleName)
	expPrivilege := entity.GrantItem{
		Object:     CollectionObjectType,
		ObjectName: AllObjectName,
		RoleName:   roleName,
		Grantor:    hp.GetUser(),
		Privilege:  "CreateIndex",
		DbName:     common.DefaultDb,
	}
	require.ElementsMatch(t, []entity.GrantItem{expPrivilege}, role.Privileges)

	// describe user
	user, _ := mc.DescribeUser(ctx, client.NewDescribeUserOption(userName))
	common.CheckErr(t, err, true)
	require.Equal(t, &entity.User{UserName: userName, Roles: []string{roleName}}, user)

	// check privilege effect
	require.Eventuallyf(t, func() bool {
		_, errIndex = mcUser.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName, index.NewAutoIndex(entity.COSINE)))
		return errIndex == nil
	}, time.Second*10, 2*time.Second, "Waiting for permission to take effect timed out")
	_, err = mcUser.LoadCollection(ctx, client.NewLoadCollectionOption(schema.CollectionName))
	common.CheckErr(t, err, false, fmt.Sprintf("permission deny to %s in the `default` database", userName))

	// drop user, role, privilege
	errDrop := mc.DropUser(ctx, client.NewDropUserOption(userName))
	common.CheckErr(t, errDrop, true)
	errRevoke := mc.RevokePrivilege(ctx, client.NewRevokePrivilegeOption(roleName, CollectionObjectType, "CreateIndex", AllObjectName))
	common.CheckErr(t, errRevoke, true)
	errDrop = mc.DropRole(ctx, client.NewDropRoleOption(roleName))
	common.CheckErr(t, errDrop, true)
}

func TestRbacDefaultV2(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{Address: hp.GetAddr(), Username: hp.GetUser(), Password: hp.GetPassword()})
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	setupTest(t, ctx, mc)

	// create user & list user
	userName := common.GenRandomString("user", 6)
	pwd := common.GenRandomString("pwd", 6)
	err := mc.CreateUser(ctx, client.NewCreateUserOption(userName, pwd))
	common.CheckErr(t, err, true)
	users, err := mc.ListUsers(ctx, client.NewListUserOption())
	common.CheckErr(t, err, true)
	require.Contains(t, users, userName)

	// create role and list role
	roleName := common.GenRandomString("role", 6)
	errRole := mc.CreateRole(ctx, client.NewCreateRoleOption(roleName))
	common.CheckErr(t, errRole, true)
	roles, _ := mc.ListRoles(ctx, client.NewListRoleOption())
	require.Contains(t, roles, roleName)

	// grant role to a user
	errGrant := mc.GrantRole(ctx, client.NewGrantRoleOption(userName, roleName))
	common.CheckErr(t, errGrant, true)

	// describe collection but permission deny
	mcUser := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{Address: hp.GetAddr(), Username: userName, Password: pwd})
	_, errFlush := mcUser.Flush(ctx, client.NewFlushOption(schema.CollectionName))
	log.Info("TestRbacDefault", zap.Error(errFlush))
	common.CheckErr(t, errFlush, false, fmt.Sprintf("permission deny to %s in the `default` database", userName))

	// grant privilege to role
	errGrant = mc.GrantPrivilegeV2(ctx, client.NewGrantPrivilegeV2Option(roleName, "CollectionAdmin", AllObjectName))
	common.CheckErr(t, errGrant, true)

	// describe role
	role, _ := mc.DescribeRole(ctx, client.NewDescribeRoleOption(roleName))
	require.Equal(t, roleName, role.RoleName)
	expPrivilege := entity.GrantItem{
		Object:     GlobalObjectType,
		ObjectName: AllObjectName,
		RoleName:   roleName,
		Grantor:    hp.GetUser(),
		Privilege:  "CollectionAdmin",
		DbName:     common.DefaultDb,
	}
	require.ElementsMatch(t, []entity.GrantItem{expPrivilege}, role.Privileges)

	// describe user
	user, _ := mc.DescribeUser(ctx, client.NewDescribeUserOption(userName))
	common.CheckErr(t, err, true)
	require.Equal(t, userName, user.UserName)
	require.ElementsMatch(t, []string{roleName}, user.Roles)

	// check privilege effect
	require.Eventuallyf(t, func() bool {
		_, errFlush = mcUser.Flush(ctx, client.NewFlushOption(schema.CollectionName))
		return errFlush == nil
	}, time.Second*10, 2*time.Second, "Waiting for permission to take effect timed out")
	errDb := mcUser.CreateDatabase(ctx, client.NewCreateDatabaseOption("db1"))
	common.CheckErr(t, errDb, false, fmt.Sprintf("permission deny to %s in the `default` database", userName))

	// drop user, role, privilege
	errDrop := mc.DropUser(ctx, client.NewDropUserOption(userName))
	common.CheckErr(t, errDrop, true)
	errRevoke := mc.RevokePrivilegeV2(ctx, client.NewRevokePrivilegeV2Option(roleName, "CollectionAdmin", "*"))
	common.CheckErr(t, errRevoke, true)
	errDrop = mc.DropRole(ctx, client.NewDropRoleOption(roleName))
	common.CheckErr(t, errDrop, true)
}

func TestCreateInvalidUser(t *testing.T) {
	// root user & username must contain only numbers, letters and underscores
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{Address: hp.GetAddr(), Username: hp.GetUser(), Password: hp.GetPassword()})
	setupTest(t, ctx, mc)

	invalidUserNames := common.GenInvalidNames()
	invalidUserNames = append(invalidUserNames, common.RootUser)
	// create user & list user
	for _, invalidName := range invalidUserNames {
		log.Info("name", zap.String("name", invalidName))
		err := mc.CreateUser(ctx, client.NewCreateUserOption(invalidName, "ccccccc"))
		common.CheckErr(t, err, false,
			"username must contain only numbers, letters and underscores",
			"username must be not empty", "user already exists",
			"the first character must be a letter",
			"the length of username must be less than %!d(string=32)")
	}

	// user exists
	err := mc.CreateUser(ctx, client.NewCreateUserOption("user1", common.GenRandomString("p", 6)))
	common.CheckErr(t, err, true)
	err = mc.CreateUser(ctx, client.NewCreateUserOption("user1", common.GenRandomString("p", 6)))
	common.CheckErr(t, err, false, "user already exists")

	// invalid password: range 6 <= value <= 256
	for _, invalidPwd := range []string{
		common.GenRandomString("p", 3),
		common.GenRandomString("p", 72),
	} {
		err := mc.CreateUser(ctx, client.NewCreateUserOption("aaa", invalidPwd))
		common.CheckErr(t, err, false, "out of range 6 <= value <= 72")
	}
}

func TestCreateUserLimit(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{Address: hp.GetAddr(), Username: hp.GetUser(), Password: hp.GetPassword()})
	setupTest(t, ctx, mc)

	// Limit to 100 users, including root
	userNumLimit := 100
	for i := 0; i < userNumLimit-1; i++ {
		err := mc.CreateUser(ctx, client.NewCreateUserOption(common.GenRandomString("user", 4), common.GenRandomString("p", 6)))
		common.CheckErr(t, err, true)
	}

	err := mc.CreateUser(ctx, client.NewCreateUserOption(common.GenRandomString("user", 4), common.GenRandomString("p", 6)))
	common.CheckErr(t, err, false, "unable to add user because the number of users has reached the limit")

	users, errList := mc.ListUsers(ctx, client.NewListUserOption())
	common.CheckErr(t, errList, true)
	require.Equal(t, userNumLimit, len(users))
}

func TestUpdatePassword(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{Address: hp.GetAddr(), Username: hp.GetUser(), Password: hp.GetPassword()})
	setupTest(t, ctx, mc)

	userName := common.GenRandomString("user", 6)
	pwd := common.GenRandomString("pwd", 6)
	newPwd := common.GenRandomString("pwd", 6)
	err := mc.CreateUser(ctx, client.NewCreateUserOption(userName, pwd))
	common.CheckErr(t, err, true)

	err = mc.UpdatePassword(ctx, client.NewUpdatePasswordOption(userName, pwd, newPwd))
	common.CheckErr(t, err, true)

	mcUser := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{Address: hp.GetAddr(), Username: userName, Password: newPwd})
	_, err = mcUser.ListCollections(ctx, client.NewListCollectionOption())
	common.CheckErr(t, err, true)

	// update multi times with multi language
	oldPwd := newPwd
	passwords := []string{"中文", "シャオミン", "샤오밍", "ಸೂರ್ಯ", "myPwd@aa.com", "φεγγάρι", "mặt trăng"}
	for i := 0; i < len(passwords); i++ {
		log.Info(t.Name(), zap.String("pwd", passwords[i]))
		err = mc.UpdatePassword(ctx, client.NewUpdatePasswordOption(userName, oldPwd, passwords[i]))
		common.CheckErr(t, err, true)
		oldPwd = passwords[i]
	}

	mcNewClient := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{Address: hp.GetAddr(), Username: userName, Password: passwords[len(passwords)-1]})
	_, err = mcNewClient.ListCollections(ctx, client.NewListCollectionOption())
	common.CheckErr(t, err, true)
}

func TestUpdatePasswordInvalid(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{Address: hp.GetAddr(), Username: hp.GetUser(), Password: hp.GetPassword()})
	setupTest(t, ctx, mc)

	userName := common.GenRandomString("user", 6)
	pwd := common.GenRandomString("pwd", 6)
	newPwd := common.GenRandomString("pwd", 6)
	err := mc.CreateUser(ctx, client.NewCreateUserOption(userName, pwd))
	common.CheckErr(t, err, true)

	for _, invalidPwd := range []string{"", "_", "(mn)", "1   ", "]]", "*&%@"} {
		err := mc.UpdatePassword(ctx, client.NewUpdatePasswordOption(userName, pwd, invalidPwd))
		common.CheckErr(t, err, false,
			"out of range 6 <= value <= 72")
	}
	// not existed user
	err = mc.UpdatePassword(ctx, client.NewUpdatePasswordOption(common.GenRandomString("user", 6), pwd, newPwd))
	common.CheckErr(t, err, false, "old password not correct for")

	// wrong old pwd
	err = mc.UpdatePassword(ctx, client.NewUpdatePasswordOption(userName, newPwd, newPwd))
	common.CheckErr(t, err, false, "old password not correct for")

	// new pwd = old pwd
	err = mc.UpdatePassword(ctx, client.NewUpdatePasswordOption(userName, pwd, pwd))
	common.CheckErr(t, err, true)
}

func TestDropUser(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{Address: hp.GetAddr(), Username: hp.GetUser(), Password: hp.GetPassword()})
	setupTest(t, ctx, mc)

	userName := common.GenRandomString("user", 6)
	pwd := common.GenRandomString("pwd", 6)
	roleName := common.GenRandomString("role", 6)
	err := mc.CreateUser(ctx, client.NewCreateUserOption(userName, pwd))
	common.CheckErr(t, err, true)
	err = mc.CreateRole(ctx, client.NewCreateRoleOption(roleName))
	common.CheckErr(t, err, true)
	err = mc.GrantRole(ctx, client.NewGrantRoleOption(userName, roleName))
	common.CheckErr(t, err, true)
	err = mc.GrantPrivilegeV2(ctx, client.NewGrantPrivilegeV2Option(roleName, "CollectionAdmin", AllObjectName))
	common.CheckErr(t, err, true)

	// drop user that bind with role
	err = mc.DropUser(ctx, client.NewDropUserOption(userName))
	common.CheckErr(t, err, true)

	// drop not existed user
	err = mc.DropUser(ctx, client.NewDropUserOption(userName))
	common.CheckErr(t, err, true)

	// delete root user
	err = mc.DropUser(ctx, client.NewDropUserOption(common.RootUser))
	common.CheckErr(t, err, false, "root user cannot be deleted")
}

func TestCreateRoleLimit(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{Address: hp.GetAddr(), Username: hp.GetUser(), Password: hp.GetPassword()})
	setupTest(t, ctx, mc)

	// Limit to 10 roles, including admin & public
	roleNumLimit := 10
	for i := 0; i < roleNumLimit-2; i++ {
		err := mc.CreateRole(ctx, client.NewCreateRoleOption(common.GenRandomString("role", 4)))
		common.CheckErr(t, err, true)
	}
	err := mc.CreateRole(ctx, client.NewCreateRoleOption(common.GenRandomString("role", 4)))
	common.CheckErr(t, err, false, "unable to create role because the number of roles has reached the limit")

	roles, errList := mc.ListRoles(ctx, client.NewListRoleOption())
	common.CheckErr(t, errList, true)
	require.Equal(t, roleNumLimit, len(roles))
}

func TestCreateInvalidRole(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{Address: hp.GetAddr(), Username: hp.GetUser(), Password: hp.GetPassword()})
	setupTest(t, ctx, mc)

	invalidNames := common.GenInvalidNames()

	// create user & list user
	for _, invalidName := range invalidNames {
		log.Info("name", zap.String("name", invalidName))
		err := mc.CreateRole(ctx, client.NewCreateRoleOption(invalidName))
		common.CheckErr(t, err, false,
			"role name can only contain numbers, letters, dollars and underscores",
			"role name should be not empty",
			"the first character of role name must be an underscore or letter",
			"the length of role name must be not greater than limit")
	}

	// role exists
	roleName := common.GenRandomString("role", 4)
	err := mc.CreateRole(ctx, client.NewCreateRoleOption(roleName))
	common.CheckErr(t, err, true)
	err = mc.CreateRole(ctx, client.NewCreateRoleOption(roleName))
	common.CheckErr(t, err, false, "already exists")

	// create admin or public role
	for _, roleName := range []string{common.AdminRole, common.PublicRole} {
		err := mc.CreateRole(ctx, client.NewCreateRoleOption(roleName))
		common.CheckErr(t, err, false, "already exists")
	}
}

func TestDropRoleBindUser(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{Address: hp.GetAddr(), Username: hp.GetUser(), Password: hp.GetPassword()})
	setupTest(t, ctx, mc)

	userName := common.GenRandomString("user", 6)
	err := mc.CreateUser(ctx, client.NewCreateUserOption(userName, common.GenRandomString("pwd", 6)))
	common.CheckErr(t, err, true)

	roleName := common.GenRandomString("role", 6)
	err = mc.CreateRole(ctx, client.NewCreateRoleOption(roleName))
	common.CheckErr(t, err, true)

	err = mc.DropRole(ctx, client.NewDropRoleOption(roleName))
	common.CheckErr(t, err, true)

	// drop not existed role
	err = mc.DropRole(ctx, client.NewDropRoleOption(roleName))
	common.CheckErr(t, err, false, "not found the role, maybe the role isn't existed or internal system error")

	// drop admin or public role
	for _, roleName := range []string{common.AdminRole, common.PublicRole} {
		err = mc.DropRole(ctx, client.NewDropRoleOption(roleName))
		common.CheckErr(t, err, false, "is a default role, which can't be dropped")
	}
}

func TestDropRoleBindPrivilege(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{Address: hp.GetAddr(), Username: hp.GetUser(), Password: hp.GetPassword()})
	setupTest(t, ctx, mc)

	roleName := common.GenRandomString("role", 6)
	err := mc.CreateRole(ctx, client.NewCreateRoleOption(roleName))
	common.CheckErr(t, err, true)

	err = mc.GrantPrivilegeV2(ctx, client.NewGrantPrivilegeV2Option(roleName, "CollectionAdmin", AllObjectName))
	common.CheckErr(t, err, true)

	err = mc.DropRole(ctx, client.NewDropRoleOption(roleName))
	common.CheckErr(t, err, false, "fail to drop the role that it has privileges")

	// revoke privilege -> drop role
	err = mc.RevokePrivilegeV2(ctx, client.NewRevokePrivilegeV2Option(roleName, "CollectionAdmin", AllObjectName))
	common.CheckErr(t, err, true)

	err = mc.DropRole(ctx, client.NewDropRoleOption(roleName))
	common.CheckErr(t, err, true)
}

func TestDescribeRole(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{Address: hp.GetAddr(), Username: hp.GetUser(), Password: hp.GetPassword()})
	resetRbac(t, ctx, mc)

	// describe role that no grants
	roleName := common.GenRandomString("role", 6)
	err := mc.CreateRole(ctx, client.NewCreateRoleOption(roleName))
	common.CheckErr(t, err, true)
	role, err := mc.DescribeRole(ctx, client.NewDescribeRoleOption(roleName))
	common.CheckErr(t, err, true)
	require.Equal(t, &entity.Role{RoleName: roleName, Privileges: []entity.GrantItem{}}, role)

	// describe role that has grants
	err = mc.GrantPrivilegeV2(ctx, client.NewGrantPrivilegeV2Option(roleName, "DatabaseAdmin", AllObjectName))
	common.CheckErr(t, err, true)

	err = mc.GrantPrivilegeV2(ctx, client.NewGrantPrivilegeV2Option(roleName, "ClusterAdmin", AllObjectName).WithDbName(AllObjectName))
	common.CheckErr(t, err, true)

	role, err = mc.DescribeRole(ctx, client.NewDescribeRoleOption(roleName))
	common.CheckErr(t, err, true)
	expRole := &entity.Role{
		RoleName: roleName,
		Privileges: []entity.GrantItem{
			{
				Object:     GlobalObjectType,
				ObjectName: AllObjectName,
				RoleName:   roleName,
				Grantor:    hp.GetUser(),
				Privilege:  "ClusterAdmin",
				DbName:     AllObjectName,
			},
			{
				Object:     GlobalObjectType,
				ObjectName: AllObjectName,
				RoleName:   roleName,
				Grantor:    hp.GetUser(),
				Privilege:  "DatabaseAdmin",
				DbName:     common.DefaultDb,
			},
		},
	}
	require.EqualValues(t, expRole, role)

	// describe a not existed role
	role, err = mc.DescribeRole(ctx, client.NewDescribeRoleOption(common.GenRandomString("role", 6)))
	common.CheckErr(t, err, false, "role not found")
}

// grant v2 use connected db as default db
func TestGrantV2PrivilegeConnectedDb(t *testing.T) {
	t.Skip("https://github.com/milvus-io/milvus/issues/40340")
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{Address: hp.GetAddr(), Username: hp.GetUser(), Password: hp.GetPassword()})
	resetRbac(t, ctx, mc)

	// create a user
	userName := common.GenRandomString("user", 6)
	pwd := common.GenRandomString("pwd", 6)
	err := mc.CreateUser(ctx, client.NewCreateUserOption(userName, pwd))
	common.CheckErr(t, err, true)

	// create a database
	dbName := common.GenRandomString("db", 6)
	err = mc.CreateDatabase(ctx, client.NewCreateDatabaseOption(dbName))
	common.CheckErr(t, err, true)

	// init client with new db
	mcDb := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{Address: hp.GetAddr(), Username: hp.GetUser(), Password: hp.GetPassword(), DBName: dbName})

	// create a role
	roleName := common.GenRandomString("role", 6)
	err = mcDb.CreateRole(ctx, client.NewCreateRoleOption(roleName))
	common.CheckErr(t, err, true)

	err = mcDb.GrantPrivilegeV2(ctx, client.NewGrantPrivilegeV2Option(roleName, "CollectionAdmin", AllObjectName))
	common.CheckErr(t, err, true)

	// describe role and check privilege
	role, err := mcDb.DescribeRole(ctx, client.NewDescribeRoleOption(roleName))
	common.CheckErr(t, err, true)
	require.Equal(t, &entity.Role{
		RoleName: roleName,
		Privileges: []entity.GrantItem{
			{
				Object:     GlobalObjectType,
				ObjectName: AllObjectName,
				RoleName:   roleName,
				Grantor:    hp.GetUser(),
				Privilege:  "CollectionAdmin",
				DbName:     dbName,
			},
		},
	}, role)
}

// grant v2 use connected db as default db
func TestGrantV2PrivilegeSpecifyDb(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{Address: hp.GetAddr(), Username: hp.GetUser(), Password: hp.GetPassword()})
	resetRbac(t, ctx, mc)

	// create a user
	userName := common.GenRandomString("user", 6)
	pwd := common.GenRandomString("pwd", 6)
	err := mc.CreateUser(ctx, client.NewCreateUserOption(userName, pwd))
	common.CheckErr(t, err, true)

	// create a database
	dbName := common.GenRandomString("db", 6)
	err = mc.CreateDatabase(ctx, client.NewCreateDatabaseOption(dbName))
	common.CheckErr(t, err, true)

	// create a role
	roleName := common.GenRandomString("role", 6)
	err = mc.CreateRole(ctx, client.NewCreateRoleOption(roleName))
	common.CheckErr(t, err, true)

	err = mc.GrantPrivilegeV2(ctx, client.NewGrantPrivilegeV2Option(roleName, "CollectionAdmin", AllObjectName).WithDbName(dbName))
	common.CheckErr(t, err, true)

	// describe role and check privilege
	role, err := mc.DescribeRole(ctx, client.NewDescribeRoleOption(roleName))
	common.CheckErr(t, err, true)
	require.Equal(t, role, &entity.Role{RoleName: roleName, Privileges: []entity.GrantItem{}})

	roleDb, errDb := mc.DescribeRole(ctx, client.NewDescribeRoleOption(roleName).WithDbName(dbName))
	common.CheckErr(t, errDb, true)
	require.Equal(t, &entity.Role{
		RoleName: roleName,
		Privileges: []entity.GrantItem{
			{
				Object:     GlobalObjectType,
				ObjectName: AllObjectName,
				RoleName:   roleName,
				Grantor:    hp.GetUser(),
				Privilege:  "CollectionAdmin",
				DbName:     dbName,
			},
		},
	}, roleDb)
}

// grant use connected db as default db
func TestGrantPrivilegeConnectedDb(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{Address: hp.GetAddr(), Username: hp.GetUser(), Password: hp.GetPassword()})
	resetRbac(t, ctx, mc)

	// create a user
	userName := common.GenRandomString("user", 6)
	pwd := common.GenRandomString("pwd", 6)
	err := mc.CreateUser(ctx, client.NewCreateUserOption(userName, pwd))
	common.CheckErr(t, err, true)

	// create a database
	dbName := common.GenRandomString("db", 6)
	err = mc.CreateDatabase(ctx, client.NewCreateDatabaseOption(dbName))
	common.CheckErr(t, err, true)

	// init client with new db
	mcDb := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{Address: hp.GetAddr(), Username: hp.GetUser(), Password: hp.GetPassword(), DBName: dbName})

	// create a role
	roleName := common.GenRandomString("role", 6)
	err = mcDb.CreateRole(ctx, client.NewCreateRoleOption(roleName))
	common.CheckErr(t, err, true)

	err = mcDb.GrantPrivilege(ctx, client.NewGrantPrivilegeOption(roleName, "Collection", "Insert", "*"))
	common.CheckErr(t, err, true)

	// describe role and check privilege
	role, err := mcDb.DescribeRole(ctx, client.NewDescribeRoleOption(roleName))
	common.CheckErr(t, err, true)
	require.Equal(t, &entity.Role{
		RoleName: roleName,
		Privileges: []entity.GrantItem{
			{
				Object:     CollectionObjectType,
				ObjectName: AllObjectName,
				RoleName:   roleName,
				Grantor:    hp.GetUser(),
				Privilege:  "Insert",
				DbName:     dbName,
			},
		},
	}, role)
}

// grant v2 use connected db as default db
func TestGrantPrivilegeSpecifyDb(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{Address: hp.GetAddr(), Username: hp.GetUser(), Password: hp.GetPassword()})
	resetRbac(t, ctx, mc)

	// create a user
	userName := common.GenRandomString("user", 6)
	pwd := common.GenRandomString("pwd", 6)
	err := mc.CreateUser(ctx, client.NewCreateUserOption(userName, pwd))
	common.CheckErr(t, err, true)

	// create a database
	dbName := common.GenRandomString("db", 6)
	err = mc.CreateDatabase(ctx, client.NewCreateDatabaseOption(dbName))
	common.CheckErr(t, err, true)

	// create a role
	roleName := common.GenRandomString("role", 6)
	err = mc.CreateRole(ctx, client.NewCreateRoleOption(roleName))
	common.CheckErr(t, err, true)

	err = mc.GrantPrivilege(ctx, client.NewGrantPrivilegeOption(roleName, "Collection", "Insert", "*").WithDbName(dbName))
	common.CheckErr(t, err, true)

	// describe role and check privilege
	role, err := mc.DescribeRole(ctx, client.NewDescribeRoleOption(roleName))
	common.CheckErr(t, err, true)
	require.Equal(t, role, &entity.Role{RoleName: roleName, Privileges: []entity.GrantItem{}})

	roleDb, errDb := mc.DescribeRole(ctx, client.NewDescribeRoleOption(roleName).WithDbName(dbName))
	common.CheckErr(t, errDb, true)
	require.Equal(t, &entity.Role{
		RoleName: roleName,
		Privileges: []entity.GrantItem{
			{
				Object:     CollectionObjectType,
				ObjectName: AllObjectName,
				RoleName:   roleName,
				Grantor:    hp.GetUser(),
				Privilege:  "Insert",
				DbName:     dbName,
			},
		},
	}, roleDb)
}

func TestGrantPrivilegeInvalid(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{Address: hp.GetAddr(), Username: hp.GetUser(), Password: hp.GetPassword()})
	// resetRbac(t, ctx, mc)

	// create a role
	roleName := common.GenRandomString("role", 6)
	err := mc.CreateRole(ctx, client.NewCreateRoleOption(roleName))
	common.CheckErr(t, err, true)

	// grant privilege to a not existed role
	err = mc.GrantPrivilege(ctx, client.NewGrantPrivilegeOption(common.GenRandomString("role", 6), "Collection", "Insert", "*"))
	common.CheckErr(t, err, false, "not found the role")
	err = mc.GrantPrivilegeV2(ctx, client.NewGrantPrivilegeV2Option(common.GenRandomString("role", 6), "CollectionAdmin", "*"))
	common.CheckErr(t, err, false, "not found the role")

	// grant privilege to a not existed objectName
	err = mc.GrantPrivilege(ctx, client.NewGrantPrivilegeOption(roleName, "Collection", "Insert", common.GenRandomString("collection", 6)))
	common.CheckErr(t, err, true)
	err = mc.GrantPrivilegeV2(ctx, client.NewGrantPrivilegeV2Option(roleName, "CollectionAdmin", common.GenRandomString("collection", 6)))
	common.CheckErr(t, err, true)

	// grant privilege to a not existed privilege
	err = mc.GrantPrivilege(ctx, client.NewGrantPrivilegeOption(roleName, "Collection", "aaa", "*"))
	common.CheckErr(t, err, false, "not found the privilege name")
	err = mc.GrantPrivilegeV2(ctx, client.NewGrantPrivilegeV2Option(roleName, "aaa", "*"))
	common.CheckErr(t, err, false, "not found the privilege name")

	// grant privilege to a not existed objectType
	err = mc.GrantPrivilege(ctx, client.NewGrantPrivilegeOption(roleName, "aaa", "Insert", "*"))
	common.CheckErr(t, err, false, "the object entity in the request is nil or invalid")

	// grant privilege to a not existed db
	err = mc.GrantPrivilege(ctx, client.NewGrantPrivilegeOption(roleName, "Collection", "Insert", "*").WithDbName(common.GenRandomString("db", 6)))
	common.CheckErr(t, err, true)
	err = mc.GrantPrivilegeV2(ctx, client.NewGrantPrivilegeV2Option(roleName, "CollectionAdmin", "*").WithDbName(common.GenRandomString("db", 6)))
	common.CheckErr(t, err, true)
}

func TestRevokePrivilegeInvalid(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{Address: hp.GetAddr(), Username: hp.GetUser(), Password: hp.GetPassword()})
	resetRbac(t, ctx, mc)

	roleName := common.GenRandomString("role", 6)
	err := mc.CreateRole(ctx, client.NewCreateRoleOption(roleName))
	common.CheckErr(t, err, true)

	// revoke privilege to a not existed role
	err = mc.RevokePrivilege(ctx, client.NewRevokePrivilegeOption(common.GenRandomString("role", 6), "Collection", "Insert", "*"))
	common.CheckErr(t, err, false, "not found the role")
	err = mc.RevokePrivilegeV2(ctx, client.NewRevokePrivilegeV2Option(common.GenRandomString("role", 6), "CollectionAdmin", "*"))
	common.CheckErr(t, err, false, "not found the role")

	// revoke privilege to a not existed objectName
	err = mc.RevokePrivilege(ctx, client.NewRevokePrivilegeOption(roleName, "Collection", "Insert", "*"))
	common.CheckErr(t, err, true)
	err = mc.RevokePrivilegeV2(ctx, client.NewRevokePrivilegeV2Option(roleName, "CollectionAdmin", common.GenRandomString("collection", 6)))
	common.CheckErr(t, err, true)

	// revoke privilege to a not existed privilege
	err = mc.RevokePrivilege(ctx, client.NewRevokePrivilegeOption(roleName, "Collection", "aaa", "*"))
	common.CheckErr(t, err, false, "not found the privilege name")
	err = mc.RevokePrivilegeV2(ctx, client.NewRevokePrivilegeV2Option(roleName, "aaa", "*"))
	common.CheckErr(t, err, false, "not found the privilege name")

	// revoke privilege to a not existed objectType
	err = mc.RevokePrivilege(ctx, client.NewRevokePrivilegeOption(roleName, "aaa", "Insert", "*"))
	common.CheckErr(t, err, false, "the object entity in the request is nil or invalid")

	// revoke privilege to a not existed db
	err = mc.RevokePrivilege(ctx, client.NewRevokePrivilegeOption(roleName, "Collection", "Insert", "*").WithDbName(common.GenRandomString("db", 6)))
	common.CheckErr(t, err, true)
	err = mc.RevokePrivilegeV2(ctx, client.NewRevokePrivilegeV2Option(roleName, "CollectionAdmin", "*").WithDbName(common.GenRandomString("db", 6)))
	common.CheckErr(t, err, true)
}

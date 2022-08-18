package dao

import (
	"errors"
	"testing"

	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestUserRole_GetUserRoles(t *testing.T) {
	var (
		userID1   = 1
		userID2   = 2
		roleID1   = 10
		roleID2   = 20
		userRoles []*dbmodel.UserRole
		getQuery  func() *sqlmock.ExpectedQuery
		err       error
	)

	// mock user and role
	getQuery = func() *sqlmock.ExpectedQuery {
		return mock.ExpectQuery("SELECT * FROM `user_role` WHERE `is_deleted` = ? AND `tenant_id` = ?").
			WithArgs(false, tenantID)
	}
	getQuery().WillReturnRows(
		sqlmock.NewRows([]string{"tenant_id", "user_id", "role_id"}).
			AddRow(tenantID, userID1, roleID1).
			AddRow(tenantID, userID2, roleID2))

	mock.ExpectQuery("SELECT * FROM `role` WHERE `role`.`id` IN (?,?)").
		WithArgs(roleID1, roleID2).
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "tenant_id", "name"}).
				AddRow(roleID1, tenantID, "foo1").
				AddRow(roleID2, tenantID, "foo2"))

	mock.ExpectQuery("SELECT * FROM `credential_users` WHERE `credential_users`.`id` IN (?,?)").
		WithArgs(userID1, userID2).
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "tenant_id", "username"}).
				AddRow(userID1, tenantID, "fo1").
				AddRow(userID2, tenantID, "fo2"))

	userRoles, err = userRoleTestDb.GetUserRoles(tenantID, 0, 0)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(userRoles))
	assert.Equal(t, "foo1", userRoles[0].Role.Name)
	assert.Equal(t, "fo1", userRoles[0].User.Username)

	getQuery().WillReturnError(errors.New("test error"))
	_, err = userRoleTestDb.GetUserRoles(tenantID, 0, 0)
	assert.Error(t, err)
}

func TestUserRole_GetUserRolesWithUserID(t *testing.T) {
	var (
		userID1   = 1
		roleID1   = 10
		roleID2   = 20
		userRoles []*dbmodel.UserRole
		err       error
	)

	mock.ExpectQuery("SELECT * FROM `user_role` WHERE `user_role`.`user_id` = ? AND `is_deleted` = ? AND `tenant_id` = ?").
		WithArgs(userID1, false, tenantID).
		WillReturnRows(
			sqlmock.NewRows([]string{"tenant_id", "user_id", "role_id"}).
				AddRow(tenantID, userID1, roleID1).
				AddRow(tenantID, userID1, roleID2))
	mock.ExpectQuery("SELECT * FROM `role` WHERE `role`.`id` IN (?,?)").
		WithArgs(roleID1, roleID2).
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "tenant_id", "name"}).
				AddRow(roleID1, tenantID, "foo1").
				AddRow(roleID2, tenantID, "foo2"))
	mock.ExpectQuery("SELECT * FROM `credential_users` WHERE `credential_users`.`id` = ?").
		WithArgs(userID1).
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "tenant_id", "username"}).
				AddRow(userID1, tenantID, "fo1"))

	userRoles, err = userRoleTestDb.GetUserRoles(tenantID, int64(userID1), 0)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(userRoles))
	assert.Equal(t, "foo2", userRoles[1].Role.Name)
	assert.Equal(t, "fo1", userRoles[0].User.Username)
}

func TestUserRole_GetUserRolesWithRoleID(t *testing.T) {
	var (
		userID1   = 1
		userID2   = 2
		roleID1   = 10
		userRoles []*dbmodel.UserRole
		err       error
	)

	mock.ExpectQuery("SELECT * FROM `user_role` WHERE `user_role`.`role_id` = ? AND `is_deleted` = ? AND `tenant_id` = ?").
		WithArgs(roleID1, false, tenantID).
		WillReturnRows(
			sqlmock.NewRows([]string{"tenant_id", "user_id", "role_id"}).
				AddRow(tenantID, userID1, roleID1).
				AddRow(tenantID, userID2, roleID1))
	mock.ExpectQuery("SELECT * FROM `role` WHERE `role`.`id` = ?").
		WithArgs(roleID1).
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "tenant_id", "name"}).
				AddRow(roleID1, tenantID, "foo1"))
	mock.ExpectQuery("SELECT * FROM `credential_users` WHERE `credential_users`.`id` IN (?,?)").
		WithArgs(userID1, userID2).
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "tenant_id", "username"}).
				AddRow(userID1, tenantID, "fo1").
				AddRow(userID2, tenantID, "fo2"))

	userRoles, err = userRoleTestDb.GetUserRoles(tenantID, 0, int64(roleID1))
	assert.NoError(t, err)
	assert.Equal(t, 2, len(userRoles))
	assert.Equal(t, "foo1", userRoles[0].Role.Name)
	assert.Equal(t, "fo2", userRoles[1].User.Username)
}

func TestUserRole_Insert(t *testing.T) {
	var (
		userRole *dbmodel.UserRole
		err      error
	)
	userRole = &dbmodel.UserRole{
		Base:   GetBase(),
		UserID: 1,
		RoleID: 1,
	}

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `user_role` (`tenant_id`,`is_deleted`,`created_at`,`updated_at`,`user_id`,`role_id`) VALUES (?,?,?,?,?,?)").
		WithArgs(userRole.TenantID, userRole.IsDeleted, userRole.CreatedAt, userRole.UpdatedAt, userRole.UserID, userRole.RoleID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	err = userRoleTestDb.Insert(userRole)
	assert.NoError(t, err)
}

func TestUserRole_InsertError(t *testing.T) {
	var (
		userRole *dbmodel.UserRole
		err      error
	)
	userRole = &dbmodel.UserRole{
		Base:   GetBase(),
		UserID: 1,
		RoleID: 1,
	}

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `user_role` (`tenant_id`,`is_deleted`,`created_at`,`updated_at`,`user_id`,`role_id`) VALUES (?,?,?,?,?,?)").
		WithArgs(userRole.TenantID, userRole.IsDeleted, userRole.CreatedAt, userRole.UpdatedAt, userRole.UserID, userRole.RoleID).
		WillReturnError(errors.New("test error"))
	mock.ExpectRollback()
	err = userRoleTestDb.Insert(userRole)
	assert.Error(t, err)
}

func TestUserRole_Delete(t *testing.T) {
	var (
		userRole *dbmodel.UserRole
		getExec  func() *sqlmock.ExpectedExec
		err      error
	)
	userRole = &dbmodel.UserRole{
		Base:   GetBase(),
		UserID: 1,
		RoleID: 1,
	}
	getExec = func() *sqlmock.ExpectedExec {
		return mock.ExpectExec("UPDATE `user_role` SET `is_deleted`=?,`updated_at`=? WHERE `user_role`.`user_id` = ? AND `user_role`.`role_id` = ? AND `is_deleted` = ? AND `tenant_id` = ?").
			WithArgs(true, AnyTime{}, userRole.UserID, userRole.RoleID, userRole.IsDeleted, userRole.TenantID)
	}
	mock.ExpectBegin()
	getExec().WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	err = userRoleTestDb.Delete(userRole.TenantID, userRole.UserID, userRole.RoleID)
	assert.NoError(t, err)

	mock.ExpectBegin()
	getExec().WillReturnError(errors.New("test error"))
	mock.ExpectRollback()
	err = userRoleTestDb.Delete(userRole.TenantID, userRole.UserID, userRole.RoleID)
	assert.Error(t, err)
}

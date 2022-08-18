package dao

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestRole_GetRoles(t *testing.T) {
	var (
		roles []*dbmodel.Role
		err   error
	)

	mock.ExpectQuery("SELECT * FROM `role` WHERE `is_deleted` = ? AND `tenant_id` = ?").
		WithArgs(false, tenantID).
		WillReturnRows(
			sqlmock.NewRows([]string{"tenant_id", "name"}).
				AddRow(tenantID, "foo1").
				AddRow(tenantID, "foo2"))

	roles, err = roleTestDb.GetRoles(tenantID, "")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(roles))
}

func TestRole_GetRoles_Error(t *testing.T) {
	mock.ExpectQuery("SELECT * FROM `role` WHERE `is_deleted` = ? AND `tenant_id` = ?").
		WithArgs(false, tenantID).
		WillReturnError(errors.New("test error"))
	_, err := roleTestDb.GetRoles(tenantID, "")
	assert.Error(t, err)
}

func TestRole_GetRoles_WithRoleName(t *testing.T) {
	var (
		roleName = "foo1"
		roles    []*dbmodel.Role
		err      error
	)
	mock.ExpectQuery("SELECT * FROM `role` WHERE `role`.`name` = ? AND `is_deleted` = ? AND `tenant_id` = ?").
		WithArgs(roleName, false, tenantID).
		WillReturnRows(
			sqlmock.NewRows([]string{"tenant_id", "name"}).
				AddRow(tenantID, roleName))
	roles, err = roleTestDb.GetRoles(tenantID, roleName)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(roles))
	assert.Equal(t, roleName, roles[0].Name)
}

func TestRole_Insert(t *testing.T) {
	var (
		role *dbmodel.Role
		err  error
	)
	role = &dbmodel.Role{
		Base: GetBase(),
		Name: "foo",
	}

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `role` (`tenant_id`,`is_deleted`,`created_at`,`updated_at`,`name`) VALUES (?,?,?,?,?)").
		WithArgs(role.TenantID, role.IsDeleted, role.CreatedAt, role.UpdatedAt, role.Name).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err = roleTestDb.Insert(role)
	assert.NoError(t, err)

}

func TestRole_Insert_Error(t *testing.T) {
	var (
		role *dbmodel.Role
		err  error
	)
	role = &dbmodel.Role{
		Base: GetBase(),
		Name: "foo",
	}
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `role` (`tenant_id`,`is_deleted`,`created_at`,`updated_at`,`name`) VALUES (?,?,?,?,?)").
		WithArgs(role.TenantID, role.IsDeleted, role.CreatedAt, role.UpdatedAt, role.Name).
		WillReturnError(errors.New("test error"))
	mock.ExpectRollback()
	err = roleTestDb.Insert(role)
	assert.Error(t, err)
}

func TestRole_Delete(t *testing.T) {
	var (
		role *dbmodel.Role
		err  error
	)
	role = &dbmodel.Role{
		Base: GetBase(),
		Name: "foo",
	}
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `role` SET `is_deleted`=?,`updated_at`=? WHERE `role`.`name` = ? AND `is_deleted` = ? AND `tenant_id` = ?").
		WithArgs(true, AnyTime{}, role.Name, role.IsDeleted, role.TenantID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err = roleTestDb.Delete(role.TenantID, role.Name)
	assert.NoError(t, err)
}

func TestRole_Delete_Error(t *testing.T) {
	var (
		role *dbmodel.Role
		err  error
	)
	role = &dbmodel.Role{
		Base: GetBase(),
		Name: "foo",
	}
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `role` SET `is_deleted`=?,`updated_at`=? WHERE `role`.`name` = ? AND `is_deleted` = ? AND `tenant_id` = ?").
		WithArgs(true, AnyTime{}, role.Name, role.IsDeleted, role.TenantID).
		WillReturnError(errors.New("test error"))
	mock.ExpectRollback()

	err = roleTestDb.Delete(role.TenantID, role.Name)
	assert.Error(t, err)
}

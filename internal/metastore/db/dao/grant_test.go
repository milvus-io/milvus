package dao

import (
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"github.com/stretchr/testify/assert"
)

func TestGrant_GetGrants(t *testing.T) {
	var (
		roleID1    = 10
		roleID2    = 20
		object     = "Collection"
		objectName = "col1"
		grants     []*dbmodel.Grant
		err        error
	)

	mock.ExpectQuery("SELECT * FROM `grant` WHERE `is_deleted` = ? AND `tenant_id` = ?").
		WithArgs(false, tenantID).
		WillReturnRows(
			sqlmock.NewRows([]string{"tenant_id", "role_id", "object", "object_name"}).
				AddRow(tenantID, roleID1, object, objectName).
				AddRow(tenantID, roleID2, object, objectName))

	mock.ExpectQuery("SELECT * FROM `role` WHERE `role`.`id` IN (?,?)").
		WithArgs(roleID1, roleID2).
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "tenant_id", "name"}).
				AddRow(roleID1, tenantID, "foo1").
				AddRow(roleID2, tenantID, "foo2"))

	grants, err = grantTestDb.GetGrants(tenantID, 0, "", "")
	mock.MatchExpectationsInOrder(false)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(grants))
	assert.Equal(t, "foo2", grants[1].Role.Name)
	assert.Equal(t, object, grants[0].Object)
	assert.Equal(t, objectName, grants[0].ObjectName)

	mock.ExpectQuery("SELECT * FROM `grant` WHERE `is_deleted` = ? AND `tenant_id` = ?").
		WithArgs(false, tenantID).
		WillReturnError(errors.New("test error"))
	_, err = grantTestDb.GetGrants(tenantID, 0, "", "")
	mock.MatchExpectationsInOrder(false)
	assert.Error(t, err)
}

func TestGrant_GetGrantsWithRoleID(t *testing.T) {
	var (
		roleID1     = 10
		object1     = "Collection"
		objectName1 = "col1"
		object2     = "Global"
		objectName2 = "*"
		grants      []*dbmodel.Grant
		err         error
	)

	mock.ExpectQuery("SELECT * FROM `grant` WHERE `grant`.`role_id` = ? AND `is_deleted` = ? AND `tenant_id` = ?").
		WithArgs(roleID1, false, tenantID).
		WillReturnRows(
			sqlmock.NewRows([]string{"tenant_id", "role_id", "object", "object_name"}).
				AddRow(tenantID, roleID1, object1, objectName1).
				AddRow(tenantID, roleID1, object2, objectName2))

	mock.ExpectQuery("SELECT * FROM `role` WHERE `role`.`id` = ?").
		WithArgs(roleID1).
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "tenant_id", "name"}).
				AddRow(roleID1, tenantID, "foo1"))

	grants, err = grantTestDb.GetGrants(tenantID, int64(roleID1), "", "")
	mock.MatchExpectationsInOrder(false)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(grants))
	assert.Equal(t, "foo1", grants[0].Role.Name)
	assert.Equal(t, object1, grants[0].Object)
	assert.Equal(t, objectName2, grants[1].ObjectName)
}

func TestGrant_GetGrantsWithObject(t *testing.T) {
	var (
		roleID     = 10
		object     = "Collection"
		objectName = "col1"
		grants     []*dbmodel.Grant
		err        error
	)

	mock.ExpectQuery("SELECT * FROM `grant` WHERE `grant`.`role_id` = ? AND `grant`.`object` = ? AND `grant`.`object_name` = ? AND `is_deleted` = ? AND `tenant_id` = ?").
		WithArgs(roleID, object, objectName, false, tenantID).
		WillReturnRows(
			sqlmock.NewRows([]string{"tenant_id", "role_id", "object", "object_name"}).
				AddRow(tenantID, roleID, object, objectName))

	mock.ExpectQuery("SELECT * FROM `role` WHERE `role`.`id` = ?").
		WithArgs(roleID).
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "tenant_id", "name"}).
				AddRow(roleID, tenantID, "foo1"))

	grants, err = grantTestDb.GetGrants(tenantID, int64(roleID), object, objectName)
	mock.MatchExpectationsInOrder(false)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(grants))
	assert.Equal(t, "foo1", grants[0].Role.Name)
	assert.Equal(t, object, grants[0].Object)
	assert.Equal(t, objectName, grants[0].ObjectName)
}

func TestGrant_Insert(t *testing.T) {
	var (
		grant *dbmodel.Grant
		err   error
	)
	grant = &dbmodel.Grant{
		Base:       GetBase(),
		RoleID:     1,
		Object:     "Global",
		ObjectName: "Col",
	}

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `grant` (`tenant_id`,`is_deleted`,`created_at`,`updated_at`,`role_id`,`object`,`object_name`) VALUES (?,?,?,?,?,?,?)").
		WithArgs(grant.TenantID, grant.IsDeleted, grant.CreatedAt, grant.UpdatedAt, grant.RoleID, grant.Object, grant.ObjectName).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	err = grantTestDb.Insert(grant)
	mock.MatchExpectationsInOrder(false)
	assert.NoError(t, err)

}

func TestGrant_Insert_Error(t *testing.T) {
	var (
		grant *dbmodel.Grant
		err   error
	)
	grant = &dbmodel.Grant{
		Base:       GetBase(),
		RoleID:     1,
		Object:     "Global",
		ObjectName: "Col",
	}

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `grant` (`tenant_id`,`is_deleted`,`created_at`,`updated_at`,`role_id`,`object`,`object_name`) VALUES (?,?,?,?,?,?,?)").
		WithArgs(grant.TenantID, grant.IsDeleted, grant.CreatedAt, grant.UpdatedAt, grant.RoleID, grant.Object, grant.ObjectName).
		WillReturnError(errors.New("test error"))
	mock.ExpectRollback()
	err = grantTestDb.Insert(grant)
	mock.MatchExpectationsInOrder(false)
	assert.Error(t, err)
}

func TestGrant_Delete(t *testing.T) {
	var (
		grant *dbmodel.Grant
		err   error
	)
	grant = &dbmodel.Grant{
		Base:       GetBase(),
		RoleID:     1,
		Object:     "Global",
		ObjectName: "Col",
	}

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `grant` SET `is_deleted`=?,`updated_at`=? WHERE `grant`.`role_id` = ? AND `grant`.`object` = ? AND `grant`.`object_name` = ? AND `is_deleted` = ? AND `tenant_id` = ?").
		WithArgs(true, AnyTime{}, grant.RoleID, grant.Object, grant.ObjectName, false, grant.TenantID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	err = grantTestDb.Delete(grant.TenantID, grant.RoleID, grant.Object, grant.ObjectName)
	mock.MatchExpectationsInOrder(false)
	assert.NoError(t, err)
}

func TestGrant_Delete_Error(t *testing.T) {
	var (
		grant *dbmodel.Grant
		err   error
	)
	grant = &dbmodel.Grant{
		Base:       GetBase(),
		RoleID:     1,
		Object:     "Global",
		ObjectName: "Col",
	}

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `grant` SET `is_deleted`=?,`updated_at`=? WHERE `grant`.`role_id` = ? AND `grant`.`object` = ? AND `grant`.`object_name` = ? AND `is_deleted` = ? AND `tenant_id` = ?").
		WithArgs(true, AnyTime{}, grant.RoleID, grant.Object, grant.ObjectName, false, grant.TenantID).
		WillReturnError(errors.New("test error"))
	mock.ExpectCommit()
	err = grantTestDb.Delete(grant.TenantID, grant.RoleID, grant.Object, grant.ObjectName)
	mock.MatchExpectationsInOrder(false)
	assert.Error(t, err)
}

package dao

import (
	"errors"
	"testing"

	"github.com/milvus-io/milvus/internal/common"

	"gorm.io/gorm"

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
		getQuery   func() *sqlmock.ExpectedQuery
		err        error
	)

	getQuery = func() *sqlmock.ExpectedQuery {
		return mock.ExpectQuery("SELECT * FROM `grant` WHERE `is_deleted` = ? AND `tenant_id` = ?").
			WithArgs(false, tenantID)
	}
	getQuery().WillReturnRows(
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
	assert.NoError(t, err)
	assert.Equal(t, 2, len(grants))
	assert.Equal(t, "foo2", grants[1].Role.Name)
	assert.Equal(t, object, grants[0].Object)
	assert.Equal(t, objectName, grants[0].ObjectName)

	getQuery().WillReturnError(errors.New("test error"))
	_, err = grantTestDb.GetGrants(tenantID, 0, "", "")
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
		getQuery    func() *sqlmock.ExpectedQuery
		err         error
	)

	getQuery = func() *sqlmock.ExpectedQuery {
		return mock.ExpectQuery("SELECT * FROM `grant` WHERE `grant`.`role_id` = ? AND `is_deleted` = ? AND `tenant_id` = ?").
			WithArgs(roleID1, false, tenantID)
	}
	getQuery().WillReturnRows(
		sqlmock.NewRows([]string{"tenant_id", "role_id", "object", "object_name"}).
			AddRow(tenantID, roleID1, object1, objectName1).
			AddRow(tenantID, roleID1, object2, objectName2))

	mock.ExpectQuery("SELECT * FROM `role` WHERE `role`.`id` = ?").
		WithArgs(roleID1).
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "tenant_id", "name"}).
				AddRow(roleID1, tenantID, "foo1"))

	grants, err = grantTestDb.GetGrants(tenantID, int64(roleID1), "", "")
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
		detail1    = "privilege1..."
		detail2    = "privilege2..."
		grants     []*dbmodel.Grant
		getQuery   func() *sqlmock.ExpectedQuery
		err        error
	)

	getQuery = func() *sqlmock.ExpectedQuery {
		return mock.ExpectQuery("SELECT * FROM `grant` WHERE `grant`.`role_id` = ? AND `grant`.`object` = ? AND `grant`.`object_name` = ? AND `is_deleted` = ? AND `tenant_id` = ?").
			WithArgs(roleID, object, objectName, false, tenantID)
	}
	getQuery().WillReturnRows(
		sqlmock.NewRows([]string{"tenant_id", "role_id", "object", "object_name", "detail"}).
			AddRow(tenantID, roleID, object, objectName, detail1).
			AddRow(tenantID, roleID, object, objectName, detail2))

	mock.ExpectQuery("SELECT * FROM `role` WHERE `role`.`id` = ?").
		WithArgs(roleID).
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "tenant_id", "name"}).
				AddRow(roleID, tenantID, "foo1"))

	grants, err = grantTestDb.GetGrants(tenantID, int64(roleID), object, objectName)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(grants))
	assert.Equal(t, "foo1", grants[0].Role.Name)
	assert.Equal(t, object, grants[1].Object)
	assert.Equal(t, objectName, grants[1].ObjectName)
	assert.Equal(t, detail2, grants[1].Detail)
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
		Detail:     "[[\"admin\",\"PrivilegeIndexDetail1\"]]",
	}

	mock.ExpectQuery("SELECT * FROM `grant` WHERE `grant`.`role_id` = ? AND `grant`.`object` = ? AND `grant`.`object_name` = ? AND `is_deleted` = ? AND `tenant_id` = ? LIMIT 1").
		WithArgs(grant.RoleID, grant.Object, grant.ObjectName, grant.IsDeleted, grant.TenantID).
		WillReturnError(gorm.ErrRecordNotFound)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `grant` (`tenant_id`,`is_deleted`,`created_at`,`updated_at`,`role_id`,`object`,`object_name`,`detail`) VALUES (?,?,?,?,?,?,?,?)").
		WithArgs(grant.TenantID, grant.IsDeleted, grant.CreatedAt, grant.UpdatedAt, grant.RoleID, grant.Object, grant.ObjectName, grant.Detail).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	err = grantTestDb.Insert(grant)
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
		Detail:     "[[\"admin\",\"PrivilegeIndexDetail2\"]]",
	}

	mock.ExpectQuery("SELECT * FROM `grant` WHERE `grant`.`role_id` = ? AND `grant`.`object` = ? AND `grant`.`object_name` = ? AND `is_deleted` = ? AND `tenant_id` = ? LIMIT 1").
		WithArgs(grant.RoleID, grant.Object, grant.ObjectName, grant.IsDeleted, grant.TenantID).
		WillReturnError(gorm.ErrRecordNotFound)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `grant` (`tenant_id`,`is_deleted`,`created_at`,`updated_at`,`role_id`,`object`,`object_name`,`detail`) VALUES (?,?,?,?,?,?,?,?)").
		WithArgs(grant.TenantID, grant.IsDeleted, grant.CreatedAt, grant.UpdatedAt, grant.RoleID, grant.Object, grant.ObjectName, grant.Detail).
		WillReturnError(errors.New("test error"))
	mock.ExpectRollback()
	err = grantTestDb.Insert(grant)
	assert.Error(t, err)
}

func TestGrant_Insert_SelectError(t *testing.T) {
	var (
		grant *dbmodel.Grant
		err   error
	)
	grant = &dbmodel.Grant{
		Base:       GetBase(),
		RoleID:     1,
		Object:     "Global",
		ObjectName: "Col",
		Detail:     "[[\"admin\",\"PrivilegeIndexDetail3\"]]",
	}

	mock.ExpectQuery("SELECT * FROM `grant` WHERE `grant`.`role_id` = ? AND `grant`.`object` = ? AND `grant`.`object_name` = ? AND `is_deleted` = ? AND `tenant_id` = ? LIMIT 1").
		WithArgs(grant.RoleID, grant.Object, grant.ObjectName, grant.IsDeleted, grant.TenantID).
		WillReturnError(errors.New("test error"))
	err = grantTestDb.Insert(grant)
	assert.Error(t, err)
}

func TestGrant_InsertDecode(t *testing.T) {
	var (
		grant *dbmodel.Grant
		err   error
	)
	grant = &dbmodel.Grant{
		Base:       GetBase(),
		RoleID:     1,
		Object:     "Global",
		ObjectName: "Col",
		Detail:     "[[\"admin\",\"PrivilegeIndexDetail\"]]",
	}

	mock.ExpectQuery("SELECT * FROM `grant` WHERE `grant`.`role_id` = ? AND `grant`.`object` = ? AND `grant`.`object_name` = ? AND `is_deleted` = ? AND `tenant_id` = ? LIMIT 1").
		WithArgs(grant.RoleID, grant.Object, grant.ObjectName, grant.IsDeleted, grant.TenantID).
		WillReturnRows(
			sqlmock.NewRows([]string{"tenant_id", "role_id", "object", "object_name", "detail"}).
				AddRow(tenantID, grant.RoleID, grant.Object, grant.ObjectName, "aaa"))
	err = grantTestDb.Insert(grant)
	assert.Error(t, err)
}

func TestGrant_InsertUpdate(t *testing.T) {
	var (
		originDetail = "[[\"admin\",\"PrivilegeLoad\"]]"
		grant        *dbmodel.Grant
		expectDetail string
		err          error
	)
	grant = &dbmodel.Grant{
		Base:       GetBase(),
		RoleID:     1,
		Object:     "Global",
		ObjectName: "Col",
		Detail:     "[[\"admin\",\"PrivilegeIndexDetail\"]]",
	}

	expectDetail, _ = dbmodel.EncodeGrantDetailForString(originDetail, grant.Detail, true)
	mock.ExpectQuery("SELECT * FROM `grant` WHERE `grant`.`role_id` = ? AND `grant`.`object` = ? AND `grant`.`object_name` = ? AND `is_deleted` = ? AND `tenant_id` = ? LIMIT 1").
		WithArgs(grant.RoleID, grant.Object, grant.ObjectName, grant.IsDeleted, grant.TenantID).
		WillReturnRows(
			sqlmock.NewRows([]string{"tenant_id", "role_id", "object", "object_name", "detail"}).
				AddRow(tenantID, grant.RoleID, grant.Object, grant.ObjectName, originDetail))
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `grant` SET `detail`=?,`updated_at`=? WHERE `grant`.`role_id` = ? AND `grant`.`object` = ? AND `grant`.`object_name` = ? AND `is_deleted` = ? AND `tenant_id` = ?").
		WithArgs(expectDetail, AnyTime{}, grant.RoleID, grant.Object, grant.ObjectName, grant.IsDeleted, grant.TenantID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	err = grantTestDb.Insert(grant)
	assert.NoError(t, err)
}

func TestGrant_InsertUpdateError(t *testing.T) {
	var (
		originDetail = "[[\"admin\",\"PrivilegeIndexDetail\"]]"
		grant        *dbmodel.Grant
		err          error
	)
	grant = &dbmodel.Grant{
		Base:       GetBase(),
		RoleID:     1,
		Object:     "Global",
		ObjectName: "Col",
		Detail:     "[[\"admin\",\"PrivilegeIndexDetail\"]]",
	}

	mock.ExpectQuery("SELECT * FROM `grant` WHERE `grant`.`role_id` = ? AND `grant`.`object` = ? AND `grant`.`object_name` = ? AND `is_deleted` = ? AND `tenant_id` = ? LIMIT 1").
		WithArgs(grant.RoleID, grant.Object, grant.ObjectName, grant.IsDeleted, grant.TenantID).
		WillReturnRows(
			sqlmock.NewRows([]string{"tenant_id", "role_id", "object", "object_name", "detail"}).
				AddRow(tenantID, grant.RoleID, grant.Object, grant.ObjectName, originDetail))
	err = grantTestDb.Insert(grant)
	assert.Error(t, err)
	assert.True(t, common.IsIgnorableError(err))
}

func TestGrant_DeleteWithoutPrivilege(t *testing.T) {
	var (
		grant     *dbmodel.Grant
		privilege = "PrivilegeIndexDetail"
		err       error
	)
	grant = &dbmodel.Grant{
		Base:       GetBase(),
		RoleID:     1,
		Object:     "Global",
		ObjectName: "Col",
		Detail:     "[[\"admin\",\"PrivilegeIndexDetail\"]]",
	}

	mock.ExpectQuery("SELECT * FROM `grant` WHERE `grant`.`role_id` = ? AND `grant`.`object` = ? AND `grant`.`object_name` = ? AND `is_deleted` = ? AND `tenant_id` = ? LIMIT 1").
		WithArgs(grant.RoleID, grant.Object, grant.ObjectName, grant.IsDeleted, grant.TenantID).
		WillReturnError(gorm.ErrRecordNotFound)
	err = grantTestDb.Delete(grant.TenantID, grant.RoleID, grant.Object, grant.ObjectName, privilege)
	assert.Error(t, err)
	assert.True(t, common.IsIgnorableError(err))
}

func TestGrant_Delete_GetError(t *testing.T) {
	var (
		grant     *dbmodel.Grant
		privilege = "PrivilegeIndexDetail"
		err       error
	)
	grant = &dbmodel.Grant{
		Base:       GetBase(),
		RoleID:     1,
		Object:     "Global",
		ObjectName: "Col",
		Detail:     "[[\"admin\",\"PrivilegeIndexDetail\"]]",
	}

	mock.ExpectQuery("SELECT * FROM `grant` WHERE `grant`.`role_id` = ? AND `grant`.`object` = ? AND `grant`.`object_name` = ? AND `is_deleted` = ? AND `tenant_id` = ? LIMIT 1").
		WithArgs(grant.RoleID, grant.Object, grant.ObjectName, grant.IsDeleted, grant.TenantID).
		WillReturnError(errors.New("test error"))
	err = grantTestDb.Delete(grant.TenantID, grant.RoleID, grant.Object, grant.ObjectName, privilege)
	assert.Error(t, err)
}

func TestGrant_Delete_DecodeError(t *testing.T) {
	var (
		grant     *dbmodel.Grant
		privilege = "PrivilegeIndexDetail"
		err       error
	)
	grant = &dbmodel.Grant{
		Base:       GetBase(),
		RoleID:     1,
		Object:     "Global",
		ObjectName: "Col",
		Detail:     "[[\"admin\",\"PrivilegeIndexDetail\"]]",
	}

	mock.ExpectQuery("SELECT * FROM `grant` WHERE `grant`.`role_id` = ? AND `grant`.`object` = ? AND `grant`.`object_name` = ? AND `is_deleted` = ? AND `tenant_id` = ? LIMIT 1").
		WithArgs(grant.RoleID, grant.Object, grant.ObjectName, grant.IsDeleted, grant.TenantID).
		WillReturnRows(
			sqlmock.NewRows([]string{"tenant_id", "role_id", "object", "object_name", "detail"}).
				AddRow(tenantID, grant.RoleID, grant.Object, grant.ObjectName, "aaa"))
	err = grantTestDb.Delete(grant.TenantID, grant.RoleID, grant.Object, grant.ObjectName, privilege)
	assert.Error(t, err)
}

func TestGrant_Delete_Mark(t *testing.T) {
	var (
		grant     *dbmodel.Grant
		privilege = "PrivilegeIndexDetail"
		err       error
	)
	grant = &dbmodel.Grant{
		Base:       GetBase(),
		RoleID:     1,
		Object:     "Global",
		ObjectName: "Col",
		Detail:     "[[\"admin\",\"PrivilegeIndexDetail\"]]",
	}

	mock.ExpectQuery("SELECT * FROM `grant` WHERE `grant`.`role_id` = ? AND `grant`.`object` = ? AND `grant`.`object_name` = ? AND `is_deleted` = ? AND `tenant_id` = ? LIMIT 1").
		WithArgs(grant.RoleID, grant.Object, grant.ObjectName, grant.IsDeleted, grant.TenantID).
		WillReturnRows(
			sqlmock.NewRows([]string{"tenant_id", "role_id", "object", "object_name", "detail"}).
				AddRow(tenantID, grant.RoleID, grant.Object, grant.ObjectName, grant.Detail))
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `grant` SET `is_deleted`=?,`updated_at`=? WHERE `grant`.`role_id` = ? AND `grant`.`object` = ? AND `grant`.`object_name` = ? AND `is_deleted` = ? AND `tenant_id` = ?").
		WithArgs(true, AnyTime{}, grant.RoleID, grant.Object, grant.ObjectName, false, grant.TenantID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	err = grantTestDb.Delete(grant.TenantID, grant.RoleID, grant.Object, grant.ObjectName, privilege)
	assert.NoError(t, err)
}

func TestGrant_Delete_Update(t *testing.T) {
	var (
		grant        *dbmodel.Grant
		privilege    = "PrivilegeIndexDetail"
		expectDetail = "[[\"admin\",\"PrivilegeLoad\"]]"
		err          error
	)
	grant = &dbmodel.Grant{
		Base:       GetBase(),
		RoleID:     1,
		Object:     "Global",
		ObjectName: "Col",
		Detail:     "[[\"admin\",\"PrivilegeIndexDetail\"],[\"admin\",\"PrivilegeLoad\"]]",
	}

	mock.ExpectQuery("SELECT * FROM `grant` WHERE `grant`.`role_id` = ? AND `grant`.`object` = ? AND `grant`.`object_name` = ? AND `is_deleted` = ? AND `tenant_id` = ? LIMIT 1").
		WithArgs(grant.RoleID, grant.Object, grant.ObjectName, grant.IsDeleted, grant.TenantID).
		WillReturnRows(
			sqlmock.NewRows([]string{"tenant_id", "role_id", "object", "object_name", "detail"}).
				AddRow(tenantID, grant.RoleID, grant.Object, grant.ObjectName, grant.Detail))
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `grant` SET `detail`=?,`updated_at`=? WHERE `grant`.`role_id` = ? AND `grant`.`object` = ? AND `grant`.`object_name` = ? AND `is_deleted` = ? AND `tenant_id` = ?").
		WithArgs(expectDetail, AnyTime{}, grant.RoleID, grant.Object, grant.ObjectName, false, grant.TenantID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	err = grantTestDb.Delete(grant.TenantID, grant.RoleID, grant.Object, grant.ObjectName, privilege)
	assert.NoError(t, err)
}

func TestGrant_Delete_UpdateError(t *testing.T) {
	var (
		grant     *dbmodel.Grant
		privilege = "PrivilegeIndexDetail"
		err       error
	)
	grant = &dbmodel.Grant{
		Base:       GetBase(),
		RoleID:     1,
		Object:     "Global",
		ObjectName: "Col",
		Detail:     "[[\"admin\",\"PrivilegeIndexLoad\"],[\"admin\",\"PrivilegeQuery\"]]",
	}

	mock.ExpectQuery("SELECT * FROM `grant` WHERE `grant`.`role_id` = ? AND `grant`.`object` = ? AND `grant`.`object_name` = ? AND `is_deleted` = ? AND `tenant_id` = ? LIMIT 1").
		WithArgs(grant.RoleID, grant.Object, grant.ObjectName, grant.IsDeleted, grant.TenantID).
		WillReturnRows(
			sqlmock.NewRows([]string{"tenant_id", "role_id", "object", "object_name", "detail"}).
				AddRow(tenantID, grant.RoleID, grant.Object, grant.ObjectName, grant.Detail))
	err = grantTestDb.Delete(grant.TenantID, grant.RoleID, grant.Object, grant.ObjectName, privilege)
	assert.Error(t, err)
	assert.True(t, common.IsIgnorableError(err))
}

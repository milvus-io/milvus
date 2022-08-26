package dao

import (
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"github.com/stretchr/testify/assert"
)

func TestGrantID_GetGrantIDs(t *testing.T) {
	var (
		grantID1   int64 = 10
		grantID2   int64 = 20
		grantorID1 int64 = 1
		grantorID2 int64 = 2
		privilege1       = "PrivilegeLoad"
		privilege2       = "PrivilegeInsert"
		grantIDs   []*dbmodel.GrantID
		err        error
	)

	mock.ExpectQuery("SELECT * FROM `grant_id` WHERE `is_deleted` = ? AND `tenant_id` = ?").
		WithArgs(false, tenantID).
		WillReturnRows(
			sqlmock.NewRows([]string{"tenant_id", "grant_id", "grantor_id", "privilege"}).
				AddRow(tenantID, grantID1, grantorID1, privilege1).
				AddRow(tenantID, grantID2, grantorID2, privilege2))

	grantIDs, err = grantIDTestDb.GetGrantIDs(tenantID, 0, "", false, false)
	mock.MatchExpectationsInOrder(false)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(grantIDs))
	assert.Equal(t, grantID1, grantIDs[0].GrantID)
	assert.Equal(t, grantorID2, grantIDs[1].GrantorID)
	assert.Equal(t, privilege2, grantIDs[1].Privilege)

	mock.ExpectQuery("SELECT * FROM `grant_id` WHERE `is_deleted` = ? AND `tenant_id` = ?").
		WithArgs(false, tenantID).
		WillReturnError(errors.New("test error"))
	_, err = grantIDTestDb.GetGrantIDs(tenantID, 0, "", false, false)
	mock.MatchExpectationsInOrder(false)
	assert.Error(t, err)
}

func TestGrantID_GetGrantIDs_Preload(t *testing.T) {
	var (
		grantID1   int64 = 10
		grantID2   int64 = 20
		grantorID1 int64 = 1
		grantorID2 int64 = 2
		privilege1       = "PrivilegeLoad"
		privilege2       = "PrivilegeInsert"
		grantIDs   []*dbmodel.GrantID
		err        error
	)

	mock.ExpectQuery("SELECT * FROM `grant_id` WHERE `is_deleted` = ? AND `tenant_id` = ?").
		WithArgs(false, tenantID).
		WillReturnRows(
			sqlmock.NewRows([]string{"tenant_id", "grant_id", "grantor_id", "privilege"}).
				AddRow(tenantID, grantID1, grantorID1, privilege1).
				AddRow(tenantID, grantID2, grantorID2, privilege2))

	mock.ExpectQuery("SELECT * FROM `grant` WHERE `grant`.`id` IN (?,?)").
		WithArgs(grantID1, grantID2).
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "tenant_id", "object"}).
				AddRow(grantID1, tenantID, "obj1").
				AddRow(grantID2, tenantID, "obj2"))

	mock.ExpectQuery("SELECT * FROM `credential_users` WHERE `credential_users`.`id` IN (?,?)").
		WithArgs(grantorID1, grantorID2).
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "tenant_id", "username"}).
				AddRow(grantorID1, tenantID, "fo1").
				AddRow(grantorID2, tenantID, "fo2"))

	grantIDs, err = grantIDTestDb.GetGrantIDs(tenantID, 0, "", true, true)
	mock.MatchExpectationsInOrder(false)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(grantIDs))
	assert.Equal(t, grantID1, grantIDs[0].GrantID)
	assert.Equal(t, "obj1", grantIDs[0].Grant.Object)
	assert.Equal(t, grantorID2, grantIDs[1].GrantorID)
	assert.Equal(t, privilege2, grantIDs[1].Privilege)
	assert.Equal(t, "fo2", grantIDs[1].Grantor.Username)

	mock.ExpectQuery("SELECT * FROM `grant_id` WHERE `is_deleted` = ? AND `tenant_id` = ?").
		WithArgs(false, tenantID).
		WillReturnError(errors.New("test error"))
	_, err = grantIDTestDb.GetGrantIDs(tenantID, 0, "", true, true)
	mock.MatchExpectationsInOrder(false)
	assert.Error(t, err)
}

func TestGrantID_GetGrantIDs_WithGrant(t *testing.T) {
	var (
		grantID1   int64 = 10
		grantorID1 int64 = 1
		grantorID2 int64 = 2
		privilege1       = "PrivilegeLoad"
		privilege2       = "PrivilegeInsert"
		grantIDs   []*dbmodel.GrantID
		err        error
	)

	mock.ExpectQuery("SELECT * FROM `grant_id` WHERE `grant_id`.`grant_id` = ? AND `is_deleted` = ? AND `tenant_id` = ?").
		WithArgs(grantID1, false, tenantID).
		WillReturnRows(
			sqlmock.NewRows([]string{"tenant_id", "grant_id", "grantor_id", "privilege"}).
				AddRow(tenantID, grantID1, grantorID1, privilege1).
				AddRow(tenantID, grantID1, grantorID2, privilege2))

	grantIDs, err = grantIDTestDb.GetGrantIDs(tenantID, grantID1, "", false, false)
	mock.MatchExpectationsInOrder(false)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(grantIDs))
	assert.Equal(t, grantID1, grantIDs[0].GrantID)
	assert.Equal(t, grantorID2, grantIDs[1].GrantorID)
	assert.Equal(t, privilege2, grantIDs[1].Privilege)
}

func TestGrantID_GetGrantIDs_WithGrantAndPrivilege(t *testing.T) {
	var (
		grantID1   int64 = 10
		grantorID1 int64 = 1
		privilege1       = "PrivilegeLoad"
		grantIDs   []*dbmodel.GrantID
		err        error
	)

	mock.ExpectQuery("SELECT * FROM `grant_id` WHERE `grant_id`.`grant_id` = ? AND `grant_id`.`privilege` = ? AND `is_deleted` = ? AND `tenant_id` = ?").
		WithArgs(grantID1, privilege1, false, tenantID).
		WillReturnRows(
			sqlmock.NewRows([]string{"tenant_id", "grant_id", "grantor_id", "privilege"}).
				AddRow(tenantID, grantID1, grantorID1, privilege1))

	grantIDs, err = grantIDTestDb.GetGrantIDs(tenantID, grantID1, privilege1, false, false)
	mock.MatchExpectationsInOrder(false)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(grantIDs))
	assert.Equal(t, grantID1, grantIDs[0].GrantID)
	assert.Equal(t, privilege1, grantIDs[0].Privilege)
}

func TestGrantID_Insert(t *testing.T) {
	var (
		grantID *dbmodel.GrantID
		err     error
	)
	grantID = &dbmodel.GrantID{
		Base:      GetBase(),
		GrantID:   1,
		GrantorID: 10,
		Privilege: "PrivilegeLoad",
	}

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `grant_id` (`tenant_id`,`is_deleted`,`created_at`,`updated_at`,`grant_id`,`privilege`,`grantor_id`) VALUES (?,?,?,?,?,?,?)").
		WithArgs(grantID.TenantID, grantID.IsDeleted, grantID.CreatedAt, grantID.UpdatedAt, grantID.GrantID, grantID.Privilege, grantID.GrantorID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	err = grantIDTestDb.Insert(grantID)
	mock.MatchExpectationsInOrder(false)
	assert.NoError(t, err)

}

func TestGrantID_Insert_Error(t *testing.T) {
	var (
		grantID *dbmodel.GrantID
		err     error
	)
	grantID = &dbmodel.GrantID{
		Base:      GetBase(),
		GrantID:   1,
		GrantorID: 10,
		Privilege: "PrivilegeLoad",
	}

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `grant_id` (`tenant_id`,`is_deleted`,`created_at`,`updated_at`,`grant_id`,`privilege`,`grantor_id`) VALUES (?,?,?,?,?,?,?)").
		WithArgs(grantID.TenantID, grantID.IsDeleted, grantID.CreatedAt, grantID.UpdatedAt, grantID.GrantID, grantID.Privilege, grantID.GrantorID).
		WillReturnError(errors.New("test error"))
	mock.ExpectRollback()
	err = grantIDTestDb.Insert(grantID)
	mock.MatchExpectationsInOrder(false)
	assert.Error(t, err)
}

func TestGrantID_Delete(t *testing.T) {
	var (
		grantID *dbmodel.GrantID
		err     error
	)
	grantID = &dbmodel.GrantID{
		Base:      GetBase(),
		GrantID:   1,
		GrantorID: 10,
		Privilege: "PrivilegeLoad",
	}

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `grant_id` SET `is_deleted`=?,`updated_at`=? WHERE `grant_id`.`grant_id` = ? AND `grant_id`.`privilege` = ? AND `is_deleted` = ? AND `tenant_id` = ?").
		WithArgs(true, AnyTime{}, grantID.GrantID, grantID.Privilege, grantID.IsDeleted, grantID.TenantID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	err = grantIDTestDb.Delete(grantID.TenantID, grantID.GrantID, grantID.Privilege)
	mock.MatchExpectationsInOrder(false)
	assert.NoError(t, err)
}

func TestGrantID_Delete_Error(t *testing.T) {
	var (
		grantID *dbmodel.GrantID
		err     error
	)
	grantID = &dbmodel.GrantID{
		Base:      GetBase(),
		GrantID:   1,
		GrantorID: 10,
		Privilege: "PrivilegeLoad",
	}

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `grant_id` SET `is_deleted`=?,`updated_at`=? WHERE `grant_id`.`grant_id` = ? AND `grant_id`.`privilege` = ? AND `is_deleted` = ? AND `tenant_id` = ?").
		WithArgs(true, AnyTime{}, grantID.GrantID, grantID.Privilege, grantID.IsDeleted, grantID.TenantID).
		WillReturnError(errors.New("test error"))
	mock.ExpectCommit()
	err = grantIDTestDb.Delete(grantID.TenantID, grantID.GrantID, grantID.Privilege)
	mock.MatchExpectationsInOrder(false)
	assert.Error(t, err)
}

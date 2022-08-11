package dao

import (
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

func TestUser_GetByUsername(t *testing.T) {
	username := "test_username_1"
	var user = &dbmodel.User{
		TenantID:          tenantID,
		Username:          username,
		EncryptedPassword: "xxx",
		IsSuper:           false,
	}

	// expectation
	mock.ExpectQuery("SELECT * FROM `credential_users` WHERE tenant_id = ? AND username = ? AND is_deleted = false LIMIT 1").
		WithArgs(tenantID, username).
		WillReturnRows(
			sqlmock.NewRows([]string{"tenant_id", "username", "encrypted_password", "is_super"}).
				AddRow(user.TenantID, user.Username, user.EncryptedPassword, user.IsSuper))

	// actual
	res, err := userTestDb.GetByUsername(tenantID, username)
	assert.Nil(t, err)
	assert.Equal(t, user, res)
}

func TestUser_GetByUsername_ErrRecordNotFound(t *testing.T) {
	username := "test_username_1"

	// expectation
	mock.ExpectQuery("SELECT * FROM `credential_users` WHERE tenant_id = ? AND username = ? AND is_deleted = false LIMIT 1").
		WithArgs(tenantID, username).
		WillReturnError(gorm.ErrRecordNotFound)

	// actual
	res, err := userTestDb.GetByUsername(tenantID, username)
	assert.Nil(t, res)
	assert.Error(t, err)
}

func TestUser_GetByUsername_Error(t *testing.T) {
	username := "test_username_1"

	// expectation
	mock.ExpectQuery("SELECT * FROM `credential_users` WHERE tenant_id = ? AND username = ? AND is_deleted = false LIMIT 1").
		WithArgs(tenantID, username).
		WillReturnError(errors.New("test error"))

	// actual
	res, err := userTestDb.GetByUsername(tenantID, username)
	assert.Nil(t, res)
	assert.Error(t, err)
}

func TestUser_ListUsername(t *testing.T) {
	var usernames = []string{
		"test_username_1",
		"test_username_2",
	}

	// expectation
	mock.ExpectQuery("SELECT `username` FROM `credential_users` WHERE tenant_id = ? AND is_deleted = false").
		WithArgs(tenantID).
		WillReturnRows(
			sqlmock.NewRows([]string{"username"}).
				AddRow(usernames[0]).
				AddRow(usernames[1]))

	// actual
	res, err := userTestDb.ListUsername(tenantID)
	assert.Nil(t, err)
	assert.Equal(t, usernames, res)
}

func TestUser_ListUsername_Error(t *testing.T) {
	// expectation
	mock.ExpectQuery("SELECT `username` FROM `credential_users` WHERE tenant_id = ? AND is_deleted = false").
		WithArgs(tenantID).
		WillReturnError(errors.New("test error"))

	// actual
	res, err := userTestDb.ListUsername(tenantID)
	assert.Nil(t, res)
	assert.Error(t, err)
}

func TestUser_Insert(t *testing.T) {
	var user = &dbmodel.User{
		TenantID:          tenantID,
		Username:          "test_username",
		EncryptedPassword: "xxx",
		IsSuper:           false,
		IsDeleted:         false,
		CreatedAt:         time.Now(),
		UpdatedAt:         time.Now(),
	}

	// expectation
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `credential_users` (`tenant_id`,`username`,`encrypted_password`,`is_super`,`is_deleted`,`created_at`,`updated_at`) VALUES (?,?,?,?,?,?,?)").
		WithArgs(user.TenantID, user.Username, user.EncryptedPassword, user.IsSuper, user.IsDeleted, user.CreatedAt, user.UpdatedAt).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// actual
	err := userTestDb.Insert(user)
	assert.Nil(t, err)
}

func TestUser_Insert_Error(t *testing.T) {
	var user = &dbmodel.User{
		TenantID:          tenantID,
		Username:          "test_username",
		EncryptedPassword: "xxx",
		IsSuper:           false,
		IsDeleted:         false,
		CreatedAt:         time.Now(),
		UpdatedAt:         time.Now(),
	}

	// expectation
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `credential_users` (`tenant_id`,`username`,`encrypted_password`,`is_super`,`is_deleted`,`created_at`,`updated_at`) VALUES (?,?,?,?,?,?,?)").
		WithArgs(user.TenantID, user.Username, user.EncryptedPassword, user.IsSuper, user.IsDeleted, user.CreatedAt, user.UpdatedAt).
		WillReturnError(errors.New("test error"))
	mock.ExpectRollback()

	// actual
	err := userTestDb.Insert(user)
	assert.Error(t, err)
}

func TestUser_MarkDeletedByUsername(t *testing.T) {
	username := "test_username_1"

	// expectation
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `indexes` SET `is_deleted`=?,`updated_at`=? WHERE tenant_id = ? AND username = ?").
		WithArgs(true, AnyTime{}, tenantID, username).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// actual
	err := userTestDb.MarkDeletedByUsername(tenantID, username)
	assert.Nil(t, err)
}

func TestUser_MarkDeletedByUsername_Error(t *testing.T) {
	username := "test_username_1"

	// expectation
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `indexes` SET `is_deleted`=?,`updated_at`=? WHERE tenant_id = ? AND username = ?").
		WithArgs(true, AnyTime{}, tenantID, username).
		WillReturnError(errors.New("test error"))
	mock.ExpectRollback()

	// actual
	err := userTestDb.MarkDeletedByUsername(tenantID, username)
	assert.Error(t, err)
}

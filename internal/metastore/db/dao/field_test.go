package dao

import (
	"testing"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"github.com/stretchr/testify/assert"
)

func TestField_GetByCollID(t *testing.T) {
	var fields = []*dbmodel.Field{
		{
			TenantID:     tenantID,
			FieldID:      fieldID1,
			FieldName:    "test_field_1",
			IsPrimaryKey: false,
			Description:  "",
			DataType:     schemapb.DataType_FloatVector,
			TypeParams:   "",
			IndexParams:  "",
			AutoID:       false,
			CollectionID: collID1,
			Ts:           ts,
		},
	}

	// expectation
	mock.ExpectQuery("SELECT * FROM `field_schemas` WHERE tenant_id = ? AND collection_id = ? AND ts = ? AND is_deleted = false").
		WithArgs(tenantID, collID1, ts).
		WillReturnRows(
			sqlmock.NewRows([]string{"tenant_id", "field_id", "field_name", "is_primary_key", "description", "data_type", "type_params", "index_params", "auto_id", "collection_id", "ts"}).
				AddRow(fields[0].TenantID, fields[0].FieldID, fields[0].FieldName, fields[0].IsPrimaryKey, fields[0].Description, fields[0].DataType, fields[0].TypeParams, fields[0].IndexParams, fields[0].AutoID, fields[0].CollectionID, fields[0].Ts))

	// actual
	res, err := fieldTestDb.GetByCollectionID(tenantID, collID1, ts)
	assert.NoError(t, err)
	assert.Equal(t, fields, res)
}

func TestField_GetByCollID_Error(t *testing.T) {
	// expectation
	mock.ExpectQuery("SELECT * FROM `field_schemas` WHERE tenant_id = ? AND collection_id = ? AND ts = ? AND is_deleted = false").
		WithArgs(tenantID, collID1, ts).
		WillReturnError(errors.New("test error"))

	// actual
	res, err := fieldTestDb.GetByCollectionID(tenantID, collID1, ts)
	assert.Nil(t, res)
	assert.Error(t, err)
}

func TestField_Insert(t *testing.T) {
	var fields = []*dbmodel.Field{
		{
			TenantID:     tenantID,
			FieldID:      fieldID1,
			FieldName:    "test_field_1",
			IsPrimaryKey: false,
			Description:  "",
			DataType:     schemapb.DataType_FloatVector,
			TypeParams:   "",
			IndexParams:  "",
			AutoID:       false,
			CollectionID: collID1,
			Ts:           ts,
			IsDeleted:    false,
			CreatedAt:    time.Now(),
			UpdatedAt:    time.Now(),
		},
	}

	// expectation
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `field_schemas` (`tenant_id`,`field_id`,`field_name`,`is_primary_key`,`description`,`data_type`,`type_params`,`index_params`,`auto_id`,`collection_id`,`ts`,`is_deleted`,`created_at`,`updated_at`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)").
		WithArgs(fields[0].TenantID, fields[0].FieldID, fields[0].FieldName, fields[0].IsPrimaryKey, fields[0].Description, fields[0].DataType, fields[0].TypeParams, fields[0].IndexParams, fields[0].AutoID, fields[0].CollectionID, fields[0].Ts, fields[0].IsDeleted, fields[0].CreatedAt, fields[0].UpdatedAt).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// actual
	err := fieldTestDb.Insert(fields)
	assert.NoError(t, err)
}

func TestField_Insert_Error(t *testing.T) {
	var fields = []*dbmodel.Field{
		{
			TenantID:     tenantID,
			FieldID:      fieldID1,
			FieldName:    "test_field_1",
			IsPrimaryKey: false,
			Description:  "",
			DataType:     schemapb.DataType_FloatVector,
			TypeParams:   "",
			IndexParams:  "",
			AutoID:       false,
			CollectionID: collID1,
			Ts:           ts,
			IsDeleted:    false,
			CreatedAt:    time.Now(),
			UpdatedAt:    time.Now(),
		},
	}

	// expectation
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `field_schemas` (`tenant_id`,`field_id`,`field_name`,`is_primary_key`,`description`,`data_type`,`type_params`,`index_params`,`auto_id`,`collection_id`,`ts`,`is_deleted`,`created_at`,`updated_at`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)").
		WithArgs(fields[0].TenantID, fields[0].FieldID, fields[0].FieldName, fields[0].IsPrimaryKey, fields[0].Description, fields[0].DataType, fields[0].TypeParams, fields[0].IndexParams, fields[0].AutoID, fields[0].CollectionID, fields[0].Ts, fields[0].IsDeleted, fields[0].CreatedAt, fields[0].UpdatedAt).
		WillReturnError(errors.New("test error"))
	mock.ExpectRollback()

	// actual
	err := fieldTestDb.Insert(fields)
	assert.Error(t, err)
}

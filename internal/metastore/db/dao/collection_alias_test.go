package dao

import (
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

func TestCollectionAlias_Insert(t *testing.T) {
	var collAliases = []*dbmodel.CollectionAlias{
		{
			TenantID:        "",
			CollectionID:    collID1,
			CollectionAlias: "test_alias_1",
			Ts:              ts,
			IsDeleted:       false,
			CreatedAt:       time.Now(),
			UpdatedAt:       time.Now(),
		},
	}

	// expectation
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `collection_aliases` (`tenant_id`,`collection_id`,`collection_alias`,`ts`,`is_deleted`,`created_at`,`updated_at`) VALUES (?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE `id`=`id`").
		WithArgs(collAliases[0].TenantID, collAliases[0].CollectionID, collAliases[0].CollectionAlias, collAliases[0].Ts, collAliases[0].IsDeleted, collAliases[0].CreatedAt, collAliases[0].UpdatedAt).
		WillReturnResult(sqlmock.NewResult(100, 2))
	mock.ExpectCommit()

	// actual
	err := aliasTestDb.Insert(collAliases)
	assert.Nil(t, err)
}

func TestCollectionAlias_Insert_Error(t *testing.T) {
	var collAliases = []*dbmodel.CollectionAlias{
		{
			TenantID:        "",
			CollectionID:    collID1,
			CollectionAlias: "test_alias_1",
			Ts:              ts,
			IsDeleted:       false,
			CreatedAt:       time.Now(),
			UpdatedAt:       time.Now(),
		},
	}

	// expectation
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `collection_aliases` (`tenant_id`,`collection_id`,`collection_alias`,`ts`,`is_deleted`,`created_at`,`updated_at`) VALUES (?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE `id`=`id`").
		WithArgs(collAliases[0].TenantID, collAliases[0].CollectionID, collAliases[0].CollectionAlias, collAliases[0].Ts, collAliases[0].IsDeleted, collAliases[0].CreatedAt, collAliases[0].UpdatedAt).
		WillReturnError(errors.New("test error"))
	mock.ExpectRollback()

	// actual
	err := aliasTestDb.Insert(collAliases)
	assert.Error(t, err)
}

func TestCollectionAlias_GetCollectionIDByName(t *testing.T) {
	alias := "test_alias_name_1"

	// expectation
	mock.ExpectQuery("SELECT `collection_id` FROM `collection_aliases` WHERE tenant_id = ? AND collection_alias = ? AND ts <= ? ORDER BY ts desc LIMIT 1").
		WithArgs(tenantID, alias, ts).
		WillReturnRows(
			sqlmock.NewRows([]string{"collection_id"}).
				AddRow(collID1))

	// actual
	res, err := aliasTestDb.GetCollectionIDByAlias(tenantID, alias, ts)
	assert.Nil(t, err)
	assert.Equal(t, collID1, res)
}

func TestCollectionAlias_GetCollectionIDByName_Error(t *testing.T) {
	alias := "test_alias_name_1"

	// expectation
	mock.ExpectQuery("SELECT `collection_id` FROM `collection_aliases` WHERE tenant_id = ? AND collection_alias = ? AND ts <= ? ORDER BY ts desc LIMIT 1").
		WithArgs(tenantID, alias, ts).
		WillReturnError(errors.New("test error"))

	// actual
	res, err := aliasTestDb.GetCollectionIDByAlias(tenantID, alias, ts)
	assert.Equal(t, typeutil.UniqueID(0), res)
	assert.Error(t, err)
}

func TestCollectionAlias_GetCollectionIDByName_ErrRecordNotFound(t *testing.T) {
	alias := "test_alias_name_1"

	// expectation
	mock.ExpectQuery("SELECT `collection_id` FROM `collection_aliases` WHERE tenant_id = ? AND collection_alias = ? AND ts <= ? ORDER BY ts desc LIMIT 1").
		WithArgs(tenantID, alias, ts).
		WillReturnError(gorm.ErrRecordNotFound)

	// actual
	res, err := aliasTestDb.GetCollectionIDByAlias(tenantID, alias, ts)
	assert.Equal(t, typeutil.UniqueID(0), res)
	assert.Error(t, err)
}

func TestCollectionAlias_ListCidTs(t *testing.T) {
	var collAliases = []*dbmodel.CollectionAlias{
		{
			CollectionID: collID1,
			Ts:           typeutil.Timestamp(2),
		},
		{
			CollectionID: collID2,
			Ts:           typeutil.Timestamp(5),
		},
	}

	// expectation
	mock.ExpectQuery("SELECT collection_id, MAX(ts) ts FROM `collection_aliases` WHERE tenant_id = ? AND ts <= ? GROUP BY `collection_id`").
		WithArgs(tenantID, ts).
		WillReturnRows(
			sqlmock.NewRows([]string{"collection_id", "ts"}).
				AddRow(collID1, typeutil.Timestamp(2)).
				AddRow(collID2, typeutil.Timestamp(5)))

	// actual
	res, err := aliasTestDb.ListCollectionIDTs(tenantID, ts)
	assert.Nil(t, err)
	assert.Equal(t, collAliases, res)
}

func TestCollectionAlias_ListCidTs_Error(t *testing.T) {
	// expectation
	mock.ExpectQuery("SELECT collection_id, MAX(ts) ts FROM `collection_aliases` WHERE tenant_id = ? AND ts <= ? GROUP BY `collection_id`").
		WithArgs(tenantID, ts).
		WillReturnError(errors.New("test error"))

	// actual
	res, err := aliasTestDb.ListCollectionIDTs(tenantID, ts)
	assert.Nil(t, res)
	assert.Error(t, err)
}

func TestCollectionAlias_List(t *testing.T) {
	var cidTsPairs = []*dbmodel.CollectionAlias{
		{
			CollectionID: collID1,
			Ts:           typeutil.Timestamp(2),
		},
		{
			CollectionID: collID2,
			Ts:           typeutil.Timestamp(5),
		},
	}
	var out = []*dbmodel.CollectionAlias{
		{
			CollectionID:    collID1,
			CollectionAlias: "test_alias_1",
		},
		{
			CollectionID:    collID2,
			CollectionAlias: "test_alias_2",
		},
	}

	// expectation
	mock.ExpectQuery("SELECT collection_id, collection_alias FROM `collection_aliases` WHERE tenant_id = ? AND is_deleted = false AND (collection_id, ts) IN ((?,?),(?,?))").
		WithArgs(tenantID, cidTsPairs[0].CollectionID, cidTsPairs[0].Ts, cidTsPairs[1].CollectionID, cidTsPairs[1].Ts).
		WillReturnRows(
			sqlmock.NewRows([]string{"collection_id", "collection_alias"}).
				AddRow(collID1, "test_alias_1").
				AddRow(collID2, "test_alias_2"))

	// actual
	res, err := aliasTestDb.List(tenantID, cidTsPairs)
	assert.Nil(t, err)
	assert.Equal(t, out, res)
}

func TestCollectionAlias_List_Error(t *testing.T) {
	var cidTsPairs = []*dbmodel.CollectionAlias{
		{
			CollectionID: collID1,
			Ts:           typeutil.Timestamp(2),
		},
		{
			CollectionID: collID2,
			Ts:           typeutil.Timestamp(5),
		},
	}

	// expectation
	mock.ExpectQuery("SELECT collection_id, collection_alias FROM `collection_aliases` WHERE tenant_id = ? AND is_deleted = false AND (collection_id, ts) IN ((?,?),(?,?))").
		WithArgs(tenantID, cidTsPairs[0].CollectionID, cidTsPairs[0].Ts, cidTsPairs[1].CollectionID, cidTsPairs[1].Ts).
		WillReturnError(errors.New("test error"))

	// actual
	res, err := aliasTestDb.List(tenantID, cidTsPairs)
	assert.Nil(t, res)
	assert.Error(t, err)
}

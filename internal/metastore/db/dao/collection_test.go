package dao

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/milvus-io/milvus/internal/metastore/db/dbcore"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const (
	tenantID      = "test_tenant"
	noTs          = typeutil.Timestamp(0)
	ts            = typeutil.Timestamp(10)
	collID1       = typeutil.UniqueID(101)
	collID2       = typeutil.UniqueID(102)
	fieldID1      = typeutil.UniqueID(501)
	indexID1      = typeutil.UniqueID(1001)
	indexID2      = typeutil.UniqueID(1002)
	segmentID1    = typeutil.UniqueID(2001)
	segmentID2    = typeutil.UniqueID(2002)
	partitionID1  = typeutil.UniqueID(3001)
	indexBuildID1 = typeutil.UniqueID(5001)
	NumRows       = 1025
)

var (
	mock            sqlmock.Sqlmock
	collTestDb      dbmodel.ICollectionDb
	aliasTestDb     dbmodel.ICollAliasDb
	channelTestDb   dbmodel.ICollChannelDb
	fieldTestDb     dbmodel.IFieldDb
	partitionTestDb dbmodel.IPartitionDb
	indexTestDb     dbmodel.IIndexDb
	segIndexTestDb  dbmodel.ISegmentIndexDb
	userTestDb      dbmodel.IUserDb
	roleTestDb      dbmodel.IRoleDb
	userRoleTestDb  dbmodel.IUserRoleDb
	grantTestDb     dbmodel.IGrantDb
)

// TestMain is the first function executed in current package, we will do some initial here
func TestMain(m *testing.M) {
	var (
		db  *sql.DB
		err error
		ctx = context.TODO()
	)

	// setting sql MUST exact match
	db, mock, err = sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		panic(err)
	}

	DB, err := gorm.Open(mysql.New(mysql.Config{
		Conn:                      db,
		SkipInitializeWithVersion: true,
	}), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	// set mocked database
	dbcore.SetGlobalDB(DB)

	collTestDb = NewMetaDomain().CollectionDb(ctx)
	aliasTestDb = NewMetaDomain().CollAliasDb(ctx)
	channelTestDb = NewMetaDomain().CollChannelDb(ctx)
	fieldTestDb = NewMetaDomain().FieldDb(ctx)
	partitionTestDb = NewMetaDomain().PartitionDb(ctx)
	indexTestDb = NewMetaDomain().IndexDb(ctx)
	segIndexTestDb = NewMetaDomain().SegmentIndexDb(ctx)
	userTestDb = NewMetaDomain().UserDb(ctx)
	roleTestDb = NewMetaDomain().RoleDb(ctx)
	userRoleTestDb = NewMetaDomain().UserRoleDb(ctx)
	grantTestDb = NewMetaDomain().GrantDb(ctx)

	// m.Run entry for executing tests
	os.Exit(m.Run())
}

// Notice: sql must be exactly matched, we can use debug() to print the sql

func TestCollection_GetCidTs_Ts0(t *testing.T) {
	var collection = &dbmodel.Collection{
		CollectionID: collID1,
		Ts:           noTs,
	}

	// expectation
	mock.ExpectQuery("SELECT collection_id, ts FROM `collections` WHERE tenant_id = ? AND collection_id = ? AND ts <= ? ORDER BY ts desc LIMIT 1").
		WithArgs(tenantID, collID1, noTs).
		WillReturnRows(
			sqlmock.NewRows([]string{"collection_id", "ts"}).
				AddRow(collID1, noTs))

	// actual
	res, err := collTestDb.GetCollectionIDTs(tenantID, collID1, noTs)
	assert.Nil(t, err)
	assert.Equal(t, collection, res)
}

func TestCollection_GetCidTs_TsNot0(t *testing.T) {
	resultTs := typeutil.Timestamp(2)
	var collection = &dbmodel.Collection{
		CollectionID: collID1,
		Ts:           resultTs,
	}

	// expectation
	mock.ExpectQuery("SELECT collection_id, ts FROM `collections` WHERE tenant_id = ? AND collection_id = ? AND ts <= ? ORDER BY ts desc LIMIT 1").
		WithArgs(tenantID, collID1, ts).
		WillReturnRows(
			sqlmock.NewRows([]string{"collection_id", "ts"}).
				AddRow(collID1, resultTs))

	// actual
	res, err := collTestDb.GetCollectionIDTs(tenantID, collID1, ts)
	assert.Nil(t, err)
	assert.Equal(t, collection, res)
}

func TestCollection_GetCidTs_TsNot0_Error(t *testing.T) {
	// expectation
	mock.ExpectQuery("SELECT collection_id, ts FROM `collections` WHERE tenant_id = ? AND collection_id = ? AND ts <= ? ORDER BY ts desc LIMIT 1").
		WithArgs(tenantID, collID1, ts).
		WillReturnError(errors.New("test error"))

	// actual
	res, err := collTestDb.GetCollectionIDTs(tenantID, collID1, ts)
	assert.Nil(t, res)
	assert.Error(t, err)
}

func TestCollection_GetCidTs_TsNot0_ErrRecordNotFound(t *testing.T) {
	// expectation
	mock.ExpectQuery("SELECT collection_id, ts FROM `collections` WHERE tenant_id = ? AND collection_id = ? AND ts <= ? ORDER BY ts desc LIMIT 1").
		WithArgs(tenantID, collID1, ts).
		WillReturnError(gorm.ErrRecordNotFound)

	// actual
	res, err := collTestDb.GetCollectionIDTs(tenantID, collID1, ts)
	assert.Nil(t, res)
	assert.Error(t, err)
}

func TestCollection_ListCidTs_TsNot0(t *testing.T) {
	var collection = []*dbmodel.Collection{
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
	mock.ExpectQuery("SELECT collection_id, MAX(ts) ts FROM `collections` WHERE tenant_id = ? AND ts <= ? GROUP BY `collection_id`").
		WithArgs(tenantID, ts).
		WillReturnRows(
			sqlmock.NewRows([]string{"collection_id", "ts"}).
				AddRow(collID1, typeutil.Timestamp(2)).
				AddRow(collID2, typeutil.Timestamp(5)))

	// actual
	res, err := collTestDb.ListCollectionIDTs(tenantID, ts)
	assert.Nil(t, err)
	assert.Equal(t, collection, res)
}

func TestCollection_ListCidTs_TsNot0_Error(t *testing.T) {
	// expectation
	mock.ExpectQuery("SELECT collection_id, MAX(ts) ts FROM `collections` WHERE tenant_id = ? AND ts <= ? GROUP BY `collection_id`").
		WithArgs(tenantID, ts).
		WillReturnError(errors.New("test error"))

	// actual
	res, err := collTestDb.ListCollectionIDTs(tenantID, ts)
	assert.Nil(t, res)
	assert.Error(t, err)
}

func TestCollection_ListCidTs_Ts0(t *testing.T) {
	var collection = []*dbmodel.Collection{
		{
			CollectionID: collID1,
			Ts:           noTs,
		},
		{
			CollectionID: collID2,
			Ts:           noTs,
		},
	}

	// expectation
	mock.ExpectQuery("SELECT collection_id, MAX(ts) ts FROM `collections` WHERE tenant_id = ? AND ts <= ? GROUP BY `collection_id`").
		WithArgs(tenantID, noTs).
		WillReturnRows(
			sqlmock.NewRows([]string{"collection_id", "ts"}).
				AddRow(collID1, noTs).
				AddRow(collID2, noTs))

	// actual
	res, err := collTestDb.ListCollectionIDTs(tenantID, noTs)
	assert.Nil(t, err)
	assert.Equal(t, collection, res)
}

func TestCollection_Get(t *testing.T) {
	var collection = &dbmodel.Collection{
		TenantID:         "",
		CollectionID:     collID1,
		CollectionName:   "test_collection_name_1",
		Description:      "",
		AutoID:           false,
		ShardsNum:        int32(2),
		StartPosition:    "",
		ConsistencyLevel: int32(commonpb.ConsistencyLevel_Eventually),
		Ts:               ts,
	}

	// expectation
	mock.ExpectQuery("SELECT * FROM `collections` WHERE tenant_id = ? AND collection_id = ? AND ts = ? AND is_deleted = false LIMIT 1").
		WithArgs(tenantID, collection.CollectionID, collection.Ts).
		WillReturnRows(
			sqlmock.NewRows([]string{"tenant_id", "collection_id", "collection_name", "description", "auto_id", "shards_num", "start_position", "consistency_level", "ts"}).
				AddRow(collection.TenantID, collection.CollectionID, collection.CollectionName, collection.Description, collection.AutoID, collection.ShardsNum, collection.StartPosition, collection.ConsistencyLevel, collection.Ts))

	// actual
	res, err := collTestDb.Get(tenantID, collID1, ts)
	assert.Nil(t, err)
	assert.Equal(t, collection, res)
}

func TestCollection_Get_Error(t *testing.T) {
	var collection = &dbmodel.Collection{
		TenantID:         "",
		CollectionID:     collID1,
		CollectionName:   "test_collection_name_1",
		Description:      "",
		AutoID:           false,
		ShardsNum:        int32(2),
		StartPosition:    "",
		ConsistencyLevel: int32(commonpb.ConsistencyLevel_Eventually),
		Ts:               ts,
	}

	// expectation
	mock.ExpectQuery("SELECT * FROM `collections` WHERE tenant_id = ? AND collection_id = ? AND ts = ? AND is_deleted = false LIMIT 1").
		WithArgs(tenantID, collection.CollectionID, collection.Ts).
		WillReturnError(errors.New("test error"))

	// actual
	res, err := collTestDb.Get(tenantID, collID1, ts)
	assert.Nil(t, res)
	assert.Error(t, err)
}

func TestCollection_Get_ErrRecordNotFound(t *testing.T) {
	var collection = &dbmodel.Collection{
		TenantID:         "",
		CollectionID:     collID1,
		CollectionName:   "test_collection_name_1",
		Description:      "",
		AutoID:           false,
		ShardsNum:        int32(2),
		StartPosition:    "",
		ConsistencyLevel: int32(commonpb.ConsistencyLevel_Eventually),
		Ts:               ts,
	}

	// expectation
	mock.ExpectQuery("SELECT * FROM `collections` WHERE tenant_id = ? AND collection_id = ? AND ts = ? AND is_deleted = false LIMIT 1").
		WithArgs(tenantID, collection.CollectionID, collection.Ts).
		WillReturnError(gorm.ErrRecordNotFound)

	// actual
	res, err := collTestDb.Get(tenantID, collID1, ts)
	assert.Nil(t, res)
	assert.Error(t, err)
}

func TestCollection_GetCollectionIDByName(t *testing.T) {
	collectionName := "test_collection_name_1"

	// expectation
	mock.ExpectQuery("SELECT `collection_id` FROM `collections` WHERE tenant_id = ? AND collection_name = ? AND ts <= ? ORDER BY ts desc LIMIT 1").
		WithArgs(tenantID, collectionName, ts).
		WillReturnRows(
			sqlmock.NewRows([]string{"collection_id"}).
				AddRow(collID1))

	// actual
	res, err := collTestDb.GetCollectionIDByName(tenantID, collectionName, ts)
	assert.Nil(t, err)
	assert.Equal(t, collID1, res)
}

func TestCollection_GetCollectionIDByName_Error(t *testing.T) {
	collectionName := "test_collection_name_1"

	// expectation
	mock.ExpectQuery("SELECT `collection_id` FROM `collections` WHERE tenant_id = ? AND collection_name = ? AND ts <= ? ORDER BY ts desc LIMIT 1").
		WithArgs(tenantID, collectionName, ts).
		WillReturnError(errors.New("test error"))

	// actual
	res, err := collTestDb.GetCollectionIDByName(tenantID, collectionName, ts)
	assert.Equal(t, typeutil.UniqueID(0), res)
	assert.Error(t, err)
}

func TestCollection_GetCollectionIDByName_ErrRecordNotFound(t *testing.T) {
	collectionName := "test_collection_name_1"

	// expectation
	mock.ExpectQuery("SELECT `collection_id` FROM `collections` WHERE tenant_id = ? AND collection_name = ? AND ts <= ? ORDER BY ts desc LIMIT 1").
		WithArgs(tenantID, collectionName, ts).
		WillReturnError(gorm.ErrRecordNotFound)

	// actual
	res, err := collTestDb.GetCollectionIDByName(tenantID, collectionName, ts)
	assert.Equal(t, typeutil.UniqueID(0), res)
	assert.Error(t, err)
}

func TestCollection_Insert(t *testing.T) {
	var collection = &dbmodel.Collection{
		TenantID:         "",
		CollectionID:     collID1,
		CollectionName:   "test_collection_name_1",
		Description:      "",
		AutoID:           false,
		ShardsNum:        int32(2),
		StartPosition:    "",
		ConsistencyLevel: int32(commonpb.ConsistencyLevel_Eventually),
		Ts:               ts,
		IsDeleted:        false,
		CreatedAt:        time.Now(),
		UpdatedAt:        time.Now(),
	}

	// expectation
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `collections` (`tenant_id`,`collection_id`,`collection_name`,`description`,`auto_id`,`shards_num`,`start_position`,`consistency_level`,`ts`,`is_deleted`,`created_at`,`updated_at`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE `id`=`id`").
		WithArgs(collection.TenantID, collection.CollectionID, collection.CollectionName, collection.Description, collection.AutoID, collection.ShardsNum, collection.StartPosition, collection.ConsistencyLevel, collection.Ts, collection.IsDeleted, collection.CreatedAt, collection.UpdatedAt).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// actual
	err := collTestDb.Insert(collection)
	assert.Nil(t, err)
}

func TestCollection_Insert_Error(t *testing.T) {
	var collection = &dbmodel.Collection{
		TenantID:         "",
		CollectionID:     collID1,
		CollectionName:   "test_collection_name_1",
		Description:      "",
		AutoID:           false,
		ShardsNum:        int32(2),
		StartPosition:    "",
		ConsistencyLevel: int32(commonpb.ConsistencyLevel_Eventually),
		Ts:               ts,
		IsDeleted:        false,
		CreatedAt:        time.Now(),
		UpdatedAt:        time.Now(),
	}

	// expectation
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `collections` (`tenant_id`,`collection_id`,`collection_name`,`description`,`auto_id`,`shards_num`,`start_position`,`consistency_level`,`ts`,`is_deleted`,`created_at`,`updated_at`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE `id`=`id`").
		WithArgs(collection.TenantID, collection.CollectionID, collection.CollectionName, collection.Description, collection.AutoID, collection.ShardsNum, collection.StartPosition, collection.ConsistencyLevel, collection.Ts, collection.IsDeleted, collection.CreatedAt, collection.UpdatedAt).
		WillReturnError(errors.New("test error"))
	mock.ExpectRollback()

	// actual
	err := collTestDb.Insert(collection)
	assert.Error(t, err)
}

type AnyTime struct{}

func (a AnyTime) Match(v driver.Value) bool {
	_, ok := v.(time.Time)
	return ok
}

func GetBase() dbmodel.Base {
	return dbmodel.Base{
		TenantID:  tenantID,
		IsDeleted: false,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}

func SuccessExec(f func()) {
	mock.ExpectBegin()
	f()
	mock.ExpectCommit()
}

func ErrorExec(f func()) {
	mock.ExpectBegin()
	f()
	mock.ExpectRollback()
}

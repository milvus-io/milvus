package dao

import (
	"context"
	"database/sql"
	"database/sql/driver"
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
	noTs          = typeutil.Timestamp(0)
	collID_1      = typeutil.UniqueID(101)
	partitionID_1 = typeutil.UniqueID(500)
	fieldID_1     = typeutil.UniqueID(1000)
	indexID_1     = typeutil.UniqueID(1500)
)

var (
	mock   sqlmock.Sqlmock
	collDb dbmodel.ICollectionDb
)

// TestMain is the first function executed in current package, we will do some initial here
func TestMain(m *testing.M) {
	var (
		db  *sql.DB
		err error
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

	collDb = NewMetaDomain().CollectionDb(context.TODO())

	// m.Run entry for executing tests
	os.Exit(m.Run())
}

// Notice: sql must be exactly matched, we can use debug() to print the sql

func Test_Insert(t *testing.T) {
	var collection = &dbmodel.Collection{
		TenantID:         "",
		CollectionID:     collID_1,
		CollectionName:   "test_collection_name_1",
		Description:      "",
		AutoID:           false,
		ShardsNum:        int32(2),
		StartPosition:    "",
		ConsistencyLevel: int32(commonpb.ConsistencyLevel_Eventually),
		Ts:               noTs,
		IsDeleted:        false,
		CreatedAt:        time.Now(),
		UpdatedAt:        time.Now(),
	}

	// expectation
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `collections` (`tenant_id`,`collection_id`,`collection_name`,`description`,`auto_id`,`shards_num`,`start_position`,`consistency_level`,`ts`,`is_deleted`,`created_at`,`updated_at`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)").
		WithArgs(collection.TenantID, collection.CollectionID, collection.CollectionName, collection.Description, collection.AutoID, collection.ShardsNum, collection.StartPosition, collection.ConsistencyLevel, collection.Ts, collection.IsDeleted, collection.CreatedAt, collection.UpdatedAt).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// actual
	err := collDb.Insert(collection)
	assert.Nil(t, err)
}

func Test_Get(t *testing.T) {
	var collection = &dbmodel.Collection{
		TenantID:         "",
		CollectionID:     collID_1,
		CollectionName:   "test_collection_name_1",
		Description:      "",
		AutoID:           false,
		ShardsNum:        int32(2),
		StartPosition:    "",
		ConsistencyLevel: int32(commonpb.ConsistencyLevel_Eventually),
		Ts:               noTs,
	}

	// expectation
	mock.ExpectQuery("SELECT * FROM `collections` WHERE collection_id = ? AND ts = ? AND is_deleted = false").
		WithArgs(collection.CollectionID, collection.Ts).
		WillReturnRows(
			sqlmock.NewRows([]string{"tenant_id", "collection_id", "collection_name", "description", "auto_id", "shards_num", "start_position", "consistency_level", "ts"}).
				AddRow(collection.TenantID, collection.CollectionID, collection.CollectionName, collection.Description, collection.AutoID, collection.ShardsNum, collection.StartPosition, collection.ConsistencyLevel, collection.Ts))

	// actual
	res, err := collDb.Get("", collID_1, noTs)
	assert.Nil(t, err)
	assert.Equal(t, collection, res)
}

type AnyTime struct{}

func (a AnyTime) Match(v driver.Value) bool {
	_, ok := v.(time.Time)
	return ok
}

func Test_MarkDeleted(t *testing.T) {
	// expectation
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `collections` SET `is_deleted`=?,`updated_at`=? WHERE collection_id = ? AND ts = 0").
		WithArgs(true, AnyTime{}, collID_1).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// actual
	err := collDb.MarkDeleted("", collID_1)
	assert.Nil(t, err)
}

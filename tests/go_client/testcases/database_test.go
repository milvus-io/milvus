package testcases

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// teardownTest
func teardownTest(t *testing.T) func(t *testing.T) {
	log.Info("setup test func")
	return func(t *testing.T) {
		log.Info("teardown func drop all non-default db")
		// drop all db
		ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
		mc := createDefaultMilvusClient(ctx, t)
		dbs, _ := mc.ListDatabase(ctx, client.NewListDatabaseOption())
		for _, db := range dbs {
			if db != common.DefaultDb {
				_ = mc.UseDatabase(ctx, client.NewUseDatabaseOption(db))
				collections, _ := mc.ListCollections(ctx, client.NewListCollectionOption())
				for _, coll := range collections {
					_ = mc.DropCollection(ctx, client.NewDropCollectionOption(coll))
				}
				_ = mc.DropDatabase(ctx, client.NewDropDatabaseOption(db))
			}
		}
	}
}

func TestDatabase(t *testing.T) {
	teardownSuite := teardownTest(t)
	defer teardownSuite(t)

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	clientDefault := createMilvusClient(ctx, t, &defaultCfg)

	// create db1
	dbName1 := common.GenRandomString("db1", 4)
	err := clientDefault.CreateDatabase(ctx, client.NewCreateDatabaseOption(dbName1))
	common.CheckErr(t, err, true)

	// list db and verify db1 in dbs
	dbs, errList := clientDefault.ListDatabase(ctx, client.NewListDatabaseOption())
	common.CheckErr(t, errList, true)
	require.Containsf(t, dbs, dbName1, fmt.Sprintf("%s db not in dbs: %v", dbName1, dbs))

	// new client with db1
	clientDB1 := createMilvusClient(ctx, t, &client.ClientConfig{Address: *addr, DBName: dbName1})

	// create collections -> verify collections contains
	_, db1Col1 := hp.CollPrepare.CreateCollection(ctx, t, clientDB1, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	_, db1Col2 := hp.CollPrepare.CreateCollection(ctx, t, clientDB1, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	collections, errListCollections := clientDB1.ListCollections(ctx, client.NewListCollectionOption())
	common.CheckErr(t, errListCollections, true)
	require.Containsf(t, collections, db1Col1.CollectionName, fmt.Sprintf("The collection %s not in: %v", db1Col1.CollectionName, collections))
	require.Containsf(t, collections, db1Col2.CollectionName, fmt.Sprintf("The collection %s not in: %v", db1Col2.CollectionName, collections))

	// create db2
	dbName2 := common.GenRandomString("db2", 4)
	err = clientDefault.CreateDatabase(ctx, client.NewCreateDatabaseOption(dbName2))
	common.CheckErr(t, err, true)
	dbs, err = clientDefault.ListDatabase(ctx, client.NewListDatabaseOption())
	common.CheckErr(t, err, true)
	require.Containsf(t, dbs, dbName2, fmt.Sprintf("%s db not in dbs: %v", dbName2, dbs))

	// using db2 -> create collection -> drop collection
	err = clientDefault.UseDatabase(ctx, client.NewUseDatabaseOption(dbName2))
	common.CheckErr(t, err, true)
	_, db2Col1 := hp.CollPrepare.CreateCollection(ctx, t, clientDefault, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	err = clientDefault.DropCollection(ctx, client.NewDropCollectionOption(db2Col1.CollectionName))
	common.CheckErr(t, err, true)

	// using empty db -> drop db2
	clientDefault.UseDatabase(ctx, client.NewUseDatabaseOption(""))
	err = clientDefault.DropDatabase(ctx, client.NewDropDatabaseOption(dbName2))
	common.CheckErr(t, err, true)

	// list db and verify db drop success
	dbs, err = clientDefault.ListDatabase(ctx, client.NewListDatabaseOption())
	common.CheckErr(t, err, true)
	require.NotContains(t, dbs, dbName2)

	// drop db1 which has some collections
	err = clientDB1.DropDatabase(ctx, client.NewDropDatabaseOption(dbName1))
	common.CheckErr(t, err, false, "must drop all collections before drop database")

	// drop all db1's collections -> drop db1
	clientDB1.UseDatabase(ctx, client.NewUseDatabaseOption(dbName1))
	err = clientDB1.DropCollection(ctx, client.NewDropCollectionOption(db1Col1.CollectionName))
	common.CheckErr(t, err, true)

	err = clientDB1.DropCollection(ctx, client.NewDropCollectionOption(db1Col2.CollectionName))
	common.CheckErr(t, err, true)

	err = clientDB1.DropDatabase(ctx, client.NewDropDatabaseOption(dbName1))
	common.CheckErr(t, err, true)

	// drop default db
	err = clientDefault.DropDatabase(ctx, client.NewDropDatabaseOption(common.DefaultDb))
	common.CheckErr(t, err, false, "can not drop default database")

	dbs, err = clientDefault.ListDatabase(ctx, client.NewListDatabaseOption())
	common.CheckErr(t, err, true)
	require.Containsf(t, dbs, common.DefaultDb, fmt.Sprintf("The db %s not in: %v", common.DefaultDb, dbs))
}

// test create with invalid db name
func TestCreateDb(t *testing.T) {
	teardownSuite := teardownTest(t)
	defer teardownSuite(t)

	// create db
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)
	dbName := common.GenRandomString("db", 4)
	err := mc.CreateDatabase(ctx, client.NewCreateDatabaseOption(dbName))
	common.CheckErr(t, err, true)

	// create existed db
	err = mc.CreateDatabase(ctx, client.NewCreateDatabaseOption(dbName))
	common.CheckErr(t, err, false, fmt.Sprintf("database already exist: %s", dbName))

	// create default db
	err = mc.CreateDatabase(ctx, client.NewCreateDatabaseOption(common.DefaultDb))
	common.CheckErr(t, err, false, fmt.Sprintf("database already exist: %s", common.DefaultDb))

	emptyErr := mc.CreateDatabase(ctx, client.NewCreateDatabaseOption(""))
	common.CheckErr(t, emptyErr, false, "database name couldn't be empty")
}

// test drop db
func TestDropDb(t *testing.T) {
	teardownSuite := teardownTest(t)
	defer teardownSuite(t)

	// create collection in default db
	listCollOpt := client.NewListCollectionOption()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)
	_, defCol := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	collections, _ := mc.ListCollections(ctx, listCollOpt)
	require.Contains(t, collections, defCol.CollectionName)

	// create db
	dbName := common.GenRandomString("db", 4)
	err := mc.CreateDatabase(ctx, client.NewCreateDatabaseOption(dbName))
	common.CheckErr(t, err, true)

	// using db and drop the db
	err = mc.UseDatabase(ctx, client.NewUseDatabaseOption(dbName))
	common.CheckErr(t, err, true)
	err = mc.DropDatabase(ctx, client.NewDropDatabaseOption(dbName))
	common.CheckErr(t, err, true)

	// verify current db
	_, err = mc.ListCollections(ctx, listCollOpt)
	common.CheckErr(t, err, false, fmt.Sprintf("database not found[database=%s]", dbName))

	// using default db and verify collections
	err = mc.UseDatabase(ctx, client.NewUseDatabaseOption(common.DefaultDb))
	common.CheckErr(t, err, true)
	collections, _ = mc.ListCollections(ctx, listCollOpt)
	require.Contains(t, collections, defCol.CollectionName)

	// drop not existed db
	err = mc.DropDatabase(ctx, client.NewDropDatabaseOption(common.GenRandomString("db", 4)))
	common.CheckErr(t, err, true)

	// drop empty db
	err = mc.DropDatabase(ctx, client.NewDropDatabaseOption(""))
	common.CheckErr(t, err, false, "database name couldn't be empty")

	// drop default db
	err = mc.DropDatabase(ctx, client.NewDropDatabaseOption(common.DefaultDb))
	common.CheckErr(t, err, false, "can not drop default database")
}

// test using db
func TestUsingDb(t *testing.T) {
	teardownSuite := teardownTest(t)
	defer teardownSuite(t)

	// create collection in default db
	listCollOpt := client.NewListCollectionOption()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	_, col := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	// collName := createDefaultCollection(ctx, t, mc, true, common.DefaultShards)
	collections, _ := mc.ListCollections(ctx, listCollOpt)
	require.Contains(t, collections, col.CollectionName)

	// using not existed db
	dbName := common.GenRandomString("db", 4)
	err := mc.UseDatabase(ctx, client.NewUseDatabaseOption(dbName))
	common.CheckErr(t, err, false, fmt.Sprintf("database not found[database=%s]", dbName))

	// using empty db
	err = mc.UseDatabase(ctx, client.NewUseDatabaseOption(""))
	common.CheckErr(t, err, true)
	collections, _ = mc.ListCollections(ctx, listCollOpt)
	require.Contains(t, collections, col.CollectionName)

	// using current db
	err = mc.UseDatabase(ctx, client.NewUseDatabaseOption(common.DefaultDb))
	common.CheckErr(t, err, true)
	collections, _ = mc.ListCollections(ctx, listCollOpt)
	require.Contains(t, collections, col.CollectionName)
}

func TestClientWithDb(t *testing.T) {
	teardownSuite := teardownTest(t)
	defer teardownSuite(t)

	listCollOpt := client.NewListCollectionOption()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)

	// connect with not existed db
	_, err := base.NewMilvusClient(ctx, &client.ClientConfig{Address: *addr, DBName: "dbName"})
	common.CheckErr(t, err, false, "database not found")

	// connect default db -> create a collection in default db
	mcDefault, errDefault := base.NewMilvusClient(ctx, &client.ClientConfig{
		Address: *addr,
		// DBName:  common.DefaultDb,
	})
	common.CheckErr(t, errDefault, true)
	_, defCol1 := hp.CollPrepare.CreateCollection(ctx, t, mcDefault, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	defCollections, _ := mcDefault.ListCollections(ctx, listCollOpt)
	require.Contains(t, defCollections, defCol1.CollectionName)
	log.Debug("default db collections:", zap.Any("default collections", defCollections))

	// create a db and create collection in db
	dbName := common.GenRandomString("db", 5)
	err = mcDefault.CreateDatabase(ctx, client.NewCreateDatabaseOption(dbName))
	common.CheckErr(t, err, true)

	// and connect with db
	mcDb, err := base.NewMilvusClient(ctx, &client.ClientConfig{
		Address: *addr,
		DBName:  dbName,
	})
	common.CheckErr(t, err, true)
	_, dbCol1 := hp.CollPrepare.CreateCollection(ctx, t, mcDb, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())

	dbCollections, _ := mcDb.ListCollections(ctx, listCollOpt)
	log.Debug("db collections:", zap.Any("db collections", dbCollections))
	require.Containsf(t, dbCollections, dbCol1.CollectionName, fmt.Sprintf("The collection %s not in: %v", dbCol1.CollectionName, dbCollections))

	// using default db and collection not in
	_ = mcDb.UseDatabase(ctx, client.NewUseDatabaseOption(common.DefaultDb))
	defCollections, _ = mcDb.ListCollections(ctx, listCollOpt)
	require.NotContains(t, defCollections, dbCol1.CollectionName)

	// connect empty db (actually default db)
	mcEmpty, err := base.NewMilvusClient(ctx, &client.ClientConfig{
		Address: *addr,
		DBName:  "",
	})
	common.CheckErr(t, err, true)
	defCollections, _ = mcEmpty.ListCollections(ctx, listCollOpt)
	require.Contains(t, defCollections, defCol1.CollectionName)
}

func TestDatabasePropertiesCollectionsNum(t *testing.T) {
	// create db with properties
	teardownSuite := teardownTest(t)
	defer teardownSuite(t)

	// create db
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)
	dbName := common.GenRandomString("db", 4)
	err := mc.CreateDatabase(ctx, client.NewCreateDatabaseOption(dbName))
	common.CheckErr(t, err, true)

	// alter database properties
	maxCollections := 2
	err = mc.AlterDatabaseProperties(ctx, client.NewAlterDatabasePropertiesOption(dbName).WithProperty(common.DatabaseMaxCollections, maxCollections))
	common.CheckErr(t, err, true)

	// describe database
	db, _ := mc.DescribeDatabase(ctx, client.NewDescribeDatabaseOption(dbName))
	require.Equal(t, map[string]string{common.DatabaseMaxCollections: strconv.Itoa(maxCollections)}, db.Properties)
	require.Equal(t, dbName, db.Name)

	// verify properties works
	mc.UseDatabase(ctx, client.NewUseDatabaseOption(dbName))
	var collections []string
	for i := 0; i < maxCollections; i++ {
		_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
		collections = append(collections, schema.CollectionName)
	}
	fields := hp.FieldsFact.GenFieldsForCollection(hp.Int64Vec, hp.TNewFieldsOption())
	schema := hp.GenSchema(hp.TNewSchemaOption().TWithFields(fields))
	err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(schema.CollectionName, schema))
	common.CheckErr(t, err, false, "exceeded the limit number of collections")

	// Other db are not restricted by this property
	mc.UseDatabase(ctx, client.NewUseDatabaseOption(common.DefaultDb))
	for i := 0; i < maxCollections+1; i++ {
		hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	}

	// drop properties
	mc.UseDatabase(ctx, client.NewUseDatabaseOption(dbName))
	errDrop := mc.DropDatabaseProperties(ctx, client.NewDropDatabasePropertiesOption(dbName, common.DatabaseMaxCollections))
	common.CheckErr(t, errDrop, true)
	_, schema1 := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	collections = append(collections, schema1.CollectionName)

	// verify collection num
	collectionsList, _ := mc.ListCollections(ctx, client.NewListCollectionOption())
	require.Subset(t, collectionsList, collections)
	require.GreaterOrEqual(t, len(collectionsList), maxCollections)

	// describe database after drop properties
	db, _ = mc.DescribeDatabase(ctx, client.NewDescribeDatabaseOption(dbName))
	require.Equal(t, map[string]string{}, db.Properties)
	require.Equal(t, dbName, db.Name)
}

func TestDatabasePropertiesRgReplicas(t *testing.T) {
	// create db with properties
	teardownSuite := teardownTest(t)
	defer teardownSuite(t)

	// create db
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)
	dbName := common.GenRandomString("db", 4)
	err := mc.CreateDatabase(ctx, client.NewCreateDatabaseOption(dbName))
	common.CheckErr(t, err, true)

	// alter database properties
	err = mc.AlterDatabaseProperties(ctx, client.NewAlterDatabasePropertiesOption(dbName).
		WithProperty(common.DatabaseResourceGroups, "rg1").WithProperty(common.DatabaseReplicaNumber, 2))
	common.CheckErr(t, err, true)

	// describe database
	db, _ := mc.DescribeDatabase(ctx, client.NewDescribeDatabaseOption(dbName))
	require.Equal(t, map[string]string{common.DatabaseResourceGroups: "rg1", common.DatabaseReplicaNumber: "2"}, db.Properties)

	mc.UseDatabase(ctx, client.NewUseDatabaseOption(dbName))
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(1000))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))

	_, err = mc.LoadCollection(ctx, client.NewLoadCollectionOption(schema.CollectionName))
	common.CheckErr(t, err, true)

	_, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithLimit(10))
	common.CheckErr(t, err, true)
}

func TestDatabasePropertyDeny(t *testing.T) {
	t.Skip("https://zilliz.atlassian.net/browse/VDC-7858")
	// create db with properties
	teardownSuite := teardownTest(t)
	defer teardownSuite(t)

	// create db and use db
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)
	dbName := common.GenRandomString("db", 4)
	err := mc.CreateDatabase(ctx, client.NewCreateDatabaseOption(dbName))
	common.CheckErr(t, err, true)

	// alter database properties and check
	err = mc.AlterDatabaseProperties(ctx, client.NewAlterDatabasePropertiesOption(dbName).
		WithProperty(common.DatabaseForceDenyWriting, true).
		WithProperty(common.DatabaseForceDenyReading, true))
	common.CheckErr(t, err, true)
	db, _ := mc.DescribeDatabase(ctx, client.NewDescribeDatabaseOption(dbName))
	require.Equal(t, map[string]string{common.DatabaseForceDenyWriting: "true", common.DatabaseForceDenyReading: "true"}, db.Properties)

	err = mc.UseDatabase(ctx, client.NewUseDatabaseOption(dbName))
	common.CheckErr(t, err, true)

	// prepare collection: create -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// reading
	_, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithLimit(10))
	common.CheckErr(t, err, false, "access has been disabled by the administrator")

	// writing
	columns, _ := hp.GenColumnsBasedSchema(schema, hp.TNewDataOption().TWithNb(10))
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName, columns...))
	common.CheckErr(t, err, false, "access has been disabled by the administrator")
}

func TestDatabaseFakeProperties(t *testing.T) {
	// create db with properties
	teardownSuite := teardownTest(t)
	defer teardownSuite(t)

	// create db
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)
	dbName := common.GenRandomString("db", 4)
	err := mc.CreateDatabase(ctx, client.NewCreateDatabaseOption(dbName))
	common.CheckErr(t, err, true)

	// alter database with useless properties
	properties := map[string]any{
		"key_1": 1,
		"key2":  1.9,
		"key-3": true,
		"key.4": "a.b.c",
	}
	for key, value := range properties {
		err = mc.AlterDatabaseProperties(ctx, client.NewAlterDatabasePropertiesOption(dbName).WithProperty(key, value))
		common.CheckErr(t, err, true)
	}

	// describe database
	db, _ := mc.DescribeDatabase(ctx, client.NewDescribeDatabaseOption(dbName))
	require.EqualValues(t, map[string]string{"key_1": "1", "key2": "1.9", "key-3": "true", "key.4": "a.b.c"}, db.Properties)

	// drop database properties
	err = mc.DropDatabaseProperties(ctx, client.NewDropDatabasePropertiesOption(dbName, "aaa"))
	common.CheckErr(t, err, true)
}

package testcases

import (
	"fmt"
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
		dbs, _ := mc.ListDatabases(ctx, client.NewListDatabaseOption())
		for _, db := range dbs {
			if db != common.DefaultDb {
				_ = mc.UsingDatabase(ctx, client.NewUseDatabaseOption(db))
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
	dbs, errList := clientDefault.ListDatabases(ctx, client.NewListDatabaseOption())
	common.CheckErr(t, errList, true)
	require.Containsf(t, dbs, dbName1, fmt.Sprintf("%s db not in dbs: %v", dbName1, dbs))

	// new client with db1 -> using db
	clientDB1 := createMilvusClient(ctx, t, &client.ClientConfig{Address: *addr, DBName: dbName1})
	t.Log("https://github.com/milvus-io/milvus/issues/34137")
	err = clientDB1.UsingDatabase(ctx, client.NewUseDatabaseOption(dbName1))
	common.CheckErr(t, err, true)

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
	dbs, err = clientDefault.ListDatabases(ctx, client.NewListDatabaseOption())
	common.CheckErr(t, err, true)
	require.Containsf(t, dbs, dbName2, fmt.Sprintf("%s db not in dbs: %v", dbName2, dbs))

	// using db2 -> create collection -> drop collection
	err = clientDefault.UsingDatabase(ctx, client.NewUseDatabaseOption(dbName2))
	common.CheckErr(t, err, true)
	_, db2Col1 := hp.CollPrepare.CreateCollection(ctx, t, clientDefault, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	err = clientDefault.DropCollection(ctx, client.NewDropCollectionOption(db2Col1.CollectionName))
	common.CheckErr(t, err, true)

	// using empty db -> drop db2
	clientDefault.UsingDatabase(ctx, client.NewUseDatabaseOption(""))
	err = clientDefault.DropDatabase(ctx, client.NewDropDatabaseOption(dbName2))
	common.CheckErr(t, err, true)

	// list db and verify db drop success
	dbs, err = clientDefault.ListDatabases(ctx, client.NewListDatabaseOption())
	common.CheckErr(t, err, true)
	require.NotContains(t, dbs, dbName2)

	// drop db1 which has some collections
	err = clientDB1.DropDatabase(ctx, client.NewDropDatabaseOption(dbName1))
	common.CheckErr(t, err, false, "must drop all collections before drop database")

	// drop all db1's collections -> drop db1
	clientDB1.UsingDatabase(ctx, client.NewUseDatabaseOption(dbName1))
	err = clientDB1.DropCollection(ctx, client.NewDropCollectionOption(db1Col1.CollectionName))
	common.CheckErr(t, err, true)

	err = clientDB1.DropCollection(ctx, client.NewDropCollectionOption(db1Col2.CollectionName))
	common.CheckErr(t, err, true)

	err = clientDB1.DropDatabase(ctx, client.NewDropDatabaseOption(dbName1))
	common.CheckErr(t, err, true)

	// drop default db
	err = clientDefault.DropDatabase(ctx, client.NewDropDatabaseOption(common.DefaultDb))
	common.CheckErr(t, err, false, "can not drop default database")

	dbs, err = clientDefault.ListDatabases(ctx, client.NewListDatabaseOption())
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
	err = mc.UsingDatabase(ctx, client.NewUseDatabaseOption(dbName))
	common.CheckErr(t, err, true)
	err = mc.DropDatabase(ctx, client.NewDropDatabaseOption(dbName))
	common.CheckErr(t, err, true)

	// verify current db
	_, err = mc.ListCollections(ctx, listCollOpt)
	common.CheckErr(t, err, false, fmt.Sprintf("database not found[database=%s]", dbName))

	// using default db and verify collections
	err = mc.UsingDatabase(ctx, client.NewUseDatabaseOption(common.DefaultDb))
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
	err := mc.UsingDatabase(ctx, client.NewUseDatabaseOption(dbName))
	common.CheckErr(t, err, false, fmt.Sprintf("database not found[database=%s]", dbName))

	// using empty db
	err = mc.UsingDatabase(ctx, client.NewUseDatabaseOption(""))
	common.CheckErr(t, err, true)
	collections, _ = mc.ListCollections(ctx, listCollOpt)
	require.Contains(t, collections, col.CollectionName)

	// using current db
	err = mc.UsingDatabase(ctx, client.NewUseDatabaseOption(common.DefaultDb))
	common.CheckErr(t, err, true)
	collections, _ = mc.ListCollections(ctx, listCollOpt)
	require.Contains(t, collections, col.CollectionName)
}

func TestClientWithDb(t *testing.T) {
	t.Skip("https://github.com/milvus-io/milvus/issues/34137")
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
	_ = mcDb.UsingDatabase(ctx, client.NewUseDatabaseOption(common.DefaultDb))
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

func TestAlterDatabase(t *testing.T) {
	t.Skip("waiting for AlterDatabase and DescribeDatabase")
}

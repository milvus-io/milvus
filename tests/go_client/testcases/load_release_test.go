package testcases

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	clientv2 "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// test load collection
func TestLoadCollection(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))

	// load
	loadTask, err := mc.LoadCollection(ctx, clientv2.NewLoadCollectionOption(schema.CollectionName))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	// describe collection
	coll, err := mc.DescribeCollection(ctx, clientv2.NewDescribeCollectionOption(schema.CollectionName))
	common.CheckErr(t, err, true)

	t.Log("https://github.com/milvus-io/milvus/issues/34149")
	log.Debug("collection", zap.Bool("loaded", coll.Loaded))

	res, err := mc.Query(ctx, clientv2.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	count, _ := res.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
	require.EqualValues(t, common.DefaultNb, count)
}

// test load not existed collection
func TestLoadCollectionNotExist(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	// connect
	mc := createDefaultMilvusClient(ctx, t)

	// Load collection
	_, errLoad := mc.LoadCollection(ctx, clientv2.NewLoadCollectionOption("collName"))
	common.CheckErr(t, errLoad, false, "collection not found[database=default][collection=collName]")
}

// test load collection async
func TestLoadCollectionAsync(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))

	// load
	_, err := mc.LoadCollection(ctx, clientv2.NewLoadCollectionOption(schema.CollectionName))
	common.CheckErr(t, err, true)

	// query
	res, err := mc.Query(ctx, clientv2.NewQueryOption(schema.CollectionName).WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	count, _ := res.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
	require.LessOrEqual(t, count, int64(common.DefaultNb*2))
}

// load collection without index
func TestLoadCollectionWithoutIndex(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())

	// load
	_, errLoad := mc.LoadCollection(ctx, clientv2.NewLoadCollectionOption(schema.CollectionName))
	common.CheckErr(t, errLoad, false, "index not found")

	// load partitions without index
	_, errLoadPartition := mc.LoadPartitions(ctx, clientv2.NewLoadPartitionsOption(schema.CollectionName, common.DefaultPartition))
	common.CheckErr(t, errLoadPartition, false, "index not found")
}

// load collection with multi partitions
func TestLoadCollectionMultiPartitions(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	parName := common.GenRandomString("p", 4)

	// create collection and partition
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	err := mc.CreatePartition(ctx, clientv2.NewCreatePartitionOption(schema.CollectionName, parName))
	common.CheckErr(t, err, true)

	// insert [0, nb) into default partition, [nb, nb*2) into new partition
	_defVec := hp.GenColumnData(common.DefaultNb, entity.FieldTypeFloatVector, *hp.TNewDataOption())
	_defPk := hp.GenColumnData(common.DefaultNb, entity.FieldTypeInt64, *hp.TNewDataOption())
	insertRes1, err1 := mc.Insert(ctx, clientv2.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(_defPk, _defVec))
	common.CheckErr(t, err1, true)
	require.EqualValues(t, common.DefaultNb, insertRes1.InsertCount)

	_parPk := hp.GenColumnData(common.DefaultNb, entity.FieldTypeInt64, *hp.TNewDataOption().TWithStart(common.DefaultNb))
	_parVec := hp.GenColumnData(common.DefaultNb, entity.FieldTypeFloatVector, *hp.TNewDataOption().TWithStart(common.DefaultNb))
	insertRes2, err2 := mc.Insert(ctx, clientv2.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(_parPk, _parVec).WithPartition(parName))
	common.CheckErr(t, err2, true)
	require.EqualValues(t, common.DefaultNb, insertRes2.InsertCount)

	// create index
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))

	// load default partition
	taskDef, errDef := mc.LoadPartitions(ctx, clientv2.NewLoadPartitionsOption(schema.CollectionName, common.DefaultPartition))
	common.CheckErr(t, errDef, true)
	err = taskDef.Await(ctx)
	common.CheckErr(t, err, true)

	// query from parName -> error
	_, err = mc.Query(ctx, clientv2.NewQueryOption(schema.CollectionName).WithOutputFields(common.QueryCountFieldName).WithPartitions(parName))
	log.Debug("error", zap.Error(err))
	common.CheckErr(t, err, false, "partition not loaded")

	// query count(*) from default partition
	res, err := mc.Query(ctx, clientv2.NewQueryOption(schema.CollectionName).WithOutputFields(common.QueryCountFieldName).WithPartitions(common.DefaultPartition).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ := res.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
	require.Equal(t, int64(common.DefaultNb), count)

	// load parName partitions
	taskPar, errPar := mc.LoadPartitions(ctx, clientv2.NewLoadPartitionsOption(schema.CollectionName, parName))
	common.CheckErr(t, errPar, true)
	err = taskPar.Await(ctx)
	common.CheckErr(t, err, true)

	// query count(*) from all partitions
	res, err = mc.Query(ctx, clientv2.NewQueryOption(schema.CollectionName).WithOutputFields(common.QueryCountFieldName).WithPartitions(parName, common.DefaultPartition).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ = res.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
	require.Equal(t, int64(common.DefaultNb*2), count)

	// release collection -> load all partitions -> query count(*) all partitions 6000
	mc.ReleaseCollection(ctx, clientv2.NewReleaseCollectionOption(schema.CollectionName))
	mc.LoadPartitions(ctx, clientv2.NewLoadPartitionsOption(schema.CollectionName, parName, common.DefaultPartition))
	res, err = mc.Query(ctx, clientv2.NewQueryOption(schema.CollectionName).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ = res.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
	require.Equal(t, int64(common.DefaultNb*2), count)
}

// test load repeated partition names
func TestLoadPartitionsRepeatedly(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	parName := common.GenRandomString("p", 4)

	// create collection and partition
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	err := mc.CreatePartition(ctx, clientv2.NewCreatePartitionOption(schema.CollectionName, parName))
	common.CheckErr(t, err, true)

	_parPk := hp.GenColumnData(common.DefaultNb, entity.FieldTypeInt64, *hp.TNewDataOption().TWithStart(common.DefaultNb))
	_parVec := hp.GenColumnData(common.DefaultNb, entity.FieldTypeFloatVector, *hp.TNewDataOption().TWithStart(common.DefaultNb))
	insertRes2, err2 := mc.Insert(ctx, clientv2.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(_parPk, _parVec).WithPartition(parName))
	common.CheckErr(t, err2, true)
	require.EqualValues(t, common.DefaultNb, insertRes2.InsertCount)

	// create index
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))

	// load partition with repeated names
	taskDef, errDef := mc.LoadPartitions(ctx, clientv2.NewLoadPartitionsOption(schema.CollectionName, parName))
	common.CheckErr(t, errDef, true)
	err = taskDef.Await(ctx)
	common.CheckErr(t, err, true)

	// query count(*) from default partition
	res, err := mc.Query(ctx, clientv2.NewQueryOption(schema.CollectionName).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ := res.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
	require.EqualValues(t, common.DefaultNb, count)
}

func TestLoadMultiVectorsIndex(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create collection -> insert -> flush -> index
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64MultiVec), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	hnswIdx := index.NewHNSWIndex(entity.IP, 8, 200)

	for _, fieldName := range []string{common.DefaultFloatVecFieldName, common.DefaultFloat16VecFieldName, common.DefaultBFloat16VecFieldName} {
		idxTask, err := mc.CreateIndex(ctx, clientv2.NewCreateIndexOption(schema.CollectionName, fieldName, hnswIdx))
		common.CheckErr(t, err, true)
		err = idxTask.Await(ctx)
		common.CheckErr(t, err, true)
	}

	_, err := mc.LoadCollection(ctx, clientv2.NewLoadCollectionOption(schema.CollectionName))
	common.CheckErr(t, err, false, "there is no vector index on field")
}

func TestLoadCollectionAllFields(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create collection -> insert -> flush -> index
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.AllFields), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))

	// load collection
	loadTask, err := mc.LoadCollection(ctx, clientv2.NewLoadCollectionOption(schema.CollectionName))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	// query count(*)
	res, err := mc.Query(ctx, clientv2.NewQueryOption(schema.CollectionName).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ := res.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
	require.EqualValues(t, common.DefaultNb, count)
}

func TestLoadCollectionSparse(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create collection -> insert -> flush -> index
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithSparseMaxLen(200))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))

	// load collection
	loadTask, err := mc.LoadCollection(ctx, clientv2.NewLoadCollectionOption(schema.CollectionName))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	// query count(*)
	res, err := mc.Query(ctx, clientv2.NewQueryOption(schema.CollectionName).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ := res.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
	require.EqualValues(t, common.DefaultNb, count)
}

func TestReleaseCollection(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(100))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// release collection
	err := mc.ReleaseCollection(ctx, clientv2.NewReleaseCollectionOption(schema.CollectionName))
	common.CheckErr(t, err, true)

	// query count(*) -> error
	_, err = mc.Query(ctx, clientv2.NewQueryOption(schema.CollectionName).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, false, "collection not loaded")
}

func TestReleaseCollectionNotExist(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	// connect
	mc := createDefaultMilvusClient(ctx, t)

	// Load collection
	errLoad := mc.ReleaseCollection(ctx, clientv2.NewReleaseCollectionOption("collName"))
	common.CheckErr(t, errLoad, false, "collection not found[database=default][collection=collName]")
}

func TestReleasePartitions(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	parName := common.GenRandomString("p", 4)
	nb := 100

	// create collection and partition
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	err := mc.CreatePartition(ctx, clientv2.NewCreatePartitionOption(schema.CollectionName, parName))
	common.CheckErr(t, err, true)

	// insert [0, nb) into default partition and new partition
	_defVec := hp.GenColumnData(nb, entity.FieldTypeFloatVector, *hp.TNewDataOption())
	_defPk := hp.GenColumnData(nb, entity.FieldTypeInt64, *hp.TNewDataOption())
	insertRes1, err1 := mc.Insert(ctx, clientv2.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(_defPk, _defVec))
	common.CheckErr(t, err1, true)
	require.EqualValues(t, nb, insertRes1.InsertCount)

	insertRes2, err2 := mc.Insert(ctx, clientv2.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(_defVec, _defPk).WithPartition(parName))
	common.CheckErr(t, err2, true)
	require.EqualValues(t, nb, insertRes2.InsertCount)

	// create index
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// release partition
	errRelease := mc.ReleasePartitions(ctx, clientv2.NewReleasePartitionsOptions(schema.CollectionName, parName, common.DefaultPartition))
	common.CheckErr(t, errRelease, true)

	// check release success
	_, errQuery := mc.Query(ctx, clientv2.NewQueryOption(schema.CollectionName).WithOutputFields(common.QueryCountFieldName).WithPartitions(parName))
	common.CheckErr(t, errQuery, false, "collection not loaded")
}

func TestReleasePartitionsNotExist(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	// connect
	mc := createDefaultMilvusClient(ctx, t)

	// Load collection
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	errLoad := mc.ReleasePartitions(ctx, clientv2.NewReleasePartitionsOptions(schema.CollectionName, "parName"))
	common.CheckErr(t, errLoad, false, "partition not found")
}

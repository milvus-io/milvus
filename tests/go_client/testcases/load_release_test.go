package testcases

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	clientv2 "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// test load collection
func TestLoadCollection(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

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
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// Load collection
	_, errLoad := mc.LoadCollection(ctx, clientv2.NewLoadCollectionOption("collName"))
	common.CheckErr(t, errLoad, false, "collection not found[database=default][collection=collName]")
}

// test load collection async
func TestLoadCollectionAsync(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

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
	mc := hp.CreateDefaultMilvusClient(ctx, t)

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
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	parName := common.GenRandomString("p", 4)

	// create collection and partition
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	err := mc.CreatePartition(ctx, clientv2.NewCreatePartitionOption(schema.CollectionName, parName))
	common.CheckErr(t, err, true)

	// insert [0, nb) into default partition, [nb, nb*2) into new partition
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(common.DefaultNb))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema).TWithPartitionName(parName), hp.TNewDataOption().TWithNb(common.DefaultNb).TWithStart(common.DefaultNb))

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
	mc := hp.CreateDefaultMilvusClient(ctx, t)

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
	mc := hp.CreateDefaultMilvusClient(ctx, t)

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
	mc := hp.CreateDefaultMilvusClient(ctx, t)

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
	mc := hp.CreateDefaultMilvusClient(ctx, t)

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

func TestLoadPartialFields(t *testing.T) {
	/*
		1.  verify the collection loaded successfully
		2.  verify the loaded fields can be searched in expr and output_fields
		3.  verify the skip fields not loaded, and cannot search/query/hybrid search with them in expr or output_fields
	*/
	// create collection -> insert -> flush -> index
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.AllFields), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))

	// load partial fields
	partialLoadedFields := []string{common.DefaultInt64FieldName, common.DefaultVarcharFieldName, common.DefaultFloatVecFieldName, common.DefaultFloat16VecFieldName}
	loadTask, err := mc.LoadCollection(ctx, clientv2.NewLoadCollectionOption(schema.CollectionName).WithLoadFields(partialLoadedFields...).WithSkipLoadDynamicField(true))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	// search loaded fields & loaded fileds expr & output loaded fields
	expr := fmt.Sprintf("%s > '2' ", common.DefaultVarcharFieldName)
	vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
	searchRes, err := mc.Search(ctx, clientv2.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithANNSField(common.DefaultFloatVecFieldName).WithOutputFields("*").WithFilter(expr))
	common.CheckErr(t, err, true)
	common.CheckSearchResult(t, searchRes, common.DefaultNq, common.DefaultLimit)
	common.CheckOutputFields(t, partialLoadedFields, searchRes[0].Fields)

	// TODO: hybrid search with loaded fields

	// search with not-loaded anns field -> Error
	_, err = mc.Search(ctx, clientv2.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithANNSField(common.DefaultBFloat16VecFieldName))
	common.CheckErr(t, err, false, "ann field \"bf16Vec\" not loaded")

	// search with expr not loaded field -> Error
	invalidExpr := fmt.Sprintf("%s > 2.0 ", common.DefaultFloatFieldName)
	_, err = mc.Search(ctx, clientv2.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithANNSField(common.DefaultFloatVecFieldName).WithFilter(invalidExpr))
	common.CheckErr(t, err, false, "data_type:Float is not loaded")

	// search with output_fields not loaded field -> Error
	_, err = mc.Search(ctx, clientv2.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithANNSField(common.DefaultFloatVecFieldName).WithOutputFields(common.DefaultBoolFieldName))
	common.CheckErr(t, err, false, "field bool is not loaded")
}

func TestLoadSkipDynamicField(t *testing.T) {
	/*
		1. load -> search output dynamic field
		2. reload and skip dynamic field
		verify:
		- search output dynamic field -> error
		- search with dynamic field in expr -> error
	*/
	// create collection -> insert -> flush -> index
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// query and verify output dynamic fields
	queryRes, err := mc.Query(ctx, clientv2.NewQueryOption(schema.CollectionName).WithOutputFields(common.DefaultDynamicFieldName).WithLimit(10))
	common.CheckErr(t, err, true)
	common.CheckOutputFields(t, []string{common.DefaultDynamicFieldName, common.DefaultInt64FieldName}, queryRes.Fields)

	// reload and skip dynamic field
	mc.ReleaseCollection(ctx, clientv2.NewReleaseCollectionOption(schema.CollectionName))
	loadTask, err := mc.LoadCollection(ctx, clientv2.NewLoadCollectionOption(schema.CollectionName).WithSkipLoadDynamicField(true))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	// search and verify output dynamic fields
	t.Log("https://github.com/milvus-io/milvus/issues/37857")
	_, err = mc.Query(ctx, clientv2.NewQueryOption(schema.CollectionName).WithOutputFields(common.DefaultDynamicNumberField).WithLimit(10))
	common.CheckErr(t, err, false, fmt.Sprintf("field %s cannot be returned since dynamic field not loaded", common.DefaultDynamicNumberField))

	_, err = mc.Query(ctx, clientv2.NewQueryOption(schema.CollectionName).WithFilter(fmt.Sprintf("%s > 0", common.DefaultDynamicNumberField)))
	common.CheckErr(t, err, false, "but dynamic field is not loaded")
}

func TestLoadPartialVectorFields(t *testing.T) {
	t.Skip("waiting for HybridSearch implementation")
	/*
		1.  verify load different vector fields has different hybrid search results
	*/
	// create collection -> insert -> flush -> index
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection -> insert -> flush -> index
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64MultiVec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))

	// load partial vector fields
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName).TWithLoadFields(common.DefaultFloatVecFieldName, common.DefaultFloat16VecFieldName))
	// TODO: verify hybrid search results
	// TODO: load partial vector fields
}

func TestLoadPartialFieldsPartitions(t *testing.T) {
	/*
		1. insert data into default partition and parName partition
		2. load default partition with partial fields -> succ & query
		3. load parName partition with different partial fields -> error
		4. load parName partition with same partial fields -> succ
		5. query from default partition and parName partition -> count=2*nb
	*/
	// no [pk, clustering, part dynamic fields] field, not all fields
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	parName := common.GenRandomString("p", 4)

	// create collection and partition
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VecAllScalar), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	err := mc.CreatePartition(ctx, clientv2.NewCreatePartitionOption(schema.CollectionName, parName))
	common.CheckErr(t, err, true)

	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(common.DefaultNb))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema).TWithPartitionName(parName), hp.TNewDataOption().TWithNb(common.DefaultNb).TWithStart(common.DefaultNb))

	// create index
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))

	// load default partition with partial fields
	partitionFields := []string{common.DefaultInt64FieldName, common.DefaultFloatVecFieldName, common.DefaultBoolFieldName}
	taskDef, errDef := mc.LoadPartitions(ctx, clientv2.NewLoadPartitionsOption(schema.CollectionName, common.DefaultPartition).WithLoadFields(partitionFields...))
	common.CheckErr(t, errDef, true)
	err = taskDef.Await(ctx)
	common.CheckErr(t, err, true)

	// query from default partition
	queryDefault, errQuery := mc.Query(ctx, clientv2.NewQueryOption(schema.CollectionName).WithOutputFields(partitionFields...).WithPartitions(common.DefaultPartition).WithLimit(common.DefaultLimit))
	common.CheckErr(t, errQuery, true)
	common.CheckOutputFields(t, partitionFields, queryDefault.Fields)

	// load parName partition with different partial fields
	diffFields := []string{common.DefaultInt64FieldName, common.DefaultFloatVecFieldName, common.DefaultVarcharFieldName}
	_, errPar := mc.LoadPartitions(ctx, clientv2.NewLoadPartitionsOption(schema.CollectionName, parName).WithLoadFields(diffFields...))
	common.CheckErr(t, errPar, false, "can't change the load field list for loaded collection")

	// load parName partition with different partial fields
	taskPar, errPar := mc.LoadPartitions(ctx, clientv2.NewLoadPartitionsOption(schema.CollectionName, parName).WithLoadFields(partitionFields...))
	common.CheckErr(t, errPar, true)
	err = taskPar.Await(ctx)
	common.CheckErr(t, err, true)

	// query from default partition and parName partition
	countMultiPartitions, err := mc.Query(ctx, clientv2.NewQueryOption(schema.CollectionName).WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	count, _ := countMultiPartitions.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
	require.Equal(t, int64(common.DefaultNb*2), count)
}

func TestLoadPartialFieldsWithoutPartitionKey(t *testing.T) {
	/*
		code fields: pk, clustering key, partition key, part dynamic fields, non-vector fields -> error
		not index: error
	*/
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// define fields & schema
	pkField := entity.NewField().WithName("pk").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	partitionKeyField := entity.NewField().WithName("partition_key").WithDataType(entity.FieldTypeInt64).WithIsPartitionKey(true)
	clusteringKeyField := entity.NewField().WithName("clustering_key").WithDataType(entity.FieldTypeInt64).WithIsClusteringKey(true)
	vecField := entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	schema := entity.NewSchema().WithName(common.GenRandomString("partial", 4)).WithField(pkField).WithField(partitionKeyField).WithField(clusteringKeyField).WithField(vecField).WithDynamicFieldEnabled(true)
	mc.CreateCollection(ctx, clientv2.NewCreateCollectionOption(schema.CollectionName, schema))

	// load partial fields without partition key fields
	_, err := mc.LoadCollection(ctx, clientv2.NewLoadCollectionOption(schema.CollectionName).WithLoadFields(pkField.Name, vecField.Name))
	common.CheckErr(t, err, false, "does not contain partition key field partition_key")
	// load partial fields without clustering key fields
	_, err = mc.LoadCollection(ctx, clientv2.NewLoadCollectionOption(schema.CollectionName).WithLoadFields(partitionKeyField.Name, pkField.Name, vecField.Name))
	common.CheckErr(t, err, false, "does not contain clustering key field clustering_key")
	// load partial fields without pk
	_, err = mc.LoadCollection(ctx, clientv2.NewLoadCollectionOption(schema.CollectionName).WithLoadFields(partitionKeyField.Name, clusteringKeyField.Name, vecField.Name))
	common.CheckErr(t, err, false, "does not contain primary key field pk")
	// load partial fields without vector fields
	_, err = mc.LoadCollection(ctx, clientv2.NewLoadCollectionOption(schema.CollectionName).WithLoadFields(partitionKeyField.Name, clusteringKeyField.Name, pkField.Name))
	common.CheckErr(t, err, false, "does not contain vector field")

	_, err = mc.LoadCollection(ctx, clientv2.NewLoadCollectionOption(schema.CollectionName).WithLoadFields(partitionKeyField.Name, clusteringKeyField.Name, pkField.Name, vecField.Name))
	common.CheckErr(t, err, false, "index not found")

	// create index
	hp.CollPrepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	_, err = mc.LoadCollection(ctx, clientv2.NewLoadCollectionOption(schema.CollectionName).WithLoadFields(partitionKeyField.Name, clusteringKeyField.Name, pkField.Name, vecField.Name))
	common.CheckErr(t, err, true)
}

func TestLoadPartialFieldsRepeated(t *testing.T) {
	/*
		1. repeated Load with different LoadFields -> error
	*/
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VecArray), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))

	loadTask, err := mc.LoadCollection(ctx, clientv2.NewLoadCollectionOption(schema.CollectionName).WithLoadFields(common.DefaultInt64FieldName, common.DefaultFloatVecFieldName, common.DefaultInt64ArrayField))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	// load with different fields
	_, err = mc.LoadCollection(ctx, clientv2.NewLoadCollectionOption(schema.CollectionName).WithLoadFields(common.DefaultInt64FieldName, common.DefaultFloatVecFieldName, common.DefaultVarcharArrayField))
	common.CheckErr(t, err, false, "can't change the load field list for loaded collection")
}

func TestReleaseCollection(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

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
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// Load collection
	errLoad := mc.ReleaseCollection(ctx, clientv2.NewReleaseCollectionOption("collName"))
	common.CheckErr(t, errLoad, false, "collection not found[database=default][collection=collName]")
}

func TestReleasePartitions(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	parName := common.GenRandomString("p", 4)
	nb := 100

	// create collection and partition
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	err := mc.CreatePartition(ctx, clientv2.NewCreatePartitionOption(schema.CollectionName, parName))
	common.CheckErr(t, err, true)

	// insert [0, nb) into default partition and new partition
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(nb))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema).TWithPartitionName(parName), hp.TNewDataOption().TWithNb(nb).TWithStart(nb))

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
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// Load collection
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	errLoad := mc.ReleasePartitions(ctx, clientv2.NewReleasePartitionsOptions(schema.CollectionName, "parName"))
	common.CheckErr(t, errLoad, false, "partition not found")
}

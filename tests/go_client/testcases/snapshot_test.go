package testcases

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

var snapshotPrefix = "snapshot"

// TestCreateSnapshot tests creating a snapshot for a collection
func TestCreateSnapshot(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// Create a collection first
	collName := common.GenRandomString(snapshotPrefix, 6)
	err := mc.CreateCollection(ctx, client.SimpleCreateCollectionOptions(collName, common.DefaultDim))
	common.CheckErr(t, err, true)

	// Get collection schema and insert data
	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	common.CheckErr(t, err, true)
	prepare, _ := hp.CollPrepare.InsertData(ctx, t, mc, hp.NewInsertParams(coll.Schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, collName)

	// Create snapshot
	snapshotName := fmt.Sprintf("snapshot_%s", common.GenRandomString(snapshotPrefix, 6))
	createOpt := client.NewCreateSnapshotOption(snapshotName, collName).
		WithDescription("Test snapshot for e2e testing")

	err = mc.CreateSnapshot(ctx, createOpt)
	common.CheckErr(t, err, true)

	// Verify snapshot was created by listing snapshots
	listOpt := client.NewListSnapshotsOption().
		WithCollectionName(collName)
	snapshots, err := mc.ListSnapshots(ctx, listOpt)
	common.CheckErr(t, err, true)
	require.Contains(t, snapshots, snapshotName)

	// Describe the snapshot
	describeOpt := client.NewDescribeSnapshotOption(snapshotName)
	resp, err := mc.DescribeSnapshot(ctx, describeOpt)
	common.CheckErr(t, err, true)
	require.Equal(t, snapshotName, resp.GetName())
	require.Equal(t, collName, resp.GetCollectionName())
	require.Equal(t, "Test snapshot for e2e testing", resp.GetDescription())
	require.Greater(t, resp.GetCreateTs(), int64(0))

	// Clean up
	dropOpt := client.NewDropSnapshotOption(snapshotName)
	err = mc.DropSnapshot(ctx, dropOpt)
	common.CheckErr(t, err, true)
}

// TestSnapshotRestoreWithMultiSegment tests the complete snapshot restore workflow with data operations
func TestSnapshotRestoreWithMultiSegment(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	insertBatchSize := 30000
	deleteBatchSize := 10000
	numOfBatch := 5

	// Step 1: Create collection and insert initial 3000 records
	collName := common.GenRandomString(snapshotPrefix, 6)
	schema := client.SimpleCreateCollectionOptions(collName, common.DefaultDim)
	schema.WithAutoID(false)
	schema.WithShardNum(10)
	err := mc.CreateCollection(ctx, schema)
	common.CheckErr(t, err, true)

	// Get collection schema
	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	common.CheckErr(t, err, true)

	// Insert records
	for i := 0; i < numOfBatch; i++ {
		insertOpt := hp.TNewDataOption().TWithNb(insertBatchSize).TWithStart(i * insertBatchSize)
		_, insertRes := hp.CollPrepare.InsertData(ctx, t, mc, hp.NewInsertParams(coll.Schema), insertOpt)
		require.Equal(t, insertBatchSize, insertRes.IDs.Len())
	}
	// Flush to ensure deletion is persisted
	_, err = mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)
	time.Sleep(10 * time.Second)

	// Verify initial data count
	queryRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ := queryRes.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(insertBatchSize*numOfBatch), count)

	// Delete records
	for i := 0; i < numOfBatch; i++ {
		deleteExpr := fmt.Sprintf("id >= %d and id < %d", insertBatchSize*i, insertBatchSize*i+deleteBatchSize)
		delRes, err := mc.Delete(ctx, client.NewDeleteOption(collName).WithExpr(deleteExpr))
		common.CheckErr(t, err, true)
		require.Equal(t, int64(deleteBatchSize), delRes.DeleteCount)
	}

	// Flush to ensure deletion is persisted
	_, err = mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)
	time.Sleep(10 * time.Second)

	// Verify data count after deletion
	queryRes2, err := mc.Query(ctx, client.NewQueryOption(collName).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ = queryRes2.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(100000), count)

	// Step 2: Create snapshot
	snapshotName := fmt.Sprintf("restore_snapshot_%s", common.GenRandomString(snapshotPrefix, 6))
	createOpt := client.NewCreateSnapshotOption(snapshotName, collName).
		WithDescription("Snapshot for restore testing with 2000 records")

	err = mc.CreateSnapshot(ctx, createOpt)
	common.CheckErr(t, err, true)

	// Verify snapshot was created
	listOpt := client.NewListSnapshotsOption().WithCollectionName(collName)
	snapshots, err := mc.ListSnapshots(ctx, listOpt)
	common.CheckErr(t, err, true)
	require.Contains(t, snapshots, snapshotName)

	// print snapshot info
	describeOpt := client.NewDescribeSnapshotOption(snapshotName)
	snapshotInfo, err := mc.DescribeSnapshot(ctx, describeOpt)
	common.CheckErr(t, err, true)
	require.Equal(t, snapshotName, snapshotInfo.GetName())
	log.Info("check snapshot info", zap.Any("info", snapshotInfo))

	// Step 3: Continue inserting more records and delete 1000 records
	// Insert more records
	for i := 0; i < numOfBatch; i++ {
		pkStart := insertBatchSize * (numOfBatch + i)
		insertOpt2 := hp.TNewDataOption().TWithNb(insertBatchSize).TWithStart(pkStart)
		_, insertRes2 := hp.CollPrepare.InsertData(ctx, t, mc, hp.NewInsertParams(coll.Schema), insertOpt2)
		require.Equal(t, insertBatchSize, insertRes2.IDs.Len())
	}

	// Verify total data count after second insertion
	queryRes3, err := mc.Query(ctx, client.NewQueryOption(collName).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ = queryRes3.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(250000), count)

	// Step 4: Restore snapshot to a new collection
	restoredCollName := fmt.Sprintf("restored_%s", collName)
	restoreOpt := client.NewRestoreSnapshotOption(snapshotName, restoredCollName)
	_, err = mc.RestoreSnapshot(ctx, restoreOpt)
	common.CheckErr(t, err, true)

	// Verify restored collection exists
	has, err := mc.HasCollection(ctx, client.NewHasCollectionOption(restoredCollName))
	common.CheckErr(t, err, true)
	require.True(t, has)

	// load restored collection
	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(restoredCollName).WithReplica(1))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	time.Sleep(3 * time.Second)

	// Verify restored partition data count
	queryRes5, err := mc.Query(ctx,
		client.NewQueryOption(restoredCollName).
			WithOutputFields(common.QueryCountFieldName).
			WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ = queryRes5.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(100000), count)

	// Clean up
	dropOpt := client.NewDropSnapshotOption(snapshotName)
	err = mc.DropSnapshot(ctx, dropOpt)
	common.CheckErr(t, err, true)
}

// TestSnapshotRestoreWithMultiShardMultiPartition tests the complete snapshot restore workflow with data operations
func TestSnapshotRestoreWithMultiShardMultiPartition(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	insertBatchSize := 3000
	deleteBatchSize := 1000

	// Step 1: Create collection and insert initial 3000 records
	collName := common.GenRandomString(snapshotPrefix, 6)
	schema := client.SimpleCreateCollectionOptions(collName, common.DefaultDim)
	schema.WithAutoID(false)
	schema.WithShardNum(3)
	err := mc.CreateCollection(ctx, schema)
	common.CheckErr(t, err, true)

	partitions := make([]string, 0)
	for i := 0; i < 10; i++ {
		partitions = append(partitions, fmt.Sprintf("part_%d", i))
		option := client.NewCreatePartitionOption(collName, partitions[i])
		err := mc.CreatePartition(ctx, option)
		common.CheckErr(t, err, true)
	}
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i] < partitions[j]
	})

	// Get collection schema
	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	common.CheckErr(t, err, true)

	// Insert records
	for i, partition := range partitions {
		insertOpt := hp.TNewDataOption().TWithNb(insertBatchSize).TWithStart(i * insertBatchSize)
		_, insertRes := hp.CollPrepare.InsertData(ctx, t, mc, hp.NewInsertParams(coll.Schema).TWithPartitionName(partition), insertOpt)
		require.Equal(t, insertBatchSize, insertRes.IDs.Len())
	}

	// Verify initial data count
	queryRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ := queryRes.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(30000), count)

	// Delete records
	for i := range partitions {
		deleteExpr := fmt.Sprintf("id >= %d and id < %d", insertBatchSize*i, insertBatchSize*i+deleteBatchSize)
		delRes, err := mc.Delete(ctx, client.NewDeleteOption(collName).WithExpr(deleteExpr))
		common.CheckErr(t, err, true)
		require.Equal(t, int64(1000), delRes.DeleteCount)
	}

	// Flush to ensure deletion is persisted
	_, err = mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)

	time.Sleep(10 * time.Second)

	// Verify data count after deletion
	queryRes2, err := mc.Query(ctx, client.NewQueryOption(collName).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ = queryRes2.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(20000), count)

	// Step 2: Create snapshot
	snapshotName := fmt.Sprintf("restore_snapshot_%s", common.GenRandomString(snapshotPrefix, 6))
	createOpt := client.NewCreateSnapshotOption(snapshotName, collName).
		WithDescription("Snapshot for restore testing with 2000 records")

	err = mc.CreateSnapshot(ctx, createOpt)
	common.CheckErr(t, err, true)

	// Verify snapshot was created
	listOpt := client.NewListSnapshotsOption().WithCollectionName(collName)
	snapshots, err := mc.ListSnapshots(ctx, listOpt)
	common.CheckErr(t, err, true)
	require.Contains(t, snapshots, snapshotName)

	// print snapshot info
	describeOpt := client.NewDescribeSnapshotOption(snapshotName)
	snapshotInfo, err := mc.DescribeSnapshot(ctx, describeOpt)
	common.CheckErr(t, err, true)
	require.Equal(t, snapshotName, snapshotInfo.GetName())
	log.Info("check snapshot info", zap.Any("info", snapshotInfo))

	// Step 3: Continue inserting more records and delete 1000 records
	// Insert more records
	for i, partition := range partitions {
		pkStart := insertBatchSize * (len(partitions) + i)
		insertOpt2 := hp.TNewDataOption().TWithNb(insertBatchSize).TWithStart(pkStart)
		_, insertRes2 := hp.CollPrepare.InsertData(ctx, t, mc, hp.NewInsertParams(coll.Schema).TWithPartitionName(partition), insertOpt2)
		require.Equal(t, insertBatchSize, insertRes2.IDs.Len())
	}

	// Verify total data count after second insertion
	queryRes3, err := mc.Query(ctx, client.NewQueryOption(collName).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ = queryRes3.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(50000), count)

	// Step 4: Restore snapshot to a new collection
	restoredCollName := fmt.Sprintf("restored_%s", collName)
	restoreOpt := client.NewRestoreSnapshotOption(snapshotName, restoredCollName)
	_, err = mc.RestoreSnapshot(ctx, restoreOpt)
	common.CheckErr(t, err, true)

	// Verify restored collection exists
	has, err := mc.HasCollection(ctx, client.NewHasCollectionOption(restoredCollName))
	common.CheckErr(t, err, true)
	require.True(t, has)

	// load restored collection
	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(restoredCollName).WithReplica(1))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	for _, partition := range partitions {
		// Verify restored partition data count (should be 2000 records from snapshot)
		queryRes5, err := mc.Query(ctx,
			client.NewQueryOption(restoredCollName).
				WithOutputFields(common.QueryCountFieldName).
				WithConsistencyLevel(entity.ClStrong).
				WithPartitions(partition))
		common.CheckErr(t, err, true)
		count, _ = queryRes5.Fields[0].GetAsInt64(0)
		require.Equal(t, int64(2000), count)
	}

	// Clean up
	dropOpt := client.NewDropSnapshotOption(snapshotName)
	err = mc.DropSnapshot(ctx, dropOpt)
	common.CheckErr(t, err, true)
}

// TestSnapshotRestoreWithMultiFields tests snapshot restore with all supported field types
func TestSnapshotRestoreWithMultiFields(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	insertBatchSize := 10000
	deleteBatchSize := 3000
	numOfBatch := 5

	// Step 1: Create collection with all field types
	collName := common.GenRandomString(snapshotPrefix, 6)

	// Create schema with all supported field types
	pkField := entity.NewField().
		WithName("id").
		WithDataType(entity.FieldTypeInt64).
		WithIsPrimaryKey(true)

	// Scalar fields
	boolField := entity.NewField().WithName("bool_field").WithDataType(entity.FieldTypeBool)
	int64Field := entity.NewField().WithName("int64_field").WithDataType(entity.FieldTypeInt64)
	floatField := entity.NewField().WithName("float_field").WithDataType(entity.FieldTypeFloat)
	varcharField := entity.NewField().WithName("varchar_field").WithDataType(entity.FieldTypeVarChar).WithMaxLength(200)
	jsonField := entity.NewField().WithName("json_field").WithDataType(entity.FieldTypeJSON)

	floatVecField := entity.NewField().WithName("float_vec").WithDataType(entity.FieldTypeFloatVector).WithDim(128)

	// Array fields - representative types
	int64ArrayField := entity.NewField().WithName("int64_array").WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeInt64).WithMaxCapacity(100)
	stringArrayField := entity.NewField().WithName("string_array").WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeVarChar).WithMaxLength(50).WithMaxCapacity(100)

	// Create schema
	schema := entity.NewSchema().
		WithName(collName).
		WithField(pkField).
		WithField(boolField).
		WithField(int64Field).
		WithField(floatField).
		WithField(varcharField).
		WithField(jsonField).
		WithField(floatVecField).
		WithField(int64ArrayField).
		WithField(stringArrayField).
		WithDynamicFieldEnabled(true)

	// Create collection with 5 shards
	createOpt := client.NewCreateCollectionOption(collName, schema).WithShardNum(5)
	err := mc.CreateCollection(ctx, createOpt)
	common.CheckErr(t, err, true)

	// Get collection schema for data insertion
	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	common.CheckErr(t, err, true)

	// Step 2a: Create indexes for vector field (required before loading)
	log.Info("Creating index for vector field")
	vecIdx := index.NewHNSWIndex(entity.L2, 8, 96)
	vecIndexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "float_vec", vecIdx))
	common.CheckErr(t, err, true)
	err = vecIndexTask.Await(ctx)
	common.CheckErr(t, err, true)

	// Step 2b: Create indexes for scalar fields to accelerate filtering
	log.Info("Creating indexes for scalar fields")
	scalarIndexFields := []string{"int64_field", "varchar_field"}
	for _, fieldName := range scalarIndexFields {
		scalarIdx := index.NewInvertedIndex()
		scalarIndexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, fieldName, scalarIdx))
		common.CheckErr(t, err, true)
		err = scalarIndexTask.Await(ctx)
		common.CheckErr(t, err, true)
	}

	// Step 2c: Create indexes for array fields
	log.Info("Creating indexes for array fields")
	arrayIndexFields := []string{"int64_array", "string_array"}
	for _, fieldName := range arrayIndexFields {
		arrayIdx := index.NewInvertedIndex()
		arrayIndexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, fieldName, arrayIdx))
		common.CheckErr(t, err, true)
		err = arrayIndexTask.Await(ctx)
		common.CheckErr(t, err, true)
	}

	// Step 2d: Load collection
	log.Info("Loading collection")
	loadOpt := client.NewLoadCollectionOption(collName).WithReplica(1)
	loadTask, err := mc.LoadCollection(ctx, loadOpt)
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	// Step 2e: Insert first batch of data (5 batches × 10,000 records)
	for i := 0; i < numOfBatch; i++ {
		insertOpt := hp.TNewDataOption().TWithNb(insertBatchSize).TWithStart(i * insertBatchSize)
		_, insertRes := hp.CollPrepare.InsertData(ctx, t, mc, hp.NewInsertParams(coll.Schema), insertOpt)
		require.Equal(t, insertBatchSize, insertRes.IDs.Len())
	}

	// Flush to ensure data is persisted
	_, err = mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)

	// Wait for flush to complete
	time.Sleep(10 * time.Second)

	// Step 3: Delete some records (3,000 from each batch)
	for i := 0; i < numOfBatch; i++ {
		deleteExpr := fmt.Sprintf("id >= %d and id < %d", insertBatchSize*i, insertBatchSize*i+deleteBatchSize)
		delRes, err := mc.Delete(ctx, client.NewDeleteOption(collName).WithExpr(deleteExpr))
		common.CheckErr(t, err, true)
		require.Equal(t, int64(deleteBatchSize), delRes.DeleteCount)
	}

	// Flush to ensure deletion is persisted
	_, err = mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)

	// Wait for flush to complete
	time.Sleep(10 * time.Second)

	// Step 4: Create snapshot
	snapshotName := fmt.Sprintf("multi_fields_snapshot_%s", common.GenRandomString(snapshotPrefix, 6))
	createSnapshotOpt := client.NewCreateSnapshotOption(snapshotName, collName).
		WithDescription("Snapshot for multi-fields restore testing")

	err = mc.CreateSnapshot(ctx, createSnapshotOpt)
	common.CheckErr(t, err, true)

	// Verify snapshot was created
	listOpt := client.NewListSnapshotsOption().WithCollectionName(collName)
	snapshots, err := mc.ListSnapshots(ctx, listOpt)
	common.CheckErr(t, err, true)
	require.Contains(t, snapshots, snapshotName)

	// Print snapshot info
	describeOpt := client.NewDescribeSnapshotOption(snapshotName)
	snapshotInfo, err := mc.DescribeSnapshot(ctx, describeOpt)
	common.CheckErr(t, err, true)
	require.Equal(t, snapshotName, snapshotInfo.GetName())
	log.Info("Created snapshot for multi-fields test", zap.Any("info", snapshotInfo))

	// Step 5: Continue inserting more records (3 batches × 10,000 records)
	// This is to verify that snapshot captures state before these insertions
	for i := 0; i < 3; i++ {
		pkStart := insertBatchSize * (numOfBatch + i)
		insertOpt2 := hp.TNewDataOption().TWithNb(insertBatchSize).TWithStart(pkStart)
		_, insertRes2 := hp.CollPrepare.InsertData(ctx, t, mc, hp.NewInsertParams(coll.Schema), insertOpt2)
		require.Equal(t, insertBatchSize, insertRes2.IDs.Len())
	}
	// Total data after this step: 35,000 + 30,000 = 65,000
	// But snapshot should restore only 35,000 records

	// Step 6: Restore snapshot to a new collection
	restoredCollName := fmt.Sprintf("restored_%s", collName)
	restoreOpt := client.NewRestoreSnapshotOption(snapshotName, restoredCollName)
	_, err = mc.RestoreSnapshot(ctx, restoreOpt)
	common.CheckErr(t, err, true)

	// Verify restored collection exists
	has, err := mc.HasCollection(ctx, client.NewHasCollectionOption(restoredCollName))
	common.CheckErr(t, err, true)
	require.True(t, has)

	// Load restored collection
	loadTask, err = mc.LoadCollection(ctx, client.NewLoadCollectionOption(restoredCollName).WithReplica(1))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	// Verify restored collection data count (should be 35,000 from snapshot)
	queryRes, err := mc.Query(ctx,
		client.NewQueryOption(restoredCollName).
			WithOutputFields(common.QueryCountFieldName).
			WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ := queryRes.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(35000), count)

	// Verify schema of restored collection
	restoredColl, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(restoredCollName))
	common.CheckErr(t, err, true)
	require.Equal(t, len(coll.Schema.Fields), len(restoredColl.Schema.Fields))
	require.True(t, restoredColl.Schema.EnableDynamicField)

	// Clean up
	dropOpt := client.NewDropSnapshotOption(snapshotName)
	err = mc.DropSnapshot(ctx, dropOpt)
	common.CheckErr(t, err, true)
}

// TestSnapshotRestoreEmptyCollection tests snapshot and restore of an empty collection
// Verifies that schema and indexes are preserved correctly without any data
func TestSnapshotRestoreEmptyCollection(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// Step 1: Create collection with multiple field types
	collName := common.GenRandomString(snapshotPrefix, 6)

	// Create schema with various field types
	pkField := entity.NewField().
		WithName("id").
		WithDataType(entity.FieldTypeInt64).
		WithIsPrimaryKey(true)

	// Scalar fields
	boolField := entity.NewField().WithName("bool_field").WithDataType(entity.FieldTypeBool)
	int64Field := entity.NewField().WithName("int64_field").WithDataType(entity.FieldTypeInt64)
	floatField := entity.NewField().WithName("float_field").WithDataType(entity.FieldTypeFloat)
	varcharField := entity.NewField().WithName("varchar_field").WithDataType(entity.FieldTypeVarChar).WithMaxLength(200)
	jsonField := entity.NewField().WithName("json_field").WithDataType(entity.FieldTypeJSON)

	// Vector field
	floatVecField := entity.NewField().WithName("float_vec").WithDataType(entity.FieldTypeFloatVector).WithDim(128)

	// Array fields
	int64ArrayField := entity.NewField().WithName("int64_array").WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeInt64).WithMaxCapacity(100)
	stringArrayField := entity.NewField().WithName("string_array").WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeVarChar).WithMaxLength(50).WithMaxCapacity(100)

	// Create schema
	schema := entity.NewSchema().
		WithName(collName).
		WithField(pkField).
		WithField(boolField).
		WithField(int64Field).
		WithField(floatField).
		WithField(varcharField).
		WithField(jsonField).
		WithField(floatVecField).
		WithField(int64ArrayField).
		WithField(stringArrayField).
		WithDynamicFieldEnabled(true)

	// Create collection with 3 shards
	createOpt := client.NewCreateCollectionOption(collName, schema).WithShardNum(3)
	err := mc.CreateCollection(ctx, createOpt)
	common.CheckErr(t, err, true)

	// Step 2: Create partitions
	partitions := make([]string, 0)
	for i := 0; i < 3; i++ {
		partName := fmt.Sprintf("part_%d", i)
		partitions = append(partitions, partName)
		partOption := client.NewCreatePartitionOption(collName, partName)
		err := mc.CreatePartition(ctx, partOption)
		common.CheckErr(t, err, true)
	}

	// Step 3: Create indexes for vector field
	log.Info("Creating index for vector field")
	vecIdx := index.NewHNSWIndex(entity.L2, 8, 96)
	vecIndexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "float_vec", vecIdx))
	common.CheckErr(t, err, true)
	err = vecIndexTask.Await(ctx)
	common.CheckErr(t, err, true)

	// Step 4: Create indexes for scalar fields
	log.Info("Creating indexes for scalar fields")
	scalarIndexFields := []string{"int64_field", "varchar_field"}
	for _, fieldName := range scalarIndexFields {
		scalarIdx := index.NewInvertedIndex()
		scalarIndexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, fieldName, scalarIdx))
		common.CheckErr(t, err, true)
		err = scalarIndexTask.Await(ctx)
		common.CheckErr(t, err, true)
	}

	// Step 5: Create indexes for array fields
	log.Info("Creating indexes for array fields")
	arrayIndexFields := []string{"int64_array", "string_array"}
	for _, fieldName := range arrayIndexFields {
		arrayIdx := index.NewInvertedIndex()
		arrayIndexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, fieldName, arrayIdx))
		common.CheckErr(t, err, true)
		err = arrayIndexTask.Await(ctx)
		common.CheckErr(t, err, true)
	}

	// Step 6: Get original collection info for later comparison
	originalColl, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	common.CheckErr(t, err, true)

	// Step 7: Create snapshot on empty collection
	snapshotName := fmt.Sprintf("empty_coll_snapshot_%s", common.GenRandomString(snapshotPrefix, 6))
	createSnapshotOpt := client.NewCreateSnapshotOption(snapshotName, collName).
		WithDescription("Snapshot for empty collection restore testing")

	err = mc.CreateSnapshot(ctx, createSnapshotOpt)
	common.CheckErr(t, err, true)

	// Verify snapshot was created
	listOpt := client.NewListSnapshotsOption().WithCollectionName(collName)
	snapshots, err := mc.ListSnapshots(ctx, listOpt)
	common.CheckErr(t, err, true)
	require.Contains(t, snapshots, snapshotName)

	// Print snapshot info
	describeOpt := client.NewDescribeSnapshotOption(snapshotName)
	snapshotInfo, err := mc.DescribeSnapshot(ctx, describeOpt)
	common.CheckErr(t, err, true)
	require.Equal(t, snapshotName, snapshotInfo.GetName())
	log.Info("Created snapshot for empty collection", zap.Any("info", snapshotInfo))

	// Step 8: Restore snapshot to a new collection
	restoredCollName := fmt.Sprintf("restored_%s", collName)
	restoreOpt := client.NewRestoreSnapshotOption(snapshotName, restoredCollName)
	_, err = mc.RestoreSnapshot(ctx, restoreOpt)
	common.CheckErr(t, err, true)

	// Step 9: Verify restored collection exists
	has, err := mc.HasCollection(ctx, client.NewHasCollectionOption(restoredCollName))
	common.CheckErr(t, err, true)
	require.True(t, has)

	// Step 10: Get restored collection info
	restoredColl, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(restoredCollName))
	common.CheckErr(t, err, true)

	// Step 11: Verify schema matches
	log.Info("Verifying schema consistency")
	require.Equal(t, len(originalColl.Schema.Fields), len(restoredColl.Schema.Fields), "Field count should match")
	require.Equal(t, originalColl.Schema.EnableDynamicField, restoredColl.Schema.EnableDynamicField, "Dynamic field setting should match")

	// Verify each field
	for i, originalField := range originalColl.Schema.Fields {
		restoredField := restoredColl.Schema.Fields[i]
		require.Equal(t, originalField.Name, restoredField.Name, "Field name should match")
		require.Equal(t, originalField.DataType, restoredField.DataType, "Field data type should match")
		require.Equal(t, originalField.PrimaryKey, restoredField.PrimaryKey, "Primary key setting should match")

		// Check vector dimensions if applicable
		if originalField.DataType == entity.FieldTypeFloatVector || originalField.DataType == entity.FieldTypeBinaryVector {
			originalDim, _ := originalField.GetDim()
			restoredDim, _ := restoredField.GetDim()
			require.Equal(t, originalDim, restoredDim, "Vector dimension should match")
		}

		// Check varchar max length if applicable
		if originalField.DataType == entity.FieldTypeVarChar {
			originalMaxLen := originalField.TypeParams[entity.TypeParamMaxLength]
			restoredMaxLen := restoredField.TypeParams[entity.TypeParamMaxLength]
			require.Equal(t, originalMaxLen, restoredMaxLen, "VarChar max length should match")
		}

		// Check array fields
		if originalField.DataType == entity.FieldTypeArray {
			originalElemType := originalField.ElementType
			restoredElemType := restoredField.ElementType
			require.Equal(t, originalElemType, restoredElemType, "Array element type should match")

			originalMaxCap := originalField.TypeParams[entity.TypeParamMaxCapacity]
			restoredMaxCap := restoredField.TypeParams[entity.TypeParamMaxCapacity]
			require.Equal(t, originalMaxCap, restoredMaxCap, "Array max capacity should match")
		}
	}

	// Step 12: Verify partitions match
	log.Info("Verifying partition consistency")
	sort.Strings(partitions)

	// Get restored collection partitions
	restoredPartitionNames, err := mc.ListPartitions(ctx, client.NewListPartitionOption(restoredCollName))
	common.CheckErr(t, err, true)

	// Exclude default partition from comparison
	filteredRestoredPartitions := make([]string, 0)
	for _, partName := range restoredPartitionNames {
		if partName != "_default" {
			filteredRestoredPartitions = append(filteredRestoredPartitions, partName)
		}
	}
	sort.Strings(filteredRestoredPartitions)
	require.Equal(t, partitions, filteredRestoredPartitions, "Partitions should match")

	// Step 13: Verify indexes match
	log.Info("Verifying index consistency")
	originalIndexes, err := mc.ListIndexes(ctx, client.NewListIndexOption(collName))
	common.CheckErr(t, err, true)
	log.Info("original indexes", zap.Any("indexes", originalIndexes))

	restoredIndexes, err := mc.ListIndexes(ctx, client.NewListIndexOption(restoredCollName))
	common.CheckErr(t, err, true)

	require.Equal(t, len(originalIndexes), len(restoredIndexes), "Index count should match")

	// Create maps for easier comparison
	originalIndexMap := make(map[string]string)
	for _, idx := range originalIndexes {
		originalIndexMap[idx] = idx
	}

	restoredIndexMap := make(map[string]string)
	for _, idx := range restoredIndexes {
		restoredIndexMap[idx] = idx
	}

	// Verify all original indexes exist in restored collection
	for fieldName := range originalIndexMap {
		_, exists := restoredIndexMap[fieldName]
		require.True(t, exists, fmt.Sprintf("Index on field %s should exist in restored collection", fieldName))
	}

	// Step 14: Load both collections and verify they have no data
	log.Info("Loading collections to verify data")
	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName).WithReplica(1))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	loadTask2, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(restoredCollName).WithReplica(1))
	common.CheckErr(t, err, true)
	err = loadTask2.Await(ctx)
	common.CheckErr(t, err, true)

	// Verify both collections have 0 records
	originalQueryRes, err := mc.Query(ctx,
		client.NewQueryOption(collName).
			WithOutputFields(common.QueryCountFieldName).
			WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	originalCount, _ := originalQueryRes.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(0), originalCount, "Original collection should have 0 records")

	restoredQueryRes, err := mc.Query(ctx,
		client.NewQueryOption(restoredCollName).
			WithOutputFields(common.QueryCountFieldName).
			WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	restoredCount, _ := restoredQueryRes.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(0), restoredCount, "Restored collection should have 0 records")

	log.Info("Empty collection snapshot and restore test completed successfully",
		zap.String("original_collection", collName),
		zap.String("restored_collection", restoredCollName),
		zap.Int("field_count", len(originalColl.Schema.Fields)),
		zap.Int("index_count", len(originalIndexes)),
		zap.Int("partition_count", len(partitions)))

	// Clean up
	dropOpt := client.NewDropSnapshotOption(snapshotName)
	err = mc.DropSnapshot(ctx, dropOpt)
	common.CheckErr(t, err, true)
}

// TestSnapshotRestoreWithJSONStats tests snapshot restore with JSON field and JSON stats
// This test verifies that JSON stats (both legacy json_key_index_log and new json_stats formats)
// are correctly preserved and restored during snapshot operations
func TestSnapshotRestoreWithJSONStats(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	insertBatchSize := 5000
	deleteBatchSize := 1000
	numOfBatch := 10

	// Step 1: Create collection with JSON field
	collName := common.GenRandomString(snapshotPrefix, 6)

	pkField := entity.NewField().
		WithName("id").
		WithDataType(entity.FieldTypeInt64).
		WithIsPrimaryKey(true)

	// JSON field for testing JSON stats
	jsonField := entity.NewField().
		WithName("json_data").
		WithDataType(entity.FieldTypeJSON)

	// VARCHAR field for additional filtering
	varcharField := entity.NewField().
		WithName("name").
		WithDataType(entity.FieldTypeVarChar).
		WithMaxLength(200)

	// Vector field
	floatVecField := entity.NewField().
		WithName("embeddings").
		WithDataType(entity.FieldTypeFloatVector).
		WithDim(128)

	// Create schema
	schema := entity.NewSchema().
		WithName(collName).
		WithField(pkField).
		WithField(jsonField).
		WithField(varcharField).
		WithField(floatVecField).
		WithDynamicFieldEnabled(true)

	// Step 2: Prepare indexes
	log.Info("Preparing indexes for collection")

	// Vector index (required for loading)
	vecIdx := index.NewHNSWIndex(entity.L2, 8, 96)
	vecIndexOpt := client.NewCreateIndexOption(collName, "embeddings", vecIdx)

	// VARCHAR index
	varcharIdx := index.NewInvertedIndex()
	varcharIndexOpt := client.NewCreateIndexOption(collName, "name", varcharIdx)

	// JSON field index - this will create JSON stats
	// Note: JSON stats may use either legacy json_key_index_log or new json_stats format
	// depending on the system configuration
	jsonIdx := index.NewAutoIndex(entity.IP)
	jsonIndexOpt := client.NewCreateIndexOption(schema.CollectionName, "json_data", jsonIdx)
	jsonIndexOpt.WithExtraParam("json_path", "json_data['string']")
	jsonIndexOpt.WithExtraParam("json_cast_type", "varchar")

	// Create collection with all indexes in one go
	log.Info("Creating collection with indexes")
	createOpt := client.NewCreateCollectionOption(collName, schema).
		WithShardNum(3).
		WithIndexOptions(vecIndexOpt, varcharIndexOpt, jsonIndexOpt)
	err := mc.CreateCollection(ctx, createOpt)
	common.CheckErr(t, err, true)

	// Step 3: Load collection
	log.Info("Loading collection")
	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName).WithReplica(1))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	// Step 4: Insert data with JSON content
	log.Info("Inserting data with JSON fields")
	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	common.CheckErr(t, err, true)

	for i := range numOfBatch {
		insertOpt := hp.TNewDataOption().TWithNb(insertBatchSize).TWithStart(i * insertBatchSize)
		_, insertRes := hp.CollPrepare.InsertData(ctx, t, mc, hp.NewInsertParams(coll.Schema), insertOpt)
		require.Equal(t, insertBatchSize, insertRes.IDs.Len())
	}

	// Flush to ensure data is persisted and JSON stats are generated
	_, err = mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)
	time.Sleep(10 * time.Second)

	// Verify initial data count
	queryRes, err := mc.Query(ctx,
		client.NewQueryOption(collName).
			WithOutputFields(common.QueryCountFieldName).
			WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	initialCount, _ := queryRes.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(insertBatchSize*numOfBatch), initialCount)
	log.Info("Initial data inserted", zap.Int64("count", initialCount))

	// Step 5: Delete some records
	for i := range numOfBatch {
		deleteExpr := fmt.Sprintf("id >= %d and id < %d", insertBatchSize*i, insertBatchSize*i+deleteBatchSize)
		delRes, err := mc.Delete(ctx, client.NewDeleteOption(collName).WithExpr(deleteExpr))
		common.CheckErr(t, err, true)
		require.Equal(t, int64(deleteBatchSize), delRes.DeleteCount)
	}

	// Flush to ensure deletion is persisted
	_, err = mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)
	time.Sleep(30 * time.Second)

	// Verify count after deletion
	queryRes2, err := mc.Query(ctx,
		client.NewQueryOption(collName).
			WithOutputFields(common.QueryCountFieldName).
			WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	countAfterDelete, _ := queryRes2.Fields[0].GetAsInt64(0)
	expectedAfterDelete := int64(insertBatchSize*numOfBatch - deleteBatchSize*numOfBatch)
	require.Equal(t, expectedAfterDelete, countAfterDelete)
	log.Info("Data after deletion", zap.Int64("count", countAfterDelete))

	// Step 6: Create snapshot
	snapshotName := fmt.Sprintf("json_stats_snapshot_%s", common.GenRandomString(snapshotPrefix, 6))
	createSnapshotOpt := client.NewCreateSnapshotOption(snapshotName, collName).
		WithDescription("Snapshot for JSON stats restore testing")

	log.Info("Creating snapshot with JSON stats")
	err = mc.CreateSnapshot(ctx, createSnapshotOpt)
	common.CheckErr(t, err, true)

	// Verify snapshot was created
	listOpt := client.NewListSnapshotsOption().WithCollectionName(collName)
	snapshots, err := mc.ListSnapshots(ctx, listOpt)
	common.CheckErr(t, err, true)
	require.Contains(t, snapshots, snapshotName)

	// Describe snapshot
	describeOpt := client.NewDescribeSnapshotOption(snapshotName)
	snapshotInfo, err := mc.DescribeSnapshot(ctx, describeOpt)
	common.CheckErr(t, err, true)
	require.Equal(t, snapshotName, snapshotInfo.GetName())
	log.Info("Snapshot created", zap.Any("info", snapshotInfo))

	// Step 7: Insert more data after snapshot (to verify snapshot point-in-time)
	for i := range 2 {
		pkStart := insertBatchSize * (numOfBatch + i)
		insertOpt2 := hp.TNewDataOption().TWithNb(insertBatchSize).TWithStart(pkStart)
		_, insertRes2 := hp.CollPrepare.InsertData(ctx, t, mc, hp.NewInsertParams(coll.Schema), insertOpt2)
		require.Equal(t, insertBatchSize, insertRes2.IDs.Len())
	}

	// Step 8: Restore snapshot to a new collection
	restoredCollName := fmt.Sprintf("restored_%s", collName)
	restoreOpt := client.NewRestoreSnapshotOption(snapshotName, restoredCollName)
	log.Info("Restoring snapshot", zap.String("target_collection", restoredCollName))
	_, err = mc.RestoreSnapshot(ctx, restoreOpt)
	common.CheckErr(t, err, true)

	// Step 9: Verify restored collection exists
	has, err := mc.HasCollection(ctx, client.NewHasCollectionOption(restoredCollName))
	common.CheckErr(t, err, true)
	require.True(t, has)

	// Step 10: Load restored collection
	log.Info("Loading restored collection")
	loadTask2, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(restoredCollName).WithReplica(1))
	common.CheckErr(t, err, true)
	err = loadTask2.Await(ctx)
	common.CheckErr(t, err, true)

	// Step 11: Verify restored collection data count matches snapshot point-in-time
	queryRes3, err := mc.Query(ctx,
		client.NewQueryOption(restoredCollName).
			WithOutputFields(common.QueryCountFieldName).
			WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	restoredCount, _ := queryRes3.Fields[0].GetAsInt64(0)
	require.Equal(t, countAfterDelete, restoredCount,
		"Restored collection should have same count as snapshot point-in-time")
	log.Info("Restored collection data verified", zap.Int64("count", restoredCount))

	// Clean up
	dropOpt2 := client.NewDropSnapshotOption(snapshotName)
	err = mc.DropSnapshot(ctx, dropOpt2)
	common.CheckErr(t, err, true)
}

// TestSnapshotRestoreAfterDropPartitionAndCollection tests snapshot restore functionality
// after dropping partitions and the entire collection
func TestSnapshotRestoreAfterDropPartitionAndCollection(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	insertBatchSize := 3000

	// Step 1: Create collection with multiple partitions
	collName := common.GenRandomString(snapshotPrefix, 6)
	schema := client.SimpleCreateCollectionOptions(collName, common.DefaultDim)
	schema.WithAutoID(false)
	schema.WithShardNum(3)
	err := mc.CreateCollection(ctx, schema)
	common.CheckErr(t, err, true)

	// Create 3 partitions
	partitions := []string{"part_0", "part_1", "part_2"}
	for _, partName := range partitions {
		err := mc.CreatePartition(ctx, client.NewCreatePartitionOption(collName, partName))
		common.CheckErr(t, err, true)
	}
	log.Info("Created partitions", zap.Strings("partitions", partitions))

	// Get collection schema
	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	common.CheckErr(t, err, true)

	// Step 2: Insert data into each partition
	log.Info("Inserting data into partitions")
	for i, partName := range partitions {
		insertOpt := hp.TNewDataOption().TWithNb(insertBatchSize).TWithStart(i * insertBatchSize)
		_, insertRes := hp.CollPrepare.InsertData(ctx, t, mc, hp.NewInsertParams(coll.Schema).TWithPartitionName(partName), insertOpt)
		require.Equal(t, insertBatchSize, insertRes.IDs.Len())
	}

	// Flush to ensure data is persisted
	_, err = mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)
	time.Sleep(10 * time.Second)

	// Verify initial data count (3 partitions * 3000 = 9000)
	queryRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ := queryRes.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(9000), count)
	log.Info("Initial data count verified", zap.Int64("count", count))

	// Step 3: Create snapshot
	snapshotName := fmt.Sprintf("drop_test_snapshot_%s", common.GenRandomString(snapshotPrefix, 6))
	createOpt := client.NewCreateSnapshotOption(snapshotName, collName).
		WithDescription("Snapshot for testing restore after drop operations")

	log.Info("Creating snapshot")
	err = mc.CreateSnapshot(ctx, createOpt)
	common.CheckErr(t, err, true)

	// Verify snapshot was created
	listOpt := client.NewListSnapshotsOption().WithCollectionName(collName)
	snapshots, err := mc.ListSnapshots(ctx, listOpt)
	common.CheckErr(t, err, true)
	require.Contains(t, snapshots, snapshotName)

	describeOpt := client.NewDescribeSnapshotOption(snapshotName)
	snapshotInfo, err := mc.DescribeSnapshot(ctx, describeOpt)
	common.CheckErr(t, err, true)
	require.Equal(t, snapshotName, snapshotInfo.GetName())
	log.Info("Snapshot created", zap.Any("info", snapshotInfo))

	// Step 4: Test scenario 1 - Drop partition and restore
	log.Info("Test scenario 1: Drop partition and restore")

	// Release the partition before dropping it
	dropPartName := "part_0"
	err = mc.ReleasePartitions(ctx, client.NewReleasePartitionsOptions(collName, dropPartName))
	common.CheckErr(t, err, true)
	log.Info("Released partition", zap.String("partition", dropPartName))

	// Drop one partition
	err = mc.DropPartition(ctx, client.NewDropPartitionOption(collName, dropPartName))
	common.CheckErr(t, err, true)
	log.Info("Dropped partition", zap.String("partition", dropPartName))

	// Wait for partition drop to take effect
	time.Sleep(5 * time.Second)

	// Verify remaining data count (2 partitions * 3000 = 6000)
	queryRes2, err := mc.Query(ctx, client.NewQueryOption(collName).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	remainingCount, _ := queryRes2.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(6000), remainingCount)
	log.Info("Data count after dropping partition", zap.Int64("count", remainingCount))

	// Restore snapshot to new collection (v1)
	restoredCollNameV1 := fmt.Sprintf("restored_v1_%s", collName)
	restoreOptV1 := client.NewRestoreSnapshotOption(snapshotName, restoredCollNameV1)
	log.Info("Restoring snapshot after partition drop", zap.String("target", restoredCollNameV1))
	_, err = mc.RestoreSnapshot(ctx, restoreOptV1)
	common.CheckErr(t, err, true)

	// Verify restored collection v1 exists
	hasV1, err := mc.HasCollection(ctx, client.NewHasCollectionOption(restoredCollNameV1))
	common.CheckErr(t, err, true)
	require.True(t, hasV1)

	// Load restored collection v1
	loadTaskV1, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(restoredCollNameV1).WithReplica(1))
	common.CheckErr(t, err, true)
	err = loadTaskV1.Await(ctx)
	common.CheckErr(t, err, true)

	// Verify restored collection v1 has all original data (9000 records)
	queryResV1, err := mc.Query(ctx,
		client.NewQueryOption(restoredCollNameV1).
			WithOutputFields(common.QueryCountFieldName).
			WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	restoredCountV1, _ := queryResV1.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(9000), restoredCountV1)
	log.Info("Restored collection v1 data verified", zap.Int64("count", restoredCountV1))

	// Verify all partitions are restored
	restoredPartitionsV1, err := mc.ListPartitions(ctx, client.NewListPartitionOption(restoredCollNameV1))
	common.CheckErr(t, err, true)
	filteredPartitionsV1 := make([]string, 0)
	for _, partName := range restoredPartitionsV1 {
		if partName != "_default" {
			filteredPartitionsV1 = append(filteredPartitionsV1, partName)
		}
	}
	sort.Strings(filteredPartitionsV1)
	require.Equal(t, partitions, filteredPartitionsV1)
	log.Info("All partitions restored in v1", zap.Strings("partitions", filteredPartitionsV1))

	// Step 5: Test scenario 2 - Drop entire collection and restore
	log.Info("Test scenario 2: Drop entire collection and restore")

	// Drop the original collection
	err = mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
	common.CheckErr(t, err, true)
	log.Info("Dropped entire collection", zap.String("collection", collName))

	// Wait for collection drop to take effect
	time.Sleep(5 * time.Second)

	// Verify collection no longer exists
	hasOriginal, err := mc.HasCollection(ctx, client.NewHasCollectionOption(collName))
	common.CheckErr(t, err, true)
	require.False(t, hasOriginal)
	log.Info("Verified collection is dropped")

	// Restore snapshot to new collection (v2)
	restoredCollNameV2 := fmt.Sprintf("restored_v2_%s", collName)
	restoreOptV2 := client.NewRestoreSnapshotOption(snapshotName, restoredCollNameV2)
	log.Info("Restoring snapshot after collection drop", zap.String("target", restoredCollNameV2))
	_, err = mc.RestoreSnapshot(ctx, restoreOptV2)
	common.CheckErr(t, err, true)

	// Verify restored collection v2 exists
	hasV2, err := mc.HasCollection(ctx, client.NewHasCollectionOption(restoredCollNameV2))
	common.CheckErr(t, err, true)
	require.True(t, hasV2)

	// Load restored collection v2
	loadTaskV2, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(restoredCollNameV2).WithReplica(1))
	common.CheckErr(t, err, true)
	err = loadTaskV2.Await(ctx)
	common.CheckErr(t, err, true)

	// Verify restored collection v2 has all original data (9000 records)
	queryResV2, err := mc.Query(ctx,
		client.NewQueryOption(restoredCollNameV2).
			WithOutputFields(common.QueryCountFieldName).
			WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	restoredCountV2, _ := queryResV2.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(9000), restoredCountV2)
	log.Info("Restored collection v2 data verified", zap.Int64("count", restoredCountV2))

	// Verify all partitions are restored
	restoredPartitionsV2, err := mc.ListPartitions(ctx, client.NewListPartitionOption(restoredCollNameV2))
	common.CheckErr(t, err, true)
	filteredPartitionsV2 := make([]string, 0)
	for _, partName := range restoredPartitionsV2 {
		if partName != "_default" {
			filteredPartitionsV2 = append(filteredPartitionsV2, partName)
		}
	}
	sort.Strings(filteredPartitionsV2)
	require.Equal(t, partitions, filteredPartitionsV2)
	log.Info("All partitions restored in v2", zap.Strings("partitions", filteredPartitionsV2))

	log.Info("Test completed successfully",
		zap.String("snapshot", snapshotName),
		zap.String("restored_v1", restoredCollNameV1),
		zap.String("restored_v2", restoredCollNameV2))

	// Clean up
	dropSnapshotOpt := client.NewDropSnapshotOption(snapshotName)
	err = mc.DropSnapshot(ctx, dropSnapshotOpt)
	common.CheckErr(t, err, true)
}

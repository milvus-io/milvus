package testcases

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v2/entity"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

var snapshotPrefix = "snapshot"

// TestCreateSnapshot tests creating a snapshot for a collection
func TestCreateSnapshot(t *testing.T) {
	t.Parallel()
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

// TestSnapshotRestoreWithDataOperations tests the complete snapshot restore workflow with data operations
func TestSnapshotRestoreWithDataOperations(t *testing.T) {
	t.Skip("Restore snapshot is not supported yet")
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// Step 1: Create collection and insert initial 3000 records
	collName := common.GenRandomString(snapshotPrefix, 6)
	schema := client.SimpleCreateCollectionOptions(collName, common.DefaultDim)
	schema.WithAutoID(false)
	err := mc.CreateCollection(ctx, schema)
	common.CheckErr(t, err, true)

	// Get collection schema
	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	common.CheckErr(t, err, true)

	// Insert 3000 records
	insertOpt := hp.TNewDataOption().TWithNb(3000).TWithStart(0)
	_, insertRes := hp.CollPrepare.InsertData(ctx, t, mc, hp.NewInsertParams(coll.Schema), insertOpt)
	require.Equal(t, 3000, insertRes.IDs.Len())

	// Verify initial data count
	queryRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ := queryRes.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(3000), count)

	// Delete 1000 records (records with id < 1000)
	deleteExpr := "id < 1000"
	delRes, err := mc.Delete(ctx, client.NewDeleteOption(collName).WithExpr(deleteExpr))
	common.CheckErr(t, err, true)
	require.Equal(t, int64(1000), delRes.DeleteCount)

	// Flush to ensure deletion is persisted
	_, err = mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)

	// Verify data count after deletion
	queryRes2, err := mc.Query(ctx, client.NewQueryOption(collName).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ = queryRes2.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(2000), count)

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

	// Step 3: Continue inserting 3000 more records and delete 1000 records
	// Insert 3000 more records (starting from 3000)
	insertOpt2 := hp.TNewDataOption().TWithNb(3000).TWithStart(3000)
	_, insertRes2 := hp.CollPrepare.InsertData(ctx, t, mc, hp.NewInsertParams(coll.Schema), insertOpt2)
	require.Equal(t, 3000, insertRes2.IDs.Len())

	// Verify total data count after second insertion
	queryRes3, err := mc.Query(ctx, client.NewQueryOption(collName).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ = queryRes3.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(5000), count)

	// Delete 1000 more records (records with id >= 2500 and id < 3500)
	deleteExpr2 := "id >= 2500 and id < 3500"
	delRes2, err := mc.Delete(ctx, client.NewDeleteOption(collName).WithExpr(deleteExpr2))
	common.CheckErr(t, err, true)
	require.Equal(t, int64(1000), delRes2.DeleteCount)

	time.Sleep(10 * time.Second)
	// Flush to ensure deletion is persisted
	_, err = mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)

	// Verify final data count in original collection
	queryRes4, err := mc.Query(ctx, client.NewQueryOption(collName).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ = queryRes4.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(4000), count)

	// Step 4: Restore snapshot to a new collection
	restoredCollName := fmt.Sprintf("restored_%s", collName)
	restoreOpt := client.NewRestoreSnapshotOption(snapshotName, restoredCollName)
	err = mc.RestoreSnapshot(ctx, restoreOpt)
	common.CheckErr(t, err, true)

	// Verify restored collection exists
	has, err := mc.HasCollection(ctx, client.NewHasCollectionOption(restoredCollName))
	common.CheckErr(t, err, true)
	require.True(t, has)

	// Verify restored collection data count (should be 2000 records from snapshot)
	queryRes5, err := mc.Query(ctx, client.NewQueryOption(restoredCollName).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ = queryRes5.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(2000), count)

	// Verify the restored collection has the correct data by querying a few records
	// Query should return records with id >= 1000 and id < 3000 (the 2000 records that were in the snapshot)
	queryExpr := "id >= 1000 and id < 3000"
	queryRes6, err := mc.Query(ctx, client.NewQueryOption(restoredCollName).WithFilter(queryExpr).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ = queryRes6.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(2000), count)

	// Verify that records with id < 1000 are not in the restored collection (they were deleted before snapshot)
	queryExpr2 := "id < 1000"
	queryRes7, err := mc.Query(ctx, client.NewQueryOption(restoredCollName).WithFilter(queryExpr2).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ = queryRes7.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(0), count)

	// Clean up
	dropOpt := client.NewDropSnapshotOption(snapshotName)
	err = mc.DropSnapshot(ctx, dropOpt)
	common.CheckErr(t, err, true)
}

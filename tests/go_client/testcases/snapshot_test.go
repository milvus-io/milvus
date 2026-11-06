package testcases

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/bulkwriter"
	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/internal/snapshotio"
	pkcommon "github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

var snapshotPrefix = "snapshot"

// flushWithRetry retries Flush if it hits the collection-level rate limiter (default: 0.1 rps),
// and awaits flush completion before returning.
func flushWithRetry(ctx context.Context, mc *base.MilvusClient, collName string) error {
	maxRetries := 5
	for i := 0; i < maxRetries; i++ {
		flushTask, err := mc.Flush(ctx, client.NewFlushOption(collName))
		if err == nil {
			return flushTask.Await(ctx)
		}
		if i < maxRetries-1 {
			mlog.Info(ctx, "flush rate limited, retrying",
				mlog.String("collection", collName),
				mlog.Int("attempt", i+1),
				mlog.Err(err))
			time.Sleep(5 * time.Second)
			continue
		}
		return err
	}
	return nil
}

// waitForRestoreComplete polls GetRestoreSnapshotState until the restore job completes or fails.
// Returns the final RestoreSnapshotInfo and any error.
func waitForRestoreComplete(ctx context.Context, mc *base.MilvusClient, jobID int64, timeout time.Duration) (*milvuspb.RestoreSnapshotInfo, error) {
	deadline := time.Now().Add(timeout)
	pollInterval := 1 * time.Second

	for time.Now().Before(deadline) {
		opt := client.NewGetRestoreSnapshotStateOption(jobID)
		info, err := mc.GetRestoreSnapshotState(ctx, opt)
		if err != nil {
			return nil, fmt.Errorf("failed to get restore state: %w", err)
		}

		switch info.GetState() {
		case milvuspb.RestoreSnapshotState_RestoreSnapshotCompleted:
			mlog.Info(ctx, "restore snapshot completed",
				mlog.FieldJobID(jobID),
				mlog.FieldCollectionName(info.GetCollectionName()))
			return info, nil
		case milvuspb.RestoreSnapshotState_RestoreSnapshotFailed:
			return info, fmt.Errorf("restore snapshot failed: jobID=%d, reason=%s", jobID, info.GetReason())
		default:
			// Still pending or executing
			mlog.Info(ctx, "waiting for restore to complete",
				mlog.FieldJobID(jobID),
				mlog.String("state", info.GetState().String()),
				mlog.Int32("progress", info.GetProgress()))
			time.Sleep(pollInterval)
		}
	}

	return nil, fmt.Errorf("timeout waiting for restore to complete: jobID=%d", jobID)
}

func waitForExportComplete(ctx context.Context, mc *base.MilvusClient, jobID int64, timeout time.Duration) (*milvuspb.ExportSnapshotInfo, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		info, err := mc.GetExportSnapshotState(ctx, client.NewGetExportSnapshotStateOption(jobID))
		if err != nil {
			return nil, fmt.Errorf("failed to get export state: %w", err)
		}
		switch info.GetState() {
		case milvuspb.ExportSnapshotState_ExportSnapshotCompleted:
			return info, nil
		case milvuspb.ExportSnapshotState_ExportSnapshotFailed:
			return info, fmt.Errorf("export snapshot failed: jobID=%d, reason=%s", jobID, info.GetReason())
		default:
			time.Sleep(time.Second)
		}
	}
	return nil, fmt.Errorf("timeout waiting for export to complete: jobID=%d", jobID)
}

func waitForSnapshotImportState(
	ctx context.Context,
	baseURL string,
	apiKey string,
	jobID string,
	wantState string,
	timeout time.Duration,
) (*bulkwriter.ImportProgressData, error) {
	deadline := time.Now().Add(timeout)
	var lastState string
	for time.Now().Before(deadline) {
		resp, err := bulkwriter.GetImportProgress(ctx,
			bulkwriter.NewGetImportProgressOption(baseURL, jobID).WithAPIKey(apiKey))
		if err != nil {
			return nil, fmt.Errorf("failed to get import state for job %s: %w", jobID, err)
		}
		if resp.Data == nil {
			return nil, fmt.Errorf("import state response has no data for job %s", jobID)
		}
		lastState = resp.Data.State
		if lastState == wantState {
			return resp.Data, nil
		}
		if lastState == "Failed" {
			return resp.Data, fmt.Errorf("import job %s failed: %s", jobID, resp.Data.Reason)
		}
		time.Sleep(2 * time.Second)
	}
	return nil, fmt.Errorf("timeout waiting for import job %s to reach %s, last state: %s", jobID, wantState, lastState)
}

func snapshotImportRESTConfig() (string, string) {
	baseURL := strings.TrimRight(hp.GetURI(), "/")
	if !strings.Contains(baseURL, "://") {
		baseURL = "http://" + baseURL
	}
	apiKey := hp.GetToken()
	if apiKey == "" && (hp.GetUser() != "" || hp.GetPassword() != "") {
		apiKey = hp.GetUser() + ":" + hp.GetPassword()
	}
	return baseURL, apiKey
}

func relocateSnapshotBundle(
	ctx context.Context,
	minioClient *miniogo.Client,
	bucket string,
	metadataURI string,
	sourcePrefix string,
	targetPrefix string,
) (string, error) {
	sourcePrefix = strings.TrimRight(sourcePrefix, "/") + "/"
	targetPrefix = strings.TrimRight(targetPrefix, "/") + "/"

	sourceObjects := make([]string, 0)
	metadataObject := ""
	for object := range minioClient.ListObjects(ctx, bucket, miniogo.ListObjectsOptions{
		Prefix:    sourcePrefix,
		Recursive: true,
	}) {
		if object.Err != nil {
			return "", fmt.Errorf("failed to list snapshot export object: %w", object.Err)
		}
		sourceObjects = append(sourceObjects, object.Key)
		if strings.HasSuffix(metadataURI, object.Key) {
			metadataObject = object.Key
		}
	}
	if len(sourceObjects) == 0 {
		return "", fmt.Errorf("snapshot export prefix %s is empty", sourcePrefix)
	}
	if metadataObject == "" {
		return "", fmt.Errorf("snapshot metadata URI %s does not identify an object under %s", metadataURI, sourcePrefix)
	}

	relocatedMetadataObject := ""
	for _, sourceObject := range sourceObjects {
		relativePath := strings.TrimPrefix(sourceObject, sourcePrefix)
		if relativePath == sourceObject {
			return "", fmt.Errorf("snapshot export object %s is outside prefix %s", sourceObject, sourcePrefix)
		}
		targetObject := targetPrefix + relativePath
		_, err := minioClient.CopyObject(ctx,
			miniogo.CopyDestOptions{Bucket: bucket, Object: targetObject},
			miniogo.CopySrcOptions{Bucket: bucket, Object: sourceObject})
		if err != nil {
			return "", fmt.Errorf("failed to relocate snapshot object %s to %s: %w", sourceObject, targetObject, err)
		}
		if _, err := minioClient.StatObject(ctx, bucket, targetObject, miniogo.StatObjectOptions{}); err != nil {
			return "", fmt.Errorf("failed to verify relocated snapshot object %s: %w", targetObject, err)
		}
		if sourceObject == metadataObject {
			relocatedMetadataObject = targetObject
		}
	}

	// Remove the original bundle before Import starts so success proves that
	// SnapshotReader rebases every self-contained reference to the new root.
	for _, sourceObject := range sourceObjects {
		if err := minioClient.RemoveObject(ctx, bucket, sourceObject, miniogo.RemoveObjectOptions{}); err != nil {
			return "", fmt.Errorf("failed to remove original snapshot object %s: %w", sourceObject, err)
		}
	}
	if object, ok := <-minioClient.ListObjects(ctx, bucket, miniogo.ListObjectsOptions{
		Prefix:    sourcePrefix,
		Recursive: true,
	}); ok {
		if object.Err != nil {
			return "", fmt.Errorf("failed to verify original snapshot prefix removal: %w", object.Err)
		}
		return "", fmt.Errorf("original snapshot object still exists after relocation: %s", object.Key)
	}

	return strings.TrimSuffix(metadataURI, metadataObject) + relocatedMetadataObject, nil
}

func newSnapshotImportSchema() *entity.Schema {
	return entity.NewSchema().
		WithDynamicFieldEnabled(false).
		WithField(entity.NewField().
			WithName("id").
			WithDataType(entity.FieldTypeInt64).
			WithIsPrimaryKey(true)).
		WithField(entity.NewField().
			WithName("tag").
			WithDataType(entity.FieldTypeVarChar).
			WithMaxLength(64)).
		WithField(entity.NewField().
			WithName("phase").
			WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().
			WithName("document").
			WithDataType(entity.FieldTypeText)).
		WithField(entity.NewField().
			WithName("vector").
			WithDataType(entity.FieldTypeFloatVector).
			WithDim(8))
}

// waitForAllIndexesBuilt polls DescribeIndex for each index in the collection until all indexes
// have finished building (PendingIndexRows == 0 and TotalRows == IndexedRows).
// If the collection has no indexes, the function returns immediately.
func waitForAllIndexesBuilt(ctx context.Context, mc *base.MilvusClient, collName string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	pollInterval := 2 * time.Second

	// List indexes once — the set of indexes doesn't change during building.
	indexes, err := mc.ListIndexes(ctx, client.NewListIndexOption(collName))
	if err != nil {
		return fmt.Errorf("failed to list indexes for collection %s: %w", collName, err)
	}
	if len(indexes) == 0 {
		return nil
	}

	for time.Now().Before(deadline) {
		allFinished := true
		for _, idxName := range indexes {
			descIdx, err := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(collName, idxName))
			if err != nil {
				return fmt.Errorf("failed to describe index %s on collection %s: %w", idxName, collName, err)
			}
			if descIdx.PendingIndexRows != 0 || descIdx.TotalRows != descIdx.IndexedRows {
				mlog.Info(ctx, "index not yet finished",
					mlog.String("collection", collName),
					mlog.String("index", idxName),
					mlog.Int64("pendingRows", descIdx.PendingIndexRows),
					mlog.Int64("totalRows", descIdx.TotalRows),
					mlog.Int64("indexedRows", descIdx.IndexedRows))
				allFinished = false
				break
			}
		}
		if allFinished {
			mlog.Info(ctx, "all indexes built", mlog.String("collection", collName), mlog.Int("numIndexes", len(indexes)))
			// Allow extra time for segment state settling (compaction, delta merge)
			// after indexes are built but before snapshot can see all segments.
			time.Sleep(5 * time.Second)
			return nil
		}
		time.Sleep(pollInterval)
	}
	return fmt.Errorf("timeout waiting for indexes to be built on collection %s", collName)
}

// TestCreateSnapshot tests creating a snapshot for a collection
func TestCreateSnapshot(t *testing.T) {
	// Heavy case (large data volume + minute-scale index/refresh/restore
	// waits): intentionally NOT run in parallel. Under t.Parallel() it competes
	// with the rest of the suite for the shared standalone cluster and flakes on
	// those timeouts; keep it serial so it gets the resources it needs.

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// Create a collection first
	collName := common.GenRandomString(snapshotPrefix, 6)
	err := mc.CreateCollection(ctx, client.SimpleCreateCollectionOptions(collName, common.DefaultDim))
	common.CheckErr(t, err, true)
	t.Cleanup(func() {
		_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

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
	listOpt := client.NewListSnapshotsOption(collName)
	snapshots, err := mc.ListSnapshots(ctx, listOpt)
	common.CheckErr(t, err, true)
	require.Contains(t, snapshots, snapshotName)

	// Describe the snapshot
	describeOpt := client.NewDescribeSnapshotOption(snapshotName, collName)
	resp, err := mc.DescribeSnapshot(ctx, describeOpt)
	common.CheckErr(t, err, true)
	require.Equal(t, snapshotName, resp.GetName())
	require.Equal(t, collName, resp.GetCollectionName())
	require.Equal(t, "Test snapshot for e2e testing", resp.GetDescription())
	require.Greater(t, resp.GetCreateTs(), int64(0))

	// Clean up
	dropOpt := client.NewDropSnapshotOption(snapshotName, collName)
	err = mc.DropSnapshot(ctx, dropOpt)
	common.CheckErr(t, err, true)
}

// TestSnapshotRestoreWithMultiSegment tests the complete snapshot restore workflow with data operations
func TestSnapshotRestoreWithMultiSegment(t *testing.T) {
	// Heavy case (large data volume + minute-scale index/refresh/restore
	// waits): intentionally NOT run in parallel. Under t.Parallel() it competes
	// with the rest of the suite for the shared standalone cluster and flakes on
	// those timeouts; keep it serial so it gets the resources it needs.

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	insertBatchSize := 20000
	deleteBatchSize := 5000
	numOfBatch := 5

	// Step 1: Create collection and insert initial 3000 records
	collName := common.GenRandomString(snapshotPrefix, 6)
	schema := client.SimpleCreateCollectionOptions(collName, common.DefaultDim)
	schema.WithAutoID(false)
	schema.WithShardNum(4)
	err := mc.CreateCollection(ctx, schema)
	common.CheckErr(t, err, true)
	collectionsToClean := []string{collName}
	t.Cleanup(func() {
		for _, c := range collectionsToClean {
			_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(c))
		}
	})

	// Get collection schema
	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	common.CheckErr(t, err, true)

	// Insert records
	for i := 0; i < numOfBatch; i++ {
		insertOpt := hp.TNewDataOption().TWithNb(insertBatchSize).TWithStart(i * insertBatchSize)
		_, insertRes := hp.CollPrepare.InsertData(ctx, t, mc, hp.NewInsertParams(coll.Schema), insertOpt)
		require.Equal(t, insertBatchSize, insertRes.IDs.Len())
	}
	// Flush to ensure data is persisted
	err = flushWithRetry(ctx, mc, collName)
	common.CheckErr(t, err, true)
	// Wait for all indexes to be built after flush
	err = waitForAllIndexesBuilt(ctx, mc, collName, 2*time.Minute)
	common.CheckErr(t, err, true)

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
	err = flushWithRetry(ctx, mc, collName)
	common.CheckErr(t, err, true)

	// Verify data count after deletion
	queryRes2, err := mc.Query(ctx, client.NewQueryOption(collName).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ = queryRes2.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(75000), count)

	// Step 2: Create snapshot
	snapshotName := fmt.Sprintf("restore_snapshot_%s", common.GenRandomString(snapshotPrefix, 6))
	createOpt := client.NewCreateSnapshotOption(snapshotName, collName).
		WithDescription("Snapshot for restore testing with 2000 records")

	err = mc.CreateSnapshot(ctx, createOpt)
	common.CheckErr(t, err, true)

	// Verify snapshot was created
	listOpt := client.NewListSnapshotsOption(collName)
	snapshots, err := mc.ListSnapshots(ctx, listOpt)
	common.CheckErr(t, err, true)
	require.Contains(t, snapshots, snapshotName)

	// print snapshot info
	describeOpt := client.NewDescribeSnapshotOption(snapshotName, collName)
	snapshotInfo, err := mc.DescribeSnapshot(ctx, describeOpt)
	common.CheckErr(t, err, true)
	require.Equal(t, snapshotName, snapshotInfo.GetName())
	mlog.Info(context.TODO(), "check snapshot info", mlog.Any("info", snapshotInfo))

	// Step 3: Continue inserting more records after snapshot to verify point-in-time restore
	postSnapshotBatches := 2
	for i := 0; i < postSnapshotBatches; i++ {
		pkStart := insertBatchSize * (numOfBatch + i)
		insertOpt2 := hp.TNewDataOption().TWithNb(insertBatchSize).TWithStart(pkStart)
		_, insertRes2 := hp.CollPrepare.InsertData(ctx, t, mc, hp.NewInsertParams(coll.Schema), insertOpt2)
		require.Equal(t, insertBatchSize, insertRes2.IDs.Len())
	}

	// Verify total data count after second insertion
	queryRes3, err := mc.Query(ctx, client.NewQueryOption(collName).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ = queryRes3.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(115000), count)

	// Step 4: Restore snapshot to a new collection
	restoredCollName := fmt.Sprintf("restored_%s", collName)
	collectionsToClean = append(collectionsToClean, restoredCollName)
	restoreOpt := client.NewRestoreSnapshotOption(snapshotName, collName, restoredCollName)
	jobID, err := mc.RestoreSnapshot(ctx, restoreOpt)
	common.CheckErr(t, err, true)

	// Wait for restore to complete
	_, err = waitForRestoreComplete(ctx, mc, jobID, 1*time.Minute)
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

	// Verify restored partition data count
	queryRes5, err := mc.Query(ctx,
		client.NewQueryOption(restoredCollName).
			WithOutputFields(common.QueryCountFieldName).
			WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ = queryRes5.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(75000), count)

	// Clean up
	dropOpt := client.NewDropSnapshotOption(snapshotName, collName)
	err = mc.DropSnapshot(ctx, dropOpt)
	common.CheckErr(t, err, true)
}

// TestSnapshotRestoreExternalReferenced restores directly from CreateSnapshot metadata.
// This covers the referenced layout, where metadata still points at the original
// Milvus storage files instead of an exported self-contained bundle.
func TestSnapshotRestoreExternalReferenced(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	insertBatchSize := 1000
	collName := common.GenRandomString(snapshotPrefix, 6)
	schema := client.SimpleCreateCollectionOptions(collName, common.DefaultDim)
	schema.WithAutoID(false)
	err := mc.CreateCollection(ctx, schema)
	common.CheckErr(t, err, true)

	collectionsToClean := []string{collName}
	t.Cleanup(func() {
		for _, c := range collectionsToClean {
			_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(c))
		}
	})

	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	common.CheckErr(t, err, true)

	insertOpt := hp.TNewDataOption().TWithNb(insertBatchSize)
	_, insertRes := hp.CollPrepare.InsertData(ctx, t, mc, hp.NewInsertParams(coll.Schema), insertOpt)
	require.Equal(t, insertBatchSize, insertRes.IDs.Len())

	err = flushWithRetry(ctx, mc, collName)
	common.CheckErr(t, err, true)
	err = waitForAllIndexesBuilt(ctx, mc, collName, 2*time.Minute)
	common.CheckErr(t, err, true)

	snapshotName := fmt.Sprintf("external_restore_snapshot_%s", common.GenRandomString(snapshotPrefix, 6))
	err = mc.CreateSnapshot(ctx, client.NewCreateSnapshotOption(snapshotName, collName).
		WithDescription("Snapshot for external restore testing"))
	common.CheckErr(t, err, true)

	snapshotInfo, err := mc.DescribeSnapshot(ctx, client.NewDescribeSnapshotOption(snapshotName, collName))
	common.CheckErr(t, err, true)
	require.Equal(t, snapshotName, snapshotInfo.GetName())
	require.NotEmpty(t, snapshotInfo.GetS3Location())

	restoredCollName := fmt.Sprintf("restored_external_%s", collName)
	collectionsToClean = append(collectionsToClean, restoredCollName)
	jobID, err := mc.RestoreExternalSnapshot(ctx,
		client.NewRestoreExternalSnapshotOption(
			restoredCollName,
			snapshotInfo.GetS3Location(),
		))
	common.CheckErr(t, err, true)

	_, err = waitForRestoreComplete(ctx, mc, jobID, 1*time.Minute)
	common.CheckErr(t, err, true)

	has, err := mc.HasCollection(ctx, client.NewHasCollectionOption(restoredCollName))
	common.CheckErr(t, err, true)
	require.True(t, has)

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(restoredCollName).WithReplica(1))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	queryRes, err := mc.Query(ctx,
		client.NewQueryOption(restoredCollName).
			WithOutputFields(common.QueryCountFieldName).
			WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ := queryRes.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(insertBatchSize), count)

	err = mc.DropSnapshot(ctx, client.NewDropSnapshotOption(snapshotName, collName))
	common.CheckErr(t, err, true)
}

// TestSnapshotRestoreExternalSelfContained restores from ExportSnapshot output.
// This covers the self-contained bundle layout under targetRoot/snapshots and
// targetRoot/files.
func TestSnapshotRestoreExternalSelfContained(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	insertBatchSize := 1000
	collName := common.GenRandomString(snapshotPrefix, 6)
	schema := client.SimpleCreateCollectionOptions(collName, common.DefaultDim)
	schema.WithAutoID(false)
	err := mc.CreateCollection(ctx, schema)
	common.CheckErr(t, err, true)

	collectionsToClean := []string{collName}
	t.Cleanup(func() {
		for _, c := range collectionsToClean {
			_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(c))
		}
	})

	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	common.CheckErr(t, err, true)

	insertOpt := hp.TNewDataOption().TWithNb(insertBatchSize)
	_, insertRes := hp.CollPrepare.InsertData(ctx, t, mc, hp.NewInsertParams(coll.Schema), insertOpt)
	require.Equal(t, insertBatchSize, insertRes.IDs.Len())

	err = flushWithRetry(ctx, mc, collName)
	common.CheckErr(t, err, true)
	err = waitForAllIndexesBuilt(ctx, mc, collName, 2*time.Minute)
	common.CheckErr(t, err, true)

	snapshotName := fmt.Sprintf("external_export_snapshot_%s", common.GenRandomString(snapshotPrefix, 6))
	err = mc.CreateSnapshot(ctx, client.NewCreateSnapshotOption(snapshotName, collName).
		WithDescription("Snapshot for exported external restore testing"))
	common.CheckErr(t, err, true)

	snapshotInfo, err := mc.DescribeSnapshot(ctx, client.NewDescribeSnapshotOption(snapshotName, collName))
	common.CheckErr(t, err, true)
	require.Equal(t, snapshotName, snapshotInfo.GetName())
	require.NotEmpty(t, snapshotInfo.GetS3Location())

	exportRoot := fmt.Sprintf("snapshot_export_%s", common.GenRandomString(snapshotPrefix, 6))
	exportJobID, err := mc.ExportSnapshot(ctx,
		client.NewExportSnapshotOption(snapshotName, collName, exportRoot))
	common.CheckErr(t, err, true)
	require.NotZero(t, exportJobID)
	exportInfo, err := waitForExportComplete(ctx, mc, exportJobID, 2*time.Minute)
	common.CheckErr(t, err, true)
	require.Positive(t, exportInfo.GetTotalBytes())
	metadataURI := exportInfo.GetSnapshotMetadataUri()
	require.NotEmpty(t, metadataURI)
	require.NotEqual(t, snapshotInfo.GetS3Location(), metadataURI)

	restoredCollName := fmt.Sprintf("restored_export_%s", collName)
	collectionsToClean = append(collectionsToClean, restoredCollName)
	jobID, err := mc.RestoreExternalSnapshot(ctx,
		client.NewRestoreExternalSnapshotOption(
			restoredCollName,
			metadataURI,
		))
	common.CheckErr(t, err, true)

	_, err = waitForRestoreComplete(ctx, mc, jobID, 1*time.Minute)
	common.CheckErr(t, err, true)

	has, err := mc.HasCollection(ctx, client.NewHasCollectionOption(restoredCollName))
	common.CheckErr(t, err, true)
	require.True(t, has)

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(restoredCollName).WithReplica(1))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	queryRes, err := mc.Query(ctx,
		client.NewQueryOption(restoredCollName).
			WithOutputFields(common.QueryCountFieldName).
			WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ := queryRes.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(insertBatchSize), count)

	err = mc.DropSnapshot(ctx, client.NewDropSnapshotOption(snapshotName, collName))
	common.CheckErr(t, err, true)
}

func TestImportStorageV3SnapshotSource(t *testing.T) {
	runStorageV3SnapshotImport(t, false, false, "")
}

func TestImportStorageV3SnapshotSourceL0(t *testing.T) {
	runStorageV3SnapshotImport(t, true, false, "")
}

func TestImportStorageV3SnapshotSourceCrossBucket(t *testing.T) {
	t.Run("without_l0", func(t *testing.T) { runStorageV3SnapshotImport(t, false, true, "") })
	t.Run("with_l0", func(t *testing.T) { runStorageV3SnapshotImport(t, true, true, "") })
}

func TestImportStorageV3SnapshotSourceBackupSchema(t *testing.T) {
	// Both layouts exercise manual commit. The first case keeps source PKs
	// despite target AutoID and fills missing optional/dynamic fields; the second
	// projects away source-only columns while folding L0 deletes.
	t.Run("target_optional", func(t *testing.T) { runStorageV3SnapshotImport(t, false, false, "target_optional") })
	t.Run("source_extra_l0", func(t *testing.T) { runStorageV3SnapshotImport(t, true, false, "source_extra") })
}

func TestImportStorageV3SnapshotSourcePartitionMapping(t *testing.T) {
	runStorageV3SnapshotImport(t, false, false, "partition_mapping")
}

// Ignoring a plaintext source's EZK needs no cipher plugin and runs in the ordinary
// suite. The source must actually be plaintext, not merely in a database whose
// name happens to be "default".
func TestImportStorageV3SnapshotSourcePlaintextIgnoresEZK(t *testing.T) {
	ctx := hp.CreateContext(t, 10*time.Minute)
	db, mc := createSnapshotImportPlaintextDatabase(t, ctx)
	for _, layout := range []string{"referenced", "self_contained"} {
		t.Run(layout, func(t *testing.T) {
			for _, tc := range []struct{ name, ezk string }{
				{name: "valid", ezk: base64.StdEncoding.EncodeToString([]byte(`{"ez_id":1}`))},
				{name: "malformed", ezk: "not-base64"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					runSnapshotImportPlaintextIgnoresEZK(t, ctx, mc, db, layout, tc.ezk)
				})
			}
		})
	}
}

// Use test-owned databases even when the cluster encrypts new databases by
// default. Child-case cleanup runs before these parent database cleanups.
func createSnapshotImportPlaintextDatabase(t *testing.T, ctx context.Context) (string, *base.MilvusClient) {
	t.Helper()
	admin := hp.CreateDefaultMilvusClient(ctx, t)
	db := common.GenRandomString("snapshot_import_db", 8)
	require.NoError(t, admin.CreateDatabase(ctx, client.NewCreateDatabaseOption(db).
		WithProperty(pkcommon.EncryptionEnabledKey, "false")))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		assert.NoError(t, admin.DropDatabase(cleanupCtx, client.NewDropDatabaseOption(db)))
	})
	return db, hp.CreateMilvusClient(ctx, t, &client.ClientConfig{DBName: db})
}

func runSnapshotImportPlaintextIgnoresEZK(t *testing.T, ctx context.Context, mc *base.MilvusClient,
	targetDB, layout, ezk string,
) {
	t.Helper()
	sourceName := common.GenRandomString("snapshot_plaintext_ezk_source", 8)
	targetName := common.GenRandomString("snapshot_plaintext_ezk_target", 8)
	snapshotName := common.GenRandomString("snapshot_plaintext_ezk", 8)
	baseURL, apiKey := snapshotImportRESTConfig()
	jobID := ""
	snapshotCreated := false
	sourceCreated, targetCreated := false, false
	var exportClient *miniogo.Client
	var exportBucket, exportPrefix string
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if jobID != "" {
			_, err := snapshotImportDatabaseRequest(cleanupCtx, baseURL, apiKey, targetDB, "abort", map[string]any{"jobId": jobID})
			assert.NoError(t, err)
		}
		if snapshotCreated {
			// Export publishes Completed before asynchronously releasing its pin.
			// Retry only that precise transient state; do not force-unpin a job
			// or hide unrelated cleanup errors. This is not a data-GC wait.
			pinCtx, cancelPinWait := context.WithTimeout(cleanupCtx, 20*time.Second)
			var dropErr error
		pinWait:
			for {
				dropErr = mc.DropSnapshot(pinCtx, client.NewDropSnapshotOption(snapshotName, sourceName))
				if client.ErrorCode(dropErr) != merr.Code(merr.ErrSnapshotPinned) {
					break
				}
				select {
				case <-pinCtx.Done():
					break pinWait
				case <-time.After(time.Second):
				}
			}
			cancelPinWait()
			assert.NoError(t, dropErr)
		}
		if sourceCreated {
			assert.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(sourceName)))
		}
		if targetCreated {
			assert.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(targetName)))
		}
		if exportClient != nil {
			for object := range exportClient.ListObjects(cleanupCtx, exportBucket, miniogo.ListObjectsOptions{Prefix: exportPrefix + "/", Recursive: true}) {
				if assert.NoError(t, object.Err) {
					assert.NoError(t, exportClient.RemoveObject(cleanupCtx, exportBucket, object.Key, miniogo.RemoveObjectOptions{}))
				}
			}
		}
	})
	newSchema := func() *entity.Schema {
		return entity.NewSchema().WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
			WithField(entity.NewField().WithName("document").WithDataType(entity.FieldTypeText)).
			WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(8))
	}
	for _, name := range []string{sourceName, targetName} {
		require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(name, newSchema()).
			WithConsistencyLevel(entity.ClStrong).
			WithIndexOptions(client.NewCreateIndexOption(name, "vector", index.NewAutoIndex(entity.L2)))))
		if name == sourceName {
			sourceCreated = true
		} else {
			targetCreated = true
		}
		info, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(name))
		require.NoError(t, err)
		require.Empty(t, info.Properties[pkcommon.EncryptionEzIDKey], "collection must be unencrypted")
	}
	// Cover both inline TEXT and an out-of-line LOB: a forwarded EZK would
	// select the unsupported CMEK+TEXT path instead of the plaintext reader.
	documents := []string{"inline text", strings.Repeat("plaintext-lob-", 6000)}
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(sourceName).
		WithInt64Column("id", []int64{11, 22}).
		WithTextColumn("document", documents).
		WithFloatVectorColumn("vector", 8, [][]float32{{1, 2, 3, 4, 5, 6, 7, 8}, {8, 7, 6, 5, 4, 3, 2, 1}}))
	require.NoError(t, err)
	require.NoError(t, flushWithRetry(ctx, mc, sourceName))
	require.NoError(t, mc.CreateSnapshot(ctx, client.NewCreateSnapshotOption(snapshotName, sourceName)))
	snapshotCreated = true
	info, err := mc.DescribeSnapshot(ctx, client.NewDescribeSnapshotOption(snapshotName, sourceName))
	require.NoError(t, err)
	metadataURI := info.GetS3Location()
	if layout == "self_contained" {
		cfg := getMinIOConfig()
		exportClient, err = newMinIOClient(cfg)
		require.NoError(t, err)
		exportBucket, exportPrefix = cfg.bucket, common.GenRandomString("snapshot_plaintext_ezk_export", 8)
		exportID, err := mc.ExportSnapshot(ctx, client.NewExportSnapshotOption(snapshotName, sourceName, exportPrefix))
		require.NoError(t, err)
		exported, err := waitForExportComplete(ctx, mc, exportID, 2*time.Minute)
		require.NoError(t, err)
		metadataURI = exported.GetSnapshotMetadataUri()
	}
	require.NotEmpty(t, metadataURI)
	resp, err := snapshotImportDatabaseRequest(ctx, baseURL, apiKey, targetDB, "create", map[string]any{
		"collectionName": targetName, "files": [][]string{{metadataURI}},
		"options": map[string]string{"backup": "true", "source_type": "snapshot", "ezk": ezk},
	})
	require.NoError(t, err)
	jobID = resp.Data.JobID
	require.Zero(t, resp.Code, resp.Message)
	require.NotEmpty(t, jobID)
	// Keep progress polling in the same test-owned database as admission.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		progress, err := snapshotImportDatabaseRequest(ctx, baseURL, apiKey, targetDB, "describe", map[string]any{"jobId": jobID})
		require.NoError(c, err)
		require.Zero(c, progress.Code, progress.Message)
		require.Equal(c, "Completed", progress.Data.State, progress.Data.Reason)
	}, 3*time.Minute, time.Second)
	jobID = "" // Completed imports need no abort during cleanup.
	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(targetName))
	require.NoError(t, err)
	require.NoError(t, loadTask.Await(ctx))
	rows, err := mc.Query(ctx, client.NewQueryOption(targetName).
		WithFilter("id in [11, 22]").WithOutputFields("id", "document").WithConsistencyLevel(entity.ClStrong))
	require.NoError(t, err)
	require.Equal(t, 2, rows.Len())
	seen := make(map[int64]string, 2)
	for i := 0; i < rows.Len(); i++ {
		id, err := rows.GetColumn("id").GetAsInt64(i)
		require.NoError(t, err)
		document, err := rows.GetColumn("document").GetAsString(i)
		require.NoError(t, err)
		seen[id] = document
	}
	require.Equal(t, map[int64]string{11: documents[0], 22: documents[1]}, seen)
}

type snapshotImportDatabaseResponse struct {
	Code    int                           `json:"code"`
	Message string                        `json:"message"`
	Data    bulkwriter.ImportProgressData `json:"data"`
}

// BulkImportOption currently has no dbName field and its response decodes
// "status" instead of native REST "code". Keep this database-aware adapter in
// the test; do not mutate the shared HTTP client or change the public SDK.
func snapshotImportDatabaseRequest(ctx context.Context, baseURL, apiKey, db, operation string, body map[string]any) (snapshotImportDatabaseResponse, error) {
	var result snapshotImportDatabaseResponse
	body["dbName"] = db
	payload, err := json.Marshal(body)
	if err != nil {
		return result, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v2/vectordb/jobs/import/"+operation, bytes.NewReader(payload))
	if err != nil {
		return result, err
	}
	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	resp, err := (&http.Client{Timeout: time.Minute}).Do(req)
	if err != nil {
		return result, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return result, fmt.Errorf("snapshot import %s returned HTTP %d", operation, resp.StatusCode)
	}
	err = json.NewDecoder(resp.Body).Decode(&result)
	return result, err
}

// Reuse the ordinary Import lifecycle without changing the no-L0 regression
// scenario. Environment configuration and service ownership stay with the suite.
func runStorageV3SnapshotImport(t *testing.T, withL0, crossBucket bool, schemaCase string) {
	layouts := []string{"referenced", "self_contained"}
	if crossBucket {
		// Export is the supported way to construct a portable bundle. Do not
		// copy the live instance's data root to manufacture a foreign fixture.
		layouts = []string{"self_contained"}
	}
	for _, layout := range layouts {
		t.Run(layout, func(t *testing.T) {
			ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
			mc := hp.CreateDefaultMilvusClient(ctx, t)
			baseURL, apiKey := snapshotImportRESTConfig()

			sourceCollection := common.GenRandomString("snapshot_import_source", 6)
			targetCollection := common.GenRandomString("snapshot_import_target", 6)
			snapshotName := common.GenRandomString("snapshot_import", 6)
			collectionsToClean := []string{sourceCollection, targetCollection}
			snapshotCreated := false
			importJobID := ""
			var minioClient *miniogo.Client
			var minioBucket string
			exportPrefixes := make([]string, 0, 2)
			foreignBucket, externalSpec := "", ""

			t.Cleanup(func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				if importJobID != "" {
					_, _ = bulkwriter.AbortImport(cleanupCtx,
						bulkwriter.NewAbortImportOption(baseURL, importJobID).WithAPIKey(apiKey))
				}
				if snapshotCreated {
					_ = mc.DropSnapshot(cleanupCtx, client.NewDropSnapshotOption(snapshotName, sourceCollection))
				}
				for _, collectionName := range collectionsToClean {
					_ = mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName))
				}
				if minioClient != nil {
					for _, prefix := range exportPrefixes {
						cleanupMinIOPrefix(cleanupCtx, minioClient, minioBucket, strings.TrimRight(prefix, "/")+"/")
					}
					if foreignBucket != "" {
						require.NoError(t, minioClient.RemoveBucket(cleanupCtx, foreignBucket))
					}
				}
			})

			targetTag := "tag"
			for _, collectionName := range collectionsToClean {
				schema := newSnapshotImportSchema()
				if schemaCase == "target_optional" && collectionName == targetCollection {
					// Preserve field positions/IDs, not names. New columns are
					// appended so they cannot alias an existing source column.
					targetTag = "renamed_tag"
					schema.Fields[0].WithIsAutoID(true)
					schema.Fields[1].WithName(targetTag)
					schema.WithDynamicFieldEnabled(true).
						WithField(entity.NewField().WithName("added_default").WithDataType(entity.FieldTypeInt64).WithDefaultValueLong(42)).
						WithField(entity.NewField().WithName("added_nullable").WithDataType(entity.FieldTypeInt64).WithNullable(true))
				}
				if schemaCase == "source_extra" && collectionName == sourceCollection {
					schema.WithField(entity.NewField().WithName("source_only").WithDataType(entity.FieldTypeInt64).WithDefaultValueLong(99))
				}
				vectorIndex := client.NewCreateIndexOption(collectionName, "vector", index.NewAutoIndex(entity.L2))
				err := mc.CreateCollection(ctx,
					client.NewCreateCollectionOption(collectionName, schema).
						WithConsistencyLevel(entity.ClStrong).
						WithIndexOptions(vectorIndex))
				require.NoError(t, err)
			}

			if schemaCase == "partition_mapping" {
				for _, name := range []string{"target_extra", "target_third"} {
					require.NoError(t, mc.CreatePartition(ctx, client.NewCreatePartitionOption(targetCollection, name)))
				}
			}
			loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(targetCollection))
			require.NoError(t, err)
			require.NoError(t, loadTask.Await(ctx))

			const (
				rowCount = 12
				firstID  = int64(30400)
			)
			ids := make([]int64, rowCount)
			tags := make([]string, rowCount)
			phases := make([]int64, rowCount)
			documents := make([]string, rowCount)
			vectors := make([][]float32, rowCount)
			expectedTags := make(map[int64]string, rowCount)
			expectedPhases := make(map[int64]int64, rowCount)
			expectedDocuments := make(map[int64]string, rowCount)
			for i := 0; i < rowCount; i++ {
				id := firstID + int64(i)
				ids[i] = id
				tags[i] = fmt.Sprintf("tag-%d", id)
				phases[i] = 304
				documents[i] = fmt.Sprintf("snapshot-lob-%d-", id) + strings.Repeat("x", 70*1024)
				vectors[i] = []float32{
					float32(i), float32(i + 1), float32(i + 2), float32(i + 3),
					float32(i + 4), float32(i + 5), float32(i + 6), float32(i + 7),
				}
				expectedTags[id] = tags[i]
				expectedPhases[id] = phases[i]
				expectedDocuments[id] = documents[i]
			}

			// Default routing merges all source partitions; explicit mapping
			// routes each source partition to the selected target instead.
			sourcePartitions := []string{"_default", "source_extra"}
			if schemaCase == "partition_mapping" {
				sourcePartitions = append(sourcePartitions, "source_third")
			}
			for _, name := range sourcePartitions[1:] {
				require.NoError(t, mc.CreatePartition(ctx, client.NewCreatePartitionOption(sourceCollection, name)))
			}
			for i, partition := range sourcePartitions {
				begin, end := i*rowCount/len(sourcePartitions), (i+1)*rowCount/len(sourcePartitions)
				insertResult, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(sourceCollection).
					WithPartition(partition).
					WithInt64Column("id", ids[begin:end]).
					WithVarcharColumn("tag", tags[begin:end]).
					WithInt64Column("phase", phases[begin:end]).
					WithTextColumn("document", documents[begin:end]).
					WithFloatVectorColumn("vector", 8, vectors[begin:end]))
				require.NoError(t, err)
				require.EqualValues(t, end-begin, insertResult.InsertCount)
			}
			require.NoError(t, flushWithRetry(ctx, mc, sourceCollection))
			require.NoError(t, waitForAllIndexesBuilt(ctx, mc, sourceCollection, 2*time.Minute))
			expectedImportRows, expectedBeforeCommit := rowCount, 0
			if withL0 {
				// Delete an old row and another PK that will be reinserted later.
				// Existing target rows are outside the source folding operation.
				_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(targetCollection).
					WithInt64Column("id", ids[:1]).WithVarcharColumn("tag", []string{"target-existing"}).
					WithInt64Column("phase", []int64{999}).WithTextColumn("document", []string{"target-existing"}).
					WithFloatVectorColumn("vector", 8, vectors[:1]))
				require.NoError(t, err)
				_, err = mc.Delete(ctx, client.NewDeleteOption(sourceCollection).
					WithExpr(fmt.Sprintf("id in [%d, %d]", firstID, firstID+1)))
				require.NoError(t, err)
				tags[1], phases[1] = "reinserted", 305
				_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(sourceCollection).
					WithInt64Column("id", ids[1:2]).WithVarcharColumn("tag", tags[1:2]).
					WithInt64Column("phase", phases[1:2]).WithTextColumn("document", documents[1:2]).
					WithFloatVectorColumn("vector", 8, vectors[1:2]))
				require.NoError(t, err)
				require.NoError(t, flushWithRetry(ctx, mc, sourceCollection))
				expectedTags[firstID], expectedPhases[firstID], expectedDocuments[firstID] = "target-existing", 999, "target-existing"
				expectedTags[firstID+1], expectedPhases[firstID+1] = tags[1], phases[1]
				expectedImportRows, expectedBeforeCommit = rowCount-1, 1
			}

			err = mc.CreateSnapshot(ctx, client.NewCreateSnapshotOption(snapshotName, sourceCollection).
				WithDescription("StorageV3 snapshot-source Import coverage"))
			require.NoError(t, err)
			snapshotCreated = true

			snapshotInfo, err := mc.DescribeSnapshot(ctx,
				client.NewDescribeSnapshotOption(snapshotName, sourceCollection))
			require.NoError(t, err)
			require.Equal(t, snapshotName, snapshotInfo.GetName())
			require.Equal(t, sourceCollection, snapshotInfo.GetCollectionName())
			snapshotSource := snapshotInfo.GetS3Location()
			require.NotEmpty(t, snapshotSource)
			var snapshotStorageCfg minioConfig
			if withL0 || layout == "self_contained" {
				snapshotStorageCfg = snapshotFixtureStorageConfig(t, getMinIOConfig(), snapshotSource)
			}
			if withL0 {
				cfg := getMinIOConfig()
				minioClient, err = newMinIOClient(cfg)
				require.NoError(t, err)
				minioBucket = cfg.bucket
				skipIfMinIOUnreachable(ctx, t, minioClient, minioBucket)
				requireSnapshotL0Delete(t, ctx, minioClient, minioBucket, snapshotStorageCfg.address, snapshotSource, "", "", firstID)
			}

			if layout == "self_contained" {
				minioCfg := getMinIOConfig()
				minioClient, err = newMinIOClient(minioCfg)
				require.NoError(t, err)
				minioBucket = minioCfg.bucket
				skipIfMinIOUnreachable(ctx, t, minioClient, minioBucket)

				exportPrefix := common.GenRandomString("snapshot_import_export", 8)
				relocatedPrefix := common.GenRandomString("snapshot_import_relocated", 8)
				exportPrefixes = append(exportPrefixes, exportPrefix, relocatedPrefix)
				exportTarget := exportPrefix
				if crossBucket {
					bucketName := strings.ToLower(strings.ReplaceAll(common.GenRandomString("snapshot-import", 10), "_", "-"))
					require.NoError(t, minioClient.MakeBucket(ctx, bucketName, miniogo.MakeBucketOptions{}))
					foreignBucket = bucketName
					minioBucket = foreignBucket
					snapshotStorageCfg.bucket = foreignBucket
					externalSpec = extTestSpec(snapshotStorageCfg, "parquet")
					exportTarget = extTestURI(snapshotStorageCfg, exportPrefix)
				}
				exportJobID, err := mc.ExportSnapshot(ctx,
					client.NewExportSnapshotOption(snapshotName, sourceCollection, exportTarget).WithExternalSpec(externalSpec))
				require.NoError(t, err)
				require.NotZero(t, exportJobID)

				exportInfo, err := waitForExportComplete(ctx, mc, exportJobID, 2*time.Minute)
				require.NoError(t, err)
				require.Positive(t, exportInfo.GetTotalBytes())
				require.NotEmpty(t, exportInfo.GetSnapshotMetadataUri())
				require.NotEqual(t, snapshotSource, exportInfo.GetSnapshotMetadataUri())

				snapshotSource, err = relocateSnapshotBundle(
					ctx,
					minioClient,
					minioBucket,
					exportInfo.GetSnapshotMetadataUri(),
					exportPrefix,
					relocatedPrefix,
				)
				require.NoError(t, err)
				if withL0 {
					requireSnapshotL0Delete(t, ctx, minioClient, minioBucket, snapshotStorageCfg.address, snapshotSource, exportPrefix, relocatedPrefix, firstID)
				}
			}

			// Import is exposed by the Go SDK through the REST-backed bulkwriter
			// package; the surrounding snapshot and data operations use milvusclient.
			importOption := bulkwriter.NewBulkImportOption(baseURL, targetCollection, [][]string{{snapshotSource}}).
				WithOption("auto_commit", "false").
				WithOption("backup", "true").
				WithOption("source_type", "snapshot").
				WithAPIKey(apiKey)
			if schemaCase == "partition_mapping" {
				// Deliberately permute destinations so index/ID-order routing
				// cannot accidentally pass this test.
				importOption.WithOption("partition_mapping", `{"_default":"target_third","source_extra":"_default","source_third":"target_extra"}`)
			}
			if crossBucket {
				// Without opt-in the foreign URI must still fail before a job is
				// admitted. Bad credentials must not fall back to instance access.
				rejected, err := bulkwriter.BulkImport(ctx, importOption)
				require.NoError(t, err)
				// BulkImport currently decodes "status", while native REST uses
				// "code". Inspect the rejection reason and absence of a job ID.
				require.Contains(t, rejected.Message, "snapshot metadata URI must match instance storage when external_spec is absent")
				require.Empty(t, rejected.Data.JobID)
				importOption.WithOption("external_spec", `{"extfs":{"access_key_id":"invalid-key","access_key_value":"invalid-secret"}}`)
				rejected, err = bulkwriter.BulkImport(ctx, importOption)
				require.NoError(t, err)
				// Invalid credentials fail during synchronous metadata capture,
				// before Pending preparation or any import task is admitted.
				require.Contains(t, rejected.Message, "failed to capture snapshot import source")
				require.Empty(t, rejected.Data.JobID)
				importOption.WithOption("external_spec", externalSpec)
			}
			importResp, err := bulkwriter.BulkImport(ctx, importOption)
			require.NoError(t, err)
			require.Empty(t, importResp.Message)
			require.NotEmpty(t, importResp.Data.JobID)
			importJobID = importResp.Data.JobID

			progress, err := waitForSnapshotImportState(
				ctx, baseURL, apiKey, importJobID, "Uncommitted", 3*time.Minute)
			require.NoError(t, err)
			require.EqualValues(t, expectedImportRows, progress.ImportedRows)

			idFilter := fmt.Sprintf("id >= %d && id < %d", firstID, firstID+rowCount)
			beforeCommit, err := mc.Query(ctx, client.NewQueryOption(targetCollection).
				WithFilter(idFilter).
				WithOutputFields("id").
				WithLimit(rowCount).
				WithConsistencyLevel(entity.ClStrong))
			require.NoError(t, err)
			require.Equal(t, expectedBeforeCommit, beforeCommit.Len(), "only preexisting target data may be visible before commit")

			_, err = bulkwriter.CommitImport(ctx,
				bulkwriter.NewCommitImportOption(baseURL, importJobID).WithAPIKey(apiKey))
			require.NoError(t, err)
			_, err = waitForSnapshotImportState(
				ctx, baseURL, apiKey, importJobID, "Completed", 3*time.Minute)
			require.NoError(t, err)
			if crossBucket {
				// The destination must own rewritten data and LOBs. Remove only
				// this test's exported bundle before reading the target again.
				for _, prefix := range exportPrefixes {
					cleanupMinIOPrefix(ctx, minioClient, foreignBucket, strings.TrimRight(prefix, "/")+"/")
				}
				for object := range minioClient.ListObjects(ctx, foreignBucket, miniogo.ListObjectsOptions{Recursive: true}) {
					require.NoError(t, object.Err)
					t.Fatalf("unexpected object in source bucket after import: %s", object.Key)
				}
			}

			refreshTask, err := mc.RefreshLoad(ctx, client.NewRefreshLoadOption(targetCollection))
			require.NoError(t, err)
			require.NoError(t, refreshTask.Await(ctx))
			if schemaCase == "partition_mapping" {
				for i, target := range []string{"target_third", "_default", "target_extra"} {
					rows, err := mc.Query(ctx, client.NewQueryOption(targetCollection).WithPartitions(target).
						WithFilter(idFilter).WithOutputFields("id").WithLimit(rowCount).WithConsistencyLevel(entity.ClStrong))
					require.NoError(t, err)
					actual := make([]int64, 0, rows.Len())
					for j := 0; j < rows.Len(); j++ {
						id, err := rows.GetColumn("id").GetAsInt64(j)
						require.NoError(t, err)
						actual = append(actual, id)
					}
					require.ElementsMatch(t, ids[i*rowCount/3:(i+1)*rowCount/3], actual, "target partition %s", target)
				}
			}

			outputFields := []string{"id", targetTag, "phase", "document"}
			if schemaCase == "target_optional" {
				outputFields = append(outputFields, "added_default", "added_nullable", "$meta")
			}
			afterCommit, err := mc.Query(ctx, client.NewQueryOption(targetCollection).
				WithFilter(idFilter).
				WithOutputFields(outputFields...).
				WithLimit(rowCount).
				WithConsistencyLevel(entity.ClStrong))
			require.NoError(t, err)
			require.Equal(t, rowCount, afterCommit.Len())
			seenIDs := make(map[int64]bool, rowCount)

			idColumn := afterCommit.GetColumn("id")
			tagColumn := afterCommit.GetColumn(targetTag)
			phaseColumn := afterCommit.GetColumn("phase")
			documentColumn := afterCommit.GetColumn("document")
			require.NotNil(t, idColumn)
			require.NotNil(t, tagColumn)
			require.NotNil(t, phaseColumn)
			require.NotNil(t, documentColumn)
			for i := 0; i < afterCommit.Len(); i++ {
				id, err := idColumn.GetAsInt64(i)
				require.NoError(t, err)
				require.False(t, seenIDs[id], "a deleted old version must not accompany its reinsert")
				seenIDs[id] = true
				tag, err := tagColumn.GetAsString(i)
				require.NoError(t, err)
				phase, err := phaseColumn.GetAsInt64(i)
				require.NoError(t, err)
				document, err := documentColumn.GetAsString(i)
				require.NoError(t, err)
				require.Equal(t, expectedTags[id], tag)
				require.Equal(t, expectedPhases[id], phase)
				require.Equal(t, expectedDocuments[id], document)
				if schemaCase == "target_optional" {
					defaultColumn := afterCommit.GetColumn("added_default")
					nullableColumn := afterCommit.GetColumn("added_nullable")
					metaColumn := afterCommit.GetColumn("$meta")
					require.NotNil(t, defaultColumn)
					require.NotNil(t, nullableColumn)
					require.NotNil(t, metaColumn)
					value, err := defaultColumn.GetAsInt64(i)
					require.NoError(t, err)
					require.EqualValues(t, 42, value)
					isNull, err := nullableColumn.IsNull(i)
					require.NoError(t, err)
					require.True(t, isNull)
					meta, err := metaColumn.Get(i)
					require.NoError(t, err)
					require.JSONEq(t, "{}", string(meta.([]byte)))
				}
			}
		})
	}
}

// Inspect the captured inventory with the shared, Go-only snapshot parser. The
// root-module dependency is test-only; do not copy Avro schemas into the SDK or
// introduce a public SDK API for storage internals. Normal Delete/Flush creates
// V1 L0 even for a V3 collection. This fixture explicitly requires that format;
// V3 L0 manifest resolution is covered by server tests, not by interpreting its
// pathless PB summaries as physical files here.
func requireSnapshotL0Delete(t *testing.T, ctx context.Context, mc *miniogo.Client, bucket, storageEndpoint, metadataURI, oldPrefix, newPrefix string, deletedPK int64) {
	t.Helper()
	read := func(objectPath string) []byte {
		objectPath, err := snapshotFixtureObjectKey(objectPath, bucket, storageEndpoint, oldPrefix, newPrefix)
		require.NoError(t, err)
		object, err := mc.GetObject(ctx, bucket, objectPath, miniogo.GetObjectOptions{})
		require.NoError(t, err)
		defer object.Close()
		data, err := io.ReadAll(object)
		require.NoError(t, err)
		return data
	}
	metadata, err := snapshotio.ParseSnapshotMetadata(read(metadataURI))
	require.NoError(t, err)
	var segments []*datapb.SegmentDescription
	for _, manifest := range metadata.GetManifestList() {
		segment, err := snapshotio.ParseSegmentManifest(read(manifest), int(metadata.GetFormatVersion()))
		require.NoError(t, err)
		segments = append(segments, segment)
	}
	found := false
	for _, delta := range segments {
		if delta.GetSegmentLevel() != datapb.SegmentLevel_L0 {
			continue
		}
		applicable := false
		for _, segment := range segments {
			if segment.GetSegmentLevel() != datapb.SegmentLevel_L0 && segment.GetChannelName() == delta.GetChannelName() &&
				(delta.GetPartitionId() == pkcommon.AllPartitionsID || delta.GetPartitionId() == segment.GetPartitionId()) {
				applicable = true
			}
		}
		if !applicable {
			continue
		}
		require.Zero(t, delta.GetStorageVersion(), "Delete/Flush fixture must contain V1 L0; update fixture inspection if the producer format changes")
		require.Empty(t, delta.GetManifestPath(), "V3 L0 paths must be resolved from its exact manifest, not PB deltalog summaries")
		for _, field := range delta.GetDeltalogs() {
			for _, log := range field.GetBinlogs() {
				// EntriesNum=0 is not proof of an empty object. Inspect the data.
				require.NotEmpty(t, log.GetLogPath())
				for _, pk := range snapshotL0DeletePKs(t, ctx, read(log.GetLogPath())) {
					found = found || pk == deletedPK
				}
			}
		}
	}
	require.True(t, found, "fixture has no applicable active L0 containing the expected delete; background compaction may have folded it before snapshot capture")
}

// Keep the server's storage identity separate from the client's dial address.
// CI clients use a namespace-qualified MinIO hostname, while the server uses
// the short service name. Export/Import enforce exact endpoint identity, and
// nested snapshot references must still match the endpoint reported by Milvus.
// Only URI checks/construction use this config; object I/O keeps the dial config.
func snapshotFixtureStorageConfig(t *testing.T, cfg minioConfig, metadataURI string) minioConfig {
	t.Helper()
	u, err := url.Parse(metadataURI)
	require.NoError(t, err)
	require.Equal(t, "minio", u.Scheme, "this fixture requires MinIO storage")
	_, err = snapshotFixtureObjectKey(metadataURI, cfg.bucket, u.Host, "", "")
	require.NoError(t, err)
	cfg.address = u.Host
	return cfg
}

func TestSnapshotFixtureStorageConfig(t *testing.T) {
	cfg := minioConfig{address: "gosdk-14906-minio.jenkins-milvus-ci:9000", bucket: "source"}
	metadataURI := "minio://gosdk-14906-minio:9000/source/files/snapshots/1/metadata/2.json"
	serverCfg := snapshotFixtureStorageConfig(t, cfg, metadataURI)
	require.Equal(t, "gosdk-14906-minio.jenkins-milvus-ci:9000", cfg.address)
	key, err := snapshotFixtureObjectKey(metadataURI, cfg.bucket, serverCfg.address, "", "")
	require.NoError(t, err)
	require.Equal(t, "files/snapshots/1/metadata/2.json", key)
	serverCfg.bucket = "destination"
	require.Equal(t, "minio://gosdk-14906-minio:9000/destination/export", extTestURI(serverCfg, "export"))
	_, err = snapshotFixtureObjectKey("minio://other:9000/source/files/delta", cfg.bucket, serverCfg.address, "", "")
	require.Error(t, err, "a foreign endpoint must not be accepted as a DNS alias")
}

// DescribeSnapshot returns an endpoint-style minio URI, while Avro references
// can be object keys or bucket-style s3 URIs. GetObject requires only the key.
// Keep this fixture Go-only; the server's snapshot storage package also links
// C++. Validate the location before rebasing so a foreign URI cannot silently
// read a same-named object from this fixture's bucket/endpoint.
func snapshotFixtureObjectKey(raw, bucket, endpoint, oldPrefix, newPrefix string) (string, error) {
	key := raw
	if strings.Contains(raw, "://") {
		u, err := url.Parse(raw)
		if err != nil {
			return "", merr.Wrap(err, "invalid snapshot fixture URI")
		}
		if u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Host == "" {
			return "", merr.WrapErrServiceInternalMsg("invalid snapshot fixture URI authority or suffix")
		}
		var uriBucket string
		switch u.Scheme {
		case "s3":
			uriBucket, key = u.Host, strings.TrimPrefix(u.Path, "/")
		case "minio", "http", "https":
			if !strings.EqualFold(u.Host, endpoint) {
				return "", merr.WrapErrServiceInternalMsg("snapshot fixture endpoint mismatch: %s", u.Host)
			}
			uriBucket, key, _ = strings.Cut(strings.TrimPrefix(u.Path, "/"), "/")
		default:
			return "", merr.WrapErrServiceInternalMsg("unsupported snapshot fixture URI scheme: %s", u.Scheme)
		}
		if uriBucket != bucket {
			return "", merr.WrapErrServiceInternalMsg("snapshot fixture bucket mismatch: %s", uriBucket)
		}
	}
	if key == "" {
		return "", merr.WrapErrServiceInternalMsg("snapshot fixture object key is empty")
	}
	if oldPrefix != "" && strings.HasPrefix(key, oldPrefix+"/") {
		key = newPrefix + strings.TrimPrefix(key, oldPrefix)
	}
	return key, nil
}

func TestSnapshotFixtureObjectKey(t *testing.T) {
	for _, tc := range []struct {
		name, raw, oldPrefix, newPrefix, want string
		wantError                             bool
	}{
		{name: "minio_metadata", raw: "minio://localhost:9000/a-bucket/files/snapshots/1/metadata/2.json", want: "files/snapshots/1/metadata/2.json"},
		{name: "s3", raw: "s3://a-bucket/files/delta", want: "files/delta"},
		{name: "http", raw: "http://localhost:9000/a-bucket/files/delta", want: "files/delta"},
		{name: "https", raw: "https://localhost:9000/a-bucket/files/delta", want: "files/delta"},
		{name: "key", raw: "files/delta", want: "files/delta"},
		{name: "escaped_uri", raw: "minio://localhost:9000/a-bucket/files/a%20b", want: "files/a b"},
		{name: "literal_key", raw: "files/a%20b?c#d", want: "files/a%20b?c#d"},
		{name: "rebase_uri", raw: "minio://localhost:9000/a-bucket/export/delta", oldPrefix: "export", newPrefix: "relocated", want: "relocated/delta"},
		{name: "rebase_key", raw: "export/delta", oldPrefix: "export", newPrefix: "relocated", want: "relocated/delta"},
		{name: "rebase_boundary", raw: "export-other/delta", oldPrefix: "export", newPrefix: "relocated", want: "export-other/delta"},
		{name: "already_relocated", raw: "relocated/delta", oldPrefix: "export", newPrefix: "relocated", want: "relocated/delta"},
		{name: "bucket_mismatch", raw: "s3://other/files/delta", wantError: true},
		{name: "minio_bucket_mismatch", raw: "minio://localhost:9000/other/files/delta", wantError: true},
		{name: "endpoint_mismatch", raw: "minio://other:9000/a-bucket/files/delta", wantError: true},
		{name: "credentials", raw: "minio://user:pass@localhost:9000/a-bucket/key", wantError: true},
		{name: "query", raw: "s3://a-bucket/key?x=y", wantError: true},
		{name: "empty_query", raw: "s3://a-bucket/key?", wantError: true},
		{name: "fragment", raw: "s3://a-bucket/key#x", wantError: true},
		{name: "empty_host", raw: "s3:///key", wantError: true},
		{name: "invalid_url", raw: "minio://%/a-bucket/key", wantError: true},
		{name: "empty_key", raw: "s3://a-bucket/", wantError: true},
		{name: "missing_key", raw: "minio://localhost:9000/a-bucket", wantError: true},
		{name: "empty", wantError: true},
		{name: "unsupported_scheme", raw: "ftp://a-bucket/key", wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key, err := snapshotFixtureObjectKey(tc.raw, "a-bucket", "localhost:9000", tc.oldPrefix, tc.newPrefix)
			if tc.wantError {
				require.Error(t, err)
				require.Empty(t, key)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, key)
		})
	}
}

// V1 framing is MagicNumber followed by a descriptor and delete events. Each
// event has a 17-byte header (timestamp, type, length, next position); a delete
// event adds two uint64 timestamps before its Parquet payload. Follow lengths,
// never scan for PAR1 or pass the enclosing binlog to Arrow: Parquet offsets
// are relative to the payload. This is a plaintext V1 fixture inspector only,
// not another Import reader. Golden fixtures come from NewDeltalogWriter.
func snapshotL0Payloads(data []byte) ([][]byte, bool) {
	const headerSize, deleteDataSize = 17, 16
	if len(data) < 4 || binary.LittleEndian.Uint32(data[:4]) != 0xfffabc {
		return nil, false
	}
	data = data[4:]
	var payloads [][]byte
	for descriptor := true; len(data) > 0; descriptor = false {
		if len(data) < headerSize {
			return nil, false
		}
		length := int(binary.LittleEndian.Uint32(data[9:13]))
		if length < headerSize || length > len(data) {
			return nil, false
		}
		if descriptor {
			// 52-byte descriptor fixed part, 8 post-header lengths, extra length.
			if data[8] != 0 || length < headerSize+52+8+4 {
				return nil, false
			}
		} else {
			if data[8] != 2 || length <= headerSize+deleteDataSize {
				return nil, false
			}
			payloads = append(payloads, data[headerSize+deleteDataSize:length])
		}
		data = data[length:]
	}
	return payloads, len(payloads) > 0
}

// TestSnapshotL0FixtureDecoder uses golden bytes from the real StorageV1
// NewDeltalogWriter (collection/partition/segment/log IDs 1/2/3/4, Int64 PKs
// 30400/30401, timestamps 200/201). Both dataNode.storage.deltalog formats are
// represented; no server or object store is needed.
func TestSnapshotL0FixtureDecoder(t *testing.T) {
	fixtures := map[string]string{
		"json":    "vPr/AAAArNxJ9IEGAGcAAABrAAAAAQAAAAAAAAACAAAAAAAAAAMAAAAAAAAA//////////8AAAAAAAAAAAAAAAAAAAAAFAAAADQQEBAQEBAQFgAAAHsib3JpZ2luYWxfc2l6ZSI6Ijc3In0AAKzcSfSBBgK7AQAA/////wEAAAAAAAAAAQAAAAAAAABQQVIxFQQVkAEVfkwVBBUAEgAAKLUv/QQAlQEAZAIgAAAAeyJwayI6MzA0MDAsInRzIjoyMDAsInBrVHlwZSI6NX0xMQMQBYfLIS6P/ARhBKfVFQAVEhUsLBUEFRAVBhUGHDYAFgAYIHsicGsiOjMwNDAxLCJ0cyI6MjAxLCJwa1R5cGUiOjV9GCB7InBrIjozMDQwMCwidHMiOjIwMCwicGtUeXBlIjo1fQAAACi1L/0EAEkAAAIAAAAEAQEDApUZdAEVBBksNQQYBnNjaGVtYRUCABUMJQIYATAlAEwcAAAAFgQZHBkcJoYDHBUMGTUQAAYZGAEwFQwWBBb2Ahb+AiakASYIHDYAFgAYIHsicGsiOjMwNDAxLCJ0cyI6MjAxLCJwa1R5cGUiOjV9GCB7InBrIjozMDQwMCwidHMiOjIwMCwicGtUeXBlIjo1fQAZLBUEFQAVAgAVABUQFQIAAAAW/gIWBCYIFv4CFAAAGQwYGXBhcnF1ZXQtZ28gdmVyc2lvbiAxNy4wLjAZHBwAAADPAAAAUEFSMQ==",
		"parquet": "vPr/AAAArNxJ9IEGAH8AAACDAAAAAQAAAAAAAAACAAAAAAAAAAMAAAAAAAAA//////////8AAAAAAAAAAAAAAAAAAAAABQAAADQQEBAQEBAQLgAAAHsib3JpZ2luYWxfc2l6ZSI6IjM0IiwidmVyc2lvbiI6Ik1VTFRJX0ZJRUxEIn0AAKzcSfSBBgIZAgAA/////wEAAAAAAAAAAQAAAAAAAABQQVIxFQQVIBUgTBUEFQASAADAdgAAAAAAAMF2AAAAAAAAFQAVBhUGLBUEFRAVBhUGHBgIwXYAAAAAAAAYCMB2AAAAAAAAFgAWABgIwXYAAAAAAAAYCMB2AAAAAAAAAAAAAQMCFQQVIBUgTBUEFQASAADIAAAAAAAAAMkAAAAAAAAAFQAVBhUGLBUEFRAVBhUGHBgIyQAAAAAAAAAYCMgAAAAAAAAAFgAWABgIyQAAAAAAAAAYCMgAAAAAAAAAAAAAAQMCFQQZPDUEGAZzY2hlbWEVBAAVBCUAGAJwayUkTKwTQBEAAAAVBCUAGAJ0cyUkTKwTQBEAAAAWBBkcGSwmyAEcFQQZNRAABhkYAnBrFQAWBBbAARbAASZEJggcGAjBdgAAAAAAABgIwHYAAAAAAAAWABYAGAjBdgAAAAAAABgIwHYAAAAAAAAAGSwVBBUAFQIAFQAVEBUCAAAAJogDHBUEGTUQAAYZGAJ0cxUAFgQWwAEWwAEmhAImyAEcGAjJAAAAAAAAABgIyAAAAAAAAAAWABYAGAjJAAAAAAAAABgIyAAAAAAAAAAAGSwVBBUAFQIAFQAVEBUCAAAAFoADFgQmCBaAAxQAABkMGBlwYXJxdWV0LWdvIHZlcnNpb24gMTcuMC4wGSwcAAAcAAAALAEAAFBBUjE=",
	}
	for name, encoded := range fixtures {
		t.Run(name, func(t *testing.T) {
			data, err := base64.StdEncoding.DecodeString(encoded)
			require.NoError(t, err)
			// Arrow can return an empty table without an error for the enclosing
			// binlog. Assert actual PKs, not just successful reader construction.
			require.Equal(t, []int64{30400, 30401}, snapshotL0DeletePKs(t, context.Background(), data))
			payloads, ok := snapshotL0Payloads(data)
			require.True(t, ok)
			require.Len(t, payloads, 1)
			_, ok = snapshotL0Payloads(payloads[0])
			require.False(t, ok, "bare Parquet is not a V1 fixture")
			for end := 0; end < len(data); end++ {
				_, ok = snapshotL0Payloads(data[:end])
				require.False(t, ok, "truncation at byte %d", end)
			}
			deleteOffset := 4 + int(binary.LittleEndian.Uint32(data[13:17]))
			// Do not silently ignore another event or trailing damage.
			multiple := append(bytes.Clone(data), data[deleteOffset:]...)
			require.Equal(t, []int64{30400, 30401, 30400, 30401}, snapshotL0DeletePKs(t, context.Background(), multiple))
			for _, mutation := range []struct {
				name   string
				offset int
				value  byte
			}{
				{"magic", 0, 0},
				{"descriptor_type", 12, 2},
				{"descriptor_short", 13, 17},
				{"delete_type", deleteOffset + 8, 1},
				{"delete_length", deleteOffset + 12, 255},
			} {
				t.Run(mutation.name, func(t *testing.T) {
					invalid := bytes.Clone(data)
					invalid[mutation.offset] = mutation.value
					_, ok := snapshotL0Payloads(invalid)
					require.False(t, ok)
				})
			}
			_, ok = snapshotL0Payloads(append(bytes.Clone(data), 1))
			require.False(t, ok)
		})
	}
}

func snapshotL0DeletePKs(t *testing.T, ctx context.Context, data []byte) []int64 {
	t.Helper()
	payloads, ok := snapshotL0Payloads(data)
	require.True(t, ok, "invalid V1 L0 fixture framing")
	var result []int64
	for _, payload := range payloads {
		func() {
			table, err := pqarrow.ReadTable(ctx, bytes.NewReader(payload),
				parquet.NewReaderProperties(memory.DefaultAllocator), pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
			require.NoError(t, err)
			defer table.Release()
			require.Positive(t, table.NumCols())
			for _, chunk := range table.Column(0).Data().Chunks() {
				require.Zero(t, chunk.NullN())
				switch pks := chunk.(type) {
				case *array.Int64: // dataNode.storage.deltalog=parquet (MULTI_FIELD).
					require.EqualValues(t, 2, table.NumCols())
					result = append(result, pks.Int64Values()...)
				case *array.String: // Default V1 stores JSON records inside Parquet.
					require.EqualValues(t, 1, table.NumCols())
					for i := 0; i < pks.Len(); i++ {
						var entry struct {
							PK     *int64  `json:"pk"`
							Ts     *uint64 `json:"ts"`
							PKType int64   `json:"pkType"`
						}
						require.NoError(t, json.Unmarshal([]byte(pks.Value(i)), &entry))
						require.NotNil(t, entry.PK)
						require.NotNil(t, entry.Ts)
						require.EqualValues(t, schemapb.DataType_Int64, entry.PKType)
						result = append(result, *entry.PK)
					}
				default:
					t.Fatalf("unexpected V1 L0 fixture PK column: %T", chunk)
				}
			}
		}()
	}
	return result
}

// TestSnapshotRestoreWithMultiShardMultiPartition tests the complete snapshot restore workflow with data operations
func TestSnapshotRestoreWithMultiShardMultiPartition(t *testing.T) {
	// Heavy case (large data volume + minute-scale index/refresh/restore
	// waits): intentionally NOT run in parallel. Under t.Parallel() it competes
	// with the rest of the suite for the shared standalone cluster and flakes on
	// those timeouts; keep it serial so it gets the resources it needs.

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
	collectionsToClean := []string{collName}
	t.Cleanup(func() {
		for _, c := range collectionsToClean {
			_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(c))
		}
	})

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
	err = flushWithRetry(ctx, mc, collName)
	common.CheckErr(t, err, true)

	// Wait for all indexes to be built after flush
	err = waitForAllIndexesBuilt(ctx, mc, collName, 2*time.Minute)
	common.CheckErr(t, err, true)

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
	listOpt := client.NewListSnapshotsOption(collName)
	snapshots, err := mc.ListSnapshots(ctx, listOpt)
	common.CheckErr(t, err, true)
	require.Contains(t, snapshots, snapshotName)

	// print snapshot info
	describeOpt := client.NewDescribeSnapshotOption(snapshotName, collName)
	snapshotInfo, err := mc.DescribeSnapshot(ctx, describeOpt)
	common.CheckErr(t, err, true)
	require.Equal(t, snapshotName, snapshotInfo.GetName())
	mlog.Info(context.TODO(), "check snapshot info", mlog.Any("info", snapshotInfo))

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
	collectionsToClean = append(collectionsToClean, restoredCollName)
	restoreOpt := client.NewRestoreSnapshotOption(snapshotName, collName, restoredCollName)
	jobID, err := mc.RestoreSnapshot(ctx, restoreOpt)
	common.CheckErr(t, err, true)

	// Wait for restore to complete
	_, err = waitForRestoreComplete(ctx, mc, jobID, 1*time.Minute)
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
	dropOpt := client.NewDropSnapshotOption(snapshotName, collName)
	err = mc.DropSnapshot(ctx, dropOpt)
	common.CheckErr(t, err, true)
}

// TestSnapshotRestoreWithMultiFields tests snapshot restore with all supported field types
func TestSnapshotRestoreWithMultiFields(t *testing.T) {
	// Heavy case (large data volume + minute-scale index/refresh/restore
	// waits): intentionally NOT run in parallel. Under t.Parallel() it competes
	// with the rest of the suite for the shared standalone cluster and flakes on
	// those timeouts; keep it serial so it gets the resources it needs.

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
	collectionsToClean := []string{collName}
	t.Cleanup(func() {
		for _, c := range collectionsToClean {
			_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(c))
		}
	})

	// Get collection schema for data insertion
	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	common.CheckErr(t, err, true)

	// Step 2a: Create indexes for vector field (required before loading)
	mlog.Info(context.TODO(), "Creating index for vector field")
	vecIdx := index.NewHNSWIndex(entity.L2, 8, 96)
	vecIndexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "float_vec", vecIdx))
	common.CheckErr(t, err, true)
	err = vecIndexTask.Await(ctx)
	common.CheckErr(t, err, true)

	// Step 2b: Create indexes for scalar fields to accelerate filtering
	mlog.Info(context.TODO(), "Creating indexes for scalar fields")
	scalarIndexFields := []string{"int64_field", "varchar_field"}
	for _, fieldName := range scalarIndexFields {
		scalarIdx := index.NewInvertedIndex()
		scalarIndexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, fieldName, scalarIdx))
		common.CheckErr(t, err, true)
		err = scalarIndexTask.Await(ctx)
		common.CheckErr(t, err, true)
	}

	// Step 2c: Create indexes for array fields
	mlog.Info(context.TODO(), "Creating indexes for array fields")
	arrayIndexFields := []string{"int64_array", "string_array"}
	for _, fieldName := range arrayIndexFields {
		arrayIdx := index.NewInvertedIndex()
		arrayIndexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, fieldName, arrayIdx))
		common.CheckErr(t, err, true)
		err = arrayIndexTask.Await(ctx)
		common.CheckErr(t, err, true)
	}

	// Step 2d: Load collection
	mlog.Info(context.TODO(), "Loading collection")
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
	err = flushWithRetry(ctx, mc, collName)
	common.CheckErr(t, err, true)

	// Wait for all indexes to be built after flush
	err = waitForAllIndexesBuilt(ctx, mc, collName, 2*time.Minute)
	common.CheckErr(t, err, true)

	// Step 3: Delete some records (3,000 from each batch)
	for i := 0; i < numOfBatch; i++ {
		deleteExpr := fmt.Sprintf("id >= %d and id < %d", insertBatchSize*i, insertBatchSize*i+deleteBatchSize)
		delRes, err := mc.Delete(ctx, client.NewDeleteOption(collName).WithExpr(deleteExpr))
		common.CheckErr(t, err, true)
		require.Equal(t, int64(deleteBatchSize), delRes.DeleteCount)
	}

	// Flush to ensure deletion is persisted
	err = flushWithRetry(ctx, mc, collName)
	common.CheckErr(t, err, true)

	// Wait for all indexes to be built after flush
	err = waitForAllIndexesBuilt(ctx, mc, collName, 2*time.Minute)
	common.CheckErr(t, err, true)

	// Step 4: Create snapshot
	snapshotName := fmt.Sprintf("multi_fields_snapshot_%s", common.GenRandomString(snapshotPrefix, 6))
	createSnapshotOpt := client.NewCreateSnapshotOption(snapshotName, collName).
		WithDescription("Snapshot for multi-fields restore testing")

	err = mc.CreateSnapshot(ctx, createSnapshotOpt)
	common.CheckErr(t, err, true)

	// Verify snapshot was created
	listOpt := client.NewListSnapshotsOption(collName)
	snapshots, err := mc.ListSnapshots(ctx, listOpt)
	common.CheckErr(t, err, true)
	require.Contains(t, snapshots, snapshotName)

	// Print snapshot info
	describeOpt := client.NewDescribeSnapshotOption(snapshotName, collName)
	snapshotInfo, err := mc.DescribeSnapshot(ctx, describeOpt)
	common.CheckErr(t, err, true)
	require.Equal(t, snapshotName, snapshotInfo.GetName())
	mlog.Info(context.TODO(), "Created snapshot for multi-fields test", mlog.Any("info", snapshotInfo))

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
	collectionsToClean = append(collectionsToClean, restoredCollName)
	restoreOpt := client.NewRestoreSnapshotOption(snapshotName, collName, restoredCollName)
	jobID, err := mc.RestoreSnapshot(ctx, restoreOpt)
	common.CheckErr(t, err, true)

	// Wait for restore to complete
	_, err = waitForRestoreComplete(ctx, mc, jobID, 1*time.Minute)
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
	dropOpt := client.NewDropSnapshotOption(snapshotName, collName)
	err = mc.DropSnapshot(ctx, dropOpt)
	common.CheckErr(t, err, true)
}

// TestSnapshotRestoreEmptyCollection tests snapshot and restore of an empty collection.
// It verifies that collection metadata, schema, and indexes are preserved without any data.
func TestSnapshotRestoreEmptyCollection(t *testing.T) {
	// Heavy case (large data volume + minute-scale index/refresh/restore
	// waits): intentionally NOT run in parallel. Under t.Parallel() it competes
	// with the rest of the suite for the shared standalone cluster and flakes on
	// those timeouts; keep it serial so it gets the resources it needs.

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

	// Create collection with non-default metadata and 3 shards.
	expectedProperties := map[string]string{
		common.CollectionTTLSeconds:         "360",
		"collection.autocompaction.enabled": "false",
		common.MmapEnabled:                  "false",
		"allow_insert_auto_id":              "false",
	}
	createOpt := client.NewCreateCollectionOption(collName, schema).
		WithShardNum(3).
		WithConsistencyLevel(entity.ClBounded)
	for key, value := range expectedProperties {
		createOpt.WithProperty(key, value)
	}
	err := mc.CreateCollection(ctx, createOpt)
	common.CheckErr(t, err, true)
	collectionsToClean := []string{collName}
	t.Cleanup(func() {
		for _, c := range collectionsToClean {
			_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(c))
		}
	})

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
	mlog.Info(context.TODO(), "Creating index for vector field")
	vecIdx := index.NewHNSWIndex(entity.L2, 8, 96)
	vecIndexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "float_vec", vecIdx))
	common.CheckErr(t, err, true)
	err = vecIndexTask.Await(ctx)
	common.CheckErr(t, err, true)

	// Step 4: Create indexes for scalar fields
	mlog.Info(context.TODO(), "Creating indexes for scalar fields")
	scalarIndexFields := []string{"int64_field", "varchar_field"}
	for _, fieldName := range scalarIndexFields {
		scalarIdx := index.NewInvertedIndex()
		scalarIndexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, fieldName, scalarIdx))
		common.CheckErr(t, err, true)
		err = scalarIndexTask.Await(ctx)
		common.CheckErr(t, err, true)
	}

	// Step 5: Create indexes for array fields
	mlog.Info(context.TODO(), "Creating indexes for array fields")
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
	listOpt := client.NewListSnapshotsOption(collName)
	snapshots, err := mc.ListSnapshots(ctx, listOpt)
	common.CheckErr(t, err, true)
	require.Contains(t, snapshots, snapshotName)

	// Print snapshot info
	describeOpt := client.NewDescribeSnapshotOption(snapshotName, collName)
	snapshotInfo, err := mc.DescribeSnapshot(ctx, describeOpt)
	common.CheckErr(t, err, true)
	require.Equal(t, snapshotName, snapshotInfo.GetName())
	mlog.Info(context.TODO(), "Created snapshot for empty collection", mlog.Any("info", snapshotInfo))

	// Step 8: Restore snapshot to a new collection
	restoredCollName := fmt.Sprintf("restored_%s", collName)
	collectionsToClean = append(collectionsToClean, restoredCollName)
	restoreOpt := client.NewRestoreSnapshotOption(snapshotName, collName, restoredCollName)
	jobID, err := mc.RestoreSnapshot(ctx, restoreOpt)
	common.CheckErr(t, err, true)

	// Wait for restore to complete
	_, err = waitForRestoreComplete(ctx, mc, jobID, 1*time.Minute)
	common.CheckErr(t, err, true)

	// Step 9: Verify restored collection exists
	has, err := mc.HasCollection(ctx, client.NewHasCollectionOption(restoredCollName))
	common.CheckErr(t, err, true)
	require.True(t, has)

	// Step 10: Get restored collection info
	restoredColl, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(restoredCollName))
	common.CheckErr(t, err, true)
	require.Equal(t, originalColl.ConsistencyLevel, restoredColl.ConsistencyLevel, "Consistency level should match")
	for key, expectedValue := range expectedProperties {
		require.Equal(t, expectedValue, originalColl.Properties[key], "Source collection property should match")
		require.Equal(t, expectedValue, restoredColl.Properties[key], "Restored collection property should match")
	}

	// Step 11: Verify schema matches
	mlog.Info(context.TODO(), "Verifying schema consistency")
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
	mlog.Info(context.TODO(), "Verifying partition consistency")
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
	mlog.Info(context.TODO(), "Verifying index consistency")
	originalIndexes, err := mc.ListIndexes(ctx, client.NewListIndexOption(collName))
	common.CheckErr(t, err, true)
	mlog.Info(context.TODO(), "original indexes", mlog.Any("indexes", originalIndexes))

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
	mlog.Info(context.TODO(), "Loading collections to verify data")
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

	mlog.Info(context.TODO(), "Empty collection snapshot and restore test completed successfully",
		mlog.String("original_collection", collName),
		mlog.String("restored_collection", restoredCollName),
		mlog.Int("field_count", len(originalColl.Schema.Fields)),
		mlog.Int("index_count", len(originalIndexes)),
		mlog.Int("partition_count", len(partitions)))

	// Clean up
	dropOpt := client.NewDropSnapshotOption(snapshotName, collName)
	err = mc.DropSnapshot(ctx, dropOpt)
	common.CheckErr(t, err, true)
}

// TestSnapshotRestoreWithJSONStats tests snapshot restore with JSON field and JSON stats
// This test verifies that JSON stats (both legacy json_key_index_log and new json_stats formats)
// are correctly preserved and restored during snapshot operations
func TestSnapshotRestoreWithJSONStats(t *testing.T) {
	// Heavy case (large data volume + minute-scale index/refresh/restore
	// waits): intentionally NOT run in parallel. Under t.Parallel() it competes
	// with the rest of the suite for the shared standalone cluster and flakes on
	// those timeouts; keep it serial so it gets the resources it needs.

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
	mlog.Info(context.TODO(), "Preparing indexes for collection")

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
	mlog.Info(context.TODO(), "Creating collection with indexes")
	createOpt := client.NewCreateCollectionOption(collName, schema).
		WithShardNum(3).
		WithIndexOptions(vecIndexOpt, varcharIndexOpt, jsonIndexOpt)
	err := mc.CreateCollection(ctx, createOpt)
	common.CheckErr(t, err, true)
	collectionsToClean := []string{collName}
	t.Cleanup(func() {
		for _, c := range collectionsToClean {
			_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(c))
		}
	})

	// Step 3: Load collection
	mlog.Info(context.TODO(), "Loading collection")
	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName).WithReplica(1))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	// Step 4: Insert data with JSON content
	mlog.Info(context.TODO(), "Inserting data with JSON fields")
	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	common.CheckErr(t, err, true)

	for i := range numOfBatch {
		insertOpt := hp.TNewDataOption().TWithNb(insertBatchSize).TWithStart(i * insertBatchSize)
		_, insertRes := hp.CollPrepare.InsertData(ctx, t, mc, hp.NewInsertParams(coll.Schema), insertOpt)
		require.Equal(t, insertBatchSize, insertRes.IDs.Len())
	}

	// Flush to ensure data is persisted and JSON stats are generated
	err = flushWithRetry(ctx, mc, collName)
	common.CheckErr(t, err, true)

	// Wait for all indexes to be built after flush
	err = waitForAllIndexesBuilt(ctx, mc, collName, 2*time.Minute)
	common.CheckErr(t, err, true)

	// Verify initial data count
	queryRes, err := mc.Query(ctx,
		client.NewQueryOption(collName).
			WithOutputFields(common.QueryCountFieldName).
			WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	initialCount, _ := queryRes.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(insertBatchSize*numOfBatch), initialCount)
	mlog.Info(context.TODO(), "Initial data inserted", mlog.Int64("count", initialCount))

	// Step 5: Delete some records
	for i := range numOfBatch {
		deleteExpr := fmt.Sprintf("id >= %d and id < %d", insertBatchSize*i, insertBatchSize*i+deleteBatchSize)
		delRes, err := mc.Delete(ctx, client.NewDeleteOption(collName).WithExpr(deleteExpr))
		common.CheckErr(t, err, true)
		require.Equal(t, int64(deleteBatchSize), delRes.DeleteCount)
	}

	// Flush to ensure deletion is persisted
	err = flushWithRetry(ctx, mc, collName)
	common.CheckErr(t, err, true)

	// Wait for all indexes to be built after flush
	err = waitForAllIndexesBuilt(ctx, mc, collName, 2*time.Minute)
	common.CheckErr(t, err, true)

	// Verify count after deletion
	queryRes2, err := mc.Query(ctx,
		client.NewQueryOption(collName).
			WithOutputFields(common.QueryCountFieldName).
			WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	countAfterDelete, _ := queryRes2.Fields[0].GetAsInt64(0)
	expectedAfterDelete := int64(insertBatchSize*numOfBatch - deleteBatchSize*numOfBatch)
	require.Equal(t, expectedAfterDelete, countAfterDelete)
	mlog.Info(context.TODO(), "Data after deletion", mlog.Int64("count", countAfterDelete))

	// Step 6: Create snapshot
	snapshotName := fmt.Sprintf("json_stats_snapshot_%s", common.GenRandomString(snapshotPrefix, 6))
	createSnapshotOpt := client.NewCreateSnapshotOption(snapshotName, collName).
		WithDescription("Snapshot for JSON stats restore testing")

	mlog.Info(context.TODO(), "Creating snapshot with JSON stats")
	err = mc.CreateSnapshot(ctx, createSnapshotOpt)
	common.CheckErr(t, err, true)

	// Verify snapshot was created
	listOpt := client.NewListSnapshotsOption(collName)
	snapshots, err := mc.ListSnapshots(ctx, listOpt)
	common.CheckErr(t, err, true)
	require.Contains(t, snapshots, snapshotName)

	// Describe snapshot
	describeOpt := client.NewDescribeSnapshotOption(snapshotName, collName)
	snapshotInfo, err := mc.DescribeSnapshot(ctx, describeOpt)
	common.CheckErr(t, err, true)
	require.Equal(t, snapshotName, snapshotInfo.GetName())
	mlog.Info(context.TODO(), "Snapshot created", mlog.Any("info", snapshotInfo))

	// Step 7: Insert more data after snapshot (to verify snapshot point-in-time)
	for i := range 2 {
		pkStart := insertBatchSize * (numOfBatch + i)
		insertOpt2 := hp.TNewDataOption().TWithNb(insertBatchSize).TWithStart(pkStart)
		_, insertRes2 := hp.CollPrepare.InsertData(ctx, t, mc, hp.NewInsertParams(coll.Schema), insertOpt2)
		require.Equal(t, insertBatchSize, insertRes2.IDs.Len())
	}

	// Step 8: Restore snapshot to a new collection
	restoredCollName := fmt.Sprintf("restored_%s", collName)
	collectionsToClean = append(collectionsToClean, restoredCollName)
	restoreOpt := client.NewRestoreSnapshotOption(snapshotName, collName, restoredCollName)
	mlog.Info(context.TODO(), "Restoring snapshot", mlog.String("target_collection", restoredCollName))
	jobID, err := mc.RestoreSnapshot(ctx, restoreOpt)
	common.CheckErr(t, err, true)

	// Wait for restore to complete
	_, err = waitForRestoreComplete(ctx, mc, jobID, 1*time.Minute)
	common.CheckErr(t, err, true)

	// Step 9: Verify restored collection exists
	has, err := mc.HasCollection(ctx, client.NewHasCollectionOption(restoredCollName))
	common.CheckErr(t, err, true)
	require.True(t, has)

	// Step 10: Load restored collection
	mlog.Info(context.TODO(), "Loading restored collection")
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
	mlog.Info(context.TODO(), "Restored collection data verified", mlog.Int64("count", restoredCount))

	// Clean up
	dropOpt2 := client.NewDropSnapshotOption(snapshotName, collName)
	err = mc.DropSnapshot(ctx, dropOpt2)
	common.CheckErr(t, err, true)
}

// TestSnapshotRestoreAfterDropPartitionAndCollection tests snapshot restore functionality
// after dropping partitions and the entire collection
func TestSnapshotRestoreAfterDropPartitionAndCollection(t *testing.T) {
	// Heavy case (large data volume + minute-scale index/refresh/restore
	// waits): intentionally NOT run in parallel. Under t.Parallel() it competes
	// with the rest of the suite for the shared standalone cluster and flakes on
	// those timeouts; keep it serial so it gets the resources it needs.

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
	collectionsToClean := []string{collName}
	t.Cleanup(func() {
		for _, c := range collectionsToClean {
			_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(c))
		}
	})

	// Create 3 partitions
	partitions := []string{"part_0", "part_1", "part_2"}
	for _, partName := range partitions {
		err := mc.CreatePartition(ctx, client.NewCreatePartitionOption(collName, partName))
		common.CheckErr(t, err, true)
	}
	mlog.Info(context.TODO(), "Created partitions", mlog.Strings("partitions", partitions))

	// Get collection schema
	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	common.CheckErr(t, err, true)

	// Step 2: Insert data into each partition
	mlog.Info(context.TODO(), "Inserting data into partitions")
	for i, partName := range partitions {
		insertOpt := hp.TNewDataOption().TWithNb(insertBatchSize).TWithStart(i * insertBatchSize)
		_, insertRes := hp.CollPrepare.InsertData(ctx, t, mc, hp.NewInsertParams(coll.Schema).TWithPartitionName(partName), insertOpt)
		require.Equal(t, insertBatchSize, insertRes.IDs.Len())
	}

	// Flush to ensure data is persisted
	err = flushWithRetry(ctx, mc, collName)
	common.CheckErr(t, err, true)

	// Wait for all indexes to be built after flush
	err = waitForAllIndexesBuilt(ctx, mc, collName, 2*time.Minute)
	common.CheckErr(t, err, true)

	// Verify initial data count (3 partitions * 3000 = 9000)
	queryRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ := queryRes.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(9000), count)
	mlog.Info(context.TODO(), "Initial data count verified", mlog.Int64("count", count))

	// Step 3: Create snapshot
	snapshotName := fmt.Sprintf("drop_test_snapshot_%s", common.GenRandomString(snapshotPrefix, 6))
	createOpt := client.NewCreateSnapshotOption(snapshotName, collName).
		WithDescription("Snapshot for testing restore after drop operations")

	mlog.Info(context.TODO(), "Creating snapshot")
	err = mc.CreateSnapshot(ctx, createOpt)
	common.CheckErr(t, err, true)

	// Verify snapshot was created
	listOpt := client.NewListSnapshotsOption(collName)
	snapshots, err := mc.ListSnapshots(ctx, listOpt)
	common.CheckErr(t, err, true)
	require.Contains(t, snapshots, snapshotName)

	describeOpt := client.NewDescribeSnapshotOption(snapshotName, collName)
	snapshotInfo, err := mc.DescribeSnapshot(ctx, describeOpt)
	common.CheckErr(t, err, true)
	require.Equal(t, snapshotName, snapshotInfo.GetName())
	mlog.Info(context.TODO(), "Snapshot created", mlog.Any("info", snapshotInfo))

	// Step 4: Test scenario 1 - Drop partition and restore
	mlog.Info(context.TODO(), "Test scenario 1: Drop partition and restore")

	// Release the partition before dropping it
	dropPartName := "part_0"
	err = mc.ReleasePartitions(ctx, client.NewReleasePartitionsOptions(collName, dropPartName))
	common.CheckErr(t, err, true)
	mlog.Info(context.TODO(), "Released partition", mlog.String("partition", dropPartName))

	// Drop one partition
	err = mc.DropPartition(ctx, client.NewDropPartitionOption(collName, dropPartName))
	common.CheckErr(t, err, true)
	mlog.Info(context.TODO(), "Dropped partition", mlog.String("partition", dropPartName))

	// Wait for partition drop to take effect
	time.Sleep(5 * time.Second)

	// Verify remaining data count (2 partitions * 3000 = 6000)
	queryRes2, err := mc.Query(ctx, client.NewQueryOption(collName).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	remainingCount, _ := queryRes2.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(6000), remainingCount)
	mlog.Info(context.TODO(), "Data count after dropping partition", mlog.Int64("count", remainingCount))

	// Restore snapshot to new collection (v1)
	restoredCollNameV1 := fmt.Sprintf("restored_v1_%s", collName)
	collectionsToClean = append(collectionsToClean, restoredCollNameV1)
	restoreOptV1 := client.NewRestoreSnapshotOption(snapshotName, collName, restoredCollNameV1)
	mlog.Info(context.TODO(), "Restoring snapshot after partition drop", mlog.String("target", restoredCollNameV1))
	jobIDV1, err := mc.RestoreSnapshot(ctx, restoreOptV1)
	common.CheckErr(t, err, true)

	// Wait for restore to complete
	_, err = waitForRestoreComplete(ctx, mc, jobIDV1, 1*time.Minute)
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
	mlog.Info(context.TODO(), "Restored collection v1 data verified", mlog.Int64("count", restoredCountV1))

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
	mlog.Info(context.TODO(), "All partitions restored in v1", mlog.Strings("partitions", filteredPartitionsV1))

	// Step 5: Test scenario 2 - Drop entire collection and restore
	mlog.Info(context.TODO(), "Test scenario 2: Drop entire collection and restore")

	// Drop the original collection
	err = mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
	common.CheckErr(t, err, true)
	mlog.Info(context.TODO(), "Dropped entire collection", mlog.String("collection", collName))

	// Wait for collection drop to take effect
	time.Sleep(5 * time.Second)

	// Verify collection no longer exists
	hasOriginal, err := mc.HasCollection(ctx, client.NewHasCollectionOption(collName))
	common.CheckErr(t, err, true)
	require.False(t, hasOriginal)
	mlog.Info(context.TODO(), "Verified collection is dropped")

	// After dropping the original collection, snapshots should be cascade-deleted.
	// Verify that restoring the snapshot from the dropped collection fails.
	restoredCollNameV2 := fmt.Sprintf("restored_v2_%s", collName)
	collectionsToClean = append(collectionsToClean, restoredCollNameV2)
	restoreOptV2 := client.NewRestoreSnapshotOption(snapshotName, collName, restoredCollNameV2)
	mlog.Info(context.TODO(), "Attempting restore after collection drop (should fail)", mlog.String("target", restoredCollNameV2))
	_, err = mc.RestoreSnapshot(ctx, restoreOptV2)
	// Expect error: source collection no longer exists
	require.Error(t, err, "restore should fail after source collection is dropped")
	mlog.Info(context.TODO(), "Correctly rejected restore after collection drop", mlog.Err(err))

	// Verify restored collection v2 does NOT exist
	hasV2, err := mc.HasCollection(ctx, client.NewHasCollectionOption(restoredCollNameV2))
	common.CheckErr(t, err, true)
	require.False(t, hasV2)

	mlog.Info(context.TODO(), "Test completed successfully",
		mlog.String("snapshot", snapshotName),
		mlog.String("restored_v1", restoredCollNameV1),
		mlog.String("restored_v2", restoredCollNameV2))

	// No cleanup needed for snapshot - it was cascade-deleted when the collection was dropped.
}

// TestSnapshotCrossDatabase tests snapshot operations across different databases.
// Verifies that ListSnapshots with db-level filtering returns only snapshots
// belonging to collections in the specified database.
func TestSnapshotCrossDatabase(t *testing.T) {
	// Heavy case (large data volume + minute-scale index/refresh/restore
	// waits): intentionally NOT run in parallel. Under t.Parallel() it competes
	// with the rest of the suite for the shared standalone cluster and flakes on
	// those timeouts; keep it serial so it gets the resources it needs.

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// Step 1: Create two databases
	dbName1 := common.GenRandomString("db1", 4)
	dbName2 := common.GenRandomString("db2", 4)
	err := mc.CreateDatabase(ctx, client.NewCreateDatabaseOption(dbName1))
	common.CheckErr(t, err, true)
	err = mc.CreateDatabase(ctx, client.NewCreateDatabaseOption(dbName2))
	common.CheckErr(t, err, true)

	// Step 2: Create collection in db1 and insert data
	clientDB1 := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{Address: hp.GetAddr(), DBName: dbName1})
	collNameDB1 := common.GenRandomString(snapshotPrefix, 6)
	err = clientDB1.CreateCollection(ctx, client.SimpleCreateCollectionOptions(collNameDB1, common.DefaultDim))
	common.CheckErr(t, err, true)

	coll1, err := clientDB1.DescribeCollection(ctx, client.NewDescribeCollectionOption(collNameDB1))
	common.CheckErr(t, err, true)
	prepare1, _ := hp.CollPrepare.InsertData(ctx, t, clientDB1, hp.NewInsertParams(coll1.Schema), hp.TNewDataOption())
	prepare1.FlushData(ctx, t, clientDB1, collNameDB1)

	// Step 3: Create collection in db2 and insert data
	clientDB2 := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{Address: hp.GetAddr(), DBName: dbName2})
	collNameDB2 := common.GenRandomString(snapshotPrefix, 6)
	err = clientDB2.CreateCollection(ctx, client.SimpleCreateCollectionOptions(collNameDB2, common.DefaultDim))
	common.CheckErr(t, err, true)

	coll2, err := clientDB2.DescribeCollection(ctx, client.NewDescribeCollectionOption(collNameDB2))
	common.CheckErr(t, err, true)
	prepare2, _ := hp.CollPrepare.InsertData(ctx, t, clientDB2, hp.NewInsertParams(coll2.Schema), hp.TNewDataOption())
	prepare2.FlushData(ctx, t, clientDB2, collNameDB2)

	// Step 4: Create snapshots in both databases
	snapNameDB1 := fmt.Sprintf("snap_db1_%s", common.GenRandomString(snapshotPrefix, 4))
	snapNameDB2 := fmt.Sprintf("snap_db2_%s", common.GenRandomString(snapshotPrefix, 4))

	err = clientDB1.CreateSnapshot(ctx, client.NewCreateSnapshotOption(snapNameDB1, collNameDB1))
	common.CheckErr(t, err, true)

	err = clientDB2.CreateSnapshot(ctx, client.NewCreateSnapshotOption(snapNameDB2, collNameDB2))
	common.CheckErr(t, err, true)

	// Step 5: List snapshots filtered by db1 — should only see db1 snapshot
	snapshotsDB1, err := clientDB1.ListSnapshots(ctx, client.NewListSnapshotsOption(collNameDB1))
	common.CheckErr(t, err, true)
	require.Contains(t, snapshotsDB1, snapNameDB1)
	require.NotContains(t, snapshotsDB1, snapNameDB2)

	// Step 6: List snapshots filtered by db2 — should only see db2 snapshot
	snapshotsDB2, err := clientDB2.ListSnapshots(ctx, client.NewListSnapshotsOption(collNameDB2))
	common.CheckErr(t, err, true)
	require.Contains(t, snapshotsDB2, snapNameDB2)
	require.NotContains(t, snapshotsDB2, snapNameDB1)

	// Step 7: List snapshots from default db for db1's collection — should fail (collection not in default db)
	_, err = mc.ListSnapshots(ctx, client.NewListSnapshotsOption(collNameDB1))
	common.CheckErr(t, err, false, "collection not found")

	// Clean up
	err = clientDB1.DropSnapshot(ctx, client.NewDropSnapshotOption(snapNameDB1, collNameDB1))
	common.CheckErr(t, err, true)
	err = clientDB2.DropSnapshot(ctx, client.NewDropSnapshotOption(snapNameDB2, collNameDB2))
	common.CheckErr(t, err, true)
}

// TestSnapshotRestoreDropAndRestoreAgain tests:
// 1. Create collection A, insert data, create snapshot A1
// 2. Restore A1 to collection B, verify both A and B can load and query/search
// 3. Drop collection B, restore A1 again to collection C
// 4. Verify both A and C can load and query/search
func TestSnapshotRestoreDropAndRestoreAgain(t *testing.T) {
	// Heavy case (large data volume + minute-scale index/refresh/restore
	// waits): intentionally NOT run in parallel. Under t.Parallel() it competes
	// with the rest of the suite for the shared standalone cluster and flakes on
	// those timeouts; keep it serial so it gets the resources it needs.

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	insertBatchSize := 3000

	// Step 1: Create collection A and insert data
	collNameA := common.GenRandomString(snapshotPrefix, 6)
	schema := client.SimpleCreateCollectionOptions(collNameA, common.DefaultDim)
	schema.WithAutoID(false)
	err := mc.CreateCollection(ctx, schema)
	common.CheckErr(t, err, true)
	collectionsToClean := []string{collNameA}
	t.Cleanup(func() {
		for _, c := range collectionsToClean {
			_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(c))
		}
	})

	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collNameA))
	common.CheckErr(t, err, true)

	insertOpt := hp.TNewDataOption().TWithNb(insertBatchSize)
	_, insertRes := hp.CollPrepare.InsertData(ctx, t, mc, hp.NewInsertParams(coll.Schema), insertOpt)
	require.Equal(t, insertBatchSize, insertRes.IDs.Len())

	err = flushWithRetry(ctx, mc, collNameA)
	common.CheckErr(t, err, true)

	// Wait for all indexes to be built after flush
	err = waitForAllIndexesBuilt(ctx, mc, collNameA, 2*time.Minute)
	common.CheckErr(t, err, true)

	// Verify data count in A
	queryRes, err := mc.Query(ctx, client.NewQueryOption(collNameA).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ := queryRes.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(insertBatchSize), count)
	mlog.Info(context.TODO(), "Collection A data inserted", mlog.String("collection", collNameA), mlog.Int64("count", count))

	// Step 2: Create snapshot A1
	snapshotName := fmt.Sprintf("snapshot_%s", common.GenRandomString(snapshotPrefix, 6))
	createOpt := client.NewCreateSnapshotOption(snapshotName, collNameA).
		WithDescription("Snapshot for drop-and-restore-again test")
	err = mc.CreateSnapshot(ctx, createOpt)
	common.CheckErr(t, err, true)
	mlog.Info(context.TODO(), "Snapshot created", mlog.String("snapshot", snapshotName))

	// Step 3: Restore A1 to collection B
	collNameB := fmt.Sprintf("restored_B_%s", collNameA)
	collectionsToClean = append(collectionsToClean, collNameB)
	restoreOptB := client.NewRestoreSnapshotOption(snapshotName, collNameA, collNameB)
	jobIDB, err := mc.RestoreSnapshot(ctx, restoreOptB)
	common.CheckErr(t, err, true)

	_, err = waitForRestoreComplete(ctx, mc, jobIDB, 2*time.Minute)
	common.CheckErr(t, err, true)
	mlog.Info(context.TODO(), "Restored snapshot to collection B", mlog.String("collection", collNameB))

	// Step 4: Verify both A and B can load, query, and search
	// Load B
	loadTaskB, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collNameB).WithReplica(1))
	common.CheckErr(t, err, true)
	err = loadTaskB.Await(ctx)
	common.CheckErr(t, err, true)

	// Query count on A
	queryResA, err := mc.Query(ctx, client.NewQueryOption(collNameA).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	countA, _ := queryResA.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(insertBatchSize), countA)

	// Query count on B
	queryResB, err := mc.Query(ctx, client.NewQueryOption(collNameB).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	countB, _ := queryResB.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(insertBatchSize), countB)

	// Search on A
	vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
	searchResA, err := mc.Search(ctx, client.NewSearchOption(collNameA, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckSearchResult(t, searchResA, common.DefaultNq, common.DefaultLimit)

	// Search on B
	searchResB, err := mc.Search(ctx, client.NewSearchOption(collNameB, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckSearchResult(t, searchResB, common.DefaultNq, common.DefaultLimit)
	mlog.Info(context.TODO(), "Both A and B verified: query and search OK")

	// Step 5: Drop collection B
	err = mc.DropCollection(ctx, client.NewDropCollectionOption(collNameB))
	common.CheckErr(t, err, true)
	time.Sleep(5 * time.Second)

	hasBAfterDrop, err := mc.HasCollection(ctx, client.NewHasCollectionOption(collNameB))
	common.CheckErr(t, err, true)
	require.False(t, hasBAfterDrop)
	mlog.Info(context.TODO(), "Collection B dropped", mlog.String("collection", collNameB))

	// Step 6: Restore A1 again to collection C
	collNameC := fmt.Sprintf("restored_C_%s", collNameA)
	collectionsToClean = append(collectionsToClean, collNameC)
	restoreOptC := client.NewRestoreSnapshotOption(snapshotName, collNameA, collNameC)
	jobIDC, err := mc.RestoreSnapshot(ctx, restoreOptC)
	common.CheckErr(t, err, true)

	_, err = waitForRestoreComplete(ctx, mc, jobIDC, 2*time.Minute)
	common.CheckErr(t, err, true)
	mlog.Info(context.TODO(), "Restored snapshot to collection C", mlog.String("collection", collNameC))

	// Step 7: Verify both A and C can load, query, and search
	// Load C
	loadTaskC, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collNameC).WithReplica(1))
	common.CheckErr(t, err, true)
	err = loadTaskC.Await(ctx)
	common.CheckErr(t, err, true)

	// Query count on A
	queryResA2, err := mc.Query(ctx, client.NewQueryOption(collNameA).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	countA2, _ := queryResA2.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(insertBatchSize), countA2)

	// Query count on C
	queryResC, err := mc.Query(ctx, client.NewQueryOption(collNameC).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	countC, _ := queryResC.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(insertBatchSize), countC)

	// Search on A
	searchResA2, err := mc.Search(ctx, client.NewSearchOption(collNameA, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckSearchResult(t, searchResA2, common.DefaultNq, common.DefaultLimit)

	// Search on C
	searchResC, err := mc.Search(ctx, client.NewSearchOption(collNameC, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckSearchResult(t, searchResC, common.DefaultNq, common.DefaultLimit)
	mlog.Info(context.TODO(), "Both A and C verified: query and search OK")

	// Clean up
	dropSnapshotOpt2 := client.NewDropSnapshotOption(snapshotName, collNameA)
	err = mc.DropSnapshot(ctx, dropSnapshotOpt2)
	common.CheckErr(t, err, true)
	mlog.Info(context.TODO(), "Test completed successfully",
		mlog.String("collectionA", collNameA),
		mlog.String("snapshot", snapshotName),
		mlog.String("collectionC", collNameC))
}

// TestSnapshotRestoreWithMultipleJSONPathIndexes tests that snapshot restore correctly
// handles multiple JSON path indexes on the same JSON field.
// This covers the bug where CopySegmentResult.index_infos used fieldID as map key,
// causing only the last index per field to survive (overwriting earlier ones).
func TestSnapshotRestoreWithMultipleJSONPathIndexes(t *testing.T) {
	// Heavy case (large data volume + minute-scale index/refresh/restore
	// waits): intentionally NOT run in parallel. Under t.Parallel() it competes
	// with the rest of the suite for the shared standalone cluster and flakes on
	// those timeouts; keep it serial so it gets the resources it needs.

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	insertBatchSize := 3000

	// Step 1: Create collection with JSON field
	collName := common.GenRandomString(snapshotPrefix, 6)

	pkField := entity.NewField().
		WithName("id").
		WithDataType(entity.FieldTypeInt64).
		WithIsPrimaryKey(true)

	jsonField := entity.NewField().
		WithName("metadata").
		WithDataType(entity.FieldTypeJSON)

	floatVecField := entity.NewField().
		WithName("embeddings").
		WithDataType(entity.FieldTypeFloatVector).
		WithDim(128)

	schema := entity.NewSchema().
		WithName(collName).
		WithField(pkField).
		WithField(jsonField).
		WithField(floatVecField)

	// Step 2: Prepare indexes - two JSON path indexes on the SAME field
	vecIdx := index.NewHNSWIndex(entity.L2, 8, 96)
	vecIndexOpt := client.NewCreateIndexOption(collName, "embeddings", vecIdx)

	// JSON path index 1: metadata["category"] as varchar
	jsonIdx1 := index.NewInvertedIndex()
	jsonIndexOpt1 := client.NewCreateIndexOption(collName, "metadata", jsonIdx1).
		WithIndexName("idx_category")
	jsonIndexOpt1.WithExtraParam("json_path", `metadata["category"]`)
	jsonIndexOpt1.WithExtraParam("json_cast_type", "varchar")

	// JSON path index 2: metadata["price"] as double
	jsonIdx2 := index.NewInvertedIndex()
	jsonIndexOpt2 := client.NewCreateIndexOption(collName, "metadata", jsonIdx2).
		WithIndexName("idx_price")
	jsonIndexOpt2.WithExtraParam("json_path", `metadata["price"]`)
	jsonIndexOpt2.WithExtraParam("json_cast_type", "double")

	// Create collection with all indexes
	createOpt := client.NewCreateCollectionOption(collName, schema).
		WithIndexOptions(vecIndexOpt, jsonIndexOpt1, jsonIndexOpt2)
	err := mc.CreateCollection(ctx, createOpt)
	common.CheckErr(t, err, true)
	collectionsToClean := []string{collName}
	t.Cleanup(func() {
		for _, c := range collectionsToClean {
			_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(c))
		}
	})

	// Step 3: Insert data with JSON containing both keys
	// Build insert columns manually to include JSON data
	idData := make([]int64, insertBatchSize)
	jsonData := make([][]byte, insertBatchSize)
	vecData := make([][]float32, insertBatchSize)
	categories := []string{"electronics", "books", "clothing", "food", "toys"}

	for i := 0; i < insertBatchSize; i++ {
		idData[i] = int64(i)
		category := categories[i%len(categories)]
		price := float64(i) * 1.5
		jsonBytes := []byte(fmt.Sprintf(`{"category": "%s", "price": %f, "stock": %d}`, category, price, i*10))
		jsonData[i] = jsonBytes

		vec := make([]float32, 128)
		for j := range vec {
			vec[j] = float32(i*128+j) * 0.001
		}
		vecData[i] = vec
	}

	idColumn := column.NewColumnInt64("id", idData)
	jsonColumn := column.NewColumnJSONBytes("metadata", jsonData)
	vecColumn := column.NewColumnFloatVector("embeddings", 128, vecData)

	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(collName, idColumn, jsonColumn, vecColumn))
	common.CheckErr(t, err, true)

	err = flushWithRetry(ctx, mc, collName)
	common.CheckErr(t, err, true)

	// Wait for all indexes to be built after flush
	err = waitForAllIndexesBuilt(ctx, mc, collName, 2*time.Minute)
	common.CheckErr(t, err, true)

	// Step 4: Verify indexes exist on source
	originalIndexes, err := mc.ListIndexes(ctx, client.NewListIndexOption(collName))
	common.CheckErr(t, err, true)
	mlog.Info(context.TODO(), "Original indexes", mlog.Strings("indexes", originalIndexes))
	require.GreaterOrEqual(t, len(originalIndexes), 3, "Should have vector + 2 JSON path indexes")

	// Load and verify query works
	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	queryRes, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithOutputFields(common.QueryCountFieldName).
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ := queryRes.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(insertBatchSize), count)

	// Step 5: Create snapshot
	snapshotName := fmt.Sprintf("snapshot_%s", common.GenRandomString(snapshotPrefix, 6))
	createSnapshotOpt := client.NewCreateSnapshotOption(snapshotName, collName).
		WithDescription("Snapshot with multiple JSON path indexes")
	err = mc.CreateSnapshot(ctx, createSnapshotOpt)
	common.CheckErr(t, err, true)
	mlog.Info(context.TODO(), "Snapshot created", mlog.String("snapshot", snapshotName))

	// Step 6: Restore to new collection
	restoredCollName := fmt.Sprintf("restored_%s", collName)
	collectionsToClean = append(collectionsToClean, restoredCollName)
	restoreOpt := client.NewRestoreSnapshotOption(snapshotName, collName, restoredCollName)
	jobID, err := mc.RestoreSnapshot(ctx, restoreOpt)
	common.CheckErr(t, err, true)

	_, err = waitForRestoreComplete(ctx, mc, jobID, 3*time.Minute)
	common.CheckErr(t, err, true)
	mlog.Info(context.TODO(), "Snapshot restored", mlog.String("restoredCollection", restoredCollName))

	// Step 7: Verify ALL indexes are restored (including both JSON path indexes)
	restoredIndexes, err := mc.ListIndexes(ctx, client.NewListIndexOption(restoredCollName))
	common.CheckErr(t, err, true)
	mlog.Info(context.TODO(), "Restored indexes", mlog.Strings("indexes", restoredIndexes))

	// Both JSON path indexes should be present
	require.Equal(t, len(originalIndexes), len(restoredIndexes),
		"Restored collection should have same number of indexes as original")

	// Verify specific index names exist
	require.Contains(t, restoredIndexes, "idx_category",
		"idx_category JSON path index should be restored")
	require.Contains(t, restoredIndexes, "idx_price",
		"idx_price JSON path index should be restored")

	// Step 8: Load and verify data
	loadTaskR, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(restoredCollName))
	common.CheckErr(t, err, true)
	err = loadTaskR.Await(ctx)
	common.CheckErr(t, err, true)

	// Query count
	queryResR, err := mc.Query(ctx, client.NewQueryOption(restoredCollName).
		WithOutputFields(common.QueryCountFieldName).
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	countR, _ := queryResR.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(insertBatchSize), countR)

	// Search
	vectors := hp.GenSearchVectors(common.DefaultNq, 128, entity.FieldTypeFloatVector)
	searchRes, err := mc.Search(ctx, client.NewSearchOption(restoredCollName, common.DefaultLimit, vectors).
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckSearchResult(t, searchRes, common.DefaultNq, common.DefaultLimit)

	// Step 9: Verify JSON path indexes are functional via filter queries
	// Filter by category (uses idx_category JSON path index)
	categoryRes, err := mc.Query(ctx, client.NewQueryOption(restoredCollName).
		WithFilter(`metadata["category"] == "electronics"`).
		WithOutputFields(common.QueryCountFieldName).
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	categoryCount, _ := categoryRes.Fields[0].GetAsInt64(0)
	// "electronics" is categories[0], assigned to i%5==0, so count = insertBatchSize/5
	require.Equal(t, int64(insertBatchSize/5), categoryCount,
		"Filter by category should return correct count via JSON path index")

	// Filter by price range (uses idx_price JSON path index)
	priceRes, err := mc.Query(ctx, client.NewQueryOption(restoredCollName).
		WithFilter(`metadata["price"] < 15`).
		WithOutputFields(common.QueryCountFieldName).
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	priceCount, _ := priceRes.Fields[0].GetAsInt64(0)
	// price = i * 1.5, so price < 15 means i < 10
	require.Equal(t, int64(10), priceCount,
		"Filter by price should return correct count via JSON path index")
	mlog.Info(context.TODO(), "Restored collection verified: query, search, and JSON path index filters OK")

	// Cleanup
	err = mc.DropSnapshot(ctx, client.NewDropSnapshotOption(snapshotName, collName))
	common.CheckErr(t, err, true)
	mlog.Info(context.TODO(), "Test completed: multiple JSON path indexes restored successfully")
}

// TestMilvusTableStructSnapshot validates native snapshot columns without SDK schema projection.
func TestMilvusTableStructSnapshot(t *testing.T) {
	for _, tc := range []struct {
		virtualPK     bool
		disableWarmup bool
	}{{}, {virtualPK: true}, {virtualPK: true, disableWarmup: true}} {
		t.Run(fmt.Sprintf("virtual_pk=%t/disable_warmup=%t", tc.virtualPK, tc.disableWarmup), func(t *testing.T) {
			ctx := hp.CreateContext(t, 10*time.Minute)
			mc := hp.CreateDefaultMilvusClient(ctx, t)
			source := common.GenRandomString("struct_source", 6)
			opt := hp.DefaultStructArraySchemaOption(source)
			sourceSchema, structSchema := hp.CreateStructArraySchema(opt)
			for _, field := range sourceSchema.Fields {
				field.WithTypeParams(pkcommon.WarmupKey, pkcommon.WarmupDisable)
				if field.Name == "clips" {
					field.WithNullable(true)
				}
			}
			for _, field := range structSchema.Fields {
				field.WithTypeParams(pkcommon.WarmupKey, pkcommon.WarmupDisable)
			}
			require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(source, sourceSchema).WithConsistencyLevel(entity.ClStrong)))
			defer mc.DropCollection(context.Background(), client.NewDropCollectionOption(source))
			data := hp.GenerateStructArrayData(16, opt)
			// Cold nullable vector-array statistics used to deadlock Vortex writer close.
			data.ClipsRows[0] = nil
			_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(source).
				WithInt64Column("id", data.IDs).
				WithFloatVectorColumn("normal_vector", data.Dim, data.NormalVectors).
				WithStructArrayColumn("clips", structSchema, data.ClipsRows))
			require.NoError(t, err)
			require.NoError(t, flushWithRetry(ctx, mc, source))
			indexAndLoad(t, ctx, mc, source, data.Dim)
			snapshot := common.GenRandomString("struct_snapshot", 6)
			require.NoError(t, mc.CreateSnapshot(ctx, client.NewCreateSnapshotOption(snapshot, source)))
			info, err := mc.DescribeSnapshot(ctx, client.NewDescribeSnapshotOption(snapshot, source))
			require.NoError(t, err)
			desc, err := mc.GetService().DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: source})
			require.NoError(t, err)
			require.NoError(t, merr.Error(desc.GetStatus()))
			schema := proto.Clone(desc.GetSchema()).(*schemapb.CollectionSchema)
			target := common.GenRandomString("struct_external", 6)
			schema.Name = target
			location, err := url.Parse(info.GetS3Location())
			require.NoError(t, err)
			cfg := getMinIOConfig()
			schema.ExternalSource = info.GetS3Location()
			if location.Scheme == "" {
				schema.ExternalSource = extTestURI(cfg, strings.TrimPrefix(location.Path, "/"))
			}
			schema.ExternalSpec = extTestSpec(cfg, "milvus-table")
			for _, field := range schema.Fields {
				field.ExternalField = field.Name
				field.AutoID = false
				if tc.virtualPK {
					field.IsPrimaryKey = false
				}
			}
			for _, parent := range schema.StructArrayFields {
				for _, field := range parent.Fields {
					field.ExternalField = ""
				}
			}
			// Selected Struct children retain their names; explicit aliases must be rejected.
			invalid := proto.Clone(schema).(*schemapb.CollectionSchema)
			invalid.Name = target + "_renamed"
			invalidChild := invalid.StructArrayFields[0].Fields[0]
			invalidChild.ExternalField = typeutil.ConcatStructFieldName(invalid.StructArrayFields[0].Name, invalidChild.Name)
			invalidChild.Name = "renamed_child"
			invalidBytes, err := proto.Marshal(invalid)
			require.NoError(t, err)
			invalidStatus, err := mc.GetService().CreateCollection(ctx, &milvuspb.CreateCollectionRequest{CollectionName: invalid.Name, Schema: invalidBytes, ShardsNum: 1})
			require.NoError(t, err)
			defer mc.DropCollection(context.Background(), client.NewDropCollectionOption(invalid.Name))
			require.ErrorIs(t, merr.Error(invalidStatus), merr.ErrParameterInvalid)
			encoded, err := proto.Marshal(schema)
			require.NoError(t, err)
			properties := []*commonpb.KeyValuePair{{Key: pkcommon.WarmupVectorIndexKey, Value: pkcommon.WarmupSync}}
			if tc.disableWarmup {
				properties = nil
				for _, key := range []string{pkcommon.WarmupScalarFieldKey, pkcommon.WarmupVectorFieldKey, pkcommon.WarmupScalarIndexKey, pkcommon.WarmupVectorIndexKey} {
					properties = append(properties, &commonpb.KeyValuePair{Key: key, Value: pkcommon.WarmupDisable})
				}
			}
			status, err := mc.GetService().CreateCollection(ctx, &milvuspb.CreateCollectionRequest{CollectionName: target, Schema: encoded, ShardsNum: 1, Properties: properties})
			require.NoError(t, err)
			require.NoError(t, merr.Error(status))
			defer mc.DropCollection(context.Background(), client.NewDropCollectionOption(target))
			refreshAndWait(ctx, t, mc, target)
			indexAndLoad(t, ctx, mc, target, hp.StructArrayDefaultDim)
			virtualPKs := make(map[int64]int64)
			assertRows := func(name, stringField string) {
				t.Helper()
				outputs := []string{"id", "normal_vector", "clips"}
				if tc.virtualPK && name == target {
					outputs = append(outputs, pkcommon.VirtualPKFieldName)
				}
				result, err := mc.Query(ctx, client.NewQueryOption(name).WithFilter("id >= 0").WithOutputFields(outputs...))
				require.NoError(t, err)
				require.Equal(t, 16, result.ResultCount)
				require.NotNil(t, result.GetColumn("id"))
				require.NotNil(t, result.GetColumn("clips"))
				require.NotNil(t, result.GetColumn("normal_vector"))
				for row := 0; row < result.ResultCount; row++ {
					id, err := result.GetColumn("id").GetAsInt64(row)
					require.NoError(t, err)
					require.GreaterOrEqual(t, id, int64(0))
					require.Less(t, id, int64(len(data.IDs)))
					if tc.virtualPK && name == target {
						pk, err := result.GetColumn(pkcommon.VirtualPKFieldName).GetAsInt64(row)
						require.NoError(t, err)
						if previous, ok := virtualPKs[id]; ok {
							require.Equal(t, previous, pk)
						} else {
							for _, previous := range virtualPKs {
								require.NotEqual(t, previous, pk)
							}
							virtualPKs[id] = pk
						}
					}
					clips, err := result.GetColumn("clips").Get(row)
					require.NoError(t, err)
					if data.ClipsRows[id] == nil {
						require.Nil(t, clips)
					} else {
						actualClips := clips.(map[string]any)
						require.Equal(t, data.ClipsRows[id]["clip_str"], actualClips[stringField])
						for _, fieldName := range []string{"clip_embedding1", "clip_embedding2"} {
							expected := data.ClipsRows[id][fieldName].([][]float32)
							actual := actualClips[fieldName].([]entity.FloatVector)
							require.Len(t, actual, len(expected))
							for i := range expected {
								require.Equal(t, entity.FloatVector(expected[i]), actual[i])
							}
						}
					}
					vector, err := result.GetColumn("normal_vector").Get(row)
					require.NoError(t, err)
					require.Equal(t, entity.FloatVector(data.NormalVectors[id]), vector)
				}
			}
			assertSearch := func(name string) {
				t.Helper()
				query := entity.FloatVectorArray{entity.FloatVector(data.ClipsRows[1]["clip_embedding1"].([][]float32)[0])}
				result, err := mc.Search(ctx, client.NewSearchOption(name, 1, []entity.Vector{query}).
					WithANNSField("clips[clip_embedding1]").WithOutputFields("id").WithSearchParam("ef", "64").
					WithConsistencyLevel(entity.ClStrong))
				require.NoError(t, err)
				require.Len(t, result, 1)
				require.NoError(t, result[0].Err)
				require.Equal(t, 1, result[0].ResultCount)
				id, err := result[0].GetColumn("id").GetAsInt64(0)
				require.NoError(t, err)
				require.EqualValues(t, 1, id)
				require.InDelta(t, 1, result[0].Scores[0], 1e-5)
				pk, err := result[0].IDs.GetAsInt64(0)
				require.NoError(t, err)
				if tc.virtualPK && name == target {
					require.Equal(t, virtualPKs[id], pk)
				} else {
					require.Equal(t, id, pk)
				}
			}
			assertRows(source, "clip_str")
			assertRows(target, "clip_str")
			assertSearch(source)
			assertSearch(target)
			require.NoError(t, mc.ReleaseCollection(ctx, client.NewReleaseCollectionOption(target)))
			task, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(target))
			require.NoError(t, err)
			require.NoError(t, task.Await(ctx))
			assertRows(target, "clip_str")
			assertSearch(target)
		})
	}
}

// TestMilvusTableStructArraySnapshot covers Array<T> children, whose physical
// columns are nested lists, including independent nulls at each list level.
// Run with queryNode.externalCollection.useTakeForOutput both true and false
// to exercise take projection and loaded-column retrieval respectively.
func TestMilvusTableStructArraySnapshot(t *testing.T) {
	for _, virtualPK := range []bool{false, true} {
		t.Run(fmt.Sprintf("virtual_pk=%t", virtualPK), func(t *testing.T) {
			ctx := hp.CreateContext(t, 10*time.Minute)
			mc := hp.CreateDefaultMilvusClient(ctx, t)
			source, target := common.GenRandomString("nested_src", 6), common.GenRandomString("nested_ext", 6)
			children := entity.NewStructSchema().
				WithField(entity.NewField().WithName("ignored").WithDataType(entity.FieldTypeInt64)).
				WithField(entity.NewField().WithName("lang").WithDataType(entity.FieldTypeVarChar).WithMaxLength(32).WithNullable(true)).
				WithField(entity.NewField().WithName("tags").WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeVarChar).
					WithMaxCapacity(4).WithMaxLength(32).WithNullable(true).WithElementNullable(true)).
				WithField(entity.NewField().WithName("nums").WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeInt64).
					WithMaxCapacity(4).WithNullable(true))
			schema := entity.NewSchema().WithName(source).
				WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
				WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(2)).
				WithField(entity.NewField().WithName("clips").WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeStruct).
					WithMaxCapacity(3).WithNullable(true).WithStructSchema(children))
			require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(source, schema).WithConsistencyLevel(entity.ClStrong)))
			defer mc.DropCollection(context.Background(), client.NewDropCollectionOption(source))
			row := func(id int, omit bool) map[string]any {
				value := fmt.Sprintf("值%d", id)
				switch id % 6 {
				case 0:
					return nil
				case 1:
					return map[string]any{"ignored": []int64{}, "lang": []*string{}, "tags": [][]*string{}, "nums": [][]int64{}}
				case 2:
					return map[string]any{"ignored": []int64{1, 2, 3}, "lang": []*string{&value, nil, &value},
						"tags": [][]*string{nil, {}, {&value, nil, elemNullStr("tail")}}, "nums": [][]int64{nil, {}, {int64(id), 2}}}
				case 3:
					if omit {
						return map[string]any{"ignored": []int64{1, 2}}
					}
					return map[string]any{"ignored": []int64{1, 2}, "lang": []*string{nil, nil}, "tags": [][]*string{nil, nil}, "nums": [][]int64{nil, nil}}
				case 4:
					return map[string]any{"ignored": []int64{1, 2, 3}, "lang": []*string{&value, &value, &value},
						"tags": [][]*string{{nil, nil}, {elemNullStr("")}, {&value}}, "nums": [][]int64{{int64(id)}, {}, nil}}
				default:
					return map[string]any{"ignored": []int64{1}, "lang": []*string{&value},
						"tags": [][]*string{{&value, elemNullStr("a"), elemNullStr("b"), elemNullStr("c")}}, "nums": [][]int64{{-1, 0, int64(id), 100}}}
				}
			}
			assertClips := func(value any, id int64, projected bool) {
				t.Helper()
				want := row(int(id), false)
				if want == nil {
					require.Nil(t, value)
					return
				}
				got, ok := value.(map[string]any)
				require.True(t, ok)
				if projected {
					delete(want, "ignored")
				}
				require.Len(t, got, len(want))
				for field, expected := range want {
					require.Contains(t, got, field)
					if id%6 == 1 {
						// Empty outer lists may decode to nil slices; inner null vs
						// empty lists below must remain distinguishable.
						require.Empty(t, got[field])
					} else {
						require.Equal(t, expected, got[field], "row=%d child=%s", id, field)
					}
				}
			}
			assertRows := func(name string, count int) {
				t.Helper()
				result, err := mc.Query(ctx, client.NewQueryOption(name).WithFilter("id >= 0").WithOutputFields("id", "clips").WithConsistencyLevel(entity.ClStrong))
				require.NoError(t, err)
				require.Equal(t, count, result.ResultCount)
				require.NotNil(t, result.GetColumn("id"))
				require.NotNil(t, result.GetColumn("clips"))
				seen := make(map[int64]bool)
				for i := 0; i < count; i++ {
					id, err := result.GetColumn("id").GetAsInt64(i)
					require.NoError(t, err)
					require.GreaterOrEqual(t, id, int64(0))
					require.Less(t, id, int64(count))
					require.False(t, seen[id])
					seen[id] = true
					clips, err := result.GetColumn("clips").Get(i)
					require.NoError(t, err)
					assertClips(clips, id, name == target)
				}
				for _, filter := range []struct {
					expr    string
					offsets map[int64][]int64
				}{
					{"$[lang] IS NULL", map[int64][]int64{2: {1}, 3: {0, 1}}},
					{"$[tags] IS NULL", map[int64][]int64{2: {0}, 3: {0, 1}}},
					{"$[nums] IS NULL", map[int64][]int64{2: {0}, 3: {0, 1}, 4: {2}}},
					{`ARRAY_CONTAINS($[tags], "tail")`, map[int64][]int64{2: {2}}},
					{"ARRAY_CONTAINS($[nums], -1)", map[int64][]int64{5: {0}}},
					{"ARRAY_LENGTH($[tags]) == 0", map[int64][]int64{2: {1}}},
				} {
					filtered, err := mc.Query(ctx, client.NewQueryOption(name).WithFilter("element_filter(clips, "+filter.expr+")").
						WithOutputFields("id").WithConsistencyLevel(entity.ClStrong))
					require.NoError(t, err, filter.expr)
					require.Equal(t, count/6*len(filter.offsets), filtered.ResultCount, filter.expr)
					require.Len(t, filtered.QueryElementOffsets, filtered.ResultCount)
					seen = make(map[int64]bool)
					for i := 0; i < filtered.ResultCount; i++ {
						id, err := filtered.GetColumn("id").GetAsInt64(i)
						require.NoError(t, err)
						require.GreaterOrEqual(t, id, int64(0))
						require.Less(t, id, int64(count))
						require.False(t, seen[id])
						seen[id] = true
						require.Contains(t, filter.offsets, id%6, filter.expr)
						require.Equal(t, filter.offsets[id%6], filtered.QueryElementOffsets[i], filter.expr)
					}
				}
				id := count - 4
				resultSearch, err := mc.Search(ctx, client.NewSearchOption(name, 1, []entity.Vector{entity.FloatVector{float32(id + 1), 1}}).
					WithANNSField("vector").WithOutputFields("id", "clips").WithConsistencyLevel(entity.ClStrong))
				require.NoError(t, err)
				require.Len(t, resultSearch, 1)
				require.NoError(t, resultSearch[0].Err)
				require.Equal(t, 1, resultSearch[0].ResultCount)
				gotID, err := resultSearch[0].GetColumn("id").GetAsInt64(0)
				require.NoError(t, err)
				require.EqualValues(t, id, gotID)
				require.InDelta(t, 0, resultSearch[0].Scores[0], 1e-5)
				clips, err := resultSearch[0].GetColumn("clips").Get(0)
				require.NoError(t, err)
				assertClips(clips, gotID, name == target)
			}
			cfg := getMinIOConfig()
			for round := 0; round < 2; round++ {
				ids, vectors, rows := []int64{}, [][]float32{}, []map[string]any{}
				for id := round * 6; id < (round+1)*6; id++ {
					ids = append(ids, int64(id))
					vectors = append(vectors, []float32{float32(id + 1), 1})
					rows = append(rows, row(id, true))
				}
				_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(source).WithInt64Column("id", ids).
					WithFloatVectorColumn("vector", 2, vectors).WithStructArrayColumn("clips", children, rows))
				require.NoError(t, err)
				require.NoError(t, flushWithRetry(ctx, mc, source))
				if round == 0 {
					indexAndLoadCollection(ctx, t, mc, source, "vector")
				} else {
					load, err := mc.RefreshLoad(ctx, client.NewRefreshLoadOption(source))
					require.NoError(t, err)
					require.NoError(t, load.Await(ctx))
				}
				assertRows(source, (round+1)*6)
				snapshot := common.GenRandomString("nested_snap", 6)
				require.NoError(t, mc.CreateSnapshot(ctx, client.NewCreateSnapshotOption(snapshot, source)))
				defer mc.DropSnapshot(context.Background(), client.NewDropSnapshotOption(snapshot, source))
				info, err := mc.DescribeSnapshot(ctx, client.NewDescribeSnapshotOption(snapshot, source))
				require.NoError(t, err)
				location := info.GetS3Location()
				if !strings.Contains(location, "://") {
					location = extTestURI(cfg, strings.TrimPrefix(location, "/"))
				}
				spec := extTestSpec(cfg, "milvus-table")
				if round == 0 {
					desc, err := mc.GetService().DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: source})
					require.NoError(t, err)
					require.NoError(t, merr.Error(desc.GetStatus()))
					mapped := proto.Clone(desc.GetSchema()).(*schemapb.CollectionSchema)
					mapped.Name, mapped.ExternalSource, mapped.ExternalSpec = target, location, spec
					for _, field := range mapped.Fields {
						field.ExternalField = field.Name
						if virtualPK {
							field.IsPrimaryKey = false
						}
					}
					parent := mapped.StructArrayFields[0]
					selected := []*schemapb.FieldSchema{}
					for _, child := range parent.Fields {
						if child.GetName() == "ignored" {
							continue
						}
						child.FieldID, child.ExternalField = 0, ""
						selected = append(selected, child)
					}
					parent.FieldID, parent.Fields = 0, selected
					require.Len(t, selected, 3)
					encoded, err := proto.Marshal(mapped)
					require.NoError(t, err)
					properties := []*commonpb.KeyValuePair{}
					for _, key := range []string{pkcommon.WarmupScalarFieldKey, pkcommon.WarmupVectorFieldKey, pkcommon.WarmupScalarIndexKey, pkcommon.WarmupVectorIndexKey} {
						properties = append(properties, &commonpb.KeyValuePair{Key: key, Value: pkcommon.WarmupDisable})
					}
					status, err := mc.GetService().CreateCollection(ctx, &milvuspb.CreateCollectionRequest{CollectionName: target, Schema: encoded, ShardsNum: 1, Properties: properties})
					require.NoError(t, err)
					require.NoError(t, merr.Error(status))
					defer mc.DropCollection(context.Background(), client.NewDropCollectionOption(target))
				}
				job, err := mc.RefreshExternalCollection(ctx, client.NewRefreshExternalCollectionOption(target).WithExternalSource(location).WithExternalSpec(spec))
				require.NoError(t, err)
				waitForRefreshComplete(ctx, t, mc, job.JobID, 2*time.Minute)
				if round == 0 {
					indexAndLoadCollection(ctx, t, mc, target, "vector")
				} else {
					load, err := mc.RefreshLoad(ctx, client.NewRefreshLoadOption(target))
					require.NoError(t, err)
					require.NoError(t, load.Await(ctx))
				}
				assertRows(target, (round+1)*6)
			}
			require.NoError(t, mc.ReleaseCollection(ctx, client.NewReleaseCollectionOption(target)))
			load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(target))
			require.NoError(t, err)
			require.NoError(t, load.Await(ctx))
			assertRows(target, 12)
		})
	}
}

// TestMilvusTableAddedStructFieldSnapshot distinguishes internal schema history
// from physical column availability, using a real AddCollectionField operation.
func TestMilvusTableAddedStructFieldSnapshot(t *testing.T) {
	for _, oldSegment := range []bool{false, true} {
		t.Run(fmt.Sprintf("pre_add_segment=%t", oldSegment), func(t *testing.T) {
			ctx := hp.CreateContext(t, 10*time.Minute)
			mc := hp.CreateDefaultMilvusClient(ctx, t)
			source, target := common.GenRandomString("added_src", 6), common.GenRandomString("added_ext", 6)
			children := entity.NewStructSchema().WithField(entity.NewField().WithName("base").WithDataType(entity.FieldTypeInt64))
			schema := entity.NewSchema().WithName(source).
				WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
				WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(2)).
				WithField(entity.NewField().WithName("clips").WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeStruct).
					WithMaxCapacity(4).WithNullable(true).WithStructSchema(children))
			require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(source, schema).WithConsistencyLevel(entity.ClStrong)))
			defer mc.DropCollection(context.Background(), client.NewDropCollectionOption(source))
			if oldSegment {
				_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(source).
					WithInt64Column("id", []int64{100}).WithFloatVectorColumn("vector", 2, [][]float32{{1, 0}}).
					WithStructArrayColumn("clips", children, []map[string]any{{"base": []int64{1}}}))
				require.NoError(t, err)
				require.NoError(t, flushWithRetry(ctx, mc, source))
			}
			require.NoError(t, mc.AddCollectionField(ctx, client.NewAddCollectionFieldOption(source,
				entity.NewField().WithName("clips[added]").WithDataType(entity.FieldTypeInt64).WithNullable(true))))
			coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(source))
			require.NoError(t, err)
			children = elemNullFindStruct(t, coll.Schema, "clips")
			desc, err := mc.GetService().DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: source})
			require.NoError(t, err)
			require.NoError(t, merr.Error(desc.GetStatus()))
			var added *schemapb.FieldSchema
			for _, field := range desc.GetSchema().GetStructArrayFields()[0].GetFields() {
				if field.GetName() == "added" {
					added = field
				}
			}
			require.NotNil(t, added)
			for _, kv := range added.GetTypeParams() {
				require.NotEqual(t, pkcommon.StructSubFieldAddedKey, kv.GetKey(), "public schema must hide internal history")
			}
			cfg := getMinIOConfig()
			minioClient, err := newMinIOClient(cfg)
			require.NoError(t, err)
			for round := 0; round < 2; round++ {
				value := int64(10 + round)
				rows := []map[string]any{nil, {"base": []int64{}, "added": []*int64{}},
					{"base": []int64{1, 2}, "added": []*int64{&value, nil}},
					{"base": []int64{3, 4}, "added": []*int64{nil, nil}}}
				start := int64(round * 4)
				_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(source).
					WithInt64Column("id", []int64{start, start + 1, start + 2, start + 3}).
					WithFloatVectorColumn("vector", 2, [][]float32{{1, 0}, {0, 1}, {1, 1}, {2, 1}}).
					WithStructArrayColumn("clips", children, rows))
				require.NoError(t, err)
				require.NoError(t, flushWithRetry(ctx, mc, source))
				if round == 0 {
					indexAndLoadCollection(ctx, t, mc, source, "vector")
				}
				snapshot := common.GenRandomString("added_snapshot", 6)
				require.NoError(t, mc.CreateSnapshot(ctx, client.NewCreateSnapshotOption(snapshot, source)))
				defer mc.DropSnapshot(context.Background(), client.NewDropSnapshotOption(snapshot, source))
				info, err := mc.DescribeSnapshot(ctx, client.NewDescribeSnapshotOption(snapshot, source))
				require.NoError(t, err)
				location := info.GetS3Location()
				serverCfg := snapshotFixtureStorageConfig(t, cfg, location)
				key, err := snapshotFixtureObjectKey(location, cfg.bucket, serverCfg.address, "", "")
				require.NoError(t, err)
				object, err := minioClient.GetObject(ctx, cfg.bucket, key, miniogo.GetObjectOptions{})
				require.NoError(t, err)
				data, err := io.ReadAll(object)
				require.NoError(t, object.Close())
				require.NoError(t, err)
				metadata, err := snapshotio.ParseSnapshotMetadata(data)
				require.NoError(t, err)
				marked := false
				for _, field := range metadata.GetCollection().GetSchema().GetStructArrayFields()[0].GetFields() {
					if field.GetFieldID() == added.GetFieldID() {
						for _, kv := range field.GetTypeParams() {
							marked = marked || kv.GetKey() == pkcommon.StructSubFieldAddedKey
						}
					}
				}
				require.True(t, marked, "real snapshot must exercise the internal origin marker")
				spec := extTestSpec(cfg, "milvus-table")
				if round == 0 {
					targetSchema := proto.Clone(desc.GetSchema()).(*schemapb.CollectionSchema)
					targetSchema.Name, targetSchema.ExternalSource, targetSchema.ExternalSpec = target, location, spec
					for _, field := range targetSchema.Fields {
						field.ExternalField = field.Name
					}
					parent := targetSchema.StructArrayFields[0]
					parent.FieldID = 0
					parent.Fields = []*schemapb.FieldSchema{proto.Clone(added).(*schemapb.FieldSchema)}
					parent.Fields[0].FieldID = 0
					encoded, err := proto.Marshal(targetSchema)
					require.NoError(t, err)
					status, err := mc.GetService().CreateCollection(ctx, &milvuspb.CreateCollectionRequest{CollectionName: target, Schema: encoded, ShardsNum: 1})
					require.NoError(t, err)
					require.NoError(t, merr.Error(status))
					defer mc.DropCollection(context.Background(), client.NewDropCollectionOption(target))
				}
				job, err := mc.RefreshExternalCollection(ctx, client.NewRefreshExternalCollectionOption(target).WithExternalSource(location).WithExternalSpec(spec))
				require.NoError(t, err)
				if oldSegment {
					require.Eventually(t, func() bool {
						progress, err := mc.GetRefreshExternalCollectionProgress(ctx, client.NewGetRefreshExternalCollectionProgressOption(job.JobID))
						require.NoError(t, err)
						require.NotEqual(t, entity.RefreshStateCompleted, progress.State, "missing physical data must not be silently synthesized")
						if progress.State != entity.RefreshStateFailed {
							return false
						}
						require.Contains(t, progress.Reason, fmt.Sprint(added.GetFieldID()))
						require.Contains(t, strings.ToLower(progress.Reason), "column")
						require.NotContains(t, progress.Reason, "definition mismatch")
						return true
					}, 2*time.Minute, time.Second)
					return
				}
				waitForRefreshComplete(ctx, t, mc, job.JobID, 2*time.Minute)
				if round == 0 {
					indexAndLoadCollection(ctx, t, mc, target, "vector")
				} else {
					load, err := mc.RefreshLoad(ctx, client.NewRefreshLoadOption(target))
					require.NoError(t, err)
					require.NoError(t, load.Await(ctx))
				}
				result, err := mc.Query(ctx, client.NewQueryOption(target).WithFilter("id >= 0").WithOutputFields("id", "clips"))
				require.NoError(t, err)
				require.Equal(t, (round+1)*4, result.ResultCount)
				seen := make(map[int64]bool)
				for i := 0; i < result.ResultCount; i++ {
					id, err := result.GetColumn("id").GetAsInt64(i)
					require.NoError(t, err)
					require.GreaterOrEqual(t, id, int64(0))
					require.Less(t, id, int64((round+1)*4))
					require.False(t, seen[id])
					seen[id] = true
					clips, err := result.GetColumn("clips").Get(i)
					require.NoError(t, err)
					if id%4 == 0 {
						require.Nil(t, clips)
						continue
					}
					actual := clips.(map[string]any)
					require.Len(t, actual, 1, "unselected sibling must not leak")
					expected := []*int64{}
					if id%4 == 2 {
						v := int64(10 + id/4)
						expected = []*int64{&v, nil}
					} else if id%4 == 3 {
						expected = []*int64{nil, nil}
					}
					require.Equal(t, expected, actual["added"])
				}
			}
		})
	}
}

// TestMilvusTableVectorArraySnapshot exercises each fixed-width vector encoding through
// a real snapshot, Struct subset mapping, virtual PK, cold load and a second snapshot refresh.
// Set indexCoord.segment.minSegmentNumRowsToEnableIndex=1 to exercise persisted indexes
// with these small fixtures; the default threshold exercises brute-force search instead.
func TestMilvusTableVectorArraySnapshot(t *testing.T) {
	const dim = 32
	floatVector := func(seed int) entity.FloatVector {
		v := make(entity.FloatVector, dim)
		v[seed%dim] = 1.5
		v[(seed+dim/2)%dim] = -0.5
		return v
	}
	t.Run("float32", func(t *testing.T) {
		testMilvusTableVectorArraySnapshot(t, floatVector, func(v []entity.FloatVector) entity.Vector { return entity.FloatVectorArray(v) })
	})
	t.Run("float16", func(t *testing.T) {
		testMilvusTableVectorArraySnapshot(t, func(i int) entity.Float16Vector { return floatVector(i).ToFloat16Vector() },
			func(v []entity.Float16Vector) entity.Vector { return entity.Float16VectorArray(v) })
	})
	t.Run("bfloat16", func(t *testing.T) {
		testMilvusTableVectorArraySnapshot(t, func(i int) entity.BFloat16Vector { return floatVector(i).ToBFloat16Vector() },
			func(v []entity.BFloat16Vector) entity.Vector { return entity.BFloat16VectorArray(v) })
	})
	t.Run("int8", func(t *testing.T) {
		testMilvusTableVectorArraySnapshot(t, func(i int) entity.Int8Vector {
			v := make(entity.Int8Vector, dim)
			v[i%dim] = 3
			v[(i+dim/2)%dim] = -1
			return v
		}, func(v []entity.Int8Vector) entity.Vector { return entity.Int8VectorArray(v) })
	})
	t.Run("binary", func(t *testing.T) {
		testMilvusTableVectorArraySnapshot(t, func(i int) entity.BinaryVector {
			v := make(entity.BinaryVector, dim/8)
			v[(i%dim)/8] = 1 << (i % 8)
			return v
		}, func(v []entity.BinaryVector) entity.Vector { return entity.BinaryVectorArray(v) })
	})
}

func testMilvusTableVectorArraySnapshot[V entity.Vector](t *testing.T, vector func(int) V, vectorArray func([]V) entity.Vector) {
	for _, elementNullable := range []bool{false, true} {
		t.Run(fmt.Sprintf("element_nullable=%t", elementNullable), func(t *testing.T) {
			ctx := hp.CreateContext(t, 10*time.Minute)
			mc := hp.CreateDefaultMilvusClient(ctx, t)
			source, target := common.GenRandomString("va_src", 6), common.GenRandomString("va_ext", 6)
			v := vector(0)
			// A source-only first child makes positional ID alignment incorrect.
			children := entity.NewStructSchema().
				WithField(entity.NewField().WithName("ignored").WithDataType(entity.FieldTypeInt64)).
				WithField(entity.NewField().WithName("slot").WithDataType(entity.FieldTypeInt64)).
				WithField(entity.NewField().WithName("vec").WithDataType(v.FieldType()).WithDim(int64(v.Dim())).WithNullable(elementNullable))
			schema := entity.NewSchema().WithName(source).
				WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
				WithField(entity.NewField().WithName("clips").WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeStruct).
					WithMaxCapacity(4).WithNullable(true).WithStructSchema(children))
			require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(source, schema).WithConsistencyLevel(entity.ClStrong)))
			defer mc.DropCollection(context.Background(), client.NewDropCollectionOption(source))

			metric := entity.MaxSimCosine
			if elementNullable {
				metric = entity.COSINE
			}
			if v.FieldType() == entity.FieldTypeBinaryVector {
				metric = entity.MaxSimHamming
				if elementNullable {
					metric = entity.HAMMING
				}
			}
			indexAndLoadVector := func(name, field string) {
				t.Helper()
				task, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(name, field, index.NewHNSWIndex(metric, 16, 200)))
				require.NoError(t, err)
				require.NoError(t, task.Await(ctx))
				load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
				require.NoError(t, err)
				require.NoError(t, load.Await(ctx))
			}
			row := func(id int) map[string]any {
				switch id % 6 {
				case 0:
					return nil // Null parent is distinct from an empty array.
				case 1:
					return map[string]any{"ignored": []int64{}, "slot": []int64{}, "vec": []V{}}
				}
				vectors := []V{vector(2 * id), vector(2*id + 1)}
				if elementNullable {
					var null V
					if id%6 == 2 || id%6 == 3 {
						vectors[0] = null
					}
					if id%6 == 3 {
						vectors[1] = null
					}
				}
				return map[string]any{"ignored": []int64{int64(id), int64(id)}, "slot": []int64{int64(2 * id), int64(2*id + 1)}, "vec": vectors}
			}
			assertClips := func(got any, child string, id int, projected bool) {
				t.Helper()
				want := row(id)
				if want == nil {
					require.Nil(t, got)
					return
				}
				clips, ok := got.(map[string]any)
				require.True(t, ok, "row %d clips must be a struct array", id)
				if projected {
					require.Len(t, clips, 2)
					require.NotContains(t, clips, "ignored")
				} else {
					require.Len(t, clips, 3)
					require.Contains(t, clips, "ignored")
					if id%6 != 1 {
						require.Equal(t, want["ignored"], clips["ignored"])
					}
				}
				require.Contains(t, clips, "slot")
				require.Contains(t, clips, child)
				if id%6 == 1 {
					// Protobuf may represent an empty repeated field as a nil slice.
					require.Len(t, clips["slot"], 0)
					require.Len(t, clips[child], 0)
				} else {
					require.Equal(t, want["slot"], clips["slot"])
					require.Equal(t, want["vec"], clips[child], "row %d vector payload/element validity", id)
				}
			}
			virtualPKs := make(map[int64]int64)
			assertRows := func(name, child string, count int) {
				t.Helper()
				outputs := []string{"id", "clips"}
				if name == target {
					outputs = append(outputs, pkcommon.VirtualPKFieldName)
				}
				result, err := mc.Query(ctx, client.NewQueryOption(name).WithFilter("id >= 0").WithOutputFields(outputs...).WithConsistencyLevel(entity.ClStrong))
				require.NoError(t, err)
				require.Equal(t, count, result.ResultCount)
				seen := make(map[int64]bool)
				for i := 0; i < result.ResultCount; i++ {
					id, err := result.GetColumn("id").GetAsInt64(i)
					require.NoError(t, err)
					require.True(t, id >= 0 && id < int64(count))
					require.False(t, seen[id], "duplicate source row %d", id)
					seen[id] = true
					got, err := result.GetColumn("clips").Get(i)
					require.NoError(t, err)
					assertClips(got, child, int(id), name == target)
					if name == target {
						pk, err := result.GetColumn(pkcommon.VirtualPKFieldName).GetAsInt64(i)
						require.NoError(t, err)
						if previous, ok := virtualPKs[id]; ok {
							require.Equal(t, previous, pk, "virtual PK changed after reload")
						} else {
							for _, previous := range virtualPKs {
								require.NotEqual(t, previous, pk)
							}
							virtualPKs[id] = pk
						}
					}
				}
			}
			assertSearch := func(name, child string, id int) {
				t.Helper()
				query := entity.Vector(vectorArray([]V{vector(2 * id), vector(2*id + 1)}))
				wantScore := float64(2)
				if elementNullable {
					query = vector(2*id + 1)
					wantScore = 1
				}
				if v.FieldType() == entity.FieldTypeBinaryVector {
					wantScore = 0
				}
				result, err := mc.Search(ctx, client.NewSearchOption(name, 1, []entity.Vector{query}).
					WithANNSField(typeutil.ConcatStructFieldName("clips", child)).
					WithSearchParam("ef", "64").WithOutputFields("id", "clips").WithConsistencyLevel(entity.ClStrong))
				require.NoError(t, err)
				require.Len(t, result, 1)
				require.NoError(t, result[0].Err)
				require.Equal(t, 1, result[0].ResultCount)
				gotID, err := result[0].GetColumn("id").GetAsInt64(0)
				require.NoError(t, err)
				require.EqualValues(t, id, gotID)
				clipsColumn := result[0].GetColumn("clips")
				require.NotNil(t, clipsColumn)
				clips, err := clipsColumn.Get(0)
				require.NoError(t, err)
				assertClips(clips, child, id, name == target)
				require.InDelta(t, wantScore, result[0].Scores[0], 1e-5)
				pk, err := result[0].IDs.GetAsInt64(0)
				require.NoError(t, err)
				if name == target {
					require.Equal(t, virtualPKs[gotID], pk)
				} else {
					require.Equal(t, gotID, pk)
				}
				if elementNullable {
					require.Equal(t, []int64{1}, result[0].ElementOffsets)
				}
			}

			for round := 0; round < 2; round++ {
				ids, rows := make([]int64, 6), make([]map[string]any, 6)
				for i := range ids {
					ids[i] = int64(round*6 + i)
					rows[i] = row(int(ids[i]))
				}
				_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(source).WithInt64Column("id", ids).WithStructArrayColumn("clips", children, rows))
				require.NoError(t, err)
				require.NoError(t, flushWithRetry(ctx, mc, source))
				if round == 0 {
					indexAndLoadVector(source, "clips[vec]")
				} else {
					load, err := mc.RefreshLoad(ctx, client.NewRefreshLoadOption(source))
					require.NoError(t, err)
					require.NoError(t, load.Await(ctx))
				}
				assertRows(source, "vec", (round+1)*6)
				assertSearch(source, "vec", round*6+2)
				snapshot := common.GenRandomString("va_snap", 6)
				require.NoError(t, mc.CreateSnapshot(ctx, client.NewCreateSnapshotOption(snapshot, source)))
				defer mc.DropSnapshot(context.Background(), client.NewDropSnapshotOption(snapshot, source))
				info, err := mc.DescribeSnapshot(ctx, client.NewDescribeSnapshotOption(snapshot, source))
				require.NoError(t, err)
				location := info.GetS3Location()
				if !strings.Contains(location, "://") {
					location = extTestURI(getMinIOConfig(), strings.TrimPrefix(location, "/"))
				}
				spec := extTestSpec(getMinIOConfig(), "milvus-table")
				if round == 0 {
					desc, err := mc.GetService().DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: source})
					require.NoError(t, err)
					require.NoError(t, merr.Error(desc.GetStatus()))
					schema := proto.Clone(desc.GetSchema()).(*schemapb.CollectionSchema)
					schema.Name, schema.ExternalSource, schema.ExternalSpec = target, location, spec
					for _, field := range schema.Fields {
						field.ExternalField, field.IsPrimaryKey, field.AutoID = field.Name, false, false
					}
					for _, parent := range schema.StructArrayFields {
						selected := make([]*schemapb.FieldSchema, 0, len(parent.Fields))
						for _, field := range parent.Fields {
							if field.GetName() == "ignored" {
								continue
							}
							selected = append(selected, field)
							field.FieldID, field.ExternalField = 0, ""
							if field.GetDataType() == schemapb.DataType_ArrayOfVector {
								require.Equal(t, elementNullable, field.GetElementNullable())
							}
						}
						parent.FieldID, parent.Fields = 0, selected
						require.Len(t, parent.Fields, 2)
					}
					encoded, err := proto.Marshal(schema)
					require.NoError(t, err)
					properties := []*commonpb.KeyValuePair{}
					for _, key := range []string{pkcommon.WarmupScalarFieldKey, pkcommon.WarmupVectorFieldKey, pkcommon.WarmupScalarIndexKey, pkcommon.WarmupVectorIndexKey} {
						properties = append(properties, &commonpb.KeyValuePair{Key: key, Value: pkcommon.WarmupDisable})
					}
					status, err := mc.GetService().CreateCollection(ctx, &milvuspb.CreateCollectionRequest{CollectionName: target, Schema: encoded, ShardsNum: 1, Properties: properties})
					require.NoError(t, err)
					require.NoError(t, merr.Error(status))
					defer mc.DropCollection(context.Background(), client.NewDropCollectionOption(target))
					created, err := mc.GetService().DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: target})
					require.NoError(t, err)
					require.NoError(t, merr.Error(created.GetStatus()))
					require.Len(t, created.GetSchema().GetStructArrayFields(), 1)
					createdParent := created.GetSchema().GetStructArrayFields()[0]
					sourceParent := desc.GetSchema().GetStructArrayFields()[0]
					require.Equal(t, sourceParent.GetFieldID(), createdParent.GetFieldID())
					require.Len(t, createdParent.GetFields(), 2)
					sourceIDs := make(map[string]int64)
					for _, field := range sourceParent.GetFields() {
						sourceIDs[field.GetName()] = field.GetFieldID()
					}
					for _, field := range createdParent.GetFields() {
						require.Contains(t, []string{"slot", "vec"}, field.GetName())
						require.Equal(t, sourceIDs[field.GetName()], field.GetFieldID())
					}
				}
				job, err := mc.RefreshExternalCollection(ctx, client.NewRefreshExternalCollectionOption(target).WithExternalSource(location).WithExternalSpec(spec))
				require.NoError(t, err)
				waitForRefreshComplete(ctx, t, mc, job.JobID, 2*time.Minute)
				if round == 0 {
					indexAndLoadVector(target, "clips[vec]")
				} else {
					load, err := mc.RefreshLoad(ctx, client.NewRefreshLoadOption(target))
					require.NoError(t, err)
					require.NoError(t, load.Await(ctx))
				}
				// Virtual PKs encode the target segment ID. Refresh may replace segments,
				// so compare Query/Search within this snapshot and require reload stability.
				virtualPKs = make(map[int64]int64)
				assertRows(target, "vec", (round+1)*6)
				assertSearch(target, "vec", round*6+2)
				if elementNullable {
					result, err := mc.Query(ctx, client.NewQueryOption(target).
						WithFilter("element_filter(clips, $[vec] IS NULL)").WithOutputFields("id").WithConsistencyLevel(entity.ClStrong))
					require.NoError(t, err)
					require.Equal(t, (round+1)*2, result.ResultCount)
					for i := 0; i < result.ResultCount; i++ {
						id, err := result.GetColumn("id").GetAsInt64(i)
						require.NoError(t, err)
						require.Contains(t, []int64{2, 3}, id%6)
						want := []int64{0}
						if id%6 == 3 {
							want = []int64{0, 1}
						}
						require.Equal(t, want, result.QueryElementOffsets[i])
					}
				}
			}
			require.NoError(t, mc.ReleaseCollection(ctx, client.NewReleaseCollectionOption(target)))
			load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(target))
			require.NoError(t, err)
			require.NoError(t, load.Await(ctx))
			assertRows(target, "vec", 12)
			assertSearch(target, "vec", 8)
		})
	}
}

func TestMilvusTableDynamicSnapshot(t *testing.T) {
	ctx := hp.CreateContext(t, 10*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	source, target := common.GenRandomString("dyn_src", 6), common.GenRandomString("dyn_ext", 6)
	schema := entity.NewSchema().WithName(source).WithDynamicFieldEnabled(true).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(4))
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(source, schema)))
	defer mc.DropCollection(context.Background(), client.NewDropCollectionOption(source))
	for round := 0; round < 2; round++ {
		_, err := mc.Insert(ctx, client.NewRowBasedInsertOption(source,
			map[string]any{"id": int64(round), "vector": []float32{1, 0, 0, 0}, "tag": "visible", "score": int64(round + 10)},
			map[string]any{"id": int64(round + 100), "vector": []float32{0, 1, 0, 0}, "tag": "hidden", "score": int64(round + 10)},
			map[string]any{"id": int64(round + 200), "vector": []float32{0, 0, 1, 0}, "tag": "visible", "score": int64(0)},
		))
		require.NoError(t, err)
		require.NoError(t, flushWithRetry(ctx, mc, source))
		snapshot := common.GenRandomString("dyn_snap", 6)
		require.NoError(t, mc.CreateSnapshot(ctx, client.NewCreateSnapshotOption(snapshot, source)))
		info, err := mc.DescribeSnapshot(ctx, client.NewDescribeSnapshotOption(snapshot, source))
		require.NoError(t, err)
		location := info.GetS3Location()
		if !strings.Contains(location, "://") {
			location = extTestURI(getMinIOConfig(), strings.TrimPrefix(location, "/"))
		}
		spec := extTestSpec(getMinIOConfig(), "milvus-table")
		if round == 0 {
			targetSchema := entity.NewSchema().WithName(target).WithDynamicFieldEnabled(true).WithExternalSource(location).WithExternalSpec(spec).
				WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true).WithExternalField("id")).
				WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(4).WithExternalField("vector"))
			require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(target, targetSchema)))
			defer mc.DropCollection(context.Background(), client.NewDropCollectionOption(target))
		}
		job, err := mc.RefreshExternalCollection(ctx, client.NewRefreshExternalCollectionOption(target).WithExternalSource(location).WithExternalSpec(spec))
		require.NoError(t, err)
		waitForRefreshComplete(ctx, t, mc, job.JobID, 2*time.Minute)
		if round == 0 {
			indexAndLoadCollection(ctx, t, mc, target, "vector")
		} else {
			// Refresh completion publishes DataCoord metadata; wait for the loaded target too.
			loadTask, err := mc.RefreshLoad(ctx, client.NewRefreshLoadOption(target))
			require.NoError(t, err)
			require.NoError(t, loadTask.Await(ctx))
		}
		result, err := mc.Query(ctx, client.NewQueryOption(target).WithFilter(`tag == "visible" && score >= 10`).WithOutputFields("id", "tag", "score", "$meta").WithConsistencyLevel(entity.ClStrong))
		require.NoError(t, err)
		require.Equal(t, round+1, result.ResultCount)
		require.NotNil(t, result.GetColumn("tag"))
		require.NotNil(t, result.GetColumn("score"))
		for row := 0; row < result.ResultCount; row++ {
			id, err := result.GetColumn("id").GetAsInt64(row)
			require.NoError(t, err)
			require.GreaterOrEqual(t, id, int64(0))
			require.LessOrEqual(t, id, int64(round))
			tag, err := result.GetColumn("tag").GetAsString(row)
			require.NoError(t, err)
			require.Equal(t, "visible", tag)
			score, err := result.GetColumn("score").GetAsInt64(row)
			require.NoError(t, err)
			require.Equal(t, id+10, score)
		}
		metaResult, err := mc.Query(ctx, client.NewQueryOption(target).WithFilter(fmt.Sprintf("id == %d", round)).WithOutputFields("$meta").WithConsistencyLevel(entity.ClStrong))
		require.NoError(t, err)
		require.Equal(t, 1, metaResult.ResultCount)
		require.NotNil(t, metaResult.GetColumn("$meta"))
		meta, err := metaResult.GetColumn("$meta").Get(0)
		require.NoError(t, err)
		require.JSONEq(t, fmt.Sprintf(`{"tag":"visible","score":%d}`, round+10), string(meta.([]byte)))
	}
}

// Import physical null $meta rows: ordinary Insert and sort compaction fill them with {}.
// Run on a dedicated instance with legacy import, sort compaction disabled, and
// statsTaskPendingLimit=-1 so TEXT_MATCH must build its index from source data.
func TestMilvusTableNullableDynamicTextSnapshot(t *testing.T) {
	for key, want := range map[string]string{
		"dataCoord.import.enableImportV3": "false",
		"dataCoord.sortCompaction.enable": "false",
		"dataCoord.statsTaskPendingLimit": "-1",
	} {
		value, err := hp.GetServerConfig(key)
		if err != nil || value != want {
			t.Skipf("requires a dedicated null-meta fixture instance with %s=%s (got %q, %v)", key, want, value, err)
		}
	}
	ctx := hp.CreateContext(t, 15*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	cfg := getMinIOConfig()
	minioClient, err := newMinIOClient(cfg)
	require.NoError(t, err)
	source, target := common.GenRandomString("null_meta_src", 6), common.GenRandomString("null_meta_ext", 6)
	prefix := "external-e2e-test/" + source
	t.Cleanup(func() { cleanupMinIOPrefix(context.Background(), minioClient, cfg.bucket, prefix) })
	makeSchema := func(name string, external bool) *entity.Schema {
		id := entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(!external)
		text := entity.NewField().WithName("text").WithDataType(entity.FieldTypeVarChar).WithMaxLength(1024).
			WithEnableAnalyzer(true).WithEnableMatch(true).WithTypeParams(pkcommon.WarmupKey, pkcommon.WarmupDisable)
		vec := entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(4).
			WithTypeParams(pkcommon.WarmupKey, pkcommon.WarmupSync)
		if external {
			id.WithExternalField("id")
			text.WithExternalField("text")
			vec.WithExternalField("vector")
		}
		return entity.NewSchema().WithName(name).WithDynamicFieldEnabled(true).WithField(id).WithField(text).WithField(vec)
	}
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(source, makeSchema(source, false))))
	t.Cleanup(func() { _ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(source)) })
	baseURL, apiKey := snapshotImportRESTConfig()
	seenSegments := make(map[int64]bool)
	for round := 0; round < 2; round++ {
		arrowSchema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "text", Type: arrow.BinaryTypes.String},
			{Name: "vector", Type: arrow.ListOfNonNullable(arrow.PrimitiveTypes.Float32)},
			{Name: "$meta", Type: arrow.BinaryTypes.String, Nullable: true},
		}, nil)
		builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
		for row := 0; row < 3; row++ {
			builder.Field(0).(*array.Int64Builder).Append(int64(round*3 + row))
			builder.Field(1).(*array.StringBuilder).Append("mandatory text snapshot")
			vec := builder.Field(2).(*array.ListBuilder)
			vec.Append(true)
			vec.ValueBuilder().(*array.Float32Builder).AppendValues([]float32{1, 0, 0, 0}, nil)
			meta := builder.Field(3).(*array.StringBuilder)
			if row == 0 {
				meta.AppendNull()
			} else {
				meta.Append(`{"tag":"visible","hidden":true}`)
			}
		}
		record := builder.NewRecord()
		var buf bytes.Buffer
		writer, err := pqarrow.NewFileWriter(arrowSchema, &buf, nil, pqarrow.DefaultWriterProps())
		require.NoError(t, err)
		require.NoError(t, writer.Write(record))
		require.NoError(t, writer.Close())
		record.Release()
		builder.Release()
		key := fmt.Sprintf("%s/round%d.parquet", prefix, round)
		uploadParquetToMinIO(ctx, t, minioClient, cfg.bucket, key, buf.Bytes())
		imported, err := bulkwriter.BulkImport(ctx, bulkwriter.NewBulkImportOption(baseURL, source, [][]string{{key}}).WithAPIKey(apiKey))
		require.NoError(t, err)
		require.Empty(t, imported.Message)
		require.NotEmpty(t, imported.Data.JobID)
		_, err = waitForSnapshotImportState(ctx, baseURL, apiKey, imported.Data.JobID, "Completed", 5*time.Minute)
		require.NoError(t, err)
		if round == 0 {
			indexAndLoadCollection(ctx, t, mc, source, "vector")
		} else {
			task, err := mc.RefreshLoad(ctx, client.NewRefreshLoadOption(source))
			require.NoError(t, err)
			require.NoError(t, task.Await(ctx))
		}
		// Prove the fixture is a physical null before snapshotting it.
		raw, err := mc.GetService().Query(ctx, &milvuspb.QueryRequest{CollectionName: source, Expr: fmt.Sprintf("id == %d", round*3), OutputFields: []string{"$meta"}, ConsistencyLevel: commonpb.ConsistencyLevel_Strong})
		require.NoError(t, err)
		require.NoError(t, merr.Error(raw.GetStatus()))
		var sourceMeta *schemapb.FieldData
		for _, field := range raw.GetFieldsData() {
			if field.GetIsDynamic() {
				sourceMeta = field
			}
		}
		require.NotNil(t, sourceMeta)
		require.Equal(t, []bool{false}, typeutil.GetFieldDataValidData(sourceMeta))
		snapshot := common.GenRandomString("null_meta_snap", 6)
		require.NoError(t, mc.CreateSnapshot(ctx, client.NewCreateSnapshotOption(snapshot, source)))
		info, err := mc.DescribeSnapshot(ctx, client.NewDescribeSnapshotOption(snapshot, source))
		require.NoError(t, err)
		location := info.GetS3Location()
		if !strings.Contains(location, "://") {
			location = extTestURI(cfg, strings.TrimPrefix(location, "/"))
		}
		spec := extTestSpec(cfg, "milvus-table")
		if round == 0 {
			schema := makeSchema(target, true).WithExternalSource(location).WithExternalSpec(spec)
			opt := client.NewCreateCollectionOption(target, schema)
			for _, key := range []string{pkcommon.WarmupScalarFieldKey, pkcommon.WarmupVectorFieldKey, pkcommon.WarmupScalarIndexKey, pkcommon.WarmupVectorIndexKey} {
				opt.WithProperty(key, pkcommon.WarmupDisable)
			}
			require.NoError(t, mc.CreateCollection(ctx, opt))
			t.Cleanup(func() { _ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(target)) })
		}
		job, err := mc.RefreshExternalCollection(ctx, client.NewRefreshExternalCollectionOption(target).WithExternalSource(location).WithExternalSpec(spec))
		require.NoError(t, err)
		waitForRefreshComplete(ctx, t, mc, job.JobID, 2*time.Minute)
		if round == 0 {
			indexAndLoadCollection(ctx, t, mc, target, "vector")
		} else {
			task, err := mc.RefreshLoad(ctx, client.NewRefreshLoadOption(target))
			require.NoError(t, err)
			require.NoError(t, task.Await(ctx))
		}
		desc, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(target))
		require.NoError(t, err)
		storageCfg := minIOConfigWithServerRootPath(t, cfg)
		segments := waitForExternalSegmentIDs(ctx, t, minioClient, storageCfg, desc.ID, 30*time.Second)
		newSegments := 0
		for _, segment := range segments {
			if seenSegments[segment] {
				continue
			}
			seenSegments[segment] = true
			newSegments++
			objects, _, err := findExternalTextIndexObjects(ctx, minioClient, storageCfg, desc.ID, segment, getFieldIDByName(t, desc.Schema, "text"))
			require.NoError(t, err)
			require.Empty(t, objects, "TEXT_MATCH must use a local index for segment %d", segment)
		}
		require.Positive(t, newSegments)
		check := func(fields []*schemapb.FieldData, rows int, projected bool) {
			t.Helper()
			var ids []int64
			var meta *schemapb.FieldData
			for _, field := range fields {
				if field.GetFieldName() == "id" {
					ids = field.GetScalars().GetLongData().GetData()
				}
				if field.GetIsDynamic() {
					meta = field
				}
			}
			require.Len(t, ids, rows)
			require.NotNil(t, meta)
			valid := typeutil.GetFieldDataValidData(meta)
			require.Len(t, valid, rows)
			values := meta.GetScalars().GetJsonData().GetData()
			valueIndex := 0
			for row, id := range ids {
				require.Equal(t, id%3 != 0, valid[row], "id %d", id)
				if !valid[row] {
					continue
				}
				// Proxy compacts nullable scalar payloads; validity retains logical rows.
				idx := valueIndex
				if len(values) == rows {
					idx = row
				}
				require.Less(t, idx, len(values))
				want := `{"tag":"visible","hidden":true}`
				if projected {
					want = `{"tag":"visible"}`
				}
				require.JSONEq(t, want, string(values[idx]))
				valueIndex++
			}
			require.Equal(t, 2*(round+1), valueIndex)
		}
		for _, output := range []string{"tag", "$meta"} {
			result, err := mc.GetService().Query(ctx, &milvuspb.QueryRequest{CollectionName: target, Expr: `text_match(text, "mandatory")`, OutputFields: []string{"id", output}, QueryParams: []*commonpb.KeyValuePair{{Key: "limit", Value: "10"}}, ConsistencyLevel: commonpb.ConsistencyLevel_Strong})
			require.NoError(t, err)
			require.NoError(t, merr.Error(result.GetStatus()))
			check(result.GetFieldsData(), (round+1)*3, output == "tag")
			placeholder, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: [][]byte{{0, 0, 128, 63, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}}}}})
			require.NoError(t, err)
			searched, err := mc.GetService().Search(ctx, &milvuspb.SearchRequest{CollectionName: target, Dsl: `text_match(text, "mandatory")`, DslType: commonpb.DslType_BoolExprV1, SearchInput: &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: placeholder}, Nq: 1, OutputFields: []string{"id", output}, ConsistencyLevel: commonpb.ConsistencyLevel_Strong, SearchParams: []*commonpb.KeyValuePair{{Key: "anns_field", Value: "vector"}, {Key: "topk", Value: "10"}, {Key: "metric_type", Value: "L2"}, {Key: "params", Value: "{}"}}})
			require.NoError(t, err)
			require.NoError(t, merr.Error(searched.GetStatus()))
			check(searched.GetResults().GetFieldsData(), (round+1)*3, output == "tag")
		}
		filtered, err := mc.Query(ctx, client.NewQueryOption(target).WithFilter(`tag == "visible" && text_match(text, "mandatory")`).WithOutputFields("id"))
		require.NoError(t, err)
		require.Equal(t, 2*(round+1), filtered.ResultCount)
	}
}

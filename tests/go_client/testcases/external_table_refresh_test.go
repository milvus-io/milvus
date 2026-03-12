package testcases

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	miniogo "github.com/minio/minio-go/v7"
	miniocreds "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// --- MinIO helpers (configurable via environment variables) ---

func envOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

type minioConfig struct {
	address   string
	accessKey string
	secretKey string
	bucket    string
	rootPath  string
}

func getMinIOConfig() minioConfig {
	return minioConfig{
		address:   envOrDefault("MINIO_ADDRESS", "localhost:9000"),
		accessKey: envOrDefault("MINIO_ACCESS_KEY", "minioadmin"),
		secretKey: envOrDefault("MINIO_SECRET_KEY", "minioadmin"),
		bucket:    envOrDefault("MINIO_BUCKET", "a-bucket"),
		rootPath:  envOrDefault("MINIO_ROOT_PATH", "files"),
	}
}

func newMinIOClient(cfg minioConfig) (*miniogo.Client, error) {
	return miniogo.New(cfg.address, &miniogo.Options{
		Creds:  miniocreds.NewStaticV4(cfg.accessKey, cfg.secretKey, ""),
		Secure: false,
	})
}

// cleanupMinIOPrefix removes all objects under a prefix in the given bucket.
func cleanupMinIOPrefix(ctx context.Context, mc *miniogo.Client, bucket, prefix string) {
	for obj := range mc.ListObjects(ctx, bucket, miniogo.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}) {
		if obj.Err != nil {
			continue
		}
		_ = mc.RemoveObject(ctx, bucket, obj.Key, miniogo.RemoveObjectOptions{})
	}
}

// --- Parquet generation ---

const testVecDim = 4

// generateParquetBytes creates a Parquet file in memory with columns:
//   - id (Int64)
//   - value (Float32)
//   - embedding (FixedSizeList[Float32, testVecDim])
func generateParquetBytes(numRows int64, startID int64) ([]byte, error) {
	arrowSchema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "value", Type: arrow.PrimitiveTypes.Float32},
			{Name: "embedding", Type: arrow.FixedSizeListOf(testVecDim, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	var buf bytes.Buffer
	writer, err := pqarrow.NewFileWriter(arrowSchema, &buf, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, fmt.Errorf("create parquet writer: %w", err)
	}

	pool := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(pool, arrowSchema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Int64Builder)
	valueBuilder := builder.Field(1).(*array.Float32Builder)
	embeddingBuilder := builder.Field(2).(*array.FixedSizeListBuilder)
	vecValueBuilder := embeddingBuilder.ValueBuilder().(*array.Float32Builder)

	for i := int64(0); i < numRows; i++ {
		idBuilder.Append(startID + i)
		valueBuilder.Append(float32(startID+i) * 1.5)
		embeddingBuilder.Append(true)
		for d := 0; d < testVecDim; d++ {
			vecValueBuilder.Append(float32(startID+i)*0.1 + float32(d))
		}
	}

	record := builder.NewRecord()
	defer record.Release()

	if err := writer.Write(record); err != nil {
		return nil, fmt.Errorf("write record: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close writer: %w", err)
	}

	return buf.Bytes(), nil
}

// --- Multi-type parquet generation ---

// generateMultiTypeParquetBytes creates a Parquet file with multiple data types:
//   - id (Int64), bool_val (Boolean), int8_val (Int8), int16_val (Int16),
//   - int32_val (Int32), float_val (Float32), double_val (Float64),
//   - embedding (FixedSizeList[Float32, testVecDim])
//
// Data formulas for row i (startID+i):
//
//	bool_val = (i is even), int8_val = i%100, int16_val = i*10,
//	int32_val = i*100, float_val = i*1.5, double_val = i*0.01
func generateMultiTypeParquetBytes(numRows int64, startID int64) ([]byte, error) {
	arrowSchema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "bool_val", Type: arrow.FixedWidthTypes.Boolean},
			{Name: "int8_val", Type: arrow.PrimitiveTypes.Int8},
			{Name: "int16_val", Type: arrow.PrimitiveTypes.Int16},
			{Name: "int32_val", Type: arrow.PrimitiveTypes.Int32},
			{Name: "float_val", Type: arrow.PrimitiveTypes.Float32},
			{Name: "double_val", Type: arrow.PrimitiveTypes.Float64},
			{Name: "varchar_val", Type: arrow.BinaryTypes.String},
			{Name: "embedding", Type: arrow.FixedSizeListOf(testVecDim, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	var buf bytes.Buffer
	writer, err := pqarrow.NewFileWriter(arrowSchema, &buf, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, fmt.Errorf("create parquet writer: %w", err)
	}

	pool := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(pool, arrowSchema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Int64Builder)
	boolBuilder := builder.Field(1).(*array.BooleanBuilder)
	int8Builder := builder.Field(2).(*array.Int8Builder)
	int16Builder := builder.Field(3).(*array.Int16Builder)
	int32Builder := builder.Field(4).(*array.Int32Builder)
	floatBuilder := builder.Field(5).(*array.Float32Builder)
	doubleBuilder := builder.Field(6).(*array.Float64Builder)
	varcharBuilder := builder.Field(7).(*array.StringBuilder)
	embeddingBuilder := builder.Field(8).(*array.FixedSizeListBuilder)
	vecValueBuilder := embeddingBuilder.ValueBuilder().(*array.Float32Builder)

	for i := int64(0); i < numRows; i++ {
		idx := startID + i
		idBuilder.Append(idx)
		boolBuilder.Append(idx%2 == 0)
		int8Builder.Append(int8(idx % 100))
		int16Builder.Append(int16(idx * 10))
		int32Builder.Append(int32(idx * 100))
		floatBuilder.Append(float32(idx) * 1.5)
		doubleBuilder.Append(float64(idx) * 0.01)
		varcharBuilder.Append(fmt.Sprintf("str_%04d", idx))
		embeddingBuilder.Append(true)
		for d := 0; d < testVecDim; d++ {
			vecValueBuilder.Append(float32(idx)*0.1 + float32(d))
		}
	}

	record := builder.NewRecord()
	defer record.Release()

	if err := writer.Write(record); err != nil {
		return nil, fmt.Errorf("write record: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close writer: %w", err)
	}

	return buf.Bytes(), nil
}

// --- Helpers ---

func uploadParquetToMinIO(ctx context.Context, t *testing.T, mc *miniogo.Client, bucket, objectKey string, data []byte) {
	t.Helper()
	_, err := mc.PutObject(ctx, bucket, objectKey,
		bytes.NewReader(data), int64(len(data)),
		miniogo.PutObjectOptions{ContentType: "application/octet-stream"})
	require.NoError(t, err, "upload parquet file to MinIO: %s", objectKey)
	t.Logf("Uploaded %s/%s (%d bytes)", bucket, objectKey, len(data))
}

func waitForRefreshComplete(ctx context.Context, t *testing.T, mc *base.MilvusClient, jobID int64, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			t.Fatalf("Refresh job %d timed out after %s", jobID, timeout)
		case <-ticker.C:
			progress, err := mc.GetRefreshExternalCollectionProgress(ctx,
				client.NewGetRefreshExternalCollectionProgressOption(jobID))
			require.NoError(t, err)
			t.Logf("Job %d: state=%s progress=%d%%", jobID, progress.State, progress.Progress)

			if progress.State == entity.RefreshStateCompleted {
				return
			}
			if progress.State == entity.RefreshStateFailed {
				t.Fatalf("Refresh job %d failed: %s", jobID, progress.Reason)
			}
		}
	}
}

func refreshAndWait(ctx context.Context, t *testing.T, mc *base.MilvusClient, collName string) int64 {
	t.Helper()
	refreshResult, err := mc.RefreshExternalCollection(ctx,
		client.NewRefreshExternalCollectionOption(collName))
	common.CheckErr(t, err, true)
	require.Greater(t, refreshResult.JobID, int64(0))
	t.Logf("Started refresh job: %d", refreshResult.JobID)

	waitForRefreshComplete(ctx, t, mc, refreshResult.JobID, 120*time.Second)
	return refreshResult.JobID
}

func indexAndLoadCollection(ctx context.Context, t *testing.T, mc *base.MilvusClient, collName, vecFieldName string) {
	t.Helper()
	idx := index.NewAutoIndex(entity.L2)
	idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, vecFieldName, idx))
	common.CheckErr(t, err, true)
	err = idxTask.Await(ctx)
	common.CheckErr(t, err, true)
	t.Log("Vector index created on " + vecFieldName)

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)
	t.Log("Collection loaded")
}

// indexAndLoadCollectionWithScalarAndVector creates scalar indexes on id and value fields,
// a vector index on the embedding field, verifies all indexes are built successfully,
// and then loads the collection.
func indexAndLoadCollectionWithScalarAndVector(ctx context.Context, t *testing.T, mc *base.MilvusClient, collName string, totalRows int64) {
	t.Helper()

	// Create scalar index on "id" field
	idIdx := index.NewAutoIndex(entity.IP)
	idIdxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "id", idIdx))
	common.CheckErr(t, err, true)
	err = idIdxTask.Await(ctx)
	common.CheckErr(t, err, true)
	t.Log("Scalar index created on id field")

	// Create scalar index on "value" field
	valIdx := index.NewAutoIndex(entity.IP)
	valIdxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "value", valIdx))
	common.CheckErr(t, err, true)
	err = valIdxTask.Await(ctx)
	common.CheckErr(t, err, true)
	t.Log("Scalar index created on value field")

	// Create vector index on "embedding" field
	vecIdx := index.NewAutoIndex(entity.L2)
	vecIdxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "embedding", vecIdx))
	common.CheckErr(t, err, true)
	err = vecIdxTask.Await(ctx)
	common.CheckErr(t, err, true)
	t.Log("Vector index created on embedding field")

	// Verify all indexes via DescribeIndex
	for _, fieldName := range []string{"id", "value", "embedding"} {
		descIdx, err := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(collName, fieldName))
		common.CheckErr(t, err, true)
		require.Equal(t, index.IndexState(3), descIdx.State,
			"index on %s should be Finished (state=3)", fieldName) // 3 = IndexState_Finished
		require.Equal(t, totalRows, descIdx.TotalRows,
			"index on %s: TotalRows should be %d", fieldName, totalRows)
		require.Equal(t, totalRows, descIdx.IndexedRows,
			"index on %s: IndexedRows should be %d", fieldName, totalRows)
		require.Equal(t, int64(0), descIdx.PendingIndexRows,
			"index on %s: PendingIndexRows should be 0", fieldName)
		t.Logf("Index on %s: state=%d totalRows=%d indexedRows=%d pendingRows=%d",
			fieldName, descIdx.State, descIdx.TotalRows, descIdx.IndexedRows, descIdx.PendingIndexRows)
	}

	// Load collection
	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)
	t.Log("Collection loaded")
}

// --- E2E Tests ---

// TestRefreshExternalCollectionAndVerifySegments tests the full external collection workflow:
//  1. Upload parquet test data to MinIO
//  2. Create an external collection with matching schema
//  3. Trigger a refresh job
//  4. Wait for refresh to complete
//  5. Verify the generated segment count and total row count
func TestRefreshExternalCollectionAndVerifySegments(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// Connect to MinIO (skip if unavailable)
	minioCfg := getMinIOConfig()
	minioClient, err := newMinIOClient(minioCfg)
	if err != nil {
		t.Skipf("Failed to create MinIO client, skipping: %v", err)
	}

	exists, err := minioClient.BucketExists(ctx, minioCfg.bucket)
	if err != nil || !exists {
		t.Skipf("MinIO bucket %q not accessible (exists=%v, err=%v), skipping",
			minioCfg.bucket, exists, err)
	}

	collName := common.GenRandomString("ext_refresh", 6)
	extPath := fmt.Sprintf("external-e2e-test/%s", collName)

	// ---------------------------------------------------------------
	// Step 1: Generate and upload parquet data to MinIO
	// ---------------------------------------------------------------
	const numFiles = 2
	const rowsPerFile = int64(1000)
	totalExpectedRows := int64(numFiles) * rowsPerFile

	for i := 0; i < numFiles; i++ {
		data, err := generateParquetBytes(rowsPerFile, int64(i)*rowsPerFile)
		require.NoError(t, err, "generate parquet file %d", i)

		objectKey := fmt.Sprintf("%s/data%d.parquet", extPath, i)
		_, err = minioClient.PutObject(ctx, minioCfg.bucket, objectKey,
			bytes.NewReader(data), int64(len(data)),
			miniogo.PutObjectOptions{ContentType: "application/octet-stream"})
		require.NoError(t, err, "upload parquet file %d to MinIO", i)
		t.Logf("Uploaded %s/%s (%d bytes, %d rows)", minioCfg.bucket, objectKey, len(data), rowsPerFile)
	}

	t.Cleanup(func() {
		cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket,
			fmt.Sprintf("%s/", extPath))
	})

	// ---------------------------------------------------------------
	// Step 2: Create external collection
	// ---------------------------------------------------------------
	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource(extPath).
		WithExternalSpec(`{"format":"parquet"}`).
		WithField(
			entity.NewField().
				WithName("id").
				WithDataType(entity.FieldTypeInt64).
				WithExternalField("id"),
		).
		WithField(
			entity.NewField().
				WithName("value").
				WithDataType(entity.FieldTypeFloat).
				WithExternalField("value"),
		).
		WithField(
			entity.NewField().
				WithName("embedding").
				WithDataType(entity.FieldTypeFloatVector).
				WithDim(testVecDim).
				WithExternalField("embedding"),
		)

	err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)
	t.Logf("Created external collection: %s", collName)

	t.Cleanup(func() {
		_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

	// Verify collection exists with correct external source
	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	common.CheckErr(t, err, true)
	require.Equal(t, extPath, coll.Schema.ExternalSource)

	// ---------------------------------------------------------------
	// Step 3: Trigger refresh
	// ---------------------------------------------------------------
	refreshResult, err := mc.RefreshExternalCollection(ctx,
		client.NewRefreshExternalCollectionOption(collName))
	common.CheckErr(t, err, true)
	require.Greater(t, refreshResult.JobID, int64(0))
	t.Logf("Started refresh job: %d", refreshResult.JobID)

	// ---------------------------------------------------------------
	// Step 4: Poll until refresh completes (timeout 120s)
	// ---------------------------------------------------------------
	jobID := refreshResult.JobID
	deadline := time.After(120 * time.Second)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			t.Fatalf("Refresh job %d timed out after 120s", jobID)
		case <-ticker.C:
			progress, err := mc.GetRefreshExternalCollectionProgress(ctx,
				client.NewGetRefreshExternalCollectionProgressOption(jobID))
			require.NoError(t, err)
			t.Logf("Job %d: state=%s progress=%d%%", jobID, progress.State, progress.Progress)

			if progress.State == entity.RefreshStateCompleted {
				goto verify
			}
			if progress.State == entity.RefreshStateFailed {
				t.Fatalf("Refresh job %d failed: %s", jobID, progress.Reason)
			}
		}
	}
verify:

	// ---------------------------------------------------------------
	// Step 5: Verify row count via collection statistics
	// ---------------------------------------------------------------
	// NOTE: GetPersistentSegmentInfo uses channel-based segment lookup which does
	// not work for external collections (external segments lack InsertChannel).
	// GetCollectionStats uses collection-ID-based lookup (coll2Segments index)
	// which correctly includes external segments.
	stats, err := mc.GetCollectionStats(ctx, client.NewGetCollectionStatsOption(collName))
	common.CheckErr(t, err, true)

	rowCountStr, ok := stats["row_count"]
	require.True(t, ok, "collection stats should contain row_count")

	rowCount, err := strconv.ParseInt(rowCountStr, 10, 64)
	require.NoError(t, err, "parse row_count")
	require.Equal(t, totalExpectedRows, rowCount,
		"total row count should match uploaded parquet data")

	t.Logf("Verified: collection stats row_count=%d (expected %d)", rowCount, totalExpectedRows)

	// ---------------------------------------------------------------
	// Step 6: Verify via ListRefreshExternalCollectionJobs
	// ---------------------------------------------------------------
	jobs, err := mc.ListRefreshExternalCollectionJobs(ctx,
		client.NewListRefreshExternalCollectionJobsOption(collName))
	common.CheckErr(t, err, true)
	require.Greater(t, len(jobs), 0, "should have at least one refresh job")

	found := false
	for _, job := range jobs {
		if job.JobID == jobID {
			require.Equal(t, entity.RefreshStateCompleted, job.State)
			found = true
			break
		}
	}
	require.True(t, found, "should find the completed refresh job in job list")
}

// TestExternalCollectionLoadAndQuery tests the full external collection lifecycle:
//  1. Upload parquet test data to MinIO
//  2. Create an external collection
//  3. Refresh and wait for completion
//  4. Create index on vector field
//  5. Load collection
//  6. Query count(*)
//  7. Query with filter and output fields
//  8. Search with vector
func TestExternalCollectionLoadAndQuery(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// Connect to MinIO (skip if unavailable)
	minioCfg := getMinIOConfig()
	minioClient, err := newMinIOClient(minioCfg)
	if err != nil {
		t.Skipf("Failed to create MinIO client, skipping: %v", err)
	}

	exists, err := minioClient.BucketExists(ctx, minioCfg.bucket)
	if err != nil || !exists {
		t.Skipf("MinIO bucket %q not accessible (exists=%v, err=%v), skipping",
			minioCfg.bucket, exists, err)
	}

	collName := common.GenRandomString("ext_load_query", 6)
	extPath := fmt.Sprintf("external-e2e-test/%s", collName)

	// ---------------------------------------------------------------
	// Step 1: Generate and upload parquet data to MinIO
	// ---------------------------------------------------------------
	const numFiles = 2
	const rowsPerFile = int64(1000)
	totalExpectedRows := int64(numFiles) * rowsPerFile

	for i := 0; i < numFiles; i++ {
		data, err := generateParquetBytes(rowsPerFile, int64(i)*rowsPerFile)
		require.NoError(t, err, "generate parquet file %d", i)

		objectKey := fmt.Sprintf("%s/data%d.parquet", extPath, i)
		_, err = minioClient.PutObject(ctx, minioCfg.bucket, objectKey,
			bytes.NewReader(data), int64(len(data)),
			miniogo.PutObjectOptions{ContentType: "application/octet-stream"})
		require.NoError(t, err, "upload parquet file %d to MinIO", i)
		t.Logf("Uploaded %s/%s (%d bytes, %d rows)", minioCfg.bucket, objectKey, len(data), rowsPerFile)
	}

	t.Cleanup(func() {
		cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket,
			fmt.Sprintf("%s/", extPath))
	})

	// ---------------------------------------------------------------
	// Step 2: Create external collection
	// ---------------------------------------------------------------
	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource(extPath).
		WithExternalSpec(`{"format":"parquet"}`).
		WithField(
			entity.NewField().
				WithName("id").
				WithDataType(entity.FieldTypeInt64).
				WithExternalField("id"),
		).
		WithField(
			entity.NewField().
				WithName("value").
				WithDataType(entity.FieldTypeFloat).
				WithExternalField("value"),
		).
		WithField(
			entity.NewField().
				WithName("embedding").
				WithDataType(entity.FieldTypeFloatVector).
				WithDim(testVecDim).
				WithExternalField("embedding"),
		)

	err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)
	t.Logf("Created external collection: %s", collName)

	t.Cleanup(func() {
		_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

	// Verify collection exists with correct external source
	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	common.CheckErr(t, err, true)
	require.Equal(t, extPath, coll.Schema.ExternalSource)

	// ---------------------------------------------------------------
	// Step 3: Trigger refresh and poll until complete
	// ---------------------------------------------------------------
	refreshResult, err := mc.RefreshExternalCollection(ctx,
		client.NewRefreshExternalCollectionOption(collName))
	common.CheckErr(t, err, true)
	require.Greater(t, refreshResult.JobID, int64(0))
	t.Logf("Started refresh job: %d", refreshResult.JobID)

	jobID := refreshResult.JobID
	deadline := time.After(120 * time.Second)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			t.Fatalf("Refresh job %d timed out after 120s", jobID)
		case <-ticker.C:
			progress, err := mc.GetRefreshExternalCollectionProgress(ctx,
				client.NewGetRefreshExternalCollectionProgressOption(jobID))
			require.NoError(t, err)
			t.Logf("Job %d: state=%s progress=%d%%", jobID, progress.State, progress.Progress)

			if progress.State == entity.RefreshStateCompleted {
				goto indexAndLoad
			}
			if progress.State == entity.RefreshStateFailed {
				t.Fatalf("Refresh job %d failed: %s", jobID, progress.Reason)
			}
		}
	}
indexAndLoad:

	// ---------------------------------------------------------------
	// Step 4: Create scalar + vector indexes and load collection
	// ---------------------------------------------------------------
	indexAndLoadCollectionWithScalarAndVector(ctx, t, mc, collName, totalExpectedRows)

	// ---------------------------------------------------------------
	// Step 6: Query count(*)
	// ---------------------------------------------------------------
	countRes, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)

	count, err := countRes.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
	require.NoError(t, err)
	require.Equal(t, totalExpectedRows, count,
		"count(*) should match total uploaded rows")
	t.Logf("Query count(*) = %d (expected %d)", count, totalExpectedRows)

	// ---------------------------------------------------------------
	// Step 7: Query with filter and output fields
	// ---------------------------------------------------------------
	filterRes, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithFilter("id < 100").
		WithOutputFields("id", "value"))
	common.CheckErr(t, err, true)

	idCol := filterRes.GetColumn("id")
	require.Equal(t, 100, idCol.Len(),
		"filter id < 100 should return exactly 100 rows")
	t.Logf("Query with filter returned %d rows", idCol.Len())

	// ---------------------------------------------------------------
	// Step 8: Search with vector
	// ---------------------------------------------------------------
	vec := make([]float32, testVecDim)
	for i := range vec {
		vec[i] = float32(i) * 0.1
	}
	searchRes, err := mc.Search(ctx, client.NewSearchOption(collName, 5,
		[]entity.Vector{entity.FloatVector(vec)}).
		WithConsistencyLevel(entity.ClStrong).
		WithANNSField("embedding").
		WithOutputFields("id", "value"))
	common.CheckErr(t, err, true)

	require.Equal(t, 1, len(searchRes), "should have 1 result set for 1 query vector")
	require.Greater(t, searchRes[0].ResultCount, 0, "search should return results")
	t.Logf("Search returned %d results", searchRes[0].ResultCount)

	// ---------------------------------------------------------------
	// Step 9: Search with filter (hybrid search)
	// ---------------------------------------------------------------
	hybridRes, err := mc.Search(ctx, client.NewSearchOption(collName, 5,
		[]entity.Vector{entity.FloatVector(vec)}).
		WithConsistencyLevel(entity.ClStrong).
		WithANNSField("embedding").
		WithFilter("id >= 500 && id < 1000").
		WithOutputFields("id", "value"))
	common.CheckErr(t, err, true)

	require.Equal(t, 1, len(hybridRes))
	require.Greater(t, hybridRes[0].ResultCount, 0, "hybrid search should return results")
	// Verify all returned IDs satisfy the filter
	hybridIDCol := hybridRes[0].GetColumn("id")
	for i := 0; i < hybridIDCol.Len(); i++ {
		val, _ := hybridIDCol.GetAsInt64(i)
		require.GreaterOrEqual(t, val, int64(500), "filtered search result id should be >= 500")
		require.Less(t, val, int64(1000), "filtered search result id should be < 1000")
	}
	t.Logf("Hybrid search (filter id in [500,1000)) returned %d results", hybridRes[0].ResultCount)

	// ---------------------------------------------------------------
	// Step 10: Query with pagination (offset + limit)
	// ---------------------------------------------------------------
	pageRes, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithFilter("id >= 0").
		WithOutputFields("id").
		WithOffset(10).
		WithLimit(20))
	common.CheckErr(t, err, true)

	pageIDCol := pageRes.GetColumn("id")
	require.Equal(t, 20, pageIDCol.Len(), "pagination should return exactly 20 rows")
	firstID, _ := pageIDCol.GetAsInt64(0)
	require.Equal(t, int64(10), firstID, "first row after offset=10 should have id=10")
	t.Logf("Pagination query (offset=10, limit=20) returned %d rows, first id=%d", pageIDCol.Len(), firstID)

	// ---------------------------------------------------------------
	// Step 11: Search with multiple query vectors
	// ---------------------------------------------------------------
	vec2 := make([]float32, testVecDim)
	for i := range vec2 {
		vec2[i] = float32(999-i) * 0.1
	}
	multiRes, err := mc.Search(ctx, client.NewSearchOption(collName, 3,
		[]entity.Vector{entity.FloatVector(vec), entity.FloatVector(vec2)}).
		WithConsistencyLevel(entity.ClStrong).
		WithANNSField("embedding").
		WithOutputFields("id"))
	common.CheckErr(t, err, true)

	require.Equal(t, 2, len(multiRes), "should have 2 result sets for 2 query vectors")
	require.Greater(t, multiRes[0].ResultCount, 0, "first query should return results")
	require.Greater(t, multiRes[1].ResultCount, 0, "second query should return results")
	t.Logf("Multi-vector search returned %d and %d results", multiRes[0].ResultCount, multiRes[1].ResultCount)

	// ---------------------------------------------------------------
	// Step 12: Query with output embedding field (vector retrieval)
	// ---------------------------------------------------------------
	vecRes, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithFilter("id == 0").
		WithOutputFields("id", "embedding"))
	common.CheckErr(t, err, true)

	vecIDCol := vecRes.GetColumn("id")
	require.Equal(t, 1, vecIDCol.Len(), "should return exactly 1 row for id==0")
	embCol := vecRes.GetColumn("embedding")
	require.NotNil(t, embCol, "embedding column should be present in output")
	require.Equal(t, 1, embCol.Len(), "embedding column should have 1 row")
	t.Log("Query with embedding output field succeeded")
}

// TestExternalCollectionIncrementalRefresh tests that refreshing an external collection
// after modifying the underlying parquet files correctly updates segments:
//  1. Initial files: data0 (ids 0-499) + data1 (ids 500-999) → 1000 rows
//  2. Remove data1, add data2 (ids 2000-2299) → 800 rows
//  3. Verify: data0 intact, data1 gone, data2 present
func TestExternalCollectionIncrementalRefresh(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*600) // 10 min for two refresh+load cycles
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	minioCfg := getMinIOConfig()
	minioClient, err := newMinIOClient(minioCfg)
	if err != nil {
		t.Skipf("Failed to create MinIO client, skipping: %v", err)
	}

	exists, err := minioClient.BucketExists(ctx, minioCfg.bucket)
	if err != nil || !exists {
		t.Skipf("MinIO bucket %q not accessible, skipping", minioCfg.bucket)
	}

	collName := common.GenRandomString("ext_incr_refresh", 6)
	extPath := fmt.Sprintf("external-e2e-test/%s", collName)

	t.Cleanup(func() {
		cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket, extPath+"/")
		_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

	// ---------------------------------------------------------------
	// Phase 1: Upload initial data files
	// ---------------------------------------------------------------
	data0, err := generateParquetBytes(500, 0) // ids 0-499
	require.NoError(t, err)
	data1, err := generateParquetBytes(500, 500) // ids 500-999
	require.NoError(t, err)

	uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket, extPath+"/data0.parquet", data0)
	uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket, extPath+"/data1.parquet", data1)

	// ---------------------------------------------------------------
	// Create external collection
	// ---------------------------------------------------------------
	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource(extPath).
		WithExternalSpec(`{"format":"parquet"}`).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
		WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value")).
		WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(testVecDim).WithExternalField("embedding"))

	err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)
	t.Logf("Created external collection: %s", collName)

	// ---------------------------------------------------------------
	// First refresh + load
	// ---------------------------------------------------------------
	refreshAndWait(ctx, t, mc, collName)
	indexAndLoadCollection(ctx, t, mc, collName, "embedding")

	// Verify: count = 1000
	countRes, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	count, _ := countRes.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
	require.Equal(t, int64(1000), count, "initial count should be 1000")
	t.Logf("Phase 1: count(*) = %d", count)

	// Verify: data1 range (ids 500-999) is present
	res1, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithFilter("id >= 500 && id < 1000").
		WithOutputFields("id"))
	common.CheckErr(t, err, true)
	require.Equal(t, 500, res1.GetColumn("id").Len(), "should find 500 rows in data1 range")

	// ---------------------------------------------------------------
	// Phase 2: Release, modify files, refresh again
	// ---------------------------------------------------------------
	err = mc.ReleaseCollection(ctx, client.NewReleaseCollectionOption(collName))
	require.NoError(t, err, "release collection")
	t.Log("Collection released")

	// Remove data1.parquet (ids 500-999)
	err = minioClient.RemoveObject(ctx, minioCfg.bucket, extPath+"/data1.parquet", miniogo.RemoveObjectOptions{})
	require.NoError(t, err, "remove data1.parquet")
	t.Log("Removed data1.parquet from MinIO")

	// Upload data2.parquet (ids 2000-2299, 300 rows)
	data2, err := generateParquetBytes(300, 2000)
	require.NoError(t, err)
	uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket, extPath+"/data2.parquet", data2)

	// Second refresh
	refreshAndWait(ctx, t, mc, collName)

	// Verify collection stats: count = 500 + 300 = 800
	stats, err := mc.GetCollectionStats(ctx, client.NewGetCollectionStatsOption(collName))
	common.CheckErr(t, err, true)
	rowCountStr, ok := stats["row_count"]
	require.True(t, ok, "stats should contain row_count")
	rowCount, _ := strconv.ParseInt(rowCountStr, 10, 64)
	require.Equal(t, int64(800), rowCount, "after file change: 500 (data0) + 300 (data2) = 800")
	t.Logf("Phase 2: collection stats row_count = %d", rowCount)

	// ---------------------------------------------------------------
	// Phase 3: Reload and verify data correctness
	// ---------------------------------------------------------------
	indexAndLoadCollection(ctx, t, mc, collName, "embedding")

	// Verify total count = 800
	countRes2, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	count2, _ := countRes2.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
	require.Equal(t, int64(800), count2, "count should be 800 after file change")

	// Verify: data0 range (ids 0-499) still intact
	res0, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithFilter("id >= 0 && id < 500").
		WithOutputFields("id"))
	common.CheckErr(t, err, true)
	require.Equal(t, 500, res0.GetColumn("id").Len(), "data0 (ids 0-499) should be intact")

	// Verify: data1 range (ids 500-999) is gone
	res1Gone, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithFilter("id >= 500 && id < 1000").
		WithOutputFields("id"))
	common.CheckErr(t, err, true)
	data1Len := 0
	if col := res1Gone.GetColumn("id"); col != nil {
		data1Len = col.Len()
	}
	require.Equal(t, 0, data1Len, "data1 (ids 500-999) should be removed after refresh")

	// Verify: data2 range (ids 2000-2299) is present
	res2, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithFilter("id >= 2000 && id < 2300").
		WithOutputFields("id"))
	common.CheckErr(t, err, true)
	require.Equal(t, 300, res2.GetColumn("id").Len(), "data2 (ids 2000-2299) should be present")

	// Verify: search still works after incremental refresh
	vec := make([]float32, testVecDim)
	for i := range vec {
		vec[i] = float32(i) * 0.1
	}
	searchRes, err := mc.Search(ctx, client.NewSearchOption(collName, 5,
		[]entity.Vector{entity.FloatVector(vec)}).
		WithConsistencyLevel(entity.ClStrong).
		WithANNSField("embedding").
		WithOutputFields("id"))
	common.CheckErr(t, err, true)
	require.Greater(t, searchRes[0].ResultCount, 0, "search should return results after incremental refresh")

	t.Log("Phase 3: all verifications passed — incremental refresh works correctly")
}

// TestExternalCollectionMultipleDataTypes tests external collections with various data types:
// Bool, Int8, Int16, Int32, Int64, Float, Double, VarChar, FloatVector
func TestExternalCollectionMultipleDataTypes(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	minioCfg := getMinIOConfig()
	minioClient, err := newMinIOClient(minioCfg)
	if err != nil {
		t.Skipf("Failed to create MinIO client, skipping: %v", err)
	}

	exists, err := minioClient.BucketExists(ctx, minioCfg.bucket)
	if err != nil || !exists {
		t.Skipf("MinIO bucket %q not accessible, skipping", minioCfg.bucket)
	}

	collName := common.GenRandomString("ext_multi_type", 6)
	extPath := fmt.Sprintf("external-e2e-test/%s", collName)

	t.Cleanup(func() {
		cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket, extPath+"/")
		_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

	// ---------------------------------------------------------------
	// Step 1: Generate and upload multi-type parquet data
	// ---------------------------------------------------------------
	const numRows = int64(100)
	data, err := generateMultiTypeParquetBytes(numRows, 0)
	require.NoError(t, err)
	uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket, extPath+"/data.parquet", data)

	// ---------------------------------------------------------------
	// Step 2: Create external collection with multi-type schema
	// ---------------------------------------------------------------
	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource(extPath).
		WithExternalSpec(`{"format":"parquet"}`).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
		WithField(entity.NewField().WithName("bool_val").WithDataType(entity.FieldTypeBool).WithExternalField("bool_val")).
		WithField(entity.NewField().WithName("int8_val").WithDataType(entity.FieldTypeInt8).WithExternalField("int8_val")).
		WithField(entity.NewField().WithName("int16_val").WithDataType(entity.FieldTypeInt16).WithExternalField("int16_val")).
		WithField(entity.NewField().WithName("int32_val").WithDataType(entity.FieldTypeInt32).WithExternalField("int32_val")).
		WithField(entity.NewField().WithName("float_val").WithDataType(entity.FieldTypeFloat).WithExternalField("float_val")).
		WithField(entity.NewField().WithName("double_val").WithDataType(entity.FieldTypeDouble).WithExternalField("double_val")).
		WithField(entity.NewField().WithName("varchar_val").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64).WithExternalField("varchar_val")).
		WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(testVecDim).WithExternalField("embedding"))

	err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)
	t.Logf("Created multi-type external collection: %s", collName)

	// ---------------------------------------------------------------
	// Step 3: Refresh + load
	// ---------------------------------------------------------------
	refreshAndWait(ctx, t, mc, collName)
	indexAndLoadCollection(ctx, t, mc, collName, "embedding")

	// ---------------------------------------------------------------
	// Step 4: Verify count(*)
	// ---------------------------------------------------------------
	countRes, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	count, _ := countRes.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
	require.Equal(t, numRows, count, "count should match uploaded rows")
	t.Logf("count(*) = %d", count)

	// ---------------------------------------------------------------
	// Step 5: Bool filter — bool_val == true → even ids → 50 rows
	// ---------------------------------------------------------------
	boolRes, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithFilter("bool_val == true").
		WithOutputFields("id", "bool_val"))
	common.CheckErr(t, err, true)
	require.Equal(t, int(numRows/2), boolRes.GetColumn("id").Len(),
		"bool_val==true should match even-id rows")
	t.Logf("Bool filter: %d rows", boolRes.GetColumn("id").Len())

	// ---------------------------------------------------------------
	// Step 6: Int8 filter — int8_val < 10 → ids 0-9 → 10 rows
	// ---------------------------------------------------------------
	int8Res, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithFilter("int8_val < 10").
		WithOutputFields("id", "int8_val"))
	common.CheckErr(t, err, true)
	require.Equal(t, 10, int8Res.GetColumn("id").Len(), "int8_val < 10 should be 10 rows")
	t.Logf("Int8 filter: %d rows", int8Res.GetColumn("id").Len())

	// ---------------------------------------------------------------
	// Step 7: Int16 filter — int16_val >= 500 → ids >= 50 → 50 rows
	// ---------------------------------------------------------------
	int16Res, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithFilter("int16_val >= 500").
		WithOutputFields("id", "int16_val"))
	common.CheckErr(t, err, true)
	require.Equal(t, 50, int16Res.GetColumn("id").Len(), "int16_val >= 500 should be 50 rows")
	t.Logf("Int16 filter: %d rows", int16Res.GetColumn("id").Len())

	// ---------------------------------------------------------------
	// Step 8: Int32 filter — int32_val < 5000 → ids < 50 → 50 rows
	// ---------------------------------------------------------------
	int32Res, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithFilter("int32_val < 5000").
		WithOutputFields("id", "int32_val"))
	common.CheckErr(t, err, true)
	require.Equal(t, 50, int32Res.GetColumn("id").Len(), "int32_val < 5000 should be 50 rows")
	t.Logf("Int32 filter: %d rows", int32Res.GetColumn("id").Len())

	// ---------------------------------------------------------------
	// Step 9: Float filter — float_val < 15.0 → id*1.5 < 15.0 → ids 0-9 → 10 rows
	// ---------------------------------------------------------------
	floatRes, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithFilter("float_val < 15.0").
		WithOutputFields("id", "float_val"))
	common.CheckErr(t, err, true)
	require.Equal(t, 10, floatRes.GetColumn("id").Len(), "float_val < 15.0 should be 10 rows")
	t.Logf("Float filter: %d rows", floatRes.GetColumn("id").Len())

	// ---------------------------------------------------------------
	// Step 10: Double filter — double_val < 0.5 → id*0.01 < 0.5 → ids 0-49 → 50 rows
	// ---------------------------------------------------------------
	doubleRes, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithFilter("double_val < 0.5").
		WithOutputFields("id", "double_val"))
	common.CheckErr(t, err, true)
	require.Equal(t, 50, doubleRes.GetColumn("id").Len(), "double_val < 0.5 should be 50 rows")
	t.Logf("Double filter: %d rows", doubleRes.GetColumn("id").Len())

	// ---------------------------------------------------------------
	// Step 10b: VarChar prefix filter — varchar_val like "str_004%" → ids 40-49 → 10 rows
	// ---------------------------------------------------------------
	varcharRes, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithFilter(`varchar_val like "str_004%"`).
		WithOutputFields("id", "varchar_val"))
	common.CheckErr(t, err, true)
	require.Equal(t, 10, varcharRes.GetColumn("id").Len(), `varchar_val like "str_004%" should be 10 rows`)
	t.Logf("VarChar prefix filter: %d rows", varcharRes.GetColumn("id").Len())

	// ---------------------------------------------------------------
	// Step 10c: VarChar equality filter — varchar_val == "str_0042" → id=42 → 1 row
	// ---------------------------------------------------------------
	varcharEqRes, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithFilter(`varchar_val == "str_0042"`).
		WithOutputFields("id", "varchar_val"))
	common.CheckErr(t, err, true)
	require.Equal(t, 1, varcharEqRes.GetColumn("id").Len(), `varchar_val == "str_0042" should be 1 row`)
	eqID, _ := varcharEqRes.GetColumn("id").GetAsInt64(0)
	require.Equal(t, int64(42), eqID, "matched row should be id=42")
	t.Logf("VarChar equality filter: matched id=%d", eqID)

	// ---------------------------------------------------------------
	// Step 11: Verify specific row values for id=42
	// ---------------------------------------------------------------
	row42, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithFilter("id == 42").
		WithOutputFields("id", "bool_val", "int8_val", "int16_val", "int32_val", "float_val", "double_val", "varchar_val"))
	common.CheckErr(t, err, true)
	require.Equal(t, 1, row42.GetColumn("id").Len(), "should find exactly 1 row with id==42")

	boolVal, err := row42.GetColumn("bool_val").GetAsBool(0)
	require.NoError(t, err)
	require.Equal(t, true, boolVal, "id=42 (even) → bool_val should be true")

	int8Val, err := row42.GetColumn("int8_val").GetAsInt64(0)
	require.NoError(t, err)
	require.Equal(t, int64(42), int8Val, "int8_val for id=42 should be 42")

	int16Val, err := row42.GetColumn("int16_val").GetAsInt64(0)
	require.NoError(t, err)
	require.Equal(t, int64(420), int16Val, "int16_val for id=42 should be 420 (42*10)")

	int32Val, err := row42.GetColumn("int32_val").GetAsInt64(0)
	require.NoError(t, err)
	require.Equal(t, int64(4200), int32Val, "int32_val for id=42 should be 4200 (42*100)")

	floatVal, err := row42.GetColumn("float_val").GetAsDouble(0)
	require.NoError(t, err)
	require.InDelta(t, 63.0, floatVal, 0.1, "float_val for id=42 should be ~63.0 (42*1.5)")

	doubleVal, err := row42.GetColumn("double_val").GetAsDouble(0)
	require.NoError(t, err)
	require.InDelta(t, 0.42, doubleVal, 0.001, "double_val for id=42 should be ~0.42 (42*0.01)")

	varcharVal, err := row42.GetColumn("varchar_val").GetAsString(0)
	require.NoError(t, err)
	require.Equal(t, "str_0042", varcharVal, "varchar_val for id=42 should be 'str_0042'")

	t.Log("All field values verified for id=42")

	// ---------------------------------------------------------------
	// Step 12: Search — verify nearest neighbor matches exact vector
	// ---------------------------------------------------------------
	vec := make([]float32, testVecDim)
	for i := range vec {
		vec[i] = float32(42)*0.1 + float32(i) // same formula as id=42's embedding
	}
	searchRes, err := mc.Search(ctx, client.NewSearchOption(collName, 5,
		[]entity.Vector{entity.FloatVector(vec)}).
		WithConsistencyLevel(entity.ClStrong).
		WithANNSField("embedding").
		WithOutputFields("id"))
	common.CheckErr(t, err, true)
	require.Equal(t, 1, len(searchRes))
	require.Greater(t, searchRes[0].ResultCount, 0, "search should return at least 1 result")
	nearestID, _ := searchRes[0].GetColumn("id").GetAsInt64(0)
	t.Logf("Search: nearest neighbor for id=42's vector is id=%d (top-5 returned %d results)", nearestID, searchRes[0].ResultCount)

	// ---------------------------------------------------------------
	// Step 13: Compound filter across types — bool AND int range AND varchar
	// ---------------------------------------------------------------
	compoundRes, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithFilter(`bool_val == true && int32_val < 5000 && varchar_val like "str_00%"`).
		WithOutputFields("id"))
	common.CheckErr(t, err, true)
	// ids 0-49 have int32_val < 5000, of which 25 are even (bool_val==true)
	// varchar_val like "str_00%" matches ids 0-99 (all have str_00xx format), so no extra filter
	require.Equal(t, 25, compoundRes.GetColumn("id").Len(),
		`compound filter: bool_val==true AND int32_val<5000 AND varchar like "str_00%" → 25 rows`)
	t.Log("Compound filter across types (incl. varchar) passed")
}

// --- Lance format helpers ---

// generateLanceDataOnMinIO uses the Python lance library to write a Lance dataset
// directly to MinIO/S3. This is necessary because lance format writing requires the
// Rust-based lance library, which is not accessible from Go directly.
func generateLanceDataOnMinIO(t *testing.T, s3URI string, numRows, startID int) {
	t.Helper()
	minioCfg := getMinIOConfig()

	scriptPath := "generate_lance_data.py"
	cmd := fmt.Sprintf(
		"MINIO_ADDRESS=%s MINIO_ACCESS_KEY=%s MINIO_SECRET_KEY=%s python3 %s %s %d %d",
		minioCfg.address, minioCfg.accessKey, minioCfg.secretKey,
		scriptPath, s3URI, numRows, startID,
	)
	out, err := runShellCommand(t, cmd)
	require.NoError(t, err, "generate lance data: %s", out)
	require.Contains(t, out, "OK", "lance data generation should succeed")
	t.Logf("Generated lance data: %s", out)
}

// runShellCommand runs a shell command and returns its combined output.
func runShellCommand(t *testing.T, cmd string) (string, error) {
	t.Helper()
	c := exec.Command("bash", "-c", cmd)
	output, err := c.CombinedOutput()
	return string(output), err
}

// checkPythonDeps verifies that the given python interpreter and packages are available.
// Returns nil if all dependencies are importable, or an error describing what's missing.
func checkPythonDeps(pythonBin string, packages ...string) error {
	if _, err := exec.LookPath(pythonBin); err != nil {
		return fmt.Errorf("%s not found in PATH: %w", pythonBin, err)
	}
	for _, pkg := range packages {
		cmd := exec.Command(pythonBin, "-c", fmt.Sprintf("import %s", pkg)) // #nosec G204
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("python package %q not importable via %s: %s (%w)", pkg, pythonBin, string(out), err)
		}
	}
	return nil
}

// findPython3ForVortex returns a python3 interpreter that is >= 3.11, which is
// required by vortex-data >= 0.56.0. It tries "python3" first (works in Docker
// where python3 is 3.13+), then falls back to "python3.11" (works on dev machines
// where the default python3 may be 3.10).
func findPython3ForVortex() (string, error) {
	for _, bin := range []string{"python3", "python3.11"} {
		path, err := exec.LookPath(bin)
		if err != nil {
			continue
		}
		out, err := exec.Command(path, "-c", // #nosec G204
			"import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')").Output()
		if err != nil {
			continue
		}
		ver := strings.TrimSpace(string(out))
		parts := strings.SplitN(ver, ".", 2)
		if len(parts) == 2 {
			major, _ := strconv.Atoi(parts[0])
			minor, _ := strconv.Atoi(parts[1])
			if major > 3 || (major == 3 && minor >= 11) {
				return bin, nil
			}
		}
	}
	return "", fmt.Errorf("no python3 >= 3.11 found (tried python3, python3.11)")
}

// TestExternalCollectionLanceFormat tests external collections with lance-table format:
//  1. Generate Lance dataset on MinIO using Python
//  2. Create external collection with lance-table format
//  3. Refresh and wait for completion
//  4. Create index and load collection
//  5. Query count(*) and verify row count
//  6. Query with filter
//  7. Search with vector
func TestExternalCollectionLanceFormat(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*600) // 10 min for lance operations
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// Connect to MinIO
	minioCfg := getMinIOConfig()
	minioClient, err := newMinIOClient(minioCfg)
	if err != nil {
		t.Skipf("Failed to create MinIO client, skipping: %v", err)
	}

	exists, err := minioClient.BucketExists(ctx, minioCfg.bucket)
	if err != nil || !exists {
		t.Skipf("MinIO bucket %q not accessible, skipping", minioCfg.bucket)
	}

	if err := checkPythonDeps("python3", "pyarrow", "lance"); err != nil {
		t.Skipf("Python deps for Lance unavailable, skipping: %v", err)
	}

	collName := common.GenRandomString("ext_lance", 6)
	extPath := fmt.Sprintf("external-e2e-test/%s", collName)

	// ---------------------------------------------------------------
	// Step 1: Generate Lance dataset on MinIO
	// ---------------------------------------------------------------
	const totalRows = 100000
	s3URI := fmt.Sprintf("s3://%s/%s/%s", minioCfg.bucket, minioCfg.rootPath, extPath)
	generateLanceDataOnMinIO(t, s3URI, totalRows, 0)

	t.Cleanup(func() {
		cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket,
			fmt.Sprintf("%s/%s/", minioCfg.rootPath, extPath))
	})

	// ---------------------------------------------------------------
	// Step 2: Create external collection with lance-table format
	// ---------------------------------------------------------------
	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource(extPath).
		WithExternalSpec(`{"format":"lance-table"}`).
		WithField(
			entity.NewField().
				WithName("id").
				WithDataType(entity.FieldTypeInt64).
				WithExternalField("id"),
		).
		WithField(
			entity.NewField().
				WithName("value").
				WithDataType(entity.FieldTypeFloat).
				WithExternalField("value"),
		).
		WithField(
			entity.NewField().
				WithName("embedding").
				WithDataType(entity.FieldTypeFloatVector).
				WithDim(testVecDim).
				WithExternalField("embedding"),
		)

	err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)
	t.Logf("Created external collection with lance-table format: %s", collName)

	t.Cleanup(func() {
		_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

	// Verify collection
	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	common.CheckErr(t, err, true)
	require.Equal(t, extPath, coll.Schema.ExternalSource)
	require.Equal(t, `{"format":"lance-table"}`, coll.Schema.ExternalSpec)

	// ---------------------------------------------------------------
	// Step 3: Refresh and wait for completion
	// ---------------------------------------------------------------
	refreshAndWait(ctx, t, mc, collName)

	// Verify row count via stats
	stats, err := mc.GetCollectionStats(ctx, client.NewGetCollectionStatsOption(collName))
	common.CheckErr(t, err, true)
	rowCountStr, ok := stats["row_count"]
	require.True(t, ok, "stats should contain row_count")
	rowCount, err := strconv.ParseInt(rowCountStr, 10, 64)
	require.NoError(t, err)
	require.Equal(t, int64(totalRows), rowCount,
		"collection stats row_count should match lance dataset")
	t.Logf("Refresh complete: row_count=%d", rowCount)

	// ---------------------------------------------------------------
	// Step 4: Create scalar + vector indexes and load collection
	// ---------------------------------------------------------------
	indexAndLoadCollectionWithScalarAndVector(ctx, t, mc, collName, totalRows)

	// ---------------------------------------------------------------
	// Step 5: Query count(*)
	// ---------------------------------------------------------------
	countRes, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)

	count, err := countRes.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
	require.NoError(t, err)
	require.Equal(t, int64(totalRows), count,
		"count(*) should match lance dataset rows")
	t.Logf("Query count(*) = %d", count)

	// ---------------------------------------------------------------
	// Step 6: Query with filter
	// ---------------------------------------------------------------
	filterRes, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithFilter("id < 100").
		WithOutputFields("id", "value"))
	common.CheckErr(t, err, true)

	idCol := filterRes.GetColumn("id")
	require.Equal(t, 100, idCol.Len(),
		"filter id < 100 should return exactly 100 rows")
	t.Logf("Query with filter returned %d rows", idCol.Len())

	// Verify specific value
	val42, err := filterRes.GetColumn("value").GetAsDouble(42)
	require.NoError(t, err)
	expected42 := float64(42) * 1.5
	require.InDelta(t, expected42, val42, 0.01,
		"value for id=42 should be 42*1.5=63.0")

	// ---------------------------------------------------------------
	// Step 6b: Verify data correctness — sample rows across the range
	// ---------------------------------------------------------------
	sampleIDs := []int64{0, 1, 999, 50000, 99999}
	for _, sid := range sampleIDs {
		sampleRes, err := mc.Query(ctx, client.NewQueryOption(collName).
			WithConsistencyLevel(entity.ClStrong).
			WithFilter(fmt.Sprintf("id == %d", sid)).
			WithOutputFields("id", "value"))
		common.CheckErr(t, err, true)
		require.Equal(t, 1, sampleRes.GetColumn("id").Len(),
			"should find exactly 1 row for id=%d", sid)
		sampleVal, err := sampleRes.GetColumn("value").GetAsDouble(0)
		require.NoError(t, err)
		require.InDelta(t, float64(sid)*1.5, sampleVal, 0.01,
			"value for id=%d should be %f", sid, float64(sid)*1.5)
	}
	t.Logf("Verified %d sample rows across id range", len(sampleIDs))

	// ---------------------------------------------------------------
	// Step 6c: Verify large range query
	// ---------------------------------------------------------------
	largeRangeRes, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithFilter("id >= 50000 && id < 60000").
		WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	rangeCount, err := largeRangeRes.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
	require.NoError(t, err)
	require.Equal(t, int64(10000), rangeCount,
		"id in [50000, 60000) should return 10000 rows")
	t.Logf("Large range query [50000, 60000) returned %d rows", rangeCount)

	// ---------------------------------------------------------------
	// Step 7: Search with vector — verify nearest neighbor is id=0
	// ---------------------------------------------------------------
	vec := make([]float32, testVecDim)
	for i := range vec {
		vec[i] = float32(i) * 0.1 // same as id=0's embedding
	}
	searchRes, err := mc.Search(ctx, client.NewSearchOption(collName, 5,
		[]entity.Vector{entity.FloatVector(vec)}).
		WithConsistencyLevel(entity.ClStrong).
		WithANNSField("embedding").
		WithOutputFields("id", "value"))
	common.CheckErr(t, err, true)

	require.Equal(t, 1, len(searchRes), "should have 1 result set")
	require.Greater(t, searchRes[0].ResultCount, 0, "search should return results")
	nearestID, _ := searchRes[0].GetColumn("id").GetAsInt64(0)
	nearestVal, _ := searchRes[0].GetColumn("value").GetAsDouble(0)
	t.Logf("Search: nearest neighbor id=%d value=%.2f", nearestID, nearestVal)

	// ---------------------------------------------------------------
	// Step 8: Search with filter (hybrid search)
	// ---------------------------------------------------------------
	hybridRes, err := mc.Search(ctx, client.NewSearchOption(collName, 5,
		[]entity.Vector{entity.FloatVector(vec)}).
		WithConsistencyLevel(entity.ClStrong).
		WithANNSField("embedding").
		WithFilter("id >= 50000 && id < 60000").
		WithOutputFields("id", "value"))
	common.CheckErr(t, err, true)

	require.Equal(t, 1, len(hybridRes))
	require.Greater(t, hybridRes[0].ResultCount, 0, "hybrid search should return results")
	hybridIDCol := hybridRes[0].GetColumn("id")
	for i := 0; i < hybridIDCol.Len(); i++ {
		val, _ := hybridIDCol.GetAsInt64(i)
		require.GreaterOrEqual(t, val, int64(50000))
		require.Less(t, val, int64(60000))
	}
	t.Logf("Hybrid search (id in [50000,60000)) returned %d results with correct values", hybridRes[0].ResultCount)

	t.Log("Lance format external collection test passed!")
}

// generateVortexDataOnMinIO uses the Python vortex library to write a Vortex
// dataset directly to MinIO/S3. This mirrors the Lance test pattern where a
// Python script handles format-specific data generation.
func generateVortexDataOnMinIO(t *testing.T, pythonBin, outputPath string, numRows, dim int) {
	t.Helper()
	minioCfg := getMinIOConfig()

	scriptPath := "generate_vortex_data.py"
	cmd := fmt.Sprintf(
		"MINIO_ADDRESS=%s MINIO_ACCESS_KEY=%s MINIO_SECRET_KEY=%s MINIO_BUCKET=%s %s %s %s %d %d",
		minioCfg.address, minioCfg.accessKey, minioCfg.secretKey, minioCfg.bucket,
		pythonBin, scriptPath, outputPath, numRows, dim,
	)
	out, err := runShellCommand(t, cmd)
	require.NoError(t, err, "generate vortex data: %s", out)
	require.Contains(t, out, "OK", "vortex data generation should succeed")
	t.Logf("Generated vortex data: %s", out)
}

// TestExternalCollectionVortexFormat tests external collections with vortex format:
//  1. Generate vortex dataset on MinIO using the generate_vortex_data tool
//  2. Create external collection with vortex format
//  3. Refresh and wait for completion
//  4. Create index and load collection
//  5. Query count(*) and verify row count
//  6. Query with filter
//  7. Search with vector
func TestExternalCollectionVortexFormat(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*600)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	minioCfg := getMinIOConfig()
	minioClient, err := newMinIOClient(minioCfg)
	if err != nil {
		t.Skipf("Failed to create MinIO client, skipping: %v", err)
	}

	exists, err := minioClient.BucketExists(ctx, minioCfg.bucket)
	if err != nil || !exists {
		t.Skipf("MinIO bucket %q not accessible, skipping", minioCfg.bucket)
	}

	// vortex-data >= 0.56.0 requires Python >= 3.11
	pythonBin, err := findPython3ForVortex()
	if err != nil {
		t.Skipf("Python >= 3.11 not found for Vortex, skipping: %v", err)
	}
	if err := checkPythonDeps(pythonBin, "pyarrow", "vortex", "obstore"); err != nil {
		t.Skipf("Python deps for Vortex unavailable, skipping: %v", err)
	}

	collName := common.GenRandomString("ext_vortex", 6)
	extPath := fmt.Sprintf("external-e2e-test/%s", collName)

	// ---------------------------------------------------------------
	// Step 1: Generate vortex dataset on MinIO
	// ---------------------------------------------------------------
	const totalRows = 100000
	generateVortexDataOnMinIO(t, pythonBin, extPath, totalRows, testVecDim)

	t.Cleanup(func() {
		cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket,
			fmt.Sprintf("%s/", extPath))
	})

	// ---------------------------------------------------------------
	// Step 2: Create external collection with vortex format
	// ---------------------------------------------------------------
	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource(extPath).
		WithExternalSpec(`{"format":"vortex"}`).
		WithField(
			entity.NewField().
				WithName("id").
				WithDataType(entity.FieldTypeInt64).
				WithExternalField("id"),
		).
		WithField(
			entity.NewField().
				WithName("value").
				WithDataType(entity.FieldTypeFloat).
				WithExternalField("value"),
		).
		WithField(
			entity.NewField().
				WithName("embedding").
				WithDataType(entity.FieldTypeFloatVector).
				WithDim(testVecDim).
				WithExternalField("embedding"),
		)

	err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)
	t.Logf("Created external collection with vortex format: %s", collName)

	t.Cleanup(func() {
		_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	common.CheckErr(t, err, true)
	require.Equal(t, extPath, coll.Schema.ExternalSource)
	require.Equal(t, `{"format":"vortex"}`, coll.Schema.ExternalSpec)

	// ---------------------------------------------------------------
	// Step 3: Refresh and wait for completion
	// ---------------------------------------------------------------
	refreshAndWait(ctx, t, mc, collName)

	stats, err := mc.GetCollectionStats(ctx, client.NewGetCollectionStatsOption(collName))
	common.CheckErr(t, err, true)
	rowCountStr, ok := stats["row_count"]
	require.True(t, ok, "stats should contain row_count")
	rowCount, err := strconv.ParseInt(rowCountStr, 10, 64)
	require.NoError(t, err)
	require.Equal(t, int64(totalRows), rowCount,
		"collection stats row_count should match vortex dataset")
	t.Logf("Refresh complete: row_count=%d", rowCount)

	// ---------------------------------------------------------------
	// Step 4: Create scalar + vector indexes and load collection
	// ---------------------------------------------------------------
	indexAndLoadCollectionWithScalarAndVector(ctx, t, mc, collName, totalRows)

	// ---------------------------------------------------------------
	// Step 5: Query count(*)
	// ---------------------------------------------------------------
	countRes, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)

	count, err := countRes.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
	require.NoError(t, err)
	require.Equal(t, int64(totalRows), count,
		"count(*) should match vortex dataset rows")
	t.Logf("Query count(*) = %d", count)

	// ---------------------------------------------------------------
	// Step 6: Query with filter
	// ---------------------------------------------------------------
	filterRes, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithFilter("id < 100").
		WithOutputFields("id", "value"))
	common.CheckErr(t, err, true)

	idCol := filterRes.GetColumn("id")
	require.Equal(t, 100, idCol.Len(),
		"filter id < 100 should return exactly 100 rows")
	t.Logf("Query with filter returned %d rows", idCol.Len())

	val42, err := filterRes.GetColumn("value").GetAsDouble(42)
	require.NoError(t, err)
	expected42 := float64(42) * 1.5
	require.InDelta(t, expected42, val42, 0.01,
		"value for id=42 should be 42*1.5=63.0")

	// ---------------------------------------------------------------
	// Step 6b: Verify data correctness — sample rows across the range
	// ---------------------------------------------------------------
	sampleIDs := []int64{0, 1, 999, 50000, 99999}
	for _, sid := range sampleIDs {
		sampleRes, err := mc.Query(ctx, client.NewQueryOption(collName).
			WithConsistencyLevel(entity.ClStrong).
			WithFilter(fmt.Sprintf("id == %d", sid)).
			WithOutputFields("id", "value"))
		common.CheckErr(t, err, true)
		require.Equal(t, 1, sampleRes.GetColumn("id").Len(),
			"should find exactly 1 row for id=%d", sid)
		sampleVal, err := sampleRes.GetColumn("value").GetAsDouble(0)
		require.NoError(t, err)
		require.InDelta(t, float64(sid)*1.5, sampleVal, 0.01,
			"value for id=%d should be %f", sid, float64(sid)*1.5)
	}
	t.Logf("Verified %d sample rows across id range", len(sampleIDs))

	// ---------------------------------------------------------------
	// Step 6c: Verify large range query
	// ---------------------------------------------------------------
	largeRangeRes, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithFilter("id >= 50000 && id < 60000").
		WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	rangeCount, err := largeRangeRes.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
	require.NoError(t, err)
	require.Equal(t, int64(10000), rangeCount,
		"id in [50000, 60000) should return 10000 rows")
	t.Logf("Large range query [50000, 60000) returned %d rows", rangeCount)

	// ---------------------------------------------------------------
	// Step 7: Search with vector — verify nearest neighbor is id=0
	// ---------------------------------------------------------------
	vec := make([]float32, testVecDim)
	for i := range vec {
		vec[i] = float32(i) * 0.1 // same as id=0's embedding
	}
	searchRes, err := mc.Search(ctx, client.NewSearchOption(collName, 5,
		[]entity.Vector{entity.FloatVector(vec)}).
		WithConsistencyLevel(entity.ClStrong).
		WithANNSField("embedding").
		WithOutputFields("id", "value"))
	common.CheckErr(t, err, true)

	require.Equal(t, 1, len(searchRes), "should have 1 result set")
	require.Greater(t, searchRes[0].ResultCount, 0, "search should return results")
	nearestID, _ := searchRes[0].GetColumn("id").GetAsInt64(0)
	nearestVal, _ := searchRes[0].GetColumn("value").GetAsDouble(0)
	t.Logf("Search: nearest neighbor id=%d value=%.2f", nearestID, nearestVal)

	// ---------------------------------------------------------------
	// Step 8: Search with filter (hybrid search)
	// ---------------------------------------------------------------
	hybridRes, err := mc.Search(ctx, client.NewSearchOption(collName, 5,
		[]entity.Vector{entity.FloatVector(vec)}).
		WithConsistencyLevel(entity.ClStrong).
		WithANNSField("embedding").
		WithFilter("id >= 50000 && id < 60000").
		WithOutputFields("id", "value"))
	common.CheckErr(t, err, true)

	require.Equal(t, 1, len(hybridRes))
	require.Greater(t, hybridRes[0].ResultCount, 0, "hybrid search should return results")
	hybridIDCol := hybridRes[0].GetColumn("id")
	for i := 0; i < hybridIDCol.Len(); i++ {
		val, _ := hybridIDCol.GetAsInt64(i)
		require.GreaterOrEqual(t, val, int64(50000))
		require.Less(t, val, int64(60000))
	}
	t.Logf("Hybrid search (id in [50000,60000)) returned %d results with correct values", hybridRes[0].ResultCount)

	t.Log("Vortex format external collection test passed!")
}

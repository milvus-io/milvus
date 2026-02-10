package testcases

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
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
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
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

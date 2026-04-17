package testcases

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// icebergTableInfo holds the output from the Python Iceberg table creator.
type icebergTableInfo struct {
	TableLocation    string `json:"table_location"`
	MetadataLocation string `json:"metadata_location"`
	SnapshotID       int64  `json:"snapshot_id"`
	NumRows          int    `json:"num_rows"`
	Dim              int    `json:"dim"`
}

// toMilvusURI converts an Iceberg-native URI (s3://bucket/key) to Milvus format (s3://host/bucket/key).
func toMilvusURI(icebergURI, host string) string {
	u, err := url.Parse(icebergURI)
	if err != nil || u.Scheme == "" {
		return icebergURI
	}
	// s3://bucket/key → bucket = u.Host, key = u.Path
	return fmt.Sprintf("%s://%s/%s%s", u.Scheme, host, u.Host, u.Path)
}

// TestExternalTableIcebergE2E tests the full Iceberg external table pipeline:
//
//	Create Iceberg table on MinIO → CreateCollection (format=iceberg-table) →
//	Refresh (with snapshot_id) → Load → Search → Query → Drop.
//
// The externalSource uses Milvus standard URI format (s3://host/bucket/path/metadata.json),
// same as parquet and lance external tables. MinIO connection details are passed via extfs.
//
// Run:
//
//	go test -v -run TestExternalTableIcebergE2E -timeout 30m -tags dynamic,test
func TestExternalTableIcebergE2E(t *testing.T) {
	if err := checkPythonDeps("python3", "pyarrow", "pyiceberg"); err != nil {
		t.Skipf("Python deps for Iceberg unavailable, skipping: %v", err)
	}

	minioEndpoint := icebergEnvOrDefault("ICEBERG_MINIO_ENDPOINT", "http://localhost:9000")
	minioAccessKey := icebergEnvOrDefault("ICEBERG_MINIO_ACCESS_KEY", "minioadmin")
	minioSecretKey := icebergEnvOrDefault("ICEBERG_MINIO_SECRET_KEY", "minioadmin")
	bucket := icebergEnvOrDefault("ICEBERG_MINIO_BUCKET", "a-bucket")
	tablePath := icebergEnvOrDefault("ICEBERG_TABLE_PATH", "iceberg-test/e2e_test_table")
	numRows := icebergEnvOrDefault("ICEBERG_NUM_ROWS", "1000")
	dim := icebergEnvOrDefault("ICEBERG_DIM", "128")

	// --- Phase 0: Create Iceberg table on MinIO using Python script ---
	t.Log("[Phase 0] Creating Iceberg test table on MinIO...")
	tableInfo := createIcebergTestTable(t, minioEndpoint, minioAccessKey, minioSecretKey, bucket, tablePath, numRows, dim)
	t.Logf("[Phase 0] Iceberg table created: metadata=%s, snapshot_id=%d, rows=%d",
		tableInfo.MetadataLocation, tableInfo.SnapshotID, tableInfo.NumRows)

	// Convert Iceberg-native URI (s3://bucket/key) to Milvus format (s3://host/bucket/key)
	// to match the URI convention used by parquet/lance external tables.
	minioHost := strings.TrimPrefix(strings.TrimPrefix(minioEndpoint, "http://"), "https://")
	externalSource := toMilvusURI(tableInfo.MetadataLocation, minioHost)

	// Build ExternalSpec with extfs for MinIO access (same pattern as cross-bucket parquet E2E)
	type externalSpecJSON struct {
		Format     string            `json:"format"`
		SnapshotID int64             `json:"snapshot_id"`
		Extfs      map[string]string `json:"extfs,omitempty"`
	}
	specObj := externalSpecJSON{
		Format:     "iceberg-table",
		SnapshotID: tableInfo.SnapshotID,
		Extfs: map[string]string{
			"region":           "us-east-1",
			"use_ssl":          "false",
			"use_virtual_host": "false",
			"cloud_provider":   "aws",
			"access_key_id":    minioAccessKey,
			"access_key_value": minioSecretKey,
		},
	}
	specBytes, err := json.Marshal(specObj)
	require.NoError(t, err)
	externalSpec := string(specBytes)

	t.Logf("=== Iceberg E2E Test ===")
	t.Logf("External Source: %s", externalSource)
	t.Logf("External Spec:  %s", externalSpec)

	// --- Phase 1: Create external collection ---
	ctx := hp.CreateContext(t, 30*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := fmt.Sprintf("iceberg_e2e_%d", time.Now().UnixMilli())
	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource(externalSource).
		WithExternalSpec(fmt.Sprintf(`{"format":"iceberg-table","snapshot_id":%d}`, tableInfo.SnapshotID)).
		WithField(entity.NewField().WithName("pk").WithDataType(entity.FieldTypeInt64).WithExternalField("pk")).
		WithField(entity.NewField().WithName("label").WithDataType(entity.FieldTypeVarChar).WithMaxLength(256).WithExternalField("label")).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(int64(tableInfo.Dim)).WithExternalField("vector"))

	t.Log("[Phase 1] Creating external collection...")
	err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)
	t.Logf("[Phase 1] Created collection: %s", collName)
	defer func() {
		t.Log("[Cleanup] Dropping collection...")
		_ = mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
	}()

	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	require.NoError(t, err)
	t.Logf("[Phase 1] Collection has %d fields, externalSource=%s", len(coll.Schema.Fields), coll.Schema.ExternalSource)

	// --- Phase 2: Refresh ---
	t.Log("[Phase 2] Triggering refresh...")
	refreshStart := time.Now()
	refreshResult, err := mc.RefreshExternalCollection(ctx,
		client.NewRefreshExternalCollectionOption(collName).
			WithExternalSpec(externalSpec))
	common.CheckErr(t, err, true)
	jobID := refreshResult.JobID
	t.Logf("[Phase 2] Refresh triggered, jobID=%d", jobID)

	deadline := time.After(10 * time.Minute)
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			t.Fatalf("[Phase 2] Refresh timed out after %s", time.Since(refreshStart))
		case <-ticker.C:
			progress, err := mc.GetRefreshExternalCollectionProgress(ctx,
				client.NewGetRefreshExternalCollectionProgressOption(jobID))
			require.NoError(t, err)
			elapsed := time.Since(refreshStart)
			t.Logf("[Phase 2] Job %d: state=%s, elapsed=%s", jobID, progress.State, elapsed.Round(time.Second))
			if progress.State == entity.RefreshStateCompleted {
				t.Logf("[Phase 2] Refresh completed in %s", elapsed)
				goto refreshDone
			}
			if progress.State == entity.RefreshStateFailed {
				t.Fatalf("[Phase 2] Refresh FAILED after %s: %s", elapsed, progress.Reason)
			}
		}
	}
refreshDone:

	// --- Phase 3: Create index + Load ---
	t.Log("[Phase 3] Creating index on vector field...")
	idxTask, err := mc.CreateIndex(ctx,
		client.NewCreateIndexOption(collName, "vector", index.NewFlatIndex(entity.COSINE)))
	require.NoError(t, err)
	err = idxTask.Await(ctx)
	require.NoError(t, err)

	t.Log("[Phase 3] Loading collection...")
	loadStart := time.Now()
	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	require.NoError(t, err)
	err = loadTask.Await(ctx)
	require.NoError(t, err)
	t.Logf("[Phase 3] Collection loaded in %s", time.Since(loadStart))

	// --- Phase 4: Search ---
	t.Log("[Phase 4] Searching...")
	queryVec := make([]float32, tableInfo.Dim)
	for i := range queryVec {
		queryVec[i] = 0.1
	}
	searchResult, err := mc.Search(ctx,
		client.NewSearchOption(collName, 10, []entity.Vector{entity.FloatVector(queryVec)}).
			WithOutputFields("pk", "label"))
	require.NoError(t, err)
	require.NotEmpty(t, searchResult)
	require.Greater(t, searchResult[0].ResultCount, 0)
	t.Logf("[Phase 4] Search returned %d results", searchResult[0].ResultCount)

	// --- Phase 5: Query ---
	t.Log("[Phase 5] Querying...")
	queryResult, err := mc.Query(ctx,
		client.NewQueryOption(collName).
			WithFilter("pk < 10").
			WithOutputFields("pk", "label"))
	require.NoError(t, err)
	require.NotNil(t, queryResult)
	t.Logf("[Phase 5] Query returned %d rows", queryResult.GetColumn("pk").Len())

	t.Log("=== Iceberg E2E Test PASSED ===")
}

// createIcebergTestTable runs the Python script to create an Iceberg table on MinIO.
func createIcebergTestTable(t *testing.T, endpoint, accessKey, secretKey, bucket, tablePath, numRows, dim string) icebergTableInfo {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok, "failed to get caller info")
	scriptPath := filepath.Join(filepath.Dir(thisFile), "testdata", "create_iceberg_table.py")
	infoPath := filepath.Join(t.TempDir(), "iceberg_table_info.json")

	cmd := exec.Command("python3", scriptPath, // #nosec G204
		"--endpoint", endpoint,
		"--access-key", accessKey,
		"--secret-key", secretKey,
		"--bucket", bucket,
		"--table-path", tablePath,
		"--num-rows", numRows,
		"--dim", dim,
		"--output", infoPath,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	require.NoError(t, err, "failed to create Iceberg test table via Python script")

	data, err := os.ReadFile(infoPath)
	require.NoError(t, err, "failed to read iceberg_table_info.json")

	var info icebergTableInfo
	err = json.Unmarshal(data, &info)
	require.NoError(t, err, "failed to parse iceberg_table_info.json")

	return info
}

func icebergEnvOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

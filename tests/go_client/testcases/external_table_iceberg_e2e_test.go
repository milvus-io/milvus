package testcases

import (
	"context"
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

// toMilvusS3URIForMinIO converts an Iceberg-native URI (s3://bucket/key) to
// Milvus form for a self-hosted S3-compatible endpoint:
// s3://host/bucket/key.
func toMilvusS3URIForMinIO(icebergURI, host string) string {
	u, err := url.Parse(icebergURI)
	if err != nil || u.Scheme == "" {
		return icebergURI
	}
	// s3://bucket/key -> bucket = u.Host, key = u.Path
	return fmt.Sprintf("s3://%s/%s%s", host, u.Host, u.Path)
}

// TestExternalTableIcebergE2E tests the full Iceberg external table pipeline:
//
//	Create Iceberg table on MinIO → CreateCollection (format=iceberg-table) →
//	Refresh (with snapshot_id) → Load → Search → Query → Drop.
//
// The externalSource uses s3://host/bucket/path/metadata.json. The scheme is
// accepted by Iceberg FileIO, while extfs.cloud_provider=minio keeps Milvus in
// self-hosted S3-compatible mode.
//
// Run:
//
//	go test -v -run TestExternalTableIcebergE2E -timeout 30m -tags dynamic,test
func TestExternalTableIcebergE2E(t *testing.T) {
	// Derive the default Iceberg MinIO endpoint from MINIO_ADDRESS (shared env var)
	// so that all external table tests use a single address knob.
	minioAddr := envOrDefault("MINIO_ADDRESS", "localhost:9000")
	minioEndpoint := envOrDefault("ICEBERG_MINIO_ENDPOINT", "http://"+minioAddr)
	minioAccessKey := envOrDefault("ICEBERG_MINIO_ACCESS_KEY", "minioadmin")
	minioSecretKey := envOrDefault("ICEBERG_MINIO_SECRET_KEY", "minioadmin")
	bucket := envOrDefault("MINIO_BUCKET", "a-bucket")
	tablePath := envOrDefault("ICEBERG_TABLE_PATH", "iceberg-test/e2e_test_table")
	numRows := envOrDefault("ICEBERG_NUM_ROWS", "1000")
	dim := envOrDefault("ICEBERG_DIM", "128")

	// --- Phase 0: Create Iceberg table on MinIO using Python script ---
	t.Log("[Phase 0] Creating Iceberg test table on MinIO...")
	tableInfo := createIcebergTable(
		t, externalDataSchemaBasic, minioEndpoint, minioAccessKey,
		minioSecretKey, bucket, tablePath, numRows, dim, "")
	t.Logf("[Phase 0] Iceberg table created: metadata=%s, snapshot_id=%d, rows=%d",
		tableInfo.MetadataLocation, tableInfo.SnapshotID, tableInfo.NumRows)

	// Convert Iceberg-native URI (s3://bucket/key) to Milvus form with the
	// configured MinIO endpoint as URI host.
	minioHost := strings.TrimPrefix(strings.TrimPrefix(minioEndpoint, "http://"), "https://")
	externalSource := toMilvusS3URIForMinIO(tableInfo.MetadataLocation, minioHost)

	// Build ExternalSpec with the minimal extfs needed for MinIO access.
	type externalSpecJSON struct {
		Format     string            `json:"format"`
		SnapshotID int64             `json:"snapshot_id,string"`
		Extfs      map[string]string `json:"extfs,omitempty"`
	}
	specObj := externalSpecJSON{
		Format:     "iceberg-table",
		SnapshotID: tableInfo.SnapshotID,
		Extfs: map[string]string{
			"access_key_id":    minioAccessKey,
			"access_key_value": minioSecretKey,
			"cloud_provider":   "minio",
			"region":           "us-east-1",
			"use_ssl":          "false",
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
		// Pass the full spec (with credentials) at CreateCollection too —
		// ValidateExtfsComplete requires an explicit credential mode.
		WithExternalSpec(externalSpec).
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
			WithExternalSource(externalSource).
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

// TestExternalTableIcebergRefreshFailsOnSchemaTypeMismatch verifies that
// RefreshExternalCollection fails during sample when the collection schema
// declares a different type from the external Arrow column type.
func TestExternalTableIcebergRefreshFailsOnSchemaTypeMismatch(t *testing.T) {
	minioAddr := envOrDefault("MINIO_ADDRESS", "localhost:9000")
	minioEndpoint := envOrDefault("ICEBERG_MINIO_ENDPOINT", "http://"+minioAddr)
	minioAccessKey := envOrDefault("ICEBERG_MINIO_ACCESS_KEY", "minioadmin")
	minioSecretKey := envOrDefault("ICEBERG_MINIO_SECRET_KEY", "minioadmin")
	bucket := envOrDefault("MINIO_BUCKET", "a-bucket")
	collName := common.GenRandomString("iceberg_schema_mismatch", 6)
	tablePath := fmt.Sprintf("iceberg-test/%s", collName)

	tableInfo := createIcebergTable(
		t, externalDataSchemaBasic, minioEndpoint, minioAccessKey,
		minioSecretKey, bucket, tablePath, "16", "4", "")

	minioHost := strings.TrimPrefix(strings.TrimPrefix(minioEndpoint, "http://"), "https://")
	externalSource := toMilvusS3URIForMinIO(tableInfo.MetadataLocation, minioHost)

	type externalSpecJSON struct {
		Format     string            `json:"format"`
		SnapshotID int64             `json:"snapshot_id,string"`
		Extfs      map[string]string `json:"extfs,omitempty"`
	}
	specObj := externalSpecJSON{
		Format:     "iceberg-table",
		SnapshotID: tableInfo.SnapshotID,
		Extfs: map[string]string{
			"access_key_id":    minioAccessKey,
			"access_key_value": minioSecretKey,
			"cloud_provider":   "minio",
			"region":           "us-east-1",
			"use_ssl":          "false",
		},
	}
	specBytes, err := json.Marshal(specObj)
	require.NoError(t, err)
	externalSpec := string(specBytes)

	ctx := hp.CreateContext(t, 10*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	t.Cleanup(func() {
		_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource(externalSource).
		WithExternalSpec(externalSpec).
		WithField(entity.NewField().WithName("pk").WithDataType(entity.FieldTypeInt64).WithExternalField("pk")).
		// The external Iceberg column "label" is a string, but the Milvus
		// schema intentionally declares it as Int64. Refresh should fail
		// while sampling field sizes, before load/search.
		WithField(entity.NewField().WithName("label_as_int").WithDataType(entity.FieldTypeInt64).WithExternalField("label")).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(int64(tableInfo.Dim)).WithExternalField("vector"))

	err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)

	refreshResult, err := mc.RefreshExternalCollection(ctx,
		client.NewRefreshExternalCollectionOption(collName).
			WithExternalSource(externalSource).
			WithExternalSpec(externalSpec))
	common.CheckErr(t, err, true)

	deadline := time.After(5 * time.Minute)
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			t.Fatalf("refresh did not fail on schema type mismatch before timeout")
		case <-ticker.C:
			progress, err := mc.GetRefreshExternalCollectionProgress(ctx,
				client.NewGetRefreshExternalCollectionProgressOption(refreshResult.JobID))
			require.NoError(t, err)
			t.Logf("schema mismatch refresh job %d: state=%s reason=%s",
				refreshResult.JobID, progress.State, progress.Reason)
			switch progress.State {
			case entity.RefreshStateCompleted:
				t.Fatalf("refresh unexpectedly completed despite label string -> Int64 schema mismatch")
			case entity.RefreshStateFailed:
				require.Contains(t, progress.Reason, "field type mismatch")
				require.Contains(t, progress.Reason, "expected Arrow int64")
				require.Contains(t, progress.Reason, "actual Arrow string")
				return
			}
		}
	}
}

// createIcebergTable runs the Python script to create an Iceberg table on MinIO.
func createIcebergTable(t *testing.T, schema, endpoint, accessKey, secretKey, bucket,
	tablePath, numRows, vecDim, binVecDim string,
) icebergTableInfo {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok, "failed to get caller info")
	scriptPath := filepath.Join(filepath.Dir(thisFile), "generate_iceberg_data.py")
	infoPath := filepath.Join(t.TempDir(), fmt.Sprintf("iceberg_%s_info.json", schema))
	args := []string{
		scriptPath,
		"--schema", schema,
		"--endpoint", endpoint,
		"--bucket", bucket,
		"--table-path", tablePath,
		"--num-rows", numRows,
		"--vec-dim", vecDim,
		"--output", infoPath,
	}
	if binVecDim != "" {
		args = append(args, "--bin-vec-dim", binVecDim)
	}
	cmd := exec.Command("python3", args...) // #nosec G204
	cmd.Env = append(os.Environ(),
		"MINIO_ACCESS_KEY="+accessKey,
		"MINIO_SECRET_KEY="+secretKey)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	require.NoError(t, err, "failed to create %s iceberg table via Python script", schema)

	data, err := os.ReadFile(infoPath)
	require.NoError(t, err, "failed to read %s", infoPath)

	var info icebergTableInfo
	require.NoError(t, json.Unmarshal(data, &info), "failed to parse %s", infoPath)
	return info
}

// TestExternalCollectionMultipleDataTypesIceberg mirrors the parquet/vortex/lance
// multi-type tests for the iceberg-table source. Iceberg lacks Int8/Int16
// primitives so int8_val/int16_val are widened to Int32 in the source schema;
// milvus narrows them on read.
func TestExternalCollectionMultipleDataTypesIceberg(t *testing.T) {
	t.Parallel()

	minioAddr := envOrDefault("MINIO_ADDRESS", "localhost:9000")
	minioEndpoint := envOrDefault("ICEBERG_MINIO_ENDPOINT", "http://"+minioAddr)
	minioAccessKey := envOrDefault("ICEBERG_MINIO_ACCESS_KEY", "minioadmin")
	minioSecretKey := envOrDefault("ICEBERG_MINIO_SECRET_KEY", "minioadmin")
	bucket := envOrDefault("MINIO_BUCKET", "a-bucket")

	collName := common.GenRandomString("ext_multi_iceberg", 6)
	tablePath := fmt.Sprintf("iceberg-test/%s", collName)

	const numRows = 100
	tableInfo := createIcebergTable(
		t, externalDataSchemaMulti, minioEndpoint, minioAccessKey,
		minioSecretKey, bucket, tablePath,
		fmt.Sprintf("%d", numRows), fmt.Sprintf("%d", testVecDim), fmt.Sprintf("%d", testBinVecDim))

	minioHost := strings.TrimPrefix(strings.TrimPrefix(minioEndpoint, "http://"), "https://")
	externalSource := toMilvusS3URIForMinIO(tableInfo.MetadataLocation, minioHost)

	type externalSpecJSON struct {
		Format     string            `json:"format"`
		SnapshotID int64             `json:"snapshot_id,string"`
		Extfs      map[string]string `json:"extfs,omitempty"`
	}
	specObj := externalSpecJSON{
		Format:     "iceberg-table",
		SnapshotID: tableInfo.SnapshotID,
		Extfs: map[string]string{
			"access_key_id":    minioAccessKey,
			"access_key_value": minioSecretKey,
			"cloud_provider":   "minio",
			"region":           "us-east-1",
			"use_ssl":          "false",
		},
	}
	specBytes, err := json.Marshal(specObj)
	require.NoError(t, err)
	externalSpec := string(specBytes)

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	t.Cleanup(func() {
		_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

	schema := buildMultiTypeExternalSchema(collName, externalSource, externalSpec)

	err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)
	t.Logf("Created multi-type iceberg external collection: %s", collName)

	runMultiTypeRefreshIndexLoadVerifyWithSourceSpec(ctx, t, mc, collName, int64(numRows), externalSource, externalSpec)
}

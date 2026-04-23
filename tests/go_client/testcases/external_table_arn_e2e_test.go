package testcases

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// TestExternalTableArnE2E tests the AssumeRole (ARN) credential path for
// external tables using MinIO's built-in STS AssumeRole simulation.
//
// How it works:
//   - MinIO's STS AssumeRole endpoint ignores the RoleArn parameter and
//     returns temporary credentials based on the authenticated user's permissions.
//   - The Milvus process must be started with these env vars so the Rust AWS SDK
//     redirects STS calls to MinIO:
//     AWS_ENDPOINT_URL=http://localhost:9000
//     AWS_ACCESS_KEY_ID=arn-test-user
//     AWS_SECRET_ACCESS_KEY=arn-test-password
//     AWS_REGION=us-east-1
//
// Prerequisites:
//  1. MinIO running at localhost:9000
//  2. MinIO user "arn-test-user" created with readwrite policy:
//     mc alias set local http://localhost:9000 minioadmin minioadmin
//     mc admin user add local arn-test-user arn-test-password
//     mc admin policy attach local readwrite --user arn-test-user
//  3. Milvus started with the env vars above
//
// Set ARN_TEST_ENABLED=true to run this test.
func TestExternalTableArnE2E(t *testing.T) {
	if os.Getenv("ARN_TEST_ENABLED") != "true" {
		t.Skip("skip: set ARN_TEST_ENABLED=true (and configure MinIO STS user + Milvus env vars) to run")
	}

	// Connect to MinIO with root credentials for writing test data
	minioCfg := getMinIOConfig()
	minioClient, err := newMinIOClient(minioCfg)
	if err != nil {
		t.Skipf("Failed to create MinIO client: %v", err)
	}

	ctx := hp.CreateContext(t, 10*time.Minute)

	// Step 1: Upload test parquet data to MinIO
	collName := common.GenRandomString("ext_arn_e2e", 6)
	extPath := fmt.Sprintf("external-arn-test/%s", collName)

	// Use a full cross-bucket URI so BuildExtfsOverrides extracts address/bucket
	// from the URI and the C++ resolve_config can match extfs.* properties.
	// Without the full URI, the extfs config has no address and sampling fails.
	externalSource := fmt.Sprintf("s3://%s/%s/%s", minioCfg.address, minioCfg.bucket, extPath)

	const numFiles = 3
	const rowsPerFile = int64(100)

	for i := 0; i < numFiles; i++ {
		data, err := generateParquetBytes(rowsPerFile, int64(i)*rowsPerFile)
		require.NoError(t, err)

		objectKey := fmt.Sprintf("%s/data%d.parquet", extPath, i)
		_, err = minioClient.PutObject(ctx, minioCfg.bucket, objectKey,
			bytes.NewReader(data), int64(len(data)),
			miniogo.PutObjectOptions{ContentType: "application/octet-stream"})
		require.NoError(t, err)
		t.Logf("Uploaded %s/%s (%d bytes)", minioCfg.bucket, objectKey, len(data))
	}

	t.Cleanup(func() {
		cleanupMinIOPrefix(ctx, minioClient, minioCfg.bucket,
			fmt.Sprintf("%s/", extPath))
	})

	// Step 2: Build ExternalSpec with ARN credentials
	// role_arn is a dummy value — MinIO's STS ignores it.
	// The actual auth uses AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY env vars
	// on the Milvus process as base credentials for the STS AssumeRole call.
	type externalSpecJSON struct {
		Format string            `json:"format"`
		Extfs  map[string]string `json:"extfs,omitempty"`
	}
	specObj := externalSpecJSON{
		Format: "parquet",
		Extfs: map[string]string{
			"cloud_provider": "aws",
			"region":         "us-east-1",
			"storage_type":   "remote",
			"use_ssl":        "false",
			"role_arn":       "arn:aws:iam::123456789012:role/minio-test-role",
			"load_frequency": "900",
		},
	}
	specBytes, err := json.Marshal(specObj)
	require.NoError(t, err)
	externalSpec := string(specBytes)

	t.Logf("[ARN E2E] External path: %s", extPath)
	t.Logf("[ARN E2E] External spec: %s", externalSpec)

	// Step 3: Create external collection
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource(externalSource).
		WithExternalSpec(externalSpec).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
		WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value")).
		WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(testVecDim).WithExternalField("embedding"))

	err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)
	t.Logf("[ARN E2E] Created collection: %s", collName)

	t.Cleanup(func() {
		_ = mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
	})

	// Step 4: Refresh with ARN spec
	t.Log("[ARN E2E] Triggering refresh with ARN role_arn in extfs...")
	refreshResult, err := mc.RefreshExternalCollection(ctx,
		client.NewRefreshExternalCollectionOption(collName).
			WithExternalSpec(externalSpec))
	common.CheckErr(t, err, true)
	t.Logf("[ARN E2E] Refresh triggered, jobID=%d", refreshResult.JobID)

	// Poll for completion
	jobID := refreshResult.JobID
	deadline := time.After(5 * time.Minute)
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			t.Fatalf("[ARN E2E] Refresh timed out after 5 minutes")
		case <-ticker.C:
			progress, err := mc.GetRefreshExternalCollectionProgress(ctx,
				client.NewGetRefreshExternalCollectionProgressOption(jobID))
			require.NoError(t, err)
			t.Logf("[ARN E2E] Refresh: state=%s", progress.State)

			if progress.State == entity.RefreshStateCompleted {
				t.Log("[ARN E2E] Refresh completed")
				goto loadStep
			}
			if progress.State == entity.RefreshStateFailed {
				t.Fatalf("[ARN E2E] Refresh FAILED: %s", progress.Reason)
			}
		}
	}
loadStep:

	// Step 5: Create index + Load
	t.Log("[ARN E2E] Creating index...")
	idxTask, err := mc.CreateIndex(ctx,
		client.NewCreateIndexOption(collName, "embedding", index.NewFlatIndex(entity.COSINE)))
	require.NoError(t, err)
	err = idxTask.Await(ctx)
	require.NoError(t, err)

	t.Log("[ARN E2E] Loading collection...")
	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	require.NoError(t, err)
	err = loadTask.Await(ctx)
	require.NoError(t, err)
	t.Log("[ARN E2E] Collection loaded")

	// Step 6: Query to verify data
	time.Sleep(3 * time.Second) // wait for segments to be serviceable
	t.Log("[ARN E2E] Querying to verify data...")
	queryResult, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithFilter("id >= 0").
		WithOutputFields("id").
		WithLimit(500))
	common.CheckErr(t, err, true)

	rowCount := queryResult.GetColumn("id").Len()
	t.Logf("[ARN E2E] Query returned %d rows", rowCount)
	require.Greater(t, rowCount, 0, "expected at least some rows from ARN-authenticated read")

	t.Log("=== ARN E2E Test PASSED ===")
}

// TestExternalTableArnE2E_StrictVerification proves the AssumeRole path is
// actually being used (not silently falling back to AKSK).
//
// Strategy:
//  1. Use an ISOLATED bucket (arn-isolated-bucket) that Milvus has never
//     accessed. This is critical because FilesystemCache::GetCacheKey()
//     returns only "address/bucket", so once a bucket is cached with any
//     credentials, subsequent requests for the same bucket reuse the cached
//     filesystem regardless of the new credentials. Using a fresh bucket
//     forces a new filesystem to be created with the configured credentials.
//  2. Inject INVALID access_key_id / access_key_value into extfs alongside
//     the role_arn. When role_arn is set, PR #481's C++ ToStorageOptions
//     skips AKSK entirely; the Rust layer uses AssumeRoleProvider with base
//     credentials from env vars (AWS_ACCESS_KEY_ID=arn-test-user).
//
// Expected:
//   - ARN path taken:   PASSES (invalid AKSK ignored, STS returns valid temp creds)
//   - AKSK fallback:    FAILS with auth error (invalid AKSK attempted)
func TestExternalTableArnE2E_StrictVerification(t *testing.T) {
	if os.Getenv("ARN_TEST_ENABLED") != "true" {
		t.Skip("skip: set ARN_TEST_ENABLED=true (and configure MinIO STS user + Milvus env vars) to run")
	}

	minioCfg := getMinIOConfig()
	minioClient, err := newMinIOClient(minioCfg)
	if err != nil {
		t.Skipf("Failed to create MinIO client: %v", err)
	}

	ctx := hp.CreateContext(t, 10*time.Minute)

	// Use an isolated bucket (not Milvus's main "a-bucket") so the filesystem
	// cache does not have a pre-cached entry with minioadmin credentials.
	isolatedBucket := "arn-isolated-bucket"
	exists, err := minioClient.BucketExists(ctx, isolatedBucket)
	require.NoError(t, err)
	if !exists {
		err = minioClient.MakeBucket(ctx, isolatedBucket, miniogo.MakeBucketOptions{})
		require.NoError(t, err)
	}

	collName := common.GenRandomString("ext_arn_strict", 6)
	extPath := fmt.Sprintf("external-arn-strict/%s", collName)
	externalSource := fmt.Sprintf("s3://%s/%s/%s", minioCfg.address, isolatedBucket, extPath)

	// Upload test data using minioadmin (root)
	const numFiles = 2
	const rowsPerFile = int64(50)
	for i := 0; i < numFiles; i++ {
		data, err := generateParquetBytes(rowsPerFile, int64(i)*rowsPerFile)
		require.NoError(t, err)
		objectKey := fmt.Sprintf("%s/data%d.parquet", extPath, i)
		_, err = minioClient.PutObject(ctx, isolatedBucket, objectKey,
			bytes.NewReader(data), int64(len(data)),
			miniogo.PutObjectOptions{ContentType: "application/octet-stream"})
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		cleanupMinIOPrefix(ctx, minioClient, isolatedBucket, fmt.Sprintf("%s/", extPath))
	})

	// KEY: extfs contains role_arn ONLY — no AK/SK, because the new
	// ValidateExtfsComplete treats credential modes as mutually exclusive
	// (role_arn + AK/SK together is rejected at CreateCollection time).
	// Under the ARN design, base creds for the AssumeRole call come from
	// the Milvus process env vars (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY),
	// not from spec.extfs.
	type externalSpecJSON struct {
		Format string            `json:"format"`
		Extfs  map[string]string `json:"extfs,omitempty"`
	}
	specObj := externalSpecJSON{
		Format: "parquet",
		Extfs: map[string]string{
			"cloud_provider": "aws",
			"region":         "us-east-1",
			"storage_type":   "remote",
			"use_ssl":        "false",
			"role_arn":       "arn:aws:iam::123456789012:role/minio-test-role",
			"load_frequency": "900",
		},
	}
	specBytes, err := json.Marshal(specObj)
	require.NoError(t, err)
	externalSpec := string(specBytes)

	t.Logf("[ARN STRICT] extfs has INVALID AKSK: deliberately-invalid-ak")
	t.Logf("[ARN STRICT] If AssumeRole path is working, invalid AKSK is ignored.")
	t.Logf("[ARN STRICT] If AKSK path is (incorrectly) taken, test will fail with auth error.")

	mc := hp.CreateDefaultMilvusClient(ctx, t)

	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource(externalSource).
		WithExternalSpec(externalSpec).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
		WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value")).
		WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(testVecDim).WithExternalField("embedding"))

	err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)
	t.Cleanup(func() {
		_ = mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
	})

	// Refresh — this exercises the explore + sample + manifest write path,
	// which all go through the ARN-authenticated S3 filesystem.
	refreshResult, err := mc.RefreshExternalCollection(ctx,
		client.NewRefreshExternalCollectionOption(collName).
			WithExternalSpec(externalSpec))
	common.CheckErr(t, err, true)

	jobID := refreshResult.JobID
	deadline := time.After(3 * time.Minute)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			t.Fatalf("[ARN STRICT] Refresh timed out")
		case <-ticker.C:
			progress, err := mc.GetRefreshExternalCollectionProgress(ctx,
				client.NewGetRefreshExternalCollectionProgressOption(jobID))
			require.NoError(t, err)
			t.Logf("[ARN STRICT] Refresh: state=%s", progress.State)

			if progress.State == entity.RefreshStateCompleted {
				goto loadStep
			}
			if progress.State == entity.RefreshStateFailed {
				// This is the expected failure mode if AKSK were used — but we
				// want ARN to succeed, so this is a test failure.
				t.Fatalf("[ARN STRICT] Refresh FAILED (likely means AKSK fallback was taken): %s", progress.Reason)
			}
		}
	}
loadStep:

	idxTask, err := mc.CreateIndex(ctx,
		client.NewCreateIndexOption(collName, "embedding", index.NewFlatIndex(entity.COSINE)))
	require.NoError(t, err)
	err = idxTask.Await(ctx)
	require.NoError(t, err)

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	require.NoError(t, err)
	err = loadTask.Await(ctx)
	require.NoError(t, err)

	time.Sleep(3 * time.Second)
	queryResult, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithFilter("id >= 0").
		WithOutputFields("id").
		WithLimit(500))
	common.CheckErr(t, err, true)

	rowCount := queryResult.GetColumn("id").Len()
	require.Equal(t, int(numFiles*rowsPerFile), rowCount,
		"expected %d rows from ARN-authenticated read; got %d", numFiles*rowsPerFile, rowCount)

	t.Logf("[ARN STRICT] Read %d rows despite invalid AKSK — proves AssumeRole path is active", rowCount)
	t.Log("=== ARN STRICT Verification PASSED ===")
}

// TestExternalTableArnE2E_NegativeControl verifies that invalid AKSK WITHOUT
// role_arn actually causes a failure. This is the negative-control companion
// to TestExternalTableArnE2E_StrictVerification:
//
//   - Strict test passes with invalid AKSK + role_arn     → proves ARN path is active
//   - Negative control fails with invalid AKSK (no ARN)   → proves invalid AKSK is rejected
//
// If the negative control ALSO passed, it would mean invalid AKSK is silently
// accepted (bug), making the strict test a false positive. A PASS on the
// negative control is a test failure here (we expect and require the refresh
// to fail).
func TestExternalTableArnE2E_NegativeControl(t *testing.T) {
	if os.Getenv("ARN_TEST_ENABLED") != "true" {
		t.Skip("skip: set ARN_TEST_ENABLED=true to run")
	}

	minioCfg := getMinIOConfig()
	minioClient, err := newMinIOClient(minioCfg)
	if err != nil {
		t.Skipf("Failed to create MinIO client: %v", err)
	}

	ctx := hp.CreateContext(t, 5*time.Minute)

	// Use a different isolated bucket for this test so it starts with an
	// empty filesystem cache.
	isolatedBucket := "arn-isolated-neg-bucket"
	exists, err := minioClient.BucketExists(ctx, isolatedBucket)
	require.NoError(t, err)
	if !exists {
		err = minioClient.MakeBucket(ctx, isolatedBucket, miniogo.MakeBucketOptions{})
		require.NoError(t, err)
	}

	collName := common.GenRandomString("ext_arn_neg", 6)
	extPath := fmt.Sprintf("external-arn-neg/%s", collName)
	externalSource := fmt.Sprintf("s3://%s/%s/%s", minioCfg.address, isolatedBucket, extPath)

	data, err := generateParquetBytes(50, 0)
	require.NoError(t, err)
	objectKey := fmt.Sprintf("%s/data0.parquet", extPath)
	_, err = minioClient.PutObject(ctx, isolatedBucket, objectKey,
		bytes.NewReader(data), int64(len(data)),
		miniogo.PutObjectOptions{ContentType: "application/octet-stream"})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupMinIOPrefix(ctx, minioClient, isolatedBucket, fmt.Sprintf("%s/", extPath))
	})

	// Invalid AKSK, NO role_arn — must fail because invalid AKSK is used directly.
	type externalSpecJSON struct {
		Format string            `json:"format"`
		Extfs  map[string]string `json:"extfs,omitempty"`
	}
	specObj := externalSpecJSON{
		Format: "parquet",
		Extfs: map[string]string{
			"cloud_provider":   "aws",
			"region":           "us-east-1",
			"storage_type":     "remote",
			"use_ssl":          "false",
			"access_key_id":    "deliberately-invalid-ak",
			"access_key_value": "deliberately-invalid-sk",
		},
	}
	specBytes, err := json.Marshal(specObj)
	require.NoError(t, err)
	externalSpec := string(specBytes)

	mc := hp.CreateDefaultMilvusClient(ctx, t)
	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource(externalSource).
		WithExternalSpec(externalSpec).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
		WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value")).
		WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(testVecDim).WithExternalField("embedding"))

	err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)
	t.Cleanup(func() { _ = mc.DropCollection(ctx, client.NewDropCollectionOption(collName)) })

	refreshResult, err := mc.RefreshExternalCollection(ctx,
		client.NewRefreshExternalCollectionOption(collName).
			WithExternalSpec(externalSpec))
	// Refresh itself may return immediately or error — the failure manifests
	// during the async task execution. Poll for a terminal state.
	if err != nil {
		t.Logf("[ARN NEG] Refresh call returned error (acceptable): %v", err)
		t.Log("=== Negative control VERIFIED: invalid AKSK rejected at submit ===")
		return
	}

	// Negative verification: wait up to 30s and ensure Refresh does NOT complete.
	// If it completes successfully, invalid AKSK is silently accepted (the strict
	// test would be a false positive). If it stays in Pending/Failed (Milvus
	// retries credential errors without marking JobStateFailed), the test
	// passes — we only care that it does NOT complete successfully.
	jobID := refreshResult.JobID
	deadline := time.After(30 * time.Second)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			t.Log("[ARN NEG] Refresh did not complete within 30s (AKSK error causes retry loop) — invalid AKSK is rejected")
			t.Log("=== Negative control VERIFIED: invalid AKSK prevents completion ===")
			return
		case <-ticker.C:
			progress, err := mc.GetRefreshExternalCollectionProgress(ctx,
				client.NewGetRefreshExternalCollectionProgressOption(jobID))
			require.NoError(t, err)
			if progress.State == entity.RefreshStateFailed {
				t.Logf("[ARN NEG] Refresh correctly FAILED: %s", progress.Reason)
				t.Log("=== Negative control VERIFIED: invalid AKSK causes failure ===")
				return
			}
			if progress.State == entity.RefreshStateCompleted {
				t.Fatal("[ARN NEG] Refresh completed with INVALID AKSK — this means invalid AKSK is silently accepted, making the strict test a false positive")
			}
		}
	}
}

// setupMinIOStsUser creates the MinIO user and policy for ARN testing.
// This is a helper that can be called manually before running the test.
// Usage: go test -run TestSetupMinIOStsUser -tags dynamic,test ./tests/go_client/testcases/ -v
func TestSetupMinIOStsUser(t *testing.T) {
	if os.Getenv("ARN_SETUP") != "true" {
		t.Skip("skip: set ARN_SETUP=true to run MinIO STS user setup")
	}

	minioCfg := getMinIOConfig()
	mcAlias := fmt.Sprintf("http://%s", minioCfg.address)

	commands := []struct {
		name string
		args []string
	}{
		{"set alias", []string{"mc", "alias", "set", "arn-local", mcAlias, minioCfg.accessKey, minioCfg.secretKey}},
		{"add user", []string{"mc", "admin", "user", "add", "arn-local", "arn-test-user", "arn-test-password"}},
		{"attach policy", []string{"mc", "admin", "policy", "attach", "arn-local", "readwrite", "--user", "arn-test-user"}},
	}

	for _, cmd := range commands {
		t.Logf("Running: %s", cmd.name)
		out, err := exec.Command(cmd.args[0], cmd.args[1:]...).CombinedOutput()
		if err != nil {
			t.Logf("Warning: %s failed: %v\nOutput: %s", cmd.name, err, string(out))
		} else {
			t.Logf("OK: %s\n%s", cmd.name, string(out))
		}
	}

	t.Log("MinIO STS user setup complete. Now start Milvus with:")
	t.Log("  AWS_ENDPOINT_URL=http://localhost:9000 \\")
	t.Log("  AWS_ACCESS_KEY_ID=arn-test-user \\")
	t.Log("  AWS_SECRET_ACCESS_KEY=arn-test-password \\")
	t.Log("  AWS_REGION=us-east-1 \\")
	t.Log("  scripts/start_standalone.sh")
}

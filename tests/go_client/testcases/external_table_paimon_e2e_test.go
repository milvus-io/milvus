// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package testcases

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

type paimonTableInfo struct {
	TableLocation string
	SnapshotIDs   []int64
}

type paimonExpectedRow struct {
	PK    int64
	Label string
}

func findPaimonLoon(t *testing.T) string {
	t.Helper()
	if configured := os.Getenv("PAIMON_LOON_BIN"); configured != "" {
		info, err := os.Stat(configured)
		require.NoError(t, err, "PAIMON_LOON_BIN does not exist")
		require.False(t, info.IsDir(), "PAIMON_LOON_BIN points to a directory")
		return configured
	}

	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok, "failed to locate the Paimon E2E source file")
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", ".."))
	candidates := []string{
		filepath.Join(repoRoot, "cmake_build", "thirdparty", "milvus-storage", "milvus-storage-build", "tools", "loon"),
		filepath.Join(repoRoot, "cmake_build", "thirdparty", "milvus-storage", "milvus-storage-src", "cpp", "build", "Release", "tools", "loon"),
	}
	for _, candidate := range candidates {
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate
		}
	}
	require.FailNow(t, "Paimon loon fixture binary not found",
		"build the C++ unit-test target or set PAIMON_LOON_BIN; checked %s", strings.Join(candidates, ", "))
	return ""
}

func createPaimonTable(t *testing.T, loon, scenario, minioAddress, accessKey, secretKey, bucket,
	warehousePath string, rows, dimension int, deletedRows []int64,
) paimonTableInfo {
	t.Helper()
	warehouse := fmt.Sprintf("s3://%s/%s/%s", minioAddress, bucket, strings.TrimPrefix(warehousePath, "/"))
	args := []string{
		"demo-table",
		"--type", "paimon",
		"--scenario", scenario,
		"--path", warehouse,
		"--dim", strconv.Itoa(dimension),
		"--prop", "extfs.paimon.address=http://" + minioAddress,
		"--prop", "extfs.paimon.bucket_name=" + bucket,
		"--prop", "extfs.paimon.access_key_id=" + accessKey,
		"--prop", "extfs.paimon.access_key_value=" + secretKey,
		"--prop", "extfs.paimon.storage_type=remote",
		"--prop", "extfs.paimon.cloud_provider=aws",
		"--prop", "extfs.paimon.region=us-east-1",
		"--prop", "extfs.paimon.use_ssl=false",
		"--prop", "extfs.paimon.use_iam=false",
		"--prop", "extfs.paimon.use_virtual_host=false",
	}
	args = append(args, "--rows", strconv.Itoa(rows))
	if len(deletedRows) > 0 {
		values := make([]string, 0, len(deletedRows))
		for _, row := range deletedRows {
			values = append(values, strconv.FormatInt(row, 10))
		}
		args = append(args, "--deletes", strings.Join(values, ","))
	}

	cmd := exec.Command(loon, args...) // #nosec G204 -- test-controlled binary and arguments
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "failed to create %s fixture with loon:\n%s", scenario, output)
	t.Logf("created %s Paimon fixture:\n%s", scenario, output)

	locationMatch := regexp.MustCompile(`(?m)^\s*(?:table_location|path):\s*(\S+)\s*$`).FindSubmatch(output)
	require.Len(t, locationMatch, 2, "loon output has no table location")
	snapshotMatch := regexp.MustCompile(`(?m)^\s*snapshots:\s*\[([0-9,]+)\]\s*$`).FindSubmatch(output)
	require.Len(t, snapshotMatch, 2, "loon output has no snapshots")

	snapshotValues := strings.Split(string(snapshotMatch[1]), ",")
	snapshotIDs := make([]int64, 0, len(snapshotValues))
	for _, value := range snapshotValues {
		snapshotID, err := strconv.ParseInt(value, 10, 64)
		require.NoError(t, err, "invalid snapshot id in loon output")
		snapshotIDs = append(snapshotIDs, snapshotID)
	}
	return paimonTableInfo{TableLocation: string(locationMatch[1]), SnapshotIDs: snapshotIDs}
}

func waitPaimonRefresh(t *testing.T, ctx context.Context, mc *base.MilvusClient, jobID int64) {
	t.Helper()
	deadline := time.After(10 * time.Minute)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			t.Fatalf("Paimon refresh job %d timed out", jobID)
		case <-ticker.C:
			progress, err := mc.GetRefreshExternalCollectionProgress(ctx,
				client.NewGetRefreshExternalCollectionProgressOption(jobID))
			require.NoError(t, err)
			switch progress.State {
			case entity.RefreshStateCompleted:
				return
			case entity.RefreshStateFailed:
				t.Fatalf("Paimon refresh job %d failed: %s", jobID, progress.Reason)
			}
		}
	}
}

func runPaimonSnapshotRead(t *testing.T, externalSource string, snapshotID int64, scanMode string, dimension int,
	expected []paimonExpectedRow, extfs map[string]string,
) {
	t.Helper()
	type externalSpecJSON struct {
		Format     string            `json:"format"`
		SnapshotID int64             `json:"snapshot_id,string"`
		ScanMode   string            `json:"scan_mode,omitempty"`
		Extfs      map[string]string `json:"extfs"`
	}
	specBytes, err := json.Marshal(externalSpecJSON{
		Format:     "paimon-table",
		SnapshotID: snapshotID,
		ScanMode:   scanMode,
		Extfs:      extfs,
	})
	require.NoError(t, err)
	externalSpec := string(specBytes)

	ctx := hp.CreateContext(t, 30*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collName := common.GenRandomString("paimon_e2e", 6)
	t.Cleanup(func() {
		_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource(externalSource).
		WithExternalSpec(externalSpec).
		WithField(entity.NewField().WithName("pk").WithDataType(entity.FieldTypeInt64).WithExternalField("pk")).
		WithField(entity.NewField().WithName("label").WithDataType(entity.FieldTypeVarChar).WithMaxLength(256).WithExternalField("label")).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(int64(dimension)).WithExternalField("vector"))
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema)))

	refreshResult, err := mc.RefreshExternalCollection(ctx,
		client.NewRefreshExternalCollectionOption(collName).
			WithExternalSource(externalSource).
			WithExternalSpec(externalSpec))
	require.NoError(t, err)
	waitPaimonRefresh(t, ctx, mc, refreshResult.JobID)

	indexTask, err := mc.CreateIndex(ctx,
		client.NewCreateIndexOption(collName, "vector", index.NewFlatIndex(entity.COSINE)))
	require.NoError(t, err)
	require.NoError(t, indexTask.Await(ctx))
	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	require.NoError(t, err)
	require.NoError(t, loadTask.Await(ctx))

	queryResult, err := mc.Query(ctx,
		client.NewQueryOption(collName).
			WithFilter("pk >= 0").
			WithOutputFields("pk", "label").
			WithLimit(100))
	require.NoError(t, err)
	pkColumn, ok := queryResult.GetColumn("pk").(*column.ColumnInt64)
	require.True(t, ok)
	labelColumn, ok := queryResult.GetColumn("label").(*column.ColumnVarChar)
	require.True(t, ok)
	require.Equal(t, len(pkColumn.Data()), len(labelColumn.Data()))

	actual := make([]paimonExpectedRow, len(pkColumn.Data()))
	for i, pk := range pkColumn.Data() {
		actual[i] = paimonExpectedRow{PK: pk, Label: labelColumn.Data()[i]}
	}
	sort.Slice(actual, func(i, j int) bool { return actual[i].PK < actual[j].PK })
	require.Equal(t, expected, actual)

	queryVector := make([]float32, dimension)
	searchResult, err := mc.Search(ctx,
		client.NewSearchOption(collName, 3, []entity.Vector{entity.FloatVector(queryVector)}).
			WithOutputFields("pk", "label"))
	require.NoError(t, err)
	require.NotEmpty(t, searchResult)
	require.Greater(t, searchResult[0].ResultCount, 0)
}

// TestExternalTablePaimonE2E mirrors the Iceberg external-table E2E while
// adding the two Paimon-specific correctness paths: row-tracking deletion
// vectors and primary-key merge-on-read. Each fixture reads both its initial
// and latest snapshot so snapshot pinning cannot be masked by latest-state
// planning.
//
// Run:
//
//	PAIMON_LOON_BIN=/path/to/loon go test -v -run TestExternalTablePaimonE2E \
//	  -timeout 45m -tags dynamic,test
func TestExternalTablePaimonE2E(t *testing.T) {
	minioAddress := envOrDefault("MINIO_ADDRESS", "localhost:9000")
	accessKey := envOrDefault("PAIMON_MINIO_ACCESS_KEY", "minioadmin")
	secretKey := envOrDefault("PAIMON_MINIO_SECRET_KEY", "minioadmin")
	bucket := envOrDefault("MINIO_BUCKET", "a-bucket")
	dimension := 4
	loon := findPaimonLoon(t)
	extfs := map[string]string{
		"access_key_id":    accessKey,
		"access_key_value": secretKey,
		"cloud_provider":   "minio",
		"region":           "us-east-1",
		"use_ssl":          "false",
	}

	appendTable := createPaimonTable(t, loon, "append-only", minioAddress, accessKey, secretKey, bucket,
		fmt.Sprintf("paimon-test/append-%d", time.Now().UnixNano()), 10, dimension, nil)
	require.Len(t, appendTable.SnapshotIDs, 1)
	appendSource := toMilvusS3URIForMinIO(appendTable.TableLocation, minioAddress)
	appendExpected := make([]paimonExpectedRow, 0, 10)
	for pk := int64(0); pk < 10; pk++ {
		appendExpected = append(appendExpected, paimonExpectedRow{PK: pk, Label: fmt.Sprintf("label_%d", pk%10)})
	}
	for _, testCase := range []struct {
		name     string
		scanMode string
	}{
		{name: "append_default_auto"},
		{name: "append_explicit_direct_file", scanMode: "direct-file"},
		{name: "append_explicit_data_split", scanMode: "data-split"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			runPaimonSnapshotRead(t, appendSource, appendTable.SnapshotIDs[0], testCase.scanMode, dimension,
				appendExpected, extfs)
		})
	}

	dvTable := createPaimonTable(t, loon, "deletion-vector", minioAddress, accessKey, secretKey, bucket,
		fmt.Sprintf("paimon-test/dv-%d", time.Now().UnixNano()), 10, dimension, []int64{0, 5, 9})
	require.Len(t, dvTable.SnapshotIDs, 2)
	dvSource := toMilvusS3URIForMinIO(dvTable.TableLocation, minioAddress)
	dvInitial := make([]paimonExpectedRow, 0, 10)
	for pk := int64(0); pk < 10; pk++ {
		dvInitial = append(dvInitial, paimonExpectedRow{PK: pk, Label: fmt.Sprintf("label_%d", pk%10)})
	}
	dvLatest := []paimonExpectedRow{
		{PK: 1, Label: "label_1"}, {PK: 2, Label: "label_2"},
		{PK: 3, Label: "label_3"}, {PK: 4, Label: "label_4"},
		{PK: 6, Label: "label_6"}, {PK: 7, Label: "label_7"},
		{PK: 8, Label: "label_8"},
	}
	t.Run("deletion_vector_initial_snapshot", func(t *testing.T) {
		runPaimonSnapshotRead(t, dvSource, dvTable.SnapshotIDs[0], "", dimension, dvInitial, extfs)
	})
	t.Run("deletion_vector_latest_snapshot", func(t *testing.T) {
		runPaimonSnapshotRead(t, dvSource, dvTable.SnapshotIDs[1], "", dimension, dvLatest, extfs)
	})

	morTable := createPaimonTable(t, loon, "merge-on-read", minioAddress, accessKey, secretKey, bucket,
		fmt.Sprintf("paimon-test/mor-%d", time.Now().UnixNano()), 6, dimension, nil)
	require.Len(t, morTable.SnapshotIDs, 2)
	morSource := toMilvusS3URIForMinIO(morTable.TableLocation, minioAddress)
	morInitial := make([]paimonExpectedRow, 0, 6)
	for pk := int64(0); pk < 6; pk++ {
		morInitial = append(morInitial, paimonExpectedRow{PK: pk, Label: fmt.Sprintf("label_%d", pk)})
	}
	morLatest := []paimonExpectedRow{
		{PK: 0, Label: "label_0"}, {PK: 1, Label: "label_1_updated"},
		{PK: 3, Label: "label_3"}, {PK: 4, Label: "label_4"},
		{PK: 5, Label: "label_5"}, {PK: 6, Label: "label_6"},
	}
	t.Run("merge_on_read_initial_snapshot", func(t *testing.T) {
		runPaimonSnapshotRead(t, morSource, morTable.SnapshotIDs[0], "", dimension, morInitial, extfs)
	})
	t.Run("merge_on_read_latest_snapshot", func(t *testing.T) {
		runPaimonSnapshotRead(t, morSource, morTable.SnapshotIDs[1], "", dimension, morLatest, extfs)
	})
}

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
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// Run against a Milvus build containing storage PR #801 and MinIO, with the
// Python dependencies in tests/go_client/requirements.txt installed.
func TestExternalTableDeltaLakeE2E(t *testing.T) {
	ctx := hp.CreateContext(t, 30*time.Minute)
	cfg := getMinIOConfig()
	store, err := newMinIOClient(cfg)
	require.NoError(t, err)
	exists, err := store.BucketExists(ctx, cfg.bucket)
	require.NoError(t, err)
	if !exists {
		require.NoError(t, store.MakeBucket(ctx, cfg.bucket, miniogo.MakeBucketOptions{}))
	}
	testPrefix := fmt.Sprintf("delta-test/%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cleanupMinIOPrefix(cleanupCtx, store, cfg.bucket, testPrefix+"/")
	})
	root := filepath.Join(t.TempDir(), "table")
	output, err := runPythonScript(t, "python3", "generate_deltalake_data.py", nil, root)
	require.NoError(t, err, "generate Delta fixture: %s", output)
	prefix := testPrefix + "/baseline"
	// Upload data first; versioned logs are then published one at a time.
	for _, name := range []string{"first.parquet", "second.parquet"} {
		data, err := os.ReadFile(filepath.Join(root, name))
		require.NoError(t, err)
		uploadParquetToMinIO(ctx, t, store, cfg.bucket, prefix+"/"+name, data)
	}
	publish := func(t *testing.T, root, prefix string, version int) {
		t.Helper()
		name := fmt.Sprintf("_delta_log/%020d.json", version)
		data, err := os.ReadFile(filepath.Join(root, name))
		require.NoError(t, err)
		uploadParquetToMinIO(ctx, t, store, cfg.bucket, prefix+"/"+name, data)
	}
	publish(t, root, prefix, 0)
	source := extTestURI(cfg, prefix)
	extfs := map[string]string{"access_key_id": cfg.accessKey, "access_key_value": cfg.secretKey, "use_ssl": "false"}
	expected := make([]deltaLakeExpectedRow, 10)
	for i := range expected {
		expected[i] = deltaLakeExpectedRow{PK: int64(i), Label: fmt.Sprintf("label_%d", i)}
	}
	mc, name := runDeltaLakeSnapshotRead(t, source, -1, 4, expected[:6], extfs)
	spec := func(version int64) string {
		b, err := json.Marshal(map[string]any{"format": "deltalake-table", "snapshot_id": version, "extfs": extfs})
		require.NoError(t, err)
		return string(b)
	}
	refresh := func(version int64, success bool) {
		job, err := mc.RefreshExternalCollection(ctx, client.NewRefreshExternalCollectionOption(name).WithExternalSource(source).WithExternalSpec(spec(version)))
		require.NoError(t, err)
		if success {
			waitForRefreshComplete(ctx, t, mc, job.JobID, 10*time.Minute)
			return
		}
		state := entity.RefreshStatePending
		require.Eventually(t, func() bool {
			progress, err := mc.GetRefreshExternalCollectionProgress(ctx, client.NewGetRefreshExternalCollectionProgressOption(job.JobID))
			if err != nil {
				return false
			}
			state = progress.State
			return state == entity.RefreshStateFailed || state == entity.RefreshStateCompleted
		}, 10*time.Minute, time.Second)
		require.Equal(t, entity.RefreshStateFailed, state)
	}
	assertCount := func(want int64) {
		require.Eventually(t, func() bool {
			result, err := mc.Query(ctx, client.NewQueryOption(name).WithOutputFields(common.QueryCountFieldName))
			if err != nil {
				return false
			}
			count, err := result.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
			return err == nil && count == want
		}, time.Minute, time.Second)
	}
	publish(t, root, prefix, 1)
	assertCount(6) // Source advancement alone must not change the imported snapshot.
	refresh(-1, true)
	assertCount(10)
	refresh(-1, true)
	assertCount(10) // Same-snapshot reuse.
	refresh(0, true)
	assertCount(6) // Version zero is a real snapshot.
	publish(t, root, prefix, 2)
	refresh(-1, true)
	assertCount(4)
	require.NoError(t, mc.ReleaseCollection(ctx, client.NewReleaseCollectionOption(name)))
	load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
	require.NoError(t, err)
	require.NoError(t, load.Await(ctx))
	assertCount(4)
	publish(t, root, prefix, 3)
	refresh(-1, false)
	assertCount(4) // Failed empty refresh preserves the previous data.
	refresh(99, false)
	assertCount(4) // Missing history cannot silently select latest.
	uploadParquetToMinIO(ctx, t, store, cfg.bucket, prefix+"/_delta_log/00000000000000000004.json",
		[]byte(`{"protocol":{"minReaderVersion":99,"minWriterVersion":99}}`+"\n"))
	refresh(4, false)
	assertCount(4)
	uploadParquetToMinIO(ctx, t, store, cfg.bucket, prefix+"/_delta_log/00000000000000000005.json", []byte("not a Delta action\n"))
	refresh(5, false)
	assertCount(4)
	savedKey := extfs["access_key_value"]
	extfs["access_key_value"] = "invalid-test-secret"
	refresh(0, false)
	assertCount(4)
	extfs["access_key_value"] = savedKey
	// Historical files remain queryable after the source becomes empty.
	runDeltaLakeSnapshotRead(t, source, 0, 4, expected[:6], extfs)
	// A second table changes its deletion vector while keeping the same file
	// and live-row count. Refresh must replace visible rows, not reuse old data.
	root = filepath.Join(t.TempDir(), "dv-table")
	output, err = runPythonScript(t, "python3", "generate_deltalake_data.py", nil, root, "--deletion-vectors")
	require.NoError(t, err, "generate Delta DV fixture: %s", output)
	prefix = testPrefix + "/dv"
	data, err := os.ReadFile(filepath.Join(root, "first.parquet"))
	require.NoError(t, err)
	uploadParquetToMinIO(ctx, t, store, cfg.bucket, prefix+"/first.parquet", data)
	publish(t, root, prefix, 0)
	publish(t, root, prefix, 1)
	source = extTestURI(cfg, prefix)
	mc, name = runDeltaLakeSnapshotRead(t, source, 1, 4, expected[2:6], extfs)
	publish(t, root, prefix, 2)
	refresh(2, true)
	assertCount(4)
	result, err := mc.Query(ctx, client.NewQueryOption(name).WithFilter("pk in [0,1]").WithOutputFields(common.QueryCountFieldName))
	require.NoError(t, err)
	count, err := result.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
	require.NoError(t, err)
	require.Equal(t, int64(2), count, "same-count DV update must replace visible rows")
	publish(t, root, prefix, 3)
	refresh(3, false)
	assertCount(4)
	runDeltaLakeSnapshotRead(t, source, 0, 4, expected[:6], extfs)

	require.NoError(t, mc.DropCollection(ctx, client.NewDropCollectionOption(name)))
	_, err = store.StatObject(ctx, cfg.bucket, prefix+"/first.parquet", miniogo.StatObjectOptions{})
	require.NoError(t, err, "drop must preserve source objects")

	for _, scenario := range []string{"--column-mapping", "--partitioned"} {
		t.Run(scenario, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "table")
			output, err := runPythonScript(t, "python3", "generate_deltalake_data.py", nil, root, scenario)
			require.NoError(t, err, "generate Delta fixture: %s", output)
			prefix := testPrefix + "/" + strings.TrimPrefix(scenario, "--")
			data, err := os.ReadFile(filepath.Join(root, "first.parquet"))
			require.NoError(t, err)
			uploadParquetToMinIO(ctx, t, store, cfg.bucket, prefix+"/first.parquet", data)
			publish(t, root, prefix, 0)
			rows := append([]deltaLakeExpectedRow(nil), expected[:6]...)
			if scenario == "--partitioned" {
				for i := range rows {
					rows[i].Label = "partition_label"
				}
			}
			runDeltaLakeSnapshotRead(t, extTestURI(cfg, prefix), 0, 4, rows, extfs)
		})
	}
}

func TestExternalTableDeltaLakeFieldTypes(t *testing.T) {
	ctx := hp.CreateContext(t, 5*time.Minute)
	cfg := getMinIOConfig()
	store, err := newMinIOClient(cfg)
	require.NoError(t, err)
	exists, err := store.BucketExists(ctx, cfg.bucket)
	require.NoError(t, err)
	if !exists {
		require.NoError(t, store.MakeBucket(ctx, cfg.bucket, miniogo.MakeBucketOptions{}))
	}

	root := t.TempDir()
	output, err := runPythonScript(t, "python3", "generate_deltalake_data.py", nil, root, "--field-types")
	require.NoError(t, err, "generate Delta field types fixture: %s", output)
	prefix := fmt.Sprintf("delta-test/%d/field-types", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cleanupMinIOPrefix(cleanupCtx, store, cfg.bucket, prefix+"/")
	})
	for _, name := range []string{"first.parquet", "_delta_log/00000000000000000000.json"} {
		data, err := os.ReadFile(filepath.Join(root, name))
		require.NoError(t, err)
		uploadParquetToMinIO(ctx, t, store, cfg.bucket, prefix+"/"+name, data)
	}

	field := func(name string, dataType entity.FieldType) *entity.Field {
		return entity.NewField().WithName(name).WithExternalField(name).WithDataType(dataType)
	}
	sparse, err := entity.NewSliceSparseEmbedding([]uint32{2, 7}, []float32{0.5, 0.25})
	require.NoError(t, err)
	fields := []struct {
		field *entity.Field
		value any
		index index.Index
	}{
		{field: field("pk", entity.FieldTypeInt64), value: int64(1)},
		{field: field("flag", entity.FieldTypeBool), value: true},
		{field: field("tiny", entity.FieldTypeInt8), value: int8(-8)},
		{field: field("small", entity.FieldTypeInt16), value: int16(-160)},
		{field: field("number", entity.FieldTypeInt32), value: int32(320)},
		{field: field("single", entity.FieldTypeFloat), value: float32(1.25)},
		{field: field("double_value", entity.FieldTypeDouble), value: 2.5},
		{field: field("label", entity.FieldTypeVarChar).WithMaxLength(64), value: "label_1"},
		{field: field("body", entity.FieldTypeText), value: "Delta text"},
		{field: field("attributes", entity.FieldTypeJSON), value: []byte(`{"score":7}`)},
		{field: field("numbers", entity.FieldTypeArray).WithElementType(entity.FieldTypeInt32).WithMaxCapacity(4), value: []int32{2, -3}},
		{field: field("event_time", entity.FieldTypeTimestamptz), value: time.UnixMicro(1704067200123456)},
		{field: field("point", entity.FieldTypeGeometry), value: "POINT (1 2)"},
		{
			field: field("vector", entity.FieldTypeFloatVector).WithDim(4),
			value: entity.FloatVector{2, 1, 2, 3},
			index: index.NewFlatIndex(entity.L2),
		},
		{
			field: field("fp16", entity.FieldTypeFloat16Vector).WithDim(4),
			value: entity.Float16Vector{0, 0x3C, 0, 0x40, 0, 0x42, 0, 0x44},
			index: index.NewFlatIndex(entity.L2),
		},
		{
			field: field("bf16", entity.FieldTypeBFloat16Vector).WithDim(4),
			value: entity.BFloat16Vector{0x80, 0x3F, 0, 0x40, 0x40, 0x40, 0x80, 0x40},
			index: index.NewFlatIndex(entity.L2),
		},
		{
			field: field("bits", entity.FieldTypeBinaryVector).WithDim(16),
			value: entity.BinaryVector{0xA5, 0xC3},
			index: index.NewBinFlatIndex(entity.HAMMING),
		},
		{
			field: field("int8_vector", entity.FieldTypeInt8Vector).WithDim(4),
			value: entity.Int8Vector{-1, 2, -3, 4},
			index: index.NewHNSWIndex(entity.COSINE, 8, 64),
		},
		{field: field("sparse", entity.FieldTypeSparseVector), value: sparse, index: index.NewSparseInvertedIndex(entity.IP, 0)},
	}

	name := common.GenRandomString("delta_field_types", 6)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	t.Cleanup(func() {
		_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(name))
	})
	schema := entity.NewSchema().WithName(name).
		WithExternalSource(extTestURI(cfg, prefix)).
		WithExternalSpec(extTestSpec(cfg, "deltalake-table"))
	outputFields := make([]string, 0, len(fields))
	for _, check := range fields {
		schema.WithField(check.field)
		outputFields = append(outputFields, check.field.Name)
	}
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(name, schema)))
	refreshAndWait(ctx, t, mc, name)
	for _, check := range fields {
		if check.index == nil {
			continue
		}
		task, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(name, check.field.Name, check.index))
		require.NoError(t, err, "%s", check.field.Name)
		require.NoError(t, task.Await(ctx), "%s", check.field.Name)
	}
	load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
	require.NoError(t, err)
	require.NoError(t, load.Await(ctx))

	result, err := mc.Query(ctx, client.NewQueryOption(name).WithFilter("pk == 1").WithOutputFields(outputFields...))
	require.NoError(t, err)
	require.Equal(t, 1, result.ResultCount)
	for _, check := range fields {
		name := check.field.Name
		col := result.GetColumn(name)
		require.NotNil(t, col, "%s", name)
		got, err := col.Get(0)
		require.NoError(t, err, "%s", name)
		switch want := check.value.(type) {
		case time.Time:
			raw, ok := got.(string)
			require.True(t, ok, "%s: expected a timestamp string, got %T", name, got)
			value, err := time.Parse(time.RFC3339Nano, raw)
			require.NoError(t, err, "%s", name)
			require.Equal(t, want.UnixMicro(), value.UnixMicro(), "%s", name)
		case entity.SparseEmbedding:
			value, ok := got.(entity.SparseEmbedding)
			require.True(t, ok, "%s: expected a sparse embedding, got %T", name, got)
			// The SDK may omit sparse dimension metadata in query results.
			require.Equal(t, want.Serialize(), value.Serialize(), "%s", name)
		default:
			require.Equal(t, want, got, "%s", name)
		}
	}
}

type deltaLakeExpectedRow struct {
	PK    int64
	Label string
}

func runDeltaLakeSnapshotRead(t *testing.T, externalSource string, snapshotID int64, dimension int,
	expected []deltaLakeExpectedRow, extfs map[string]string,
) (*base.MilvusClient, string) {
	t.Helper()
	type externalSpecJSON struct {
		Format     string            `json:"format"`
		SnapshotID int64             `json:"snapshot_id,string"`
		Extfs      map[string]string `json:"extfs"`
	}
	specBytes, err := json.Marshal(externalSpecJSON{
		Format:     "deltalake-table",
		SnapshotID: snapshotID,
		Extfs:      extfs,
	})
	require.NoError(t, err)
	externalSpec := string(specBytes)

	ctx := hp.CreateContext(t, 30*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collName := common.GenRandomString("deltalake_e2e", 6)
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
	waitForRefreshComplete(ctx, t, mc, refreshResult.JobID, 10*time.Minute)

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

	actual := make([]deltaLakeExpectedRow, len(pkColumn.Data()))
	for i, pk := range pkColumn.Data() {
		actual[i] = deltaLakeExpectedRow{PK: pk, Label: labelColumn.Data()[i]}
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
	return mc, collName
}

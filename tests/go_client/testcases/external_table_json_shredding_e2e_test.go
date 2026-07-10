package testcases

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

const runExternalJSONShreddingE2EEnv = "RUN_EXTERNAL_JSON_SHREDDING_E2E"

func TestExternalTableJSONShreddingE2E(t *testing.T) {
	if !isTruthyEnv(runExternalJSONShreddingE2EEnv) {
		t.Skipf("set %s=1 to run this local external JSON shredding e2e", runExternalJSONShreddingE2EEnv)
	}

	ctx := hp.CreateContext(t, 10*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	restoreConfig := alterMilvusConfigForExternalJSONShredding(t, map[string]string{
		"common.enabledJSONShredding":         "true",
		"common.usingJSONShreddingForQuery":   "true",
		"dataCoord.jsonShreddingMaxColumns":   "2",
		"dataCoord.jsonShreddingTriggerCount": "10",
	})
	t.Cleanup(restoreConfig)

	minioCfg := getMinIOConfig()
	minioClient, err := newMinIOClient(minioCfg)
	require.NoError(t, err)
	skipIfMinIOUnreachable(ctx, t, minioClient, minioCfg.bucket)

	collName := common.GenRandomString("ext_json_shred", 6)
	extPath := fmt.Sprintf("external-e2e-test/%s", collName)
	t.Cleanup(func() {
		_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
		cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket, extPath+"/")
	})

	const numRows = int64(1000)
	data, err := generateExternalJSONShreddingParquetBytes(numRows, 0, testVecDim)
	require.NoError(t, err)
	uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket, extPath+"/data.parquet", data)

	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource(extTestURI(minioCfg, extPath)).
		WithExternalSpec(extTestSpec(minioCfg, "parquet")).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
		WithField(entity.NewField().WithName("json_val").WithDataType(entity.FieldTypeJSON).WithExternalField("json_val")).
		WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(testVecDim).WithExternalField("embedding"))

	err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)

	refreshAndWait(ctx, t, mc, collName)

	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	common.CheckErr(t, err, true)
	jsonFieldID := getFieldIDByName(t, coll.Schema, "json_val")

	minioCfg = minIOConfigWithServerRootPath(t, minioCfg)
	segments := waitForExternalSegmentIDs(ctx, t, minioClient, minioCfg, coll.ID, 2*time.Minute)
	jsonStatsObjects := waitForExternalJSONStatsObjects(ctx, t, minioClient, minioCfg,
		coll.ID, jsonFieldID, segments, 6*time.Minute)
	require.Len(t, jsonStatsObjects, len(segments))

	idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(
		collName, "embedding", index.NewAutoIndex(entity.L2)))
	common.CheckErr(t, err, true)
	common.CheckErr(t, idxTask.Await(ctx), true)

	beforeLoadStatsCount := externalJSONMetricCount(t, "internal_json_stats_latency", "load_latency")
	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	common.CheckErr(t, loadTask.Await(ctx), true)
	waitForMetricIncrease(t, "internal_json_stats_latency", "load_latency",
		beforeLoadStatsCount, 90*time.Second)

	beforeJSONFilterCount := externalJSONMetricCount(t, "internal_json_filter_latency", "json_stats_latency")
	const expectedScoreRows = int(numRows / 10)
	for i := 0; i < 3; i++ {
		res, err := mc.Query(ctx, client.NewQueryOption(collName).
			WithConsistencyLevel(entity.ClStrong).
			WithFilter(`json_val["score"] < 10`).
			WithOutputFields("id").
			WithLimit(expectedScoreRows))
		common.CheckErr(t, err, true)
		require.Equal(t, expectedScoreRows, res.ResultCount)
		assertExternalJSONScoreRows(t, res, 10)
	}
	waitForMetricIncrease(t, "internal_json_filter_latency", "json_stats_latency",
		beforeJSONFilterCount, 90*time.Second)

	assertExternalJSONShreddingLogEvidence(t)
	t.Logf("External JSON shredding verified: collection=%s collectionID=%d jsonFieldID=%d segments=%v",
		collName, coll.ID, jsonFieldID, segments)
}

func alterMilvusConfigForExternalJSONShredding(t *testing.T, configs map[string]string) func() {
	t.Helper()

	prevValues := make(map[string]string, len(configs))
	for key, value := range configs {
		prev, err := hp.AlterServerConfig(key, value)
		require.NoError(t, err, "alter Milvus config %s=%s", key, value)
		prevValues[key] = prev
		t.Logf("Altered Milvus config %s=%s (prev=%q)", key, value, prev)
	}

	return func() {
		for key, value := range prevValues {
			if value == "" {
				continue
			}
			if _, err := hp.AlterServerConfig(key, value); err != nil {
				t.Logf("Failed to restore Milvus config %s=%s: %v", key, value, err)
			}
		}
	}
}

func generateExternalJSONShreddingParquetBytes(numRows int64, startID int64, dim int) ([]byte, error) {
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "json_val", Type: arrow.BinaryTypes.String},
		{Name: "embedding", Type: &arrow.FixedSizeBinaryType{ByteWidth: dim * 4}},
	}, nil)

	var buf bytes.Buffer
	writer, err := pqarrow.NewFileWriter(arrowSchema, &buf, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, err
	}

	pool := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(pool, arrowSchema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Int64Builder)
	jsonBuilder := builder.Field(1).(*array.StringBuilder)
	vectorBuilder := builder.Field(2).(*array.FixedSizeBinaryBuilder)

	for i := int64(0); i < numRows; i++ {
		id := startID + i
		idBuilder.Append(id)
		jsonBytes, err := json.Marshal(externalJSONShreddingPayload(id))
		if err != nil {
			return nil, err
		}
		jsonBuilder.Append(string(jsonBytes))

		vectorBytes := make([]byte, dim*4)
		for d := 0; d < dim; d++ {
			value := float32(id%100)*0.01 + float32(d)
			binary.LittleEndian.PutUint32(vectorBytes[d*4:], math.Float32bits(value))
		}
		vectorBuilder.Append(vectorBytes)
	}

	record := builder.NewRecord()
	defer record.Release()
	if err := writer.Write(record); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func externalJSONShreddingPayload(id int64) map[string]any {
	payload := map[string]any{
		"score":  id % 100,
		"bucket": id % 5,
	}
	if id%4 == 0 {
		payload["category"] = fmt.Sprintf("cat_%d", id%3)
	}
	if id%5 == 0 {
		payload["tag"] = fmt.Sprintf("tag_%d", id%7)
	}
	if id%6 == 0 {
		payload["cold"] = id % 11
	}
	if id%7 == 0 {
		payload["rare"] = fmt.Sprintf("rare_%d", id)
	}
	return payload
}

type externalJSONStatsObjects struct {
	all       []string
	meta      []string
	shared    []string
	shredding []string
	prefix    string
}

func findExternalJSONStatsObjects(ctx context.Context, mc *miniogo.Client, cfg minioConfig,
	collectionID, segmentID, fieldID int64,
) (externalJSONStatsObjects, error) {
	var lastErr error
	segmentMarker := fmt.Sprintf("/%d/_stats/json_stats.%d/", segmentID, fieldID)
	for _, prefix := range externalSegmentPrefixes(cfg, collectionID) {
		objects, err := listMinIOObjectsWithPrefix(ctx, mc, cfg.bucket, prefix)
		if err != nil {
			lastErr = err
			continue
		}

		evidence := externalJSONStatsObjects{prefix: prefix}
		for _, object := range objects {
			if !strings.Contains(object, segmentMarker) {
				continue
			}
			evidence.all = append(evidence.all, object)
			switch {
			case strings.HasSuffix(object, "/meta.json"):
				evidence.meta = append(evidence.meta, object)
			case strings.Contains(object, "/shared_key_index/"):
				evidence.shared = append(evidence.shared, object)
			case strings.Contains(object, "/shredding_data/"):
				evidence.shredding = append(evidence.shredding, object)
			}
		}
		if len(evidence.all) > 0 {
			return evidence, nil
		}
	}
	return externalJSONStatsObjects{}, lastErr
}

func waitForExternalJSONStatsObjects(ctx context.Context, t *testing.T, mc *miniogo.Client, cfg minioConfig,
	collectionID, fieldID int64, segments []int64, timeout time.Duration,
) map[int64]externalJSONStatsObjects {
	t.Helper()
	require.NotEmpty(t, segments, "external collection should have persisted segments")

	deadline := time.After(timeout)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		statsObjects := make(map[int64]externalJSONStatsObjects, len(segments))
		var missing []string
		for _, segment := range segments {
			evidence, err := findExternalJSONStatsObjects(ctx, mc, cfg, collectionID, segment, fieldID)
			require.NoError(t, err)
			switch {
			case len(evidence.meta) == 0:
				missing = append(missing, fmt.Sprintf("%d:meta", segment))
			case len(evidence.shared) == 0:
				missing = append(missing, fmt.Sprintf("%d:shared_key_index", segment))
			case len(evidence.shredding) == 0:
				missing = append(missing, fmt.Sprintf("%d:shredding_data", segment))
			default:
				statsObjects[segment] = evidence
				t.Logf("Found external JSON stats objects for segment %d under %s: meta=%v shared=%v shredding=%v",
					segment, evidence.prefix, evidence.meta, evidence.shared, evidence.shredding)
			}
		}
		if len(missing) == 0 {
			return statsObjects
		}

		t.Logf("Waiting for external JSON stats objects, missing: %v", missing)
		select {
		case <-deadline:
			t.Fatalf("Timed out waiting for external JSON stats objects for collection %d field %d; missing=%v",
				collectionID, fieldID, missing)
		case <-ticker.C:
		}
	}
}

func assertExternalJSONScoreRows(t *testing.T, res client.ResultSet, threshold int64) {
	t.Helper()

	idCol, ok := res.GetColumn("id").(*column.ColumnInt64)
	require.True(t, ok, "id column present")
	for i, id := range idCol.Data() {
		require.Less(t, id%100, threshold, "row %d should satisfy json_val[score] < %d", i, threshold)
	}
}

func externalManagementBaseURL() string {
	host := hostFromMilvusAddr(hp.GetAddr())
	if host == "" {
		host = "localhost"
	}
	return "http://" + net.JoinHostPort(host, "9091")
}

func externalJSONMetricCount(t *testing.T, metricName, metricType string) float64 {
	t.Helper()

	httpClient := &http.Client{Timeout: 10 * time.Second}
	resp, err := httpClient.Get(externalManagementBaseURL() + "/metrics")
	require.NoError(t, err, "read Milvus metrics")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "read Milvus metrics")

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	value, ok := parsePrometheusHistogramCount(string(body), metricName, metricType)
	if !ok {
		return 0
	}
	return value
}

func parsePrometheusHistogramCount(metricsText, metricName, metricType string) (float64, bool) {
	prefix := metricName + "_count"
	label := `type="` + metricType + `"`
	scanner := bufio.NewScanner(strings.NewReader(metricsText))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, prefix) || !strings.Contains(line, label) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		value, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err == nil {
			return value, true
		}
	}
	return 0, false
}

func waitForMetricIncrease(t *testing.T, metricName, metricType string, before float64, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var last float64
	for time.Now().Before(deadline) {
		last = externalJSONMetricCount(t, metricName, metricType)
		if last > before {
			t.Logf("Metric %s{type=%q} count increased: before=%v after=%v",
				metricName, metricType, before, last)
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("Metric %s{type=%q} count did not increase within %s: before=%v last=%v",
		metricName, metricType, timeout, before, last)
}

func assertExternalJSONShreddingLogEvidence(t *testing.T) {
	t.Helper()

	logFiles := externalJSONShreddingLogFiles(t)
	if len(logFiles) == 0 {
		t.Log("No local Milvus log files found; storage objects and metrics were used as hard JSON shredding evidence")
		return
	}

	patterns := map[string][]string{
		"build": {
			"field enable json key index, create json key index done",
			"build json stats for segment",
			"write meta file:",
		},
		"load": {
			"load json stats success",
			"Load JsonKeyStats",
			"load json stats for segment",
		},
		"query": {
			"using shredding data's field",
			"binary_range_json_by_stats",
			"plan_options.expr_use_json_stats",
		},
	}

	hits := make(map[string]int, len(patterns))
	for _, file := range logFiles {
		scanExternalJSONShreddingLogFile(t, file, patterns, hits)
	}
	t.Logf("External JSON shredding log evidence from %d files: %v", len(logFiles), hits)

	require.Greater(t, hits["build"], 0, "local Milvus logs should contain JSON stats build evidence")
	require.Greater(t, hits["load"], 0, "local Milvus logs should contain JSON stats load evidence")
	if hits["query"] == 0 {
		t.Log("No query-path JSON stats log line found; query-path verification used Prometheus metrics instead")
	}
}

func externalJSONShreddingLogFiles(t *testing.T) []string {
	t.Helper()

	var candidates []string
	for _, envKey := range []string{"MILVUS_LOG_PATH", "MILVUS_LOG_FILE"} {
		if value := strings.TrimSpace(os.Getenv(envKey)); value != "" {
			candidates = append(candidates, value)
		}
	}
	for _, envKey := range []string{"MILVUS_LOG_DIR", "MILVUS_LOG_PATHS"} {
		if value := strings.TrimSpace(os.Getenv(envKey)); value != "" {
			candidates = append(candidates, filepath.Join(value, "*.log"))
		}
	}
	candidates = append(candidates,
		"logs/*.log",
		"*.log",
		"/tmp/milvus/logs/*.log",
		"/tmp/milvus*.log",
		"/tmp/standalone*.log",
	)

	seen := make(map[string]struct{})
	var files []string
	for _, candidate := range candidates {
		matches, err := filepath.Glob(candidate)
		if err != nil || len(matches) == 0 {
			if _, err := os.Stat(candidate); err == nil {
				matches = []string{candidate}
			}
		}
		for _, match := range matches {
			info, err := os.Stat(match)
			if err != nil || info.IsDir() {
				continue
			}
			if _, ok := seen[match]; ok {
				continue
			}
			seen[match] = struct{}{}
			files = append(files, match)
		}
	}
	return files
}

func scanExternalJSONShreddingLogFile(t *testing.T, file string, patterns map[string][]string, hits map[string]int) {
	t.Helper()

	f, err := os.Open(file)
	if err != nil {
		t.Logf("Skip unreadable Milvus log file %s: %v", file, err)
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		for kind, kindPatterns := range patterns {
			for _, pattern := range kindPatterns {
				if strings.Contains(line, pattern) {
					hits[kind]++
					break
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		t.Logf("Failed to scan Milvus log file %s: %v", file, err)
	}
}

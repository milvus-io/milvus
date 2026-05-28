package testcases

import (
	"bytes"
	"context"
	"fmt"
	"path"
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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	milvuscommon "github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

func generateTextParquetBytes(numRows int64, startID int64) ([]byte, error) {
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "text", Type: arrow.BinaryTypes.String},
	}, nil)

	var buf bytes.Buffer
	writer, err := pqarrow.NewFileWriter(arrowSchema, &buf, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, err
	}

	pool := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(pool, arrowSchema)
	defer builder.Release()

	texts := []string{
		"the quick brown fox jumps over the lazy dog",
		"a fast red fox leaps across the sleeping hound",
		"machine learning algorithms process large datasets efficiently",
		"deep neural networks learn complex patterns from data",
		"natural language processing enables text understanding",
		"vector databases store and search high dimensional embeddings",
		"distributed systems handle concurrent requests at scale",
		"cloud computing provides elastic infrastructure resources",
		"search engines index documents for fast retrieval",
		"information retrieval systems rank results by relevance",
	}

	idBuilder := builder.Field(0).(*array.Int64Builder)
	textBuilder := builder.Field(1).(*array.StringBuilder)
	for i := int64(0); i < numRows; i++ {
		idBuilder.Append(startID + i)
		textBuilder.Append(texts[i%int64(len(texts))])
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

func generateTextMatchParquetBytes(numRows int64, startID int64) ([]byte, error) {
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "text_en", Type: arrow.BinaryTypes.String},
		{Name: "text_zh", Type: arrow.BinaryTypes.String},
	}, nil)

	var buf bytes.Buffer
	writer, err := pqarrow.NewFileWriter(arrowSchema, &buf, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, err
	}

	pool := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(pool, arrowSchema)
	defer builder.Release()

	textsEN := []string{
		"the quick brown fox jumps over the lazy dog",
		"a fast red fox leaps across the sleeping hound",
		"machine learning algorithms process large datasets efficiently",
		"deep neural networks learn complex patterns from data",
		"natural language processing enables text understanding",
		"vector databases store and search high dimensional embeddings",
		"distributed systems handle concurrent requests at scale",
		"cloud computing provides elastic infrastructure resources",
		"search engines index documents for fast retrieval",
		"information retrieval systems rank results by relevance",
	}
	textsZH := []string{
		"敏捷的棕色狐狸跳过了懒狗",
		"快速的红色狐狸越过沉睡的猎犬",
		"机器学习算法高效处理大规模数据集",
		"深度神经网络从数据中学习复杂模式",
		"自然语言处理支持文本理解",
		"向量数据库存储并搜索高维嵌入",
		"分布式系统可以处理大规模并发请求",
		"云计算提供弹性的基础设施资源",
		"搜索引擎为快速检索建立文档索引",
		"信息检索系统按照相关性排序结果",
	}

	idBuilder := builder.Field(0).(*array.Int64Builder)
	enBuilder := builder.Field(1).(*array.StringBuilder)
	zhBuilder := builder.Field(2).(*array.StringBuilder)
	for i := int64(0); i < numRows; i++ {
		idx := i % int64(len(textsEN))
		idBuilder.Append(startID + i)
		enBuilder.Append(textsEN[idx])
		zhBuilder.Append(textsZH[idx])
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

func waitForExternalTextMatchLoad(ctx context.Context, t *testing.T, mc *base.MilvusClient, collName string, timeout time.Duration) {
	t.Helper()

	deadline := time.After(timeout)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var lastErr error
	for attempt := 1; ; attempt++ {
		loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
		if err == nil {
			err = loadTask.Await(ctx)
		}
		if err == nil {
			t.Logf("Collection loaded after %d attempt(s)", attempt)
			return
		}
		lastErr = err
		t.Logf("Load attempt %d failed: %v", attempt, err)
		_ = mc.ReleaseCollection(context.Background(), client.NewReleaseCollectionOption(collName))

		select {
		case <-deadline:
			t.Fatalf("Load timed out: %v", lastErr)
		case <-ticker.C:
		}
	}
}

func getFieldIDByName(t *testing.T, schema *entity.Schema, fieldName string) int64 {
	t.Helper()
	for _, field := range schema.Fields {
		if field.Name == fieldName {
			require.NotZero(t, field.ID, "field %q should have an assigned field ID", fieldName)
			return field.ID
		}
	}
	t.Fatalf("field %q not found in schema", fieldName)
	return 0
}

func withTrailingSlash(prefix string) string {
	return strings.TrimSuffix(prefix, "/") + "/"
}

func externalSegmentPrefixes(cfg minioConfig, collectionID int64) []string {
	base := withTrailingSlash(path.Join(milvuscommon.SegmentInsertLogPath, strconv.FormatInt(collectionID, 10)))
	if cfg.rootPath != "" {
		base = withTrailingSlash(path.Join(cfg.rootPath, base))
	}
	return []string{base}
}

func minIOConfigWithServerRootPath(t *testing.T, cfg minioConfig) minioConfig {
	t.Helper()

	rootPath, err := hp.GetServerConfig("minio.rootPath")
	if err != nil {
		t.Logf("Use fallback MinIO root path %q: failed to read Milvus config: %v", cfg.rootPath, err)
		return cfg
	}
	cfg.rootPath = strings.Trim(rootPath, "/")
	t.Logf("Use Milvus MinIO root path %q for persisted segment object checks", cfg.rootPath)
	return cfg
}

func listMinIOObjectsWithPrefix(ctx context.Context, mc *miniogo.Client, bucket, prefix string) ([]string, error) {
	var objects []string
	for obj := range mc.ListObjects(ctx, bucket, miniogo.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		objects = append(objects, obj.Key)
	}
	return objects, nil
}

func findExternalTextIndexObjects(ctx context.Context, mc *miniogo.Client, cfg minioConfig,
	collectionID, segmentID, fieldID int64,
) ([]string, string, error) {
	var lastErr error
	segmentMarker := fmt.Sprintf("/%d/_stats/text_index.%d/", segmentID, fieldID)
	for _, prefix := range externalSegmentPrefixes(cfg, collectionID) {
		objects, err := listMinIOObjectsWithPrefix(ctx, mc, cfg.bucket, prefix)
		if err != nil {
			lastErr = err
			continue
		}
		matched := make([]string, 0)
		for _, object := range objects {
			if strings.Contains(object, segmentMarker) {
				matched = append(matched, object)
			}
		}
		if len(matched) > 0 {
			return matched, prefix, nil
		}
	}
	return nil, "", lastErr
}

func parseExternalSegmentID(objectKey string, collectionID int64) (int64, bool) {
	marker := fmt.Sprintf("%s/%d/", milvuscommon.SegmentInsertLogPath, collectionID)
	idx := strings.Index(objectKey, marker)
	if idx < 0 {
		return 0, false
	}
	rest := objectKey[idx+len(marker):]
	parts := strings.Split(rest, "/")
	if len(parts) < 3 || parts[1] == "" {
		return 0, false
	}
	segmentID, err := strconv.ParseInt(parts[1], 10, 64)
	return segmentID, err == nil
}

func listExternalSegmentIDs(ctx context.Context, mc *miniogo.Client, cfg minioConfig,
	collectionID int64,
) ([]int64, error) {
	segmentSet := make(map[int64]struct{})
	for _, prefix := range externalSegmentPrefixes(cfg, collectionID) {
		objects, err := listMinIOObjectsWithPrefix(ctx, mc, cfg.bucket, prefix)
		if err != nil {
			return nil, err
		}
		for _, object := range objects {
			if !strings.Contains(object, "/_metadata/manifest-") {
				continue
			}
			if segmentID, ok := parseExternalSegmentID(object, collectionID); ok {
				segmentSet[segmentID] = struct{}{}
			}
		}
	}

	segments := make([]int64, 0, len(segmentSet))
	for segmentID := range segmentSet {
		segments = append(segments, segmentID)
	}
	return segments, nil
}

func waitForExternalSegmentIDs(ctx context.Context, t *testing.T, mc *miniogo.Client, cfg minioConfig,
	collectionID int64, timeout time.Duration,
) []int64 {
	t.Helper()

	deadline := time.After(timeout)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		segments, err := listExternalSegmentIDs(ctx, mc, cfg, collectionID)
		require.NoError(t, err)
		if len(segments) > 0 {
			t.Logf("Found external segment IDs for collection %d: %v", collectionID, segments)
			return segments
		}

		select {
		case <-deadline:
			t.Fatalf("Timed out waiting for external segment manifests for collection %d", collectionID)
		case <-ticker.C:
		}
	}
}

func waitForExternalTextIndexObjects(ctx context.Context, t *testing.T, mc *miniogo.Client, cfg minioConfig,
	collectionID, fieldID int64, segments []int64, timeout time.Duration,
) map[int64][]string {
	t.Helper()
	require.NotEmpty(t, segments, "external collection should have persisted segments")

	deadline := time.After(timeout)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		indexObjects := make(map[int64][]string, len(segments))
		var missing []int64
		for _, segment := range segments {
			objects, prefix, err := findExternalTextIndexObjects(ctx, mc, cfg,
				collectionID, segment, fieldID)
			require.NoError(t, err)
			if len(objects) == 0 {
				missing = append(missing, segment)
				continue
			}
			indexObjects[segment] = objects
			t.Logf("Found external text index objects for segment %d under %s: %v",
				segment, prefix, objects)
		}
		if len(missing) == 0 {
			return indexObjects
		}

		t.Logf("Waiting for external text index objects, missing segments: %v", missing)
		select {
		case <-deadline:
			t.Fatalf("Timed out waiting for external text index objects, missing segments: %v", missing)
		case <-ticker.C:
		}
	}
}

func TestExternalTableTextMatch(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	minioCfg := getMinIOConfig()
	minioClient, err := newMinIOClient(minioCfg)
	if err != nil {
		t.Skipf("MinIO unavailable: %v", err)
	}
	skipIfMinIOUnreachable(ctx, t, minioClient, minioCfg.bucket)

	collName := common.GenRandomString("ext_text_match", 6)
	extPath := fmt.Sprintf("external-e2e-test/%s", collName)

	const numFiles, rowsPerFile = 10, int64(5000)
	totalRows := int64(numFiles) * rowsPerFile
	for i := 0; i < numFiles; i++ {
		data, err := generateTextMatchParquetBytes(rowsPerFile, int64(i)*rowsPerFile)
		require.NoError(t, err)
		key := fmt.Sprintf("%s/data%d.parquet", extPath, i)
		uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket, key, data)
	}
	t.Cleanup(func() {
		cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket, extPath+"/")
	})

	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource(extTestURI(minioCfg, extPath)).
		WithExternalSpec(extTestSpec(minioCfg, "parquet")).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).
			WithExternalField("id")).
		WithField(entity.NewField().WithName("text_en").WithDataType(entity.FieldTypeVarChar).
			WithMaxLength(1024).WithExternalField("text_en").
			WithEnableAnalyzer(true).WithAnalyzerParams(map[string]any{"tokenizer": "standard"}).
			WithEnableMatch(true)).
		WithField(entity.NewField().WithName("text_zh").WithDataType(entity.FieldTypeVarChar).
			WithMaxLength(1024).WithExternalField("text_zh").
			WithEnableAnalyzer(true).WithAnalyzerParams(map[string]any{"tokenizer": "jieba"}).
			WithEnableMatch(true)).
		WithField(entity.NewField().WithName("sparse_en").WithDataType(entity.FieldTypeSparseVector)).
		WithField(entity.NewField().WithName("sparse_zh").WithDataType(entity.FieldTypeSparseVector)).
		WithFunction(entity.NewFunction().WithName("bm25_en").
			WithInputFields("text_en").WithOutputFields("sparse_en").WithType(entity.FunctionTypeBM25)).
		WithFunction(entity.NewFunction().WithName("bm25_zh").
			WithInputFields("text_zh").WithOutputFields("sparse_zh").WithType(entity.FunctionTypeBM25))

	err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)
	t.Cleanup(func() {
		_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

	refreshAndWait(ctx, t, mc, collName)

	stats, err := mc.GetCollectionStats(ctx, client.NewGetCollectionStatsOption(collName))
	common.CheckErr(t, err, true)
	rc, _ := strconv.ParseInt(stats["row_count"], 10, 64)
	require.Equal(t, totalRows, rc)

	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	common.CheckErr(t, err, true)
	require.Equal(t, extTestURI(minioCfg, extPath), coll.Schema.ExternalSource)
	textENFieldID := getFieldIDByName(t, coll.Schema, "text_en")
	textZHFieldID := getFieldIDByName(t, coll.Schema, "text_zh")

	minioCfg = minIOConfigWithServerRootPath(t, minioCfg)
	segments := waitForExternalSegmentIDs(ctx, t, minioClient, minioCfg, coll.ID, 30*time.Second)

	sparseIdx := index.NewSparseInvertedIndex(entity.BM25, 0.1)
	idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "sparse_en", sparseIdx))
	common.CheckErr(t, err, true)
	common.CheckErr(t, idxTask.Await(ctx), true)
	idxTask, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "sparse_zh", sparseIdx))
	common.CheckErr(t, err, true)
	common.CheckErr(t, idxTask.Await(ctx), true)

	textENIndexObjects := waitForExternalTextIndexObjects(ctx, t, minioClient, minioCfg,
		coll.ID, textENFieldID, segments, 300*time.Second)
	require.Len(t, textENIndexObjects, len(segments))
	textZHIndexObjects := waitForExternalTextIndexObjects(ctx, t, minioClient, minioCfg,
		coll.ID, textZHFieldID, segments, 300*time.Second)
	require.Len(t, textZHIndexObjects, len(segments))

	waitForExternalTextMatchLoad(ctx, t, mc, collName, 300*time.Second)

	cases := []struct {
		filter      string
		expectedMod int64
	}{
		{`text_match(text_en, "quick brown", minimum_should_match=2)`, 0},
		{`text_match(text_en, "vector databases", minimum_should_match=2)`, 5},
		{`text_match(text_zh, "棕色狐狸", minimum_should_match=2)`, 0},
		{`text_match(text_zh, "向量数据库", minimum_should_match=2)`, 5},
	}
	for _, c := range cases {
		expectedRows := int(totalRows / 10)
		res, err := mc.Query(ctx, client.NewQueryOption(collName).
			WithFilter(c.filter).
			WithOutputFields("id").
			WithLimit(expectedRows))
		common.CheckErr(t, err, true)
		require.Equal(t, expectedRows, res.ResultCount, "filter %s", c.filter)
		idCol, ok := res.GetColumn("id").(*column.ColumnInt64)
		require.True(t, ok, "id column present")
		seen := make(map[int64]struct{}, res.ResultCount)
		for i := 0; i < res.ResultCount; i++ {
			id := idCol.Data()[i]
			require.Equal(t, c.expectedMod, id%10, "filter %s returned id=%d", c.filter, id)
			seen[id] = struct{}{}
		}
		require.Len(t, seen, expectedRows, "filter %s should not return duplicate ids", c.filter)
	}

	t.Logf("External table text match E2E passed with %d rows and persisted text index", totalRows)
}

// TestExternalTableBM25Function tests create, refresh, index, load, query, and search
// for an external table with BM25 function output field.
func TestExternalTableBM25Function(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	minioCfg := getMinIOConfig()
	minioClient, err := newMinIOClient(minioCfg)
	if err != nil {
		t.Skipf("MinIO unavailable: %v", err)
	}
	exists, err := minioClient.BucketExists(ctx, minioCfg.bucket)
	if err != nil || !exists {
		t.Skipf("MinIO bucket %q not accessible", minioCfg.bucket)
	}

	collName := common.GenRandomString("ext_bm25", 6)
	extPath := fmt.Sprintf("external-e2e-test/%s", collName)

	// Step 1: Upload text parquet to MinIO
	const numFiles, rowsPerFile = 10, int64(3000)
	totalRows := int64(numFiles) * rowsPerFile
	for i := 0; i < numFiles; i++ {
		data, err := generateTextParquetBytes(rowsPerFile, int64(i)*rowsPerFile)
		require.NoError(t, err)
		key := fmt.Sprintf("%s/data%d.parquet", extPath, i)
		_, err = minioClient.PutObject(ctx, minioCfg.bucket, key,
			bytes.NewReader(data), int64(len(data)),
			miniogo.PutObjectOptions{ContentType: "application/octet-stream"})
		require.NoError(t, err)
		t.Logf("Uploaded %s (%d rows)", key, rowsPerFile)
	}
	t.Cleanup(func() {
		cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket, extPath+"/")
	})

	// Step 2: Create external collection with PK + BM25 function
	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource(extTestURI(minioCfg, extPath)).
		WithExternalSpec(extTestSpec(minioCfg, "parquet")).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).
			WithExternalField("id")).
		WithField(entity.NewField().WithName("text").WithDataType(entity.FieldTypeVarChar).
			WithMaxLength(1024).WithExternalField("text").
			WithEnableAnalyzer(true).WithAnalyzerParams(map[string]any{"tokenizer": "standard"})).
		WithField(entity.NewField().WithName("sparse_vec").WithDataType(entity.FieldTypeSparseVector)).
		WithFunction(entity.NewFunction().WithName("bm25_fn").
			WithInputFields("text").WithOutputFields("sparse_vec").WithType(entity.FunctionTypeBM25))

	err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)
	t.Logf("Created collection: %s", collName)
	t.Cleanup(func() {
		_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	common.CheckErr(t, err, true)
	require.Equal(t, extTestURI(minioCfg, extPath), coll.Schema.ExternalSource)
	require.Len(t, coll.Schema.Functions, 1)

	// Step 3: Refresh
	refreshResult, err := mc.RefreshExternalCollection(ctx,
		client.NewRefreshExternalCollectionOption(collName))
	common.CheckErr(t, err, true)
	t.Logf("Refresh job: %d", refreshResult.JobID)

	deadline := time.After(180 * time.Second)
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			t.Fatalf("Refresh timed out")
		case <-ticker.C:
			p, err := mc.GetRefreshExternalCollectionProgress(ctx,
				client.NewGetRefreshExternalCollectionProgressOption(refreshResult.JobID))
			require.NoError(t, err)
			t.Logf("Refresh: state=%s", p.State)
			if p.State == entity.RefreshStateCompleted {
				goto refreshDone
			}
			if p.State == entity.RefreshStateFailed {
				t.Fatalf("Refresh failed: %s", p.Reason)
			}
		}
	}
refreshDone:

	stats, err := mc.GetCollectionStats(ctx, client.NewGetCollectionStatsOption(collName))
	common.CheckErr(t, err, true)
	rc, _ := strconv.ParseInt(stats["row_count"], 10, 64)
	require.Equal(t, totalRows, rc)
	t.Logf("Verified row count: %d", rc)

	// Step 4: Create index
	sparseIdx := index.NewSparseInvertedIndex(entity.BM25, 0.1)
	idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "sparse_vec", sparseIdx))
	common.CheckErr(t, err, true)
	err = idxTask.Await(ctx)
	common.CheckErr(t, err, true)
	t.Log("Index created")

	// Step 5: Load collection
	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)
	t.Log("Collection loaded")

	// Step 6: BM25 search; each query should rank the matching text class first.
	// Corpus repeats 10 distinct sentences, so rows where id%10 matches the
	// queried class should dominate the top results.
	cases := []struct {
		query       string
		expectedMod int64
	}{
		{"machine learning", 2},
		{"vector databases", 5},
		{"neural networks", 3},
		{"cloud computing", 7},
	}
	for _, c := range cases {
		res, err := mc.Search(ctx, client.NewSearchOption(collName, 10,
			[]entity.Vector{entity.Text(c.query)}).
			WithANNSField("sparse_vec").
			WithOutputFields("id"))
		common.CheckErr(t, err, true)
		require.Greater(t, len(res), 0)
		hits := res[0]
		require.Greater(t, hits.Len(), 0, "BM25 %q must match", c.query)
		// All top results must belong to the expected text class (id % 10).
		idCol, ok := hits.GetColumn("id").(*column.ColumnInt64)
		require.True(t, ok, "id column present")
		for i := 0; i < hits.Len(); i++ {
			require.Equal(t, c.expectedMod, idCol.Data()[i]%10,
				"query %q hit id=%d (score=%f) should be from class %d",
				c.query, idCol.Data()[i], hits.Scores[i], c.expectedMod)
		}
		t.Logf("BM25 %q -> %d hits, all in class %d (top-5 ids=%v scores=%v)",
			c.query, hits.Len(), c.expectedMod, idCol.Data()[:min(5, hits.Len())],
			hits.Scores[:min(5, hits.Len())])
	}

	// Note: BM25 sparse_vec is intentionally not retrievable by users
	// (CanRetrieveRawFieldData returns false for BM25 function outputs),
	// so the take() fast path for function-output fields is exercised in
	// the MinHash test below (mh_sig is user-visible).

	t.Log("All E2E steps passed: create, refresh, index, load, search")
}

// generateMinHashParquetBytes writes a parquet file with int64 id + varchar
// doc where each row is a phrase with controlled shingle overlap, so MinHash
// signatures give distinguishable Jaccard similarities.
func generateMinHashParquetBytes(numRows int64, startID int64) ([]byte, error) {
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "doc", Type: arrow.BinaryTypes.String},
	}, nil)
	var buf bytes.Buffer
	writer, err := pqarrow.NewFileWriter(arrowSchema, &buf, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, err
	}
	pool := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(pool, arrowSchema)
	defer builder.Release()

	// 5 classes, 5 near-duplicate phrases per class produce high within-class Jaccard.
	classes := [][]string{
		{
			"the quick brown fox jumps over the lazy dog today",
			"the quick brown fox jumps over the lazy dog again",
			"a quick brown fox jumps over the lazy dog today",
			"the quick brown fox leaps over the lazy dog today",
			"the quick brown fox jumps over a lazy dog today",
		},
		{
			"machine learning models process large datasets efficiently",
			"machine learning models process huge datasets efficiently",
			"machine learning systems process large datasets efficiently",
			"machine learning models analyze large datasets efficiently",
			"machine learning models process large datasets quickly",
		},
		{
			"vector databases store and search high dimensional embeddings",
			"vector databases index and search high dimensional embeddings",
			"vector databases store and retrieve high dimensional embeddings",
			"vector databases store and search dense dimensional embeddings",
			"vector databases store and search high quality embeddings",
		},
		{
			"distributed systems handle concurrent requests at massive scale",
			"distributed systems handle parallel requests at massive scale",
			"distributed services handle concurrent requests at massive scale",
			"distributed systems handle concurrent queries at massive scale",
			"distributed systems handle concurrent requests at enormous scale",
		},
		{
			"natural language processing enables text understanding widely",
			"natural language processing enables text analysis widely",
			"natural language processing allows text understanding widely",
			"natural language models enable text understanding widely",
			"natural language processing enables language understanding widely",
		},
	}

	idBuilder := builder.Field(0).(*array.Int64Builder)
	docBuilder := builder.Field(1).(*array.StringBuilder)
	for i := int64(0); i < numRows; i++ {
		classIdx := i % int64(len(classes))
		variantIdx := (i / int64(len(classes))) % int64(len(classes[classIdx]))
		idBuilder.Append(startID + i)
		docBuilder.Append(classes[classIdx][variantIdx])
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

// TestExternalTableMinHashFunction verifies MinHash function output on an
// external collection: refresh computes the MinHash signature over the doc
// field, index + load bring it online, and MINHASH_LSH search with the same
// input phrase ranks same-class rows at the top.
func TestExternalTableMinHashFunction(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	minioCfg := getMinIOConfig()
	minioClient, err := newMinIOClient(minioCfg)
	if err != nil {
		t.Skipf("MinIO unavailable: %v", err)
	}
	exists, err := minioClient.BucketExists(ctx, minioCfg.bucket)
	if err != nil || !exists {
		t.Skipf("MinIO bucket %q not accessible", minioCfg.bucket)
	}

	collName := common.GenRandomString("ext_minhash", 6)
	extPath := fmt.Sprintf("external-e2e-test/%s", collName)

	const numFiles, rowsPerFile = 10, int64(3000)
	totalRows := int64(numFiles) * rowsPerFile
	for i := 0; i < numFiles; i++ {
		data, err := generateMinHashParquetBytes(rowsPerFile, int64(i)*rowsPerFile)
		require.NoError(t, err)
		key := fmt.Sprintf("%s/data%d.parquet", extPath, i)
		_, err = minioClient.PutObject(ctx, minioCfg.bucket, key,
			bytes.NewReader(data), int64(len(data)),
			miniogo.PutObjectOptions{ContentType: "application/octet-stream"})
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket, extPath+"/")
	})

	// MinHash signature vector: num_hashes=16 gives dim=512, stored as BinaryVector.
	const numHashes = 16
	const mhDim = numHashes * 32

	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource(extTestURI(minioCfg, extPath)).
		WithExternalSpec(extTestSpec(minioCfg, "parquet")).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).
			WithExternalField("id")).
		WithField(entity.NewField().WithName("doc").WithDataType(entity.FieldTypeVarChar).
			WithMaxLength(1024).WithExternalField("doc").
			WithEnableAnalyzer(true).WithAnalyzerParams(map[string]any{"tokenizer": "standard"})).
		WithField(entity.NewField().WithName("mh_sig").WithDataType(entity.FieldTypeBinaryVector).
			WithDim(mhDim)).
		WithFunction(entity.NewFunction().WithName("mh_fn").
			WithInputFields("doc").WithOutputFields("mh_sig").
			WithType(schemapb.FunctionType_MinHash).
			WithParam("num_hashes", strconv.Itoa(numHashes)).
			WithParam("shingle_size", "3"))

	err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)
	t.Cleanup(func() {
		_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

	// Refresh
	refreshResult, err := mc.RefreshExternalCollection(ctx,
		client.NewRefreshExternalCollectionOption(collName))
	common.CheckErr(t, err, true)
	deadline := time.After(180 * time.Second)
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			t.Fatalf("Refresh timed out")
		case <-ticker.C:
			p, err := mc.GetRefreshExternalCollectionProgress(ctx,
				client.NewGetRefreshExternalCollectionProgressOption(refreshResult.JobID))
			require.NoError(t, err)
			if p.State == entity.RefreshStateCompleted {
				goto mhRefreshDone
			}
			if p.State == entity.RefreshStateFailed {
				t.Fatalf("Refresh failed: %s", p.Reason)
			}
		}
	}
mhRefreshDone:

	stats, err := mc.GetCollectionStats(ctx, client.NewGetCollectionStatsOption(collName))
	common.CheckErr(t, err, true)
	rc, _ := strconv.ParseInt(stats["row_count"], 10, 64)
	require.Equal(t, totalRows, rc)

	// MINHASH_LSH index with Jaccard metric and lsh_band=4.
	mhIdx := index.NewMinHashLSHIndex(entity.JACCARD, 4).WithRawData(true)
	idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "mh_sig", mhIdx))
	common.CheckErr(t, err, true)
	err = idxTask.Await(ctx)
	common.CheckErr(t, err, true)

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	// Search: pass raw text; server runs MinHash function on it to get query sig.
	queries := []struct {
		text        string
		expectedMod int64
	}{
		{"the quick brown fox jumps over the lazy dog today", 0},
		{"machine learning models process large datasets efficiently", 1},
		{"vector databases store and search high dimensional embeddings", 2},
		{"distributed systems handle concurrent requests at massive scale", 3},
		{"natural language processing enables text understanding widely", 4},
	}
	for _, q := range queries {
		res, err := mc.Search(ctx, client.NewSearchOption(collName, 10,
			[]entity.Vector{entity.Text(q.text)}).
			WithANNSField("mh_sig").
			WithOutputFields("id"))
		common.CheckErr(t, err, true)
		require.Greater(t, len(res), 0)
		hits := res[0]
		require.Greater(t, hits.Len(), 0, "MinHash search %q must match", q.text)
		idCol, ok := hits.GetColumn("id").(*column.ColumnInt64)
		require.True(t, ok)
		// Majority of hits must share the query's class (allow a few LSH false-positives).
		matched := 0
		for i := 0; i < hits.Len(); i++ {
			if idCol.Data()[i]%int64(len(queries)) == q.expectedMod {
				matched++
			}
		}
		require.GreaterOrEqualf(t, matched, hits.Len()/2+1,
			"MinHash %q: only %d/%d hits in class %d",
			q.text, matched, hits.Len(), q.expectedMod)
		t.Logf("MinHash %q -> %d/%d hits in class %d", q.text, matched, hits.Len(), q.expectedMod)
	}

	// Exercise take() fast path for the MinHash signature output.
	takeRes, err := mc.Search(ctx, client.NewSearchOption(collName, 5,
		[]entity.Vector{entity.Text(queries[0].text)}).
		WithANNSField("mh_sig").
		WithOutputFields("id", "mh_sig"))
	common.CheckErr(t, err, true)
	require.Greater(t, len(takeRes), 0)
	sigCol, ok := takeRes[0].GetColumn("mh_sig").(*column.ColumnBinaryVector)
	require.True(t, ok, "mh_sig output column must be present")
	require.Equal(t, takeRes[0].Len(), sigCol.Len())
	require.Greater(t, len(sigCol.Data()), 0)
	t.Logf("take() path returned %d mh_sig rows (%d bytes/row)", sigCol.Len(),
		len(sigCol.Data()[0]))

	t.Log("MinHash E2E passed: create, refresh, index, load, search")
}

// generateDocParquetBytes writes a parquet file with int64 id + varchar doc.
// Rows repeat one of N distinct phrases, so embedding a phrase as the query
// should rank rows from the same phrase class first.
func generateDocParquetBytes(numRows int64, startID int64, phrases []string) ([]byte, error) {
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "doc", Type: arrow.BinaryTypes.String},
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
	docBuilder := builder.Field(1).(*array.StringBuilder)
	for i := int64(0); i < numRows; i++ {
		idBuilder.Append(startID + i)
		docBuilder.Append(phrases[i%int64(len(phrases))])
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

// TestExternalTableTextEmbeddingFunction verifies TextEmbedding function output
// on an external collection: refresh streams docs through the configured TEI
// service, index + load bring it online, and a vector search with the same
// phrase ranks rows of the same class first.
func TestExternalTableTextEmbeddingFunction(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	minioCfg := getMinIOConfig()
	minioClient, err := newMinIOClient(minioCfg)
	if err != nil {
		t.Skipf("MinIO unavailable: %v", err)
	}
	exists, err := minioClient.BucketExists(ctx, minioCfg.bucket)
	if err != nil || !exists {
		t.Skipf("MinIO bucket %q not accessible", minioCfg.bucket)
	}

	phrases := []string{
		"machine learning models process large datasets",
		"vector databases store high dimensional embeddings",
		"distributed systems handle concurrent requests",
		"natural language processing enables text understanding",
		"cloud computing provides elastic infrastructure",
	}
	teiEndpoint := hp.GetTEIEndpoint()
	teiEmbeddings, err := hp.CallTEIDirectly(teiEndpoint, phrases)
	if err != nil {
		t.Skipf("Skip TextEmbedding external-table test: TEI endpoint %s unavailable: %v", teiEndpoint, err)
	}
	require.Len(t, teiEmbeddings, len(phrases))

	dim := hp.GetTEIModelDim()
	require.Equal(t, dim, len(teiEmbeddings[0]), "TEI embedding dimension should match test config")

	collName := common.GenRandomString("ext_tei", 6)
	extPath := fmt.Sprintf("external-e2e-test/%s", collName)

	const numFiles, rowsPerFile = 5, int64(200)
	totalRows := int64(numFiles) * rowsPerFile
	for i := 0; i < numFiles; i++ {
		data, err := generateDocParquetBytes(rowsPerFile, int64(i)*rowsPerFile, phrases)
		require.NoError(t, err)
		key := fmt.Sprintf("%s/data%d.parquet", extPath, i)
		_, err = minioClient.PutObject(ctx, minioCfg.bucket, key,
			bytes.NewReader(data), int64(len(data)),
			miniogo.PutObjectOptions{ContentType: "application/octet-stream"})
		require.NoError(t, err)
		t.Logf("Uploaded %s (%d rows)", key, rowsPerFile)
	}
	t.Cleanup(func() {
		cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket, extPath+"/")
	})

	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource(extTestURI(minioCfg, extPath)).
		WithExternalSpec(extTestSpec(minioCfg, "parquet")).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).
			WithExternalField("id")).
		WithField(entity.NewField().WithName("doc").WithDataType(entity.FieldTypeVarChar).
			WithMaxLength(1024).WithExternalField("doc")).
		WithField(entity.NewField().WithName("dense").WithDataType(entity.FieldTypeFloatVector).
			WithDim(int64(dim))).
		WithFunction(entity.NewFunction().WithName("tei_fn").
			WithInputFields("doc").WithOutputFields("dense").
			WithType(entity.FunctionTypeTextEmbedding).
			WithParam("provider", "TEI").
			WithParam("endpoint", teiEndpoint))

	err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)
	t.Logf("Created collection: %s", collName)
	t.Cleanup(func() {
		_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

	refreshResult, err := mc.RefreshExternalCollection(ctx,
		client.NewRefreshExternalCollectionOption(collName))
	common.CheckErr(t, err, true)
	t.Logf("Refresh job: %d", refreshResult.JobID)

	deadline := time.After(180 * time.Second)
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			t.Fatalf("Refresh timed out")
		case <-ticker.C:
			p, err := mc.GetRefreshExternalCollectionProgress(ctx,
				client.NewGetRefreshExternalCollectionProgressOption(refreshResult.JobID))
			require.NoError(t, err)
			t.Logf("Refresh: state=%s", p.State)
			if p.State == entity.RefreshStateCompleted {
				goto teiRefreshDone
			}
			if p.State == entity.RefreshStateFailed {
				t.Fatalf("Refresh failed: %s", p.Reason)
			}
		}
	}
teiRefreshDone:

	stats, err := mc.GetCollectionStats(ctx, client.NewGetCollectionStatsOption(collName))
	common.CheckErr(t, err, true)
	rc, _ := strconv.ParseInt(stats["row_count"], 10, 64)
	require.Equal(t, totalRows, rc)
	t.Logf("Verified row count: %d", rc)

	idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "dense",
		index.NewFlatIndex(entity.L2)))
	common.CheckErr(t, err, true)
	require.NoError(t, idxTask.Await(ctx))
	t.Log("Index created")

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	require.NoError(t, loadTask.Await(ctx))
	t.Log("Collection loaded")

	// For each class, query with its phrase and verify the top hits share
	// class == id % len(phrases).
	numClasses := int64(len(phrases))
	for classID, phrase := range phrases {
		res, err := mc.Search(ctx, client.NewSearchOption(collName, 10,
			[]entity.Vector{entity.Text(phrase)}).
			WithANNSField("dense").
			WithOutputFields("id"))
		common.CheckErr(t, err, true)
		require.Greater(t, len(res), 0)
		hits := res[0]
		require.Greater(t, hits.Len(), 0)
		idCol, ok := hits.GetColumn("id").(*column.ColumnInt64)
		require.True(t, ok)
		matched := 0
		for i := 0; i < hits.Len(); i++ {
			if idCol.Data()[i]%numClasses == int64(classID) {
				matched++
			}
		}
		// The query phrase is identical to one repeated document phrase, so
		// same-class rows should dominate even with the shared CI TEI model.
		minMatched := hits.Len() / int(numClasses)
		if minMatched < 1 {
			minMatched = 1
		}
		require.GreaterOrEqualf(t, matched, minMatched,
			"TextEmbedding %q: only %d/%d hits in class %d", phrase, matched, hits.Len(), classID)
		t.Logf("TextEmbedding %q -> %d/%d hits in class %d", phrase, matched, hits.Len(), classID)
	}

	// Exercise the take() fast path: retrieve id + dense together. Proxy
	// issues a requery to fetch the dense vector, which exercises
	// VirtualPKChunkedColumn GetAllChunks + take() for function output.
	takeRes, err := mc.Search(ctx, client.NewSearchOption(collName, 5,
		[]entity.Vector{entity.Text(phrases[0])}).
		WithANNSField("dense").
		WithOutputFields("id", "dense"))
	common.CheckErr(t, err, true)
	require.Greater(t, len(takeRes), 0)
	denseCol, ok := takeRes[0].GetColumn("dense").(*column.ColumnFloatVector)
	require.True(t, ok, "dense output column must be present")
	require.Equal(t, takeRes[0].Len(), denseCol.Len())
	require.Greater(t, len(denseCol.Data()), 0)
	require.Equal(t, dim, len(denseCol.Data()[0]))
	idCol := takeRes[0].GetColumn("id").(*column.ColumnInt64)
	matchedDense := 0
	for i := 0; i < denseCol.Len(); i++ {
		if idCol.Data()[i]%int64(len(phrases)) != 0 {
			continue
		}
		got := denseCol.Data()[i]
		require.Greater(t, hp.CosineSimilarity(got, teiEmbeddings[0]), float32(0.99),
			"row %d (id=%d) should carry the TEI embedding for %q", i, idCol.Data()[i], phrases[0])
		matchedDense++
	}
	require.Greater(t, matchedDense, 0, "take() should return at least one hit for %q", phrases[0])
	t.Logf("take() path returned %d dense rows (%d floats/row), values match TEI",
		denseCol.Len(), len(denseCol.Data()[0]))

	t.Log("TextEmbedding E2E passed: create, refresh, index, load, search")
}

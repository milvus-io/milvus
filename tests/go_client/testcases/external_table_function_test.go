package testcases

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	mrand "math/rand"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
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

// TestExternalTableBM25Function tests create → refresh → index → load → query → search
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
		WithExternalSource(extPath).
		WithExternalSpec(`{"format":"parquet"}`).
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
	require.Equal(t, extPath, coll.Schema.ExternalSource)
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

	// Step 6: BM25 search — each query should rank the matching text class first.
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
		t.Logf("BM25 %q → %d hits, all in class %d (top-5 ids=%v scores=%v)",
			c.query, hits.Len(), c.expectedMod, idCol.Data()[:min(5, hits.Len())],
			hits.Scores[:min(5, hits.Len())])
	}

	// Note: BM25 sparse_vec is intentionally not retrievable by users
	// (CanRetrieveRawFieldData returns false for BM25 function outputs),
	// so the take() fast path for function-output fields is exercised in
	// the MinHash test below (mh_sig is user-visible).

	t.Log("All E2E steps passed: create → refresh → index → load → search ✓")
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

	// 5 classes, 5 near-duplicate phrases per class → high within-class Jaccard.
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

	// MinHash signature vector: num_hashes=16 → dim=512, stored as BinaryVector.
	const numHashes = 16
	const mhDim = numHashes * 32

	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource(extPath).
		WithExternalSpec(`{"format":"parquet"}`).
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
		t.Logf("MinHash %q → %d/%d hits in class %d", q.text, matched, hits.Len(), q.expectedMod)
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

	t.Log("MinHash E2E passed: create → refresh → index → load → search ✓")
}

// generateDocParquetBytes writes a parquet file with int64 id + varchar doc,
// where each row repeats one of N distinct phrases. Rows with the same phrase
// share the same deterministic TEI embedding (see mockTEIEmbed), so a
// Top-K vector search with the phrase as the query should hit rows of the
// same class first.
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

// mockTEIEmbed returns a deterministic, length-independent embedding for a
// given phrase. Identical text → identical vector, so two rows with the same
// phrase are collinear and land at distance 0 under L2.
func mockTEIEmbed(text string, dim int) []float32 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(text))
	seed := h.Sum64()
	vec := make([]float32, dim)
	rng := mrand.New(mrand.NewSource(int64(seed)))
	var norm float64
	for i := 0; i < dim; i++ {
		v := rng.NormFloat64()
		vec[i] = float32(v)
		norm += v * v
	}
	norm = math.Sqrt(norm)
	if norm == 0 {
		norm = 1
	}
	for i := range vec {
		vec[i] = float32(float64(vec[i]) / norm)
	}
	return vec
}

// startMockTEIServer spins up an HTTP server implementing the TEI /embed API
// on 127.0.0.1:<random>. Returns the endpoint URL and a stop func.
func startMockTEIServer(t *testing.T, dim int) (endpoint string, stop func()) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	mux := http.NewServeMux()
	mux.HandleFunc("/embed", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer r.Body.Close()
		var req struct {
			Inputs []string `json:"inputs"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		out := make([][]float32, len(req.Inputs))
		for i, s := range req.Inputs {
			out[i] = mockTEIEmbed(s, dim)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(ln) }()

	// Milvus runs as a native process on this host in the local E2E setup,
	// so loopback is reachable directly. (Dockerised Milvus would need
	// host.docker.internal on macOS or host-network on Linux.)
	endpoint = fmt.Sprintf("http://127.0.0.1:%d", ln.Addr().(*net.TCPAddr).Port)
	t.Logf("mock TEI listening on %s", endpoint)

	stop = func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}
	return endpoint, stop
}

// TestExternalTableTextEmbeddingFunction verifies TextEmbedding function output
// on an external collection: refresh streams docs through a mock TEI server to
// compute the dense column, index + load bring it online, and a vector search
// with the same phrase ranks rows of the same class first.
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

	const dim = 64
	teiEndpoint, stopTEI := startMockTEIServer(t, dim)
	defer stopTEI()

	phrases := []string{
		"machine learning models process large datasets",
		"vector databases store high dimensional embeddings",
		"distributed systems handle concurrent requests",
		"natural language processing enables text understanding",
		"cloud computing provides elastic infrastructure",
	}

	collName := common.GenRandomString("ext_tei", 6)
	extPath := fmt.Sprintf("external-e2e-test/%s", collName)

	const numFiles, rowsPerFile = 10, int64(3000)
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
		WithExternalSource(extPath).
		WithExternalSpec(`{"format":"parquet"}`).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).
			WithExternalField("id")).
		WithField(entity.NewField().WithName("doc").WithDataType(entity.FieldTypeVarChar).
			WithMaxLength(1024).WithExternalField("doc")).
		WithField(entity.NewField().WithName("dense").WithDataType(entity.FieldTypeFloatVector).
			WithDim(dim)).
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
		// Deterministic embedding → same-class rows cluster at distance 0,
		// so at least Top-K/classes hits must match exactly.
		minMatched := hits.Len() / int(numClasses)
		if minMatched < 1 {
			minMatched = 1
		}
		require.GreaterOrEqualf(t, matched, minMatched,
			"TextEmbedding %q: only %d/%d hits in class %d", phrase, matched, hits.Len(), classID)
		t.Logf("TextEmbedding %q → %d/%d hits in class %d", phrase, matched, hits.Len(), classID)
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
	// Verify deterministic embedding: every hit for phrases[0] must carry
	// exactly the vector mockTEIEmbed(phrases[0]).
	expected := mockTEIEmbed(phrases[0], dim)
	idCol := takeRes[0].GetColumn("id").(*column.ColumnInt64)
	for i := 0; i < denseCol.Len(); i++ {
		if idCol.Data()[i]%int64(len(phrases)) != 0 {
			continue
		}
		got := denseCol.Data()[i]
		for j := 0; j < dim; j++ {
			require.InDelta(t, expected[j], got[j], 1e-5,
				"row %d (id=%d) dim %d", i, idCol.Data()[i], j)
		}
	}
	t.Logf("take() path returned %d dense rows (%d floats/row), values match mock TEI",
		denseCol.Len(), len(denseCol.Data()[0]))

	t.Log("TextEmbedding E2E passed: create → refresh → index → load → search ✓")
}

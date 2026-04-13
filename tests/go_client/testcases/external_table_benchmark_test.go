package testcases

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// ============================================================================
// Benchmark Configuration
// ============================================================================

type scaleConfig struct {
	TotalRows   int64
	NumFiles    int
	RowsPerFile int64
}

var scalePresets = map[string]scaleConfig{
	"100k": {TotalRows: 100_000, NumFiles: 10, RowsPerFile: 10_000},
	"500k": {TotalRows: 500_000, NumFiles: 10, RowsPerFile: 50_000},
	"1m":   {TotalRows: 1_000_000, NumFiles: 10, RowsPerFile: 100_000},
	"10m":  {TotalRows: 10_000_000, NumFiles: 100, RowsPerFile: 100_000},
	"100m": {TotalRows: 100_000_000, NumFiles: 1000, RowsPerFile: 100_000},
}

type benchConfig struct {
	Scale           string
	TotalRows       int64
	NumFiles        int
	RowsPerFile     int64
	VecDim          int
	TopK            int
	QueryIterations int
	Concurrency     int
}

// loadBenchScales parses BENCH_SCALE which supports comma-separated values.
// e.g. BENCH_SCALE=small,medium,large
func loadBenchScales() []string {
	scaleStr := envOrDefault("BENCH_SCALE", "100k")
	parts := strings.Split(scaleStr, ",")
	var scales []string
	for _, s := range parts {
		s = strings.TrimSpace(s)
		if _, ok := scalePresets[s]; ok {
			scales = append(scales, s)
		}
	}
	if len(scales) == 0 {
		return []string{"100k"}
	}
	return scales
}

func loadBenchConfigForScale(scale string) benchConfig {
	preset, ok := scalePresets[scale]
	if !ok {
		preset = scalePresets["100k"]
	}

	cfg := benchConfig{
		Scale:           scale,
		TotalRows:       preset.TotalRows,
		NumFiles:        preset.NumFiles,
		RowsPerFile:     preset.RowsPerFile,
		VecDim:          benchEnvInt("BENCH_VEC_DIM", testVecDim),
		TopK:            benchEnvInt("BENCH_TOPK", 10),
		QueryIterations: benchEnvInt("BENCH_QUERY_ITERATIONS", 10),
		Concurrency:     benchEnvInt("BENCH_CONCURRENCY", 1),
	}

	if v := os.Getenv("BENCH_TOTAL_ROWS"); v != "" {
		cfg.TotalRows, _ = strconv.ParseInt(v, 10, 64)
	}
	if v := os.Getenv("BENCH_NUM_FILES"); v != "" {
		cfg.NumFiles, _ = strconv.Atoi(v)
	}
	if cfg.NumFiles > 0 {
		cfg.RowsPerFile = cfg.TotalRows / int64(cfg.NumFiles)
	}

	return cfg
}

func benchEnvInt(key string, defaultVal int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return defaultVal
}

func (c benchConfig) String() string {
	return fmt.Sprintf("scale=%s rows=%d files=%d rowsPerFile=%d dim=%d topK=%d iterations=%d concurrency=%d",
		c.Scale, c.TotalRows, c.NumFiles, c.RowsPerFile, c.VecDim, c.TopK, c.QueryIterations, c.Concurrency)
}

// ============================================================================
// Multi-Scale Test Runners
// ============================================================================

// forEachScale runs fn for each configured scale, creating subtests.
func forEachScale(t *testing.T, fn func(t *testing.T, env *benchEnv)) {
	scales := loadBenchScales()
	for _, scale := range scales {
		scale := scale
		t.Run("scale_"+scale, func(t *testing.T) {
			cfg := loadBenchConfigForScale(scale)
			globalReport.addConfig(scale, cfg)
			globalReport.setCurrentScale(scale)
			t.Logf("=== Scale: %s Config: %s ===", scale, cfg)
			env := newBenchEnvWithConfig(t, cfg)
			fn(t, env)
		})
	}
}

// forEachScaleConfig runs fn for each configured scale without creating a benchEnv.
func forEachScaleConfig(t *testing.T, fn func(t *testing.T, cfg benchConfig)) {
	scales := loadBenchScales()
	for _, scale := range scales {
		scale := scale
		t.Run("scale_"+scale, func(t *testing.T) {
			cfg := loadBenchConfigForScale(scale)
			globalReport.addConfig(scale, cfg)
			globalReport.setCurrentScale(scale)
			t.Logf("=== Scale: %s Config: %s ===", scale, cfg)
			fn(t, cfg)
		})
	}
}

// ============================================================================
// Latency Measurement & Report
// ============================================================================

type latencyStats struct {
	Name    string
	Count   int
	Min     time.Duration
	Max     time.Duration
	Mean    time.Duration
	P50     time.Duration
	P95     time.Duration
	P99     time.Duration
	Stddev  time.Duration
	Samples []time.Duration
}

// benchReport collects all benchmark results and generates a Markdown report.
type benchReport struct {
	mu           sync.Mutex
	entries      []reportEntry
	configs      map[string]benchConfig // scale -> config
	currentScale string
	started      time.Time
}

type reportEntry struct {
	TestName string
	Scale    string
	Stats    latencyStats
	Extra    string // optional extra info (e.g. throughput)
}

var globalReport = &benchReport{
	started: time.Now(),
	configs: make(map[string]benchConfig),
}

func (r *benchReport) add(testName string, stats latencyStats, extra string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, reportEntry{
		TestName: testName,
		Scale:    r.currentScale,
		Stats:    stats,
		Extra:    extra,
	})
}

func (r *benchReport) setCurrentScale(scale string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.currentScale = scale
}

func (r *benchReport) addConfig(scale string, cfg benchConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.configs[scale] = cfg
}

func fmtDuration(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%.1fus", float64(d)/float64(time.Microsecond))
	}
	if d < time.Second {
		return fmt.Sprintf("%.1fms", float64(d)/float64(time.Millisecond))
	}
	return fmt.Sprintf("%.2fs", float64(d)/float64(time.Second))
}

// writeMarkdownReport generates a Markdown report file.
// When multiple scales are configured, it generates a comparison table with scalability ratios.
func (r *benchReport) writeMarkdownReport(t *testing.T) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.entries) == 0 {
		t.Log("No benchmark entries collected, skipping report generation")
		return
	}

	// Collect all scales that actually have data
	scaleSet := make(map[string]bool)
	for _, e := range r.entries {
		if e.Scale != "" {
			scaleSet[e.Scale] = true
		}
	}
	var scales []string
	for s := range scaleSet {
		scales = append(scales, s)
	}
	sort.Slice(scales, func(i, j int) bool {
		return r.configs[scales[i]].TotalRows < r.configs[scales[j]].TotalRows
	})
	multiScale := len(scales) > 1

	reportDir := envOrDefault("BENCH_REPORT_DIR", ".")
	scaleLabel := strings.Join(scales, "_")
	ts := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("%s/bench_report_%s_%s.md", reportDir, scaleLabel, ts)

	var buf bytes.Buffer
	buf.WriteString("# External Table Benchmark Report\n\n")
	buf.WriteString(fmt.Sprintf("**Date:** %s\n\n", time.Now().Format("2006-01-02 15:04:05")))
	buf.WriteString(fmt.Sprintf("**Scales:** %s\n\n", strings.Join(scales, ", ")))
	buf.WriteString(fmt.Sprintf("**Total Duration:** %s\n\n", fmtDuration(time.Since(r.started))))

	// Configuration table — one column per scale
	buf.WriteString("## Configuration\n\n")
	if multiScale {
		buf.WriteString("| Parameter |")
		for _, s := range scales {
			buf.WriteString(fmt.Sprintf(" %s |", s))
		}
		buf.WriteString("\n|-----------|")
		for range scales {
			buf.WriteString("-------|")
		}
		buf.WriteString("\n")
		for _, param := range []string{"Total Rows", "Num Files", "Rows/File", "Vec Dim", "TopK", "Iterations", "Concurrency"} {
			buf.WriteString(fmt.Sprintf("| %s |", param))
			for _, s := range scales {
				cfg := r.configs[s]
				var val string
				switch param {
				case "Total Rows":
					val = fmt.Sprintf("%d", cfg.TotalRows)
				case "Num Files":
					val = fmt.Sprintf("%d", cfg.NumFiles)
				case "Rows/File":
					val = fmt.Sprintf("%d", cfg.RowsPerFile)
				case "Vec Dim":
					val = fmt.Sprintf("%d", cfg.VecDim)
				case "TopK":
					val = fmt.Sprintf("%d", cfg.TopK)
				case "Iterations":
					val = fmt.Sprintf("%d", cfg.QueryIterations)
				case "Concurrency":
					val = fmt.Sprintf("%d", cfg.Concurrency)
				}
				buf.WriteString(fmt.Sprintf(" %s |", val))
			}
			buf.WriteString("\n")
		}
	} else {
		cfg := r.configs[scales[0]]
		buf.WriteString("| Parameter | Value |\n")
		buf.WriteString("|-----------|-------|\n")
		buf.WriteString(fmt.Sprintf("| Total Rows | %d |\n", cfg.TotalRows))
		buf.WriteString(fmt.Sprintf("| Num Files | %d |\n", cfg.NumFiles))
		buf.WriteString(fmt.Sprintf("| Rows/File | %d |\n", cfg.RowsPerFile))
		buf.WriteString(fmt.Sprintf("| Vec Dim | %d |\n", cfg.VecDim))
		buf.WriteString(fmt.Sprintf("| TopK | %d |\n", cfg.TopK))
		buf.WriteString(fmt.Sprintf("| Query Iterations | %d |\n", cfg.QueryIterations))
		buf.WriteString(fmt.Sprintf("| Concurrency | %d |\n", cfg.Concurrency))
	}
	buf.WriteString("\n")

	// Benchmark descriptions
	buf.WriteString("## Benchmark Descriptions\n\n")
	buf.WriteString("| ID | Name | Description |\n")
	buf.WriteString("|:--:|------|-------------|\n")
	buf.WriteString("| B1 | CreateCollection | Create external collection (simple & multi-type schema) |\n")
	buf.WriteString("| B2 | Refresh | Full refresh: discover all Parquet files and create segments |\n")
	buf.WriteString("| B3 | IncrementalRefresh | Add a new file then refresh; measures delta detection cost |\n")
	buf.WriteString("| B4 | IndexBuilding | Build scalar (id, value) and HNSW vector indexes |\n")
	buf.WriteString("| B5 | LoadCollection | Load collection into memory (release then re-load) |\n")
	buf.WriteString("| B6 | CountQuery | `count(*)` query |\n")
	buf.WriteString("| B7 | ScalarFilter | Scalar filter at various selectivities (1 row / 1% / 10% / 25%) |\n")
	buf.WriteString("| B8 | VectorSearch | ANN search with varying topK (5 / 10 / 50 / 100), output `id` only |\n")
	buf.WriteString("| B9 | HybridSearch | Vector search + scalar range filter (10% / 50% selectivity) |\n")
	buf.WriteString("| B10 | MultiVectorSearch | Batch vector search with nq = 1 / 5 / 10 / 50 |\n")
	buf.WriteString("| B11 | Pagination | Search with offset 0 / 100 / 1K / 5K / 10K, limit 100 |\n")
	buf.WriteString("| B12 | OutputFields | Query returning different field combos (id / scalar / vector / all) |\n")
	buf.WriteString("| B13 | ExternalVsRegular | Same queries on external table vs regular table side-by-side |\n")
	buf.WriteString("| B15 | ManyFiles | Refresh with 100 small files (same total rows) |\n")
	buf.WriteString("| B16 | LargeSingleFile | Refresh with 1 large file (same total rows) |\n")
	buf.WriteString("| B17 | ConcurrentSearch | Concurrent vector search throughput (ops/s) |\n")
	buf.WriteString("| B19 | IdempotentRefresh | Consecutive refreshes with no data change |\n")
	buf.WriteString("| B20 | MultiDataTypes | 9-field schema (Bool/Int/Float/VarChar/Vec) refresh + queries |\n")
	buf.WriteString("| B21 | UseTakeForOutput | Compare bulkLoad (useTakeForOutput=false) vs take (=true): LoadCollection + cold search topK=5 all fields |\n")
	buf.WriteString("\n")
	buf.WriteString("> **Note:** All query benchmarks (B6-B13, B17, B20) are **cold queries** — no warmup round is discarded. ")
	buf.WriteString("Output fetch path is controlled by `queryNode.externalCollection.useTakeForOutput` (default: `true`). ")
	buf.WriteString("B21 explicitly tests both values.\n\n")

	// Per-scale detailed results
	for _, scale := range scales {
		if multiScale {
			cfg := r.configs[scale]
			buf.WriteString(fmt.Sprintf("## Results — %s (%d rows)\n\n", scale, cfg.TotalRows))
		} else {
			buf.WriteString("## Results\n\n")
		}
		buf.WriteString("| Benchmark | Iterations | Min | Mean | P50 | P95 | P99 | Max | Stddev | Extra |\n")
		buf.WriteString("|-----------|-----------|-----|------|-----|-----|-----|-----|--------|-------|\n")
		for _, e := range r.entries {
			if e.Scale != scale {
				continue
			}
			s := e.Stats
			buf.WriteString(fmt.Sprintf("| %s | %d | %s | %s | %s | %s | %s | %s | %s | %s |\n",
				s.Name, s.Count,
				fmtDuration(s.Min), fmtDuration(s.Mean), fmtDuration(s.P50),
				fmtDuration(s.P95), fmtDuration(s.P99), fmtDuration(s.Max),
				fmtDuration(s.Stddev), e.Extra))
		}
		buf.WriteString("\n")
	}

	// Multi-scale comparison & scalability analysis (combined table)
	if multiScale {
		buf.WriteString("## Scale Comparison & Scalability Analysis\n\n")

		smallestScale := scales[0]
		largestScale := scales[len(scales)-1]
		dataRatio := float64(r.configs[largestScale].TotalRows) / float64(r.configs[smallestScale].TotalRows)
		buf.WriteString(fmt.Sprintf("**Data scale ratio** (%s → %s): **%.1fx** (%d → %d rows)\n\n",
			smallestScale, largestScale, dataRatio,
			r.configs[smallestScale].TotalRows, r.configs[largestScale].TotalRows))

		// Collect unique benchmark names in order
		var benchNames []string
		seen := make(map[string]bool)
		for _, e := range r.entries {
			if !seen[e.Stats.Name] {
				benchNames = append(benchNames, e.Stats.Name)
				seen[e.Stats.Name] = true
			}
		}

		// Build lookup: benchName -> scale -> entry
		lookup := make(map[string]map[string]*reportEntry)
		for i := range r.entries {
			e := &r.entries[i]
			if _, ok := lookup[e.Stats.Name]; !ok {
				lookup[e.Stats.Name] = make(map[string]*reportEntry)
			}
			lookup[e.Stats.Name][e.Scale] = e
		}

		// Header
		buf.WriteString("| Benchmark |")
		for _, s := range scales {
			buf.WriteString(fmt.Sprintf(" %s (Mean) |", s))
		}
		buf.WriteString(" Ratio | Scaling | Assessment |\n")
		buf.WriteString("|-----------|")
		for range scales {
			buf.WriteString("------------|")
		}
		buf.WriteString(":---:|:---:|:---:|\n")

		// Rows
		for _, name := range benchNames {
			buf.WriteString(fmt.Sprintf("| %s |", name))
			var firstMean, lastMean time.Duration
			for i, s := range scales {
				if e, ok := lookup[name][s]; ok {
					buf.WriteString(fmt.Sprintf(" %s |", fmtDuration(e.Stats.Mean)))
					if i == 0 {
						firstMean = e.Stats.Mean
					}
					lastMean = e.Stats.Mean
				} else {
					buf.WriteString(" - |")
				}
			}
			if firstMean > 0 {
				latRatio := float64(lastMean) / float64(firstMean)
				var scaling, assessment string
				switch {
				case latRatio <= 1.2:
					scaling = "O(1)"
					assessment = "Excellent"
				case latRatio <= dataRatio*0.5:
					scaling = "Sub-linear"
					assessment = "Good"
				case latRatio <= dataRatio*1.2:
					scaling = "Linear"
					assessment = "Expected"
				default:
					scaling = "Super-linear"
					assessment = "Needs investigation"
				}
				buf.WriteString(fmt.Sprintf(" %.2fx | %s | %s |", latRatio, scaling, assessment))
			} else {
				buf.WriteString(" - | - | - |")
			}
			buf.WriteString("\n")
		}
		buf.WriteString("\n")
	}

	buf.WriteString("\n## Summary\n\n")
	buf.WriteString(fmt.Sprintf("- **Total benchmarks:** %d\n", len(r.entries)))
	buf.WriteString(fmt.Sprintf("- **Scales tested:** %s\n", strings.Join(scales, ", ")))
	buf.WriteString("- **Generated by:** `go test -run TestBenchmarkExternal`\n")

	if err := os.WriteFile(filename, buf.Bytes(), 0o600); err != nil {
		t.Logf("WARNING: failed to write report to %s: %v", filename, err)
		t.Logf("\n%s", buf.String())
		return
	}
	t.Logf("=== Benchmark report written to: %s ===", filename)
}

func measureLatency(t *testing.T, name string, iterations int, fn func()) latencyStats {
	t.Helper()
	samples := make([]time.Duration, 0, iterations)
	for i := 0; i < iterations; i++ {
		start := time.Now()
		fn()
		samples = append(samples, time.Since(start))
	}
	stats := computeStats(name, samples)
	t.Logf("=== %s === iterations=%d min=%v max=%v mean=%v p50=%v p95=%v p99=%v stddev=%v",
		name, stats.Count, stats.Min, stats.Max, stats.Mean, stats.P50, stats.P95, stats.P99, stats.Stddev)
	globalReport.add(t.Name(), stats, "")
	return stats
}

func recordSingleMeasurement(t *testing.T, name string, duration time.Duration, extra string) {
	t.Helper()
	stats := computeStats(name, []time.Duration{duration})
	globalReport.add(t.Name(), stats, extra)
}

func computeStats(name string, samples []time.Duration) latencyStats {
	n := len(samples)
	if n == 0 {
		return latencyStats{Name: name}
	}
	sorted := make([]time.Duration, n)
	copy(sorted, samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var sum float64
	for _, s := range sorted {
		sum += float64(s)
	}
	mean := sum / float64(n)

	var variance float64
	for _, s := range sorted {
		diff := float64(s) - mean
		variance += diff * diff
	}
	variance /= float64(n)

	return latencyStats{
		Name:    name,
		Count:   n,
		Min:     sorted[0],
		Max:     sorted[n-1],
		Mean:    time.Duration(mean),
		P50:     sorted[int(float64(n)*0.50)],
		P95:     sorted[int(math.Min(float64(n)*0.95, float64(n-1)))],
		P99:     sorted[int(math.Min(float64(n)*0.99, float64(n-1)))],
		Stddev:  time.Duration(math.Sqrt(variance)),
		Samples: samples,
	}
}

// ============================================================================
// Parquet Generation with Configurable Dimension
// ============================================================================

func generateParquetBytesWithDim(numRows, startID int64, dim int) ([]byte, error) {
	// Milvus expects FloatVector stored as FixedSizeBinary(dim*4) in Parquet,
	// not as FixedSizeList<Float32>.
	vecByteWidth := dim * 4
	arrowSchema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "value", Type: arrow.PrimitiveTypes.Float32},
			{Name: "embedding", Type: &arrow.FixedSizeBinaryType{ByteWidth: vecByteWidth}},
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
	embeddingBuilder := builder.Field(2).(*array.FixedSizeBinaryBuilder)

	vecBuf := make([]byte, vecByteWidth)
	for i := int64(0); i < numRows; i++ {
		idx := startID + i
		idBuilder.Append(idx)
		valueBuilder.Append(float32(idx) * 1.5)
		for d := 0; d < dim; d++ {
			binary.LittleEndian.PutUint32(vecBuf[d*4:], math.Float32bits(float32(idx)*0.1+float32(d)))
		}
		embeddingBuilder.Append(vecBuf)
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

// ============================================================================
// Benchmark Setup Helpers
// ============================================================================

type benchEnv struct {
	ctx         context.Context
	mc          *base.MilvusClient
	minioClient *miniogo.Client
	minioCfg    minioConfig
	cfg         benchConfig
}

func newBenchEnvWithConfig(t *testing.T, cfg benchConfig) *benchEnv {
	t.Helper()
	t.Logf("=== Benchmark Config: %s ===", cfg)

	ctx := hp.CreateContext(t, time.Second*3600) // 1h max
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	minioCfg := getMinIOConfig()
	minioClient, err := newMinIOClient(minioCfg)
	if err != nil {
		t.Skipf("Failed to create MinIO client: %v", err)
	}

	exists, err := minioClient.BucketExists(ctx, minioCfg.bucket)
	if err != nil || !exists {
		t.Skipf("MinIO bucket %q not accessible", minioCfg.bucket)
	}

	return &benchEnv{ctx: ctx, mc: mc, minioClient: minioClient, minioCfg: minioCfg, cfg: cfg}
}

// setupExternalCollection creates an external collection with data uploaded to MinIO,
// refreshed, indexed (scalar + vector), and loaded. Returns collection name.
func (env *benchEnv) setupExternalCollection(t *testing.T) string {
	t.Helper()
	return env.setupExternalCollectionWithOptions(t, env.cfg.NumFiles, env.cfg.RowsPerFile, env.cfg.VecDim)
}

func (env *benchEnv) setupExternalCollectionWithOptions(t *testing.T, numFiles int, rowsPerFile int64, dim int) string {
	t.Helper()
	collName := common.GenRandomString("bench_ext", 6)
	extPath := fmt.Sprintf("external-bench/%s", collName)
	totalRows := int64(numFiles) * rowsPerFile

	// Upload Parquet files
	t.Logf("Uploading %d Parquet files (%d rows each, dim=%d)...", numFiles, rowsPerFile, dim)
	uploadStart := time.Now()
	for i := 0; i < numFiles; i++ {
		data, err := generateParquetBytesWithDim(rowsPerFile, int64(i)*rowsPerFile, dim)
		require.NoError(t, err, "generate parquet file %d", i)
		objectKey := fmt.Sprintf("%s/data%d.parquet", extPath, i)
		uploadParquetToMinIO(env.ctx, t, env.minioClient, env.minioCfg.bucket, objectKey, data)
	}
	t.Logf("Upload complete: %v", time.Since(uploadStart))

	t.Cleanup(func() {
		cleanupMinIOPrefix(context.Background(), env.minioClient, env.minioCfg.bucket, extPath+"/")
		_ = env.mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

	// Create external collection
	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource(extPath).
		WithExternalSpec(`{"format":"parquet"}`).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
		WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value")).
		WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(int64(dim)).WithExternalField("embedding"))

	err := env.mc.CreateCollection(env.ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)

	// Refresh
	refreshAndWait(env.ctx, t, env.mc, collName)

	// Index + Load
	indexAndLoadCollectionWithScalarAndVector(env.ctx, t, env.mc, collName, totalRows)

	return collName
}

func (env *benchEnv) makeQueryVec() []float32 {
	vec := make([]float32, env.cfg.VecDim)
	for i := range vec {
		vec[i] = float32(i) * 0.1
	}
	return vec
}

func (env *benchEnv) makeRandomQueryVec() []float32 {
	vec := make([]float32, env.cfg.VecDim)
	for i := range vec {
		vec[i] = rand.Float32()
	}
	return vec
}

// ============================================================================
// B1: CreateCollection Latency
// ============================================================================

func TestBenchmarkExternalCreateCollection(t *testing.T) {
	forEachScaleConfig(t, func(t *testing.T, cfg benchConfig) {
		ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
		mc := hp.CreateDefaultMilvusClient(ctx, t)

		measureLatency(t, "B1_CreateCollection_SimpleSchema", cfg.QueryIterations, func() {
			collName := common.GenRandomString("bench_create", 6)
			schema := entity.NewSchema().
				WithName(collName).
				WithExternalSource("s3://test-bucket/bench/").
				WithExternalSpec(`{"format":"parquet"}`).
				WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
				WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(int64(cfg.VecDim)).WithExternalField("embedding"))

			err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
			common.CheckErr(t, err, true)

			_ = mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
		})

		measureLatency(t, "B1_CreateCollection_MultiTypeSchema", cfg.QueryIterations, func() {
			collName := common.GenRandomString("bench_create_mt", 6)
			schema := entity.NewSchema().
				WithName(collName).
				WithExternalSource("s3://test-bucket/bench/").
				WithExternalSpec(`{"format":"parquet"}`).
				WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
				WithField(entity.NewField().WithName("bool_val").WithDataType(entity.FieldTypeBool).WithExternalField("bool_val")).
				WithField(entity.NewField().WithName("int8_val").WithDataType(entity.FieldTypeInt8).WithExternalField("int8_val")).
				WithField(entity.NewField().WithName("int16_val").WithDataType(entity.FieldTypeInt16).WithExternalField("int16_val")).
				WithField(entity.NewField().WithName("int32_val").WithDataType(entity.FieldTypeInt32).WithExternalField("int32_val")).
				WithField(entity.NewField().WithName("float_val").WithDataType(entity.FieldTypeFloat).WithExternalField("float_val")).
				WithField(entity.NewField().WithName("double_val").WithDataType(entity.FieldTypeDouble).WithExternalField("double_val")).
				WithField(entity.NewField().WithName("varchar_val").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64).WithExternalField("varchar_val")).
				WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(int64(cfg.VecDim)).WithExternalField("embedding"))

			err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
			common.CheckErr(t, err, true)

			_ = mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
		})
	})
}

// ============================================================================
// B2: RefreshExternalCollection Latency
// ============================================================================

func TestBenchmarkExternalRefresh(t *testing.T) {
	forEachScale(t, func(t *testing.T, env *benchEnv) {
		testBenchmarkExternalRefresh(t, env)
	})
}

func testBenchmarkExternalRefresh(t *testing.T, env *benchEnv) {
	collName := common.GenRandomString("bench_refresh", 6)
	extPath := fmt.Sprintf("external-bench/%s", collName)

	// Upload data
	t.Logf("Uploading %d files (%d rows each)...", env.cfg.NumFiles, env.cfg.RowsPerFile)
	for i := 0; i < env.cfg.NumFiles; i++ {
		data, err := generateParquetBytesWithDim(env.cfg.RowsPerFile, int64(i)*env.cfg.RowsPerFile, env.cfg.VecDim)
		require.NoError(t, err)
		objectKey := fmt.Sprintf("%s/data%d.parquet", extPath, i)
		uploadParquetToMinIO(env.ctx, t, env.minioClient, env.minioCfg.bucket, objectKey, data)
	}

	t.Cleanup(func() {
		cleanupMinIOPrefix(context.Background(), env.minioClient, env.minioCfg.bucket, extPath+"/")
		_ = env.mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

	// Create collection
	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource(extPath).
		WithExternalSpec(`{"format":"parquet"}`).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
		WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value")).
		WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(int64(env.cfg.VecDim)).WithExternalField("embedding"))

	err := env.mc.CreateCollection(env.ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)

	// Measure refresh
	start := time.Now()
	refreshAndWait(env.ctx, t, env.mc, collName)
	refreshDuration := time.Since(start)
	t.Logf("=== B2_Refresh === rows=%d duration=%v", env.cfg.TotalRows, refreshDuration)
	recordSingleMeasurement(t, "B2_Refresh", refreshDuration, fmt.Sprintf("rows=%d", env.cfg.TotalRows))

	// Verify row count
	stats, err := env.mc.GetCollectionStats(env.ctx, client.NewGetCollectionStatsOption(collName))
	common.CheckErr(t, err, true)
	rowCountStr := stats["row_count"]
	rowCount, _ := strconv.ParseInt(rowCountStr, 10, 64)
	require.Equal(t, env.cfg.TotalRows, rowCount)
}

// ============================================================================
// B3: Incremental Refresh Performance
// ============================================================================

func TestBenchmarkExternalIncrementalRefresh(t *testing.T) {
	forEachScale(t, benchIncrementalRefresh)
}

func benchIncrementalRefresh(t *testing.T, env *benchEnv) {
	collName := common.GenRandomString("bench_incr", 6)
	extPath := fmt.Sprintf("external-bench/%s", collName)

	// Upload initial data (use half of configured files)
	initialFiles := env.cfg.NumFiles / 2
	if initialFiles < 1 {
		initialFiles = 1
	}

	for i := 0; i < initialFiles; i++ {
		data, err := generateParquetBytesWithDim(env.cfg.RowsPerFile, int64(i)*env.cfg.RowsPerFile, env.cfg.VecDim)
		require.NoError(t, err)
		objectKey := fmt.Sprintf("%s/data%d.parquet", extPath, i)
		uploadParquetToMinIO(env.ctx, t, env.minioClient, env.minioCfg.bucket, objectKey, data)
	}

	t.Cleanup(func() {
		cleanupMinIOPrefix(context.Background(), env.minioClient, env.minioCfg.bucket, extPath+"/")
		_ = env.mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource(extPath).
		WithExternalSpec(`{"format":"parquet"}`).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
		WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value")).
		WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(int64(env.cfg.VecDim)).WithExternalField("embedding"))

	err := env.mc.CreateCollection(env.ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)

	// Initial refresh
	start := time.Now()
	refreshAndWait(env.ctx, t, env.mc, collName)
	initialRefreshDuration := time.Since(start)
	t.Logf("=== B3_InitialRefresh === files=%d rows=%d duration=%v",
		initialFiles, int64(initialFiles)*env.cfg.RowsPerFile, initialRefreshDuration)
	recordSingleMeasurement(t, "B3_InitialRefresh", initialRefreshDuration,
		fmt.Sprintf("files=%d rows=%d", initialFiles, int64(initialFiles)*env.cfg.RowsPerFile))

	// Add one more file
	newFileIdx := initialFiles
	data, err := generateParquetBytesWithDim(env.cfg.RowsPerFile, int64(newFileIdx)*env.cfg.RowsPerFile, env.cfg.VecDim)
	require.NoError(t, err)
	objectKey := fmt.Sprintf("%s/data%d.parquet", extPath, newFileIdx)
	uploadParquetToMinIO(env.ctx, t, env.minioClient, env.minioCfg.bucket, objectKey, data)

	// Incremental refresh (add file)
	start = time.Now()
	refreshAndWait(env.ctx, t, env.mc, collName)
	incrAddDuration := time.Since(start)
	t.Logf("=== B3_IncrementalRefresh_AddFile === duration=%v (vs initial=%v)", incrAddDuration, initialRefreshDuration)
	recordSingleMeasurement(t, "B3_IncrementalRefresh_AddFile", incrAddDuration,
		fmt.Sprintf("vs_initial=%v", initialRefreshDuration))

	// Verify row count
	stats, err := env.mc.GetCollectionStats(env.ctx, client.NewGetCollectionStatsOption(collName))
	common.CheckErr(t, err, true)
	rowCountStr := stats["row_count"]
	rowCount, _ := strconv.ParseInt(rowCountStr, 10, 64)
	expectedRows := int64(initialFiles+1) * env.cfg.RowsPerFile
	require.Equal(t, expectedRows, rowCount)
}

// ============================================================================
// B4: Index Building Latency
// ============================================================================

func TestBenchmarkExternalIndexBuilding(t *testing.T) {
	forEachScale(t, benchIndexBuilding)
}

func benchIndexBuilding(t *testing.T, env *benchEnv) {
	collName := common.GenRandomString("bench_idx", 6)
	extPath := fmt.Sprintf("external-bench/%s", collName)

	// Upload and create collection
	for i := 0; i < env.cfg.NumFiles; i++ {
		data, err := generateParquetBytesWithDim(env.cfg.RowsPerFile, int64(i)*env.cfg.RowsPerFile, env.cfg.VecDim)
		require.NoError(t, err)
		objectKey := fmt.Sprintf("%s/data%d.parquet", extPath, i)
		uploadParquetToMinIO(env.ctx, t, env.minioClient, env.minioCfg.bucket, objectKey, data)
	}

	t.Cleanup(func() {
		cleanupMinIOPrefix(context.Background(), env.minioClient, env.minioCfg.bucket, extPath+"/")
		_ = env.mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource(extPath).
		WithExternalSpec(`{"format":"parquet"}`).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
		WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value")).
		WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(int64(env.cfg.VecDim)).WithExternalField("embedding"))

	err := env.mc.CreateCollection(env.ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)
	refreshAndWait(env.ctx, t, env.mc, collName)

	// Measure scalar index on "id"
	start := time.Now()
	idIdxTask, err := env.mc.CreateIndex(env.ctx, client.NewCreateIndexOption(collName, "id", index.NewAutoIndex(entity.IP)))
	common.CheckErr(t, err, true)
	err = idIdxTask.Await(env.ctx)
	common.CheckErr(t, err, true)
	scalarIdxDuration := time.Since(start)
	t.Logf("=== B4_ScalarIndex_id === rows=%d duration=%v", env.cfg.TotalRows, scalarIdxDuration)
	recordSingleMeasurement(t, "B4_ScalarIndex_id", scalarIdxDuration, fmt.Sprintf("rows=%d", env.cfg.TotalRows))

	// Measure scalar index on "value"
	start = time.Now()
	valIdxTask, err := env.mc.CreateIndex(env.ctx, client.NewCreateIndexOption(collName, "value", index.NewAutoIndex(entity.IP)))
	common.CheckErr(t, err, true)
	err = valIdxTask.Await(env.ctx)
	common.CheckErr(t, err, true)
	scalarValIdxDuration := time.Since(start)
	t.Logf("=== B4_ScalarIndex_value === rows=%d duration=%v", env.cfg.TotalRows, scalarValIdxDuration)
	recordSingleMeasurement(t, "B4_ScalarIndex_value", scalarValIdxDuration, fmt.Sprintf("rows=%d", env.cfg.TotalRows))

	// Measure vector index
	start = time.Now()
	vecIdxTask, err := env.mc.CreateIndex(env.ctx, client.NewCreateIndexOption(collName, "embedding", index.NewAutoIndex(entity.L2)))
	common.CheckErr(t, err, true)
	err = vecIdxTask.Await(env.ctx)
	common.CheckErr(t, err, true)
	vectorIdxDuration := time.Since(start)
	t.Logf("=== B4_VectorIndex === rows=%d dim=%d duration=%v", env.cfg.TotalRows, env.cfg.VecDim, vectorIdxDuration)
	recordSingleMeasurement(t, "B4_VectorIndex", vectorIdxDuration, fmt.Sprintf("rows=%d dim=%d", env.cfg.TotalRows, env.cfg.VecDim))

	// Verify indexes
	for _, fieldName := range []string{"id", "value", "embedding"} {
		descIdx, err := env.mc.DescribeIndex(env.ctx, client.NewDescribeIndexOption(collName, fieldName))
		common.CheckErr(t, err, true)
		require.Equal(t, index.IndexState(3), descIdx.State, "index on %s should be Finished", fieldName)
		require.Equal(t, env.cfg.TotalRows, descIdx.TotalRows, "index on %s TotalRows", fieldName)
	}
}

// ============================================================================
// B5: LoadCollection Latency
// ============================================================================

func TestBenchmarkExternalLoadCollection(t *testing.T) {
	forEachScale(t, benchLoadCollection)
}

func benchLoadCollection(t *testing.T, env *benchEnv) {
	collName := common.GenRandomString("bench_load", 6)
	extPath := fmt.Sprintf("external-bench/%s", collName)

	for i := 0; i < env.cfg.NumFiles; i++ {
		data, err := generateParquetBytesWithDim(env.cfg.RowsPerFile, int64(i)*env.cfg.RowsPerFile, env.cfg.VecDim)
		require.NoError(t, err)
		objectKey := fmt.Sprintf("%s/data%d.parquet", extPath, i)
		uploadParquetToMinIO(env.ctx, t, env.minioClient, env.minioCfg.bucket, objectKey, data)
	}

	t.Cleanup(func() {
		cleanupMinIOPrefix(context.Background(), env.minioClient, env.minioCfg.bucket, extPath+"/")
		_ = env.mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource(extPath).
		WithExternalSpec(`{"format":"parquet"}`).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
		WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value")).
		WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(int64(env.cfg.VecDim)).WithExternalField("embedding"))

	err := env.mc.CreateCollection(env.ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)
	refreshAndWait(env.ctx, t, env.mc, collName)

	// Create indexes (required before load)
	indexAndLoadCollectionWithScalarAndVector(env.ctx, t, env.mc, collName, env.cfg.TotalRows)
	// Release to re-measure load
	err = env.mc.ReleaseCollection(env.ctx, client.NewReleaseCollectionOption(collName))
	require.NoError(t, err)
	time.Sleep(2 * time.Second)

	// Measure load
	start := time.Now()
	loadTask, err := env.mc.LoadCollection(env.ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	err = loadTask.Await(env.ctx)
	common.CheckErr(t, err, true)
	loadDuration := time.Since(start)
	t.Logf("=== B5_LoadCollection === rows=%d dim=%d duration=%v", env.cfg.TotalRows, env.cfg.VecDim, loadDuration)
	recordSingleMeasurement(t, "B5_LoadCollection", loadDuration, fmt.Sprintf("rows=%d dim=%d", env.cfg.TotalRows, env.cfg.VecDim))

	// Verify loaded — query count
	countRes, err := env.mc.Query(env.ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	count, _ := countRes.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
	require.Equal(t, env.cfg.TotalRows, count)
}

// ============================================================================
// B6: Count Query
// ============================================================================

func TestBenchmarkExternalCountQuery(t *testing.T) {
	forEachScale(t, benchCountQuery)
}

func benchCountQuery(t *testing.T, env *benchEnv) {
	collName := env.setupExternalCollection(t)

	measureLatency(t, "B6_CountQuery", env.cfg.QueryIterations, func() {
		countRes, err := env.mc.Query(env.ctx, client.NewQueryOption(collName).
			WithConsistencyLevel(entity.ClStrong).
			WithOutputFields(common.QueryCountFieldName))
		common.CheckErr(t, err, true)
		count, _ := countRes.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
		require.Equal(t, env.cfg.TotalRows, count)
	})
}

// ============================================================================
// B7: Scalar Filter Query
// ============================================================================

func TestBenchmarkExternalScalarFilter(t *testing.T) {
	forEachScale(t, benchScalarFilter)
}

func benchScalarFilter(t *testing.T, env *benchEnv) {
	collName := env.setupExternalCollection(t)

	totalRows := env.cfg.TotalRows

	// High selectivity: single row
	measureLatency(t, "B7_ScalarFilter_SingleRow", env.cfg.QueryIterations, func() {
		res, err := env.mc.Query(env.ctx, client.NewQueryOption(collName).
			WithConsistencyLevel(entity.ClStrong).
			WithFilter("id == 42").
			WithOutputFields("id", "value"))
		common.CheckErr(t, err, true)
		require.Equal(t, 1, res.GetColumn("id").Len())
	})

	// 1% selectivity
	top1pct := totalRows / 100
	measureLatency(t, "B7_ScalarFilter_1pct", env.cfg.QueryIterations, func() {
		res, err := env.mc.Query(env.ctx, client.NewQueryOption(collName).
			WithConsistencyLevel(entity.ClStrong).
			WithFilter(fmt.Sprintf("id < %d", top1pct)).
			WithOutputFields("id"))
		common.CheckErr(t, err, true)
		require.Equal(t, int(top1pct), res.GetColumn("id").Len())
	})

	// 10% selectivity
	top10pct := totalRows / 10
	measureLatency(t, "B7_ScalarFilter_10pct", env.cfg.QueryIterations, func() {
		res, err := env.mc.Query(env.ctx, client.NewQueryOption(collName).
			WithConsistencyLevel(entity.ClStrong).
			WithFilter(fmt.Sprintf("id < %d", top10pct)).
			WithOutputFields(common.QueryCountFieldName))
		common.CheckErr(t, err, true)
		count, _ := res.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
		require.Equal(t, top10pct, count)
	})

	// Range filter
	rangeLow := totalRows / 4
	rangeHigh := totalRows / 2
	measureLatency(t, "B7_ScalarFilter_Range25pct", env.cfg.QueryIterations, func() {
		res, err := env.mc.Query(env.ctx, client.NewQueryOption(collName).
			WithConsistencyLevel(entity.ClStrong).
			WithFilter(fmt.Sprintf("id >= %d && id < %d", rangeLow, rangeHigh)).
			WithOutputFields(common.QueryCountFieldName))
		common.CheckErr(t, err, true)
		count, _ := res.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
		require.Equal(t, rangeHigh-rangeLow, count)
	})
}

// ============================================================================
// B8: Vector Search
// ============================================================================

func TestBenchmarkExternalVectorSearch(t *testing.T) {
	forEachScale(t, benchVectorSearch)
}

func benchVectorSearch(t *testing.T, env *benchEnv) {
	collName := env.setupExternalCollection(t)

	vec := env.makeQueryVec()

	for _, topK := range []int{5, 10, 50, 100} {
		name := fmt.Sprintf("B8_VectorSearch_topK%d", topK)
		measureLatency(t, name, env.cfg.QueryIterations, func() {
			searchRes, err := env.mc.Search(env.ctx, client.NewSearchOption(collName, topK,
				[]entity.Vector{entity.FloatVector(vec)}).
				WithConsistencyLevel(entity.ClStrong).
				WithANNSField("embedding").
				WithOutputFields("id"))
			common.CheckErr(t, err, true)
			require.Equal(t, 1, len(searchRes))
			require.Greater(t, searchRes[0].ResultCount, 0)
		})
	}
}

// ============================================================================
// B9: Hybrid Search (Vector + Scalar Filter)
// ============================================================================

func TestBenchmarkExternalHybridSearch(t *testing.T) {
	forEachScale(t, benchHybridSearch)
}

func benchHybridSearch(t *testing.T, env *benchEnv) {
	collName := env.setupExternalCollection(t)

	vec := env.makeQueryVec()
	halfRows := env.cfg.TotalRows / 2

	// Range filter — 50% selectivity
	measureLatency(t, "B9_HybridSearch_Range50pct", env.cfg.QueryIterations, func() {
		searchRes, err := env.mc.Search(env.ctx, client.NewSearchOption(collName, env.cfg.TopK,
			[]entity.Vector{entity.FloatVector(vec)}).
			WithConsistencyLevel(entity.ClStrong).
			WithANNSField("embedding").
			WithFilter(fmt.Sprintf("id >= %d", halfRows)).
			WithOutputFields("id"))
		common.CheckErr(t, err, true)
		require.Greater(t, searchRes[0].ResultCount, 0)
		// Verify filter
		for i := 0; i < searchRes[0].GetColumn("id").Len(); i++ {
			val, _ := searchRes[0].GetColumn("id").GetAsInt64(i)
			require.GreaterOrEqual(t, val, halfRows)
		}
	})

	// Range filter — 10% selectivity
	top10pct := env.cfg.TotalRows / 10
	measureLatency(t, "B9_HybridSearch_Range10pct", env.cfg.QueryIterations, func() {
		searchRes, err := env.mc.Search(env.ctx, client.NewSearchOption(collName, env.cfg.TopK,
			[]entity.Vector{entity.FloatVector(vec)}).
			WithConsistencyLevel(entity.ClStrong).
			WithANNSField("embedding").
			WithFilter(fmt.Sprintf("id < %d", top10pct)).
			WithOutputFields("id"))
		common.CheckErr(t, err, true)
		require.Greater(t, searchRes[0].ResultCount, 0)
	})
}

// ============================================================================
// B10: Multi-Vector Search
// ============================================================================

func TestBenchmarkExternalMultiVectorSearch(t *testing.T) {
	forEachScale(t, benchMultiVectorSearch)
}

func benchMultiVectorSearch(t *testing.T, env *benchEnv) {
	collName := env.setupExternalCollection(t)

	for _, nq := range []int{1, 5, 10, 50} {
		vectors := make([]entity.Vector, nq)
		for i := range vectors {
			vectors[i] = entity.FloatVector(env.makeRandomQueryVec())
		}

		name := fmt.Sprintf("B10_MultiVectorSearch_nq%d", nq)
		measureLatency(t, name, env.cfg.QueryIterations, func() {
			searchRes, err := env.mc.Search(env.ctx, client.NewSearchOption(collName, env.cfg.TopK, vectors).
				WithConsistencyLevel(entity.ClStrong).
				WithANNSField("embedding").
				WithOutputFields("id"))
			common.CheckErr(t, err, true)
			require.Equal(t, nq, len(searchRes))
			for _, r := range searchRes {
				require.Greater(t, r.ResultCount, 0)
			}
		})
	}
}

// ============================================================================
// B11: Query with Pagination
// ============================================================================

func TestBenchmarkExternalPagination(t *testing.T) {
	forEachScale(t, benchPagination)
}

func benchPagination(t *testing.T, env *benchEnv) {
	collName := env.setupExternalCollection(t)

	pageSize := int64(100)
	offsets := []int64{0, 100, 1000}
	if env.cfg.TotalRows > 10000 {
		offsets = append(offsets, 5000, 10000)
	}

	for _, offset := range offsets {
		name := fmt.Sprintf("B11_Pagination_offset%d_limit%d", offset, pageSize)
		measureLatency(t, name, env.cfg.QueryIterations, func() {
			res, err := env.mc.Query(env.ctx, client.NewQueryOption(collName).
				WithConsistencyLevel(entity.ClStrong).
				WithFilter("id >= 0").
				WithOutputFields("id").
				WithOffset(int(offset)).
				WithLimit(int(pageSize)))
			common.CheckErr(t, err, true)
			require.Equal(t, int(pageSize), res.GetColumn("id").Len())
		})
	}
}

// ============================================================================
// B12: Query with Output Fields
// ============================================================================

func TestBenchmarkExternalOutputFields(t *testing.T) {
	forEachScale(t, benchOutputFields)
}

func benchOutputFields(t *testing.T, env *benchEnv) {
	collName := env.setupExternalCollection(t)

	limit := int64(100)

	// IDs only (no output fields)
	measureLatency(t, "B12_OutputFields_IDsOnly", env.cfg.QueryIterations, func() {
		_, err := env.mc.Query(env.ctx, client.NewQueryOption(collName).
			WithConsistencyLevel(entity.ClStrong).
			WithFilter(fmt.Sprintf("id < %d", limit)).
			WithOutputFields("id"))
		common.CheckErr(t, err, true)
	})

	// Scalar fields
	measureLatency(t, "B12_OutputFields_Scalar", env.cfg.QueryIterations, func() {
		_, err := env.mc.Query(env.ctx, client.NewQueryOption(collName).
			WithConsistencyLevel(entity.ClStrong).
			WithFilter(fmt.Sprintf("id < %d", limit)).
			WithOutputFields("id", "value"))
		common.CheckErr(t, err, true)
	})

	// Vector field
	measureLatency(t, "B12_OutputFields_Vector", env.cfg.QueryIterations, func() {
		_, err := env.mc.Query(env.ctx, client.NewQueryOption(collName).
			WithConsistencyLevel(entity.ClStrong).
			WithFilter(fmt.Sprintf("id < %d", limit)).
			WithOutputFields("id", "embedding"))
		common.CheckErr(t, err, true)
	})

	// All fields
	measureLatency(t, "B12_OutputFields_All", env.cfg.QueryIterations, func() {
		_, err := env.mc.Query(env.ctx, client.NewQueryOption(collName).
			WithConsistencyLevel(entity.ClStrong).
			WithFilter(fmt.Sprintf("id < %d", limit)).
			WithOutputFields("id", "value", "embedding"))
		common.CheckErr(t, err, true)
	})
}

// ============================================================================
// B13: External vs Regular Collection — Query Performance
// ============================================================================

func TestBenchmarkExternalVsRegularQuery(t *testing.T) {
	forEachScale(t, benchVsRegularQuery)
}

func benchVsRegularQuery(t *testing.T, env *benchEnv) {
	// Setup external collection
	extCollName := env.setupExternalCollection(t)

	// Setup regular collection with same data
	regCollName := common.GenRandomString("bench_reg", 6)
	t.Logf("Creating regular collection for comparison: %s", regCollName)

	regSchema := entity.NewSchema().
		WithName(regCollName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat)).
		WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(int64(env.cfg.VecDim)))

	err := env.mc.CreateCollection(env.ctx, client.NewCreateCollectionOption(regCollName, regSchema))
	common.CheckErr(t, err, true)

	t.Cleanup(func() {
		_ = env.mc.DropCollection(context.Background(), client.NewDropCollectionOption(regCollName))
	})

	// Insert data into regular collection in batches
	batchSize := int64(10000)
	for batchOffset := int64(0); batchOffset < env.cfg.TotalRows; batchOffset += batchSize {
		remaining := env.cfg.TotalRows - batchOffset
		if remaining > batchSize {
			remaining = batchSize
		}
		ids := make([]int64, remaining)
		values := make([]float32, remaining)
		embeddings := make([][]float32, remaining)
		for i := int64(0); i < remaining; i++ {
			idx := batchOffset + i
			ids[i] = idx
			values[i] = float32(idx) * 1.5
			vec := make([]float32, env.cfg.VecDim)
			for d := range vec {
				vec[d] = float32(idx)*0.1 + float32(d)
			}
			embeddings[i] = vec
		}

		insertOpt := client.NewColumnBasedInsertOption(regCollName).
			WithInt64Column("id", ids).
			WithColumns(column.NewColumnFloat("value", values)).
			WithFloatVectorColumn("embedding", env.cfg.VecDim, embeddings)

		_, err := env.mc.Insert(env.ctx, insertOpt)
		common.CheckErr(t, err, true)
	}

	// Flush
	flushTask, err := env.mc.Flush(env.ctx, client.NewFlushOption(regCollName))
	common.CheckErr(t, err, true)
	err = flushTask.Await(env.ctx)
	common.CheckErr(t, err, true)

	// Index + Load regular collection
	vecIdx := index.NewAutoIndex(entity.L2)
	vecIdxTask, err := env.mc.CreateIndex(env.ctx, client.NewCreateIndexOption(regCollName, "embedding", vecIdx))
	common.CheckErr(t, err, true)
	err = vecIdxTask.Await(env.ctx)
	common.CheckErr(t, err, true)

	loadTask, err := env.mc.LoadCollection(env.ctx, client.NewLoadCollectionOption(regCollName))
	common.CheckErr(t, err, true)
	err = loadTask.Await(env.ctx)
	common.CheckErr(t, err, true)

	vec := env.makeQueryVec()

	// Compare: Count Query
	extCountStats := measureLatency(t, "B13_CountQuery_External", env.cfg.QueryIterations, func() {
		res, err := env.mc.Query(env.ctx, client.NewQueryOption(extCollName).
			WithConsistencyLevel(entity.ClStrong).
			WithOutputFields(common.QueryCountFieldName))
		common.CheckErr(t, err, true)
		count, _ := res.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
		require.Equal(t, env.cfg.TotalRows, count)
	})

	regCountStats := measureLatency(t, "B13_CountQuery_Regular", env.cfg.QueryIterations, func() {
		res, err := env.mc.Query(env.ctx, client.NewQueryOption(regCollName).
			WithConsistencyLevel(entity.ClStrong).
			WithOutputFields(common.QueryCountFieldName))
		common.CheckErr(t, err, true)
		count, _ := res.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
		require.Equal(t, env.cfg.TotalRows, count)
	})
	t.Logf("B13 Count: External=%v Regular=%v ratio=%.2fx",
		extCountStats.Mean, regCountStats.Mean,
		float64(extCountStats.Mean)/float64(regCountStats.Mean))

	// Compare: Vector Search
	extSearchStats := measureLatency(t, "B13_VectorSearch_External", env.cfg.QueryIterations, func() {
		searchRes, err := env.mc.Search(env.ctx, client.NewSearchOption(extCollName, env.cfg.TopK,
			[]entity.Vector{entity.FloatVector(vec)}).
			WithConsistencyLevel(entity.ClStrong).
			WithANNSField("embedding").
			WithOutputFields("id"))
		common.CheckErr(t, err, true)
		require.Greater(t, searchRes[0].ResultCount, 0)
	})

	regSearchStats := measureLatency(t, "B13_VectorSearch_Regular", env.cfg.QueryIterations, func() {
		searchRes, err := env.mc.Search(env.ctx, client.NewSearchOption(regCollName, env.cfg.TopK,
			[]entity.Vector{entity.FloatVector(vec)}).
			WithConsistencyLevel(entity.ClStrong).
			WithANNSField("embedding").
			WithOutputFields("id"))
		common.CheckErr(t, err, true)
		require.Greater(t, searchRes[0].ResultCount, 0)
	})
	t.Logf("B13 Search: External=%v Regular=%v ratio=%.2fx",
		extSearchStats.Mean, regSearchStats.Mean,
		float64(extSearchStats.Mean)/float64(regSearchStats.Mean))

	// Compare: Scalar filter
	filterExpr := fmt.Sprintf("id < %d", env.cfg.TotalRows/10)
	extFilterStats := measureLatency(t, "B13_ScalarFilter_External", env.cfg.QueryIterations, func() {
		_, err := env.mc.Query(env.ctx, client.NewQueryOption(extCollName).
			WithConsistencyLevel(entity.ClStrong).
			WithFilter(filterExpr).
			WithOutputFields(common.QueryCountFieldName))
		common.CheckErr(t, err, true)
	})

	regFilterStats := measureLatency(t, "B13_ScalarFilter_Regular", env.cfg.QueryIterations, func() {
		_, err := env.mc.Query(env.ctx, client.NewQueryOption(regCollName).
			WithConsistencyLevel(entity.ClStrong).
			WithFilter(filterExpr).
			WithOutputFields(common.QueryCountFieldName))
		common.CheckErr(t, err, true)
	})
	t.Logf("B13 Filter: External=%v Regular=%v ratio=%.2fx",
		extFilterStats.Mean, regFilterStats.Mean,
		float64(extFilterStats.Mean)/float64(regFilterStats.Mean))
}

// ============================================================================
// B15: Large Number of Files
// ============================================================================

func TestBenchmarkExternalManyFiles(t *testing.T) {
	forEachScale(t, benchManyFiles)
}

func benchManyFiles(t *testing.T, env *benchEnv) {
	// Use 100 small files regardless of BENCH_SCALE
	numFiles := 100
	rowsPerFile := env.cfg.TotalRows / int64(numFiles)
	if rowsPerFile < 100 {
		rowsPerFile = 100
	}

	collName := common.GenRandomString("bench_manyfiles", 6)
	extPath := fmt.Sprintf("external-bench/%s", collName)
	totalRows := int64(numFiles) * rowsPerFile

	t.Logf("Uploading %d files (%d rows each = %d total)", numFiles, rowsPerFile, totalRows)

	for i := 0; i < numFiles; i++ {
		data, err := generateParquetBytesWithDim(rowsPerFile, int64(i)*rowsPerFile, env.cfg.VecDim)
		require.NoError(t, err)
		objectKey := fmt.Sprintf("%s/data%d.parquet", extPath, i)
		uploadParquetToMinIO(env.ctx, t, env.minioClient, env.minioCfg.bucket, objectKey, data)
	}

	t.Cleanup(func() {
		cleanupMinIOPrefix(context.Background(), env.minioClient, env.minioCfg.bucket, extPath+"/")
		_ = env.mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource(extPath).
		WithExternalSpec(`{"format":"parquet"}`).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
		WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value")).
		WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(int64(env.cfg.VecDim)).WithExternalField("embedding"))

	err := env.mc.CreateCollection(env.ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)

	start := time.Now()
	refreshAndWait(env.ctx, t, env.mc, collName)
	refreshDuration := time.Since(start)
	t.Logf("=== B15_ManyFiles_Refresh === files=%d rows=%d duration=%v", numFiles, totalRows, refreshDuration)
	recordSingleMeasurement(t, "B15_ManyFiles_Refresh", refreshDuration, fmt.Sprintf("files=%d rows=%d", numFiles, totalRows))

	// Verify
	stats, err := env.mc.GetCollectionStats(env.ctx, client.NewGetCollectionStatsOption(collName))
	common.CheckErr(t, err, true)
	rowCount, _ := strconv.ParseInt(stats["row_count"], 10, 64)
	require.Equal(t, totalRows, rowCount)
}

// ============================================================================
// B16: Large Single File
// ============================================================================

func TestBenchmarkExternalLargeSingleFile(t *testing.T) {
	forEachScale(t, benchLargeSingleFile)
}

func benchLargeSingleFile(t *testing.T, env *benchEnv) {
	collName := common.GenRandomString("bench_bigfile", 6)
	extPath := fmt.Sprintf("external-bench/%s", collName)
	totalRows := env.cfg.TotalRows

	t.Logf("Uploading 1 file with %d rows", totalRows)

	data, err := generateParquetBytesWithDim(totalRows, 0, env.cfg.VecDim)
	require.NoError(t, err)
	objectKey := fmt.Sprintf("%s/data.parquet", extPath)
	uploadParquetToMinIO(env.ctx, t, env.minioClient, env.minioCfg.bucket, objectKey, data)

	t.Cleanup(func() {
		cleanupMinIOPrefix(context.Background(), env.minioClient, env.minioCfg.bucket, extPath+"/")
		_ = env.mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource(extPath).
		WithExternalSpec(`{"format":"parquet"}`).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
		WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value")).
		WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(int64(env.cfg.VecDim)).WithExternalField("embedding"))

	err = env.mc.CreateCollection(env.ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)

	start := time.Now()
	refreshAndWait(env.ctx, t, env.mc, collName)
	refreshDuration := time.Since(start)
	t.Logf("=== B16_LargeSingleFile_Refresh === rows=%d duration=%v", totalRows, refreshDuration)
	recordSingleMeasurement(t, "B16_LargeSingleFile_Refresh", refreshDuration, fmt.Sprintf("rows=%d", totalRows))

	// Verify
	stats, err := env.mc.GetCollectionStats(env.ctx, client.NewGetCollectionStatsOption(collName))
	common.CheckErr(t, err, true)
	rowCount, _ := strconv.ParseInt(stats["row_count"], 10, 64)
	require.Equal(t, totalRows, rowCount)

	// Load and verify query works
	indexAndLoadCollection(env.ctx, t, env.mc, collName, "embedding")

	countRes, err := env.mc.Query(env.ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	count, _ := countRes.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
	require.Equal(t, totalRows, count)
	t.Logf("=== B16_LargeSingleFile === verified count=%d", count)
}

// ============================================================================
// B17: Concurrent Query Load
// ============================================================================

func TestBenchmarkExternalConcurrentQuery(t *testing.T) {
	forEachScale(t, benchConcurrentQuery)
}

func benchConcurrentQuery(t *testing.T, env *benchEnv) {
	collName := env.setupExternalCollection(t)

	vec := env.makeQueryVec()

	concurrencyLevels := []int{1, 5, 10, 20}
	if env.cfg.Concurrency > 0 {
		concurrencyLevels = []int{env.cfg.Concurrency}
	}

	for _, concurrency := range concurrencyLevels {
		t.Run(fmt.Sprintf("concurrency_%d", concurrency), func(t *testing.T) {
			var wg sync.WaitGroup
			var mu sync.Mutex
			var allLatencies []time.Duration
			errCount := 0

			totalOps := env.cfg.QueryIterations * concurrency
			start := time.Now()

			for g := 0; g < concurrency; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < env.cfg.QueryIterations; i++ {
						opStart := time.Now()
						searchRes, err := env.mc.Search(env.ctx, client.NewSearchOption(collName, env.cfg.TopK,
							[]entity.Vector{entity.FloatVector(vec)}).
							WithConsistencyLevel(entity.ClStrong).
							WithANNSField("embedding").
							WithOutputFields("id"))
						elapsed := time.Since(opStart)

						mu.Lock()
						if err != nil {
							errCount++
						} else {
							if len(searchRes) > 0 && searchRes[0].ResultCount > 0 {
								allLatencies = append(allLatencies, elapsed)
							}
						}
						mu.Unlock()
					}
				}()
			}

			wg.Wait()
			totalDuration := time.Since(start)

			stats := computeStats(fmt.Sprintf("B17_ConcurrentSearch_c%d", concurrency), allLatencies)
			throughput := float64(len(allLatencies)) / totalDuration.Seconds()

			t.Logf("=== B17_ConcurrentSearch === concurrency=%d totalOps=%d successOps=%d errors=%d",
				concurrency, totalOps, len(allLatencies), errCount)
			t.Logf("  throughput=%.1f ops/s totalDuration=%v", throughput, totalDuration)
			t.Logf("  latency: min=%v max=%v mean=%v p50=%v p95=%v p99=%v",
				stats.Min, stats.Max, stats.Mean, stats.P50, stats.P95, stats.P99)

			require.Zero(t, errCount, "no errors expected in concurrent search")

			globalReport.add(t.Name(), stats, fmt.Sprintf("throughput=%.1f ops/s, concurrency=%d", throughput, concurrency))
		})
	}
}

// ============================================================================
// B19: Multiple Refreshes (Idempotency)
// ============================================================================

func TestBenchmarkExternalIdempotentRefresh(t *testing.T) {
	forEachScale(t, benchIdempotentRefresh)
}

func benchIdempotentRefresh(t *testing.T, env *benchEnv) {
	collName := common.GenRandomString("bench_idemp", 6)
	extPath := fmt.Sprintf("external-bench/%s", collName)

	// Upload data (smaller set for idempotency test)
	numFiles := 5
	rowsPerFile := env.cfg.RowsPerFile
	totalRows := int64(numFiles) * rowsPerFile

	for i := 0; i < numFiles; i++ {
		data, err := generateParquetBytesWithDim(rowsPerFile, int64(i)*rowsPerFile, env.cfg.VecDim)
		require.NoError(t, err)
		objectKey := fmt.Sprintf("%s/data%d.parquet", extPath, i)
		uploadParquetToMinIO(env.ctx, t, env.minioClient, env.minioCfg.bucket, objectKey, data)
	}

	t.Cleanup(func() {
		cleanupMinIOPrefix(context.Background(), env.minioClient, env.minioCfg.bucket, extPath+"/")
		_ = env.mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource(extPath).
		WithExternalSpec(`{"format":"parquet"}`).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
		WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value")).
		WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(int64(env.cfg.VecDim)).WithExternalField("embedding"))

	err := env.mc.CreateCollection(env.ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)

	// First refresh
	start := time.Now()
	refreshAndWait(env.ctx, t, env.mc, collName)
	firstRefreshDuration := time.Since(start)
	t.Logf("=== B19_FirstRefresh === rows=%d duration=%v", totalRows, firstRefreshDuration)
	recordSingleMeasurement(t, "B19_FirstRefresh", firstRefreshDuration, fmt.Sprintf("rows=%d", totalRows))

	// Second refresh (no changes)
	start = time.Now()
	refreshAndWait(env.ctx, t, env.mc, collName)
	secondRefreshDuration := time.Since(start)
	t.Logf("=== B19_SecondRefresh_NoChange === duration=%v (vs first=%v)", secondRefreshDuration, firstRefreshDuration)
	recordSingleMeasurement(t, "B19_SecondRefresh_NoChange", secondRefreshDuration, fmt.Sprintf("vs_first=%v", firstRefreshDuration))

	// Verify row count unchanged
	stats, err := env.mc.GetCollectionStats(env.ctx, client.NewGetCollectionStatsOption(collName))
	common.CheckErr(t, err, true)
	rowCount, _ := strconv.ParseInt(stats["row_count"], 10, 64)
	require.Equal(t, totalRows, rowCount, "row count should be unchanged after idempotent refresh")

	// Third refresh (still no changes)
	start = time.Now()
	refreshAndWait(env.ctx, t, env.mc, collName)
	thirdRefreshDuration := time.Since(start)
	t.Logf("=== B19_ThirdRefresh_NoChange === duration=%v", thirdRefreshDuration)
	recordSingleMeasurement(t, "B19_ThirdRefresh_NoChange", thirdRefreshDuration, "")

	// Verify still same
	stats, err = env.mc.GetCollectionStats(env.ctx, client.NewGetCollectionStatsOption(collName))
	common.CheckErr(t, err, true)
	rowCount, _ = strconv.ParseInt(stats["row_count"], 10, 64)
	require.Equal(t, totalRows, rowCount)
}

// ============================================================================
// B20: All Supported Data Types
// ============================================================================

func TestBenchmarkExternalMultiDataTypes(t *testing.T) {
	forEachScale(t, benchMultiDataTypes)
}

func benchMultiDataTypes(t *testing.T, env *benchEnv) {
	collName := common.GenRandomString("bench_types", 6)
	extPath := fmt.Sprintf("external-bench/%s", collName)

	// Use smaller dataset for type coverage test
	numRows := env.cfg.TotalRows
	if numRows > 100_000 {
		numRows = 100_000
	}

	data, err := generateMultiTypeParquetBytes(numRows, 0)
	require.NoError(t, err)
	objectKey := fmt.Sprintf("%s/data.parquet", extPath)
	uploadParquetToMinIO(env.ctx, t, env.minioClient, env.minioCfg.bucket, objectKey, data)

	t.Cleanup(func() {
		cleanupMinIOPrefix(context.Background(), env.minioClient, env.minioCfg.bucket, extPath+"/")
		_ = env.mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

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

	err = env.mc.CreateCollection(env.ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)

	// Refresh
	start := time.Now()
	refreshAndWait(env.ctx, t, env.mc, collName)
	refreshDuration := time.Since(start)
	t.Logf("=== B20_Refresh === rows=%d fields=9 duration=%v", numRows, refreshDuration)
	recordSingleMeasurement(t, "B20_Refresh_MultiType", refreshDuration, fmt.Sprintf("rows=%d fields=9", numRows))

	// Index + Load
	indexAndLoadCollection(env.ctx, t, env.mc, collName, "embedding")

	// Count
	measureLatency(t, "B20_CountQuery_MultiType", env.cfg.QueryIterations, func() {
		res, err := env.mc.Query(env.ctx, client.NewQueryOption(collName).
			WithConsistencyLevel(entity.ClStrong).
			WithOutputFields(common.QueryCountFieldName))
		common.CheckErr(t, err, true)
		count, _ := res.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
		require.Equal(t, numRows, count)
	})

	// Bool filter
	measureLatency(t, "B20_BoolFilter", env.cfg.QueryIterations, func() {
		res, err := env.mc.Query(env.ctx, client.NewQueryOption(collName).
			WithConsistencyLevel(entity.ClStrong).
			WithFilter("bool_val == true").
			WithOutputFields(common.QueryCountFieldName))
		common.CheckErr(t, err, true)
		count, _ := res.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
		require.Equal(t, numRows/2, count)
	})

	// VarChar filter
	measureLatency(t, "B20_VarCharFilter", env.cfg.QueryIterations, func() {
		res, err := env.mc.Query(env.ctx, client.NewQueryOption(collName).
			WithConsistencyLevel(entity.ClStrong).
			WithFilter(`varchar_val like "str_004%"`).
			WithOutputFields("id"))
		common.CheckErr(t, err, true)
		require.Greater(t, res.GetColumn("id").Len(), 0)
	})

	// Compound filter
	measureLatency(t, "B20_CompoundFilter", env.cfg.QueryIterations, func() {
		res, err := env.mc.Query(env.ctx, client.NewQueryOption(collName).
			WithConsistencyLevel(entity.ClStrong).
			WithFilter("bool_val == true && int32_val < 5000").
			WithOutputFields(common.QueryCountFieldName))
		common.CheckErr(t, err, true)
		count, _ := res.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
		require.Greater(t, count, int64(0))
	})

	// Vector search
	vec := make([]float32, testVecDim)
	for i := range vec {
		vec[i] = float32(42)*0.1 + float32(i)
	}
	measureLatency(t, "B20_VectorSearch_MultiType", env.cfg.QueryIterations, func() {
		searchRes, err := env.mc.Search(env.ctx, client.NewSearchOption(collName, env.cfg.TopK,
			[]entity.Vector{entity.FloatVector(vec)}).
			WithConsistencyLevel(entity.ClStrong).
			WithANNSField("embedding").
			WithOutputFields("id", "bool_val", "varchar_val"))
		common.CheckErr(t, err, true)
		require.Greater(t, searchRes[0].ResultCount, 0)
	})

	// Verify specific row
	row42, err := env.mc.Query(env.ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithFilter("id == 42").
		WithOutputFields("id", "bool_val", "int8_val", "int16_val", "int32_val", "float_val", "double_val", "varchar_val"))
	common.CheckErr(t, err, true)
	require.Equal(t, 1, row42.GetColumn("id").Len())

	boolVal, _ := row42.GetColumn("bool_val").GetAsBool(0)
	require.Equal(t, true, boolVal, "id=42 (even) → bool_val=true")

	varcharVal, _ := row42.GetColumn("varchar_val").GetAsString(0)
	require.Equal(t, "str_0042", varcharVal)

	t.Log("B20: All data types verified successfully")
}

// ============================================================================
// B21: Access Mode Comparison (load / lazyLoad / take)
// ============================================================================

func TestBenchmarkExternalYYAccessMode(t *testing.T) {
	forEachScaleConfig(t, func(t *testing.T, cfg benchConfig) {
		ctx := hp.CreateContext(t, time.Second*3600)
		mc := hp.CreateDefaultMilvusClient(ctx, t)
		minioCfg := getMinIOConfig()
		minioClient, err := newMinIOClient(minioCfg)
		if err != nil {
			t.Skipf("Failed to create MinIO client: %v", err)
		}

		// Prepare data once
		collName := common.GenRandomString("bench_mode", 6)
		extPath := fmt.Sprintf("external-bench/%s", collName)
		for i := 0; i < cfg.NumFiles; i++ {
			data, err := generateParquetBytesWithDim(cfg.RowsPerFile, int64(i)*cfg.RowsPerFile, cfg.VecDim)
			require.NoError(t, err)
			objectKey := fmt.Sprintf("%s/data%d.parquet", extPath, i)
			uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket, objectKey, data)
		}
		t.Cleanup(func() {
			cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket, extPath+"/")
		})

		// Connect to etcd for runtime config changes
		etcdEndpoint := envOrDefault("ETCD_ENDPOINTS", "localhost:2379")
		etcdPrefix := envOrDefault("ETCD_ROOT_PATH", "by-dev")
		etcdClient, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{etcdEndpoint},
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			t.Skipf("Failed to connect etcd: %v", err)
		}
		defer etcdClient.Close()

		// Part8 replaced the legacy "accessMode" enum (load/lazyLoad/take) with a
		// boolean useTakeForOutput. This benchmark now compares take=true vs
		// take=false instead of three modes.
		setUseTake := func(useTake string) {
			key := etcdPrefix + "/config/queryNode/externalCollection/useTakeForOutput"
			_, err := etcdClient.Put(ctx, key, useTake)
			require.NoError(t, err)
			t.Logf("Set useTakeForOutput=%s", useTake)
			time.Sleep(2 * time.Second) // wait for config refresh
		}

		// Save original and restore at cleanup
		origKey := etcdPrefix + "/config/queryNode/externalCollection/useTakeForOutput"
		origResp, _ := etcdClient.Get(ctx, origKey)
		t.Cleanup(func() {
			if len(origResp.Kvs) > 0 {
				_, _ = etcdClient.Put(context.Background(), origKey, string(origResp.Kvs[0].Value))
			} else {
				_, _ = etcdClient.Delete(context.Background(), origKey)
			}
		})

		vec := make([]float32, cfg.VecDim)
		for i := range vec {
			vec[i] = float32(i) * 0.1
		}

		// modeLabel is used purely for benchmark naming so existing report parsers
		// (B21_LoadCollection_<label>) keep working.
		modes := []struct{ label, useTake string }{
			{"bulkLoad", "false"}, // useTakeForOutput=false: bulk-fetch into segcore (legacy "load"/"lazyLoad")
			{"take", "true"},      // useTakeForOutput=true:  per-row take() at query time
		}
		for _, mode := range modes {
			t.Run("mode_"+mode.label, func(t *testing.T) {
				setUseTake(mode.useTake)

				// Create collection per mode
				modeColl := fmt.Sprintf("%s_%s", collName, mode.label)
				schema := entity.NewSchema().
					WithName(modeColl).
					WithExternalSource(extPath).
					WithExternalSpec(`{"format":"parquet"}`).
					WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
					WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value")).
					WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(int64(cfg.VecDim)).WithExternalField("embedding"))

				err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(modeColl, schema))
				common.CheckErr(t, err, true)
				t.Cleanup(func() {
					_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(modeColl))
				})

				refreshAndWait(ctx, t, mc, modeColl)
				indexAndLoadCollectionWithScalarAndVector(ctx, t, mc, modeColl, cfg.TotalRows)

				// Release and re-load to measure load time under this access mode
				err = mc.ReleaseCollection(ctx, client.NewReleaseCollectionOption(modeColl))
				require.NoError(t, err)
				time.Sleep(2 * time.Second)

				// Measure LoadCollection
				loadStart := time.Now()
				loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(modeColl))
				common.CheckErr(t, err, true)
				err = loadTask.Await(ctx)
				common.CheckErr(t, err, true)
				loadDuration := time.Since(loadStart)

				benchName := fmt.Sprintf("B21_LoadCollection_%s", mode.label)
				t.Logf("=== %s === rows=%d duration=%v", benchName, cfg.TotalRows, loadDuration)
				recordSingleMeasurement(t, benchName, loadDuration, fmt.Sprintf("mode=%s rows=%d", mode.label, cfg.TotalRows))

				// Measure cold search topK=5 with all output fields
				searchName := fmt.Sprintf("B21_ColdSearch_topK5_allFields_%s", mode.label)
				measureLatency(t, searchName, cfg.QueryIterations, func() {
					searchRes, err := mc.Search(ctx, client.NewSearchOption(modeColl, 5,
						[]entity.Vector{entity.FloatVector(vec)}).
						WithConsistencyLevel(entity.ClStrong).
						WithANNSField("embedding").
						WithOutputFields("id", "value", "embedding"))
					common.CheckErr(t, err, true)
					require.Greater(t, searchRes[0].ResultCount, 0)
				})
			})
		}
	})
}

// ============================================================================
// Report Generator — runs last alphabetically among TestBenchmarkExternal*
// ============================================================================

func TestBenchmarkExternalZZReport(t *testing.T) {
	globalReport.writeMarkdownReport(t)
}

package testcases

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
)

// The standard FunctionChain builder is missing (issue #54317). This option
// exercises the SDK's public extension point without bypassing Client.Search.
type pyudfSearchOption struct {
	client.SearchOption
	chain *schemapb.FunctionChain
}

func (o pyudfSearchOption) Request() (*milvuspb.SearchRequest, error) {
	req, err := o.SearchOption.Request()
	if err != nil {
		return nil, err
	}
	req.FunctionChains = []*schemapb.FunctionChain{o.chain}
	return req, nil
}

type pyudfHybridOption struct {
	client.HybridSearchOption
	chain *schemapb.FunctionChain
}

func (o pyudfHybridOption) HybridRequest() (*milvuspb.HybridSearchRequest, error) {
	req, err := o.HybridSearchOption.HybridRequest()
	if err != nil {
		return nil, err
	}
	req.FunctionChains = []*schemapb.FunctionChain{o.chain}
	return req, nil
}

func pyudfString(value string) *schemapb.FunctionParamValue {
	return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_StringValue{StringValue: value}}
}

func pyudfChain(resource, mode string) *schemapb.FunctionChain {
	args := make([]*schemapb.FunctionChainExprArg, 0, 2)
	for _, name := range []string{"$score", "ts"} {
		args = append(args, &schemapb.FunctionChainExprArg{
			Arg: &schemapb.FunctionChainExprArg_Column{Column: &schemapb.FunctionChainColumnArg{Name: name}},
		})
	}
	return &schemapb.FunctionChain{
		Stage: schemapb.FunctionChainStage_FunctionChainStageL2Rerank,
		Ops: []*schemapb.FunctionChainOp{
			{
				Op: "map", Outputs: []string{"$score"},
				Expr: &schemapb.FunctionChainExpr{
					Name: "py_udf", Args: args,
					Params: map[string]*schemapb.FunctionParamValue{
						"resource_name": pyudfString(resource),
						"udf_params": {Value: &schemapb.FunctionParamValue_ObjectValue{
							ObjectValue: &schemapb.FunctionParamObject{Fields: map[string]*schemapb.FunctionParamValue{
								"mode": pyudfString(mode),
							}},
						}},
					},
				},
			},
			{
				Op: "sort", Inputs: []string{"$score", "$id"},
				Params: map[string]*schemapb.FunctionParamValue{
					"column": pyudfString("$score"), "tie_break_col": pyudfString("$id"),
					"desc": {Value: &schemapb.FunctionParamValue_BoolValue{BoolValue: true}},
				},
			},
		},
	}
}

func registerPyUDFWheel(t *testing.T, ctx context.Context, mc *base.MilvusClient) string {
	t.Helper()
	cfg := getMinIOConfig()
	store, err := newMinIOClient(cfg)
	require.NoError(t, err)
	packageName := common.GenRandomString("go_pyudf_", 8)
	var wheel bytes.Buffer
	writer := zip.NewWriter(&wheel)
	files := map[string]string{
		packageName + "/__init__.py": `import pyarrow as pa
class UDF:
    def transform_query(self, params, columns):
        if params.get("mode") == "exception":
            raise RuntimeError("controlled Go PyUDF failure")
        values = [v.as_py() * 2.0 for v in columns[1]]
        if params.get("mode") == "wrong_rows":
            values = values[:-1]
        return [pa.array(values, type=pa.float32())]
def create_udf(context):
    return UDF()
`,
		packageName + "-0.0.1.dist-info/entry_points.txt": "[milvus.pyudf]\nmain = " + packageName + ":create_udf\n",
		packageName + "-0.0.1.dist-info/METADATA":         "Metadata-Version: 2.1\nName: " + packageName + "\nVersion: 0.0.1\n",
	}
	for name, content := range files {
		entry, err := writer.Create(name)
		require.NoError(t, err)
		_, err = entry.Write([]byte(content))
		require.NoError(t, err)
	}
	require.NoError(t, writer.Close())
	objectPath := fmt.Sprintf("pyudf/%s/%s-0.0.1-py3-none-any.whl", packageName, packageName)
	_, err = store.PutObject(ctx, cfg.bucket, objectPath, bytes.NewReader(wheel.Bytes()), int64(wheel.Len()),
		miniogo.PutObjectOptions{ContentType: "application/zip"})
	require.NoError(t, err)
	// Remove the registration before the object, including an ambiguous Add result.
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
		defer cancel()
		if err := removeFileResourceEventually(cleanupCtx, mc, packageName); err != nil {
			t.Errorf("remove PyUDF resource %q: %v; retaining wheel", packageName, err)
			return
		}
		if err := store.RemoveObject(cleanupCtx, cfg.bucket, objectPath, miniogo.RemoveObjectOptions{}); err != nil {
			t.Errorf("remove PyUDF wheel: %v", err)
		}
	})
	require.NoError(t, mc.AddFileResource(ctx, client.NewAddFileResourceOption(packageName, objectPath)))
	return packageName
}

func assertPyUDFResults(t *testing.T, results []client.ResultSet, nq int, reranked bool) {
	t.Helper()
	require.Len(t, results, nq)
	ids, scores := []int64{1, 2, 3}, []float32{0, 0.0001, 0.0004}
	if reranked {
		ids, scores = []int64{3, 2, 1}, []float32{60, 40, 20}
	}
	for _, result := range results {
		require.NoError(t, result.Err)
		require.Equal(t, 3, result.ResultCount)
		require.Equal(t, 3, result.IDs.Len())
		require.InDeltaSlice(t, scores, result.Scores, 0.000001)
		ts := result.GetColumn("ts")
		require.NotNil(t, ts)
		require.Equal(t, 3, ts.Len())
		for i, expected := range ids {
			id, err := result.IDs.GetAsInt64(i)
			require.NoError(t, err)
			require.Equal(t, expected, id)
			value, err := ts.GetAsInt64(i)
			require.NoError(t, err)
			require.Equal(t, expected*10, value)
		}
	}
}

func TestPyUDFSearchContracts(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	mc := createFileResourceMilvusClient(ctx, t)
	resource := registerPyUDFWheel(t, ctx, mc)
	name := common.GenRandomString("go_pyudf", 8)
	schema := entity.NewSchema().WithName(name).WithAutoID(false).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("ts").WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(2))
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(name, schema).WithConsistencyLevel(entity.ClStrong)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(name)))
	})
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name,
		column.NewColumnInt64("id", []int64{1, 2, 3}),
		column.NewColumnInt64("ts", []int64{10, 20, 30}),
		column.NewColumnFloatVector("vector", 2, [][]float32{{0, 0}, {0.01, 0}, {0.02, 0}})))
	require.NoError(t, err)
	flush, err := mc.Flush(ctx, client.NewFlushOption(name))
	require.NoError(t, err)
	require.NoError(t, flush.Await(ctx))
	build, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(name, "vector", index.NewFlatIndex(entity.L2)))
	require.NoError(t, err)
	require.NoError(t, build.Await(ctx))
	load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
	require.NoError(t, err)
	require.NoError(t, load.Await(ctx))
	search := func(nq int, chain *schemapb.FunctionChain) ([]client.ResultSet, error) {
		vectors := make([]entity.Vector, nq)
		for i := range nq {
			vectors[i] = entity.FloatVector{0, 0}
		}
		option := client.NewSearchOption(name, 3, vectors).WithANNSField("vector").
			WithOutputFields("ts").WithConsistencyLevel(entity.ClStrong)
		if chain == nil {
			return mc.Search(ctx, option)
		}
		return mc.Search(ctx, pyudfSearchOption{SearchOption: option, chain: chain})
	}
	// Wait only for initial FileResource synchronization, never for UDF errors.
	deadline := time.Now().Add(30 * time.Second)
	for {
		results, err := search(1, pyudfChain(resource, ""))
		if err == nil {
			assertPyUDFResults(t, results, 1, true)
			break
		}
		message := err.Error()
		transient := strings.Contains(message, "file resource snapshot is not ready") ||
			strings.Contains(message, "file resource") && strings.Contains(message, "not found")
		require.True(t, transient && time.Now().Before(deadline), "initial synchronization: %v", err)
		time.Sleep(200 * time.Millisecond)
	}
	for _, tc := range []struct {
		name, mode, message, level string
		nq                         int
		code                       int32
	}{
		{name: "single_query", nq: 1, level: "L0"},
		{name: "multiple_queries", nq: 2, level: "L2"},
		{name: "user_exception", nq: 1, level: "L1", mode: "exception", code: 2400, message: "controlled Go PyUDF failure"},
		{name: "wrong_rows", nq: 1, level: "L1", mode: "wrong_rows", code: 2400, message: "has 2 rows, expected 3"},
		{name: "missing_resource", nq: 1, level: "L1", code: 1100, message: "not found"},
		{name: "unsupported_stage", nq: 1, level: "L1", code: 1100, message: "stage"},
	} {
		t.Run(tc.level+"/search_"+tc.name, func(t *testing.T) {
			chain := pyudfChain(resource, tc.mode)
			if tc.name == "missing_resource" {
				chain = pyudfChain(common.GenRandomString("missing_pyudf", 8), "")
			}
			if tc.name == "unsupported_stage" {
				chain.Stage = schemapb.FunctionChainStage_FunctionChainStageL1Rerank
			}
			results, err := search(tc.nq, chain)
			if tc.code == 0 {
				require.NoError(t, err)
				assertPyUDFResults(t, results, tc.nq, true)
			} else {
				require.ErrorContains(t, err, tc.message)
				require.Equal(t, tc.code, client.ErrorCode(err))
				if tc.mode == "exception" {
					require.ErrorContains(t, err, "worker UDF_FAILED")
				}
				if tc.mode == "wrong_rows" {
					require.ErrorContains(t, err, "py_udf: output column 0 chunk 0")
				}
				require.Empty(t, results, "a rejected request must not return partial search results")
			}
			ordinary, err := search(1, nil)
			require.NoError(t, err)
			assertPyUDFResults(t, ordinary, 1, false)
			recovered, err := search(1, pyudfChain(resource, ""))
			require.NoError(t, err)
			assertPyUDFResults(t, recovered, 1, true)
		})
	}
	hybrid := func(nq int, chain *schemapb.FunctionChain) ([]client.ResultSet, error) {
		left, right := make([]entity.Vector, nq), make([]entity.Vector, nq)
		for i := range nq {
			left[i], right[i] = entity.FloatVector{0, 0}, entity.FloatVector{0.02, 0}
		}
		option := client.NewHybridSearchOption(name, 3,
			client.NewAnnRequest("vector", 3, left...), client.NewAnnRequest("vector", 3, right...)).
			WithOutputFields("ts").WithConsistencyLevel(entity.ClStrong)
		return mc.HybridSearch(ctx, pyudfHybridOption{HybridSearchOption: option, chain: chain})
	}
	merge := func() *schemapb.FunctionChainOp {
		return &schemapb.FunctionChainOp{Op: "merge", Params: map[string]*schemapb.FunctionParamValue{
			"strategy": pyudfString("rrf"),
			"k":        {Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: 60}},
		}}
	}
	// Opposite ANN orders yield independently known RRF scores before UDF mapping.
	fused := pyudfChain(resource, "")
	fused.Ops = []*schemapb.FunctionChainOp{merge(), fused.Ops[1]}
	baseline, err := hybrid(1, fused)
	require.NoError(t, err)
	require.Len(t, baseline, 1)
	require.Equal(t, 3, baseline[0].ResultCount)
	require.InDeltaSlice(t, []float32{1.0/61 + 1.0/63, 1.0/61 + 1.0/63, 2.0 / 62}, baseline[0].Scores, 0.000001)
	for i, id := range []int64{1, 3, 2} {
		actual, err := baseline[0].IDs.GetAsInt64(i)
		require.NoError(t, err)
		require.Equal(t, id, actual)
	}
	for _, tc := range []struct {
		name, mode, message, level string
		nq                         int
		code                       int32
	}{
		{name: "single_query", nq: 1, level: "L0"},
		{name: "multiple_queries", nq: 2, level: "L2"},
		{name: "missing_merge", nq: 1, level: "L1", code: 1100, message: "exactly one merge"},
		{name: "user_exception", nq: 1, level: "L1", mode: "exception", code: 2400, message: "controlled Go PyUDF failure"},
		{name: "wrong_rows", nq: 1, level: "L1", mode: "wrong_rows", code: 2400, message: "has 2 rows, expected 3"},
	} {
		t.Run(tc.level+"/hybrid_"+tc.name, func(t *testing.T) {
			chain := pyudfChain(resource, tc.mode)
			if tc.name != "missing_merge" {
				chain.Ops = append([]*schemapb.FunctionChainOp{merge()}, chain.Ops...)
			}
			results, err := hybrid(tc.nq, chain)
			if tc.code == 0 {
				require.NoError(t, err)
				assertPyUDFResults(t, results, tc.nq, true)
			} else {
				require.ErrorContains(t, err, tc.message)
				require.Equal(t, tc.code, client.ErrorCode(err))
				require.Empty(t, results)
			}
			recovered := pyudfChain(resource, "")
			recovered.Ops = append([]*schemapb.FunctionChainOp{merge()}, recovered.Ops...)
			results, err = hybrid(1, recovered)
			require.NoError(t, err)
			assertPyUDFResults(t, results, 1, true)
		})
	}
}

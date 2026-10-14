// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package testcases

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"io"
	"maps"
	"math/bits"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

const (
	rlsSchemaBinaryDim = 64
	// Keep mixed-segment iterator settings aligned with the Python RLS suite.
	rlsIteratorBatchSize = 127
	rlsHNSWEF            = 4096
)

// The oracle is application-owned row data; it is never populated from a server read.
type rlsSchemaRow map[string]any

type rlsSchemaCase struct {
	vector              string
	defaults, autoID    bool
	varchar, nullable   bool
	special, structured bool
	nullVector          bool
	dim                 int
	partitionKey        string
	clusteringKey       string
	isolation           bool
}

// Small fixtures deliberately use distinct distances for exact ranking and range boundaries.
func rlsSchemaVector(t *testing.T, kind string, n int) entity.Vector {
	t.Helper()
	v := entity.FloatVector{float32(n), 1, 2, 3, 4, 5, 6, 7}
	switch kind {
	case "binary":
		require.GreaterOrEqual(t, n, 1)
		require.LessOrEqual(t, n, rlsSchemaBinaryDim)
		v := make(entity.BinaryVector, rlsSchemaBinaryDim/8)
		for i := range n {
			v[i/8] |= 1 << (i % 8)
		}
		return v
	case "float16":
		return v.ToFloat16Vector()
	case "bfloat16":
		return v.ToBFloat16Vector()
	case "int8":
		return entity.Int8Vector{int8(n), 1, 2, 3, 4, 5, 6, 7}
	case "sparse":
		v, err := entity.NewSliceSparseEmbedding([]uint32{1, 3, 100}, []float32{float32(n), float32(65 - n), 2})
		require.NoError(t, err)
		return v
	default:
		return v
	}
}

func rlsVectorValue(v entity.Vector) any {
	switch v := v.(type) {
	case entity.FloatVector:
		return []float32(v)
	case entity.Float16Vector:
		return []byte(v)
	case entity.BFloat16Vector:
		return []byte(v)
	case entity.BinaryVector:
		return []byte(v)
	case entity.Int8Vector:
		return []int8(v)
	default:
		return v
	}
}

func (tc rlsSchemaCase) vectorDim() int {
	if tc.dim != 0 {
		return tc.dim
	}
	if tc.vector == "binary" {
		return rlsSchemaBinaryDim
	}
	return 8
}

func (tc rlsSchemaCase) vectorColumn() column.Column {
	switch tc.vector {
	case "binary":
		return column.NewColumnBinaryVector("vector", tc.vectorDim(), nil)
	case "float16":
		return column.NewColumnFloat16Vector("vector", tc.vectorDim(), nil)
	case "bfloat16":
		return column.NewColumnBFloat16Vector("vector", tc.vectorDim(), nil)
	case "int8":
		return column.NewColumnInt8Vector("vector", tc.vectorDim(), nil)
	case "sparse":
		return column.NewColumnSparseVectors("vector", nil)
	default:
		return column.NewColumnFloatVector("vector", tc.vectorDim(), nil)
	}
}

func rlsSchemaIndex(kind string) index.Index {
	switch kind {
	case "binary":
		return index.NewBinFlatIndex(entity.HAMMING)
	case "int8":
		return index.NewHNSWIndex(entity.L2, 16, 200)
	case "sparse":
		return index.NewSparseInvertedIndex(entity.IP, 0)
	default:
		return index.NewFlatIndex(entity.L2)
	}
}

func rlsSchemaColumns(t *testing.T, tc rlsSchemaCase) []column.Column {
	t.Helper()
	pk := column.Column(column.NewColumnInt64("id", nil))
	if tc.varchar {
		pk = column.NewColumnVarChar("id", nil)
	}
	vec := tc.vectorColumn()
	vec.SetNullable(tc.nullVector)
	cols := []column.Column{
		pk, column.NewColumnVarChar("owner", nil), vec,
		column.NewColumnBool("flag", nil), column.NewColumnInt8("i8", nil), column.NewColumnInt16("i16", nil), column.NewColumnInt32("i32", nil), column.NewColumnInt64("i64", nil),
		column.NewColumnFloat("f32", nil), column.NewColumnDouble("f64", nil), column.NewColumnVarChar("text", nil), column.NewColumnJSONBytes("doc", nil),
		column.NewColumnBoolArray("a_bool", nil), column.NewColumnInt8Array("a_i8", nil), column.NewColumnInt16Array("a_i16", nil), column.NewColumnInt32Array("a_i32", nil), column.NewColumnInt64Array("a_i64", nil), column.NewColumnFloatArray("a_f32", nil), column.NewColumnDoubleArray("a_f64", nil), column.NewColumnVarCharArray("a_text", nil),
	}
	for _, c := range cols[3:] {
		c.SetNullable(tc.nullable)
	}
	if tc.special {
		cols = append(cols, column.NewColumnGeometryWKT("shape", nil))
		cols = append(cols, column.NewColumnTimestamptzIsoString("instant", nil))
	}
	if tc.structured {
		c := column.NewColumnStructArray("objects", []column.Column{
			column.NewColumnVarCharArray("label", nil),
			column.NewColumnInt64Array("number", nil),
			column.NewColumnFloatVectorArray("embedding", 4, nil),
		})
		cols = append(cols, c)
	}
	return cols
}

func rlsSchemaData(t *testing.T, tc rlsSchemaCase, id, revision int, owner string) rlsSchemaRow {
	t.Helper()
	n := id + revision*10
	vec := rlsVectorValue(rlsSchemaVector(t, tc.vector, n))
	pk := any(int64(id))
	if tc.varchar {
		pk = fmt.Sprintf("用户/%03d", id)
	}
	r := rlsSchemaRow{
		"id": pk, "owner": owner, "vector": vec, "flag": n%2 == 0,
		"i8": int8(-n), "i16": int16(-300 - n), "i32": int32(100000 + n), "i64": 9007199254740993 + int64(n),
		"f32": float32(n) + 0.25, "f64": float64(n) + 0.125, "text": fmt.Sprintf("中文_%d_🧪", n),
		"doc":    []byte(fmt.Sprintf(`{"n":%d,"nested":{"owner":%q},"items":[1,null,"中"]}`, n, owner)),
		"a_bool": []bool{true, false}, "a_i8": []int8{-128, int8(n), 127}, "a_i16": []int16{-32768, int16(n), 32767}, "a_i32": []int32{-2147483648, int32(n), 2147483647},
		"a_i64": []int64{-9223372036854775808, int64(n), 9223372036854775807}, "a_f32": []float32{-0.5, float32(n)}, "a_f64": []float64{-0.25, float64(n)}, "a_text": []string{"", owner, "中文"},
	}
	if tc.nullable && (id+revision)%3 == 0 {
		for k := range r {
			if k != "id" && k != "owner" && k != "vector" {
				r[k] = nil
			}
		}
	}
	if tc.nullVector && (id+revision)%3 == 0 {
		r["vector"] = nil
	}
	if tc.special {
		r["shape"] = fmt.Sprintf("POINT (%d 1)", n)
		r["instant"] = time.Date(2024, 1, 1, 0, 0, n, 0, time.UTC).Format(time.RFC3339Nano)
	}
	if tc.structured {
		r["objects"] = map[string]any{"label": []string{owner, "中"}, "number": []int64{int64(n), -1}, "embedding": []entity.FloatVector{{float32(n), 1, 2, 3}, {1, 2, 3, 4}}}
	}
	return r
}

func rlsStructSchema() *entity.StructSchema {
	return entity.NewStructSchema().WithField(entity.NewField().WithName("label").WithDataType(entity.FieldTypeVarChar).WithMaxLength(256)).WithField(entity.NewField().WithName("number").WithDataType(entity.FieldTypeInt64)).WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(4))
}

func rlsSchemaWriteColumns(t *testing.T, tc rlsSchemaCase, rows ...rlsSchemaRow) []column.Column {
	t.Helper()
	var result []column.Column
	for _, c := range rlsSchemaColumns(t, tc) {
		if _, ok := rows[0][c.Name()]; !ok {
			continue
		}
		if c.Name() == "vector" && tc.nullVector {
			valid := make([]bool, len(rows))
			var floats [][]float32
			var bytes [][]byte
			var ints [][]int8
			var sparse []entity.SparseEmbedding
			for i, r := range rows {
				if r["vector"] == nil {
					continue
				}
				valid[i] = true
				switch v := r["vector"].(type) {
				case []float32:
					floats = append(floats, v)
				case []byte:
					bytes = append(bytes, v)
				case []int8:
					ints = append(ints, v)
				case entity.SparseEmbedding:
					sparse = append(sparse, v)
				}
			}
			var col column.Column
			var err error
			switch tc.vector {
			case "float32":
				col, err = column.NewNullableColumnFloatVector("vector", tc.vectorDim(), floats, valid)
			case "binary":
				col, err = column.NewNullableColumnBinaryVector("vector", tc.vectorDim(), bytes, valid)
			case "float16":
				col, err = column.NewNullableColumnFloat16Vector("vector", tc.vectorDim(), bytes, valid)
			case "bfloat16":
				col, err = column.NewNullableColumnBFloat16Vector("vector", tc.vectorDim(), bytes, valid)
			case "int8":
				col, err = column.NewNullableColumnInt8Vector("vector", tc.vectorDim(), ints, valid)
			case "sparse":
				col, err = column.NewNullableColumnSparseFloatVector("vector", sparse, valid)
			}
			require.NoError(t, err)
			result = append(result, col)
			continue
		}
		if c.Name() == "doc" && tc.nullable {
			var values [][]byte
			valid := make([]bool, len(rows))
			for i, r := range rows {
				if r["doc"] != nil {
					values = append(values, r["doc"].([]byte))
					valid[i] = true
				}
			}
			col, err := column.NewNullableColumnJSONBytes("doc", values, valid)
			require.NoError(t, err)
			result = append(result, col)
			continue
		}
		for _, r := range rows {
			if r[c.Name()] == nil {
				require.NoError(t, c.AppendNull(), "field=%s", c.Name())
			} else {
				require.NoError(t, c.AppendValue(r[c.Name()]), "field=%s", c.Name())
			}
		}
		result = append(result, c)
	}
	return result
}

func createRLSSchemaCollection(ctx context.Context, t *testing.T, mc *client.Client, tc rlsSchemaCase) string {
	t.Helper()
	name := common.GenRandomString("rls_schema_", 8)
	schema := entity.NewSchema().WithName(name)
	for _, c := range rlsSchemaColumns(t, tc) {
		f := entity.NewField().WithName(c.Name()).WithDataType(c.Type()).WithNullable(c.Nullable())
		f.WithIsPartitionKey(c.Name() == tc.partitionKey).WithIsClusteringKey(c.Name() == tc.clusteringKey)
		if c.Name() == "id" {
			f.WithIsPrimaryKey(true).WithIsAutoID(tc.autoID)
		}
		if c.Type() == entity.FieldTypeVarChar {
			f.WithMaxLength(256)
		}
		if tc.defaults {
			switch c.Name() {
			case "flag":
				f.WithDefaultValueBool(true)
			case "i8", "i16", "i32":
				f.WithDefaultValueInt(7)
			case "i64":
				f.WithDefaultValueLong(9007199254740993)
			case "f32":
				f.WithDefaultValueFloat(0.25)
			case "f64":
				f.WithDefaultValueDouble(0.125)
			case "text":
				f.WithDefaultValueString("default_中")
			case "owner":
				f.WithDefaultValueString("alice")
			}
		}
		if c.Type() == entity.FieldTypeArray {
			f.WithElementType(entity.FieldType(c.FieldData().GetScalars().GetArrayData().GetElementType())).WithMaxCapacity(32).WithMaxLength(256)
		}
		if c.Name() == "objects" {
			f.WithElementType(entity.FieldTypeStruct).WithStructSchema(rlsStructSchema())
		}
		if c.Name() == "vector" && tc.vector != "sparse" {
			f.WithDim(int64(tc.vectorDim()))
		}
		schema.WithField(f)
	}
	opt := client.NewCreateCollectionOption(name, schema).WithShardNum(1).WithProperty("rls.enabled", true).WithProperty("collection.autocompaction.enabled", false).WithConsistencyLevel(entity.ClStrong)
	if tc.partitionKey != "" {
		opt.WithNumPartitions(2).WithProperty("partitionkey.isolation", tc.isolation)
	}
	require.NoError(t, mc.CreateCollection(ctx, opt))
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanup, client.NewDropCollectionOption(name)))
	})
	createRLSPolicy(ctx, t, mc, name, "owner", "owner == $current_principal", "owner == $current_principal", rlsActions...)
	return name
}

func prepareRLSSchema(ctx context.Context, t *testing.T, mc *client.Client, tc rlsSchemaCase) string {
	t.Helper()
	name := createRLSSchemaCollection(ctx, t, mc, tc)
	idx := rlsSchemaIndex(tc.vector)
	task, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(name, "vector", idx))
	require.NoError(t, err)
	require.NoError(t, task.Await(ctx))
	if tc.structured {
		task, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(name, "objects[embedding]", index.NewHNSWIndex(entity.MaxSimCosine, 16, 200)))
		require.NoError(t, err)
		require.NoError(t, task.Await(ctx))
	}
	load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
	require.NoError(t, err)
	require.NoError(t, load.Await(ctx))
	return name
}

func checkRLSSchemaRows(t *testing.T, result client.ResultSet, want map[any]rlsSchemaRow, fields []string) []any {
	t.Helper()
	require.NoError(t, result.Err)
	ids := result.IDs
	if ids == nil {
		ids = result.GetColumn("id")
	}
	if result.Len() == 0 {
		return nil
	}
	require.NotNil(t, ids)
	// Resolve columns once; the mixed fixture checks thousands of complete rows.
	columns := make(map[string]column.Column, len(fields))
	for _, f := range fields {
		columns[f] = result.GetColumn(f)
		require.NotNil(t, columns[f], "missing output field=%s", f)
	}
	seen := make([]any, 0, result.Len())
	seenIDs := make(map[any]bool, result.Len())
	for i := range result.Len() {
		id, err := ids.Get(i)
		if err != nil {
			require.NoError(t, err)
		}
		expected, ok := want[id]
		if !ok {
			require.True(t, ok, "unexpected/hidden id=%v", id)
		}
		if seenIDs[id] {
			require.FailNow(t, "duplicate PK", "id=%v", id)
		}
		seenIDs[id] = true
		seen = append(seen, id)
		for _, f := range fields {
			c := columns[f]
			null, err := c.IsNull(i)
			if err != nil {
				require.NoError(t, err)
			}
			if (expected[f] == nil) != null {
				require.Equal(t, expected[f] == nil, null, "id=%v field=%s null", id, f)
			}
			if null {
				continue
			}
			actual, err := c.Get(i)
			if err != nil {
				require.NoError(t, err)
			}
			if f == "extra" {
				n, err := c.GetAsInt64(i)
				if err != nil {
					require.NoError(t, err)
				}
				require.Equal(t, expected[f], n)
			} else if f == "vector" && c.Type() == entity.FieldTypeSparseVector {
				wantBytes, gotBytes := expected[f].(entity.SparseEmbedding).Serialize(), actual.(entity.SparseEmbedding).Serialize()
				if !bytes.Equal(wantBytes, gotBytes) {
					require.Equal(t, wantBytes, gotBytes)
				}
			} else if f == "doc" {
				if !bytes.Equal(expected[f].([]byte), actual.([]byte)) {
					require.JSONEq(t, string(expected[f].([]byte)), string(actual.([]byte)), "id=%v", id)
				}
			} else if f == "instant" {
				wantTime, err := time.Parse(time.RFC3339Nano, expected[f].(string))
				if err != nil {
					require.NoError(t, err)
				}
				gotTime, err := time.Parse(time.RFC3339Nano, actual.(string))
				if err != nil {
					require.NoError(t, err)
				}
				require.True(t, wantTime.Equal(gotTime))
			} else {
				if vector, ok := actual.(entity.Vector); ok {
					actual = rlsVectorValue(vector)
				}
				if !reflect.DeepEqual(expected[f], actual) {
					require.EqualValues(t, expected[f], actual, "id=%v field=%s", id, f)
				}
			}
		}
	}
	return seen
}

// Count keys in linear time instead of pairwise matching thousands of rows.
func checkRLSSchemaIDs(t *testing.T, want, got []any) {
	t.Helper()
	wantCounts, gotCounts := map[any]int{}, map[any]int{}
	for _, id := range want {
		wantCounts[id]++
	}
	for _, id := range got {
		gotCounts[id]++
	}
	differences := map[any][2]int{}
	for id, count := range wantCounts {
		if gotCounts[id] != count {
			differences[id] = [2]int{count, gotCounts[id]}
		}
	}
	for id, count := range gotCounts {
		if wantCounts[id] != count {
			differences[id] = [2]int{wantCounts[id], count}
		}
	}
	require.Empty(t, differences, "PK -> [expected count, actual count]; expected rows=%d actual rows=%d", len(want), len(got))
}

// Every read checks exact IDs and complete values; iterator pages additionally check duplicates across pages.
func checkRLSSchemaReads(ctx context.Context, t *testing.T, mc *client.Client, name string, tc rlsSchemaCase, all map[any]rlsSchemaRow) {
	t.Helper()
	checkRLSSchemaState(ctx, t, mc, name, tc, all, true)
}

func checkRLSSchemaState(ctx context.Context, t *testing.T, mc *client.Client, name string, tc rlsSchemaCase, all map[any]rlsSchemaRow, allReads bool) {
	t.Helper()
	fields := []string{}
	for _, c := range rlsSchemaColumns(t, tc) {
		fields = append(fields, c.Name())
	}
	qv := rlsSchemaVector(t, tc.vector, 1)
	for _, principal := range []string{"alice", "bob", "nobody", ""} {
		t.Run("principal_"+principal, func(t *testing.T) {
			want := map[any]rlsSchemaRow{}
			for id, r := range all {
				if principal == "" || r["owner"] == principal {
					want[id] = r
				}
			}
			keys := slices.Collect(maps.Keys(want))
			searchWant := maps.Clone(want)
			maps.DeleteFunc(searchWant, func(_ any, r rlsSchemaRow) bool { return r["vector"] == nil })
			searchKeys := slices.Collect(maps.Keys(searchWant))
			checkSearch := func(t *testing.T, r client.ResultSet) {
				checkRLSSchemaIDs(t, searchKeys, checkRLSSchemaRows(t, r, searchWant, fields))
			}
			check := func(t *testing.T, r client.ResultSet, fs []string) {
				checkRLSSchemaIDs(t, keys, checkRLSSchemaRows(t, r, want, fs))
			}
			t.Run("query", func(t *testing.T) {
				r, e := mc.Query(ctx, client.NewQueryOption(name).WithFilter("owner != ''").WithOutputFields(fields...).WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong))
				require.NoError(t, e)
				check(t, r, fields)
			})
			if !allReads {
				return
			}
			t.Run("projection", func(t *testing.T) {
				fs := []string{"id", "doc", "a_i64", "f64"}
				r, e := mc.Query(ctx, client.NewQueryOption(name).WithFilter("owner != ''").WithOutputFields(fs...).WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong))
				require.NoError(t, e)
				check(t, r, fs)
			})
			t.Run("pagination", func(t *testing.T) {
				var got []any
				for offset := 0; offset < len(want)+2; offset += 2 {
					r, err := mc.Query(ctx, client.NewQueryOption(name).WithFilter("owner != ''").WithOffset(offset).WithLimit(2).WithOutputFields(fields...).WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong))
					if tc.structured && offset >= len(want) && err != nil && err.Error() == "FieldData type not matched" {
						checkRLSSchemaIDs(t, keys, got)
						t.Skipf("xfail: https://github.com/milvus-io/milvus/issues/54240, malformed empty StructArray page: %v", err)
					}
					require.NoError(t, err)
					got = append(got, checkRLSSchemaRows(t, r, want, fields)...)
					if r.Len() < 2 {
						break
					}
				}
				checkRLSSchemaIDs(t, keys, got)
			})
			t.Run("get", func(t *testing.T) {
				pk := rlsSchemaColumns(t, tc)[0]
				for id := range all {
					require.NoError(t, pk.AppendValue(id))
				}
				r, e := mc.Get(ctx, client.NewQueryOption(name).WithIDs(pk).WithOutputFields(fields...).WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong))
				require.NoError(t, e)
				check(t, r, fields)
			})
			t.Run("count", func(t *testing.T) {
				r, e := mc.Query(ctx, client.NewQueryOption(name).WithFilter("owner != ''").WithOutputFields("count(*)").WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong))
				require.NoError(t, e)
				n, e := r.GetColumn("count(*)").GetAsInt64(0)
				require.NoError(t, e)
				require.EqualValues(t, len(want), n)
			})
			t.Run("query_iterator", func(t *testing.T) {
				it, e := mc.QueryIterator(ctx, client.NewQueryIteratorOption(name).WithBatchSize(2).WithOutputFields(fields...).WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong))
				require.NoError(t, e)
				var got []any
				ended := false
				for range len(all) + 2 {
					r, e := it.Next(ctx)
					if errors.Is(e, io.EOF) {
						ended = true
						break
					}
					require.NoError(t, e)
					got = append(got, checkRLSSchemaRows(t, r, want, fields)...)
				}
				require.True(t, ended)
				checkRLSSchemaIDs(t, keys, got)
			})
			t.Run("search", func(t *testing.T) {
				queries := []entity.Vector{qv, rlsSchemaVector(t, tc.vector, 64)}
				rs, e := mc.Search(ctx, client.NewSearchOption(name, 2, queries).WithANNSField("vector").WithOutputFields(fields...).WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong))
				require.NoError(t, e)
				require.Len(t, rs, len(queries))
				for nq, r := range rs {
					expected := rlsRankedIDs(queries[nq], searchWant)
					got := checkRLSSchemaRows(t, r, searchWant, fields)
					if len(expected) == 0 {
						require.Empty(t, got)
					} else {
						require.Equal(t, expected[:min(2, len(expected))], got, "nq=%d ordered top K", nq)
					}
					require.Len(t, r.Scores, len(got))
					for i, id := range got {
						require.InDelta(t, rlsVectorDistance(queries[nq], want[id]["vector"]), r.Scores[i], 0.001, "nq=%d id=%v distance", nq, id)
					}
				}
			})
			t.Run("hybrid", func(t *testing.T) {
				rs, e := mc.HybridSearch(ctx, client.NewHybridSearchOption(name, 100, client.NewAnnRequest("vector", 100, qv), client.NewAnnRequest("vector", 100, qv).WithFilter("owner == 'bob'")).WithReranker(client.NewRRFReranker()).WithOutputFields(fields...).WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong))
				require.NoError(t, e)
				require.Len(t, rs, 1)
				checkSearch(t, rs[0])
			})
			t.Run("search_iterator", func(t *testing.T) {
				it, e := mc.SearchIterator(ctx, client.NewSearchIteratorOption(name, qv).WithANNSField("vector").WithBatchSize(2).WithOutputFields(fields...).WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong))
				require.NoError(t, e)
				var got []any
				ended := false
				for range len(all) + 2 {
					r, e := it.Next(ctx)
					if errors.Is(e, io.EOF) {
						ended = true
						break
					}
					require.NoError(t, e)
					got = append(got, checkRLSSchemaRows(t, r, searchWant, fields)...)
				}
				require.True(t, ended)
				checkRLSSchemaIDs(t, searchKeys, got)
			})
			if len(searchKeys) > 0 {
				t.Run("search_by_ids", func(t *testing.T) {
					t.Skip("SearchByIDs is not available in the 2.6 public API")
				})
			}
		})
	}
}

// Compute scores from every encoded component, independently of the server and row IDs.
func rlsVectorDistance(query entity.Vector, value any) float32 {
	var q, v []float32
	switch query := query.(type) {
	case entity.BinaryVector:
		distance := 0
		for i, b := range value.([]byte) {
			distance += bits.OnesCount8(b ^ query[i])
		}
		return float32(distance)
	case entity.SparseEmbedding:
		weights := make(map[uint32]float32, query.Len())
		for i := range query.Len() {
			pos, weight, _ := query.Get(i)
			weights[pos] = weight
		}
		var score float32
		stored := value.(entity.SparseEmbedding)
		for i := range stored.Len() {
			pos, weight, _ := stored.Get(i)
			score += weights[pos] * weight
		}
		return score
	case entity.Float16Vector:
		q, v = query.ToFloat32Vector(), entity.Float16Vector(value.([]byte)).ToFloat32Vector()
	case entity.BFloat16Vector:
		q, v = query.ToFloat32Vector(), entity.BFloat16Vector(value.([]byte)).ToFloat32Vector()
	case entity.Int8Vector:
		var score float32
		for i, n := range value.([]int8) {
			d := float32(n) - float32(query[i])
			score += d * d
		}
		return score
	case entity.FloatVector:
		q, v = query, value.([]float32)
	}
	var score float32
	for i, n := range v {
		d := n - q[i]
		score += d * d
	}
	return score
}

func rlsRankedIDs(query entity.Vector, rows map[any]rlsSchemaRow) []any {
	ids := slices.Collect(maps.Keys(rows))
	scores := make(map[any]float32, len(rows))
	for id, row := range rows {
		scores[id] = rlsVectorDistance(query, row["vector"])
	}
	_, sparse := query.(entity.SparseEmbedding)
	slices.SortFunc(ids, func(a, b any) int {
		order := cmp.Compare(scores[a], scores[b])
		if sparse {
			return -order
		}
		return order
	})
	return ids
}

// Six vector types and wide schemas exercise supported serializers with a full-value oracle.
func TestRLSSchemaReadWriteMatrix(t *testing.T) {
	for _, vector := range []string{"float32", "binary", "float16", "bfloat16", "int8", "sparse"} {
		for _, profile := range []string{"required_int64", "nullable_int64", "required_varchar", "nullable_varchar", "special", "structured", "nullable_vector"} {
			if vector != "float32" && (profile == "special" || profile == "structured") {
				continue
			}
			t.Run(vector+"/"+profile, func(t *testing.T) {
				// Collections and clients are independent; Flush retries the shared rate limit.
				t.Parallel()
				tc := rlsSchemaCase{
					vector:     vector,
					varchar:    profile == "required_varchar" || profile == "nullable_varchar",
					nullable:   profile == "nullable_int64" || profile == "nullable_varchar",
					special:    profile == "special",
					structured: profile == "structured",
					nullVector: profile == "nullable_vector",
				}
				ctx := hp.CreateContext(t, 8*time.Minute)
				mc := hp.CreateDefaultMilvusClient(ctx, t).Client
				name := prepareRLSSchema(ctx, t, mc, tc)
				all := map[any]rlsSchemaRow{}
				modes := []string{"column", "row"}
				if tc.special || tc.nullable || tc.nullVector {
					// Geometry and nullable row serialization are outside this CI matrix.
					modes = []string{"column"}
				}
				for _, mode := range modes {
					write := func(rows []rlsSchemaRow, upsert, partial bool, principal string) (writeErr error) {
						defer func() {
							if recovered := recover(); recovered != nil {
								writeErr = errors.Newf("SDK write panic: %v", recovered)
							}
						}()
						if mode == "row" {
							values := make([]any, len(rows))
							for i, r := range rows {
								values[i] = rlsSchemaRowInput(r)
							}
							opt := rlsRowWriteOption(name, principal, false, partial, values...)
							if upsert {
								_, e := mc.Upsert(ctx, opt)
								return e
							}
							_, e := mc.Insert(ctx, opt)
							return e
						}
						opt := client.NewColumnBasedInsertOption(name, rlsSchemaWriteColumns(t, tc, rows...)...).WithRLSPrincipal(principal).WithPartialUpdate(partial)
						if upsert {
							_, e := mc.Upsert(ctx, opt)
							return e
						}
						_, e := mc.Insert(ctx, opt)
						return e
					}
					t.Run(mode, func(t *testing.T) {
						if tc.structured && mode == "row" {
							t.Skip("StructArray row serialization is not supported by the 2.6 Go SDK")
						}
						for _, stage := range []string{"insert", "full_upsert", "partial_upsert", "rejected_insert", "rejected_upsert", "rejected_partial", "hidden_upsert", "hidden_partial", "transfer_owner", "delete_ids", "delete_filter", "delete_template"} {
							stagePassed := t.Run(stage, func(t *testing.T) {
								switch stage {
								case "insert", "full_upsert":
									rev := 0
									if stage == "full_upsert" {
										rev = 1
									}
									for _, owner := range []string{"alice", "bob"} {
										var rows []rlsSchemaRow
										for id := 1; id <= 8; id++ {
											if (id%2 == 1) == (owner == "alice") {
												rows = append(rows, rlsSchemaData(t, tc, id, rev, owner))
											}
										}
										err := write(rows, stage == "full_upsert", false, owner)
										require.NoError(t, err)
										for _, r := range rows {
											all[r["id"]] = r
										}
									}
								case "partial_upsert":
									for _, owner := range []string{"alice", "bob"} {
										r := rlsSchemaData(t, tc, 1, 2, owner)
										if owner == "bob" {
											r = rlsSchemaData(t, tc, 2, 2, owner)
										}
										patch := rlsSchemaRow{"id": r["id"], "doc": r["doc"], "a_i64": r["a_i64"], "f64": r["f64"]}
										err := write([]rlsSchemaRow{patch}, true, true, owner)
										checkRLSStructArrayUpsertError(t, err)
										for k, v := range patch {
											all[r["id"]][k] = v
										}
									}
								case "hidden_upsert", "hidden_partial", "transfer_owner":
									id, owner := 2, "alice"
									if stage == "transfer_owner" {
										id, owner = 1, "bob"
									}
									r := rlsSchemaData(t, tc, id, 3, owner)
									if stage == "hidden_partial" {
										r = rlsSchemaRow{"id": r["id"], "f64": float64(999)}
									}
									require.ErrorContains(t, write([]rlsSchemaRow{r}, true, stage == "hidden_partial", "alice"), "RLS")
								case "rejected_insert", "rejected_upsert", "rejected_partial":
									rows := []rlsSchemaRow{rlsSchemaData(t, tc, 20, 0, "alice"), rlsSchemaData(t, tc, 21, 0, "bob")}
									require.ErrorContains(t, write(rows, stage != "rejected_insert", stage == "rejected_partial", "alice"), "RLS")
								default:
									opt := client.NewDeleteOption(name).WithRLSPrincipal("alice")
									switch stage {
									case "delete_ids":
										if tc.varchar {
											opt.WithStringIDs("id", []string{"用户/001", "用户/002"})
										} else {
											opt.WithInt64IDs("id", []int64{1, 2})
										}
										delete(all, rlsSchemaData(t, tc, 1, 0, "alice")["id"])
									case "delete_filter":
										opt.WithExpr("i8 <= -13 and i8 >= -14")
										maps.DeleteFunc(all, func(_ any, r rlsSchemaRow) bool {
											n, ok := r["i8"].(int8)
											return ok && r["owner"] == "alice" && n <= -13 && n >= -14
										})
									case "delete_template":
										t.Skip("Delete expression templates are not available in the 2.6 Go SDK")
										for id, r := range all {
											if r["owner"] == "alice" {
												delete(all, id)
											}
										}
									}
									_, err := mc.Delete(ctx, opt)
									require.NoError(t, err)
								}
								// Rejected writes still compare every field for both owners and the administrator.
								allReads := stage == "insert" || stage == "partial_upsert" || stage == "delete_template"
								t.Run("live", func(t *testing.T) { checkRLSSchemaState(ctx, t, mc, name, tc, all, allReads) })
								if stage == "full_upsert" || stage == "partial_upsert" {
									rlsFlush(ctx, t, mc, name)
									t.Run("flushed", func(t *testing.T) { checkRLSSchemaState(ctx, t, mc, name, tc, all, stage == "partial_upsert") })
								}
							})
							if !stagePassed {
								return
							}
						}
						// Reset via an authorized administrative delete before exercising the other serialization path.
						_, err := mc.Delete(ctx, client.NewDeleteOption(name).WithExpr("owner != ''").WithSkipRLS(true))
						require.NoError(t, err)
						clear(all)
					})
				}
			})
		}
	}
}

// every scalar array operator checks complete rows and mixed-owner atomicity.
func TestRLSSchemaPartialOperators(t *testing.T) {
	for _, mode := range []string{"column", "row"} {
		t.Run(mode, func(t *testing.T) {
			ctx := hp.CreateContext(t, 10*time.Minute)
			mc := hp.CreateDefaultMilvusClient(ctx, t).Client
			// Scalar ARRAY operators do not require StructArray; its inheritance is covered by the schema matrix.
			tc := rlsSchemaCase{vector: "float32"}
			name := prepareRLSSchema(ctx, t, mc, tc)
			all := map[any]rlsSchemaRow{}
			for i, owner := range []string{"alice", "bob"} {
				r := rlsSchemaData(t, tc, i+1, 0, owner)
				_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name, rlsSchemaWriteColumns(t, tc, r)...).WithRLSPrincipal(owner))
				require.NoError(t, err)
				all[r["id"]] = r
			}
			tests := []struct {
				name                                       string
				seed, operand, appended, removed, replaced any
			}{
				{"a_bool", []bool{false}, []bool{true}, []bool{false, true}, []bool{false}, []bool{true}},
				{"a_i8", []int8{1, 2}, []int8{99}, []int8{1, 2, 99}, []int8{1, 2}, []int8{99, 2}},
				{"a_i16", []int16{1, 2}, []int16{99}, []int16{1, 2, 99}, []int16{1, 2}, []int16{99, 2}},
				{"a_i32", []int32{1, 2}, []int32{99}, []int32{1, 2, 99}, []int32{1, 2}, []int32{99, 2}},
				{"a_i64", []int64{1, 2}, []int64{99}, []int64{1, 2, 99}, []int64{1, 2}, []int64{99, 2}},
				{"a_f32", []float32{1, 2}, []float32{99}, []float32{1, 2, 99}, []float32{1, 2}, []float32{99, 2}},
				{"a_f64", []float64{1, 2}, []float64{99}, []float64{1, 2, 99}, []float64{1, 2}, []float64{99, 2}},
				{"a_text", []string{"one", "two"}, []string{"中"}, []string{"one", "two", "中"}, []string{"one", "two"}, []string{"中", "two"}},
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					_, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(name, rlsSchemaWriteColumns(t, tc, rlsSchemaRow{"id": int64(1), test.name: test.seed})...).WithPartialUpdate(true).WithRLSPrincipal("alice"))
					require.NoError(t, err)
					all[int64(1)][test.name] = test.seed
					for _, op := range []string{"append", "remove", "path_replace"} {
						t.Run(op, func(t *testing.T) {
							write := func(ids []int64) error {
								rows := make([]rlsSchemaRow, len(ids))
								values := make([]any, len(ids))
								for i, id := range ids {
									rows[i] = rlsSchemaRow{"id": id, test.name: test.operand}
									values[i] = map[string]any(rows[i])
								}
								if mode == "row" {
									o := client.NewRowBasedInsertOption(name, values...).WithRLSPrincipal("alice")
									switch op {
									case "append":
										o.WithArrayAppend(test.name)
									case "remove":
										o.WithArrayRemove(test.name)
									default:
										t.Skip("Path replacement is not available in the 2.6 public API")
									}
									_, e := mc.Upsert(ctx, o)
									return e
								}
								o := client.NewColumnBasedInsertOption(name, rlsSchemaWriteColumns(t, tc, rows...)...).WithRLSPrincipal("alice")
								switch op {
								case "append":
									o.WithArrayAppend(test.name)
								case "remove":
									o.WithArrayRemove(test.name)
								default:
									t.Skip("Path replacement is not available in the 2.6 public API")
								}
								_, e := mc.Upsert(ctx, o)
								return e
							}
							require.ErrorContains(t, write([]int64{1, 2}), "RLS")
							t.Run("rejected_mixed", func(t *testing.T) { checkRLSSchemaState(ctx, t, mc, name, tc, all, false) })
							require.NoError(t, write([]int64{1}))
							switch op {
							case "append":
								all[int64(1)][test.name] = test.appended
							case "remove":
								all[int64(1)][test.name] = test.removed
							default:
								all[int64(1)][test.name] = test.replaced
							}
							t.Run("accepted", func(t *testing.T) { checkRLSSchemaState(ctx, t, mc, name, tc, all, false) })
						})
					}
				})
			}
			for _, field := range []string{"doc", "objects"} {
				t.Run(field+"_path", func(t *testing.T) {
					t.Skip("Path replacement is not available in the 2.6 public API")
				})
			}
			rlsFlush(ctx, t, mc, name)
			t.Run("flushed", func(t *testing.T) { checkRLSSchemaReads(ctx, t, mc, name, tc, all) })
		})
	}
}

func checkRLSStructArrayUpsertError(t *testing.T, err error) {
	t.Helper()
	if err != nil && strings.Contains(err.Error(), "the dim (0) of field data(objects[embedding])") && strings.Contains(err.Error(), "[actual=0]") {
		t.Skipf("xfail: https://github.com/milvus-io/milvus/issues/54241, inherited StructArray vector dimension is zero: %v", err)
	}
	require.NoError(t, err)
}

func operandBytes(value any) []byte {
	b, _ := value.([]byte)
	return b
}

// defaults must materialize before CHECK and preserve unrelated fields on partial updates.
func TestRLSSchemaDefaults(t *testing.T) {
	defaults := rlsSchemaRow{"owner": "alice", "flag": true, "i8": int8(7), "i16": int16(7), "i32": int32(7), "i64": int64(9007199254740993), "f32": float32(0.25), "f64": float64(0.125), "text": "default_中"}
	for _, mode := range []string{"column"} {
		t.Run(mode, func(t *testing.T) {
			ctx := hp.CreateContext(t, 8*time.Minute)
			mc := hp.CreateDefaultMilvusClient(ctx, t).Client
			tc := rlsSchemaCase{vector: "float32", nullable: true, defaults: true}
			name := prepareRLSSchema(ctx, t, mc, tc)
			all := map[any]rlsSchemaRow{}
			bob := rlsSchemaData(t, tc, 2, 0, "bob")
			_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name, rlsSchemaWriteColumns(t, tc, bob)...).WithRLSPrincipal("bob"))
			require.NoError(t, err)
			all[bob["id"]] = bob
			for _, stage := range []string{"omitted_insert", "null_insert", "explicit_insert", "omitted_full_upsert", "null_partial_upsert", "rejected_check"} {
				t.Run(stage, func(t *testing.T) {
					defer func() {
						if p := recover(); p != nil {
							t.Errorf("SDK write panic: %v", p)
						}
					}()
					id := 1
					if stage == "null_insert" {
						id = 3
					}
					if stage == "explicit_insert" {
						id = 4
					}
					if stage == "rejected_check" {
						id = 5
					}
					r := rlsSchemaData(t, tc, id, 1, "alice")
					expected := maps.Clone(r)
					for k, v := range defaults {
						switch stage {
						case "omitted_insert", "omitted_full_upsert":
							delete(r, k)
							expected[k] = v
						case "null_insert":
							if k != "owner" {
								r[k] = nil
								expected[k] = v
							}
						}
					}
					partial := stage == "null_partial_upsert"
					upsert := partial || stage == "omitted_full_upsert"
					if partial {
						r = rlsSchemaRow{"id": int64(1), "f64": nil, "doc": nil}
						require.Contains(t, all, int64(1), "earlier insert failed; partial update cannot be exercised")
						expected = maps.Clone(all[int64(1)])
						expected["f64"] = float64(0.125)
						expected["doc"] = nil
					}
					if stage == "rejected_check" {
						r["owner"] = "bob"
					}
					if mode == "column" {
						o := client.NewColumnBasedInsertOption(name, rlsSchemaWriteColumns(t, tc, r)...).WithRLSPrincipal("alice").WithPartialUpdate(partial)
						if upsert {
							_, err = mc.Upsert(ctx, o)
						} else {
							_, err = mc.Insert(ctx, o)
						}
					} else {
						o := rlsRowWriteOption(name, "alice", false, partial, rlsSchemaRowInput(r))
						if upsert {
							_, err = mc.Upsert(ctx, o)
						} else {
							_, err = mc.Insert(ctx, o)
						}
					}
					if stage == "rejected_check" {
						require.ErrorContains(t, err, "RLS")
					} else {
						require.NoError(t, err)
						all[expected["id"]] = expected
					}
					checkRLSSchemaReads(ctx, t, mc, name, tc, all)
				})
			}
			rlsFlush(ctx, t, mc, name)
			t.Run("flushed", func(t *testing.T) { checkRLSSchemaReads(ctx, t, mc, name, tc, all) })
		})
	}
}

// generated primary keys are opaque; all application-owned payload fields remain exact.
func TestRLSSchemaAutoID(t *testing.T) {
	for _, varchar := range []bool{false, true} {
		for _, mode := range []string{"column", "row"} {
			t.Run(fmt.Sprintf("varchar_%v/%s", varchar, mode), func(t *testing.T) {
				ctx := hp.CreateContext(t, 6*time.Minute)
				mc := hp.CreateDefaultMilvusClient(ctx, t).Client
				tc := rlsSchemaCase{vector: "float32", varchar: varchar, autoID: true}
				name := prepareRLSSchema(ctx, t, mc, tc)
				all := map[any]rlsSchemaRow{}
				for i, owner := range []string{"alice", "bob", "alice", "bob"} {
					r := rlsSchemaData(t, tc, i+1, 0, owner)
					delete(r, "id")
					var result client.InsertResult
					var err error
					if mode == "row" {
						result, err = mc.Insert(ctx, client.NewRowBasedInsertOption(name, rlsSchemaRowInput(r)).WithRLSPrincipal(owner))
					} else {
						result, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(name, rlsSchemaWriteColumns(t, tc, r)...).WithRLSPrincipal(owner))
					}
					require.NoError(t, err)
					require.EqualValues(t, 1, result.InsertCount)
					id, err := result.IDs.Get(0)
					require.NoError(t, err)
					require.NotContains(t, all, id)
					r["id"] = id
					all[id] = r
				}
				t.Run("insert", func(t *testing.T) { checkRLSSchemaReads(ctx, t, mc, name, tc, all) })
				for _, partial := range []bool{false, true} {
					t.Run(fmt.Sprintf("upsert_partial_%v", partial), func(t *testing.T) {
						// The 2.6 server replaces AutoID PKs; snapshot the IDs before changing the oracle.
						for _, id := range slices.Collect(maps.Keys(all)) {
							old := all[id]
							if old["owner"] != "alice" {
								continue
							}
							r := maps.Clone(old)
							r["text"] = "updated_中"
							patch := r
							if partial {
								patch = rlsSchemaRow{"id": id, "text": "updated_中"}
							}
							var result client.UpsertResult
							var err error
							if mode == "row" {
								// Row-based 2.6 options otherwise omit an AutoID primary key.
								option := client.NewRowBasedInsertOption(name, map[string]any(patch)).WithKeepAutoIDPk(true).WithRLSPrincipal("alice")
								option.WithPartialUpdate(partial)
								result, err = mc.Upsert(ctx, option)
							} else {
								result, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(name, rlsSchemaWriteColumns(t, tc, patch)...).WithPartialUpdate(partial).WithRLSPrincipal("alice"))
							}
							require.NoError(t, err)
							require.EqualValues(t, 1, result.UpsertCount)
							newID, err := result.IDs.Get(0)
							require.NoError(t, err)
							require.NotContains(t, all, newID)
							delete(all, id)
							r["id"] = newID
							all[newID] = r
						}
						checkRLSSchemaReads(ctx, t, mc, name, tc, all)
					})
				}
				_, err := mc.Delete(ctx, client.NewDeleteOption(name).WithExpr("owner != ''").WithRLSPrincipal("alice"))
				require.NoError(t, err)
				maps.DeleteFunc(all, func(_ any, r rlsSchemaRow) bool { return r["owner"] == "alice" })
				t.Run("delete", func(t *testing.T) { checkRLSSchemaReads(ctx, t, mc, name, tc, all) })
				rlsFlush(ctx, t, mc, name)
				t.Run("flushed", func(t *testing.T) { checkRLSSchemaReads(ctx, t, mc, name, tc, all) })
			})
		}
	}
}

func rlsSchemaRowInput(r rlsSchemaRow) map[string]any {
	return maps.Clone(map[string]any(r))
}

// specialized reads verify application-owned state after each mutation.
func TestRLSSchemaSearchVariants(t *testing.T) {
	for _, vector := range []string{"float32", "binary", "float16", "bfloat16", "int8", "sparse"} {
		for _, varchar := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/varchar_%v", vector, varchar), func(t *testing.T) {
				t.Parallel()
				ctx := hp.CreateContext(t, 5*time.Minute)
				mc := hp.CreateDefaultMilvusClient(ctx, t).Client
				tc := rlsSchemaCase{vector: vector, varchar: varchar}
				name := prepareRLSSchema(ctx, t, mc, tc)
				all := map[any]rlsSchemaRow{}
				var fields []string
				for _, c := range rlsSchemaColumns(t, tc) {
					fields = append(fields, c.Name())
				}
				qv := rlsSchemaVector(t, vector, 1)
				for i := 1; i <= 8; i++ {
					owner := "alice"
					if i%2 == 0 {
						owner = "bob"
					}
					r := rlsSchemaData(t, tc, i, 0, owner)
					_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name, rlsSchemaWriteColumns(t, tc, r)...).WithRLSPrincipal(owner))
					require.NoError(t, err)
					all[r["id"]] = r
				}
				for _, stage := range []string{"insert", "full_upsert", "partial_upsert", "delete", "flushed"} {
					t.Run(stage, func(t *testing.T) {
						changed := rlsSchemaData(t, tc, 1, 1, "alice")
						switch stage {
						case "full_upsert":
							_, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(name, rlsSchemaWriteColumns(t, tc, changed)...).WithRLSPrincipal("alice"))
							require.NoError(t, err)
							all[changed["id"]] = changed
						case "partial_upsert":
							patch := rlsSchemaRow{"id": changed["id"], "f64": float64(99.125)}
							_, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(name, rlsSchemaWriteColumns(t, tc, patch)...).WithPartialUpdate(true).WithRLSPrincipal("alice"))
							require.NoError(t, err)
							all[changed["id"]]["f64"] = patch["f64"]
						case "delete":
							opt := client.NewDeleteOption(name).WithRLSPrincipal("alice")
							if varchar {
								opt.WithStringIDs("id", []string{changed["id"].(string)})
							} else {
								opt.WithInt64IDs("id", []int64{changed["id"].(int64)})
							}
							_, err := mc.Delete(ctx, opt)
							require.NoError(t, err)
							delete(all, changed["id"])
						case "flushed":
							rlsFlush(ctx, t, mc, name)
						}
						for _, principal := range []string{"alice", "bob", "nobody", ""} {
							t.Run("principal_"+principal, func(t *testing.T) {
								want := maps.Clone(all)
								maps.DeleteFunc(want, func(_ any, r rlsSchemaRow) bool { return principal != "" && r["owner"] != principal })
								for _, mode := range []string{"range", "group", "aggregation", "ordered_query", "template_query"} {
									t.Run(mode, func(t *testing.T) {
										if mode == "ordered_query" || mode == "template_query" {
											o := client.NewQueryOption(name).WithFilter("i32 >= {minimum}").WithTemplateParam("minimum", int64(100000)).WithOutputFields(fields...).WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong)
											if mode == "ordered_query" {
												t.Skip("Query ordering is not available in the 2.6 public API")
											}
											r, err := mc.Query(ctx, o)
											require.NoError(t, err)
											ids := checkRLSSchemaRows(t, r, want, fields)
											checkRLSSchemaIDs(t, slices.Collect(maps.Keys(want)), ids)
											if mode == "ordered_query" {
												for i := 1; i < len(ids); i++ {
													require.Greater(t, want[ids[i-1]]["f64"].(float64), want[ids[i]]["f64"].(float64))
												}
											}
											return
										}
										o := client.NewSearchOption(name, 100, []entity.Vector{qv}).WithANNSField("vector").WithOutputFields(fields...).WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong)
										rangeWant := maps.Clone(want)
										switch mode {
										case "range":
											params := index.NewCustomAnnParam()
											threshold := float32(9)
											if vector == "binary" {
												threshold = 2
											}
											if vector == "sparse" {
												params.WithRadius(3900)
												params.WithRangeFilter(4200)
												maps.DeleteFunc(rangeWant, func(_ any, r rlsSchemaRow) bool {
													d := rlsVectorDistance(qv, r["vector"])
													return d <= 3900 || d > 4200
												})
											} else {
												params.WithRadius(float64(threshold))
												params.WithRangeFilter(0)
												maps.DeleteFunc(rangeWant, func(_ any, r rlsSchemaRow) bool { return rlsVectorDistance(qv, r["vector"]) >= threshold })
											}
											o.WithAnnParam(params)
										case "group":
											o.WithGroupByField("owner")
										case "aggregation":
											t.Skip("Search aggregation is not available in the 2.6 public API")
										}
										rs, err := mc.Search(ctx, o)
										if mode == "group" && vector == "binary" {
											require.ErrorContains(t, err, "not support search_group_by operation based on binary vector column")
											return // Binary vectors reject legacy group-by search.
										}
										require.NoError(t, err)
										require.Len(t, rs, 1)
										r := rs[0]
										require.NoError(t, r.Err)
										if mode == "range" {
											checkRLSSchemaIDs(t, slices.Collect(maps.Keys(rangeWant)), checkRLSSchemaRows(t, r, rangeWant, fields))
											return
										}
										owners := map[string][]any{}
										for id, row := range want {
											owner := row["owner"].(string)
											owners[owner] = append(owners[owner], id)
										}
										if mode == "group" {
											ids := checkRLSSchemaRows(t, r, want, fields)
											require.Len(t, ids, len(owners))
											seen := map[string]bool{}
											for i, id := range ids {
												owner := want[id]["owner"].(string)
												require.False(t, seen[owner])
												seen[owner] = true
												g, e := r.GroupByValue.Get(i)
												require.NoError(t, e)
												require.Equal(t, owner, g)
												groupRows := map[any]rlsSchemaRow{}
												for _, candidate := range owners[owner] {
													groupRows[candidate] = want[candidate]
												}
												require.Equal(t, rlsRankedIDs(qv, groupRows)[0], id, "group=%s nearest row", owner)

											}
											return
										}
									})
								}
							})
						}
					})
				}
			})
		}
	}
}

// Generate concrete tagged struct fields for each vector representation, without six duplicate row types.
func rlsSchemaStructInput(r rlsSchemaRow) any {
	names := slices.Sorted(maps.Keys(r))
	fields := make([]reflect.StructField, len(names))
	for i, name := range names {
		fields[i] = reflect.StructField{Name: fmt.Sprintf("Field%d", i), Type: reflect.TypeOf(r[name]), Tag: reflect.StructTag(fmt.Sprintf(`milvus:"name:%s"`, name))}
	}
	value := reflect.New(reflect.StructOf(fields))
	for i, name := range names {
		value.Elem().Field(i).Set(reflect.ValueOf(r[name]))
	}
	return value.Interface()
}

// existing/new mixed upsert batches exercise all vectors and both PK types through all serializers.
func TestRLSSchemaUpsertCreatesRows(t *testing.T) {
	for _, vector := range []string{"float32", "binary", "float16", "bfloat16", "int8", "sparse"} {
		for _, varchar := range []bool{false, true} {
			for _, mode := range []string{"column", "row_map", "row_struct"} {
				t.Run(fmt.Sprintf("%s/varchar_%v/%s", vector, varchar, mode), func(t *testing.T) {
					t.Parallel()
					ctx := hp.CreateContext(t, 6*time.Minute)
					mc := hp.CreateDefaultMilvusClient(ctx, t).Client
					tc := rlsSchemaCase{vector: vector, varchar: varchar}
					name := prepareRLSSchema(ctx, t, mc, tc)
					all := map[any]rlsSchemaRow{}
					for i, owner := range []string{"alice", "bob", "alice", "bob"} {
						r := rlsSchemaData(t, tc, i+1, 0, owner)
						_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name, rlsSchemaWriteColumns(t, tc, r)...).WithRLSPrincipal(owner))
						require.NoError(t, err)
						all[r["id"]] = r
					}
					for _, stage := range []string{"full_mixed", "partial_create", "partial_mixed", "rejected_mixed"} {
						t.Run(stage, func(t *testing.T) {
							ids := []int{1, 5}
							partial := false
							switch stage {
							case "partial_create":
								ids = []int{7}
								partial = true
							case "partial_mixed":
								ids = []int{3, 9}
								partial = true
							case "rejected_mixed":
								ids = []int{1, 2}
								partial = true
							}
							rows := make([]rlsSchemaRow, len(ids))
							values := make([]any, len(ids))
							for i, id := range ids {
								rows[i] = rlsSchemaData(t, tc, id, 2, "alice")
								if mode == "row_struct" {
									values[i] = rlsSchemaStructInput(rows[i])
								} else {
									values[i] = rlsSchemaRowInput(rows[i])
								}
							}
							var option client.UpsertOption
							if mode == "column" {
								option = client.NewColumnBasedInsertOption(name, rlsSchemaWriteColumns(t, tc, rows...)...).WithPartialUpdate(partial).WithRLSPrincipal("alice")
							} else {
								option = rlsRowWriteOption(name, "alice", false, partial, values...)
							}
							result, err := mc.Upsert(ctx, option)
							if stage == "rejected_mixed" {
								require.ErrorContains(t, err, "RLS")
							} else {
								require.NoError(t, err)
								require.EqualValues(t, len(rows), result.UpsertCount)
								for _, r := range rows {
									all[r["id"]] = r
								}
							}
							checkRLSSchemaReads(ctx, t, mc, name, tc, all)
						})
					}
					rlsFlush(ctx, t, mc, name)
					t.Run("flushed", func(t *testing.T) { checkRLSSchemaReads(ctx, t, mc, name, tc, all) })
				})
			}
		}
	}
}

// Match the shared fixture: indexed sealed 3000, unindexed sealed 500, growing 500.
func prepareRLSMixedSegments(ctx context.Context, t *testing.T, mc *client.Client, tc rlsSchemaCase) (string, map[any]rlsSchemaRow) {
	t.Helper()
	// Default CI uses a 1024-row index threshold; verify segment state through public APIs below.
	name := createRLSSchemaCollection(ctx, t, mc, tc)
	copies := 1
	var partitionValues []any
	if tc.partitionKey != "" {
		copies = 2
		partitionValues = rlsPartitionValues(t, tc.partitionKey)
	}
	all := make(map[any]rlsSchemaRow, 4000*copies)
	vectors := rlsRandomVectorColumn(tc, 4000*copies)
	seenVectors := make(map[string]bool, 4000*copies)
	for _, batch := range []struct{ start, count int }{{0, 3000 * copies}, {3000 * copies, 500 * copies}, {3500 * copies, 500 * copies}} {
		rows := make([]rlsSchemaRow, batch.count)
		for i := range rows {
			id := batch.start + i + 1
			owner := "alice"
			if id%2 == 0 {
				owner = "bob"
			}
			// Scalar boundary fixtures stay small; PKs and vectors span the entire dataset.
			r := rlsSchemaData(t, tc, id%50+1, 0, owner)
			if tc.partitionKey != "" {
				r[tc.partitionKey] = partitionValues[((id-1)/2)%2]
			}
			if tc.clusteringKey != "" {
				divisor := 2
				if tc.partitionKey != "" {
					divisor = 4
				}
				r[tc.clusteringKey] = rlsKeyValue(tc.clusteringKey, ((id-1)/divisor)%2)
			}
			r["id"] = int64(id)
			if tc.varchar {
				r["id"] = fmt.Sprintf("用户/%04d", id)
			}
			value, err := vectors.Get(id - 1)
			require.NoError(t, err)
			r["vector"] = rlsVectorValue(rlsQueryVector(tc.vector, value))
			encoded := string(rlsQueryVector(tc.vector, value).Serialize())
			require.False(t, seenVectors[encoded], "duplicate encoded vector at id=%d", id)
			seenVectors[encoded] = true
			rows[i], all[r["id"]] = r, r
		}
		_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name, rlsSchemaWriteColumns(t, tc, rows...)...).WithSkipRLS(true))
		require.NoError(t, err)
		if batch.start < 3500*copies {
			rlsFlush(ctx, t, mc, name)
		}
		if batch.start == 3000*copies {
			idx := rlsSchemaIndex(tc.vector)
			switch tc.vector {
			case "binary":
				idx = index.NewBinIvfFlatIndex(entity.HAMMING, 64)
			case "float32", "float16", "bfloat16":
				idx = index.NewIvfFlatIndex(entity.L2, 64)
			}
			if tc.isolation {
				idx = index.NewHNSWIndex(entity.L2, 16, 200)
			}
			task, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(name, "vector", idx))
			require.NoError(t, err)
			require.NoError(t, task.Await(ctx))
			load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
			require.NoError(t, err)
			require.NoError(t, load.Await(ctx))
		}
	}
	checkRLSMixedSegments(ctx, t, mc, name, copies)
	return name, all
}

func rlsKeyValue(field string, n int) any {
	if field == "i64" {
		return int64(n)
	}
	return fmt.Sprintf("bucket_%d", n)
}

// Choose one key per physical partition; both owners share both keys.
func rlsPartitionValues(t *testing.T, field string) []any {
	t.Helper()
	values := make([]any, 2)
	for n := range 100 {
		value := rlsKeyValue(field, n)
		var c column.Column = column.NewColumnVarChar(field, []string{fmt.Sprint(value)})
		if field == "i64" {
			c = column.NewColumnInt64(field, []int64{value.(int64)})
		}
		buckets, err := typeutil.HashKey2Partitions(c.FieldData(), []string{"_default_0", "_default_1"})
		require.NoError(t, err)
		values[buckets[0]] = value
		if values[0] != nil && values[1] != nil {
			return values
		}
	}
	t.Fatal("could not choose distinct physical partitions")
	return nil
}

func rlsRandomVectorColumn(tc rlsSchemaCase, count int) column.Column {
	return hp.GenColumnData(count, tc.vectorColumn().Type(), *hp.TNewDataOption().TWithDim(tc.vectorDim()).TWithFieldName("vector"))
}

func rlsQueryVector(kind string, value any) entity.Vector {
	if vector, ok := value.(entity.Vector); ok {
		return vector
	}
	switch kind {
	case "binary":
		return entity.BinaryVector(value.([]byte))
	case "float16":
		return entity.Float16Vector(value.([]byte))
	case "bfloat16":
		return entity.BFloat16Vector(value.([]byte))
	case "int8":
		return entity.Int8Vector(value.([]int8))
	case "sparse":
		return value.(entity.SparseEmbedding)
	default:
		return entity.FloatVector(value.([]float32))
	}
}

func checkRLSMixedSegments(ctx context.Context, t *testing.T, mc *client.Client, name string, partitionCount ...int) {
	t.Helper()
	copies := 1
	if len(partitionCount) > 0 {
		copies = partitionCount[0]
	}
	// The typed SDK has no loaded-segment wrapper. IndexID is collection metadata,
	// not proof of physical indexing; the fixture requires the documented row threshold.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err := mc.GetService().GetQuerySegmentInfo(ctx, &milvuspb.GetQuerySegmentInfoRequest{CollectionName: name})
		if !assert.NoError(c, err) {
			return
		}
		if !assert.NoError(c, merr.Error(resp.GetStatus())) {
			return
		}
		byID := map[int64]*milvuspb.QuerySegmentInfo{}
		for _, segment := range resp.GetInfos() {
			byID[segment.GetSegmentID()] = segment
		}
		if !assert.Len(c, byID, 2*copies) {
			return
		}
		partitions := map[int64][]int64{}
		for _, segment := range byID {
			partitions[segment.GetPartitionID()] = append(partitions[segment.GetPartitionID()], segment.GetNumRows())
			assert.Equal(c, commonpb.SegmentState_Sealed, segment.GetState())
		}
		assert.Len(c, partitions, copies)
		for _, rows := range partitions {
			assert.ElementsMatch(c, []int64{3000, 500}, rows)
		}
	}, 90*time.Second, time.Second)
	idx, err := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(name, "vector"))
	require.NoError(t, err)
	require.Zero(t, idx.PendingIndexRows)
}

func checkRLSMixedReads(ctx context.Context, t *testing.T, mc *client.Client, name string, tc rlsSchemaCase, all, sealed map[any]rlsSchemaRow) {
	t.Helper()
	var fields []string
	for _, c := range rlsSchemaColumns(t, tc) {
		fields = append(fields, c.Name())
	}
	for _, principal := range []string{"alice", "bob", "nobody", ""} {
		t.Run("principal_"+principal, func(t *testing.T) {
			want := maps.Clone(all)
			maps.DeleteFunc(want, func(_ any, r rlsSchemaRow) bool { return principal != "" && r["owner"] != principal })
			keys := slices.Collect(maps.Keys(want))
			t.Run("query_get_count", func(t *testing.T) {
				r, err := mc.Query(ctx, client.NewQueryOption(name).WithFilter("owner != ''").WithOutputFields(fields...).WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong))
				require.NoError(t, err)
				checkRLSSchemaIDs(t, keys, checkRLSSchemaRows(t, r, want, fields))
				pk := rlsSchemaColumns(t, tc)[0]
				for id := range all {
					require.NoError(t, pk.AppendValue(id))
				}
				r, err = mc.Get(ctx, client.NewQueryOption(name).WithIDs(pk).WithOutputFields(fields...).WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong))
				require.NoError(t, err)
				checkRLSSchemaIDs(t, keys, checkRLSSchemaRows(t, r, want, fields))
				r, err = mc.Query(ctx, client.NewQueryOption(name).WithFilter("owner != ''").WithOutputFields("count(*)").WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong))
				require.NoError(t, err)
				n, err := r.GetColumn("count(*)").GetAsInt64(0)
				require.NoError(t, err)
				require.EqualValues(t, len(want), n)
			})
			t.Run("query_iterator", func(t *testing.T) {
				it, err := mc.QueryIterator(ctx, client.NewQueryIteratorOption(name).WithBatchSize(511).WithOutputFields(fields...).WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong))
				require.NoError(t, err)
				var got []any
				seen := map[any]bool{}
				ended := false
				// A valid page can be smaller than the requested batch after version deduplication.
				for range len(want) + 1 {
					r, err := it.Next(ctx)
					if errors.Is(err, io.EOF) {
						ended = true
						break
					}
					require.NoError(t, err)
					page := checkRLSSchemaRows(t, r, want, fields)
					require.NotEmpty(t, page, "an empty iterator page must return EOF")
					for _, id := range page {
						require.False(t, seen[id], "duplicate iterator ID across pages: %v", id)
						seen[id] = true
					}
					got = append(got, page...)
				}
				require.True(t, ended)
				checkRLSSchemaIDs(t, keys, got)
			})
			// Query each original PK range so a missing execution path cannot hide behind total count.
			for _, group := range []struct {
				name       string
				start, end int
			}{{"indexed", 1, 3001}, {"unindexed", 3001, 3501}, {"growing", 3501, 4001}} {
				t.Run(group.name, func(t *testing.T) {
					groupWant := map[any]rlsSchemaRow{}
					pk := rlsSchemaColumns(t, tc)[0]
					for id := group.start; id < group.end; id++ {
						key := any(int64(id))
						if tc.varchar {
							key = fmt.Sprintf("用户/%04d", id)
						}
						require.NoError(t, pk.AppendValue(key))
						if row, ok := want[key]; ok {
							groupWant[key] = row
						}
					}
					seed := any(int64(group.start))
					if tc.varchar {
						seed = fmt.Sprintf("用户/%04d", group.start)
					}
					query := rlsQueryVector(tc.vector, all[seed]["vector"])
					filter := "id in {ids}"
					ids := slices.Collect(maps.Keys(groupWant))
					// The requested IDs include both owners, independently of expected authorization.
					var requested any
					if tc.varchar {
						requested = pk.(*column.ColumnVarChar).Data()
					} else {
						requested = pk.(*column.ColumnInt64).Data()
					}
					params := index.NewCustomAnnParam()
					params.WithExtraParam("nprobe", 64)
					params.WithExtraParam("ef", rlsHNSWEF)
					checkTopK := func(t *testing.T, result client.ResultSet, query entity.Vector, rrf bool) {
						t.Helper()
						got := checkRLSSchemaRows(t, result, groupWant, fields)
						require.Len(t, got, min(20, len(groupWant)))
						ranked := rlsRankedIDs(query, groupWant)
						require.Len(t, result.Scores, len(got))
						for i, id := range got {
							score := rlsVectorDistance(query, groupWant[id]["vector"])
							// Sparse postings may use FP16 storage by default.
							tolerance := float32(0.001)
							if tc.vector == "sparse" {
								tolerance = max(tolerance, score*0.0005)
							}
							if rrf {
								require.InDelta(t, 1.0/float64(60+i+1), result.Scores[i], 1e-6)
							} else {
								require.InDelta(t, score, result.Scores[i], float64(tolerance))
							}
							if i > 0 {
								if rrf || tc.vector == "sparse" {
									require.GreaterOrEqual(t, result.Scores[i-1], result.Scores[i])
								} else {
									require.LessOrEqual(t, result.Scores[i-1], result.Scores[i])
								}
							}
							if tc.vector != "int8" {
								// Compare scores instead of PK order when equally close rows tie.
								require.InDelta(t, rlsVectorDistance(query, groupWant[ranked[i]]["vector"]), score, float64(tolerance))
							}
						}
						if tc.vector == "int8" && len(got) > 0 {
							recalled := 0
							cutoff := rlsVectorDistance(query, groupWant[ranked[len(got)-1]]["vector"])
							for _, id := range got {
								if rlsVectorDistance(query, groupWant[id]["vector"]) <= cutoff {
									recalled++
								}
							}
							require.GreaterOrEqual(t, float64(recalled)/float64(len(got)), 0.9, "HNSW top-K recall")
						}
					}
					t.Run("search", func(t *testing.T) {
						rs, err := mc.Search(ctx, client.NewSearchOption(name, 20, []entity.Vector{query}).WithANNSField("vector").WithFilter(filter).WithTemplateParam("ids", requested).WithAnnParam(params).WithOutputFields(fields...).WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong))
						require.NoError(t, err)
						require.Len(t, rs, 1)
						checkTopK(t, rs[0], query, false)
					})
					t.Run("hybrid", func(t *testing.T) {
						req := client.NewAnnRequest("vector", 20, query).WithFilter(filter).WithTemplateParam("ids", requested).WithAnnParam(params)
						rs, err := mc.HybridSearch(ctx, client.NewHybridSearchOption(name, 20, req).WithReranker(client.NewRRFReranker().WithK(60)).WithOutputFields(fields...).WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong))
						require.NoError(t, err)
						require.Len(t, rs, 1)
						checkTopK(t, rs[0], query, true)
					})
					if len(ids) > 0 {
						t.Run("search_by_ids", func(t *testing.T) {
							t.Skip("SearchByIDs is not available in the 2.6 public API")
						})
					}
					for _, ignoreGrowing := range []bool{false, true} {
						t.Run(fmt.Sprintf("search_iterator/ignore_growing_%v", ignoreGrowing), func(t *testing.T) {
							expected := ids
							if ignoreGrowing {
								expected = nil
								for _, id := range ids {
									if _, ok := sealed[id]; ok {
										expected = append(expected, id)
									}
								}
							}
							it, err := mc.SearchIterator(ctx, client.NewSearchIteratorOption(name, query).WithANNSField("vector").WithFilter(filter).WithTemplateParam("ids", requested).WithAnnParam(params).WithIgnoreGrowing(ignoreGrowing).WithBatchSize(rlsIteratorBatchSize).WithOutputFields(fields...).WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong))
							if tc.vector == "binary" && err != nil && len(expected) > 0 {
								require.True(t, strings.Contains(err.Error(), "index not supported") || (group.name == "indexed" && strings.Contains(err.Error(), "Failed to create iterators from index")), "unexpected binary iterator error: %v", err)
								return
							}
							require.NoError(t, err)
							var got []any
							var pageBounds []float32
							ended := false
							for range (group.end-group.start)/rlsIteratorBatchSize + 3 {
								r, err := it.Next(ctx)
								if errors.Is(err, io.EOF) {
									ended = true
									break
								}
								if tc.vector == "binary" && err != nil && len(expected) > 0 {
									require.True(t, strings.Contains(err.Error(), "index not supported") || (group.name == "indexed" && strings.Contains(err.Error(), "Failed to create iterators from index")), "unexpected binary iterator error: %v", err)
									return
								}
								require.NoError(t, err)
								got = append(got, checkRLSSchemaRows(t, r, groupWant, fields)...)
								require.Len(t, r.Scores, r.Len())
								if len(r.Scores) > 0 {
									pageBounds = append(pageBounds, r.Scores[len(r.Scores)-1])
								}
							}
							require.True(t, ended)
							if (tc.vector == "binary" || tc.vector == "int8") && len(got) < len(expected) {
								seen := make(map[any]bool, len(got))
								for _, id := range got {
									require.Contains(t, expected, id)
									require.False(t, seen[id], "duplicate iterator ID: %v", id)
									seen[id] = true
								}
								var missing []any
								for _, id := range expected {
									if !seen[id] {
										// Other incompleteness remains a failure, not an expected tie loss.
										require.Contains(t, pageBounds, rlsVectorDistance(query, groupWant[id]["vector"]), "missing ID is not tied at a page boundary: %v", id)
										missing = append(missing, id)
									}
								}
								t.Skipf("xfail: https://github.com/milvus-io/milvus/issues/54188, SearchIterator drops page-boundary ties: %v", missing)
							}
							checkRLSSchemaIDs(t, expected, got)
						})
					}
				})
			}
		})
	}
}

// Both principals own data in every segment shape, with distinct random vectors.
func TestRLSSchemaMixedSegments(t *testing.T) {
	for _, vector := range []string{"float32", "binary", "float16", "bfloat16", "int8", "sparse"} {
		for _, varchar := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/varchar_%v", vector, varchar), func(t *testing.T) {
				t.Parallel()
				ctx := hp.CreateContext(t, 10*time.Minute)
				mc := hp.CreateDefaultMilvusClient(ctx, t).Client
				tc := rlsSchemaCase{vector: vector, varchar: varchar, dim: 128}
				name, all := prepareRLSMixedSegments(ctx, t, mc, tc)
				sealed := map[any]rlsSchemaRow{}
				for id := 1; id <= 3500; id++ {
					key := any(int64(id))
					if varchar {
						key = fmt.Sprintf("用户/%04d", id)
					}
					sealed[key] = all[key]
				}

				t.Run("reads", func(t *testing.T) { checkRLSMixedReads(ctx, t, mc, name, tc, all, sealed) })
				checkRLSMixedSegments(ctx, t, mc, name)
				key := func(id int) any {
					if varchar {
						return fmt.Sprintf("用户/%04d", id)
					}
					return int64(id)
				}
				for stageIndex, stage := range []string{"full_column", "full_row", "partial_column", "partial_row", "delete_ids", "delete_filter", "delete_template", "insert_column", "insert_row"} {
					if !t.Run(stage, func(t *testing.T) {
						for ownerIndex, owner := range []string{"alice", "bob"} {
							for _, start := range []int{0, 3000, 3500} {
								id := start + 3 + stageIndex*2 + ownerIndex
								row := maps.Clone(all[key(id)])
								row["text"] = stage + "_" + owner
								row["f64"] = float64(99.125)
								var err error
								switch stage {
								case "full_column", "full_row", "partial_column", "partial_row":
									patch := row
									partial := stage == "partial_column" || stage == "partial_row"
									if partial {
										patch = rlsSchemaRow{"id": row["id"], "text": row["text"], "f64": row["f64"]}
									} else {
										v, e := rlsRandomVectorColumn(tc, 1).Get(0)
										require.NoError(t, e)
										row["vector"] = rlsVectorValue(rlsQueryVector(tc.vector, v))
									}
									if stage == "full_row" || stage == "partial_row" {
										_, err = mc.Upsert(ctx, rlsRowWriteOption(name, owner, false, partial, rlsSchemaRowInput(patch)))
									} else {
										_, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(name, rlsSchemaWriteColumns(t, tc, patch)...).WithPartialUpdate(partial).WithRLSPrincipal(owner))
									}
									require.NoError(t, err)
									all[row["id"]] = row
									delete(sealed, row["id"])
								case "delete_ids", "delete_filter", "delete_template":
									opt := client.NewDeleteOption(name).WithRLSPrincipal(owner)
									if stage == "delete_ids" {
										if varchar {
											opt.WithStringIDs("id", []string{key(id).(string)})
										} else {
											opt.WithInt64IDs("id", []int64{key(id).(int64)})
										}
									} else if stage == "delete_template" {
										t.Skip("Delete expression templates are not available in the 2.6 Go SDK")
									} else {
										if varchar {
											opt.WithExpr(fmt.Sprintf("id == %q", key(id)))
										} else {
											opt.WithExpr(fmt.Sprintf("id == %d", id))
										}
									}
									_, err = mc.Delete(ctx, opt)
									require.NoError(t, err)
									delete(all, key(id))
									delete(sealed, key(id))
								case "insert_column", "insert_row":
									row["id"] = key(5000 + start + ownerIndex)
									if stage == "insert_row" {
										row["id"] = key(9000 + start + ownerIndex)
									}
									v, e := rlsRandomVectorColumn(tc, 1).Get(0)
									require.NoError(t, e)
									row["vector"] = rlsVectorValue(rlsQueryVector(tc.vector, v))
									if stage == "insert_row" {
										_, err = mc.Insert(ctx, client.NewRowBasedInsertOption(name, rlsSchemaRowInput(row)).WithRLSPrincipal(owner))
									} else {
										_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(name, rlsSchemaWriteColumns(t, tc, row)...).WithRLSPrincipal(owner))
									}
									require.NoError(t, err)
									all[row["id"]] = row
								}
							}
							// Check the other principal before it performs its own writes.
							checkRLSSchemaState(ctx, t, mc, name, tc, all, false)
						}
					}) {
						return
					}
				}
				checkRLSMixedSegments(ctx, t, mc, name)
				t.Run("reads_after_writes", func(t *testing.T) { checkRLSMixedReads(ctx, t, mc, name, tc, all, sealed) })
			})
		}
	}
}

// Keep the row option when setting partial updates: the 2.6 SDK setter returns
// its embedded column option, which bypasses row serialization when chained.
func rlsRowWriteOption(name, principal string, skipRLS, partial bool, rows ...any) interface {
	client.InsertOption
	client.UpsertOption
} {
	option := client.NewRowBasedInsertOption(name, rows...).WithRLSPrincipal(principal).WithSkipRLS(skipRLS)
	option.WithPartialUpdate(partial)
	return option
}

// Key routing and pruning must preserve the same application-owned RLS oracle.
func TestRLSSchemaKeys(t *testing.T) {
	for _, field := range []string{"i64", "text"} {
		for _, mode := range []string{"partition", "isolation", "clustering", "partition_and_clustering"} {
			t.Run(mode+"/"+field, func(t *testing.T) {
				t.Parallel()
				ctx := hp.CreateContext(t, 15*time.Minute)
				mc := hp.CreateDefaultMilvusClient(ctx, t).Client
				tc := rlsSchemaCase{vector: "float32", dim: 128, isolation: mode == "isolation"}
				if mode != "clustering" {
					tc.partitionKey = field
				}
				if mode == "clustering" {
					tc.clusteringKey = field
				} else if mode == "partition_and_clustering" {
					tc.clusteringKey = "text"
					if field == "text" {
						tc.clusteringKey = "i64"
					}
				}
				name, all := prepareRLSMixedSegments(ctx, t, mc, tc)
				copies := 1
				if tc.partitionKey != "" {
					copies = 2
				}
				sealed := maps.Clone(all)
				maps.DeleteFunc(sealed, func(id any, _ rlsSchemaRow) bool { return id.(int64) > int64(3500*copies) })
				// Bound each phase independently; the combined matrix can exceed one shared deadline.
				t.Run("reads", func(t *testing.T) {
					checkRLSKeyReads(hp.CreateContext(t, 10*time.Minute), t, mc, name, tc, all, sealed)
				})
				for stageIndex, stage := range []string{"full_column", "full_row", "partial_column", "partial_row", "delete_ids", "delete_filter", "delete_template", "insert_column", "insert_row", "upsert_new"} {
					if !t.Run(stage, func(t *testing.T) {
						ctx := hp.CreateContext(t, 3*time.Minute)
						for ownerIndex, owner := range []string{"alice", "bob"} {
							for _, start := range []int{0, 3000 * copies, 3500 * copies} {
								// Four consecutive IDs contain both owners in both key buckets.
								for bucket := range 2 {
									id := int64(start + 1 + stageIndex*4 + bucket*2 + ownerIndex)
									row := maps.Clone(all[id])
									row["f64"] = float64(101 + stageIndex)
									var err error
									switch stage {
									case "full_column", "full_row", "partial_column", "partial_row":
										partial := stage == "partial_column" || stage == "partial_row"
										if tc.clusteringKey != "" {
											row[tc.clusteringKey] = rlsKeyValue(tc.clusteringKey, 1-bucket)
										}
										patch := row
										if partial {
											patch = rlsSchemaRow{"id": id, "f64": row["f64"]}
											if tc.clusteringKey != "" {
												patch[tc.clusteringKey] = row[tc.clusteringKey]
											}
											// Partition-key partial updates must identify their routing key.
											if tc.partitionKey != "" {
												patch[tc.partitionKey] = row[tc.partitionKey]
											}
										}
										if stage == "full_row" || stage == "partial_row" {
											_, err = mc.Upsert(ctx, rlsRowWriteOption(name, owner, false, partial, rlsSchemaRowInput(patch)))
										} else {
											_, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(name, rlsSchemaWriteColumns(t, tc, patch)...).WithPartialUpdate(partial).WithRLSPrincipal(owner))
										}
										require.NoError(t, err)
										all[id] = row
										delete(sealed, id)
									case "delete_ids", "delete_filter", "delete_template":
										opt := client.NewDeleteOption(name).WithRLSPrincipal(owner)
										if stage == "delete_ids" {
											opt.WithInt64IDs("id", []int64{id})
										} else if stage == "delete_template" {
											t.Skip("Delete expression templates are not available in the 2.6 Go SDK")
										} else {
											opt.WithExpr(fmt.Sprintf("id == %d", id))
										}
										_, err = mc.Delete(ctx, opt)
										require.NoError(t, err)
										delete(all, id)
										delete(sealed, id)
									case "insert_column", "insert_row", "upsert_new":
										row["id"] = id + int64(20000*(stageIndex+1))
										row["vector"] = common.GenFloatVector(tc.vectorDim())
										if stage == "insert_row" {
											_, err = mc.Insert(ctx, client.NewRowBasedInsertOption(name, rlsSchemaRowInput(row)).WithRLSPrincipal(owner))
										} else if stage == "upsert_new" {
											_, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(name, rlsSchemaWriteColumns(t, tc, row)...).WithRLSPrincipal(owner))
										} else {
											_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(name, rlsSchemaWriteColumns(t, tc, row)...).WithRLSPrincipal(owner))
										}
										require.NoError(t, err)
										all[row["id"]] = row
									}
								}
							}
							checkRLSSchemaState(ctx, t, mc, name, tc, all, false)
						}
					}) {
						return
					}
				}
				checkRLSMixedSegments(hp.CreateContext(t, 2*time.Minute), t, mc, name, copies)
				t.Run("reads_after_writes", func(t *testing.T) {
					checkRLSKeyReads(hp.CreateContext(t, 10*time.Minute), t, mc, name, tc, all, sealed)
				})
				if tc.clusteringKey != "" {
					t.Run("clustering_compaction", func(t *testing.T) {
						ctx := hp.CreateContext(t, 15*time.Minute)
						rlsFlush(ctx, t, mc, name)
						var resp *milvuspb.ManualCompactionResponse
						var targets []int64
						require.EventuallyWithT(t, func(c *assert.CollectT) {
							if resp == nil {
								// The typed SDK does not expose MajorCompaction; use the public RPC.
								var err error
								resp, err = mc.GetService().ManualCompaction(ctx, &milvuspb.ManualCompactionRequest{CollectionName: name, MajorCompaction: true})
								if !assert.NoError(c, err) || !assert.NoError(c, merr.Error(resp.GetStatus())) {
									resp = nil
									return
								}
							}
							if !assert.Positive(c, resp.GetCompactionID()) || !assert.Positive(c, resp.GetCompactionPlanCount(), "must execute physical clustering work") {
								resp = nil
								return
							}
							state, err := mc.GetService().GetCompactionStateWithPlans(ctx, &milvuspb.GetCompactionPlansRequest{CompactionID: resp.GetCompactionID()})
							if !assert.NoError(c, err) || !assert.NoError(c, merr.Error(state.GetStatus())) {
								return
							}
							if !assert.Equal(c, commonpb.CompactionState_Completed, state.GetState()) {
								return
							}
							status, err := mc.GetService().GetCompactionState(ctx, &milvuspb.GetCompactionStateRequest{CompactionID: resp.GetCompactionID()})
							if !assert.NoError(c, err) || !assert.NoError(c, merr.Error(status.GetStatus())) {
								return
							}
							assert.Zero(c, status.GetFailedPlanNo())
							assert.Zero(c, status.GetTimeoutPlanNo())
							assert.Len(c, state.GetMergeInfos(), int(resp.GetCompactionPlanCount()))
							targets = nil
							for _, plan := range state.GetMergeInfos() {
								// The 2.6 public protocol exposes only the first output of each plan.
								assert.Positive(c, plan.GetTarget())
								targets = append(targets, plan.GetTarget())
							}
						}, 3*time.Minute, time.Second)
						require.NoError(t, mc.ReleaseCollection(ctx, client.NewReleaseCollectionOption(name)))
						load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
						require.NoError(t, err)
						require.NoError(t, load.Await(ctx))
						require.EventuallyWithT(t, func(c *assert.CollectT) {
							loaded, err := mc.GetService().GetQuerySegmentInfo(ctx, &milvuspb.GetQuerySegmentInfoRequest{CollectionName: name})
							if !assert.NoError(c, err) || !assert.NoError(c, merr.Error(loaded.GetStatus())) {
								return
							}
							ids := map[int64]bool{}
							for _, segment := range loaded.GetInfos() {
								ids[segment.GetSegmentID()] = true
							}
							for _, target := range targets {
								assert.Contains(c, ids, target, "load each reported clustering output")
							}
						}, time.Minute, time.Second)
						checkRLSSchemaState(ctx, t, mc, name, tc, all, false)
						checkRLSKeyReads(ctx, t, mc, name, tc, all, all)
					})
				}
			})
		}
	}
}

func checkRLSKeyReads(ctx context.Context, t *testing.T, mc *client.Client, name string, tc rlsSchemaCase, all, sealed map[any]rlsSchemaRow) {
	t.Helper()
	field := cmp.Or(tc.partitionKey, tc.clusteringKey)
	values := []any{rlsKeyValue(field, 0), rlsKeyValue(field, 1)}
	if tc.partitionKey != "" {
		values = rlsPartitionValues(t, field)
	}
	fields := []string{"id", "owner", "vector", "i64", "text", "f64"}
	params := index.NewCustomAnnParam()
	params.WithExtraParam("nprobe", 64)
	params.WithExtraParam("ef", rlsHNSWEF)
	for _, value := range values {
		clusterValues := []any{nil}
		if tc.partitionKey != "" && tc.clusteringKey != "" {
			clusterValues = append(clusterValues, rlsKeyValue(tc.clusteringKey, 0), rlsKeyValue(tc.clusteringKey, 1))
		}
		for _, clusterValue := range clusterValues {
			filter := fmt.Sprintf("%s == {key}", field)
			literalFilter := fmt.Sprintf("%s == %v", field, value)
			if field == "text" {
				literalFilter = fmt.Sprintf("%s == %q", field, value)
			}
			if clusterValue != nil {
				condition := fmt.Sprintf(" AND %s == %v", tc.clusteringKey, clusterValue)
				if tc.clusteringKey == "text" {
					condition = fmt.Sprintf(" AND %s == %q", tc.clusteringKey, clusterValue)
				}
				filter += condition
				literalFilter += condition
			}
			for _, principal := range []string{"alice", "bob", "nobody", ""} {
				t.Run(fmt.Sprintf("%v/cluster_%v/principal_%s", value, clusterValue, principal), func(t *testing.T) {
					want := maps.Clone(all)
					maps.DeleteFunc(want, func(_ any, row rlsSchemaRow) bool {
						return row[field] != value || (clusterValue != nil && row[tc.clusteringKey] != clusterValue) || (principal != "" && row["owner"] != principal)
					})
					ids := slices.Collect(maps.Keys(want))
					query := rlsQueryVector(tc.vector, all[int64(1)]["vector"])
					t.Run("query_get_count", func(t *testing.T) {
						r, err := mc.Query(ctx, client.NewQueryOption(name).WithFilter(filter).WithTemplateParam("key", value).WithOutputFields(fields...).WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong))
						require.NoError(t, err)
						checkRLSSchemaIDs(t, ids, checkRLSSchemaRows(t, r, want, fields))
						var requested []int64
						for id, row := range all {
							if row[field] == value && (clusterValue == nil || row[tc.clusteringKey] == clusterValue) {
								requested = append(requested, id.(int64))
							}
						}
						r, err = mc.Get(ctx, client.NewQueryOption(name).WithIDs(column.NewColumnInt64("id", requested)).WithOutputFields(fields...).WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong))
						require.NoError(t, err)
						checkRLSSchemaIDs(t, ids, checkRLSSchemaRows(t, r, want, fields))
						r, err = mc.Query(ctx, client.NewQueryOption(name).WithFilter(filter).WithTemplateParam("key", value).WithOutputFields("count(*)").WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong))
						require.NoError(t, err)
						n, err := r.GetColumn("count(*)").GetAsInt64(0)
						require.NoError(t, err)
						require.EqualValues(t, len(want), n)
					})
					t.Run("query_iterator", func(t *testing.T) {
						it, err := mc.QueryIterator(ctx, client.NewQueryIteratorOption(name).WithFilter(literalFilter).WithBatchSize(511).WithOutputFields(fields...).WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong))
						require.NoError(t, err)
						var got []any
						seen := map[any]bool{}
						ended := false
						for range len(want) + 1 {
							r, err := it.Next(ctx)
							if errors.Is(err, io.EOF) {
								ended = true
								break
							}
							require.NoError(t, err)
							page := checkRLSSchemaRows(t, r, want, fields)
							require.NotEmpty(t, page, "an empty iterator page must return EOF")
							for _, id := range page {
								require.False(t, seen[id], "duplicate iterator ID across pages: %v", id)
								seen[id] = true
							}
							got = append(got, page...)
						}
						require.True(t, ended)
						checkRLSSchemaIDs(t, ids, got)
					})
					checkTopK := func(t *testing.T, result client.ResultSet, query entity.Vector, rrf bool) {
						t.Helper()
						got := checkRLSSchemaRows(t, result, want, fields)
						require.Len(t, got, min(20, len(want)))
						ranked := rlsRankedIDs(query, want)
						require.Len(t, result.Scores, len(got))
						recalled := 0
						for i, id := range got {
							score := rlsVectorDistance(query, want[id]["vector"])
							if i > 0 {
								require.LessOrEqual(t, rlsVectorDistance(query, want[got[i-1]]["vector"]), score)
							}
							if rrf {
								require.InDelta(t, 1.0/float64(61+i), result.Scores[i], 1e-6)
							} else {
								require.InDelta(t, score, result.Scores[i], 0.001)
							}
							if !tc.isolation {
								require.InDelta(t, rlsVectorDistance(query, want[ranked[i]]["vector"]), score, 0.001)
							}
							if score <= rlsVectorDistance(query, want[ranked[len(got)-1]]["vector"]) {
								recalled++
							}
						}
						if tc.isolation && len(got) > 0 {
							require.GreaterOrEqual(t, float64(recalled)/float64(len(got)), 0.9)
						}
					}
					t.Run("search", func(t *testing.T) {
						rs, err := mc.Search(ctx, client.NewSearchOption(name, 20, []entity.Vector{query}).WithANNSField("vector").WithAnnParam(params).WithFilter(filter).WithTemplateParam("key", value).WithOutputFields(fields...).WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong))
						require.NoError(t, err)
						require.Len(t, rs, 1)
						checkTopK(t, rs[0], query, false)
					})
					t.Run("hybrid", func(t *testing.T) {
						req := client.NewAnnRequest("vector", 20, query).WithAnnParam(params).WithFilter(filter).WithTemplateParam("key", value)
						rs, err := mc.HybridSearch(ctx, client.NewHybridSearchOption(name, 20, req).WithReranker(client.NewRRFReranker().WithK(60)).WithOutputFields(fields...).WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong))
						require.NoError(t, err)
						require.Len(t, rs, 1)
						checkTopK(t, rs[0], query, true)
					})
					if len(ids) > 0 {
						t.Run("search_by_ids", func(t *testing.T) {
							t.Skip("SearchByIDs is not available in the 2.6 public API")
						})
					}
					for _, ignoreGrowing := range []bool{false, true} {
						t.Run(fmt.Sprintf("search_iterator/ignore_growing_%v", ignoreGrowing), func(t *testing.T) {
							expected := ids
							if ignoreGrowing {
								expected = nil
								for _, id := range ids {
									if _, ok := sealed[id]; ok {
										expected = append(expected, id)
									}
								}
							}
							it, err := mc.SearchIterator(ctx, client.NewSearchIteratorOption(name, query).WithANNSField("vector").WithAnnParam(params).WithFilter(filter).WithTemplateParam("key", value).WithBatchSize(rlsIteratorBatchSize).WithIgnoreGrowing(ignoreGrowing).WithOutputFields(fields...).WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong))
							require.NoError(t, err)
							var got []any
							var pageBounds []float32
							ended := false
							for range len(all)/rlsIteratorBatchSize + 3 {
								r, err := it.Next(ctx)
								if errors.Is(err, io.EOF) {
									ended = true
									break
								}
								require.NoError(t, err)
								got = append(got, checkRLSSchemaRows(t, r, want, fields)...)
								require.Len(t, r.Scores, r.Len())
								if len(r.Scores) > 0 {
									pageBounds = append(pageBounds, r.Scores[len(r.Scores)-1])
								}
							}
							require.True(t, ended)
							seen := make(map[any]bool, len(got))
							for _, id := range got {
								if ignoreGrowing {
									require.Contains(t, sealed, id, "iterator returned a growing row")
								}
								require.False(t, seen[id], "duplicate iterator ID: %v", id)
								seen[id] = true
							}
							if len(got) < len(expected) {
								missing := make(map[any]rlsSchemaRow)
								var missingIDs []int64
								for _, id := range expected {
									if !seen[id] {
										missing[id] = want[id]
										missingIDs = append(missingIDs, id.(int64))
									}
								}
								// Use server scores: FLOAT_VECTOR reductions may round differently from the oracle.
								rs, err := mc.Search(ctx, client.NewSearchOption(name, len(missingIDs), []entity.Vector{query}).WithANNSField("vector").WithAnnParam(params).WithFilter(literalFilter+" AND id in {missing_ids}").WithTemplateParam("missing_ids", missingIDs).WithIgnoreGrowing(ignoreGrowing).WithOutputFields(fields...).WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong))
								require.NoError(t, err)
								require.Len(t, rs, 1)
								recovered := checkRLSSchemaRows(t, rs[0], missing, fields)
								checkRLSSchemaIDs(t, slices.Collect(maps.Keys(missing)), recovered)
								require.Len(t, rs[0].Scores, len(recovered))
								for i, id := range recovered {
									require.Contains(t, pageBounds, rs[0].Scores[i], "missing ID is not tied at a page boundary: %v", id)
								}
								t.Skipf("xfail: https://github.com/milvus-io/milvus/issues/54188, SearchIterator drops page-boundary ties: %v", missingIDs)
							}
							checkRLSSchemaIDs(t, expected, got)
						})
					}
				})
			}
		}
	}
	if tc.isolation {
		t.Run("reject_missing_or_multiple_partition_keys", func(t *testing.T) {
			for _, filter := range []string{"owner != ''", fmt.Sprintf("%s in {keys}", field)} {
				opt := client.NewSearchOption(name, 20, []entity.Vector{entity.FloatVector(common.GenFloatVector(tc.vectorDim()))}).WithANNSField("vector").WithAnnParam(params).WithFilter(filter).WithRLSPrincipal("alice")
				if field == "i64" {
					opt.WithTemplateParam("keys", []int64{values[0].(int64), values[1].(int64)})
				} else {
					opt.WithTemplateParam("keys", []string{values[0].(string), values[1].(string)})
				}
				_, err := mc.Search(ctx, opt)
				require.ErrorContains(t, err, "partition key")
			}
		})
	}
	for _, keyField := range []string{tc.partitionKey, tc.clusteringKey} {
		if keyField == "" {
			continue
		}
		for _, op := range []string{"in", ">"} {
			for _, principal := range []string{"alice", "bob", "nobody", ""} {
				t.Run(fmt.Sprintf("key_filter/%s/%s/principal_%s", keyField, op, principal), func(t *testing.T) {
					keyValues := []any{rlsKeyValue(keyField, 0), rlsKeyValue(keyField, 1)}
					if keyField == tc.partitionKey {
						keyValues = rlsPartitionValues(t, keyField)
					}
					want := maps.Clone(all)
					maps.DeleteFunc(want, func(_ any, row rlsSchemaRow) bool {
						if principal != "" && row["owner"] != principal {
							return true
						}
						if op == "in" {
							return !slices.Contains(keyValues, row[keyField])
						}
						if keyField == "i64" {
							return row[keyField].(int64) <= keyValues[0].(int64)
						}
						return row[keyField].(string) <= keyValues[0].(string)
					})
					opt := client.NewQueryOption(name).WithFilter(keyField + " " + op + " {value}").WithOutputFields(fields...).WithRLSPrincipal(principal).WithSkipRLS(principal == "").WithConsistencyLevel(entity.ClStrong)
					if op == ">" {
						opt.WithTemplateParam("value", keyValues[0])
					} else if keyField == "i64" {
						opt.WithTemplateParam("value", []int64{keyValues[0].(int64), keyValues[1].(int64)})
					} else {
						opt.WithTemplateParam("value", []string{keyValues[0].(string), keyValues[1].(string)})
					}
					r, err := mc.Query(ctx, opt)
					require.NoError(t, err)
					checkRLSSchemaIDs(t, slices.Collect(maps.Keys(want)), checkRLSSchemaRows(t, r, want, fields))
				})
			}
		}
	}
}

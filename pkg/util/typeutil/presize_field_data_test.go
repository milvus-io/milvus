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

package typeutil

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

const presizeTestDim = 8

// presizeTestSource builds one source of every column kind with rows rows.
// valid gives the validity of the nullable columns. The nullable vector
// payload is compact, holding only the valid rows; the nullable vector array
// keeps a placeholder for each null row.
func presizeTestSource(base int, rows int, valid []bool) []*schemapb.FieldData {
	scalar := func(id int64, dt schemapb.DataType, data *schemapb.ScalarField) *schemapb.FieldData {
		return &schemapb.FieldData{
			Type: dt, FieldId: id, FieldName: fmt.Sprintf("f%d", id),
			Field: &schemapb.FieldData_Scalars{Scalars: data},
		}
	}
	vector := func(id int64, dt schemapb.DataType, dim int64, data *schemapb.VectorField) *schemapb.FieldData {
		data.Dim = dim
		return &schemapb.FieldData{
			Type: dt, FieldId: id, FieldName: fmt.Sprintf("f%d", id),
			Field: &schemapb.FieldData_Vectors{Vectors: data},
		}
	}

	var (
		bools      []bool
		ints       []int32
		longs      []int64
		floats     []float32
		doubles    []float64
		strs       []string
		jsons      [][]byte
		arrays     []*schemapb.ScalarField
		geoms      [][]byte
		floatVec   []float32
		binaryVec  []byte
		f16Vec     []byte
		bf16Vec    []byte
		int8Vec    []byte
		sparse     [][]byte
		vecArrays  []*schemapb.VectorField
		nullFloats []float32
		nullArrays []*schemapb.VectorField
	)
	for r := 0; r < rows; r++ {
		v := base + r
		bools = append(bools, v%2 == 0)
		ints = append(ints, int32(v))
		longs = append(longs, int64(v))
		floats = append(floats, float32(v))
		doubles = append(doubles, float64(v))
		strs = append(strs, fmt.Sprintf("s%d", v))
		jsons = append(jsons, []byte(fmt.Sprintf(`{"v":%d}`, v)))
		arrays = append(arrays, &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{
			LongData: &schemapb.LongArray{Data: []int64{int64(v), int64(v + 1)}},
		}})
		geoms = append(geoms, []byte{byte(v)})
		for d := 0; d < presizeTestDim; d++ {
			floatVec = append(floatVec, float32(v*presizeTestDim+d))
			int8Vec = append(int8Vec, byte(v+d))
			f16Vec = append(f16Vec, byte(v), byte(d))
			bf16Vec = append(bf16Vec, byte(d), byte(v))
		}
		binaryVec = append(binaryVec, byte(v))
		sparse = append(sparse, CreateSparseFloatRow([]uint32{uint32(v), uint32(v + 100)}, []float32{1, 2}))
		vecArrays = append(vecArrays, &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{
			FloatVector: &schemapb.FloatArray{Data: []float32{float32(v), float32(v)}},
		}})
		if valid[r] {
			for d := 0; d < presizeTestDim; d++ {
				nullFloats = append(nullFloats, float32(v*presizeTestDim+d))
			}
			nullArrays = append(nullArrays, vecArrays[r])
		} else {
			nullArrays = append(nullArrays, &schemapb.VectorField{Dim: 2})
		}
	}
	sparseDim := int64(base + rows + 100)

	nullableLong := scalar(103, schemapb.DataType_Int64, &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{
		LongData: &schemapb.LongArray{Data: append([]int64(nil), longs...)},
	}})
	SetFieldDataValidData(nullableLong, append([]bool(nil), valid...))
	nullableVec := vector(118, schemapb.DataType_FloatVector, presizeTestDim, &schemapb.VectorField{})
	if len(nullFloats) > 0 {
		nullableVec.GetVectors().Data = &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: nullFloats}}
	}
	SetFieldDataValidData(nullableVec, append([]bool(nil), valid...))
	nullableVecArray := vector(120, schemapb.DataType_ArrayOfVector, 2, &schemapb.VectorField{Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{
		Dim: 2, Data: nullArrays, ElementType: schemapb.DataType_FloatVector,
	}}})
	SetFieldDataValidData(nullableVecArray, append([]bool(nil), valid...))

	return []*schemapb.FieldData{
		scalar(100, schemapb.DataType_Bool, &schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: bools}}}),
		scalar(101, schemapb.DataType_Int32, &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: ints}}}),
		scalar(102, schemapb.DataType_Int64, &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: longs}}}),
		nullableLong,
		scalar(104, schemapb.DataType_Float, &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: floats}}}),
		scalar(105, schemapb.DataType_Double, &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: doubles}}}),
		scalar(106, schemapb.DataType_VarChar, &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: strs}}}),
		scalar(107, schemapb.DataType_JSON, &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: jsons}}}),
		scalar(108, schemapb.DataType_Array, &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
			Data: arrays, ElementType: schemapb.DataType_Int64,
		}}}),
		scalar(109, schemapb.DataType_Geometry, &schemapb.ScalarField{Data: &schemapb.ScalarField_GeometryData{GeometryData: &schemapb.GeometryArray{Data: geoms}}}),
		scalar(110, schemapb.DataType_Timestamptz, &schemapb.ScalarField{Data: &schemapb.ScalarField_TimestamptzData{TimestamptzData: &schemapb.TimestamptzArray{Data: longs}}}),
		vector(111, schemapb.DataType_FloatVector, presizeTestDim, &schemapb.VectorField{Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: floatVec}}}),
		vector(112, schemapb.DataType_BinaryVector, presizeTestDim, &schemapb.VectorField{Data: &schemapb.VectorField_BinaryVector{BinaryVector: binaryVec}}),
		vector(113, schemapb.DataType_Float16Vector, presizeTestDim, &schemapb.VectorField{Data: &schemapb.VectorField_Float16Vector{Float16Vector: f16Vec}}),
		vector(114, schemapb.DataType_BFloat16Vector, presizeTestDim, &schemapb.VectorField{Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: bf16Vec}}),
		vector(115, schemapb.DataType_Int8Vector, presizeTestDim, &schemapb.VectorField{Data: &schemapb.VectorField_Int8Vector{Int8Vector: int8Vec}}),
		vector(116, schemapb.DataType_SparseFloatVector, sparseDim, &schemapb.VectorField{Data: &schemapb.VectorField_SparseFloatVector{
			SparseFloatVector: &schemapb.SparseFloatArray{Dim: sparseDim, Contents: sparse},
		}}),
		vector(117, schemapb.DataType_ArrayOfVector, 2, &schemapb.VectorField{Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{
			Dim: 2, Data: vecArrays, ElementType: schemapb.DataType_FloatVector,
		}}}),
		nullableVec,
		{
			Type: schemapb.DataType_Int64, FieldId: 119, FieldName: "dyn", IsDynamic: true,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{
				LongData: &schemapb.LongArray{Data: longs},
			}}},
		},
		nullableVecArray,
	}
}

type presizeTestPick struct {
	source int
	row    int64
}

func appendPicks(dst []*schemapb.FieldData, sources [][]*schemapb.FieldData, picks []presizeTestPick) {
	computers := make([]*FieldDataIdxComputer, len(sources))
	for i, source := range sources {
		computers[i] = NewFieldDataIdxComputer(source)
	}
	for _, p := range picks {
		AppendFieldData(dst, sources[p.source], p.row, computers[p.source].Compute(p.row)...)
	}
}

func reversed(source []*schemapb.FieldData) []*schemapb.FieldData {
	slices.Reverse(source)
	return source
}

func TestPresizeAppendFieldDataMatchesLazyAppend(t *testing.T) {
	allValid := []bool{true, true, true, true}
	someNull := []bool{true, false, false, true}
	allNull := []bool{false, false, false, false}

	cases := []struct {
		name    string
		sources [][]*schemapb.FieldData
		picks   []presizeTestPick
	}{
		{
			name:    "one source, all rows",
			sources: [][]*schemapb.FieldData{presizeTestSource(0, 4, allValid)},
			picks:   []presizeTestPick{{0, 0}, {0, 1}, {0, 2}, {0, 3}},
		},
		{
			name: "interleaved sources, some null",
			sources: [][]*schemapb.FieldData{
				presizeTestSource(0, 4, someNull),
				presizeTestSource(10, 4, allValid),
			},
			picks: []presizeTestPick{{1, 0}, {0, 0}, {0, 1}, {1, 1}, {0, 3}, {1, 3}},
		},
		{
			name: "only null rows of the nullable columns",
			sources: [][]*schemapb.FieldData{
				presizeTestSource(0, 4, allNull),
				presizeTestSource(10, 4, someNull),
			},
			picks: []presizeTestPick{{0, 0}, {1, 1}, {0, 3}, {1, 2}},
		},
		{
			name: "only null rows of a source with vector payload",
			sources: [][]*schemapb.FieldData{
				presizeTestSource(0, 4, someNull),
			},
			picks: []presizeTestPick{{0, 2}, {0, 1}},
		},
		{
			name: "empty first source",
			sources: [][]*schemapb.FieldData{
				nil,
				presizeTestSource(20, 4, someNull),
			},
			picks: []presizeTestPick{{1, 2}, {1, 0}},
		},
		{
			name: "sources in different layouts",
			sources: [][]*schemapb.FieldData{
				presizeTestSource(0, 4, someNull),
				reversed(presizeTestSource(10, 4, allValid)),
			},
			picks: []presizeTestPick{{1, 0}, {0, 1}, {1, 3}, {0, 3}},
		},
		{
			name: "fewer picks than presized rows",
			sources: [][]*schemapb.FieldData{
				presizeTestSource(0, 4, someNull),
			},
			picks: []presizeTestPick{{0, 3}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var sampleLen int
			for _, source := range tc.sources {
				if len(source) > 0 {
					sampleLen = len(source)
					break
				}
			}
			lazy := make([]*schemapb.FieldData, sampleLen)
			appendPicks(lazy, tc.sources, tc.picks)

			presized := PresizeAppendFieldData(4, tc.sources...)
			require.Len(t, presized, sampleLen)
			appendPicks(presized, tc.sources, tc.picks)

			for i := range lazy {
				require.NotNil(t, lazy[i])
				assert.True(t, proto.Equal(lazy[i], presized[i]),
					"column %d differs:\nlazy:     %v\npresized: %v", lazy[i].GetFieldId(), lazy[i], presized[i])
			}
		})
	}
}

func TestPresizeAppendFieldDataColumns(t *testing.T) {
	valid := []bool{true, false, true, true}
	source := presizeTestSource(0, 4, valid)
	const rows = 3

	t.Run("no rows keeps every column nil", func(t *testing.T) {
		for _, n := range []int64{0, -1} {
			dst := PresizeAppendFieldData(n, source)
			require.Len(t, dst, len(source))
			for _, fd := range dst {
				assert.Nil(t, fd)
			}
		}
		assert.Empty(t, PresizeAppendFieldData(rows))
		assert.Empty(t, PresizeAppendFieldData(rows, nil, nil))
	})

	dst := PresizeAppendFieldData(rows, source)
	byID := make(map[int64]*schemapb.FieldData)
	for i, fd := range dst {
		if fd != nil {
			assert.Equal(t, source[i].GetFieldId(), fd.GetFieldId())
			byID[fd.GetFieldId()] = fd
		}
	}

	capacities := []struct {
		fieldID  int64
		capacity func(*schemapb.FieldData) int
		want     int
	}{
		{100, func(fd *schemapb.FieldData) int { return cap(fd.GetScalars().GetBoolData().GetData()) }, rows},
		{102, func(fd *schemapb.FieldData) int { return cap(fd.GetScalars().GetLongData().GetData()) }, rows},
		{103, func(fd *schemapb.FieldData) int { return cap(GetFieldDataValidData(fd)) }, rows},
		{106, func(fd *schemapb.FieldData) int { return cap(fd.GetScalars().GetStringData().GetData()) }, rows},
		{108, func(fd *schemapb.FieldData) int { return cap(fd.GetScalars().GetArrayData().GetData()) }, rows},
		{111, func(fd *schemapb.FieldData) int { return cap(fd.GetVectors().GetFloatVector().GetData()) }, rows * presizeTestDim},
		{112, func(fd *schemapb.FieldData) int { return cap(fd.GetVectors().GetBinaryVector()) }, rows * presizeTestDim / 8},
		{113, func(fd *schemapb.FieldData) int { return cap(fd.GetVectors().GetFloat16Vector()) }, rows * presizeTestDim * 2},
		{115, func(fd *schemapb.FieldData) int { return cap(fd.GetVectors().GetInt8Vector()) }, rows * presizeTestDim},
		{117, func(fd *schemapb.FieldData) int { return cap(fd.GetVectors().GetVectorArray().GetData()) }, rows},
		{120, func(fd *schemapb.FieldData) int { return cap(fd.GetVectors().GetVectorArray().GetData()) }, rows},
	}
	for _, c := range capacities {
		fd, ok := byID[c.fieldID]
		require.True(t, ok, "field %d not presized", c.fieldID)
		assert.Equal(t, c.want, c.capacity(fd), "field %d", c.fieldID)
	}

	// Columns whose lazily built form depends on the appended rows.
	for _, fieldID := range []int64{116, 118} {
		_, ok := byID[fieldID]
		assert.False(t, ok, "field %d should stay nil", fieldID)
	}

	// A vector column is nullable when any source carries validity for it.
	allValid := presizeTestSource(0, 4, []bool{true, true, true, true})
	SetFieldDataValidData(allValid[18], nil)
	dst = PresizeAppendFieldData(rows, allValid, source)
	assert.Nil(t, dst[18])

	// Sources in different layouts leave every column to AppendFieldData.
	for _, other := range [][]*schemapb.FieldData{source[1:], reversed(presizeTestSource(0, 4, valid))} {
		for _, fd := range PresizeAppendFieldData(rows, source, other) {
			assert.Nil(t, fd)
		}
	}

	// A dense vector without a Dim takes it from the first appended row.
	noDim := presizeTestSource(0, 4, valid)
	noDim[11].GetVectors().Dim = 0
	assert.Nil(t, PresizeAppendFieldData(rows, noDim)[11])
}

func TestPrepareResultFieldDataNullableVector(t *testing.T) {
	const rows = 5
	source := presizeTestSource(0, 4, []bool{true, false, true, true})
	dst := PrepareResultFieldData(source, rows)

	// A compact nullable vector reserves payload for the valid share of the
	// sample, 3 of 4 rows, and validity for every row.
	nullable := dst[18]
	require.NotNil(t, nullable.GetVectors().GetFloatVector())
	assert.Equal(t, 4*presizeTestDim, cap(nullable.GetVectors().GetFloatVector().GetData()))
	assert.Equal(t, rows, cap(GetFieldDataValidData(nullable)))

	// With every row null it reserves no payload, but is still typed.
	allNullSource := presizeTestSource(0, 4, []bool{false, false, false, false})
	allNullSource[18].GetVectors().Data = &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{}}
	allNull := PrepareResultFieldData(allNullSource, rows)[18]
	require.NotNil(t, allNull.GetVectors().GetFloatVector())
	assert.Zero(t, cap(allNull.GetVectors().GetFloatVector().GetData()))

	assert.Equal(t, rows*presizeTestDim, cap(dst[11].GetVectors().GetFloatVector().GetData()))
	assert.Equal(t, rows, cap(dst[20].GetVectors().GetVectorArray().GetData()))
}

func TestAppendFieldDataByColumnGrowsExistingColumn(t *testing.T) {
	const srcRows = 1000
	valid := make([]bool, srcRows)
	for i := range valid {
		valid[i] = i%3 != 0
	}
	src := presizeTestSource(0, srcRows, valid)
	indices := make([]int64, srcRows)
	for i := range indices {
		indices[i] = int64(i)
	}

	cases := []struct {
		col   int
		reset func(*schemapb.FieldData)
		len   func(*schemapb.FieldData) int
	}{
		{
			col: 2,
			reset: func(fd *schemapb.FieldData) {
				d := fd.GetScalars().GetLongData()
				d.Data = d.Data[:1:1]
			},
			len: func(fd *schemapb.FieldData) int { return len(fd.GetScalars().GetLongData().GetData()) },
		},
		{
			col: 3,
			reset: func(fd *schemapb.FieldData) {
				d := fd.GetScalars()
				d.GetLongData().Data = d.GetLongData().Data[:1:1]
				d.ValidData = d.ValidData[:1:1]
			},
			len: func(fd *schemapb.FieldData) int {
				return len(fd.GetScalars().GetLongData().GetData()) + len(GetFieldDataValidData(fd))
			},
		},
		{
			col: 11,
			reset: func(fd *schemapb.FieldData) {
				d := fd.GetVectors().GetFloatVector()
				d.Data = d.Data[:presizeTestDim:presizeTestDim]
			},
			len: func(fd *schemapb.FieldData) int { return len(fd.GetVectors().GetFloatVector().GetData()) },
		},
		{
			col: 14,
			reset: func(fd *schemapb.FieldData) {
				d := fd.GetVectors().GetData().(*schemapb.VectorField_Bfloat16Vector)
				d.Bfloat16Vector = d.Bfloat16Vector[: 2*presizeTestDim : 2*presizeTestDim]
			},
			len: func(fd *schemapb.FieldData) int { return len(fd.GetVectors().GetBfloat16Vector()) },
		},
		{
			col: 16,
			reset: func(fd *schemapb.FieldData) {
				d := fd.GetVectors().GetSparseFloatVector()
				d.Contents = d.Contents[:1:1]
			},
			len: func(fd *schemapb.FieldData) int { return len(fd.GetVectors().GetSparseFloatVector().GetContents()) },
		},
	}
	for _, tc := range cases {
		t.Run(src[tc.col].GetFieldName(), func(t *testing.T) {
			dst := proto.Clone(src[tc.col]).(*schemapb.FieldData)
			tc.reset(dst)
			base := tc.len(dst)
			AppendFieldDataByColumn(dst, src[tc.col], indices)
			full := tc.len(dst)
			assert.Greater(t, full, base)

			allocs := testing.AllocsPerRun(10, func() {
				tc.reset(dst)
				AppendFieldDataByColumn(dst, src[tc.col], indices)
			})
			assert.Equal(t, full, tc.len(dst))
			// One growth per slice (data and validity), not one per
			// doubling. slices.Grow allocates twice when built with -N.
			assert.LessOrEqual(t, allocs, 4.0)
		})
	}

	t.Run("output", func(t *testing.T) {
		dst := proto.Clone(src[3]).(*schemapb.FieldData)
		dst.GetScalars().GetLongData().Data = []int64{-1}
		SetFieldDataValidData(dst, []bool{false})
		AppendFieldDataByColumn(dst, src[3], []int64{0, 1, 2})
		assert.Equal(t, []bool{false, false, true, true}, GetFieldDataValidData(dst))
		assert.Equal(t, []int64{-1, 0, 1, 2}, dst.GetScalars().GetLongData().GetData())
	})
}

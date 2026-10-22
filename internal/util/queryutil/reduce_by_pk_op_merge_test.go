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

package queryutil

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// mergeInput describes one input of the pk+ts merge. Exactly one of intPKs and
// strPKs is set. payload sizes the per-row VarChar column so rowSize varies.
type mergeInput struct {
	intPKs  []int64
	strPKs  []string
	ts      []int64 // nil: no timestamp column
	payload []int   // per-row VarChar length; nil: all 4
	elems   [][]int32
	hasMore bool
	elemLvl bool
}

func (in mergeInput) rows() int {
	if in.intPKs != nil {
		return len(in.intPKs)
	}
	return len(in.strPKs)
}

func (in mergeInput) build() *timestampedResult {
	n := in.rows()
	ids := &schemapb.IDs{}
	if in.intPKs != nil {
		ids.IdField = &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: in.intPKs}}
	} else {
		ids.IdField = &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: in.strPKs}}
	}
	strs := make([]string, n)
	for i := range strs {
		l := 4
		if in.payload != nil {
			l = in.payload[i]
		}
		strs[i] = strings.Repeat("x", l)
	}
	fields := []*schemapb.FieldData{{
		Type:    schemapb.DataType_VarChar,
		FieldId: 101,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: strs}},
		}},
	}}
	if in.ts != nil {
		fields = append(fields, &schemapb.FieldData{
			Type:    schemapb.DataType_Int64,
			FieldId: 1,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: in.ts}},
			}},
		})
	}
	r := &internalpb.RetrieveResults{Ids: ids, FieldsData: fields, HasMoreResult: in.hasMore, ElementLevel: in.elemLvl}
	for _, e := range in.elems {
		r.ElementIndices = append(r.ElementIndices, &internalpb.ElementIndices{Indices: e})
	}
	tr, err := newTimestampedResult(r)
	if err != nil {
		tr = &timestampedResult{result: r}
	}
	return tr
}

func buildMergeInputs(ins []mergeInput) []*timestampedResult {
	out := make([]*timestampedResult, len(ins))
	for i, in := range ins {
		out[i] = in.build()
	}
	return out
}

// referenceSelect runs the map path, which is the pre-fast-path implementation.
func referenceSelect(op *ReduceByPKWithTimestampOperator, results []*timestampedResult) ([]rowRef, error) {
	calcs := make([]*rowSizeCalculator, len(results))
	for i, r := range results {
		calcs[i] = newRowSizeCalculator(r.result).withArrowRecord(r.record)
	}
	return op.selectRowsByPKWithTimestampMap(results, calcs)
}

// fastPathTaken reports whether selectSortedByPK accepted the input.
func fastPathTaken(op *ReduceByPKWithTimestampOperator, results []*timestampedResult) bool {
	calcs := make([]*rowSizeCalculator, len(results))
	for i, r := range results {
		calcs[i] = newRowSizeCalculator(r.result)
	}
	intPKs, strPKs := typedPKs(results)
	switch {
	case intPKs != nil:
		_, ok, _ := selectSortedByPK(op, results, intPKs, calcs)
		return ok
	case strPKs != nil:
		_, ok, _ := selectSortedByPK(op, results, strPKs, calcs)
		return ok
	}
	return false
}

func TestSelectRowsByPKWithTimestamp_Table(t *testing.T) {
	ref := func(res, row int) rowRef { return rowRef{resultIdx: res, rowIdx: int64(row)} }
	cases := []struct {
		name       string
		reduceType reduce.IReduceType
		limit      int64
		maxSize    int64
		inputs     []mergeInput
		want       []rowRef
		wantErr    bool
		wantFast   bool
	}{
		{
			name: "newest ts wins across inputs",
			inputs: []mergeInput{
				{intPKs: []int64{1, 2, 3}, ts: []int64{10, 10, 10}},
				{intPKs: []int64{2, 3, 4}, ts: []int64{20, 5, 10}},
			},
			want:     []rowRef{ref(0, 0), ref(1, 0), ref(0, 2), ref(1, 2)},
			wantFast: true,
		},
		{
			name: "equal ts ties go to the lowest input",
			inputs: []mergeInput{
				{intPKs: []int64{5}, ts: []int64{7}},
				{intPKs: []int64{5}, ts: []int64{7}},
				{intPKs: []int64{5}, ts: []int64{7}},
			},
			want:     []rowRef{ref(0, 0)},
			wantFast: true,
		},
		{
			name: "duplicate inside one input, later copy newer",
			inputs: []mergeInput{
				{intPKs: []int64{1, 1, 2}, ts: []int64{3, 9, 1}},
			},
			want:     []rowRef{ref(0, 1), ref(0, 2)},
			wantFast: true,
		},
		{
			name: "zero ts never replaces",
			inputs: []mergeInput{
				{intPKs: []int64{1, 1}, ts: []int64{0, 0}},
				{intPKs: []int64{1}, ts: []int64{0}},
			},
			want:     []rowRef{ref(0, 0)},
			wantFast: true,
		},
		{
			name: "missing ts column reads as zero",
			inputs: []mergeInput{
				{intPKs: []int64{1, 2}},
				{intPKs: []int64{1}, ts: []int64{4}},
			},
			want:     []rowRef{ref(1, 0), ref(0, 1)},
			wantFast: true,
		},
		{
			name: "string pks",
			inputs: []mergeInput{
				{strPKs: []string{"a", "b", "d"}, ts: []int64{1, 1, 1}},
				{strPKs: []string{"b", "c"}, ts: []int64{2, 2}},
			},
			want:     []rowRef{ref(0, 0), ref(1, 0), ref(1, 1), ref(0, 2)},
			wantFast: true,
		},
		{
			name:  "limit stops early",
			limit: 2,
			inputs: []mergeInput{
				{intPKs: []int64{1, 3, 5}, ts: []int64{1, 1, 1}},
				{intPKs: []int64{2, 4}, ts: []int64{1, 1}},
			},
			want:     []rowRef{ref(0, 0), ref(1, 0)},
			wantFast: true,
		},
		{
			name:       "drain stops in-order reduce",
			reduceType: reduce.IReduceInOrder,
			inputs: []mergeInput{
				{intPKs: []int64{1, 2}, ts: []int64{1, 1}, hasMore: true},
				{intPKs: []int64{3, 4}, ts: []int64{1, 1}},
			},
			want:     []rowRef{ref(0, 0), ref(0, 1)},
			wantFast: true,
		},
		{
			name:       "drain ignored for no-order reduce",
			reduceType: reduce.IReduceNoOrder,
			inputs: []mergeInput{
				{intPKs: []int64{1, 2}, ts: []int64{1, 1}, hasMore: true},
				{intPKs: []int64{3}, ts: []int64{1}},
			},
			want:     []rowRef{ref(0, 0), ref(0, 1), ref(1, 0)},
			wantFast: true,
		},
		{
			name:    "maxOutputSize exceeded",
			maxSize: 20,
			inputs: []mergeInput{
				{intPKs: []int64{1, 2, 3}, ts: []int64{1, 1, 1}, payload: []int{8, 8, 8}},
			},
			wantErr:  true,
			wantFast: true,
		},
		{
			name:  "element-level counts elements, replacement swaps them",
			limit: 4,
			inputs: []mergeInput{
				{intPKs: []int64{1, 2}, ts: []int64{1, 1}, elemLvl: true, elems: [][]int32{{0}, {0, 1}}},
				{intPKs: []int64{1, 3}, ts: []int64{2, 1}, elemLvl: true, elems: [][]int32{{0, 1, 2}, {0}}},
			},
			// pk1 -> input1 (3 elems), pk2 -> 2 more = 5 >= 4.
			want:     []rowRef{ref(1, 0), ref(0, 1)},
			wantFast: true,
		},
		{
			name: "unsorted input falls back to the map path",
			inputs: []mergeInput{
				{intPKs: []int64{3, 1, 2}, ts: []int64{1, 1, 1}},
				{intPKs: []int64{1}, ts: []int64{2}},
			},
			want:     []rowRef{ref(1, 0), ref(0, 0), ref(0, 2)},
			wantFast: false,
		},
		{
			name: "mixed pk types fall back to the map path",
			inputs: []mergeInput{
				{intPKs: []int64{1}, ts: []int64{1}},
				{strPKs: []string{"a"}, ts: []int64{1}},
			},
			wantFast: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			op := NewReduceByPKWithTimestampOperator(tc.reduceType, tc.maxSize, tc.limit, nil)
			results := buildMergeInputs(tc.inputs)
			assert.Equal(t, tc.wantFast, fastPathTaken(op, results))

			got, err := op.selectRowsByPKWithTimestamp(results)
			wantRows, wantErr := referenceSelect(op, results)
			assert.Equal(t, wantErr != nil, err != nil)
			assert.Equal(t, wantRows, got)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tc.want != nil {
				assert.Equal(t, tc.want, got)
			}
		})
	}
}

// TestSelectRowsByPKWithTimestamp_Differential holds the fast path to the map
// path over random inputs: duplicates within and across inputs, timestamps with
// zeros and ties, drain, limit, size guard, element level, and unsorted inputs.
func TestSelectRowsByPKWithTimestamp_Differential(t *testing.T) {
	rng := rand.New(rand.NewSource(20261020))
	reduceTypes := []reduce.IReduceType{reduce.IReduceNoOrder, reduce.IReduceInOrder, reduce.IReduceInOrderForBest}
	fast, total := 0, 0
	for iter := 0; iter < 5000; iter++ {
		useStr := rng.Intn(2) == 0
		elemLvl := rng.Intn(6) == 0
		n := 1 + rng.Intn(9)
		ins := make([]mergeInput, n)
		for i := range ins {
			rows := rng.Intn(25)
			keys := make([]int, rows)
			for j := range keys {
				keys[j] = rng.Intn(40)
			}
			sort.Ints(keys)
			if rng.Intn(10) == 0 && rows > 1 {
				rng.Shuffle(rows, func(a, b int) { keys[a], keys[b] = keys[b], keys[a] })
			}
			in := mergeInput{hasMore: rng.Intn(4) == 0, elemLvl: elemLvl}
			if useStr {
				in.strPKs = make([]string, rows)
				for j, k := range keys {
					// Variable width keeps lexical order meaningful: "k10" < "k9".
					in.strPKs[j] = fmt.Sprintf("k%d", k)
				}
				if !sort.StringsAreSorted(in.strPKs) && rng.Intn(2) == 0 {
					sort.Strings(in.strPKs)
				}
			} else {
				in.intPKs = make([]int64, rows)
				for j, k := range keys {
					in.intPKs[j] = int64(k)
				}
			}
			if rng.Intn(8) != 0 {
				in.ts = make([]int64, rows)
				for j := range in.ts {
					in.ts[j] = int64(rng.Intn(4)) // 0 and ties are common
				}
			}
			in.payload = make([]int, rows)
			for j := range in.payload {
				in.payload[j] = rng.Intn(12)
			}
			if elemLvl {
				for j := 0; j < rows; j++ {
					in.elems = append(in.elems, make([]int32, rng.Intn(4)))
				}
			}
			ins[i] = in
		}
		var limit, maxSize int64
		if rng.Intn(3) == 0 {
			limit = int64(1 + rng.Intn(30))
		}
		if rng.Intn(3) == 0 {
			maxSize = int64(20 + rng.Intn(300))
		}
		op := NewReduceByPKWithTimestampOperator(reduceTypes[rng.Intn(3)], maxSize, limit, nil)
		results := buildMergeInputs(ins)

		got, err := op.selectRowsByPKWithTimestamp(results)
		want, wantErr := referenceSelect(op, results)
		require.Equal(t, wantErr != nil, err != nil, "iter %d", iter)
		require.Equal(t, want, got, "iter %d", iter)
		total++
		if fastPathTaken(op, results) {
			fast++
		}
	}
	// Most cases must exercise the fast path, or the test proves nothing.
	assert.Greater(t, fast, total/2)
}

// TestReduceByPKWithTimestamp_RunUsesFastPath checks the operator end to end
// on the fast path, including the merged columns.
func TestReduceByPKWithTimestamp_RunUsesFastPath(t *testing.T) {
	op := NewReduceByPKWithTimestampOperator(reduce.IReduceNoOrder, 0, 0, nil)
	results := []*internalpb.RetrieveResults{
		mergeInput{intPKs: []int64{1, 2}, ts: []int64{1, 1}, payload: []int{1, 2}}.build().result,
		mergeInput{intPKs: []int64{2, 3}, ts: []int64{5, 1}, payload: []int{3, 4}}.build().result,
	}
	out, err := op.Run(t.Context(), nil, results)
	require.NoError(t, err)
	merged := out[0].(*internalpb.RetrieveResults)
	assert.Equal(t, []int64{1, 2, 3}, merged.GetIds().GetIntId().GetData())
	assert.Equal(t, []string{"x", "xxx", "xxxx"}, merged.GetFieldsData()[0].GetScalars().GetStringData().GetData())
	assert.Equal(t, []int64{1, 5, 1}, merged.GetFieldsData()[1].GetScalars().GetLongData().GetData())
}

func BenchmarkSelectRowsByPKWithTimestamp(b *testing.B) {
	const rowsPerInput = 1000
	for _, pkType := range []string{"int64", "string"} {
		for _, n := range []int{1, 8} {
			rng := rand.New(rand.NewSource(1))
			ins := make([]mergeInput, n)
			for i := range ins {
				keys := make([]int, rowsPerInput)
				for j := range keys {
					keys[j] = rng.Intn(rowsPerInput * n * 4)
				}
				sort.Ints(keys)
				in := mergeInput{ts: make([]int64, rowsPerInput)}
				for j := range in.ts {
					in.ts[j] = int64(1 + rng.Intn(100))
				}
				if pkType == "int64" {
					in.intPKs = make([]int64, rowsPerInput)
					for j, k := range keys {
						in.intPKs[j] = int64(k)
					}
				} else {
					in.strPKs = make([]string, rowsPerInput)
					for j, k := range keys {
						in.strPKs[j] = fmt.Sprintf("pk-%012d", k)
					}
				}
				ins[i] = in
			}
			results := buildMergeInputs(ins)
			op := NewReduceByPKWithTimestampOperator(reduce.IReduceNoOrder, 1<<40, 0, nil)
			name := fmt.Sprintf("%s/N=%d", pkType, n)
			b.Run(name+"/map", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					if _, err := referenceSelect(op, results); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run(name+"/sorted", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					if _, err := op.selectRowsByPKWithTimestamp(results); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

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

package testutils

import (
	"encoding/binary"
	"math"
	"math/rand"
	"sort"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func SparseFloatRowSetAt(row []byte, pos int, idx uint32, value float32) {
	binary.LittleEndian.PutUint32(row[pos*8:], idx)
	binary.LittleEndian.PutUint32(row[pos*8+4:], math.Float32bits(value))
}

func CreateSparseFloatRow(indices []uint32, values []float32) []byte {
	row := make([]byte, len(indices)*8)
	for i := 0; i < len(indices); i++ {
		SparseFloatRowSetAt(row, i, indices[i], values[i])
	}
	return row
}

func GenerateSparseFloatVectors(numRows int) *schemapb.SparseFloatArray {
	dim := 700
	avgNnz := 20
	var contents [][]byte
	maxDim := 0

	uniqueAndSort := func(indices []uint32) []uint32 {
		seen := make(map[uint32]bool)
		var result []uint32
		for _, value := range indices {
			if _, ok := seen[value]; !ok {
				seen[value] = true
				result = append(result, value)
			}
		}
		sort.Slice(result, func(i, j int) bool {
			return result[i] < result[j]
		})
		return result
	}

	for i := 0; i < numRows; i++ {
		nnz := rand.Intn(avgNnz*2) + 1
		indices := make([]uint32, 0, nnz)
		for j := 0; j < nnz; j++ {
			indices = append(indices, uint32(rand.Intn(dim)))
		}
		indices = uniqueAndSort(indices)
		values := make([]float32, 0, len(indices))
		for j := 0; j < len(indices); j++ {
			values = append(values, rand.Float32())
		}
		if len(indices) > 0 && int(indices[len(indices)-1])+1 > maxDim {
			maxDim = int(indices[len(indices)-1]) + 1
		}
		row_bytes := CreateSparseFloatRow(indices, values)

		contents = append(contents, row_bytes)
	}
	return &schemapb.SparseFloatArray{
		Dim:      int64(maxDim),
		Contents: contents,
	}
}

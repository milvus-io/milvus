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

package index

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type utilSuite struct {
	suite.Suite
}

func (s *utilSuite) Test_mapToKVPairs() {
	indexParams := map[string]string{
		"index_type": "IVF_FLAT",
		"dim":        "128",
		"nlist":      "1024",
	}

	s.Equal(3, len(mapToKVPairs(indexParams)))
}

func (s *utilSuite) Test_estimateFieldDataSize() {
	dim := int64(128)
	numRows := int64(1000)

	// BinaryVector: dim/8 * numRows
	size, err := estimateFieldDataSize(dim, numRows, schemapb.DataType_BinaryVector)
	s.NoError(err)
	s.Equal(uint64(dim)/8*uint64(numRows), size)

	// FloatVector: dim * numRows * 4
	size, err = estimateFieldDataSize(dim, numRows, schemapb.DataType_FloatVector)
	s.NoError(err)
	s.Equal(uint64(dim)*uint64(numRows)*4, size)

	// Float16Vector: dim * numRows * 2
	size, err = estimateFieldDataSize(dim, numRows, schemapb.DataType_Float16Vector)
	s.NoError(err)
	s.Equal(uint64(dim)*uint64(numRows)*2, size)

	// BFloat16Vector: dim * numRows * 2
	size, err = estimateFieldDataSize(dim, numRows, schemapb.DataType_BFloat16Vector)
	s.NoError(err)
	s.Equal(uint64(dim)*uint64(numRows)*2, size)

	// SparseFloatVector: error
	size, err = estimateFieldDataSize(dim, numRows, schemapb.DataType_SparseFloatVector)
	s.Error(err)
	s.Equal(uint64(0), size)

	// Unknown type: 0, no error
	size, err = estimateFieldDataSize(dim, numRows, schemapb.DataType_Int64)
	s.NoError(err)
	s.Equal(uint64(0), size)
}

func (s *utilSuite) Test_getFieldDataSizeFromBinlogs() {
	insertLogs := []*datapb.FieldBinlog{
		{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{
				{MemorySize: 1024},
				{MemorySize: 2048},
			},
		},
		{
			FieldID: 101,
			Binlogs: []*datapb.Binlog{
				{MemorySize: 4096},
			},
		},
	}

	// Match field 100
	s.Equal(uint64(3072), getFieldDataSizeFromBinlogs(insertLogs, 100))

	// Match field 101
	s.Equal(uint64(4096), getFieldDataSizeFromBinlogs(insertLogs, 101))

	// No match
	s.Equal(uint64(0), getFieldDataSizeFromBinlogs(insertLogs, 999))

	// Nil insert logs
	s.Equal(uint64(0), getFieldDataSizeFromBinlogs(nil, 100))
}

func Test_utilSuite(t *testing.T) {
	suite.Run(t, new(utilSuite))
}

func generateFloats(num int) []float32 {
	data := make([]float32, num)
	for i := 0; i < num; i++ {
		data[i] = rand.Float32()
	}
	return data
}

func generateLongs(num int) []int64 {
	data := make([]int64, num)
	for i := 0; i < num; i++ {
		data[i] = rand.Int63()
	}
	return data
}

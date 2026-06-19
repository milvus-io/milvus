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

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
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

func (s *utilSuite) Test_getFieldDataSizeFromBinlogs() {
	// Storage V2/V3 splits vector fields into their own column group,
	// where GroupID (used as FieldBinlog.FieldID) equals the vector field's ID.
	// Non-vector fields may be grouped together under a different GroupID.
	vectorFieldID := int64(101)
	insertLogs := []*datapb.FieldBinlog{
		{
			// Non-vector column group: GroupID=100, contains scalar fields 100 and 102
			FieldID:     100,
			ChildFields: []int64{100, 102},
			Binlogs: []*datapb.Binlog{
				{MemorySize: 1024},
				{MemorySize: 2048},
			},
		},
		{
			// Vector field in its own column group: GroupID = FieldID = 101
			FieldID:     vectorFieldID,
			ChildFields: []int64{vectorFieldID},
			Binlogs: []*datapb.Binlog{
				{MemorySize: 4096},
				{MemorySize: 8192},
			},
		},
	}

	// Vector field matches by FieldID (== GroupID)
	s.Equal(uint64(12288), getFieldDataSizeFromBinlogs(insertLogs, vectorFieldID))

	// Scalar field grouped under GroupID=100
	s.Equal(uint64(3072), getFieldDataSizeFromBinlogs(insertLogs, 100))

	// No match
	s.Equal(uint64(0), getFieldDataSizeFromBinlogs(insertLogs, 999))

	// Nil insert logs
	s.Equal(uint64(0), getFieldDataSizeFromBinlogs(nil, vectorFieldID))
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

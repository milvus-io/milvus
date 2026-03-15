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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/pkg/v2/common"
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

func Test_utilSuite(t *testing.T) {
	suite.Run(t, new(utilSuite))
}

func Test_getCurrentScalarIndexVersion(t *testing.T) {
	// getCurrentScalarIndexVersion delegates to common.ClampScalarIndexVersion.
	// This test verifies the delegation is correct and hasn't been changed
	// to use CurrentScalarIndexEngineVersion (which would be wrong).
	maximum := common.MaximumScalarIndexEngineVersion

	assert.Equal(t, int32(0), getCurrentScalarIndexVersion(0))
	assert.Equal(t, maximum, getCurrentScalarIndexVersion(maximum))
	assert.Equal(t, maximum, getCurrentScalarIndexVersion(maximum+1))

	// Verify it matches the shared helper
	assert.Equal(t, common.ClampScalarIndexVersion(maximum+5), getCurrentScalarIndexVersion(maximum+5))
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

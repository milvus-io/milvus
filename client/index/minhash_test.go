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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/client/v2/entity"
)

func TestMinHashLSHIndex(t *testing.T) {
	// Test WithElementBitWidth
	idx := NewMinHashLSHIndex(entity.JACCARD, 32)

	params := idx.Params()

	assert.NotEmpty(t, params)
	assert.EqualValues(t, entity.JACCARD, params[MetricTypeKey])
	assert.EqualValues(t, MinHashLSH, params[IndexTypeKey])

	// Test WithLSHCodeInMem
	idx.WithElementBitWidth(128)
	idx.WithLSHCodeInMem(1024)
	idx.WithRawData(true)
	idx.WithBloomFilterFalsePositiveProb(0.01)

	params = idx.Params()
	assert.EqualValues(t, "1024", params[minhashLSHCodeInMemKey])
	assert.EqualValues(t, "128", params[minhashElementBitWidthKey])
	assert.EqualValues(t, "true", params[minhashWithRawdataKey])
	assert.EqualValues(t, "0.01", params[minhashLSHBloomFPProbKey])
}

func TestMinHashAnnParam(t *testing.T) {
	ap := NewMinHashLSHAnnParam()

	// Test WithSearchWithJACCARD
	ap.WithSearchWithJACCARD(true)
	params := ap.Params()
	assert.EqualValues(t, "true", params[minhashSearchWithJaccardKey])

	// Test WithRefineK
	ap.WithRefineK(10)
	params = ap.Params()
	assert.EqualValues(t, "10", params[minhashRefineK])

	// Test WithBatchSearch
	ap.WithBatchSearch(true)
	params = ap.Params()
	assert.EqualValues(t, "true", params[minhashLSHBatchSearchKey])
}

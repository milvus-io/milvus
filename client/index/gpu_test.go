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

	"github.com/milvus-io/milvus/client/v3/entity"
)

// Each GPU builder must report its own type, both on the wire and through
// IndexType(). GPU_IVF_PQ and GPU_CAGRA used to send GPU_IVF_FLAT, and none of
// them set baseIndex.indexType at all.
func TestGPUIndexTypes(t *testing.T) {
	type testCase struct {
		tag    string
		idx    Index
		expect IndexType
	}

	for _, tc := range []testCase{
		{tag: "brute_force", idx: NewGPUBruteForceIndex(entity.L2), expect: GPUBruteForce},
		{tag: "ivf_flat", idx: NewGPUIVPFlatIndex(entity.L2), expect: GPUIvfFlat},
		{tag: "ivf_pq", idx: NewGPUIVPPQIndex(entity.L2), expect: GPUIvfPQ},
		{tag: "cagra", idx: NewGPUCagraIndex(entity.L2, 128, 64), expect: GPUCagra},
	} {
		t.Run(tc.tag, func(t *testing.T) {
			assert.EqualValues(t, tc.expect, tc.idx.Params()[IndexTypeKey])
			assert.EqualValues(t, tc.expect, tc.idx.IndexType())
			assert.EqualValues(t, entity.L2, tc.idx.Params()[MetricTypeKey])
		})
	}
}

func TestGPUUnsetBuildParamsAreOmitted(t *testing.T) {
	// nlist / m / nbits are unreachable through these constructors, so they sat
	// at zero and were emitted as "0" — below every accepted range, which fails
	// the build. An absent key lets the server apply its default instead.
	flat := NewGPUIVPFlatIndex(entity.L2).Params()
	assert.NotContains(t, flat, ivfNlistKey)

	pq := NewGPUIVPPQIndex(entity.L2).Params()
	assert.NotContains(t, pq, ivfNlistKey)
	assert.NotContains(t, pq, ivfPQMKey)
	assert.NotContains(t, pq, ivfPQNbits)

	// They remain reachable without changing the constructor signature.
	withNlist := WithExtraIndexParams(NewGPUIVPFlatIndex(entity.L2), map[string]string{
		ivfNlistKey: "1024",
	}).Params()
	assert.Equal(t, "1024", withNlist[ivfNlistKey])
}

func TestGPUCagraGraphDegreeKey(t *testing.T) {
	result := NewGPUCagraIndex(entity.L2, 128, 64).Params()
	// The key used to be declared as `"graph_degree"` — backquotes around a
	// string that itself contains the quote characters.
	assert.Equal(t, "64", result["graph_degree"])
	assert.NotContains(t, result, `"graph_degree"`)
	assert.Equal(t, "128", result[cagraInterGraphDegreeKey])
}

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

func TestAISAQ(t *testing.T) {
	result := NewAISAQIndex(entity.L2).Params()
	assert.EqualValues(t, entity.L2, result[MetricTypeKey])
	assert.EqualValues(t, AISAQ, result[IndexTypeKey])

	// Every build param is range-checked server-side, so an unset one must stay
	// absent rather than being sent as a zero.
	assert.Len(t, result, 2)
}

func TestAISAQBuildParams(t *testing.T) {
	result := NewAISAQIndex(entity.L2).
		WithInlinePQ(8).
		WithPQCacheSize(1 << 20).
		WithRearrange(true).
		WithPQReadIOEngine("uring").
		WithNumEntryPoints(4).
		WithPQReadPageCacheSize(4096).
		Params()

	assert.Equal(t, "8", result[aisaqInlinePQKey])
	assert.Equal(t, "1048576", result[aisaqPQCacheSizeKey])
	assert.Equal(t, "true", result[aisaqRearrangeKey])
	assert.Equal(t, "uring", result[aisaqPQReadIOEngineKey])
	assert.Equal(t, "4", result[aisaqNumEntryPointsKey])
	assert.Equal(t, "4096", result[aisaqPQReadPageCacheSizeKey])
}

func TestAISAQAnnParam(t *testing.T) {
	ap := NewAISAQAnnParam(100)
	result := ap.Params()
	assert.Equal(t, 100, result[diskANNSearchListKey])
	assert.NotContains(t, result, aisaqBeamwidthKey)
	assert.NotContains(t, result, aisaqVectorsBeamwidthKey)

	result = ap.WithBeamwidth(8).WithVectorsBeamwidth(2).WithPQReadPageCacheSize(4096).Params()
	assert.Equal(t, 8, result[aisaqBeamwidthKey])
	assert.Equal(t, 2, result[aisaqVectorsBeamwidthKey])
	assert.Equal(t, 4096, result[aisaqPQReadPageCacheSizeKey])
}

// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package delegator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

func TestBytesOffsetToRuneOffset(t *testing.T) {
	// test with chinese
	text := "你好世界" // 12 bytes, 4 runes
	spans := SpanList{{0, 6}, {6, 12}}
	err := bytesOffsetToRuneOffset(text, spans)
	assert.NoError(t, err)
	assert.Equal(t, SpanList{{0, 2}, {2, 4}}, spans)

	// test with emoji
	text = "Hello👋World" // 15 bytes, 11 runes
	spans = SpanList{{0, 5}, {5, 9}, {9, 14}}
	err = bytesOffsetToRuneOffset(text, spans)
	assert.NoError(t, err)
	assert.Equal(t, SpanList{{0, 5}, {5, 6}, {6, 11}}, spans)
}

func TestValidateNprobeForIVF(t *testing.T) {
	createSearchRequest := func(searchParams string) *internalpb.SearchRequest {
		plan := &planpb.PlanNode{
			Node: &planpb.PlanNode_VectorAnns{
				VectorAnns: &planpb.VectorANNS{
					QueryInfo: &planpb.QueryInfo{
						SearchParams: searchParams,
					},
				},
			},
		}
		serializedPlan, _ := proto.Marshal(plan)
		return &internalpb.SearchRequest{
			SerializedExprPlan: serializedPlan,
		}
	}

	tests := []struct {
		name        string
		req         *internalpb.SearchRequest
		indexType   string
		expectError bool
	}{
		{
			name:        "non-IVF index type should pass",
			req:         createSearchRequest(`{"nprobe": 0}`),
			indexType:   "HNSW",
			expectError: false,
		},
		{
			name:        "IVF_FLAT with valid nprobe",
			req:         createSearchRequest(`{"nprobe": 10}`),
			indexType:   "IVF_FLAT",
			expectError: false,
		},
		{
			name:        "IVF_FLAT with nprobe 0 should fail",
			req:         createSearchRequest(`{"nprobe": 0}`),
			indexType:   "IVF_FLAT",
			expectError: true,
		},
		{
			name:        "IVF_PQ with nprobe 0 should fail",
			req:         createSearchRequest(`{"nprobe": 0}`),
			indexType:   "IVF_PQ",
			expectError: true,
		},
		{
			name:        "IVF_FLAT with missing nprobe should pass",
			req:         createSearchRequest(`{}`),
			indexType:   "IVF_FLAT",
			expectError: false,
		},
		{
			name:        "IVF_FLAT with empty search params should pass",
			req:         createSearchRequest(``),
			indexType:   "IVF_FLAT",
			expectError: false,
		},
		{
			name: "IVF_FLAT with nil serialized plan should pass",
			req: &internalpb.SearchRequest{
				SerializedExprPlan: nil,
			},
			indexType:   "IVF_FLAT",
			expectError: false,
		},
		{
			name:        "SCANN with nprobe 0 should fail",
			req:         createSearchRequest(`{"nprobe": 0}`),
			indexType:   "SCANN",
			expectError: true,
		},
		{
			name:        "GPU_IVF_FLAT with nprobe 0 should fail",
			req:         createSearchRequest(`{"nprobe": 0}`),
			indexType:   "GPU_IVF_FLAT",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateNprobeForIVF(tt.req, tt.indexType)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

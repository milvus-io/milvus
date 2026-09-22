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

package httpserver

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/requestutil"
)

func TestRequestV2_GetCollectionName(t *testing.T) {
	tests := []struct {
		name string
		req  requestutil.CollectionNameGetter
		want string
	}{
		{"RenameCollectionReq", &RenameCollectionReq{CollectionName: "col1"}, "col1"},
		{"QueryReqV2", &QueryReqV2{CollectionName: "col2"}, "col2"},
		{"CollectionIDReq", &CollectionIDReq{CollectionName: "col3"}, "col3"},
		{"CollectionFilterReq", &CollectionFilterReq{CollectionName: "col4"}, "col4"},
		{"CollectionDataReq", &CollectionDataReq{CollectionName: "col5"}, "col5"},
		{"SearchReqV2", &SearchReqV2{CollectionName: "col6"}, "col6"},
		{"HybridSearchReq", &HybridSearchReq{CollectionName: "col7"}, "col7"},
		{"PartitionsReq", &PartitionsReq{CollectionName: "col8"}, "col8"},
		{"GrantV2Req", &GrantV2Req{CollectionName: "col9"}, "col9"},
		{"IndexParamReq", &IndexParamReq{CollectionName: "col10"}, "col10"},
		{"CollectionReq", &CollectionReq{CollectionName: "col11"}, "col11"},
		{"RunAnalyzerReq", &RunAnalyzerReq{CollectionName: "col12"}, "col12"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.req.GetCollectionName())
		})
	}
}

func TestBuildFieldPartialUpdateOps(t *testing.T) {
	ops, err := buildFieldPartialUpdateOpsV2([]FieldPartialUpdateOpReq{
		{FieldName: "tags", Op: "ARRAY_APPEND"},
		{FieldName: "scores", Op: "ARRAY_REMOVE"},
		{FieldName: "profile", Op: "PATH_REPLACE", Path: "[1][age]"},
	})
	assert.NoError(t, err)
	assert.Len(t, ops, 3)
	assert.Equal(t, "tags", ops[0].GetFieldName())
	assert.Equal(t, schemapb.FieldPartialUpdateOp_ARRAY_APPEND, ops[0].GetOp())
	assert.Equal(t, "scores", ops[1].GetFieldName())
	assert.Equal(t, schemapb.FieldPartialUpdateOp_ARRAY_REMOVE, ops[1].GetOp())
	assert.Equal(t, "profile", ops[2].GetFieldName())
	assert.Equal(t, schemapb.FieldPartialUpdateOp_PATH_REPLACE, ops[2].GetOp())
	assert.Equal(t, "[1][age]", ops[2].GetPath())
}

func TestBuildFieldPartialUpdateOps_RejectsUnknownOp(t *testing.T) {
	_, err := buildFieldPartialUpdateOps([]FieldPartialUpdateOpReq{
		{FieldName: "tags", Op: "ARRAY_EXTEND"},
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported partial update op")
}

func TestBuildFieldPartialUpdateOps_PathReplaceIsV2Only(t *testing.T) {
	fieldOps := []FieldPartialUpdateOpReq{{FieldName: "profile", Op: "PATH_REPLACE", Path: "[1][age]"}}
	_, err := buildFieldPartialUpdateOps(fieldOps)
	assert.Error(t, err)

	ops, err := buildFieldPartialUpdateOpsV2(fieldOps)
	assert.NoError(t, err)
	assert.Len(t, ops, 1)
	assert.Equal(t, schemapb.FieldPartialUpdateOp_PATH_REPLACE, ops[0].GetOp())
	assert.Equal(t, "[1][age]", ops[0].GetPath())
}

func TestBuildFieldPartialUpdateOps_LegacyBuilderDoesNotForwardPath(t *testing.T) {
	ops, err := buildFieldPartialUpdateOps([]FieldPartialUpdateOpReq{{
		FieldName: "tags",
		Op:        "ARRAY_APPEND",
		Path:      "[1]",
	}})
	assert.NoError(t, err)
	assert.Len(t, ops, 1)
	assert.Empty(t, ops[0].GetPath())
}

func TestHasNonReplaceFieldPartialUpdateOp(t *testing.T) {
	assert.False(t, hasNonReplaceFieldPartialUpdateOp(nil))
	assert.False(t, hasNonReplaceFieldPartialUpdateOp([]*schemapb.FieldPartialUpdateOp{{Op: schemapb.FieldPartialUpdateOp_REPLACE}}))
	assert.True(t, hasNonReplaceFieldPartialUpdateOp([]*schemapb.FieldPartialUpdateOp{{Op: schemapb.FieldPartialUpdateOp_PATH_REPLACE}}))
}

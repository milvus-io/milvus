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

	"github.com/milvus-io/milvus/pkg/v2/util/requestutil"
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

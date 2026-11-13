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
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// validateResponseFormat rejects invalid formats before schema lookup or RPC.
func validateResponseFormat(c *gin.Context, value string) (bool, error) {
	namespaced, err := useNamespacedResponse(value)
	if err != nil {
		HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
	}
	return namespaced, err
}

func buildQueryRespWithMetadata(namespaced bool, needFields []string, fieldDataList []*schemapb.FieldData, metadata queryResultMetadata,
	enableInt64 bool, collectionSchema *schemapb.CollectionSchema, requestedFields []string, vectorEncoding floatVectorEncoding,
) ([]map[string]interface{}, error) {
	if !namespaced {
		rows, err := buildQueryRespWithEncoding(0, needFields, fieldDataList, metadata.ids, metadata.scores, enableInt64, collectionSchema, vectorEncoding)
		if err == nil {
			if metadata.searchOffsets != nil {
				err = setSearchElementOffsets(rows, metadata.searchOffsets)
			} else if metadata.queryOffsets != nil {
				err = setQueryElementOffsets(rows, metadata.queryOffsets)
			}
		}
		return rows, err
	}
	rows, err := buildQueryRespWithFormat(0, needFields, fieldDataList, metadata.ids, metadata.scores, enableInt64, collectionSchema, true, requestedFields, vectorEncoding)
	if err != nil {
		return nil, err
	}
	if len(metadata.searchOffsets.GetData()) == 0 && len(metadata.queryOffsets) == 0 {
		return rows, nil
	}
	// Reuse the offset setters after decoding, with each row's metadata as the destination.
	systemRows := make([]map[string]interface{}, len(rows))
	for i, row := range rows {
		systemRows[i] = row["system"].(map[string]interface{})
	}
	if err := setSearchElementOffsets(systemRows, metadata.searchOffsets); err != nil {
		return nil, err
	}
	if err := setQueryElementOffsets(systemRows, metadata.queryOffsets); err != nil {
		return nil, err
	}
	return rows, nil
}

func namespacedJSONFieldsToStrings(rows []map[string]interface{}, names map[string]struct{}) {
	for _, row := range rows {
		fields := row["fields"].(map[string]interface{})
		for name := range names {
			if raw, ok := fields[name].(json.RawMessage); ok {
				fields[name] = string(raw)
			}
		}
	}
}

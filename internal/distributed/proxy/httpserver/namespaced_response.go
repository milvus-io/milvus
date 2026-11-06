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
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func buildNamespacedQueryResp(rowsNum int64, needFields []string, fieldDataList []*schemapb.FieldData, metadata queryResultMetadata,
	enableInt64 bool, collectionSchema *schemapb.CollectionSchema, requestedFields []string,
) ([]map[string]interface{}, error) {
	rows, err := buildQueryRespWithFormat(rowsNum, needFields, fieldDataList, metadata.ids, metadata.scores, enableInt64, collectionSchema, true, requestedFields)
	if err != nil {
		return nil, err
	}
	// Offset validation stays outside the row builder, after all fields are decoded.
	if err := metadata.checkOffsetCount(len(rows)); err != nil {
		return nil, err
	}
	if offsets := metadata.searchOffsets.GetData(); len(offsets) != 0 {
		for i, row := range rows {
			row["meta"].(map[string]interface{})[HTTPReturnOffset] = offsets[i]
		}
	}
	if len(metadata.queryOffsets) != 0 {
		for i, row := range rows {
			row["meta"].(map[string]interface{})[HTTPReturnOffset] = nonNilSlice(metadata.queryOffsets[i].GetIndices().GetData())
		}
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

func buildNamespacedSearchAggregationResp(results *schemapb.SearchResultData, enableInt64 bool, collectionSchema *schemapb.CollectionSchema) ([]gin.H, error) {
	return buildSearchAggregationRespWithFormat(results, enableInt64, collectionSchema, true)
}

func buildNamespacedAggHitsResp(hits []*schemapb.AggHit, enableInt64 bool, collectionSchema *schemapb.CollectionSchema) []gin.H {
	resp := make([]gin.H, 0, len(hits))
	pkFieldName := getRESTPrimaryFieldName(collectionSchema)
	dynamicField := typeutil.GetDynamicField(collectionSchema)
	for _, hit := range hits {
		row := newNamespacedResultRow()
		if hit == nil {
			resp = append(resp, row.output)
			continue
		}
		row.fields[pkFieldName] = aggHitPKToRESTAny(hit, enableInt64)
		row.meta[HTTPReturnDistance] = hit.GetScore()
		for _, field := range hit.GetFields() {
			if field == nil {
				continue
			}
			fieldName := field.GetFieldName()
			if fieldName == "" {
				fieldName = strconv.FormatInt(field.GetFieldId(), 10)
			}
			fields := row.fields
			if dynamicField != nil && field.GetFieldId() == dynamicField.GetFieldID() {
				// Route the aggregation column as received, without decoding its JSON payload.
				fields = row.dynamicFields
			}
			fields[fieldName] = aggHitFieldValueToRESTAny(field, enableInt64)
		}
		resp = append(resp, row.output)
	}
	return resp
}

func (metadata queryResultMetadata) checkOffsetCount(rows int) error {
	if n := len(metadata.searchOffsets.GetData()); n != 0 && n != rows {
		return wrapResultStageErr("search element offsets", merr.WrapErrServiceInternalMsg(
			"search result has %d element indices for %d hits", n, rows))
	}
	if n := len(metadata.queryOffsets); n != 0 && n != rows {
		return wrapResultStageErr("query element offsets", merr.WrapErrServiceInternalMsg(
			"query result has %d element index lists for %d rows", n, rows))
	}
	return nil
}

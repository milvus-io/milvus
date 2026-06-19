/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tasks

import (
	"fmt"

	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// marshalReduceResult converts a mergeResult into a SearchResultData proto.
// It exports $id, $score, and (when present) the $group_by column from the
// merged DataFrame produced by heapMergeReduce. Output field data is filled in
// later by lateMaterializeOutputFields.
func marshalReduceResult(result *mergeResult) (*schemapb.SearchResultData, error) {
	if result == nil || result.DF == nil {
		return nil, merr.WrapErrServiceInternal("nil reduce result")
	}
	groupByColumns := groupByColumnNames(result.DF)
	data, err := chain.ToSearchResultDataWithOptions(result.DF, &chain.ExportOptions{
		GroupByFields: groupByColumns,
		// $element_indices is segment-specific (element-level search metadata)
		// and written to SearchResultData.ElementIndices below; keep it out of
		// the generic FieldsData export.
		SkipColumns: []string{elementIndicesCol},
	})
	if err != nil {
		return nil, err
	}
	// chain.ExportOptions populates GroupByFieldValues.FieldName from internal
	// $group_by_<fieldID> column names. Clear them so proxy.fillFieldNames
	// resolves the real schema field names from FieldId.
	for _, gbv := range data.GetGroupByFieldValues() {
		if gbv != nil {
			gbv.FieldName = ""
		}
	}
	elementIndices, err := exportElementIndices(result.DF)
	if err != nil {
		return nil, err
	}
	data.ElementIndices = elementIndices
	return data, nil
}

// exportElementIndices extracts the $element_indices int32 column from the
// merged DataFrame and widens it to int64 for the SearchResultData.ElementIndices
// LongArray proto field. Returns nil if the column is absent or empty. A
// non-int32 chunk is a contract violation (heapMergeReduce emits int32 only)
// and is surfaced as an error rather than silently dropped, since skipping a
// chunk would misalign ElementIndices with the rest of the result rows.
func exportElementIndices(df *chain.DataFrame) (*schemapb.LongArray, error) {
	if !df.HasColumn(elementIndicesCol) {
		return nil, nil
	}
	col := df.Column(elementIndicesCol)
	if col == nil {
		return nil, nil
	}
	data := make([]int64, 0, col.Len())
	for i, chunk := range col.Chunks() {
		int32Arr, ok := chunk.(*array.Int32)
		if !ok {
			return nil, merr.WrapErrServiceInternal(
				fmt.Sprintf("%s chunk %d: expected *array.Int32, got %T", elementIndicesCol, i, chunk))
		}
		for j := 0; j < int32Arr.Len(); j++ {
			data = append(data, int64(int32Arr.Value(j)))
		}
	}
	if len(data) == 0 {
		return nil, nil
	}
	return &schemapb.LongArray{Data: data}, nil
}

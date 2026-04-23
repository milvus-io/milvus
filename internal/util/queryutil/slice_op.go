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

package queryutil

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// SliceOperator applies offset and limit to a RetrieveResult.
// This operator is used at proxy-side to apply final pagination.
//
// For element-level queries (StructArray), offset and limit count individual
// array elements rather than documents. A single document may contribute
// multiple elements, and offset/limit boundaries can fall mid-document,
// requiring ElementIndices trimming.
type SliceOperator struct {
	limit  int64
	offset int64
}

// NewSliceOperator creates a new slice operator.
// limit: maximum number of results to return (-1 for unlimited)
// offset: number of results to skip
func NewSliceOperator(limit, offset int64) *SliceOperator {
	return &SliceOperator{
		limit:  limit,
		offset: offset,
	}
}

func (op *SliceOperator) Name() string {
	return OpSlice
}

// Run applies offset and limit to a RetrieveResult.
// Input[0]: *internalpb.RetrieveResults
// Output[0]: *internalpb.RetrieveResults (sliced)
func (op *SliceOperator) Run(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
	_, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "SliceOperator")
	defer sp.End()

	result := inputs[0].(*internalpb.RetrieveResults)

	if result == nil || len(result.GetFieldsData()) == 0 {
		return []any{result}, nil
	}

	var sliced *internalpb.RetrieveResults
	if result.GetElementLevel() {
		sliced = op.applyElementLevelSlice(result)
	} else {
		sliced = op.applySlice(result)
	}
	return []any{sliced}, nil
}

// applySlice applies doc-level offset and limit to the result.
func (op *SliceOperator) applySlice(result *internalpb.RetrieveResults) *internalpb.RetrieveResults {
	size := getRowCount(result)
	if size == 0 {
		return result
	}

	start := int(op.offset)
	if start >= size {
		return &internalpb.RetrieveResults{}
	}

	end := size
	if op.limit > 0 && start+int(op.limit) < end {
		end = start + int(op.limit)
	}

	if start == 0 && end == size {
		return result
	}

	return rangeSliceRetrieveResults(result, start, end)
}

// applyElementLevelSlice applies element-level offset and limit.
// Offset and limit count individual array elements, not documents.
// When a boundary falls mid-document, the document's ElementIndices are trimmed.
func (op *SliceOperator) applyElementLevelSlice(result *internalpb.RetrieveResults) *internalpb.RetrieveResults {
	docCount := getRowCount(result)
	elemIndices := result.GetElementIndices()
	if docCount == 0 || len(elemIndices) == 0 {
		return result
	}

	// Phase 1: Apply offset by skipping elements
	startDoc := 0
	var startTrimCount int // number of elements to trim from the start of startDoc (0 = no trim)
	if op.offset > 0 {
		var skipped int64
		for startDoc < docCount && skipped < op.offset {
			if startDoc >= len(elemIndices) {
				break
			}
			docElemCount := int64(len(elemIndices[startDoc].GetIndices()))
			if skipped+docElemCount > op.offset {
				// Offset lands mid-document: record how many elements to trim
				startTrimCount = int(op.offset - skipped)
				break
			}
			skipped += docElemCount
			startDoc++
		}
		if startDoc >= docCount {
			return &internalpb.RetrieveResults{ElementLevel: true}
		}
	}

	// Phase 2: Apply limit by counting elements
	endDoc := docCount   // default: take all remaining
	var endTrimCount int // number of elements to keep in endDoc-1 (0 = no trim, keep all)
	if op.limit > 0 {
		var collected int64
		for i := startDoc; i < docCount; i++ {
			if i >= len(elemIndices) {
				endDoc = i
				break
			}
			docElemCount := int64(len(elemIndices[i].GetIndices()))
			if i == startDoc && startTrimCount > 0 {
				docElemCount -= int64(startTrimCount)
			}
			if collected+docElemCount >= op.limit {
				// Limit reached at or within this document
				remaining := op.limit - collected
				fullCount := int64(len(elemIndices[i].GetIndices()))
				if i == startDoc && startTrimCount > 0 {
					// Both trim from start and end of same doc
					endTrimCount = startTrimCount + int(remaining)
				} else if remaining < fullCount {
					endTrimCount = int(remaining)
				}
				endDoc = i + 1
				break
			}
			collected += docElemCount
		}
	}

	if startDoc >= endDoc {
		return &internalpb.RetrieveResults{ElementLevel: true}
	}

	// Phase 3: Slice field data at document granularity [startDoc, endDoc)
	sliced := rangeSliceRetrieveResults(result, startDoc, endDoc)

	// Phase 4: Apply element-level trims to ElementIndices
	if startTrimCount > 0 && endTrimCount > 0 && startDoc+1 == endDoc {
		// Both trims apply to the same (single) document
		original := sliced.GetElementIndices()[0].GetIndices()
		sliced.ElementIndices[0] = &internalpb.ElementIndices{
			Indices: original[startTrimCount:endTrimCount],
		}
	} else {
		if startTrimCount > 0 && len(sliced.GetElementIndices()) > 0 {
			original := sliced.GetElementIndices()[0].GetIndices()
			sliced.ElementIndices[0] = &internalpb.ElementIndices{
				Indices: original[startTrimCount:],
			}
		}
		if endTrimCount > 0 && len(sliced.GetElementIndices()) > 0 {
			lastIdx := len(sliced.GetElementIndices()) - 1
			original := sliced.GetElementIndices()[lastIdx].GetIndices()
			sliced.ElementIndices[lastIdx] = &internalpb.ElementIndices{
				Indices: original[:endTrimCount],
			}
		}
	}

	return sliced
}

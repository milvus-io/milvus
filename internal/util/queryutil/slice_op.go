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

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	var err error
	if result.GetElementLevel() {
		sliced, err = op.applyElementLevelSlice(result)
	} else {
		sliced, err = op.applySlice(result)
	}
	if err != nil {
		return nil, err
	}
	return []any{sliced}, nil
}

// applySlice applies doc-level offset and limit to the result.
func (op *SliceOperator) applySlice(result *internalpb.RetrieveResults) (*internalpb.RetrieveResults, error) {
	size := getRowCount(result)
	if size == 0 {
		return result, nil
	}

	window := op.selectDocWindow(size)
	if window.start == window.end {
		return rangeSliceRetrieveResults(result, window.start, window.end)
	}
	if window.start == 0 && window.end == size {
		return result, nil
	}
	return rangeSliceRetrieveResults(result, window.start, window.end)
}

// applyElementLevelSlice applies element-level offset and limit.
// Offset and limit count individual array elements, not documents.
// When a boundary falls mid-document, the document's ElementIndices are trimmed.
func (op *SliceOperator) applyElementLevelSlice(result *internalpb.RetrieveResults) (*internalpb.RetrieveResults, error) {
	docCount := getRowCount(result)
	elemIndices := result.GetElementIndices()
	if docCount == 0 || len(elemIndices) == 0 {
		return result, nil
	}

	window := op.selectElementWindow(docCount, func(row int) (*internalpb.ElementIndices, bool) {
		if row >= len(elemIndices) {
			return nil, false
		}
		return elemIndices[row], true
	})
	if window.start >= window.end {
		sliced, err := rangeSliceRetrieveResults(result, window.start, window.end)
		if err != nil {
			return nil, err
		}
		sliced.ElementLevel = true
		return sliced, nil
	}
	sliced, err := rangeSliceRetrieveResults(result, window.start, window.end)
	if err != nil {
		return nil, err
	}
	window.trimElements(sliced)
	return sliced, nil
}

// rowWindow describes a page before its field data is built. Element-level
// pages retain complete documents and trim only their ElementIndices.
type rowWindow struct {
	start, end           int
	firstTrim, lastCount int
}

func (op *SliceOperator) selectDocWindow(count int) rowWindow {
	if op.offset >= int64(count) {
		return rowWindow{start: count, end: count}
	}
	window := rowWindow{start: int(op.offset), end: count}
	if op.limit > 0 && op.limit < int64(count-window.start) {
		window.end = window.start + int(op.limit)
	}
	return window
}

// selectElementWindow is shared by payload slicing and refs-only pagination.
// indicesAt reports false for absent metadata so existing single-source Slice
// semantics are preserved; multi-source callers validate shape separately.
func (op *SliceOperator) selectElementWindow(docCount int, indicesAt func(int) (*internalpb.ElementIndices, bool)) rowWindow {
	window := rowWindow{end: docCount}
	if op.offset > 0 {
		var skipped int64
		for window.start < docCount && skipped < op.offset {
			indices, ok := indicesAt(window.start)
			if !ok {
				break
			}
			n := int64(len(indices.GetIndices()))
			if skipped+n > op.offset {
				window.firstTrim = int(op.offset - skipped)
				break
			}
			skipped += n
			window.start++
		}
		if window.start == docCount {
			return window
		}
	}
	if op.limit > 0 {
		var collected int64
		for row := window.start; row < docCount; row++ {
			indices, ok := indicesAt(row)
			if !ok {
				window.end = row
				break
			}
			fullCount := len(indices.GetIndices())
			n := int64(fullCount)
			if row == window.start {
				n -= int64(window.firstTrim)
			}
			if collected+n >= op.limit {
				remaining := int(op.limit - collected)
				if row == window.start && window.firstTrim > 0 {
					window.lastCount = window.firstTrim + remaining
				} else if remaining < fullCount {
					window.lastCount = remaining
				}
				window.end = row + 1
				break
			}
			collected += n
		}
	}
	return window
}

func (window rowWindow) trimElements(result *internalpb.RetrieveResults) {
	indices := result.GetElementIndices()
	if window.firstTrim > 0 && window.lastCount > 0 && len(indices) == 1 {
		indices[0] = &internalpb.ElementIndices{Indices: indices[0].GetIndices()[window.firstTrim:window.lastCount]}
		return
	}
	if window.firstTrim > 0 && len(indices) > 0 {
		indices[0] = &internalpb.ElementIndices{Indices: indices[0].GetIndices()[window.firstTrim:]}
	}
	if window.lastCount > 0 && len(indices) > 0 {
		last := len(indices) - 1
		indices[last] = &internalpb.ElementIndices{Indices: indices[last].GetIndices()[:window.lastCount]}
	}
}

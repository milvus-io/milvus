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

	sliced := op.applySlice(result)
	return []any{sliced}, nil
}

// applySlice applies offset and limit to the result.
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

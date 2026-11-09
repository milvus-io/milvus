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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// NewSortAndCheckPKPageOperator combines proxy PK selection and pagination.
// PK validation and iterator drain handling finish before pagination; payloads
// are then materialized only for the final page. Its output is equivalent to
// NewSortAndCheckPKOperator followed by NewSliceOperator.
func NewSortAndCheckPKPageOperator(reduceType reduce.IReduceType, schema *schemapb.CollectionSchema, limit, offset int64) Operator {
	return &pkPageOperator{
		reduce: ReduceByPKOperator{reduceType: reduceType, schema: schema},
		page:   SliceOperator{limit: limit, offset: offset},
	}
}

type pkPageOperator struct {
	reduce ReduceByPKOperator
	page   SliceOperator
}

func (op *pkPageOperator) Name() string {
	return OpReduceByPK
}

func (op *pkPageOperator) Run(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
	_, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "ReduceByPKPageOperator")
	defer sp.End()

	rawResults := inputs[0].([]*internalpb.RetrieveResults)
	// Validate before filtering so empty shards retain their declared outputs.
	emptyComputed, err := prepareComputedResults(rawResults)
	if err != nil {
		return nil, err
	}
	results, hasMore := filterPKResults(rawResults)
	if len(results) == 0 {
		return []any{&internalpb.RetrieveResults{ComputedResults: emptyComputed}}, nil
	}
	if len(results) == 1 {
		// Preserve the existing single-source validation contract: the proxy
		// trusts the shard's PK ordering/deduplication and only applies Slice.
		return op.page.Run(ctx, span, results[0])
	}

	rows, err := op.reduce.selectRowsByPK(results)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return []any{&internalpb.RetrieveResults{HasMoreResult: hasMore, ComputedResults: emptyComputed}}, nil
	}

	window := op.page.selectRowWindow(results, rows)
	// The range materializer also checks rows outside the page. Moving
	// pagination must not hide malformed fields that the full merge rejected.
	result, err := buildMergedRetrieveResultsRange(results, rows, window.start, window.end, op.reduce.schema)
	if err != nil {
		return nil, err
	}
	if window.start == window.end {
		return []any{&internalpb.RetrieveResults{ElementLevel: result.GetElementLevel(), ComputedResults: result.GetComputedResults()}}, nil
	}

	if result.GetElementLevel() {
		window.trimElements(result)
	} else if window.start == 0 && window.end == len(rows) {
		// Slice preserves the original result only when a doc-level page
		// covers it entirely. Partial slices do not propagate HasMoreResult.
		result.HasMoreResult = hasMore
	}
	return []any{result}, nil
}

// selectRowWindow reads pagination metadata through refs without constructing
// intermediate IDs or field payloads. SliceOperator uses the same window logic.
func (op *SliceOperator) selectRowWindow(results []*internalpb.RetrieveResults, rows []rowRef) rowWindow {
	if !results[rows[0].resultIdx].GetElementLevel() {
		return op.selectDocWindow(len(rows))
	}
	return op.selectElementWindow(len(rows), func(row int) (*internalpb.ElementIndices, bool) {
		ref := rows[row]
		indices := results[ref.resultIdx].GetElementIndices()
		if ref.rowIdx >= int64(len(indices)) {
			// The full-selection materializer reports the existing shape error.
			return nil, true
		}
		return indices[ref.rowIdx], true
	})
}

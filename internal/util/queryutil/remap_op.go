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
	"fmt"

	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// RemapOperator reorders FieldsData by pre-computed positional indices.
// Used by GROUP BY + ORDER BY path (newAggRemapOperator) where the field
// layout is known at pipeline construction time.
//
// outputIndices maps each desired output position to the input FieldsData index.
type RemapOperator struct {
	outputIndices []int
}

// NewRemapOperator creates a remap operator with pre-computed index mapping.
func NewRemapOperator(outputIndices []int) *RemapOperator {
	return &RemapOperator{outputIndices: outputIndices}
}

func (op *RemapOperator) Name() string {
	return OpRemap
}

// Run remaps FieldsData by positional indices.
// Input[0]: *internalpb.RetrieveResults
// Output[0]: *internalpb.RetrieveResults (with reordered FieldsData)
func (op *RemapOperator) Run(_ context.Context, _ trace.Span, inputs ...any) ([]any, error) {
	result := inputs[0].(*internalpb.RetrieveResults)

	if result == nil || len(result.GetFieldsData()) == 0 || len(op.outputIndices) == 0 {
		return []any{result}, nil
	}

	fieldsData := result.GetFieldsData()
	newFieldsData := make([]*schemapb.FieldData, 0, len(op.outputIndices))
	for _, pos := range op.outputIndices {
		if pos < 0 || pos >= len(fieldsData) {
			return nil, fmt.Errorf("RemapOperator: output index %d out of range [0, %d)", pos, len(fieldsData))
		}
		newFieldsData = append(newFieldsData, fieldsData[pos])
	}

	result.FieldsData = newFieldsData
	return []any{result}, nil
}

// FieldIDRemapOperator reorders FieldsData by matching FieldID at runtime.
// Used by ORDER BY (non-aggregation) path where the user's desired output
// fields are identified by FieldID.
//
// This approach is more robust than name-based matching because:
//   - Dynamic field subkeys (e.g., "x") map to the same FieldID as "$meta"
//   - No need to reconstruct segcore's positional layout in Go
//   - Eliminates implicit dependency on C++ BuildOrderByProjectNode layout
type FieldIDRemapOperator struct {
	desiredFieldIDs []int64
}

// NewFieldIDRemapOperator creates a remap operator that matches fields by FieldID.
// desiredFieldIDs should come from filterSystemFields(RetrieveRequest.OutputFieldsId).
func NewFieldIDRemapOperator(desiredFieldIDs []int64) *FieldIDRemapOperator {
	return &FieldIDRemapOperator{desiredFieldIDs: desiredFieldIDs}
}

func (op *FieldIDRemapOperator) Name() string {
	return OpRemap
}

// Run reorders FieldsData to match desiredFieldIDs order.
// Fields not in desiredFieldIDs (e.g., PK, timestamp, ORDER BY-only fields)
// are excluded from the output.
//
// Input[0]: *internalpb.RetrieveResults
// Output[0]: *internalpb.RetrieveResults (with reordered FieldsData)
func (op *FieldIDRemapOperator) Run(_ context.Context, _ trace.Span, inputs ...any) ([]any, error) {
	result := inputs[0].(*internalpb.RetrieveResults)

	if result == nil || len(result.GetFieldsData()) == 0 || len(op.desiredFieldIDs) == 0 {
		return []any{result}, nil
	}

	// Build FieldID → position map from actual result
	idToPos := make(map[int64]int, len(result.GetFieldsData()))
	for i, fd := range result.GetFieldsData() {
		idToPos[fd.GetFieldId()] = i
	}

	newFieldsData := make([]*schemapb.FieldData, 0, len(op.desiredFieldIDs))
	for _, fid := range op.desiredFieldIDs {
		if pos, ok := idToPos[fid]; ok {
			newFieldsData = append(newFieldsData, result.GetFieldsData()[pos])
		}
		// Fields not found are silently skipped — this can happen when
		// a desired field was not returned by segcore (e.g., schema evolution,
		// field added after some segments were created).
	}

	result.FieldsData = newFieldsData
	return []any{result}, nil
}

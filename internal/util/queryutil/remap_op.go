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

	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

// RemapOperator reorders FieldsData from segcore's positional layout
// to the user's requested output field order.
//
// Segcore returns: [pk, orderby_fields, non-sort-output-fields]
// User wants: their original outputFields order (without pk/sort-only fields).
//
// outputIndices maps each desired output position to the segcore FieldsData index.
// For example, if user wants [A, B, C] and segcore returns [pk, B, C, A],
// outputIndices = [3, 1, 2].
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

// Run remaps FieldsData from segcore positional layout to user's output order.
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
		if pos < len(fieldsData) {
			newFieldsData = append(newFieldsData, fieldsData[pos])
		}
	}

	result.FieldsData = newFieldsData
	return []any{result}, nil
}

// BuildRemapIndices computes the index mapping from segcore positional layout
// to user's desired output field order.
//
// Parameters:
//   - pkFieldName: primary key field name
//   - orderByFieldNames: ORDER BY field names in sort priority order
//   - outputFieldNames: all output field names sent to segcore (from output_field_ids)
//   - userOutputFields: user's requested output field names in desired order
//
// The segcore positional layout is: [pk, orderby_fields(deduped), remaining_output_fields(deduped)]
// This function reconstructs that layout and maps each userOutputField to its position.
func BuildRemapIndices(
	pkFieldName string,
	orderByFieldNames []string,
	outputFieldNames []string,
	userOutputFields []string,
) []int {
	// Reconstruct segcore positional layout (mirrors PlanProto.cpp logic)
	var segcoreLayout []string
	seen := make(map[string]bool)

	// Position 0: PK
	segcoreLayout = append(segcoreLayout, pkFieldName)
	seen[pkFieldName] = true

	// Positions 1..N: ORDER BY fields (deduped)
	for _, name := range orderByFieldNames {
		if !seen[name] {
			segcoreLayout = append(segcoreLayout, name)
			seen[name] = true
		}
	}

	// Positions N+1..M: remaining output fields (deduped)
	for _, name := range outputFieldNames {
		if !seen[name] {
			segcoreLayout = append(segcoreLayout, name)
			seen[name] = true
		}
	}

	// Build name-to-position map
	nameToPos := make(map[string]int, len(segcoreLayout))
	for i, name := range segcoreLayout {
		nameToPos[name] = i
	}

	// Map each user output field to its segcore position
	indices := make([]int, 0, len(userOutputFields))
	for _, name := range userOutputFields {
		if pos, ok := nameToPos[name]; ok {
			indices = append(indices, pos)
		}
	}

	return indices
}

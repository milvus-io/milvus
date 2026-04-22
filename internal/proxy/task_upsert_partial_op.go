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

package proxy

import (
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// hasNonReplacePartialOp reports whether req carries any non-REPLACE
// FieldPartialUpdateOp. Used to decide implicit promotion to
// partial_update=true and to short-circuit validation when no op is set.
func hasNonReplacePartialOp(req *milvuspb.UpsertRequest) bool {
	for _, op := range req.GetFieldOps() {
		if op.GetOp() != schemapb.FieldPartialUpdateOp_REPLACE {
			return true
		}
	}
	return false
}

// validateFieldPartialUpdateOps validates FieldPartialUpdateOp entries
// attached to an UpsertRequest. It enforces:
//
//   - Each op targets a field present in the collection schema.
//   - Array-specific ops (ARRAY_APPEND, ARRAY_REMOVE) require the target
//     field to have DataType_Array.
//   - The target field must not be the primary key.
//   - The FieldData carried in fields_data must declare a matching
//     ElementType when the op targets an Array field.
//   - For ARRAY_APPEND, the per-row payload length must not, on its own,
//     already exceed max_capacity. The merged length is additionally
//     enforced at merge time by ApplyArrayRowOp.
//   - A given field_name appears at most once across field_ops.
//
// Returns (true, nil) when at least one non-REPLACE op was observed, so
// the caller may implicitly promote partial_update=true.
func validateFieldPartialUpdateOps(req *milvuspb.UpsertRequest, schema *schemapb.CollectionSchema) (bool, error) {
	fieldOps := req.GetFieldOps()
	if len(fieldOps) == 0 {
		return false, nil
	}

	// Precompute PK names and a lookup table of FieldData by name so we
	// can validate payload alignment in O(1) per op.
	pkFields := make(map[string]struct{})
	for _, f := range schema.GetFields() {
		if f.GetIsPrimaryKey() {
			pkFields[f.GetName()] = struct{}{}
		}
	}
	fieldDataByName := make(map[string]*schemapb.FieldData, len(req.GetFieldsData()))
	for _, fd := range req.GetFieldsData() {
		fieldDataByName[fd.GetFieldName()] = fd
	}

	nonReplaceSeen := false
	seenOpFields := make(map[string]struct{}, len(fieldOps))
	for _, opMsg := range fieldOps {
		name := opMsg.GetFieldName()
		if name == "" {
			return false, merr.WrapErrParameterInvalidMsg("FieldPartialUpdateOp.field_name is required")
		}
		if _, dup := seenOpFields[name]; dup {
			return false, merr.WrapErrParameterInvalidMsg(
				fmt.Sprintf("duplicate partial-update op for field %q", name))
		}
		seenOpFields[name] = struct{}{}

		op := opMsg.GetOp()
		if op == schemapb.FieldPartialUpdateOp_REPLACE {
			// An explicit REPLACE is legal but indistinguishable from no
			// op at all. Accept silently — no further validation needed.
			continue
		}
		nonReplaceSeen = true

		if _, isPK := pkFields[name]; isPK {
			return false, merr.WrapErrParameterInvalidMsg(
				fmt.Sprintf("field %q is the primary key and cannot carry a partial-update op", name))
		}

		fieldSchema, err := findFieldSchemaByName(schema, name)
		if err != nil {
			return false, err
		}

		switch op {
		case schemapb.FieldPartialUpdateOp_ARRAY_APPEND, schemapb.FieldPartialUpdateOp_ARRAY_REMOVE:
			if fieldSchema.GetDataType() != schemapb.DataType_Array {
				return false, merr.WrapErrParameterInvalidMsg(
					fmt.Sprintf("op %s requires Array field, but field %q is %s",
						op.String(), name, fieldSchema.GetDataType().String()))
			}
		default:
			return false, merr.WrapErrParameterInvalidMsg(
				fmt.Sprintf("unsupported partial update op: %s", op.String()))
		}

		fd, ok := fieldDataByName[name]
		if !ok {
			return false, merr.WrapErrParameterInvalidMsg(
				fmt.Sprintf("partial-update op targets field %q not present in fields_data", name))
		}
		if got := fd.GetScalars().GetArrayData().GetElementType(); got != schemapb.DataType_None && got != fieldSchema.GetElementType() {
			return false, merr.WrapErrParameterInvalidMsg(
				fmt.Sprintf("field %q expects element type %s but request provides %s",
					name, fieldSchema.GetElementType().String(), got.String()))
		}

		if op == schemapb.FieldPartialUpdateOp_ARRAY_APPEND {
			if err := checkArrayAppendPayloadWithinCapacity(fd, fieldSchema); err != nil {
				return false, err
			}
		}
	}
	return nonReplaceSeen, nil
}

// buildFieldOpMap returns a fieldName → op lookup. Callers use it during
// merge to decide which op (if any) to apply to each Array field. A nil
// map (no ops) is a valid return for the all-REPLACE default.
func buildFieldOpMap(req *milvuspb.UpsertRequest) map[string]schemapb.FieldPartialUpdateOp_OpType {
	fieldOps := req.GetFieldOps()
	if len(fieldOps) == 0 {
		return nil
	}
	out := make(map[string]schemapb.FieldPartialUpdateOp_OpType, len(fieldOps))
	for _, op := range fieldOps {
		out[op.GetFieldName()] = op.GetOp()
	}
	return out
}

// findFieldSchemaByName locates a field by name in the given collection
// schema. Returns a descriptive parameter-invalid error when not found.
func findFieldSchemaByName(schema *schemapb.CollectionSchema, name string) (*schemapb.FieldSchema, error) {
	for _, f := range schema.GetFields() {
		if f.GetName() == name {
			return f, nil
		}
	}
	return nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("field %q not found in collection schema", name))
}

// checkArrayAppendPayloadWithinCapacity checks that the per-row payload
// length itself does not already exceed max_capacity. The final merged
// length is enforced at merge time by ApplyArrayRowOp.
//
// When max_capacity is not declared or is non-positive, the check is a
// no-op (matches the behavior of legacy upserts with no capacity gate).
func checkArrayAppendPayloadWithinCapacity(fd *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) error {
	maxCap := readMaxCapacity(fieldSchema)
	if maxCap <= 0 {
		return nil
	}
	rows := fd.GetScalars().GetArrayData().GetData()
	for rowIdx, row := range rows {
		got := perRowArrayLen(row, fieldSchema.GetElementType())
		if got > maxCap {
			return merr.WrapErrParameterInvalidMsg(
				fmt.Sprintf("ARRAY_APPEND payload for field %q row %d has length %d exceeding max_capacity %d",
					fd.GetFieldName(), rowIdx, got, maxCap))
		}
	}
	return nil
}

// readMaxCapacity returns the declared max_capacity of an Array field, or
// 0 when missing/invalid. The value lives in type_params under the
// well-known common.MaxCapacityKey key.
func readMaxCapacity(fieldSchema *schemapb.FieldSchema) int {
	for _, kv := range fieldSchema.GetTypeParams() {
		if kv.GetKey() != common.MaxCapacityKey {
			continue
		}
		v, err := strconv.Atoi(kv.GetValue())
		if err != nil {
			return 0
		}
		return v
	}
	return 0
}

// perRowArrayLen returns the element count of a single Array row given
// its declared ElementType. Unsupported element types return 0.
func perRowArrayLen(row *schemapb.ScalarField, elementType schemapb.DataType) int {
	switch elementType {
	case schemapb.DataType_Bool:
		return len(row.GetBoolData().GetData())
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		return len(row.GetIntData().GetData())
	case schemapb.DataType_Int64:
		return len(row.GetLongData().GetData())
	case schemapb.DataType_Float:
		return len(row.GetFloatData().GetData())
	case schemapb.DataType_Double:
		return len(row.GetDoubleData().GetData())
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		return len(row.GetStringData().GetData())
	default:
		return 0
	}
}

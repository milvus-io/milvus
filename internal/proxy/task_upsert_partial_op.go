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
	"slices"
	"strconv"
	"strings"
	"unicode"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	pathReplaceParentArray       = "array"
	pathReplaceParentStructArray = "struct_array"
)

func observePathReplaceParentOperations(req *milvuspb.UpsertRequest, plans map[string]*fieldPartialUpdatePlan) {
	for _, plan := range plans {
		if !plan.isPathReplace() {
			continue
		}
		parentType := pathReplaceParentArray
		if plan.structParent != nil {
			parentType = pathReplaceParentStructArray
		}
		metrics.ProxyPathReplaceParentOperations.WithLabelValues(
			paramtable.GetStringNodeID(),
			req.GetDbName(),
			req.GetCollectionName(),
			parentType,
		).Inc()
	}
}

type fieldPartialUpdatePlan struct {
	op              schemapb.FieldPartialUpdateOp_OpType
	arrayParent     *schemapb.FieldSchema
	structParent    *schemapb.StructArrayFieldSchema
	index           int
	explicitChild   *schemapb.FieldSchema
	operandChildren []*schemapb.FieldSchema
}

func (p *fieldPartialUpdatePlan) isPathReplace() bool {
	return p != nil && p.op == schemapb.FieldPartialUpdateOp_PATH_REPLACE
}

// TODO: Recursive Array fields do not support ARRAY_APPEND, ARRAY_REMOVE, or
// PATH_REPLACE.
func resolveFieldPartialUpdateOps(req *milvuspb.UpsertRequest, schema *schemapb.CollectionSchema) (map[string]*fieldPartialUpdatePlan, bool, error) {
	fieldOps := req.GetFieldOps()
	if len(fieldOps) == 0 {
		return nil, false, nil
	}
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		return nil, false, err
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
		if _, duplicate := fieldDataByName[fd.GetFieldName()]; duplicate {
			return nil, false, merr.WrapErrParameterInvalidMsg("duplicate fields_data entry for field %q", fd.GetFieldName())
		}
		fieldDataByName[fd.GetFieldName()] = fd
	}

	nonReplaceSeen := false
	seenOpFields := make(map[string]struct{}, len(fieldOps))
	plans := make(map[string]*fieldPartialUpdatePlan, len(fieldOps))
	for _, opMsg := range fieldOps {
		name := opMsg.GetFieldName()
		if name == "" {
			return nil, false, merr.WrapErrParameterMissingMsg("FieldPartialUpdateOp.field_name is required")
		}
		if _, dup := seenOpFields[name]; dup {
			return nil, false, merr.WrapErrParameterInvalidMsg(
				fmt.Sprintf("duplicate partial-update op for field %q", name))
		}
		seenOpFields[name] = struct{}{}

		op := opMsg.GetOp()
		if op != schemapb.FieldPartialUpdateOp_PATH_REPLACE && opMsg.GetPath() != "" {
			return nil, false, merr.WrapErrParameterInvalidMsg("path is only supported for PATH_REPLACE, field %q uses %s", name, op.String())
		}
		if op == schemapb.FieldPartialUpdateOp_REPLACE {
			if typeutil.IsStructSubField(name) {
				return nil, false, merr.WrapErrParameterInvalidMsg(
					"partial struct update is not supported for struct sub-field %q; use the whole struct field instead", name)
			}
			// An explicit REPLACE is legal but indistinguishable from no
			// op at all. Accept silently — no further validation needed.
			plans[name] = &fieldPartialUpdatePlan{op: op}
			continue
		}
		nonReplaceSeen = true

		if _, isPK := pkFields[name]; isPK {
			return nil, false, merr.WrapErrParameterInvalidMsg(
				fmt.Sprintf("field %q is the primary key and cannot carry a partial-update op", name))
		}
		if typeutil.IsStructSubField(name) {
			return nil, false, merr.WrapErrParameterInvalidMsg(
				"op %s is not supported for struct field %q", op.String(), name)
		}

		if op == schemapb.FieldPartialUpdateOp_PATH_REPLACE {
			fd, ok := fieldDataByName[name]
			if !ok {
				return nil, false, merr.WrapErrParameterInvalidMsg(
					fmt.Sprintf("partial-update op targets field %q not present in fields_data", name))
			}
			index, childName, hasChild, err := parsePathReplace(opMsg.GetPath())
			if err != nil {
				return nil, false, merr.Wrapf(err, "invalid PATH_REPLACE path for field %q", name)
			}

			if structSchema := schemaHelper.GetStructArrayFieldFromName(name); structSchema != nil {
				plan := &fieldPartialUpdatePlan{op: op, structParent: structSchema, index: index}
				if hasChild {
					childSchema := findStructChildSchema(structSchema, childName)
					if childSchema == nil {
						return nil, false, merr.WrapErrParameterInvalidMsg(
							"child %q not found in struct field %q", childName, name)
					}
					plan.explicitChild = childSchema
				}
				children, err := validatePathReplaceStructOperand(fd, plan, int(req.GetNumRows()))
				if err != nil {
					return nil, false, err
				}
				plan.operandChildren = children
				plans[name] = plan
				continue
			}

			fieldSchema, err := findFieldSchemaByName(schema, name)
			if err != nil {
				return nil, false, err
			}
			if fieldSchema.GetDataType() != schemapb.DataType_Array || typeutil.IsNestedArrayTypeSchema(fieldSchema.GetTypeSchema()) {
				return nil, false, merr.WrapErrParameterInvalidMsg(
					"PATH_REPLACE requires a non-recursive Array or ArrayOfStruct field, but field %q is %s", name, fieldSchema.GetDataType().String())
			}
			if hasChild {
				return nil, false, merr.WrapErrParameterInvalidMsg(
					"child segment is only supported for ArrayOfStruct field %q", name)
			}
			if err := validatePathReplaceArrayOperand(fd, fieldSchema, int(req.GetNumRows())); err != nil {
				return nil, false, err
			}
			plans[name] = &fieldPartialUpdatePlan{op: op, arrayParent: fieldSchema, index: index}
			continue
		}

		if schemaHelper.GetStructArrayFieldFromName(name) != nil {
			return nil, false, merr.WrapErrParameterInvalidMsg(
				"op %s is not supported for struct field %q", op.String(), name)
		}

		fieldSchema, err := findFieldSchemaByName(schema, name)
		if err != nil {
			return nil, false, err
		}

		switch op {
		case schemapb.FieldPartialUpdateOp_ARRAY_APPEND, schemapb.FieldPartialUpdateOp_ARRAY_REMOVE:
			if typeutil.IsNestedArrayTypeSchema(fieldSchema.GetTypeSchema()) {
				return nil, false, merr.WrapErrParameterInvalidMsg(
					"op %s is not supported for recursive ARRAY field %q", op.String(), name)
			}
			if fieldSchema.GetDataType() != schemapb.DataType_Array {
				return nil, false, merr.WrapErrParameterInvalidMsg(
					fmt.Sprintf("op %s requires Array field, but field %q is %s",
						op.String(), name, fieldSchema.GetDataType().String()))
			}
		default:
			return nil, false, merr.WrapErrParameterInvalidMsg(
				fmt.Sprintf("unsupported partial update op: %s", op.String()))
		}
		fd, ok := fieldDataByName[name]
		if !ok {
			return nil, false, merr.WrapErrParameterInvalidMsg(
				fmt.Sprintf("partial-update op targets field %q not present in fields_data", name))
		}

		// Reject malformed FieldData early -- a request that declares an
		// Array op but carries no ArrayData would otherwise panic on a nil
		// deref inside the merge path.
		if fd.GetScalars() == nil || fd.GetScalars().GetArrayData() == nil {
			return nil, false, merr.WrapErrParameterInvalidMsg(
				fmt.Sprintf("partial-update op field %q payload is not an Array", name))
		}
		if got := fd.GetScalars().GetArrayData().GetElementType(); got != schemapb.DataType_None && got != fieldSchema.GetElementType() {
			return nil, false, merr.WrapErrParameterInvalidMsg(
				fmt.Sprintf("field %q expects element type %s but request provides %s",
					name, fieldSchema.GetElementType().String(), got.String()))
		}

		if op == schemapb.FieldPartialUpdateOp_ARRAY_APPEND {
			if err := checkArrayAppendPayloadWithinCapacity(fd, fieldSchema); err != nil {
				return nil, false, err
			}
		}
		plans[name] = &fieldPartialUpdatePlan{op: op, arrayParent: fieldSchema}
	}
	return plans, nonReplaceSeen, nil
}

func parsePathReplace(path string) (int, string, bool, error) {
	if path == "" {
		return 0, "", false, merr.WrapErrParameterMissingMsg("path is required")
	}
	if strings.IndexFunc(path, unicode.IsSpace) >= 0 || path[0] != '[' {
		return 0, "", false, merr.WrapErrParameterInvalidMsg("expected canonical [index] or [index][child] syntax")
	}
	indexEnd := strings.IndexByte(path, ']')
	if indexEnd < 2 {
		return 0, "", false, merr.WrapErrParameterInvalidMsg("index segment is missing or empty")
	}
	indexText := path[1:indexEnd]
	if indexText != "0" && indexText[0] == '0' {
		return 0, "", false, merr.WrapErrParameterInvalidMsg("index %q is not canonical", indexText)
	}
	for _, ch := range indexText {
		if ch < '0' || ch > '9' {
			return 0, "", false, merr.WrapErrParameterInvalidMsg("index %q is not a non-negative decimal integer", indexText)
		}
	}
	parsedIndex, err := strconv.ParseInt(indexText, 10, strconv.IntSize)
	if err != nil {
		return 0, "", false, merr.WrapErrParameterInvalidMsg("index %q is out of range", indexText)
	}

	suffix := path[indexEnd+1:]
	if suffix == "" {
		return int(parsedIndex), "", false, nil
	}
	if len(suffix) < 3 || suffix[0] != '[' || suffix[len(suffix)-1] != ']' {
		return 0, "", false, merr.WrapErrParameterInvalidMsg("invalid child segment")
	}
	child := suffix[1 : len(suffix)-1]
	if child == "" || strings.ContainsAny(child, "[]") {
		return 0, "", false, merr.WrapErrParameterInvalidMsg("invalid child segment")
	}
	return int(parsedIndex), child, true, nil
}

func findStructChildSchema(parent *schemapb.StructArrayFieldSchema, childName string) *schemapb.FieldSchema {
	if typeutil.IsStructSubField(childName) {
		if rawName, err := typeutil.ExtractStructFieldName(childName); err == nil {
			childName = rawName
		}
	}
	for _, child := range parent.GetFields() {
		name := child.GetName()
		if typeutil.IsStructSubField(name) {
			rawName, err := typeutil.ExtractStructFieldName(name)
			if err == nil {
				name = rawName
			}
		}
		if name == childName {
			return child
		}
	}
	return nil
}

func structChildRawName(child *schemapb.FieldSchema) string {
	name := child.GetName()
	if typeutil.IsStructSubField(name) {
		if rawName, err := typeutil.ExtractStructFieldName(name); err == nil {
			return rawName
		}
	}
	return name
}

func validatePathReplaceArrayOperand(fd *schemapb.FieldData, schema *schemapb.FieldSchema, rowCount int) error {
	if fd.GetType() != schemapb.DataType_Array || fd.GetScalars().GetArrayData() == nil {
		return merr.WrapErrParameterInvalidMsg("PATH_REPLACE field %q expects Array FieldData", fd.GetFieldName())
	}
	arrayData := fd.GetScalars().GetArrayData()
	if arrayData.GetElementType() != schema.GetElementType() {
		return merr.WrapErrParameterInvalidMsg("field %q expects element type %s but request provides %s", fd.GetFieldName(), schema.GetElementType().String(), arrayData.GetElementType().String())
	}
	if err := validateNonNullOperandRows(fd, rowCount); err != nil {
		return err
	}
	if len(arrayData.GetData()) != rowCount {
		return merr.WrapErrParameterInvalidMsg("PATH_REPLACE field %q has %d operand rows, expected %d", fd.GetFieldName(), len(arrayData.GetData()), rowCount)
	}
	for rowIndex, row := range arrayData.GetData() {
		rowLen, err := scalarArrayRowElementCount(row, schema.GetElementType())
		if err != nil {
			return merr.Wrapf(err, "PATH_REPLACE field %q row %d is invalid", fd.GetFieldName(), rowIndex)
		}
		if rowLen != 1 {
			return merr.WrapErrParameterInvalidMsg("PATH_REPLACE field %q row %d must contain exactly one element", fd.GetFieldName(), rowIndex)
		}
	}
	return nil
}

func validatePathReplaceStructOperand(fd *schemapb.FieldData, plan *fieldPartialUpdatePlan, rowCount int) ([]*schemapb.FieldSchema, error) {
	if fd.GetType() != schemapb.DataType_ArrayOfStruct || fd.GetStructArrays() == nil {
		return nil, merr.WrapErrParameterInvalidMsg("PATH_REPLACE field %q expects ArrayOfStruct FieldData", fd.GetFieldName())
	}
	operandFields := fd.GetStructArrays().GetFields()
	if len(operandFields) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("PATH_REPLACE struct field %q requires at least one child", fd.GetFieldName())
	}

	children := make([]*schemapb.FieldSchema, 0, len(operandFields))
	seenChildren := make(map[string]struct{}, len(operandFields))
	for _, childData := range operandFields {
		childSchema := findStructChildSchema(plan.structParent, childData.GetFieldName())
		if childSchema == nil {
			return nil, merr.WrapErrParameterInvalidMsg("child %q not found in struct field %q", childData.GetFieldName(), fd.GetFieldName())
		}
		childName := structChildRawName(childSchema)
		if _, duplicate := seenChildren[childName]; duplicate {
			return nil, merr.WrapErrParameterInvalidMsg("duplicate child %q in PATH_REPLACE operand for struct field %q", childData.GetFieldName(), fd.GetFieldName())
		}
		seenChildren[childName] = struct{}{}
		if childData.GetType() != childSchema.GetDataType() {
			return nil, merr.WrapErrParameterInvalidMsg("child %q expects type %s but request provides %s", structChildRawName(childSchema), childSchema.GetDataType().String(), childData.GetType().String())
		}
		if err := validateNonNullOperandRows(childData, rowCount); err != nil {
			return nil, err
		}
		if err := validatePathReplaceStructChildRows(childData, childSchema, rowCount); err != nil {
			return nil, err
		}
		children = append(children, childSchema)
	}

	if plan.explicitChild != nil {
		if len(children) != 1 || structChildRawName(children[0]) != structChildRawName(plan.explicitChild) {
			return nil, merr.WrapErrParameterInvalidMsg("PATH_REPLACE field %q path child %q requires exactly that child in the operand", fd.GetFieldName(), structChildRawName(plan.explicitChild))
		}
	}
	return children, nil
}

func validatePathReplaceStructChildRows(fd *schemapb.FieldData, schema *schemapb.FieldSchema, rowCount int) error {
	switch schema.GetDataType() {
	case schemapb.DataType_Array:
		arrayData := fd.GetScalars().GetArrayData()
		if arrayData == nil || arrayData.GetElementType() != schema.GetElementType() {
			return merr.WrapErrParameterInvalidMsg("PATH_REPLACE child %q has incompatible Array payload", fd.GetFieldName())
		}
		if len(arrayData.GetData()) != rowCount {
			return merr.WrapErrParameterInvalidMsg("PATH_REPLACE child %q has %d operand rows, expected %d", fd.GetFieldName(), len(arrayData.GetData()), rowCount)
		}
		for rowIndex, row := range arrayData.GetData() {
			rowLen, err := scalarArrayRowElementCount(row, schema.GetElementType())
			if err != nil {
				return merr.Wrapf(err, "PATH_REPLACE child %q row %d is invalid", fd.GetFieldName(), rowIndex)
			}
			if rowLen != 1 {
				return merr.WrapErrParameterInvalidMsg("PATH_REPLACE child %q row %d must contain exactly one element", fd.GetFieldName(), rowIndex)
			}
		}
	case schemapb.DataType_ArrayOfVector:
		vectors := fd.GetVectors()
		vectorArray := vectors.GetVectorArray()
		if vectorArray == nil || vectorArray.GetElementType() != schema.GetElementType() {
			return merr.WrapErrParameterInvalidMsg("PATH_REPLACE child %q has incompatible ArrayOfVector payload", fd.GetFieldName())
		}
		dim, err := typeutil.GetDim(schema)
		if err != nil || dim <= 0 || vectors.GetDim() != dim || vectorArray.GetDim() != dim {
			return merr.WrapErrParameterInvalidMsg("PATH_REPLACE child %q has incompatible ArrayOfVector dimension", fd.GetFieldName())
		}
		if len(vectorArray.GetData()) != rowCount {
			return merr.WrapErrParameterInvalidMsg("PATH_REPLACE child %q has %d operand rows, expected %d", fd.GetFieldName(), len(vectorArray.GetData()), rowCount)
		}
		for rowIndex, row := range vectorArray.GetData() {
			count, err := vectorArrayRowElementCount(row, schema.GetElementType(), dim)
			if err != nil {
				return merr.Wrapf(err, "PATH_REPLACE child %q row %d is invalid", fd.GetFieldName(), rowIndex)
			}
			if count != 1 {
				return merr.WrapErrParameterInvalidMsg("PATH_REPLACE child %q row %d must contain exactly one vector", fd.GetFieldName(), rowIndex)
			}
		}
	default:
		return merr.WrapErrParameterInvalidMsg("PATH_REPLACE does not support struct child %q of type %s", fd.GetFieldName(), schema.GetDataType().String())
	}
	return nil
}

func validateNonNullOperandRows(fd *schemapb.FieldData, rowCount int) error {
	validData := typeutil.GetFieldDataValidData(fd)
	if len(validData) != 0 && len(validData) != rowCount {
		return merr.WrapErrParameterInvalidMsg("PATH_REPLACE field %q valid_data has length %d, expected %d", fd.GetFieldName(), len(validData), rowCount)
	}
	for rowIndex, valid := range validData {
		if !valid {
			return merr.WrapErrParameterInvalidMsg("PATH_REPLACE field %q operand row %d must not be null", fd.GetFieldName(), rowIndex)
		}
	}
	return nil
}

func updateArrayFieldByColumnWithPathReplace(
	base, update *schemapb.FieldData,
	baseIndices, updateIndices []int64,
	index int,
) error {
	if base == nil || update == nil || len(baseIndices) == 0 {
		return nil
	}
	if len(baseIndices) != len(updateIndices) {
		return merr.WrapErrParameterInvalidMsg(
			"baseIndices and updateIndices length mismatch: %d vs %d",
			len(baseIndices), len(updateIndices))
	}
	if base.GetType() != schemapb.DataType_Array || update.GetType() != schemapb.DataType_Array {
		return merr.WrapErrParameterInvalidMsg("PATH_REPLACE requires Array field data")
	}
	baseArray := base.GetScalars().GetArrayData()
	updateArray := update.GetScalars().GetArrayData()
	if baseArray == nil || updateArray == nil {
		return merr.WrapErrParameterInvalidMsg("PATH_REPLACE requires non-nil ArrayData on both base and update")
	}
	if baseArray.GetElementType() != updateArray.GetElementType() {
		return merr.WrapErrParameterInvalidMsg(
			"PATH_REPLACE element type mismatch: %s vs %s",
			baseArray.GetElementType().String(), updateArray.GetElementType().String())
	}

	baseRows := baseArray.GetData()
	updateRows := updateArray.GetData()
	baseValidData := typeutil.GetFieldDataValidData(base)
	updateValidData := typeutil.GetFieldDataValidData(update)
	for i, baseIndex := range baseIndices {
		updateIndex := updateIndices[i]
		if baseIndex < 0 || int(baseIndex) >= len(baseRows) || updateIndex < 0 || int(updateIndex) >= len(updateRows) {
			return merr.WrapErrParameterInvalidMsg("PATH_REPLACE row index is out of range")
		}
		if len(baseValidData) > 0 && int(baseIndex) >= len(baseValidData) {
			return merr.WrapErrParameterInvalidMsg("PATH_REPLACE base valid_data is shorter than its row mapping")
		}
		if len(updateValidData) > 0 && int(updateIndex) >= len(updateValidData) {
			return merr.WrapErrParameterInvalidMsg("PATH_REPLACE operand valid_data is shorter than its row mapping")
		}
		if len(baseValidData) > 0 && !baseValidData[baseIndex] {
			return merr.WrapErrParameterInvalidMsg("PATH_REPLACE cannot target a null parent Array row")
		}
		if len(updateValidData) > 0 && !updateValidData[updateIndex] {
			return merr.WrapErrParameterInvalidMsg("PATH_REPLACE operand Array row must not be null")
		}
		replaced, err := replaceArrayRowElement(
			baseRows[baseIndex], updateRows[updateIndex], index, baseArray.GetElementType())
		if err != nil {
			return err
		}
		baseRows[baseIndex] = replaced
	}
	return nil
}

func replaceArrayRowElement(
	base, update *schemapb.ScalarField,
	index int,
	elementType schemapb.DataType,
) (*schemapb.ScalarField, error) {
	baseLen, err := scalarArrayRowElementCount(base, elementType)
	if err != nil {
		return nil, err
	}
	if index < 0 || index >= baseLen {
		return nil, merr.WrapErrParameterInvalidMsg(
			"PATH_REPLACE index %d is out of range for Array length %d", index, baseLen)
	}
	updateLen, err := scalarArrayRowElementCount(update, elementType)
	if err != nil {
		return nil, err
	}
	if updateLen != 1 {
		return nil, merr.WrapErrParameterInvalidMsg(
			"PATH_REPLACE operand must contain exactly one Array element")
	}

	out := proto.Clone(base).(*schemapb.ScalarField)
	switch elementType {
	case schemapb.DataType_Bool:
		out.GetBoolData().Data = replaceArrayValue(out.GetBoolData().GetData(), update.GetBoolData().GetData(), index, 1)
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		out.GetIntData().Data = replaceArrayValue(out.GetIntData().GetData(), update.GetIntData().GetData(), index, 1)
	case schemapb.DataType_Int64:
		out.GetLongData().Data = replaceArrayValue(out.GetLongData().GetData(), update.GetLongData().GetData(), index, 1)
	case schemapb.DataType_Float:
		out.GetFloatData().Data = replaceArrayValue(out.GetFloatData().GetData(), update.GetFloatData().GetData(), index, 1)
	case schemapb.DataType_Double:
		out.GetDoubleData().Data = replaceArrayValue(out.GetDoubleData().GetData(), update.GetDoubleData().GetData(), index, 1)
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		out.GetStringData().Data = replaceArrayValue(out.GetStringData().GetData(), update.GetStringData().GetData(), index, 1)
	default:
		return nil, merr.WrapErrParameterInvalidMsg(
			"PATH_REPLACE does not support Array element type %s", elementType.String())
	}
	return out, nil
}

func replaceVectorArrayRowElement(
	base, update *schemapb.VectorField,
	index int,
	elementType schemapb.DataType,
	dim int64,
) (*schemapb.VectorField, error) {
	baseLen, err := vectorArrayRowElementCount(base, elementType, dim)
	if err != nil {
		return nil, err
	}
	if index < 0 || index >= baseLen {
		return nil, merr.WrapErrParameterInvalidMsg(
			"PATH_REPLACE index %d is out of range for ArrayOfVector length %d", index, baseLen)
	}
	updateLen, err := vectorArrayRowElementCount(update, elementType, dim)
	if err != nil {
		return nil, err
	}
	if updateLen != 1 {
		return nil, merr.WrapErrParameterInvalidMsg("PATH_REPLACE operand must contain exactly one vector")
	}

	out := proto.Clone(base).(*schemapb.VectorField)
	switch elementType {
	case schemapb.DataType_FloatVector:
		out.GetFloatVector().Data = replaceArrayValue(
			out.GetFloatVector().GetData(), update.GetFloatVector().GetData(), index, int(dim))
	case schemapb.DataType_BinaryVector:
		out.Data = &schemapb.VectorField_BinaryVector{BinaryVector: replaceArrayValue(
			out.GetBinaryVector(), update.GetBinaryVector(), index, int((dim+7)/8))}
	case schemapb.DataType_Float16Vector:
		out.Data = &schemapb.VectorField_Float16Vector{Float16Vector: replaceArrayValue(
			out.GetFloat16Vector(), update.GetFloat16Vector(), index, int(dim*2))}
	case schemapb.DataType_BFloat16Vector:
		out.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: replaceArrayValue(
			out.GetBfloat16Vector(), update.GetBfloat16Vector(), index, int(dim*2))}
	case schemapb.DataType_Int8Vector:
		out.Data = &schemapb.VectorField_Int8Vector{Int8Vector: replaceArrayValue(
			out.GetInt8Vector(), update.GetInt8Vector(), index, int(dim))}
	default:
		return nil, merr.WrapErrParameterInvalidMsg(
			"PATH_REPLACE does not support ArrayOfVector element type %s", elementType.String())
	}
	return out, nil
}

func replaceArrayValue[T any](base, update []T, index, width int) []T {
	out := slices.Clone(base)
	copy(out[index*width:(index+1)*width], update)
	return out
}

func scalarArrayRowElementCount(row *schemapb.ScalarField, elementType schemapb.DataType) (int, error) {
	if row == nil {
		return 0, merr.WrapErrParameterInvalidMsg("PATH_REPLACE requires a non-nil Array row")
	}
	if len(row.GetValidData()) != 0 {
		return 0, merr.WrapErrParameterInvalidMsg("PATH_REPLACE does not support Array element valid_data")
	}
	switch elementType {
	case schemapb.DataType_Bool:
		if _, ok := row.GetData().(*schemapb.ScalarField_BoolData); !ok || row.GetBoolData() == nil {
			return 0, merr.WrapErrParameterInvalidMsg(
				"Array row payload does not match element type %s", elementType.String())
		}
		return len(row.GetBoolData().GetData()), nil
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		if _, ok := row.GetData().(*schemapb.ScalarField_IntData); !ok || row.GetIntData() == nil {
			return 0, merr.WrapErrParameterInvalidMsg(
				"Array row payload does not match element type %s", elementType.String())
		}
		return len(row.GetIntData().GetData()), nil
	case schemapb.DataType_Int64:
		if _, ok := row.GetData().(*schemapb.ScalarField_LongData); !ok || row.GetLongData() == nil {
			return 0, merr.WrapErrParameterInvalidMsg(
				"Array row payload does not match element type %s", elementType.String())
		}
		return len(row.GetLongData().GetData()), nil
	case schemapb.DataType_Float:
		if _, ok := row.GetData().(*schemapb.ScalarField_FloatData); !ok || row.GetFloatData() == nil {
			return 0, merr.WrapErrParameterInvalidMsg(
				"Array row payload does not match element type %s", elementType.String())
		}
		return len(row.GetFloatData().GetData()), nil
	case schemapb.DataType_Double:
		if _, ok := row.GetData().(*schemapb.ScalarField_DoubleData); !ok || row.GetDoubleData() == nil {
			return 0, merr.WrapErrParameterInvalidMsg(
				"Array row payload does not match element type %s", elementType.String())
		}
		return len(row.GetDoubleData().GetData()), nil
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		if _, ok := row.GetData().(*schemapb.ScalarField_StringData); !ok || row.GetStringData() == nil {
			return 0, merr.WrapErrParameterInvalidMsg(
				"Array row payload does not match element type %s", elementType.String())
		}
		return len(row.GetStringData().GetData()), nil
	default:
		return 0, merr.WrapErrParameterInvalidMsg(
			"PATH_REPLACE does not support Array element type %s", elementType.String())
	}
}

func vectorArrayRowElementCount(row *schemapb.VectorField, elementType schemapb.DataType, dim int64) (int, error) {
	if row == nil || dim <= 0 {
		return 0, merr.WrapErrParameterInvalidMsg(
			"PATH_REPLACE requires a non-nil vector row and a positive dimension")
	}
	if len(row.GetValidData()) != 0 {
		return 0, merr.WrapErrParameterInvalidMsg(
			"PATH_REPLACE does not support ArrayOfVector element valid_data")
	}
	var payloadLen, width int
	switch elementType {
	case schemapb.DataType_FloatVector:
		if _, ok := row.GetData().(*schemapb.VectorField_FloatVector); !ok || row.GetFloatVector() == nil {
			return 0, merr.WrapErrParameterInvalidMsg(
				"ArrayOfVector row payload does not match element type %s", elementType.String())
		}
		payloadLen, width = len(row.GetFloatVector().GetData()), int(dim)
	case schemapb.DataType_BinaryVector:
		if _, ok := row.GetData().(*schemapb.VectorField_BinaryVector); !ok {
			return 0, merr.WrapErrParameterInvalidMsg(
				"ArrayOfVector row payload does not match element type %s", elementType.String())
		}
		payloadLen, width = len(row.GetBinaryVector()), int((dim+7)/8)
	case schemapb.DataType_Float16Vector:
		if _, ok := row.GetData().(*schemapb.VectorField_Float16Vector); !ok {
			return 0, merr.WrapErrParameterInvalidMsg(
				"ArrayOfVector row payload does not match element type %s", elementType.String())
		}
		payloadLen, width = len(row.GetFloat16Vector()), int(dim*2)
	case schemapb.DataType_BFloat16Vector:
		if _, ok := row.GetData().(*schemapb.VectorField_Bfloat16Vector); !ok {
			return 0, merr.WrapErrParameterInvalidMsg(
				"ArrayOfVector row payload does not match element type %s", elementType.String())
		}
		payloadLen, width = len(row.GetBfloat16Vector()), int(dim*2)
	case schemapb.DataType_Int8Vector:
		if _, ok := row.GetData().(*schemapb.VectorField_Int8Vector); !ok {
			return 0, merr.WrapErrParameterInvalidMsg(
				"ArrayOfVector row payload does not match element type %s", elementType.String())
		}
		payloadLen, width = len(row.GetInt8Vector()), int(dim)
	default:
		return 0, merr.WrapErrParameterInvalidMsg(
			"PATH_REPLACE does not support ArrayOfVector element type %s", elementType.String())
	}
	if width <= 0 || payloadLen%width != 0 {
		return 0, merr.WrapErrParameterInvalidMsg(
			"vector payload length %d is not divisible by element width %d", payloadLen, width)
	}
	return payloadLen / width, nil
}

func hasPathReplacePlan(plans map[string]*fieldPartialUpdatePlan) bool {
	for _, plan := range plans {
		if plan.isPathReplace() {
			return true
		}
	}
	return false
}

func validateExistingArrayPathRows(field *schemapb.FieldData, dataIndices, rowIndices []int64, plan *fieldPartialUpdatePlan) error {
	if field.GetType() != schemapb.DataType_Array || field.GetScalars().GetArrayData() == nil {
		return merr.WrapErrServiceInternalMsg("retrieved field %q is not valid Array data", field.GetFieldName())
	}
	if len(dataIndices) != len(rowIndices) {
		return merr.WrapErrServiceInternalMsg("retrieved field %q has inconsistent row mappings", field.GetFieldName())
	}
	arrayData := field.GetScalars().GetArrayData()
	if arrayData.GetElementType() != plan.arrayParent.GetElementType() {
		return merr.WrapErrServiceInternalMsg("retrieved field %q has element type %s, expected %s", field.GetFieldName(), arrayData.GetElementType().String(), plan.arrayParent.GetElementType().String())
	}
	parentValidData := typeutil.GetFieldDataValidData(field)
	for i, dataIndex := range dataIndices {
		rowIndex := rowIndices[i]
		if len(parentValidData) > 0 {
			if rowIndex < 0 || int(rowIndex) >= len(parentValidData) {
				return merr.WrapErrServiceInternalMsg("retrieved field %q has malformed parent valid_data", field.GetFieldName())
			}
			if !parentValidData[rowIndex] {
				return merr.WrapErrParameterInvalidMsg(
					"PATH_REPLACE cannot target null parent row %d of field %q", i, field.GetFieldName())
			}
		}
		if dataIndex < 0 || int(dataIndex) >= len(arrayData.GetData()) {
			return merr.WrapErrServiceInternalMsg("retrieved field %q is missing row data", field.GetFieldName())
		}
		row := arrayData.GetData()[dataIndex]
		if row == nil {
			return merr.WrapErrServiceInternalMsg("retrieved field %q contains a nil Array row", field.GetFieldName())
		}
		rowLen, err := scalarArrayRowElementCount(row, plan.arrayParent.GetElementType())
		if err != nil {
			return merr.WrapErrServiceInternalErr(err, "retrieved field %q row payload is malformed", field.GetFieldName())
		}
		if plan.index >= rowLen {
			return merr.WrapErrParameterInvalidMsg(
				"PATH_REPLACE index %d is out of range for field %q row %d with length %d", plan.index, field.GetFieldName(), i, rowLen)
		}
	}
	return nil
}

func validateExistingStructPathRows(field *schemapb.FieldData, plan *fieldPartialUpdatePlan, rowIndices []int64) error {
	if field.GetType() != schemapb.DataType_ArrayOfStruct || field.GetStructArrays() == nil {
		return merr.WrapErrServiceInternalMsg("retrieved field %q is not valid ArrayOfStruct data", field.GetFieldName())
	}
	retrievedChildren := field.GetStructArrays().GetFields()
	if len(retrievedChildren) != len(plan.structParent.GetFields()) {
		return merr.WrapErrServiceInternalMsg("retrieved struct field %q has %d children, expected %d", field.GetFieldName(), len(retrievedChildren), len(plan.structParent.GetFields()))
	}
	seenChildren := make(map[string]struct{}, len(retrievedChildren))
	for _, childData := range retrievedChildren {
		childSchema := findStructChildSchema(plan.structParent, childData.GetFieldName())
		if childSchema == nil {
			return merr.WrapErrServiceInternalMsg("retrieved struct field %q contains unknown child %q", field.GetFieldName(), childData.GetFieldName())
		}
		childName := structChildRawName(childSchema)
		if _, duplicate := seenChildren[childName]; duplicate {
			return merr.WrapErrServiceInternalMsg("retrieved struct field %q contains duplicate child %q", field.GetFieldName(), childName)
		}
		seenChildren[childName] = struct{}{}
	}

	children := make([]*schemapb.FieldData, 0, len(plan.structParent.GetFields()))
	for _, childSchema := range plan.structParent.GetFields() {
		childData := findStructChildFieldData(field.GetStructArrays().GetFields(), childSchema)
		if childData == nil {
			return merr.WrapErrServiceInternalMsg("retrieved struct field %q is missing child %q", field.GetFieldName(), structChildRawName(childSchema))
		}
		children = append(children, childData)
	}

	for requestRow, rowIndex := range rowIndices {
		expectedLen := -1
		parentValiditySet := false
		parentValid := true
		for childIndex, childData := range children {
			childSchema := plan.structParent.GetFields()[childIndex]
			validData := typeutil.GetFieldDataValidData(childData)
			currentParentValid := true
			if len(validData) > 0 {
				if rowIndex < 0 || int(rowIndex) >= len(validData) {
					return merr.WrapErrServiceInternalMsg("retrieved struct child %q has malformed parent valid_data", childData.GetFieldName())
				}
				currentParentValid = validData[rowIndex]
			}
			if parentValiditySet && currentParentValid != parentValid {
				return merr.WrapErrServiceInternalMsg("retrieved struct field %q has inconsistent child parent validity", field.GetFieldName())
			}
			parentValid = currentParentValid
			parentValiditySet = true
			if !parentValid {
				continue
			}

			rowLen, err := structChildRowLen(childData, childSchema, int(rowIndex))
			if err != nil {
				return merr.Wrapf(err, "retrieved struct child %q is malformed", childData.GetFieldName())
			}
			if expectedLen == -1 {
				expectedLen = rowLen
			} else if rowLen != expectedLen {
				return merr.WrapErrServiceInternalMsg("retrieved struct field %q has unaligned child lengths at row %d", field.GetFieldName(), requestRow)
			}
		}
		if !parentValid {
			return merr.WrapErrParameterInvalidMsg(
				"PATH_REPLACE cannot target null parent row %d of field %q", requestRow, field.GetFieldName())
		}
		if plan.index >= expectedLen {
			return merr.WrapErrParameterInvalidMsg(
				"PATH_REPLACE index %d is out of range for field %q row %d with length %d", plan.index, field.GetFieldName(), requestRow, expectedLen)
		}
	}
	return nil
}

func structChildRowLen(field *schemapb.FieldData, schema *schemapb.FieldSchema, rowIndex int) (int, error) {
	switch schema.GetDataType() {
	case schemapb.DataType_Array:
		if field.GetType() != schemapb.DataType_Array {
			return 0, merr.WrapErrServiceInternalMsg("expected Array child data, got %s", field.GetType().String())
		}
		arrayData := field.GetScalars().GetArrayData()
		if arrayData == nil || arrayData.GetElementType() != schema.GetElementType() || rowIndex < 0 || rowIndex >= len(arrayData.GetData()) {
			return 0, merr.WrapErrServiceInternalMsg("missing Array row %d", rowIndex)
		}
		row := arrayData.GetData()[rowIndex]
		if typeutil.IsNestedArrayTypeSchema(schema.GetTypeSchema()) {
			if row.GetData() == nil || row.GetArrayData() == nil {
				return 0, merr.WrapErrServiceInternalMsg("malformed nested Array row %d", rowIndex)
			}
			return len(row.GetArrayData().GetData()), nil
		}
		rowLen, err := scalarArrayRowElementCount(row, schema.GetElementType())
		if err != nil {
			return 0, merr.WrapErrServiceInternalErr(err, "malformed Array row %d", rowIndex)
		}
		return rowLen, nil
	case schemapb.DataType_ArrayOfVector:
		if field.GetType() != schemapb.DataType_ArrayOfVector {
			return 0, merr.WrapErrServiceInternalMsg("expected ArrayOfVector child data, got %s", field.GetType().String())
		}
		vectorArray := field.GetVectors().GetVectorArray()
		if vectorArray == nil || vectorArray.GetElementType() != schema.GetElementType() || rowIndex < 0 || rowIndex >= len(vectorArray.GetData()) {
			return 0, merr.WrapErrServiceInternalMsg("missing ArrayOfVector row %d", rowIndex)
		}
		dim, err := typeutil.GetDim(schema)
		if err != nil || dim <= 0 || field.GetVectors().GetDim() != dim || vectorArray.GetDim() != dim {
			return 0, merr.WrapErrServiceInternalMsg("ArrayOfVector row %d has incompatible dimension metadata", rowIndex)
		}
		row := vectorArray.GetData()[rowIndex]
		rowLen, err := vectorArrayRowElementCount(row, schema.GetElementType(), dim)
		if err != nil {
			return 0, merr.WrapErrServiceInternalErr(err, "malformed ArrayOfVector row %d", rowIndex)
		}
		return rowLen, nil
	default:
		return 0, merr.WrapErrServiceInternalMsg("unsupported child type %s", schema.GetDataType().String())
	}
}

func applyStructPathReplace(dst, operand *schemapb.FieldData, plan *fieldPartialUpdatePlan, dstIndices, operandIndices []int64) error {
	if len(dstIndices) != len(operandIndices) {
		return merr.WrapErrServiceInternalMsg("PATH_REPLACE struct row mappings are inconsistent")
	}
	for _, childSchema := range plan.operandChildren {
		dstChild := findStructChildFieldData(dst.GetStructArrays().GetFields(), childSchema)
		operandChild := findStructChildFieldData(operand.GetStructArrays().GetFields(), childSchema)
		if dstChild == nil || operandChild == nil {
			return merr.WrapErrServiceInternalMsg("PATH_REPLACE struct child %q disappeared after validation", structChildRawName(childSchema))
		}
		for i, dstIndex := range dstIndices {
			operandIndex := operandIndices[i]
			switch childSchema.GetDataType() {
			case schemapb.DataType_Array:
				dstRows := dstChild.GetScalars().GetArrayData().GetData()
				operandRows := operandChild.GetScalars().GetArrayData().GetData()
				replaced, err := replaceArrayRowElement(dstRows[dstIndex], operandRows[operandIndex], plan.index, childSchema.GetElementType())
				if err != nil {
					return merr.WrapErrServiceInternalErr(err, "failed to materialize PATH_REPLACE child %q", structChildRawName(childSchema))
				}
				dstRows[dstIndex] = replaced
			case schemapb.DataType_ArrayOfVector:
				dim, err := typeutil.GetDim(childSchema)
				if err != nil {
					return merr.WrapErrServiceInternalErr(err, "failed to resolve dimension for PATH_REPLACE child %q", structChildRawName(childSchema))
				}
				dstRows := dstChild.GetVectors().GetVectorArray().GetData()
				operandRows := operandChild.GetVectors().GetVectorArray().GetData()
				replaced, err := replaceVectorArrayRowElement(dstRows[dstIndex], operandRows[operandIndex], plan.index, childSchema.GetElementType(), dim)
				if err != nil {
					return merr.WrapErrServiceInternalErr(err, "failed to materialize PATH_REPLACE child %q", structChildRawName(childSchema))
				}
				dstRows[dstIndex] = replaced
			default:
				return merr.WrapErrServiceInternalMsg("unsupported PATH_REPLACE child type %s", childSchema.GetDataType().String())
			}
		}
	}
	return nil
}

func findStructChildFieldData(fields []*schemapb.FieldData, childSchema *schemapb.FieldSchema) *schemapb.FieldData {
	rawName := structChildRawName(childSchema)
	for _, field := range fields {
		if field.GetFieldId() != 0 && childSchema.GetFieldID() != 0 && field.GetFieldId() == childSchema.GetFieldID() {
			return field
		}
		name := field.GetFieldName()
		if typeutil.IsStructSubField(name) {
			if extracted, err := typeutil.ExtractStructFieldName(name); err == nil {
				name = extracted
			}
		}
		if name == rawName {
			return field
		}
	}
	return nil
}

// findFieldSchemaByName locates a field by name in the given collection
// schema. Returns a descriptive parameter-invalid error when not found.
func findFieldSchemaByName(schema *schemapb.CollectionSchema, name string) (*schemapb.FieldSchema, error) {
	for _, f := range schema.GetFields() {
		if f.GetName() == name {
			return f, nil
		}
	}
	return nil, merr.WrapErrParameterInvalidMsg("field %q not found in collection schema", name)
}

// checkArrayAppendPayloadWithinCapacity checks that the per-row payload
// length itself does not already exceed max_capacity. The final merged
// length is enforced at merge time by ApplyArrayRowOp.
//
// When max_capacity is not declared or is non-positive, the check is a
// no-op (matches the behavior of legacy upserts with no capacity gate).
func checkArrayAppendPayloadWithinCapacity(fd *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) error {
	maxCap := readMaxCapacity(fieldSchema)
	if maxCap < 0 {
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
// -1 when missing / malformed. -1 is a sentinel understood by both the
// proxy pre-check and typeutil.ApplyArrayRowOp as "no capacity gate",
// keeping the two sides consistent.
func readMaxCapacity(fieldSchema *schemapb.FieldSchema) int {
	for _, kv := range fieldSchema.GetTypeParams() {
		if kv.GetKey() != common.MaxCapacityKey {
			continue
		}
		v, err := strconv.Atoi(kv.GetValue())
		if err != nil {
			return -1
		}
		return v
	}
	return -1
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

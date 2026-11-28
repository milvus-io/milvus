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

package dml

import (
	"math"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/fieldvalidator"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/parameterutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func validateUpdatePKs(ids *schemapb.IDs, schema *schemapb.CollectionSchema) error {
	// Count the original request, including duplicate PKs, before routing or
	// any future deduplication. Byte limits remain independently enforced.
	count := typeutil.GetSizeOfIDs(ids)
	limit := paramtable.Get().ProxyCfg.PKUpdateMaxPKCount.GetAsInt()
	if count > limit {
		return merr.Wrapf(merr.ErrParameterTooLarge, "update primary key count %d exceeds limit %d", count, limit)
	}
	pk, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return err
	}
	switch pk.GetDataType() {
	case schemapb.DataType_Int64:
		if len(ids.GetIntId().GetData()) == 0 {
			return merr.WrapErrParameterInvalidMsg("update requires nonempty INT64 primary keys")
		}
	case schemapb.DataType_VarChar:
		keys := ids.GetStrId().GetData()
		if len(keys) == 0 {
			return merr.WrapErrParameterInvalidMsg("update requires nonempty VARCHAR primary keys")
		}
		maxLength, err := parameterutil.GetMaxLength(pk)
		if err != nil {
			return err
		}
		for _, key := range keys {
			if !utf8.ValidString(key) || int64(len(key)) > maxLength {
				return merr.WrapErrParameterInvalidMsg("invalid update primary key: invalid UTF-8 or max_length exceeded")
			}
		}
	default:
		return merr.WrapErrServiceInternalMsg("unsupported primary key schema type %s", pk.GetDataType())
	}
	return nil
}

// Convert only the supplied literal to the existing one-row validation shape.
// No defaults, omitted columns or PK expansion are materialized here.
func updateLiteralScalars(field *schemapb.FieldSchema, literal *schemapb.UpdateLiteral) (*schemapb.ScalarField, error) {
	badType := func() (*schemapb.ScalarField, error) {
		return nil, merr.WrapErrParameterInvalidMsg("update literal does not match field %s (%s)", field.GetName(), field.GetDataType())
	}
	if field.GetDataType() == schemapb.DataType_Array {
		arr := literal.GetArrayValue()
		if arr == nil || arr.GetData() == nil {
			return badType()
		}
		for _, v := range arr.GetFloatData().GetData() {
			if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
				return nil, merr.WrapErrParameterInvalidMsg("update array %s contains a non-finite value", field.GetName())
			}
		}
		for _, v := range arr.GetDoubleData().GetData() {
			if math.IsNaN(v) || math.IsInf(v, 0) {
				return nil, merr.WrapErrParameterInvalidMsg("update array %s contains a non-finite value", field.GetName())
			}
		}
		for _, v := range arr.GetStringData().GetData() {
			if !utf8.ValidString(v) {
				return nil, merr.WrapErrParameterInvalidMsg("update array %s contains invalid UTF-8", field.GetName())
			}
		}
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
			ElementType: field.GetElementType(), Data: []*schemapb.ScalarField{arr},
		}}}, nil
	}
	v := literal.GetScalarValue()
	if v == nil {
		return badType()
	}
	s := &schemapb.ScalarField{}
	switch x := v.GetData().(type) {
	case *schemapb.ValueField_BoolData:
		if field.GetDataType() != schemapb.DataType_Bool {
			return badType()
		}
		s.Data = &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{x.BoolData}}}
	case *schemapb.ValueField_IntData:
		if field.GetDataType() != schemapb.DataType_Int8 && field.GetDataType() != schemapb.DataType_Int16 && field.GetDataType() != schemapb.DataType_Int32 {
			return badType()
		}
		s.Data = &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{x.IntData}}}
	case *schemapb.ValueField_LongData:
		if field.GetDataType() != schemapb.DataType_Int64 {
			return badType()
		}
		s.Data = &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{x.LongData}}}
	case *schemapb.ValueField_StringData:
		if field.GetDataType() != schemapb.DataType_VarChar || !utf8.ValidString(x.StringData) {
			return badType()
		}
		s.Data = &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{x.StringData}}}
	case *schemapb.ValueField_TimestamptzData:
		if field.GetDataType() != schemapb.DataType_Timestamptz {
			return badType()
		}
		s.Data = &schemapb.ScalarField_TimestamptzData{TimestamptzData: &schemapb.TimestamptzArray{Data: []int64{x.TimestamptzData}}}
	default:
		return badType()
	}
	return s, nil
}

func validateUpdateAssignments(schema *schemapb.CollectionSchema, assignments []*milvuspb.UpdateAssignment) ([]*messagespb.UpdateAssignment, error) {
	if len(assignments) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("update requires at least one assignment")
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		return nil, err
	}
	restricted := typeutil.PKUpdateFieldRestriction(schema)
	seen := map[int64]bool{}
	result := make([]*messagespb.UpdateAssignment, 0, len(assignments))
	validator := fieldvalidator.NewValidateUtil(fieldvalidator.WithNANCheck(), fieldvalidator.WithMaxLenCheck(), fieldvalidator.WithOverflowCheck(), fieldvalidator.WithMaxCapCheck())
	for _, a := range assignments {
		if a == nil || a.GetFieldName() == "" || a.GetLiteral().GetValue() == nil {
			return nil, merr.WrapErrParameterInvalidMsg("assignment requires a field name and an explicit literal")
		}
		field, err := helper.GetFieldFromName(a.GetFieldName())
		if err != nil {
			return nil, err
		}
		if seen[field.GetFieldID()] {
			return nil, merr.WrapErrParameterInvalidMsg("duplicate update assignment for field %s", a.GetFieldName())
		}
		seen[field.GetFieldID()] = true
		if restricted(field) {
			return nil, merr.WrapErrParameterInvalidMsg("field %s cannot be updated", field.GetName())
		}
		if !typeutil.SupportsPKUpdateType(field) {
			return nil, merr.WrapErrParameterInvalidMsg("field %s has unsupported update type %s", field.GetName(), field.GetDataType())
		}
		literal := proto.Clone(a.GetLiteral()).(*schemapb.UpdateLiteral)
		if n, ok := literal.GetValue().(*schemapb.UpdateLiteral_SetNull); ok {
			if !n.SetNull {
				return nil, merr.WrapErrParameterInvalidMsg("update set_null must be true for field %s", field.GetName())
			}
			if value := fieldvalidator.ScalarDefaultValue(field); value != nil {
				literal.Value = &schemapb.UpdateLiteral_ScalarValue{ScalarValue: proto.Clone(value).(*schemapb.ValueField)}
			} else if !field.GetNullable() {
				return nil, merr.WrapErrParameterInvalidMsg("field %s does not accept this NULL assignment", field.GetName())
			}
		} else {
			scalars, err := updateLiteralScalars(field, literal)
			if err != nil {
				return nil, err
			}
			fd := &schemapb.FieldData{FieldName: field.GetName(), FieldId: field.GetFieldID(), Type: field.GetDataType(), Field: &schemapb.FieldData_Scalars{Scalars: scalars}}
			if field.GetNullable() {
				typeutil.SetFieldDataValidData(fd, []bool{true})
			}
			// UpdateLiteral carries already typed UTC microseconds, unlike the
			// insert API's timestamp string conversion.
			if field.GetDataType() != schemapb.DataType_Timestamptz {
				if err := validator.Validate([]*schemapb.FieldData{fd}, helper, 1); err != nil {
					return nil, err
				}
			}
		}
		result = append(result, &messagespb.UpdateAssignment{FieldId: field.GetFieldID(), Literal: literal})
	}
	return result, nil
}

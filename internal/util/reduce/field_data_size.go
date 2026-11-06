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

package reduce

import (
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func bytesFieldSize(field protowire.Number, size int) int {
	return protowire.SizeTag(field) + protowire.SizeBytes(size)
}

func intFieldSize(field protowire.Number, value int64) int {
	if value == 0 {
		return 0
	}
	return protowire.SizeTag(field) + protowire.SizeVarint(uint64(value))
}

// IDsRowSize returns the encoded IDs message size for one row, without its
// enclosing SearchResults/RetrieveResults field tag. A nil IDs message is absent.
func IDsRowSize(ids *schemapb.IDs, row int64) (int, error) {
	switch data := ids.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		n, err := scalarRowSize(data.IntId.GetData(), row, false, func(v int64) int { return protowire.SizeVarint(uint64(v)) })
		return bytesFieldSize(1, n), err
	case *schemapb.IDs_StrId:
		n, err := scalarRowSize(data.StrId.GetData(), row, false, func(v string) int { return len(v) })
		return bytesFieldSize(2, n), err
	case nil:
		return 0, nil
	default:
		return 0, merr.WrapErrServiceInternalMsg("unsupported IDs representation %T", data)
	}
}

// scalarRowSize includes the array's data tag and length prefix. Missing null
// scalar values become zero placeholders, as in Query's result materializer.
func scalarRowSize[T any](data []T, row int64, null bool, size func(T) int) (int, error) {
	var value T
	if row >= 0 && row < int64(len(data)) {
		value = data[row]
	} else if !null {
		return 0, merr.WrapErrServiceInternalMsg("Unit row %d is outside field data length %d", row, len(data))
	}
	return bytesFieldSize(1, size(value)), nil
}

// FieldDataRowSize sizes a one-row FieldData with the retained descriptors and
// dual-written row validity. dataRow is the compact vector index, or row otherwise.
// search selects Search's vector metadata rules rather than Query's. Nested
// array values already are protobuf messages and are sized in place.
func FieldDataRowSize(field *schemapb.FieldData, row, dataRow int64, search bool) (int, error) {
	return fieldDataRowSize(field, row, dataRow, false, search)
}

// GroupByFieldRowSize matches WriteGroupByFieldValues: a one-row group field
// carries validity only when null, with its scalar value zero-filled.
func GroupByFieldRowSize(field *schemapb.FieldData, row int64) (int, error) {
	dataRow := row
	valid := typeutil.GetFieldDataValidData(field)
	if row >= 0 && row < int64(len(valid)) && !valid[row] {
		dataRow = -1
	}
	return fieldDataRowSize(field, row, dataRow, true, true)
}

func fieldDataRowSize(field *schemapb.FieldData, row, dataRow int64, group, search bool) (int, error) {
	if field == nil || row < 0 {
		return 0, merr.WrapErrServiceInternalMsg("invalid Unit field or row %d", row)
	}
	validity := typeutil.GetFieldDataValidData(field)
	if len(validity) > 0 && row >= int64(len(validity)) {
		return 0, merr.WrapErrServiceInternalMsg("Unit row %d is outside validity length %d", row, len(validity))
	}
	null := dataRow < 0 || (len(validity) > 0 && !validity[row])
	size := intFieldSize(1, int64(field.GetType())) + intFieldSize(5, field.GetFieldId())
	if name := field.GetFieldName(); name != "" {
		size += bytesFieldSize(2, len(name))
	}
	if field.GetIsDynamic() && !group {
		size += 2
	}
	var payload int
	var err error
	switch data := field.GetField().(type) {
	case *schemapb.FieldData_Scalars:
		payload, err = scalarFieldRowSize(data.Scalars, dataRow, null)
		if (!group && len(validity) > 0) || (group && null) {
			payload += bytesFieldSize(17, 1)
			size += bytesFieldSize(6, 1)
		}
		size += bytesFieldSize(3, payload)
	case *schemapb.FieldData_Vectors:
		payload, err = vectorFieldRowSize(data.Vectors, dataRow, null, search)
		if len(validity) > 0 || dataRow < 0 {
			payload += bytesFieldSize(9, 1)
			size += bytesFieldSize(6, 1)
		}
		size += bytesFieldSize(4, payload)
	case *schemapb.FieldData_StructArrays:
		for _, child := range data.StructArrays.GetFields() {
			var childSize int
			childSize, err = FieldDataRowSize(child, row, row, search)
			if err != nil {
				return 0, err
			}
			payload += bytesFieldSize(1, childSize)
		}
		size += bytesFieldSize(8, payload)
	default:
		return 0, merr.WrapErrServiceInternalMsg("unsupported Unit field representation %T", data)
	}
	return size, err
}

func scalarFieldRowSize(field *schemapb.ScalarField, row int64, null bool) (int, error) {
	var tag protowire.Number
	var size int
	var err error
	int32Size := func(v int32) int { return protowire.SizeVarint(uint64(v)) }
	int64Size := func(v int64) int { return protowire.SizeVarint(uint64(v)) }
	stringSize := func(v string) int { return len(v) }
	bytesSize := func(v []byte) int { return len(v) }
	switch data := field.GetData().(type) {
	case *schemapb.ScalarField_BoolData:
		tag = 1
		size, err = scalarRowSize(data.BoolData.GetData(), row, null, func(bool) int { return 1 })
	case *schemapb.ScalarField_IntData:
		tag = 2
		size, err = scalarRowSize(data.IntData.GetData(), row, null, int32Size)
	case *schemapb.ScalarField_LongData:
		tag = 3
		size, err = scalarRowSize(data.LongData.GetData(), row, null, int64Size)
	case *schemapb.ScalarField_FloatData:
		tag = 4
		size, err = scalarRowSize(data.FloatData.GetData(), row, null, func(float32) int { return 4 })
	case *schemapb.ScalarField_DoubleData:
		tag = 5
		size, err = scalarRowSize(data.DoubleData.GetData(), row, null, func(float64) int { return 8 })
	case *schemapb.ScalarField_StringData:
		tag = 6
		size, err = scalarRowSize(data.StringData.GetData(), row, null, stringSize)
	case *schemapb.ScalarField_BytesData:
		tag = 7
		size, err = scalarRowSize(data.BytesData.GetData(), row, null, bytesSize)
	case *schemapb.ScalarField_ArrayData:
		tag = 8
		size, err = scalarRowSize(data.ArrayData.GetData(), row, null, func(v *schemapb.ScalarField) int { return proto.Size(v) })
		size += intFieldSize(2, int64(data.ArrayData.GetElementType()))
	case *schemapb.ScalarField_JsonData:
		tag = 9
		size, err = scalarRowSize(data.JsonData.GetData(), row, null, bytesSize)
	case *schemapb.ScalarField_GeometryData:
		tag = 10
		size, err = scalarRowSize(data.GeometryData.GetData(), row, null, bytesSize)
	case *schemapb.ScalarField_TimestamptzData:
		tag = 11
		size, err = scalarRowSize(data.TimestamptzData.GetData(), row, null, int64Size)
	case *schemapb.ScalarField_GeometryWktData:
		tag = 12
		size, err = scalarRowSize(data.GeometryWktData.GetData(), row, null, stringSize)
	case *schemapb.ScalarField_MolData:
		tag = 13
		size, err = scalarRowSize(data.MolData.GetData(), row, null, bytesSize)
	case *schemapb.ScalarField_MolSmilesData:
		tag = 14
		size, err = scalarRowSize(data.MolSmilesData.GetData(), row, null, stringSize)
	case *schemapb.ScalarField_DateData:
		tag = 15
		size, err = scalarRowSize(data.DateData.GetData(), row, null, int32Size)
	case *schemapb.ScalarField_TimeData:
		tag = 16
		size, err = scalarRowSize(data.TimeData.GetData(), row, null, int64Size)
	case nil:
		return 0, nil
	default:
		return 0, merr.WrapErrServiceInternalMsg("unsupported Unit scalar representation %T", data)
	}
	return bytesFieldSize(tag, size), err
}

func vectorFieldRowSize(field *schemapb.VectorField, row int64, null, search bool) (int, error) {
	dim := field.GetDim()
	size := intFieldSize(1, dim)
	var tag protowire.Number
	var width, length int64
	switch data := field.GetData().(type) {
	case *schemapb.VectorField_FloatVector:
		tag, width, length = 2, dim, int64(len(data.FloatVector.GetData()))
	case *schemapb.VectorField_BinaryVector:
		tag, width, length = 3, dim/8, int64(len(data.BinaryVector))
	case *schemapb.VectorField_Float16Vector:
		tag, width, length = 4, dim*2, int64(len(data.Float16Vector))
	case *schemapb.VectorField_Bfloat16Vector:
		tag, width, length = 5, dim*2, int64(len(data.Bfloat16Vector))
	case *schemapb.VectorField_Int8Vector:
		tag, width, length = 7, dim, int64(len(data.Int8Vector))
	case *schemapb.VectorField_SparseFloatVector:
		// Query retains the source sparse dimension; Search derives it from
		// the selected row and leaves the enclosing vector dimension unset.
		sparseDim, payload := int64(0), 0
		if !null {
			n, err := scalarRowSize(data.SparseFloatVector.GetContents(), row, false, func(v []byte) int { return len(v) })
			if err != nil {
				return 0, err
			}
			payload = n
			sparseDim = data.SparseFloatVector.GetDim()
			if search {
				sparseDim = typeutil.SparseFloatRowDim(data.SparseFloatVector.Contents[row])
			}
		}
		size = 0
		if !search {
			size = intFieldSize(1, sparseDim)
		}
		payload += intFieldSize(2, sparseDim)
		return size + bytesFieldSize(6, payload), nil
	case *schemapb.VectorField_VectorArray:
		array := data.VectorArray
		arrayDim := array.GetDim()
		if !search && dim != 0 {
			arrayDim = dim
		}
		payload := intFieldSize(1, arrayDim) + intFieldSize(3, int64(array.GetElementType()))
		if !search && null {
			// Query emits a typed empty VectorField for an absent null array row.
			switch array.GetElementType() {
			case schemapb.DataType_FloatVector, schemapb.DataType_BinaryVector,
				schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector, schemapb.DataType_Int8Vector:
				return size + bytesFieldSize(8, payload+bytesFieldSize(2, intFieldSize(1, arrayDim)+2)), nil
			default:
				return 0, merr.WrapErrServiceInternalMsg("unsupported null vector-array element type %s", array.GetElementType())
			}
		}
		if row < 0 || row >= int64(len(array.GetData())) {
			return 0, merr.WrapErrServiceInternalMsg("Unit vector-array row %d is out of range", row)
		}
		return size + bytesFieldSize(8, payload+bytesFieldSize(2, proto.Size(array.Data[row]))), nil
	case nil:
		return size, nil
	default:
		return 0, merr.WrapErrServiceInternalMsg("unsupported Unit vector representation %T", data)
	}
	if dim < 0 || (!null && (row < 0 || width < 0 || width > length || row >= length/max(1, width))) {
		return 0, merr.WrapErrServiceInternalMsg("Unit vector row %d exceeds payload length %d (dim %d)", row, length, dim)
	}
	payload := int(width)
	if null {
		payload = 0
	}
	if tag == 2 {
		if payload > 0 {
			payload = bytesFieldSize(1, payload*4)
		}
	}
	return size + bytesFieldSize(tag, payload), nil
}

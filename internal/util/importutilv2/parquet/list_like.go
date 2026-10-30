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

package parquet

import (
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"golang.org/x/exp/constraints"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type listLikeArray struct {
	rows      int
	values    arrow.Array
	isNull    func(int) bool
	rangeAt   func(int) (int64, int64)
	fixedSize int32
}

func newListLikeArray(chunk arrow.Array, field *schemapb.FieldSchema) (*listLikeArray, error) {
	switch list := chunk.(type) {
	case *array.List:
		return &listLikeArray{
			rows:   list.Len(),
			values: list.ListValues(),
			isNull: list.IsNull,
			rangeAt: func(i int) (int64, int64) {
				start, end := list.ValueOffsets(i)
				return start, end
			},
			fixedSize: -1,
		}, nil
	case *array.FixedSizeList:
		fixedSize := list.DataType().(*arrow.FixedSizeListType).Len()
		return &listLikeArray{
			rows:      list.Len(),
			values:    list.ListValues(),
			isNull:    list.IsNull,
			rangeAt:   list.ValueOffsets,
			fixedSize: fixedSize,
		}, nil
	default:
		return nil, WrapTypeErr(field, chunk.DataType().Name())
	}
}

func (l *listLikeArray) Len() int {
	return l.rows
}

func (l *listLikeArray) IsNull(i int) bool {
	return l.isNull(i)
}

func (l *listLikeArray) ListValues() arrow.Array {
	return l.values
}

func (l *listLikeArray) ValueOffsets(i int) (int64, int64) {
	return l.rangeAt(i)
}

func (l *listLikeArray) FixedSize() (int32, bool) {
	return l.fixedSize, l.fixedSize >= 0
}

func canBulkCopyUint8ListValues(listReader *listLikeArray, uint8Reader *array.Uint8) bool {
	_, fixedSize := listReader.FixedSize()
	return !fixedSize && uint8Reader.NullN() == 0
}

func getListLikeArrayData[T any](listReader *listLikeArray, getElement func(int) (T, error), outputArray func(arr []T, valid bool)) error {
	_, fixedSize := listReader.FixedSize()
	for i := 0; i < listReader.Len(); i++ {
		if fixedSize && listReader.IsNull(i) {
			outputArray(nil, false)
			continue
		}

		start, end := listReader.ValueOffsets(i)
		arrData := make([]T, 0, end-start)
		for j := start; j < end; j++ {
			elementVal, err := getElement(int(j))
			if err != nil {
				return err
			}
			arrData = append(arrData, elementVal)
		}
		valid := start != end
		if fixedSize {
			valid = !listReader.IsNull(i)
		}
		outputArray(arrData, valid)
	}
	return nil
}

func checkListLikeVectorAligned(listReader *listLikeArray, dim int, dataType schemapb.DataType) error {
	if dataType == schemapb.DataType_SparseFloatVector {
		return nil
	}

	if fixedSize, ok := listReader.FixedSize(); ok {
		expected, err := expectedVectorListLength(dim, dataType)
		if err != nil {
			return err
		}
		return checkVectorAlignWithDim([]int32{0, fixedSize}, expected)
	}

	offsets := make([]int32, 0, listReader.Len()+1)
	for i := 0; i < listReader.Len(); i++ {
		start, _ := listReader.ValueOffsets(i)
		offsets = append(offsets, int32(start))
	}
	if listReader.Len() > 0 {
		_, end := listReader.ValueOffsets(listReader.Len() - 1)
		offsets = append(offsets, int32(end))
	} else {
		offsets = append(offsets, 0)
	}
	return checkVectorAligned(offsets, dim, dataType)
}

func checkNullableListLikeVectorAligned(listReader *listLikeArray, dim int, dataType schemapb.DataType) error {
	if dataType == schemapb.DataType_SparseFloatVector {
		return nil
	}

	expected, err := expectedVectorListLength(dim, dataType)
	if err != nil {
		return err
	}
	return checkNullableListLikeVectorAlignedWithExpected(listReader, expected)
}

func checkListLikeVectorAlignedWithExpected(listReader *listLikeArray, expected int32) error {
	if fixedSize, ok := listReader.FixedSize(); ok {
		return checkVectorAlignWithDim([]int32{0, fixedSize}, expected)
	}
	offsets := make([]int32, 0, listReader.Len()+1)
	for i := 0; i < listReader.Len(); i++ {
		start, _ := listReader.ValueOffsets(i)
		offsets = append(offsets, int32(start))
	}
	if listReader.Len() > 0 {
		_, end := listReader.ValueOffsets(listReader.Len() - 1)
		offsets = append(offsets, int32(end))
	} else {
		offsets = append(offsets, 0)
	}
	return checkVectorAlignWithDim(offsets, expected)
}

func checkNullableListLikeVectorAlignedWithExpected(listReader *listLikeArray, expected int32) error {
	for i := 0; i < listReader.Len(); i++ {
		if listReader.IsNull(i) {
			continue
		}
		start, end := listReader.ValueOffsets(i)
		if end-start != int64(expected) {
			return checkVectorAlignWithDim([]int32{0, int32(end - start)}, expected)
		}
	}
	return nil
}

func expectedVectorListLength(dim int, dataType schemapb.DataType) (int32, error) {
	switch dataType {
	case schemapb.DataType_BinaryVector:
		return int32(dim / 8), nil
	case schemapb.DataType_FloatVector:
		return int32(dim), nil
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		return int32(dim * 2), nil
	case schemapb.DataType_Int8Vector:
		return int32(dim), nil
	default:
		return 0, merr.WrapErrParameterInvalidMsg("unexpected vector data type %s", dataType.String())
	}
}

func integerOrFloatElementGetter[T constraints.Integer | constraints.Float](field *schemapb.FieldSchema, valueReader arrow.Array) (func(int) (T, error), error) {
	switch valueReader.DataType().ID() {
	case arrow.INT8:
		int8Reader := valueReader.(*array.Int8)
		return func(i int) (T, error) {
			if int8Reader.IsNull(i) {
				return 0, WrapNullElementErr(field)
			}
			return T(int8Reader.Value(i)), nil
		}, nil
	case arrow.INT16:
		int16Reader := valueReader.(*array.Int16)
		return func(i int) (T, error) {
			if int16Reader.IsNull(i) {
				return 0, WrapNullElementErr(field)
			}
			return T(int16Reader.Value(i)), nil
		}, nil
	case arrow.INT32:
		int32Reader := valueReader.(*array.Int32)
		return func(i int) (T, error) {
			if int32Reader.IsNull(i) {
				return 0, WrapNullElementErr(field)
			}
			return T(int32Reader.Value(i)), nil
		}, nil
	case arrow.INT64:
		int64Reader := valueReader.(*array.Int64)
		return func(i int) (T, error) {
			if int64Reader.IsNull(i) {
				return 0, WrapNullElementErr(field)
			}
			return T(int64Reader.Value(i)), nil
		}, nil
	case arrow.FLOAT32:
		float32Reader := valueReader.(*array.Float32)
		return func(i int) (T, error) {
			if float32Reader.IsNull(i) {
				return 0, WrapNullElementErr(field)
			}
			return T(float32Reader.Value(i)), nil
		}, nil
	case arrow.FLOAT64:
		float64Reader := valueReader.(*array.Float64)
		return func(i int) (T, error) {
			if float64Reader.IsNull(i) {
				return 0, WrapNullElementErr(field)
			}
			return T(float64Reader.Value(i)), nil
		}, nil
	default:
		return nil, WrapTypeErr(field, valueReader.DataType().Name())
	}
}

func readIntegerOrFloatListLikeData[T constraints.Integer | constraints.Float](field *schemapb.FieldSchema, listReader *listLikeArray, outputArray func(arr []T, valid bool)) error {
	getElement, err := integerOrFloatElementGetter[T](field, listReader.ListValues())
	if err != nil {
		return err
	}
	return getListLikeArrayData(listReader, getElement, outputArray)
}

// appendFlatIntegerOrFloatListLike appends the elements of every list row to
// flat in row order, without allocating a slice per row.
func appendFlatIntegerOrFloatListLike[T constraints.Integer | constraints.Float](field *schemapb.FieldSchema, listReader *listLikeArray, flat []T) ([]T, error) {
	valueReader := listReader.ListValues()
	if valueReader.NullN() == 0 {
		if grown, ok := bulkAppendFlatValues(flat, listReader, valueReader); ok {
			return grown, nil
		}
	}
	getElement, err := integerOrFloatElementGetter[T](field, valueReader)
	if err != nil {
		return flat, err
	}
	for i := 0; i < listReader.Len(); i++ {
		start, end := listReader.ValueOffsets(i)
		for j := start; j < end; j++ {
			elementVal, err := getElement(int(j))
			if err != nil {
				return flat, err
			}
			flat = append(flat, elementVal)
		}
	}
	return flat, nil
}

// bulkAppendFlatValues copies list values into flat in bulk when the arrow
// element type matches T exactly, and reports ok=false otherwise.
func bulkAppendFlatValues[T constraints.Integer | constraints.Float](flat []T, listReader *listLikeArray, valueReader arrow.Array) ([]T, bool) {
	rows := listReader.Len()
	switch valueReader := valueReader.(type) {
	case *array.Int8:
		dst, ok := any(&flat).(*[]int8)
		if !ok {
			return flat, false
		}
		values := valueReader.Int8Values()
		for i := 0; i < rows; i++ {
			start, end := listReader.ValueOffsets(i)
			*dst = append(*dst, values[start:end]...)
		}
		return any(*dst).([]T), true
	case *array.Int16:
		dst, ok := any(&flat).(*[]int16)
		if !ok {
			return flat, false
		}
		values := valueReader.Int16Values()
		for i := 0; i < rows; i++ {
			start, end := listReader.ValueOffsets(i)
			*dst = append(*dst, values[start:end]...)
		}
		return any(*dst).([]T), true
	case *array.Int32:
		dst, ok := any(&flat).(*[]int32)
		if !ok {
			return flat, false
		}
		values := valueReader.Int32Values()
		for i := 0; i < rows; i++ {
			start, end := listReader.ValueOffsets(i)
			*dst = append(*dst, values[start:end]...)
		}
		return any(*dst).([]T), true
	case *array.Int64:
		dst, ok := any(&flat).(*[]int64)
		if !ok {
			return flat, false
		}
		values := valueReader.Int64Values()
		for i := 0; i < rows; i++ {
			start, end := listReader.ValueOffsets(i)
			*dst = append(*dst, values[start:end]...)
		}
		return any(*dst).([]T), true
	case *array.Float32:
		dst, ok := any(&flat).(*[]float32)
		if !ok {
			return flat, false
		}
		values := valueReader.Float32Values()
		for i := 0; i < rows; i++ {
			start, end := listReader.ValueOffsets(i)
			*dst = append(*dst, values[start:end]...)
		}
		return any(*dst).([]T), true
	case *array.Float64:
		dst, ok := any(&flat).(*[]float64)
		if !ok {
			return flat, false
		}
		values := valueReader.Float64Values()
		for i := 0; i < rows; i++ {
			start, end := listReader.ValueOffsets(i)
			*dst = append(*dst, values[start:end]...)
		}
		return any(*dst).([]T), true
	default:
		return flat, false
	}
}

func checkFloatListValueType(field *schemapb.FieldSchema, valueReader arrow.Array) error {
	switch valueReader.DataType().ID() {
	case arrow.FLOAT32, arrow.FLOAT64:
		return nil
	default:
		return WrapTypeErr(field, valueReader.DataType().Name())
	}
}

func appendFloatListRangeAsFloat32(field *schemapb.FieldSchema, valueReader arrow.Array, flat []float32, start, end int64) ([]float32, error) {
	switch valueReader := valueReader.(type) {
	case *array.Float32:
		if valueReader.NullN() == 0 {
			return append(flat, valueReader.Float32Values()[start:end]...), nil
		}
		for j := start; j < end; j++ {
			if valueReader.IsNull(int(j)) {
				return flat, WrapNullElementErr(field)
			}
			flat = append(flat, valueReader.Value(int(j)))
		}
		return flat, nil
	case *array.Float64:
		for j := start; j < end; j++ {
			if valueReader.IsNull(int(j)) {
				return flat, WrapNullElementErr(field)
			}
			flat = append(flat, float32(valueReader.Value(int(j))))
		}
		return flat, nil
	default:
		return flat, WrapTypeErr(field, valueReader.DataType().Name())
	}
}

func appendFlatFloatListLikeDataAsFloat32(field *schemapb.FieldSchema, listReader *listLikeArray, flat []float32) ([]float32, error) {
	valueReader := listReader.ListValues()
	if err := checkFloatListValueType(field, valueReader); err != nil {
		return flat, err
	}
	for i := 0; i < listReader.Len(); i++ {
		start, end := listReader.ValueOffsets(i)
		var err error
		if flat, err = appendFloatListRangeAsFloat32(field, valueReader, flat, start, end); err != nil {
			return flat, err
		}
	}
	return flat, nil
}

func appendFlatNullableFloatListLikeDataAsFloat32(field *schemapb.FieldSchema, listReader *listLikeArray, flat []float32, validData []bool) ([]float32, []bool, error) {
	valueReader := listReader.ListValues()
	if err := checkFloatListValueType(field, valueReader); err != nil {
		return flat, validData, err
	}
	_, fixedSize := listReader.FixedSize()
	for i := 0; i < listReader.Len(); i++ {
		start, end := listReader.ValueOffsets(i)
		valid := start != end
		if fixedSize {
			valid = !listReader.IsNull(i)
		}
		validData = append(validData, valid)
		if !valid {
			continue
		}
		var err error
		if flat, err = appendFloatListRangeAsFloat32(field, valueReader, flat, start, end); err != nil {
			return flat, validData, err
		}
	}
	return flat, validData, nil
}

func readBoolListLikeData(field *schemapb.FieldSchema, listReader *listLikeArray, outputArray func(arr []bool, valid bool)) error {
	valueReader := listReader.ListValues()
	boolReader, ok := valueReader.(*array.Boolean)
	if !ok {
		return WrapTypeErr(field, valueReader.DataType().Name())
	}
	return getListLikeArrayData(listReader, func(i int) (bool, error) {
		if boolReader.IsNull(i) {
			return false, WrapNullElementErr(field)
		}
		return boolReader.Value(i), nil
	}, outputArray)
}

func readStringListLikeData(field *schemapb.FieldSchema, listReader *listLikeArray, checkValue func(string) error, outputArray func(arr []string, valid bool)) error {
	valueReader := listReader.ListValues()
	stringReader, ok := valueReader.(*array.String)
	if !ok {
		return WrapTypeErr(field, valueReader.DataType().Name())
	}
	return getListLikeArrayData(listReader, func(i int) (string, error) {
		if stringReader.IsNull(i) {
			return "", WrapNullElementErr(field)
		}
		val := stringReader.Value(i)
		if err := checkValue(val); err != nil {
			return val, err
		}
		return val, nil
	}, outputArray)
}

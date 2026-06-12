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

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
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

func readIntegerOrFloatListLikeData[T constraints.Integer | constraints.Float](field *schemapb.FieldSchema, listReader *listLikeArray, outputArray func(arr []T, valid bool)) error {
	valueReader := listReader.ListValues()
	switch valueReader.DataType().ID() {
	case arrow.INT8:
		int8Reader := valueReader.(*array.Int8)
		return getListLikeArrayData(listReader, func(i int) (T, error) {
			if int8Reader.IsNull(i) {
				return 0, WrapNullElementErr(field)
			}
			return T(int8Reader.Value(i)), nil
		}, outputArray)
	case arrow.INT16:
		int16Reader := valueReader.(*array.Int16)
		return getListLikeArrayData(listReader, func(i int) (T, error) {
			if int16Reader.IsNull(i) {
				return 0, WrapNullElementErr(field)
			}
			return T(int16Reader.Value(i)), nil
		}, outputArray)
	case arrow.INT32:
		int32Reader := valueReader.(*array.Int32)
		return getListLikeArrayData(listReader, func(i int) (T, error) {
			if int32Reader.IsNull(i) {
				return 0, WrapNullElementErr(field)
			}
			return T(int32Reader.Value(i)), nil
		}, outputArray)
	case arrow.INT64:
		int64Reader := valueReader.(*array.Int64)
		return getListLikeArrayData(listReader, func(i int) (T, error) {
			if int64Reader.IsNull(i) {
				return 0, WrapNullElementErr(field)
			}
			return T(int64Reader.Value(i)), nil
		}, outputArray)
	case arrow.FLOAT32:
		float32Reader := valueReader.(*array.Float32)
		return getListLikeArrayData(listReader, func(i int) (T, error) {
			if float32Reader.IsNull(i) {
				return 0, WrapNullElementErr(field)
			}
			return T(float32Reader.Value(i)), nil
		}, outputArray)
	case arrow.FLOAT64:
		float64Reader := valueReader.(*array.Float64)
		return getListLikeArrayData(listReader, func(i int) (T, error) {
			if float64Reader.IsNull(i) {
				return 0, WrapNullElementErr(field)
			}
			return T(float64Reader.Value(i)), nil
		}, outputArray)
	default:
		return WrapTypeErr(field, valueReader.DataType().Name())
	}
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

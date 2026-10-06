package fieldvalidator

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestValidateUtil_FloatingArrayNaN(t *testing.T) {
	for _, elementType := range []schemapb.DataType{schemapb.DataType_Float, schemapb.DataType_Double} {
		for _, structField := range []bool{false, true} {
			fieldName := "values"
			if structField {
				fieldName = typeutil.ConcatStructFieldName("items", fieldName)
			}
			t.Run(fmt.Sprintf("%s/%s", elementType, fieldName), func(t *testing.T) {
				for _, tc := range []struct {
					name      string
					values    []float64
					validData []bool
					disable   bool
					nanIndex  int
				}{
					{name: "nan_first", values: []float64{math.NaN(), 1, 2}, nanIndex: 0},
					{name: "nan_middle", values: []float64{1, math.NaN(), 2}, nanIndex: 1},
					{name: "nan_last", values: []float64{1, 2, math.NaN()}, nanIndex: 2},
					{name: "nan_after_leading_null", values: []float64{math.NaN(), 2}, validData: []bool{false, true, true}, nanIndex: 1},
					{name: "nan_after_middle_null", values: []float64{1, math.NaN()}, validData: []bool{true, false, true}, nanIndex: 2},
					{name: "nan_before_trailing_null", values: []float64{1, math.NaN()}, validData: []bool{true, true, false}, nanIndex: 1},
					{name: "leading_null", values: []float64{1, 2}, validData: []bool{false, true, true}, nanIndex: -1},
					{name: "middle_null", values: []float64{1, 2}, validData: []bool{true, false, true}, nanIndex: -1},
					{name: "trailing_null", values: []float64{1, 2}, validData: []bool{true, true, false}, nanIndex: -1},
					{name: "all_null", validData: []bool{false, false}, nanIndex: -1},
					{name: "empty", nanIndex: -1},
					{name: "infinities", values: []float64{math.Inf(-1), math.Inf(1)}, nanIndex: -1},
					{name: "check_disabled", values: []float64{math.NaN()}, disable: true, nanIndex: -1},
				} {
					t.Run(tc.name, func(t *testing.T) {
						row := floatingArrayNaNTestRow(elementType, tc.values, tc.validData)
						fieldSchema := &schemapb.FieldSchema{
							Name: fieldName, DataType: schemapb.DataType_Array,
							ElementType: elementType, ElementNullable: tc.validData != nil,
						}
						schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{fieldSchema}}
						if structField {
							schema.Fields = nil
							schema.StructArrayFields = []*schemapb.StructArrayFieldSchema{{
								Name: "items", Fields: []*schemapb.FieldSchema{fieldSchema},
							}}
						}
						helper, err := typeutil.CreateSchemaHelper(schema)
						require.NoError(t, err)
						field := &schemapb.FieldData{
							FieldName: fieldName, Type: schemapb.DataType_Array,
							Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
								Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
									ElementType: elementType, Data: []*schemapb.ScalarField{row},
								}},
							}},
						}
						validator := NewValidateUtil()
						validator.checkNAN = !tc.disable
						err = validator.Validate([]*schemapb.FieldData{field}, helper, 1)
						if tc.nanIndex >= 0 {
							require.ErrorIs(t, err, merr.ErrParameterInvalid)
							require.Equal(t, merr.InputError, merr.GetErrorType(err))
							require.Contains(t, err.Error(), fieldName)
							require.Contains(t, err.Error(), fmt.Sprintf("row 0 array element %d is NaN", tc.nanIndex))
							return
						}
						require.NoError(t, err)
						require.Equal(t, tc.validData, typeutil.GetArrayElementValidData(row))
						values := floatingArrayNaNTestValues(row, elementType)
						if tc.validData != nil {
							require.Len(t, values, len(tc.validData))
							payloadIndex := 0
							for i, valid := range tc.validData {
								if valid {
									require.Equal(t, tc.values[payloadIndex], values[i])
									payloadIndex++
								} else {
									require.Zero(t, values[i])
								}
							}
						} else if tc.disable {
							require.True(t, math.IsNaN(values[0]))
						} else {
							require.Equal(t, tc.values, values)
						}
					})
				}
			})
		}
	}
}

func TestValidateUtil_FloatingArrayNaNInvalidCompactPayload(t *testing.T) {
	for _, elementType := range []schemapb.DataType{schemapb.DataType_Float, schemapb.DataType_Double} {
		t.Run(elementType.String(), func(t *testing.T) {
			row := floatingArrayNaNTestRow(elementType, []float64{math.NaN()}, []bool{false})
			err := NewValidateUtil().checkArrayElement(&schemapb.ArrayArray{Data: []*schemapb.ScalarField{row}}, &schemapb.FieldSchema{
				Name: "values", ElementType: elementType, ElementNullable: true,
			})
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			require.Contains(t, err.Error(), "compact payload")
			require.NotContains(t, err.Error(), "is NaN")
		})
	}
}

func floatingArrayNaNTestRow(elementType schemapb.DataType, values []float64, validData []bool) *schemapb.ScalarField {
	row := &schemapb.ScalarField{ValidData: validData}
	if elementType == schemapb.DataType_Float {
		var floats []float32
		for _, value := range values {
			floats = append(floats, float32(value))
		}
		row.Data = &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: floats}}
	} else {
		row.Data = &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: values}}
	}
	return row
}

func floatingArrayNaNTestValues(row *schemapb.ScalarField, elementType schemapb.DataType) []float64 {
	if elementType == schemapb.DataType_Float {
		var values []float64
		for _, value := range row.GetFloatData().GetData() {
			values = append(values, float64(value))
		}
		return values
	}
	return row.GetDoubleData().GetData()
}

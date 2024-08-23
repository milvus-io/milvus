package csv

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type RowParser interface {
	Parse(raw []string) (Row, error)
}
type rowParser struct {
	header       []string
	name2Dim     map[string]int
	name2Field   map[string]*schemapb.FieldSchema
	pkField      *schemapb.FieldSchema
	dynamicField *schemapb.FieldSchema
}

func NewRowParser(schema *schemapb.CollectionSchema, header []string) (RowParser, error) {
	name2Field := lo.KeyBy(schema.GetFields(),
		func(field *schemapb.FieldSchema) string {
			return field.GetName()
		})

	name2Dim := make(map[string]int)
	for name, field := range name2Field {
		if typeutil.IsVectorType(field.GetDataType()) && !typeutil.IsSparseFloatVectorType(field.GetDataType()) {
			dim, err := typeutil.GetDim(field)
			if err != nil {
				return nil, err
			}
			name2Dim[name] = int(dim)
		}
	}

	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}

	if pkField.GetAutoID() {
		delete(name2Field, pkField.GetName())
	}

	dynamicField := typeutil.GetDynamicField(schema)
	if dynamicField != nil {
		delete(name2Field, dynamicField.GetName())
	}

	// check if csv header provides the primary key while it should be auto-generated
	if pkField.GetAutoID() && lo.Contains(header, pkField.GetName()) {
		return nil, merr.WrapErrImportFailed(
			fmt.Sprintf("the primary key '%s' is auto-generated, no need to provide", pkField.GetName()))
	}

	// check whether csv header contains all fields in schema
	// except auto generated primary key and dynamic field
	nameMap := make(map[string]bool)
	for _, name := range header {
		nameMap[name] = true
	}
	for fieldName := range name2Field {
		if _, ok := nameMap[fieldName]; !ok && (fieldName != dynamicField.GetName()) && (fieldName != pkField.GetName() && !pkField.GetAutoID()) {
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("value of field is missed: '%s'", fieldName))
		}
	}

	return &rowParser{
		name2Dim:     name2Dim,
		header:       header,
		name2Field:   name2Field,
		pkField:      pkField,
		dynamicField: dynamicField,
	}, nil
}

func (r *rowParser) Parse(strArr []string) (Row, error) {
	if len(strArr) != len(r.header) {
		return nil, merr.WrapErrImportFailed("the number of fields in the row is not equal to the header")
	}

	row := make(Row)
	dynamicValues := make(map[string]string)
	for index, value := range strArr {
		if field, ok := r.name2Field[r.header[index]]; ok {
			data, err := r.parseEntity(field, value)
			if err != nil {
				return nil, err
			}
			row[field.GetFieldID()] = data
		} else if r.dynamicField != nil {
			dynamicValues[r.header[index]] = value
		} else {
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("the field '%s' is not defined in schema", r.header[index]))
		}
	}

	// combine the redundant pairs into dynamic field
	// for csv which is directly uploaded to minio, it's necessary to check and put the fields not in schema into dynamic field
	if r.dynamicField != nil {
		err := r.combineDynamicRow(dynamicValues, row)
		if err != nil {
			return nil, err
		}
	}
	return row, nil
}

func (r *rowParser) combineDynamicRow(dynamicValues map[string]string, row Row) error {
	dynamicFieldID := r.dynamicField.GetFieldID()
	MetaName := r.dynamicField.GetName()
	if len(dynamicValues) == 0 {
		row[dynamicFieldID] = []byte("{}")
		return nil
	}

	newDynamicValues := make(map[string]any)
	if str, ok := dynamicValues[MetaName]; ok {
		// parse $meta field to json object
		var mp map[string]interface{}
		err := json.Unmarshal([]byte(str), &mp)
		if err != nil {
			return merr.WrapErrImportFailed("illegal value for dynamic field, not a JSON format string")
		}
		// put the all dynamic fields into newDynamicValues
		for k, v := range mp {
			if _, ok = dynamicValues[k]; ok {
				return merr.WrapErrImportFailed(fmt.Sprintf("duplicated key in dynamic field, key=%s", k))
			}
			newDynamicValues[k] = v
		}
		// remove $meta field from dynamicValues
		delete(dynamicValues, MetaName)
	}
	// put dynamic fields (except $meta) into newDynamicValues
	// due to the limit of csv, the number value is stored as string
	for k, v := range dynamicValues {
		newDynamicValues[k] = v
	}

	// check if stasify the json format
	dynamicBytes, err := json.Marshal(newDynamicValues)
	if err != nil {
		return merr.WrapErrImportFailed("illegal value for dynamic field, not a JSON object")
	}
	row[dynamicFieldID] = dynamicBytes

	return nil
}

func (r *rowParser) parseEntity(field *schemapb.FieldSchema, obj string) (any, error) {
	switch field.GetDataType() {
	case schemapb.DataType_Bool:
		b, err := strconv.ParseBool(obj)
		if err != nil {
			return false, r.wrapTypeError(obj, field)
		}
		return b, nil
	case schemapb.DataType_Int8:
		num, err := strconv.ParseInt(obj, 10, 8)
		if err != nil {
			return 0, r.wrapTypeError(obj, field)
		}
		return int8(num), nil
	case schemapb.DataType_Int16:
		num, err := strconv.ParseInt(obj, 10, 16)
		if err != nil {
			return 0, r.wrapTypeError(obj, field)
		}
		return int16(num), nil
	case schemapb.DataType_Int32:
		num, err := strconv.ParseInt(obj, 10, 32)
		if err != nil {
			return 0, r.wrapTypeError(obj, field)
		}
		return int32(num), nil
	case schemapb.DataType_Int64:
		num, err := strconv.ParseInt(obj, 10, 64)
		if err != nil {
			return 0, r.wrapTypeError(obj, field)
		}
		return num, nil
	case schemapb.DataType_Float:
		num, err := strconv.ParseFloat(obj, 32)
		if err != nil {
			return 0, r.wrapTypeError(obj, field)
		}
		return float32(num), nil
	case schemapb.DataType_Double:
		num, err := strconv.ParseFloat(obj, 64)
		if err != nil {
			return 0, r.wrapTypeError(obj, field)
		}
		return num, nil
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		return obj, nil
	case schemapb.DataType_BinaryVector:
		var vec []byte
		err := json.Unmarshal([]byte(obj), &vec)
		if err != nil {
			return nil, r.wrapTypeError(obj, field)
		}
		if len(vec) != r.name2Dim[field.GetName()]/8 {
			return nil, r.wrapDimError(len(vec)*8, field)
		}
		return vec, nil
	case schemapb.DataType_JSON:
		var data interface{}
		err := json.Unmarshal([]byte(obj), &data)
		if err != nil {
			return nil, err
		}
		return []byte(obj), nil
	case schemapb.DataType_FloatVector:
		var vec []float32
		err := json.Unmarshal([]byte(obj), &vec)
		if err != nil {
			return nil, r.wrapTypeError(obj, field)
		}
		if len(vec) != r.name2Dim[field.GetName()] {
			return nil, r.wrapDimError(len(vec), field)
		}
		return vec, nil
	case schemapb.DataType_Float16Vector:
		var vec []float32
		err := json.Unmarshal([]byte(obj), &vec)
		if err != nil {
			return nil, r.wrapTypeError(obj, field)
		}
		if len(vec) != r.name2Dim[field.GetName()] {
			return nil, r.wrapDimError(len(vec), field)
		}
		vec2 := make([]byte, len(vec)*2)
		for i := 0; i < len(vec); i++ {
			copy(vec2[i*2:], typeutil.Float32ToFloat16Bytes(vec[i]))
		}
		return vec2, nil
	case schemapb.DataType_BFloat16Vector:
		var vec []float32
		err := json.Unmarshal([]byte(obj), &vec)
		if err != nil {
			return nil, r.wrapTypeError(obj, field)
		}
		if len(vec) != r.name2Dim[field.GetName()] {
			return nil, r.wrapDimError(len(vec), field)
		}
		vec2 := make([]byte, len(vec)*2)
		for i := 0; i < len(vec); i++ {
			copy(vec2[i*2:], typeutil.Float32ToBFloat16Bytes(vec[i]))
		}
		return vec2, nil
	case schemapb.DataType_SparseFloatVector:
		// use dec.UseNumber() to avoid float64 precision loss
		var vec map[string]interface{}
		dec := json.NewDecoder(strings.NewReader(obj))
		dec.UseNumber()
		err := dec.Decode(&vec)
		if err != nil {
			return nil, r.wrapTypeError(obj, field)
		}
		vec2, err := typeutil.CreateSparseFloatRowFromMap(vec)
		if err != nil {
			return nil, err
		}
		return vec2, nil
	case schemapb.DataType_Array:
		var vec []interface{}
		desc := json.NewDecoder(strings.NewReader(obj))
		desc.UseNumber()
		err := desc.Decode(&vec)
		if err != nil {
			return nil, r.wrapTypeError(obj, field)
		}
		scalarFieldData, err := r.arrayToFieldData(vec, field.GetElementType())
		if err != nil {
			return nil, err
		}
		return scalarFieldData, nil
	default:
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("parse csv failed, unsupport data type: %s",
			field.GetDataType().String()))
	}
}

func (r *rowParser) arrayToFieldData(arr []interface{}, eleType schemapb.DataType) (*schemapb.ScalarField, error) {
	switch eleType {
	case schemapb.DataType_Bool:
		values := make([]bool, 0)
		for i := 0; i < len(arr); i++ {
			value, ok := arr[i].(bool)
			if !ok {
				return nil, r.wrapArrayValueTypeError(arr, eleType)
			}
			values = append(values, value)
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_BoolData{
				BoolData: &schemapb.BoolArray{
					Data: values,
				},
			},
		}, nil
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		values := make([]int32, 0)
		for i := 0; i < len(arr); i++ {
			value, ok := arr[i].(json.Number)
			if !ok {
				return nil, r.wrapArrayValueTypeError(arr, eleType)
			}
			num, err := strconv.ParseInt(value.String(), 10, 32)
			if err != nil {
				return nil, err
			}
			values = append(values, int32(num))
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{
					Data: values,
				},
			},
		}, nil
	case schemapb.DataType_Int64:
		values := make([]int64, 0)
		for i := 0; i < len(arr); i++ {
			value, ok := arr[i].(json.Number)
			if !ok {
				return nil, r.wrapArrayValueTypeError(arr, eleType)
			}
			num, err := strconv.ParseInt(value.String(), 10, 64)
			if err != nil {
				return nil, err
			}
			values = append(values, num)
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{
				LongData: &schemapb.LongArray{
					Data: values,
				},
			},
		}, nil
	case schemapb.DataType_Float:
		values := make([]float32, 0)
		for i := 0; i < len(arr); i++ {
			value, ok := arr[i].(json.Number)
			if !ok {
				return nil, r.wrapArrayValueTypeError(arr, eleType)
			}
			num, err := strconv.ParseFloat(value.String(), 32)
			if err != nil {
				return nil, err
			}
			values = append(values, float32(num))
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_FloatData{
				FloatData: &schemapb.FloatArray{
					Data: values,
				},
			},
		}, nil
	case schemapb.DataType_Double:
		values := make([]float64, 0)
		for i := 0; i < len(arr); i++ {
			value, ok := arr[i].(json.Number)
			if !ok {
				return nil, r.wrapArrayValueTypeError(arr, eleType)
			}
			num, err := strconv.ParseFloat(value.String(), 64)
			if err != nil {
				return nil, err
			}
			values = append(values, num)
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_DoubleData{
				DoubleData: &schemapb.DoubleArray{
					Data: values,
				},
			},
		}, nil
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		values := make([]string, 0)
		for i := 0; i < len(arr); i++ {
			value, ok := arr[i].(string)
			if !ok {
				return nil, r.wrapArrayValueTypeError(arr, eleType)
			}
			values = append(values, value)
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{
				StringData: &schemapb.StringArray{
					Data: values,
				},
			},
		}, nil
	default:
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("parse csv failed, unsupport data type: %s", eleType.String()))
	}
}

func (r *rowParser) wrapTypeError(v any, field *schemapb.FieldSchema) error {
	return merr.WrapErrImportFailed(fmt.Sprintf("expected type '%s' for field '%s', got type '%T' with value '%v'",
		field.GetDataType().String(), field.GetName(), v, v))
}

func (r *rowParser) wrapDimError(actualDim int, field *schemapb.FieldSchema) error {
	return merr.WrapErrImportFailed(fmt.Sprintf("expected dim '%d' for field '%s' with type '%s', got dim '%d'",
		r.name2Dim[field.GetName()], field.GetName(), field.GetDataType().String(), actualDim))
}

func (r *rowParser) wrapArrayValueTypeError(v any, eleType schemapb.DataType) error {
	return merr.WrapErrImportFailed(fmt.Sprintf("expected element type '%s' in array field, got type '%T' with value '%v'",
		eleType.String(), v, v))
}

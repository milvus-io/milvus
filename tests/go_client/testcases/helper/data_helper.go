package helper

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/tests/go_client/common"
)

// insert params
type InsertParams struct {
	Schema        *entity.Schema
	PartitionName string
	IsRows        bool
}

func NewInsertParams(schema *entity.Schema) *InsertParams {
	return &InsertParams{
		Schema: schema,
	}
}

func (opt *InsertParams) TWithPartitionName(partitionName string) *InsertParams {
	opt.PartitionName = partitionName
	return opt
}

func (opt *InsertParams) TWithIsRows(isRows bool) *InsertParams {
	opt.IsRows = isRows
	return opt
}

// GenDataOption -- create column data --
type GenDataOption struct {
	nb               int
	start            int
	dim              int
	maxLen           int
	sparseMaxLen     int
	maxCapacity      int
	elementType      entity.FieldType
	fieldName        string
	textLang         string
	texts            []string
	textEmptyPercent int
	validData        []bool
}

func (opt *GenDataOption) TWithNb(nb int) *GenDataOption {
	opt.nb = nb
	return opt
}

func (opt *GenDataOption) TWithDim(dim int) *GenDataOption {
	opt.dim = dim
	return opt
}

func (opt *GenDataOption) TWithMaxLen(maxLen int) *GenDataOption {
	opt.maxLen = maxLen
	return opt
}

func (opt *GenDataOption) TWithSparseMaxLen(sparseMaxLen int) *GenDataOption {
	opt.sparseMaxLen = sparseMaxLen
	return opt
}

func (opt *GenDataOption) TWithMaxCapacity(maxCap int) *GenDataOption {
	opt.maxCapacity = maxCap
	return opt
}

func (opt *GenDataOption) TWithStart(start int) *GenDataOption {
	opt.start = start
	return opt
}

func (opt *GenDataOption) TWithFieldName(fieldName string) *GenDataOption {
	opt.fieldName = fieldName
	return opt
}

func (opt *GenDataOption) TWithElementType(eleType entity.FieldType) *GenDataOption {
	opt.elementType = eleType
	return opt
}

func (opt *GenDataOption) TWithTextLang(lang string) *GenDataOption {
	opt.textLang = lang
	return opt
}

func (opt *GenDataOption) TWithTextData(texts []string) *GenDataOption {
	opt.texts = texts
	return opt
}

func (opt *GenDataOption) TWithTextEmptyPercent(percent int) *GenDataOption {
	opt.textEmptyPercent = percent
	return opt
}

func (opt *GenDataOption) TWithValidData(validData []bool) *GenDataOption {
	opt.validData = validData
	return opt
}

func TNewDataOption() *GenDataOption {
	return &GenDataOption{
		nb:               common.DefaultNb,
		start:            0,
		dim:              common.DefaultDim,
		maxLen:           common.TestMaxLen,
		sparseMaxLen:     common.TestMaxLen,
		maxCapacity:      common.TestCapacity,
		elementType:      entity.FieldTypeNone,
		fieldName:        "",
		textLang:         "",
		textEmptyPercent: 0,
	}
}

func GenArrayColumnData(nb int, eleType entity.FieldType, option GenDataOption) column.Column {
	start := option.start
	fieldName := option.fieldName
	if option.fieldName == "" {
		fieldName = GetFieldNameByElementType(eleType)
	}
	capacity := option.maxCapacity
	validDataLen := GetValidDataLen(option.validData)
	switch eleType {
	case entity.FieldTypeBool:
		boolValues := make([][]bool, 0, nb)
		for i := start; i < start+nb; i++ {
			boolArray := make([]bool, 0, capacity)
			for j := 0; j < capacity; j++ {
				boolArray = append(boolArray, i%2 == 0)
			}
			boolValues = append(boolValues, boolArray)
		}
		if validDataLen > 0 {
			nullableColumn, err := column.NewNullableColumnBoolArray(fieldName, boolValues, option.validData)
			if err != nil {
				log.Fatal("NewNullableColumnBoolArray failed", zap.Error(err))
			}
			return nullableColumn
		}
		return column.NewColumnBoolArray(fieldName, boolValues)
	case entity.FieldTypeInt8:
		int8Values := make([][]int8, 0, nb)
		for i := start; i < start+nb; i++ {
			int8Array := make([]int8, 0, capacity)
			for j := 0; j < capacity; j++ {
				int8Array = append(int8Array, int8(i+j))
			}
			int8Values = append(int8Values, int8Array)
		}
		if validDataLen > 0 {
			nullableColumn, err := column.NewNullableColumnInt8Array(fieldName, int8Values, option.validData)
			if err != nil {
				log.Fatal("NewNullableColumnInt8Array failed", zap.Error(err))
			}
			return nullableColumn
		}
		return column.NewColumnInt8Array(fieldName, int8Values)
	case entity.FieldTypeInt16:
		int16Values := make([][]int16, 0, nb)
		for i := start; i < start+nb; i++ {
			int16Array := make([]int16, 0, capacity)
			for j := 0; j < capacity; j++ {
				int16Array = append(int16Array, int16(i+j))
			}
			int16Values = append(int16Values, int16Array)
		}
		if validDataLen > 0 {
			nullableColumn, err := column.NewNullableColumnInt16Array(fieldName, int16Values, option.validData)
			if err != nil {
				log.Fatal("NewNullableColumnInt16Array failed", zap.Error(err))
			}
			return nullableColumn
		}
		return column.NewColumnInt16Array(fieldName, int16Values)
	case entity.FieldTypeInt32:
		int32Values := make([][]int32, 0, nb)
		for i := start; i < start+nb; i++ {
			int32Array := make([]int32, 0, capacity)
			for j := 0; j < capacity; j++ {
				int32Array = append(int32Array, int32(i+j))
			}
			int32Values = append(int32Values, int32Array)
		}
		if validDataLen > 0 {
			nullableColumn, err := column.NewNullableColumnInt32Array(fieldName, int32Values, option.validData)
			if err != nil {
				log.Fatal("NewNullableColumnInt32Array failed", zap.Error(err))
			}
			return nullableColumn
		}
		return column.NewColumnInt32Array(fieldName, int32Values)
	case entity.FieldTypeInt64:
		int64Values := make([][]int64, 0, nb)
		for i := start; i < start+nb; i++ {
			int64Array := make([]int64, 0, capacity)
			for j := 0; j < capacity; j++ {
				int64Array = append(int64Array, int64(i+j))
			}
			int64Values = append(int64Values, int64Array)
		}
		if validDataLen > 0 {
			nullableColumn, err := column.NewNullableColumnInt64Array(fieldName, int64Values, option.validData)
			if err != nil {
				log.Fatal("NewNullableColumnInt64Array failed", zap.Error(err))
			}
			return nullableColumn
		}
		return column.NewColumnInt64Array(fieldName, int64Values)
	case entity.FieldTypeFloat:
		floatValues := make([][]float32, 0, nb)
		for i := start; i < start+nb; i++ {
			floatArray := make([]float32, 0, capacity)
			for j := 0; j < capacity; j++ {
				floatArray = append(floatArray, float32(i+j))
			}
			floatValues = append(floatValues, floatArray)
		}
		if validDataLen > 0 {
			nullableColumn, err := column.NewNullableColumnFloatArray(fieldName, floatValues, option.validData)
			if err != nil {
				log.Fatal("NewNullableColumnFloatArray failed", zap.Error(err))
			}
			return nullableColumn
		}
		return column.NewColumnFloatArray(fieldName, floatValues)
	case entity.FieldTypeDouble:
		doubleValues := make([][]float64, 0, nb)
		for i := start; i < start+nb; i++ {
			doubleArray := make([]float64, 0, capacity)
			for j := 0; j < capacity; j++ {
				doubleArray = append(doubleArray, float64(i+j))
			}
			doubleValues = append(doubleValues, doubleArray)
		}
		if validDataLen > 0 {
			nullableColumn, err := column.NewNullableColumnDoubleArray(fieldName, doubleValues, option.validData)
			if err != nil {
				log.Fatal("NewNullableColumnDoubleArray failed", zap.Error(err))
			}
			return nullableColumn
		}
		return column.NewColumnDoubleArray(fieldName, doubleValues)
	case entity.FieldTypeVarChar:
		varcharValues := make([][]string, 0, nb)
		for i := start; i < start+nb; i++ {
			varcharArray := make([]string, 0, capacity)
			for j := 0; j < capacity; j++ {
				var buf bytes.Buffer
				buf.WriteString(strconv.Itoa(i + j))
				varcharArray = append(varcharArray, buf.String())
			}
			varcharValues = append(varcharValues, varcharArray)
		}
		if validDataLen > 0 {
			nullableColumn, err := column.NewNullableColumnVarCharArray(fieldName, varcharValues, option.validData)
			if err != nil {
				log.Fatal("NewNullableColumnVarCharArray failed", zap.Error(err))
			}
			return nullableColumn
		}
		return column.NewColumnVarCharArray(fieldName, varcharValues)
	default:
		log.Fatal("GenArrayColumnData failed", zap.Any("ElementType", eleType))
		return nil
	}
}

type JSONStruct struct {
	Number int32   `json:"number,omitempty" milvus:"name:number"`
	String string  `json:"string,omitempty" milvus:"name:string"`
	Float  float32 `json:"float,omitempty" milvus:"name:float"`
	*BoolStruct
	List        []int64   `json:"list,omitempty" milvus:"name:list"`
	FloatArray  []float64 `json:"floatArray,omitempty" milvus:"name:floatArray"`
	StringArray []string  `json:"stringArray,omitempty" milvus:"name:stringArray"`
}

// GenDefaultJSONData gen default column with data
func GenDefaultJSONData(nb int, option GenDataOption) [][]byte {
	jsonValues := make([][]byte, 0, nb)
	start := option.start
	var m interface{}
	for i := start; i < start+nb; i++ {
		// kv value
		_bool := &BoolStruct{
			Bool: i%2 == 0,
		}
		if i < (start+nb)/2 {
			if i%2 == 0 {
				m = JSONStruct{
					String:      strconv.Itoa(i),
					BoolStruct:  _bool,
					FloatArray:  []float64{float64(i), float64(i), float64(i)},
					StringArray: []string{fmt.Sprintf("%05d", i)},
				}
			} else {
				m = JSONStruct{
					Number:     int32(i),
					Float:      float32(i),
					String:     strconv.Itoa(i),
					BoolStruct: _bool,
					List:       []int64{int64(i), int64(i + 1)},
				}
			}
		} else {
			// int, float, string, list
			switch i % 4 {
			case 0:
				m = i
			case 1:
				m = float32(i)
			case 2:
				m = strconv.Itoa(i)
			case 3:
				m = []int64{int64(i), int64(i + 1)}
			}
		}
		bs, err := json.Marshal(&m)
		if err != nil {
			log.Fatal("Marshal json field failed", zap.Error(err))
		}
		jsonValues = append(jsonValues, bs)
	}
	return jsonValues
}

func GenNestedJSON(depth int, value any) map[string]interface{} {
	if depth == 1 {
		return map[string]interface{}{"value": value}
	}
	return map[string]interface{}{
		fmt.Sprintf("level%d", depth): GenNestedJSON(depth-1, value),
	}
}

func GenNestedJSONExprKey(depth int, jsonField string) string {
	var pathParts []string
	for i := depth; i > 1; i-- {
		pathParts = append(pathParts, fmt.Sprintf("level%d", i))
	}
	pathParts = append(pathParts, "value")
	return fmt.Sprintf("%s['%s']", jsonField, strings.Join(pathParts, "']['"))
}

func GetValidDataLen(validData []bool) int {
	validDataLen := 0
	for _, valid := range validData {
		if valid {
			validDataLen++
		}
	}
	return validDataLen
}

func GetAllFunctionsOutputFields(schema *entity.Schema) []string {
	var outputFields []string
	for _, fn := range schema.Functions {
		if fn.Type == entity.FunctionTypeBM25 || fn.Type == entity.FunctionTypeTextEmbedding {
			outputFields = append(outputFields, fn.OutputFieldNames...)
		}
	}
	return outputFields
}

func GenColumnDataWithOption(fieldType entity.FieldType, option GenDataOption) column.Column {
	return GenColumnData(option.nb, fieldType, option)
}

func GenDefaultGeometryData(nb int, option GenDataOption) []string {
	const (
		point           = "POINT (30.123 -10.456)"
		linestring      = "LINESTRING (30.123 -10.456, 10.789 30.123, -40.567 40.890)"
		polygon         = "POLYGON ((30.123 -10.456, 40.678 40.890, 20.345 40.567, 10.123 20.456, 30.123 -10.456))"
		multipoint      = "MULTIPOINT ((10.111 40.222), (40.333 30.444), (20.555 20.666), (30.777 10.888))"
		multilinestring = "MULTILINESTRING ((10.111 10.222, 20.333 20.444), (15.555 15.666, 25.777 25.888), (-30.999 20.000, 40.111 30.222))"
		multipolygon    = "MULTIPOLYGON (((30.123 -10.456, 40.678 40.890, 20.345 40.567, 10.123 20.456, 30.123 -10.456)),((15.123 5.456, 25.678 5.890, 25.345 15.567, 15.123 15.456, 15.123 5.456)))"
	)
	wktArray := [6]string{point, linestring, polygon, multipoint, multilinestring, multipolygon}
	geometryValues := make([]string, 0, nb)
	start := option.start
	for i := start; i < start+nb; i++ {
		geometryValues = append(geometryValues, wktArray[i%6])
	}
	return geometryValues
}

// GenColumnData GenColumnDataOption except dynamic column
func GenColumnData(nb int, fieldType entity.FieldType, option GenDataOption) column.Column {
	dim := option.dim
	sparseMaxLen := option.sparseMaxLen
	start := option.start
	fieldName := option.fieldName
	validDataLen := nb
	if option.validData != nil {
		validDataLen = GetValidDataLen(option.validData)
		if validDataLen > nb {
			validDataLen = nb
		}
	}
	log.Debug("GenColumnData", zap.Any("FieldType", fieldType), zap.Int("nb", nb), zap.Int("start", start), zap.Int("validDataLen", validDataLen))

	if option.fieldName == "" {
		fieldName = GetFieldNameByFieldType(fieldType, TWithElementType(option.elementType))
	}
	switch fieldType {
	case entity.FieldTypeInt64:
		int64Values := make([]int64, 0, validDataLen)
		for i := start; i < start+validDataLen; i++ {
			int64Values = append(int64Values, int64(i))
		}

		if validDataLen < nb {
			nullableColumn, err := column.NewNullableColumnInt64(fieldName, int64Values, option.validData)
			if err != nil {
				log.Fatal("NewNullableColumnInt64 failed", zap.Error(err))
			}
			return nullableColumn
		}
		return column.NewColumnInt64(fieldName, int64Values)

	case entity.FieldTypeInt8:
		count := 0
		int8Values := make([]int8, 0, validDataLen)
		for i := start; i < start+validDataLen; i++ {
			int8Values = append(int8Values, int8(i))
			if int8(i) == -1 {
				count += 1
			}
		}
		if validDataLen < nb {
			nullableColumn, err := column.NewNullableColumnInt8(fieldName, int8Values, option.validData)
			if err != nil {
				log.Fatal("NewNullableColumnInt8 failed", zap.Error(err))
			}
			return nullableColumn
		}
		return column.NewColumnInt8(fieldName, int8Values)

	case entity.FieldTypeInt16:
		int16Values := make([]int16, 0, validDataLen)
		for i := start; i < start+validDataLen; i++ {
			int16Values = append(int16Values, int16(i))
		}
		if validDataLen < nb {
			nullableColumn, err := column.NewNullableColumnInt16(fieldName, int16Values, option.validData)
			if err != nil {
				log.Fatal("NewNullableColumnInt16 failed", zap.Error(err))
			}
			return nullableColumn
		}
		return column.NewColumnInt16(fieldName, int16Values)

	case entity.FieldTypeInt32:
		int32Values := make([]int32, 0, validDataLen)
		for i := start; i < start+validDataLen; i++ {
			int32Values = append(int32Values, int32(i))
		}
		if validDataLen < nb {
			nullableColumn, err := column.NewNullableColumnInt32(fieldName, int32Values, option.validData)
			if err != nil {
				log.Fatal("NewNullableColumnInt32 failed", zap.Error(err))
			}
			return nullableColumn
		}
		return column.NewColumnInt32(fieldName, int32Values)

	case entity.FieldTypeBool:
		boolValues := make([]bool, 0, validDataLen)
		for i := start; i < start+validDataLen; i++ {
			boolValues = append(boolValues, i%2 == 0)
		}
		if validDataLen < nb {
			nullableColumn, err := column.NewNullableColumnBool(fieldName, boolValues, option.validData)
			if err != nil {
				log.Fatal("NewNullableColumnBool failed", zap.Error(err))
			}
			return nullableColumn
		}
		return column.NewColumnBool(fieldName, boolValues)

	case entity.FieldTypeFloat:
		floatValues := make([]float32, 0, validDataLen)
		for i := start; i < start+validDataLen; i++ {
			floatValues = append(floatValues, float32(i))
		}
		if validDataLen < nb {
			nullableColumn, err := column.NewNullableColumnFloat(fieldName, floatValues, option.validData)
			if err != nil {
				log.Fatal("NewNullableColumnFloat failed", zap.Error(err))
			}
			return nullableColumn
		}
		return column.NewColumnFloat(fieldName, floatValues)

	case entity.FieldTypeDouble:
		floatValues := make([]float64, 0, validDataLen)
		for i := start; i < start+validDataLen; i++ {
			floatValues = append(floatValues, float64(i))
		}
		if validDataLen < nb {
			nullableColumn, err := column.NewNullableColumnDouble(fieldName, floatValues, option.validData)
			if err != nil {
				log.Fatal("NewNullableColumnDouble failed", zap.Error(err))
			}
			return nullableColumn
		}
		return column.NewColumnDouble(fieldName, floatValues)

	case entity.FieldTypeVarChar:
		varcharValues := make([]string, 0, validDataLen)
		if option.textLang != "" {
			// Use language-specific text generation
			var lang string
			switch option.textLang {
			case "en", "english":
				lang = "en"
			case "zh", "chinese":
				lang = "zh"
			default:
				// Fallback to en for unsupported languages
				log.Warn("Unsupported language, fallback to English", zap.String("language", option.textLang))
				lang = "en"
			}

			// Generate text data with empty values based on textEmptyPercent
			for i := 0; i < validDataLen; i++ {
				if rand.Float64()*100 < float64(option.textEmptyPercent) {
					varcharValues = append(varcharValues, "")
				} else {
					varcharValues = append(varcharValues, common.GenText(lang))
				}
			}
		} else {
			// Default behavior: sequential numbers
			for i := start; i < start+validDataLen; i++ {
				varcharValues = append(varcharValues, strconv.Itoa(i))
			}
		}
		if len(option.texts) > 0 {
			// Replace part of varcharValues with texts from option
			for i := 0; i < len(option.texts) && i < len(varcharValues); i++ {
				varcharValues[i] = option.texts[i]
			}
		}
		if validDataLen < nb {
			nullableColumn, err := column.NewNullableColumnVarChar(fieldName, varcharValues, option.validData)
			if err != nil {
				log.Fatal("NewNullableColumnVarChar failed", zap.Error(err))
			}
			return nullableColumn
		}
		return column.NewColumnVarChar(fieldName, varcharValues)

	case entity.FieldTypeArray:
		return GenArrayColumnData(validDataLen, option.elementType, option)

	case entity.FieldTypeJSON:
		jsonValues := GenDefaultJSONData(validDataLen, option)
		if validDataLen < nb {
			nullableColumn, err := column.NewNullableColumnJSONBytes(fieldName, jsonValues, option.validData)
			if err != nil {
				log.Fatal("NewNullableColumnJSONBytes failed", zap.Error(err))
			}
			return nullableColumn
		}
		return column.NewColumnJSONBytes(fieldName, jsonValues)

	case entity.FieldTypeGeometry:
		geometryValues := GenDefaultGeometryData(validDataLen, option)
		if validDataLen < nb {
			nullableColumn, err := column.NewNullableColumnGeometryWKT(fieldName, geometryValues, option.validData)
			if err != nil {
				log.Fatal("NewNullableColumnGeometryWKT failed", zap.Error(err))
			}
			return nullableColumn
		}
		return column.NewColumnGeometryWKT(fieldName, geometryValues)

	case entity.FieldTypeFloatVector:
		if validDataLen < nb {
			log.Warn("GenColumnData", zap.String("Note", "fieldType FloatVector not support valid data"))
		}
		vecFloatValues := make([][]float32, 0, nb)
		for i := start; i < start+nb; i++ {
			vec := common.GenFloatVector(dim)
			vecFloatValues = append(vecFloatValues, vec)
		}
		return column.NewColumnFloatVector(fieldName, option.dim, vecFloatValues)

	case entity.FieldTypeBinaryVector:
		if validDataLen < nb {
			log.Warn("GenColumnData", zap.String("Note", "fieldType FloatVector not support valid data"))
		}
		binaryVectors := make([][]byte, 0, nb)
		for i := 0; i < nb; i++ {
			vec := common.GenBinaryVector(dim)
			binaryVectors = append(binaryVectors, vec)
		}
		return column.NewColumnBinaryVector(fieldName, dim, binaryVectors)
	case entity.FieldTypeFloat16Vector:
		if validDataLen < nb {
			log.Warn("GenColumnData", zap.String("Note", "fieldType FloatVector not support valid data"))
		}
		fp16Vectors := make([][]byte, 0, nb)
		for i := start; i < start+nb; i++ {
			vec := common.GenFloat16Vector(dim)
			fp16Vectors = append(fp16Vectors, vec)
		}
		return column.NewColumnFloat16Vector(fieldName, dim, fp16Vectors)

	case entity.FieldTypeBFloat16Vector:
		if validDataLen < nb {
			log.Warn("GenColumnData", zap.String("Note", "fieldType FloatVector not support valid data"))
		}
		bf16Vectors := make([][]byte, 0, nb)
		for i := start; i < start+nb; i++ {
			vec := common.GenBFloat16Vector(dim)
			bf16Vectors = append(bf16Vectors, vec)
		}
		return column.NewColumnBFloat16Vector(fieldName, dim, bf16Vectors)

	case entity.FieldTypeSparseVector:
		if validDataLen < nb {
			log.Warn("GenColumnData", zap.String("Note", "fieldType FloatVector not support valid data"))
		}
		vectors := make([]entity.SparseEmbedding, 0, nb)
		for i := start; i < start+nb; i++ {
			vec := common.GenSparseVector(sparseMaxLen)
			vectors = append(vectors, vec)
		}
		return column.NewColumnSparseVectors(fieldName, vectors)

	default:
		log.Fatal("GenColumnData failed", zap.Any("FieldType", fieldType))
		return nil
	}
}

func GenColumnDataWithFp32VecConversion(nb int, fieldType entity.FieldType, option GenDataOption) column.Column {
	dim := option.dim
	start := option.start
	fieldName := option.fieldName
	if option.fieldName == "" {
		fieldName = GetFieldNameByFieldType(fieldType, TWithElementType(option.elementType))
	}
	switch fieldType {
	case entity.FieldTypeFloat16Vector:
		fp16Vectors := make([][]byte, 0, nb)
		for i := start; i < start+nb; i++ {
			vec := entity.FloatVector(common.GenFloatVector(dim)).ToFloat16Vector()
			fp16Vectors = append(fp16Vectors, vec)
		}
		return column.NewColumnFloat16Vector(fieldName, dim, fp16Vectors)

	case entity.FieldTypeBFloat16Vector:
		bf16Vectors := make([][]byte, 0, nb)
		for i := start; i < start+nb; i++ {
			vec := entity.FloatVector(common.GenFloatVector(dim)).ToBFloat16Vector()
			bf16Vectors = append(bf16Vectors, vec)
		}
		return column.NewColumnBFloat16Vector(fieldName, dim, bf16Vectors)

	default:
		log.Fatal("GenFp16OrBf16ColumnDataFromFloatVector failed", zap.Any("FieldType", fieldType))
		return nil
	}
}

func GenDynamicColumnDataWithOption(option GenDataOption) []column.Column {
	return GenDynamicColumnData(option.start, option.nb)
}

func GenDynamicColumnData(start int, nb int) []column.Column {
	type ListStruct struct {
		List []int64 `json:"list" milvus:"name:list"`
	}

	// gen number, string bool list data column
	numberValues := make([]int32, 0, nb)
	stringValues := make([]string, 0, nb)
	boolValues := make([]bool, 0, nb)
	listValues := make([][]byte, 0, nb)
	m := make(map[string]interface{})
	for i := start; i < start+nb; i++ {
		numberValues = append(numberValues, int32(i))
		stringValues = append(stringValues, strconv.Itoa(i))
		boolValues = append(boolValues, i%3 == 0)
		m["list"] = ListStruct{
			List: []int64{int64(i), int64(i + 1)},
		}
		bs, err := json.Marshal(m)
		if err != nil {
			log.Fatal("Marshal json field failed:", zap.Error(err))
		}
		listValues = append(listValues, bs)
	}
	data := []column.Column{
		column.NewColumnInt32(common.DefaultDynamicNumberField, numberValues),
		column.NewColumnString(common.DefaultDynamicStringField, stringValues),
		column.NewColumnBool(common.DefaultDynamicBoolField, boolValues),
		column.NewColumnJSONBytes(common.DefaultDynamicListField, listValues),
	}
	return data
}

func MergeColumnsToDynamic(nb int, columns []column.Column, columnName string) *column.ColumnJSONBytes {
	values := make([][]byte, 0, nb)
	for i := 0; i < nb; i++ {
		m := make(map[string]interface{})
		for _, c := range columns {
			// range guaranteed
			m[c.Name()], _ = c.Get(i)
		}
		bs, err := json.Marshal(&m)
		if err != nil {
			log.Fatal("MergeColumnsToDynamic failed:", zap.Error(err))
		}
		values = append(values, bs)
	}
	jsonColumn := column.NewColumnJSONBytes(columnName, values).WithIsDynamic(true)

	return jsonColumn
}

// GenTextDocuments generates realistic text documents for embedding tests
func GenTextDocuments(count int, lang string) []string {
	documents := make([]string, count)

	var templates []string
	switch lang {
	case "english", "en":
		templates = []string{
			"This is a document about artificial intelligence and machine learning technologies in modern computing systems",
			"Vector databases enable efficient similarity search for high-dimensional data in AI applications",
			"Text embeddings transform natural language into numerical representations for semantic understanding",
			"Information retrieval systems help users find relevant documents from large collections of data",
			"Natural language processing enables computers to understand and generate human language effectively",
			"Database management systems provide structured storage and efficient querying of information",
			"Search algorithms rank and retrieve the most relevant results for user queries",
			"Machine learning models learn patterns from data to make predictions and classifications",
			"Deep learning neural networks process complex patterns in images, text, and other data types",
			"Data science combines statistics, programming, and domain knowledge to extract insights",
		}
	case "chinese", "zh":
		templates = []string{
			"这是关于人工智能和机器学习技术的文档，介绍现代计算系统中的应用",
			"向量数据库为高维数据提供高效的相似性搜索功能，支持AI应用开发",
			"文本嵌入技术将自然语言转换为数值表示，实现语义理解和分析",
			"信息检索系统帮助用户从大规模数据集合中找到相关的文档内容",
			"自然语言处理技术使计算机能够理解和生成人类语言",
			"数据库管理系统提供结构化存储和高效的信息查询功能",
			"搜索算法对用户查询结果进行排序和检索，返回最相关的内容",
			"机器学习模型从数据中学习模式，进行预测和分类任务",
			"深度学习神经网络处理图像、文本等复杂数据类型中的模式",
			"数据科学结合统计学、编程和领域知识来提取有价值的洞察",
		}
	default:
		// Default to English
		templates = []string{
			"Document about technology and innovation in the digital age",
			"Analysis of modern computing systems and their applications",
			"Research on data processing and information management",
			"Study of algorithms and their implementation in software",
			"Overview of database systems and their optimization techniques",
		}
	}

	for i := 0; i < count; i++ {
		baseTemplate := templates[i%len(templates)]
		documents[i] = fmt.Sprintf("%s. Document ID: %d", baseTemplate, i)
	}

	return documents
}

// CosineSimilarity calculates cosine similarity between two float32 vectors
func CosineSimilarity(a, b []float32) float32 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}

	var dotProduct, normA, normB float32
	for i := 0; i < len(a); i++ {
		dotProduct += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	if normA == 0 || normB == 0 {
		return 0
	}

	// Use math.Sqrt for more accurate calculation
	return dotProduct / (float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB))))
}

// GenLongText generates long text with specified word count
func GenLongText(wordCount int, lang string) string {
	var words []string
	switch lang {
	case "chinese", "zh":
		words = []string{"人工智能", "机器学习", "深度学习", "神经网络", "数据挖掘", "自然语言", "处理技术", "计算机", "算法优化", "信息检索", "向量数据库", "语义搜索", "文本分析", "知识图谱", "智能系统"}
	case "english", "en":
		words = []string{"artificial", "intelligence", "machine", "learning", "deep", "neural", "network", "algorithm", "database", "search", "vector", "embedding", "semantic", "analysis", "information", "retrieval", "computing", "technology", "system", "data", "processing", "optimization", "performance", "scalability", "efficiency"}
	default:
		words = []string{"the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog", "and", "runs", "through", "forest", "with", "great", "speed", "while", "chasing", "rabbit", "under", "bright", "moonlight", "across", "green", "fields", "toward", "distant", "mountains"}
	}

	result := make([]string, wordCount)
	for i := 0; i < wordCount; i++ {
		result[i] = words[i%len(words)]
	}

	return strings.Join(result, " ")
}

// CallTEIDirectly calls TEI endpoint directly to get embeddings
func CallTEIDirectly(endpoint string, texts []string) ([][]float32, error) {
	// TEI API request structure
	type TEIRequest struct {
		Inputs []string `json:"inputs"`
	}

	// Create request
	reqBody := TEIRequest{Inputs: texts}
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Make HTTP request to TEI
	resp, err := http.Post(endpoint+"/embed", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to call TEI endpoint: %w", err)
	}
	defer resp.Body.Close()

	// Read response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Parse response - TEI returns array of arrays
	var embeddings [][]float32
	if err := json.Unmarshal(body, &embeddings); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return embeddings, nil
}

// Column-level option system
type ColumnOption struct {
	FieldName string
	Options   *GenDataOption
}

type ColumnOptions []ColumnOption

func TNewColumnOptions() ColumnOptions {
	return make(ColumnOptions, 0)
}

func (cos ColumnOptions) WithColumnOption(fieldName string, options *GenDataOption) ColumnOptions {
	return append(cos, ColumnOption{
		FieldName: fieldName,
		Options:   options,
	})
}

func (cos ColumnOptions) GetColumnOption(fieldName string) *GenDataOption {
	for _, co := range cos {
		if co.FieldName == fieldName {
			return co.Options
		}
	}
	return nil
}

// GenColumnsBasedSchema generates columns based on schema with field-specific options
func GenColumnsBasedSchema(schema *entity.Schema, columnOpts ColumnOptions) ([]column.Column, []column.Column) {
	if nil == schema || schema.CollectionName == "" {
		log.Fatal("[GenColumnsBasedSchema] Nil Schema is not expected")
	}
	fields := schema.Fields
	columns := make([]column.Column, 0, len(fields)+1)
	var dynamicColumns []column.Column

	for _, field := range fields {
		if field.AutoID {
			continue
		}
		if slices.Contains(GetAllFunctionsOutputFields(schema), field.Name) {
			continue
		}

		// Get field-specific options
		fieldOpt := columnOpts.GetColumnOption(field.Name)
		if fieldOpt == nil {
			fieldOpt = TNewDataOption()
		}

		// Set field name and element type if needed
		fieldOpt.fieldName = field.Name
		if field.DataType == entity.FieldTypeArray {
			fieldOpt.elementType = field.ElementType
		}

		columns = append(columns, GenColumnData(fieldOpt.nb, field.DataType, *fieldOpt))
	}

	// Check if dynamic field is enabled
	if schema.EnableDynamicField {
		// Use default options for dynamic columns
		dynamicOpt := columnOpts.GetColumnOption(common.DefaultDynamicFieldName)
		if dynamicOpt == nil {
			dynamicOpt = TNewDataOption()
		}
		dynamicColumns = GenDynamicColumnData(dynamicOpt.start, dynamicOpt.nb)
	}

	return columns, dynamicColumns
}

func GenColumnsBasedSchemaWithFp32VecConversion(schema *entity.Schema, option *GenDataOption) ([]column.Column, []column.Column) {
	if nil == schema || schema.CollectionName == "" {
		log.Fatal("[GenColumnsBasedSchema] Nil Schema is not expected")
	}
	fields := schema.Fields
	columns := make([]column.Column, 0, len(fields)+1)
	var dynamicColumns []column.Column
	for _, field := range fields {
		if field.DataType == entity.FieldTypeArray {
			option.TWithElementType(field.ElementType)
		}
		if field.AutoID {
			continue
		}
		if field.DataType == entity.FieldTypeFloat16Vector || field.DataType == entity.FieldTypeBFloat16Vector {
			columns = append(columns, GenColumnDataWithFp32VecConversion(option.nb, field.DataType, *option))
		} else {
			columns = append(columns, GenColumnData(option.nb, field.DataType, *option))
		}
	}
	if schema.EnableDynamicField {
		dynamicColumns = GenDynamicColumnData(option.start, option.nb)
	}
	return columns, dynamicColumns
}

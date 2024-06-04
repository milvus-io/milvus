package helper

import (
	"bytes"
	"strconv"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/tests/go_client/common"
	"go.uber.org/zap"
)

// insert params
type InsertParams struct {
	Schema        *entity.Schema
	PartitionName string
	Start         int
	Nb            int
	IsRows        bool
}

func NewInsertParams(schema *entity.Schema, nb int) *InsertParams {
	return &InsertParams{
		Schema: schema,
		Nb:     nb,
	}
}

func (opt *InsertParams) TWithPartitionName(partitionName string) *InsertParams {
	opt.PartitionName = partitionName
	return opt
}

func (opt *InsertParams) TWithStart(start int) *InsertParams {
	opt.Start = start
	return opt
}

func (opt *InsertParams) TWithIsRows(isRows bool) *InsertParams {
	opt.IsRows = isRows
	return opt
}

// GenColumnDataOption -- create column data --
type GenColumnOption struct {
	dim         int64
	maxLen      int64
	start       int
	fieldName   string
	elementType entity.FieldType
}

func (opt *GenColumnOption) TWithDim(dim int64) *GenColumnOption {
	opt.dim = dim
	return opt
}

func (opt *GenColumnOption) TWithMaxLen(maxLen int64) *GenColumnOption {
	opt.maxLen = maxLen
	return opt
}

func (opt *GenColumnOption) TWithStart(start int) *GenColumnOption {
	opt.start = start
	return opt
}

func (opt *GenColumnOption) TWithFieldName(fieldName string) *GenColumnOption {
	opt.fieldName = fieldName
	return opt
}

func (opt *GenColumnOption) TWithElementType(eleType entity.FieldType) *GenColumnOption {
	opt.elementType = eleType
	return opt
}

func TNewColumnOption() *GenColumnOption {
	return &GenColumnOption{
		dim:    common.DefaultDim,
		maxLen: common.TestMaxLen,
		start:  0,
	}
}

func GenArrayColumnData(nb int, eleType entity.FieldType, option GenColumnOption) column.Column {
	start := option.start
	fieldName := option.fieldName
	if option.fieldName == "" {
		fieldName = GetFieldNameByElementType(eleType)
	}
	capacity := int(option.maxLen)
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
		return column.NewColumnDoubleArray(fieldName, doubleValues)
	case entity.FieldTypeVarChar:
		varcharValues := make([][][]byte, 0, nb)
		for i := start; i < start+nb; i++ {
			varcharArray := make([][]byte, 0, capacity)
			for j := 0; j < capacity; j++ {
				var buf bytes.Buffer
				buf.WriteString(strconv.Itoa(i + j))
				varcharArray = append(varcharArray, buf.Bytes())
			}
			varcharValues = append(varcharValues, varcharArray)
		}
		return column.NewColumnVarCharArray(fieldName, varcharValues)
	default:
		log.Fatal("GenArrayColumnData failed", zap.Any("ElementType", eleType))
		return nil
	}
}

// GenColumnData GenColumnDataOption
func GenColumnData(nb int, fieldType entity.FieldType, option GenColumnOption) column.Column {
	dim := int(option.dim)
	maxLen := int(option.maxLen)
	start := option.start
	fieldName := option.fieldName
	if option.fieldName == "" {
		fieldName = GetFieldNameByFieldType(fieldType, option.elementType)
	}
	switch fieldType {
	case entity.FieldTypeInt64:
		int64Values := make([]int64, 0, nb)
		for i := start; i < start+nb; i++ {
			int64Values = append(int64Values, int64(i))
		}
		return column.NewColumnInt64(fieldName, int64Values)

	case entity.FieldTypeInt8:
		int8Values := make([]int8, 0, nb)
		for i := start; i < start+nb; i++ {
			int8Values = append(int8Values, int8(i))
		}
		return column.NewColumnInt8(fieldName, int8Values)

	case entity.FieldTypeInt16:
		int16Values := make([]int16, 0, nb)
		for i := start; i < start+nb; i++ {
			int16Values = append(int16Values, int16(i))
		}
		return column.NewColumnInt16(fieldName, int16Values)

	case entity.FieldTypeInt32:
		int32Values := make([]int32, 0, nb)
		for i := start; i < start+nb; i++ {
			int32Values = append(int32Values, int32(i))
		}
		return column.NewColumnInt32(fieldName, int32Values)

	case entity.FieldTypeBool:
		boolValues := make([]bool, 0, nb)
		for i := start; i < start+nb; i++ {
			boolValues = append(boolValues, i/2 == 0)
		}
		return column.NewColumnBool(fieldName, boolValues)

	case entity.FieldTypeFloat:
		floatValues := make([]float32, 0, nb)
		for i := start; i < start+nb; i++ {
			floatValues = append(floatValues, float32(i))
		}
		return column.NewColumnFloat(fieldName, floatValues)

	case entity.FieldTypeDouble:
		floatValues := make([]float64, 0, nb)
		for i := start; i < start+nb; i++ {
			floatValues = append(floatValues, float64(i))
		}
		return column.NewColumnDouble(fieldName, floatValues)

	case entity.FieldTypeVarChar:
		varcharValues := make([]string, 0, nb)
		for i := start; i < start+nb; i++ {
			varcharValues = append(varcharValues, strconv.Itoa(i))
		}
		return column.NewColumnVarChar(fieldName, varcharValues)

	case entity.FieldTypeArray:
		return GenArrayColumnData(nb, option.elementType, option)

	case entity.FieldTypeFloatVector:
		vecFloatValues := make([][]float32, 0, nb)
		for i := start; i < start+nb; i++ {
			vec := common.GenFloatVector(dim)
			vecFloatValues = append(vecFloatValues, vec)
		}
		return column.NewColumnFloatVector(fieldName, int(option.dim), vecFloatValues)
	case entity.FieldTypeBinaryVector:
		binaryVectors := make([][]byte, 0, nb)
		for i := 0; i < nb; i++ {
			vec := common.GenBinaryVector(dim)
			binaryVectors = append(binaryVectors, vec)
		}
		return column.NewColumnBinaryVector(fieldName, dim, binaryVectors)
	case entity.FieldTypeFloat16Vector:
		fp16Vectors := make([][]byte, 0, nb)
		for i := start; i < start+nb; i++ {
			vec := common.GenFloat16Vector(dim)
			fp16Vectors = append(fp16Vectors, vec)
		}
		return column.NewColumnFloat16Vector(fieldName, dim, fp16Vectors)
	case entity.FieldTypeBFloat16Vector:
		bf16Vectors := make([][]byte, 0, nb)
		for i := start; i < start+nb; i++ {
			vec := common.GenBFloat16Vector(dim)
			bf16Vectors = append(bf16Vectors, vec)
		}
		return column.NewColumnBFloat16Vector(fieldName, dim, bf16Vectors)
	case entity.FieldTypeSparseVector:
		vectors := make([]entity.SparseEmbedding, 0, nb)
		for i := start; i < start+nb; i++ {
			vec := common.GenSparseVector(maxLen)
			vectors = append(vectors, vec)
		}
		return column.NewColumnSparseVectors(fieldName, vectors)
	default:
		log.Fatal("GenColumnData failed", zap.Any("FieldType", fieldType))
		return nil
	}
}

func GenDynamicFieldData(start int, nb int) []column.Column {
	type ListStruct struct {
		List []int64 `json:"list" milvus:"name:list"`
	}

	// gen number, string bool list data column
	numberValues := make([]int32, 0, nb)
	stringValues := make([]string, 0, nb)
	boolValues := make([]bool, 0, nb)
	//listValues := make([][]byte, 0, Nb)
	//m := make(map[string]interface{})
	for i := start; i < start+nb; i++ {
		numberValues = append(numberValues, int32(i))
		stringValues = append(stringValues, strconv.Itoa(i))
		boolValues = append(boolValues, i%3 == 0)
		//m["list"] = ListStruct{
		//	List: []int64{int64(i), int64(i + 1)},
		//}
		//bs, err := json.Marshal(m)
		//if err != nil {
		//	log.Fatalf("Marshal json field failed: %s", err)
		//}
		//listValues = append(listValues, bs)
	}
	data := []column.Column{
		column.NewColumnInt32(common.DefaultDynamicNumberField, numberValues),
		column.NewColumnString(common.DefaultDynamicStringField, stringValues),
		column.NewColumnBool(common.DefaultDynamicBoolField, boolValues),
		//entity.NewColumnJSONBytes(DefaultDynamicListField, listValues),
	}
	return data
}

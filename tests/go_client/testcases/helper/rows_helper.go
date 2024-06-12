package helper

import (
	"bytes"
	"strconv"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/tests/go_client/common"
)

type Dynamic struct {
	Number int32  `json:"dynamicNumber,omitempty" milvus:"name:dynamicNumber"`
	String string `json:"dynamicString,omitempty" milvus:"name:dynamicString"`
	*BoolDynamic
	List []int64 `json:"dynamicList,omitempty" milvus:"name:dynamicList"`
}

type BaseRow struct {
	*BoolStruct
	Int8      int8                   `json:"int8,omitempty" milvus:"name:int8"`
	Int16     int16                  `json:"int16,omitempty" milvus:"name:int16"`
	Int32     int32                  `json:"int32,omitempty" milvus:"name:int32"`
	Int64     int64                  `json:"int64,omitempty" milvus:"name:int64"`
	Float     float32                `json:"float,omitempty" milvus:"name:float"`
	Double    float64                `json:"double,omitempty" milvus:"name:double"`
	Varchar   string                 `json:"varchar,omitempty" milvus:"name:varchar"`
	JSON      *JSONStruct            `json:"json,omitempty" milvus:"name:json"`
	FloatVec  []float32              `json:"floatVec,omitempty" milvus:"name:floatVec"`
	Fp16Vec   []byte                 `json:"fp16Vec,omitempty" milvus:"name:fp16Vec"`
	Bf16Vec   []byte                 `json:"bf16Vec,omitempty" milvus:"name:bf16Vec"`
	BinaryVec []byte                 `json:"binaryVec,omitempty" milvus:"name:binaryVec"`
	SparseVec entity.SparseEmbedding `json:"sparseVec,omitempty" milvus:"name:sparseVec"`
	Array
	Dynamic
}

type BoolStruct struct {
	Bool bool `json:"bool" milvus:"name:bool"`
}

type BoolDynamic struct {
	Bool bool `json:"dynamicBool" milvus:"name:dynamicBool"`
}

type Array struct {
	BoolArray    []bool    `json:"boolArray,omitempty" milvus:"name:boolArray"`
	Int8Array    []int8    `json:"int8Array,omitempty" milvus:"name:int8Array"`
	Int16Array   []int16   `json:"int16Array,omitempty" milvus:"name:int16Array"`
	Int32Array   []int32   `json:"int32Array,omitempty" milvus:"name:int32Array"`
	Int64Array   []int64   `json:"int64Array,omitempty" milvus:"name:int64Array"`
	FloatArray   []float32 `json:"floatArray,omitempty" milvus:"name:floatArray"`
	DoubleArray  []float64 `json:"doubleArray,omitempty" milvus:"name:doubleArray"`
	VarcharArray [][]byte  `json:"varcharArray,omitempty" milvus:"name:varcharArray"`
}

func getBool(b bool) *bool {
	return &b
}

func GenDynamicRow(index int) Dynamic {
	var dynamic Dynamic
	_bool := &BoolDynamic{
		Bool: index%2 == 0,
	}
	if index%2 == 0 {
		dynamic = Dynamic{
			Number:      int32(index),
			String:      strconv.Itoa(index),
			BoolDynamic: _bool,
		}
	} else {
		dynamic = Dynamic{
			Number:      int32(index),
			String:      strconv.Itoa(index),
			BoolDynamic: _bool,
			List:        []int64{int64(index), int64(index + 1)},
		}
	}
	return dynamic
}

func GenJsonRow(index int) *JSONStruct {
	var jsonStruct JSONStruct
	_bool := &BoolStruct{
		Bool: index%2 == 0,
	}
	if index%2 == 0 {
		jsonStruct = JSONStruct{
			String:     strconv.Itoa(index),
			BoolStruct: _bool,
		}
	} else {
		jsonStruct = JSONStruct{
			Number:     int32(index),
			String:     strconv.Itoa(index),
			BoolStruct: _bool,
			List:       []int64{int64(index), int64(index + 1)},
		}
	}
	return &jsonStruct
}

func GenInt64VecRows(nb int, enableDynamicField bool, autoID bool, option GenDataOption) []interface{} {
	dim := option.dim
	start := option.start

	rows := make([]interface{}, 0, nb)

	// BaseRow generate insert rows
	for i := start; i < start+nb; i++ {
		baseRow := BaseRow{
			FloatVec: common.GenFloatVector(dim),
		}
		if !autoID {
			baseRow.Int64 = int64(i + 1)
		}
		if enableDynamicField {
			baseRow.Dynamic = GenDynamicRow(i + 1)
		}
		rows = append(rows, &baseRow)
	}
	return rows
}

func GenInt64VarcharSparseRows(nb int, enableDynamicField bool, autoID bool, option GenDataOption) []interface{} {
	start := option.start

	rows := make([]interface{}, 0, nb)

	// BaseRow generate insert rows
	for i := start; i < start+nb; i++ {
		vec := common.GenSparseVector(2)
		//log.Info("", zap.Any("SparseVec", vec))
		baseRow := BaseRow{
			Varchar:   strconv.Itoa(i + 1),
			SparseVec: vec,
		}
		if !autoID {
			baseRow.Int64 = int64(i + 1)
		}
		if enableDynamicField {
			baseRow.Dynamic = GenDynamicRow(i + 1)
		}
		rows = append(rows, &baseRow)
	}
	return rows
}

func GenAllFieldsRows(nb int, enableDynamicField bool, option GenDataOption) []interface{} {
	rows := make([]interface{}, 0, nb)

	// BaseRow generate insert rows
	dim := option.dim
	start := option.start

	for i := start; i < start+nb; i++ {
		_bool := &BoolStruct{
			Bool: i%2 == 0,
		}
		baseRow := BaseRow{
			Int64:      int64(i + 1),
			BoolStruct: _bool,
			Int8:       int8(i + 1),
			Int16:      int16(i + 1),
			Int32:      int32(i + 1),
			Float:      float32(i + 1),
			Double:     float64(i + 1),
			Varchar:    strconv.Itoa(i + 1),
			JSON:       GenJsonRow(i + 1),
			FloatVec:   common.GenFloatVector(dim),
			Fp16Vec:    common.GenFloat16Vector(dim),
			Bf16Vec:    common.GenBFloat16Vector(dim),
			BinaryVec:  common.GenBinaryVector(dim),
		}
		baseRow.Array = GenAllArrayRow(i, option)
		if enableDynamicField {
			baseRow.Dynamic = GenDynamicRow(i + 1)
		}
		rows = append(rows, &baseRow)
	}
	return rows
}

func GenAllArrayRow(index int, option GenDataOption) Array {
	capacity := option.maxCapacity
	boolRow := make([]bool, 0, capacity)
	int8Row := make([]int8, 0, capacity)
	int16Row := make([]int16, 0, capacity)
	int32Row := make([]int32, 0, capacity)
	int64Row := make([]int64, 0, capacity)
	floatRow := make([]float32, 0, capacity)
	doubleRow := make([]float64, 0, capacity)
	varcharRow := make([][]byte, 0, capacity)
	for j := 0; j < capacity; j++ {
		boolRow = append(boolRow, index%2 == 0)
		int8Row = append(int8Row, int8(index+j))
		int16Row = append(int16Row, int16(index+j))
		int32Row = append(int32Row, int32(index+j))
		int64Row = append(int64Row, int64(index+j))
		floatRow = append(floatRow, float32(index+j))
		doubleRow = append(doubleRow, float64(index+j))
		var buf bytes.Buffer
		buf.WriteString(strconv.Itoa(index + j))
		varcharRow = append(varcharRow, buf.Bytes())
	}
	arrayRow := Array{
		BoolArray:    boolRow,
		Int8Array:    int8Row,
		Int16Array:   int16Row,
		Int32Array:   int32Row,
		Int64Array:   int64Row,
		FloatArray:   floatRow,
		DoubleArray:  doubleRow,
		VarcharArray: varcharRow,
	}
	return arrayRow
}

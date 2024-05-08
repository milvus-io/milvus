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

package importv2

import (
	"context"
	rand2 "crypto/rand"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/parquet"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
	"github.com/samber/lo"
	"github.com/sbinet/npyio"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/storage"
	pq "github.com/milvus-io/milvus/internal/util/importutilv2/parquet"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/testutils"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

const dim = 128

func createInsertData(t *testing.T, schema *schemapb.CollectionSchema, rowCount int) *storage.InsertData {
	insertData, err := storage.NewInsertData(schema)
	assert.NoError(t, err)
	for _, field := range schema.GetFields() {
		if field.GetAutoID() {
			continue
		}
		switch field.GetDataType() {
		case schemapb.DataType_Bool:
			boolData := make([]bool, 0)
			for i := 0; i < rowCount; i++ {
				boolData = append(boolData, i%3 != 0)
			}
			insertData.Data[field.GetFieldID()] = &storage.BoolFieldData{Data: boolData}
		case schemapb.DataType_Float:
			floatData := make([]float32, 0)
			for i := 0; i < rowCount; i++ {
				floatData = append(floatData, float32(i/2))
			}
			insertData.Data[field.GetFieldID()] = &storage.FloatFieldData{Data: floatData}
		case schemapb.DataType_Double:
			doubleData := make([]float64, 0)
			for i := 0; i < rowCount; i++ {
				doubleData = append(doubleData, float64(i/5))
			}
			insertData.Data[field.GetFieldID()] = &storage.DoubleFieldData{Data: doubleData}
		case schemapb.DataType_Int8:
			int8Data := make([]int8, 0)
			for i := 0; i < rowCount; i++ {
				int8Data = append(int8Data, int8(i%256))
			}
			insertData.Data[field.GetFieldID()] = &storage.Int8FieldData{Data: int8Data}
		case schemapb.DataType_Int16:
			int16Data := make([]int16, 0)
			for i := 0; i < rowCount; i++ {
				int16Data = append(int16Data, int16(i%65536))
			}
			insertData.Data[field.GetFieldID()] = &storage.Int16FieldData{Data: int16Data}
		case schemapb.DataType_Int32:
			int32Data := make([]int32, 0)
			for i := 0; i < rowCount; i++ {
				int32Data = append(int32Data, int32(i%1000))
			}
			insertData.Data[field.GetFieldID()] = &storage.Int32FieldData{Data: int32Data}
		case schemapb.DataType_Int64:
			int64Data := make([]int64, 0)
			for i := 0; i < rowCount; i++ {
				int64Data = append(int64Data, int64(i))
			}
			insertData.Data[field.GetFieldID()] = &storage.Int64FieldData{Data: int64Data}
		case schemapb.DataType_BinaryVector:
			dim, err := typeutil.GetDim(field)
			assert.NoError(t, err)
			binVecData := make([]byte, 0)
			total := rowCount * int(dim) / 8
			for i := 0; i < total; i++ {
				binVecData = append(binVecData, byte(i%256))
			}
			insertData.Data[field.GetFieldID()] = &storage.BinaryVectorFieldData{Data: binVecData, Dim: int(dim)}
		case schemapb.DataType_FloatVector:
			dim, err := typeutil.GetDim(field)
			assert.NoError(t, err)
			floatVecData := make([]float32, 0)
			total := rowCount * int(dim)
			for i := 0; i < total; i++ {
				floatVecData = append(floatVecData, rand.Float32())
			}
			insertData.Data[field.GetFieldID()] = &storage.FloatVectorFieldData{Data: floatVecData, Dim: int(dim)}
		case schemapb.DataType_Float16Vector:
			dim, err := typeutil.GetDim(field)
			assert.NoError(t, err)
			total := int64(rowCount) * dim * 2
			float16VecData := make([]byte, total)
			_, err = rand2.Read(float16VecData)
			assert.NoError(t, err)
			insertData.Data[field.GetFieldID()] = &storage.Float16VectorFieldData{Data: float16VecData, Dim: int(dim)}
		case schemapb.DataType_BFloat16Vector:
			dim, err := typeutil.GetDim(field)
			assert.NoError(t, err)
			total := int64(rowCount) * dim * 2
			bfloat16VecData := make([]byte, total)
			_, err = rand2.Read(bfloat16VecData)
			assert.NoError(t, err)
			insertData.Data[field.GetFieldID()] = &storage.BFloat16VectorFieldData{Data: bfloat16VecData, Dim: int(dim)}
		case schemapb.DataType_SparseFloatVector:
			sparseFloatVecData := testutils.GenerateSparseFloatVectors(rowCount)
			insertData.Data[field.GetFieldID()] = &storage.SparseFloatVectorFieldData{
				SparseFloatArray: *sparseFloatVecData,
			}
		case schemapb.DataType_String, schemapb.DataType_VarChar:
			varcharData := make([]string, 0)
			for i := 0; i < rowCount; i++ {
				varcharData = append(varcharData, strconv.Itoa(i))
			}
			insertData.Data[field.GetFieldID()] = &storage.StringFieldData{Data: varcharData}
		case schemapb.DataType_JSON:
			jsonData := make([][]byte, 0)
			for i := 0; i < rowCount; i++ {
				jsonData = append(jsonData, []byte(fmt.Sprintf("{\"y\": %d}", i)))
			}
			insertData.Data[field.GetFieldID()] = &storage.JSONFieldData{Data: jsonData}
		case schemapb.DataType_Array:
			arrayData := make([]*schemapb.ScalarField, 0)
			for i := 0; i < rowCount; i++ {
				arrayData = append(arrayData, &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{int32(i), int32(i + 1), int32(i + 2)},
						},
					},
				})
			}
			insertData.Data[field.GetFieldID()] = &storage.ArrayFieldData{Data: arrayData}
		default:
			panic(fmt.Sprintf("unexpected data type: %s", field.GetDataType().String()))
		}
	}
	return insertData
}

func randomString(length int) string {
	letterRunes := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, length)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func buildArrayData(dataType, elemType schemapb.DataType, dim, rows int) arrow.Array {
	mem := memory.NewGoAllocator()
	switch dataType {
	case schemapb.DataType_Bool:
		builder := array.NewBooleanBuilder(mem)
		for i := 0; i < rows; i++ {
			builder.Append(i%2 == 0)
		}
		return builder.NewBooleanArray()
	case schemapb.DataType_Int8:
		builder := array.NewInt8Builder(mem)
		for i := 0; i < rows; i++ {
			builder.Append(int8(i))
		}
		return builder.NewInt8Array()
	case schemapb.DataType_Int16:
		builder := array.NewInt16Builder(mem)
		for i := 0; i < rows; i++ {
			builder.Append(int16(i))
		}
		return builder.NewInt16Array()
	case schemapb.DataType_Int32:
		builder := array.NewInt32Builder(mem)
		for i := 0; i < rows; i++ {
			builder.Append(int32(i))
		}
		return builder.NewInt32Array()
	case schemapb.DataType_Int64:
		builder := array.NewInt64Builder(mem)
		for i := 0; i < rows; i++ {
			builder.Append(int64(i))
		}
		return builder.NewInt64Array()
	case schemapb.DataType_Float:
		builder := array.NewFloat32Builder(mem)
		for i := 0; i < rows; i++ {
			builder.Append(float32(i) * 0.1)
		}
		return builder.NewFloat32Array()
	case schemapb.DataType_Double:
		builder := array.NewFloat64Builder(mem)
		for i := 0; i < rows; i++ {
			builder.Append(float64(i) * 0.02)
		}
		return builder.NewFloat64Array()
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		builder := array.NewStringBuilder(mem)
		for i := 0; i < rows; i++ {
			builder.Append(randomString(10))
		}
		return builder.NewStringArray()
	case schemapb.DataType_BinaryVector:
		builder := array.NewListBuilder(mem, &arrow.Uint8Type{})
		offsets := make([]int32, 0, rows)
		valid := make([]bool, 0)
		rowBytes := dim / 8
		for i := 0; i < rowBytes*rows; i++ {
			builder.ValueBuilder().(*array.Uint8Builder).Append(uint8(i % 256))
		}
		for i := 0; i < rows; i++ {
			offsets = append(offsets, int32(rowBytes*i))
			valid = append(valid, true)
		}
		builder.AppendValues(offsets, valid)
		return builder.NewListArray()
	case schemapb.DataType_FloatVector:
		builder := array.NewListBuilder(mem, &arrow.Float32Type{})
		offsets := make([]int32, 0, rows)
		valid := make([]bool, 0, rows)
		for i := 0; i < dim*rows; i++ {
			builder.ValueBuilder().(*array.Float32Builder).Append(float32(i))
		}
		for i := 0; i < rows; i++ {
			offsets = append(offsets, int32(dim*i))
			valid = append(valid, true)
		}
		builder.AppendValues(offsets, valid)
		return builder.NewListArray()
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		builder := array.NewListBuilder(mem, &arrow.Uint8Type{})
		offsets := make([]int32, 0, rows)
		valid := make([]bool, 0)
		rowBytes := dim * 2
		for i := 0; i < rowBytes*rows; i++ {
			builder.ValueBuilder().(*array.Uint8Builder).Append(uint8(i % 256))
		}
		for i := 0; i < rows; i++ {
			offsets = append(offsets, int32(rowBytes*i))
			valid = append(valid, true)
		}
		builder.AppendValues(offsets, valid)
		return builder.NewListArray()
	case schemapb.DataType_SparseFloatVector:
		sparsefloatVecData := make([]byte, 0)
		builder := array.NewListBuilder(mem, &arrow.Uint8Type{})
		offsets := make([]int32, 0, rows+1)
		valid := make([]bool, 0, rows)
		vecData := testutils.GenerateSparseFloatVectors(rows)
		offsets = append(offsets, 0)
		for i := 0; i < rows; i++ {
			rowVecData := vecData.GetContents()[i]
			sparsefloatVecData = append(sparsefloatVecData, rowVecData...)
			offsets = append(offsets, offsets[i]+int32(len(rowVecData)))
			valid = append(valid, true)
		}
		builder.ValueBuilder().(*array.Uint8Builder).AppendValues(sparsefloatVecData, nil)
		builder.AppendValues(offsets, valid)
		return builder.NewListArray()
	case schemapb.DataType_JSON:
		builder := array.NewStringBuilder(mem)
		for i := 0; i < rows; i++ {
			builder.Append(fmt.Sprintf("{\"a\": \"%s\", \"b\": %d}", randomString(3), i))
		}
		return builder.NewStringArray()
	case schemapb.DataType_Array:
		offsets := make([]int32, 0, rows)
		valid := make([]bool, 0, rows)
		index := 0
		for i := 0; i < rows; i++ {
			index += i % 10
			offsets = append(offsets, int32(index))
			valid = append(valid, true)
		}
		switch elemType {
		case schemapb.DataType_Bool:
			builder := array.NewListBuilder(mem, &arrow.BooleanType{})
			valueBuilder := builder.ValueBuilder().(*array.BooleanBuilder)
			for i := 0; i < index; i++ {
				valueBuilder.Append(i%2 == 0)
			}
			builder.AppendValues(offsets, valid)
			return builder.NewListArray()
		case schemapb.DataType_Int8:
			builder := array.NewListBuilder(mem, &arrow.Int8Type{})
			valueBuilder := builder.ValueBuilder().(*array.Int8Builder)
			for i := 0; i < index; i++ {
				valueBuilder.Append(int8(i))
			}
			builder.AppendValues(offsets, valid)
			return builder.NewListArray()
		case schemapb.DataType_Int16:
			builder := array.NewListBuilder(mem, &arrow.Int16Type{})
			valueBuilder := builder.ValueBuilder().(*array.Int16Builder)
			for i := 0; i < index; i++ {
				valueBuilder.Append(int16(i))
			}
			builder.AppendValues(offsets, valid)
			return builder.NewListArray()
		case schemapb.DataType_Int32:
			builder := array.NewListBuilder(mem, &arrow.Int32Type{})
			valueBuilder := builder.ValueBuilder().(*array.Int32Builder)
			for i := 0; i < index; i++ {
				valueBuilder.Append(int32(i))
			}
			builder.AppendValues(offsets, valid)
			return builder.NewListArray()
		case schemapb.DataType_Int64:
			builder := array.NewListBuilder(mem, &arrow.Int64Type{})
			valueBuilder := builder.ValueBuilder().(*array.Int64Builder)
			for i := 0; i < index; i++ {
				valueBuilder.Append(int64(i))
			}
			builder.AppendValues(offsets, valid)
			return builder.NewListArray()
		case schemapb.DataType_Float:
			builder := array.NewListBuilder(mem, &arrow.Float32Type{})
			valueBuilder := builder.ValueBuilder().(*array.Float32Builder)
			for i := 0; i < index; i++ {
				valueBuilder.Append(float32(i) * 0.1)
			}
			builder.AppendValues(offsets, valid)
			return builder.NewListArray()
		case schemapb.DataType_Double:
			builder := array.NewListBuilder(mem, &arrow.Float64Type{})
			valueBuilder := builder.ValueBuilder().(*array.Float64Builder)
			for i := 0; i < index; i++ {
				valueBuilder.Append(float64(i) * 0.02)
			}
			builder.AppendValues(offsets, valid)
			return builder.NewListArray()
		case schemapb.DataType_VarChar, schemapb.DataType_String:
			builder := array.NewListBuilder(mem, &arrow.StringType{})
			valueBuilder := builder.ValueBuilder().(*array.StringBuilder)
			for i := 0; i < index; i++ {
				valueBuilder.Append(randomString(5) + "-" + fmt.Sprintf("%d", i))
			}
			builder.AppendValues(offsets, valid)
			return builder.NewListArray()
		}
	}
	return nil
}

func GenerateParquetFile(filePath string, schema *schemapb.CollectionSchema, numRows int) error {
	w, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return err
	}

	pqSchema, err := pq.ConvertToArrowSchema(schema)
	if err != nil {
		return err
	}
	fw, err := pqarrow.NewFileWriter(pqSchema, w, parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(int64(numRows))), pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}
	defer fw.Close()

	columns := make([]arrow.Array, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		if field.GetIsPrimaryKey() && field.GetAutoID() {
			continue
		}
		columnData := buildArrayData(field.DataType, field.ElementType, dim, numRows)
		columns = append(columns, columnData)
	}
	recordBatch := array.NewRecord(pqSchema, columns, int64(numRows))
	return fw.Write(recordBatch)
}

func GenerateNumpyFiles(cm storage.ChunkManager, schema *schemapb.CollectionSchema, rowCount int) (*internalpb.ImportFile, error) {
	paths := make([]string, 0)
	for _, field := range schema.GetFields() {
		if field.GetAutoID() && field.GetIsPrimaryKey() {
			continue
		}
		path := fmt.Sprintf("%s/%s.npy", cm.RootPath(), field.GetName())
		err := GenerateNumpyFile(path, rowCount, field.GetDataType())
		if err != nil {
			return nil, err
		}
		paths = append(paths, path)
	}
	return &internalpb.ImportFile{
		Paths: paths,
	}, nil
}

func GenerateNumpyFile(filePath string, rowCount int, dType schemapb.DataType) error {
	writeFn := func(path string, data interface{}) error {
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		defer f.Close()

		err = npyio.Write(f, data)
		if err != nil {
			return err
		}

		return nil
	}

	switch dType {
	case schemapb.DataType_Bool:
		boolData := make([]bool, 0)
		for i := 0; i < rowCount; i++ {
			boolData = append(boolData, i%3 != 0)
		}
		err := writeFn(filePath, boolData)
		if err != nil {
			return err
		}
	case schemapb.DataType_Float:
		floatData := make([]float32, 0)
		for i := 0; i < rowCount; i++ {
			floatData = append(floatData, float32(i/2))
		}
		err := writeFn(filePath, floatData)
		if err != nil {
			return err
		}
	case schemapb.DataType_Double:
		doubleData := make([]float64, 0)
		for i := 0; i < rowCount; i++ {
			doubleData = append(doubleData, float64(i/5))
		}
		err := writeFn(filePath, doubleData)
		if err != nil {
			return err
		}
	case schemapb.DataType_Int8:
		int8Data := make([]int8, 0)
		for i := 0; i < rowCount; i++ {
			int8Data = append(int8Data, int8(i%256))
		}
		err := writeFn(filePath, int8Data)
		if err != nil {
			return err
		}
	case schemapb.DataType_Int16:
		int16Data := make([]int16, 0)
		for i := 0; i < rowCount; i++ {
			int16Data = append(int16Data, int16(i%65536))
		}
		err := writeFn(filePath, int16Data)
		if err != nil {
			return err
		}
	case schemapb.DataType_Int32:
		int32Data := make([]int32, 0)
		for i := 0; i < rowCount; i++ {
			int32Data = append(int32Data, int32(i%1000))
		}
		err := writeFn(filePath, int32Data)
		if err != nil {
			return err
		}
	case schemapb.DataType_Int64:
		int64Data := make([]int64, 0)
		for i := 0; i < rowCount; i++ {
			int64Data = append(int64Data, int64(i))
		}
		err := writeFn(filePath, int64Data)
		if err != nil {
			return err
		}
	case schemapb.DataType_BinaryVector:
		const rowBytes = dim / 8
		binVecData := make([][rowBytes]byte, 0, rowCount)
		for i := 0; i < rowCount; i++ {
			vec := [rowBytes]byte{}
			for j := 0; j < rowBytes; j++ {
				vec[j] = byte((i + j) % 256)
			}
			binVecData = append(binVecData, vec)
		}
		err := writeFn(filePath, binVecData)
		if err != nil {
			return err
		}
	case schemapb.DataType_FloatVector:
		data := make([][dim]float32, 0, rowCount)
		for i := 0; i < rowCount; i++ {
			vec := [dim]float32{}
			for j := 0; j < dim; j++ {
				vec[j] = rand.Float32()
			}
			data = append(data, vec)
		}
		err := writeFn(filePath, data)
		if err != nil {
			return err
		}
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		const rowBytes = dim * 2
		data := make([][rowBytes]byte, 0, rowCount)
		for i := 0; i < rowCount; i++ {
			vec := [rowBytes]byte{}
			for j := 0; j < rowBytes; j++ {
				vec[j] = byte(rand.Uint32() % 256)
			}
			data = append(data, vec)
		}
		err := writeFn(filePath, data)
		if err != nil {
			return err
		}
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		varcharData := make([]string, 0)
		for i := 0; i < rowCount; i++ {
			varcharData = append(varcharData, strconv.Itoa(i))
		}
		err := writeFn(filePath, varcharData)
		if err != nil {
			return err
		}
	case schemapb.DataType_JSON:
		jsonData := make([][]byte, 0)
		for i := 0; i < rowCount; i++ {
			jsonData = append(jsonData, []byte(fmt.Sprintf("{\"y\": %d}", i)))
		}
		err := writeFn(filePath, jsonData)
		if err != nil {
			return err
		}
	case schemapb.DataType_Array:
		arrayData := make([]*schemapb.ScalarField, 0)
		for i := 0; i < rowCount; i++ {
			arrayData = append(arrayData, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: []int32{int32(i), int32(i + 1), int32(i + 2)},
					},
				},
			})
		}
		err := writeFn(filePath, arrayData)
		if err != nil {
			return err
		}
	default:
		panic(fmt.Sprintf("unimplemented data type: %s", dType.String()))
	}

	return nil
}

func GenerateJSONFile(t *testing.T, filePath string, schema *schemapb.CollectionSchema, count int) {
	insertData := createInsertData(t, schema, count)
	rows := make([]map[string]any, 0, count)
	fieldIDToField := lo.KeyBy(schema.GetFields(), func(field *schemapb.FieldSchema) int64 {
		return field.GetFieldID()
	})
	for i := 0; i < count; i++ {
		data := make(map[int64]interface{})
		for fieldID, v := range insertData.Data {
			dataType := fieldIDToField[fieldID].GetDataType()
			if fieldIDToField[fieldID].GetAutoID() {
				continue
			}
			switch dataType {
			case schemapb.DataType_Array:
				data[fieldID] = v.GetRow(i).(*schemapb.ScalarField).GetIntData().GetData()
			case schemapb.DataType_JSON:
				data[fieldID] = string(v.GetRow(i).([]byte))
			case schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector, schemapb.DataType_SparseFloatVector:
				bytes := v.GetRow(i).([]byte)
				ints := make([]int, 0, len(bytes))
				for _, b := range bytes {
					ints = append(ints, int(b))
				}
				data[fieldID] = ints
			default:
				data[fieldID] = v.GetRow(i)
			}
		}
		row := lo.MapKeys(data, func(_ any, fieldID int64) string {
			return fieldIDToField[fieldID].GetName()
		})
		rows = append(rows, row)
	}

	jsonBytes, err := json.Marshal(rows)
	assert.NoError(t, err)

	err = os.WriteFile(filePath, jsonBytes, 0o644) // nolint
	assert.NoError(t, err)
}

func WaitForImportDone(ctx context.Context, c *integration.MiniClusterV2, jobID string) error {
	for {
		resp, err := c.Proxy.GetImportProgress(ctx, &internalpb.GetImportProgressRequest{
			JobID: jobID,
		})
		if err != nil {
			return err
		}
		if err = merr.Error(resp.GetStatus()); err != nil {
			return err
		}
		switch resp.GetState() {
		case internalpb.ImportJobState_Completed:
			return nil
		case internalpb.ImportJobState_Failed:
			return merr.WrapErrImportFailed(resp.GetReason())
		default:
			log.Info("import progress", zap.String("jobID", jobID),
				zap.Int64("progress", resp.GetProgress()),
				zap.String("state", resp.GetState().String()))
			time.Sleep(1 * time.Second)
		}
	}
}

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
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow/go/v12/arrow/array"
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
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

const dim = 128

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

	insertData, err := testutil.CreateInsertData(schema, numRows)
	if err != nil {
		return err
	}

	columns, err := testutil.BuildArrayData(schema, insertData)
	if err != nil {
		return err
	}

	recordBatch := array.NewRecord(pqSchema, columns, int64(numRows))
	return fw.Write(recordBatch)
}

func GenerateNumpyFiles(cm storage.ChunkManager, schema *schemapb.CollectionSchema, rowCount int) (*internalpb.ImportFile, error) {
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

	insertData, err := testutil.CreateInsertData(schema, rowCount)
	if err != nil {
		return nil, err
	}

	var data interface{}
	paths := make([]string, 0)
	for _, field := range schema.GetFields() {
		if field.GetAutoID() && field.GetIsPrimaryKey() {
			continue
		}
		path := fmt.Sprintf("%s/%s.npy", cm.RootPath(), field.GetName())

		fieldID := field.GetFieldID()
		fieldData := insertData.Data[fieldID]
		dType := field.GetDataType()
		switch dType {
		case schemapb.DataType_Bool:
			data = insertData.Data[fieldID].(*storage.BoolFieldData).Data
		case schemapb.DataType_Int8:
			data = insertData.Data[fieldID].(*storage.Int8FieldData).Data
		case schemapb.DataType_Int16:
			data = insertData.Data[fieldID].(*storage.Int16FieldData).Data
		case schemapb.DataType_Int32:
			data = insertData.Data[fieldID].(*storage.Int32FieldData).Data
		case schemapb.DataType_Int64:
			data = insertData.Data[fieldID].(*storage.Int64FieldData).Data
		case schemapb.DataType_Float:
			data = insertData.Data[fieldID].(*storage.FloatFieldData).Data
		case schemapb.DataType_Double:
			data = insertData.Data[fieldID].(*storage.DoubleFieldData).Data
		case schemapb.DataType_String, schemapb.DataType_VarChar:
			data = insertData.Data[fieldID].(*storage.StringFieldData).Data
		case schemapb.DataType_BinaryVector:
			vecData := insertData.Data[fieldID].(*storage.BinaryVectorFieldData).Data
			if dim != insertData.Data[fieldID].(*storage.BinaryVectorFieldData).Dim {
				panic(fmt.Sprintf("dim mis-match: %d, %d", dim, insertData.Data[fieldID].(*storage.BinaryVectorFieldData).Dim))
			}
			const rowBytes = dim / 8
			rows := len(vecData) / rowBytes
			binVecData := make([][rowBytes]byte, 0, rows)
			for i := 0; i < rows; i++ {
				rowVec := [rowBytes]byte{}
				copy(rowVec[:], vecData[i*rowBytes:(i+1)*rowBytes])
				binVecData = append(binVecData, rowVec)
			}
			data = binVecData
		case schemapb.DataType_FloatVector:
			vecData := insertData.Data[fieldID].(*storage.FloatVectorFieldData).Data
			if dim != insertData.Data[fieldID].(*storage.FloatVectorFieldData).Dim {
				panic(fmt.Sprintf("dim mis-match: %d, %d", dim, insertData.Data[fieldID].(*storage.FloatVectorFieldData).Dim))
			}
			rows := len(vecData) / dim
			floatVecData := make([][dim]float32, 0, rows)
			for i := 0; i < rows; i++ {
				rowVec := [dim]float32{}
				copy(rowVec[:], vecData[i*dim:(i+1)*dim])
				floatVecData = append(floatVecData, rowVec)
			}
			data = floatVecData
		case schemapb.DataType_Float16Vector:
			vecData := insertData.Data[fieldID].(*storage.Float16VectorFieldData).Data
			if dim != insertData.Data[fieldID].(*storage.Float16VectorFieldData).Dim {
				panic(fmt.Sprintf("dim mis-match: %d, %d", dim, insertData.Data[fieldID].(*storage.Float16VectorFieldData).Dim))
			}
			const rowBytes = dim * 2
			rows := len(vecData) / rowBytes
			float16VecData := make([][rowBytes]byte, 0, rows)
			for i := 0; i < rows; i++ {
				rowVec := [rowBytes]byte{}
				copy(rowVec[:], vecData[i*rowBytes:(i+1)*rowBytes])
				float16VecData = append(float16VecData, rowVec)
			}
			data = float16VecData
		case schemapb.DataType_BFloat16Vector:
			vecData := insertData.Data[fieldID].(*storage.BFloat16VectorFieldData).Data
			if dim != insertData.Data[fieldID].(*storage.BFloat16VectorFieldData).Dim {
				panic(fmt.Sprintf("dim mis-match: %d, %d", dim, insertData.Data[fieldID].(*storage.BFloat16VectorFieldData).Dim))
			}
			const rowBytes = dim * 2
			rows := len(vecData) / rowBytes
			bfloat16VecData := make([][rowBytes]byte, 0, rows)
			for i := 0; i < rows; i++ {
				rowVec := [rowBytes]byte{}
				copy(rowVec[:], vecData[i*rowBytes:(i+1)*rowBytes])
				bfloat16VecData = append(bfloat16VecData, rowVec)
			}
			data = bfloat16VecData
		case schemapb.DataType_SparseFloatVector:
			jsonStrs := make([]string, 0, fieldData.RowNum())
			for i := 0; i < fieldData.RowNum(); i++ {
				row := fieldData.GetRow(i)
				mapData := typeutil.SparseFloatBytesToMap(row.([]byte))
				// convert to JSON format
				jsonBytes, err := json.Marshal(mapData)
				if err != nil {
					panic(err)
				}
				jsonStrs = append(jsonStrs, string(jsonBytes))
			}
			data = jsonStrs
		case schemapb.DataType_JSON:
			data = insertData.Data[fieldID].(*storage.JSONFieldData).Data
		case schemapb.DataType_Array:
			data = insertData.Data[fieldID].(*storage.ArrayFieldData).Data
		default:
			panic(fmt.Sprintf("unsupported data type: %s", dType.String()))
		}

		err := writeFn(path, data)
		if err != nil {
			return nil, err
		}
		paths = append(paths, path)
	}
	return &internalpb.ImportFile{
		Paths: paths,
	}, nil
}

func GenerateJSONFile(t *testing.T, filePath string, schema *schemapb.CollectionSchema, count int) {
	insertData, err := testutil.CreateInsertData(schema, count)
	assert.NoError(t, err)
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
			case schemapb.DataType_BinaryVector:
				bytes := v.GetRow(i).([]byte)
				ints := make([]int, 0, len(bytes))
				for _, b := range bytes {
					ints = append(ints, int(b))
				}
				data[fieldID] = ints
			case schemapb.DataType_Float16Vector:
				bytes := v.GetRow(i).([]byte)
				data[fieldID] = typeutil.Float16BytesToFloat32Vector(bytes)
			case schemapb.DataType_BFloat16Vector:
				bytes := v.GetRow(i).([]byte)
				data[fieldID] = typeutil.BFloat16BytesToFloat32Vector(bytes)
			case schemapb.DataType_SparseFloatVector:
				bytes := v.GetRow(i).([]byte)
				data[fieldID] = typeutil.SparseFloatBytesToMap(bytes)
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

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
	"encoding/csv"
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
	"github.com/milvus-io/milvus/internal/storage"
	pq "github.com/milvus-io/milvus/internal/util/importutilv2/parquet"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/tests/integration"
)

const dim = 128

func CheckLogID(fieldBinlogs []*datapb.FieldBinlog) error {
	for _, fieldBinlog := range fieldBinlogs {
		for _, l := range fieldBinlog.GetBinlogs() {
			if l.GetLogID() == 0 {
				return fmt.Errorf("unexpected log id 0")
			}
		}
	}
	return nil
}

func GenerateParquetFile(filePath string, schema *schemapb.CollectionSchema, numRows int) error {
	_, err := GenerateParquetFileAndReturnInsertData(filePath, schema, numRows)
	return err
}

func GenerateParquetFileAndReturnInsertData(filePath string, schema *schemapb.CollectionSchema, numRows int) (*storage.InsertData, error) {
	w, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return nil, err
	}

	pqSchema, err := pq.ConvertToArrowSchema(schema, false)
	if err != nil {
		return nil, err
	}
	fw, err := pqarrow.NewFileWriter(pqSchema, w, parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(int64(numRows))), pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, err
	}
	defer fw.Close()

	insertData, err := testutil.CreateInsertData(schema, numRows)
	if err != nil {
		return nil, err
	}

	columns, err := testutil.BuildArrayData(schema, insertData, false)
	if err != nil {
		return nil, err
	}

	recordBatch := array.NewRecord(pqSchema, columns, int64(numRows))
	return insertData, fw.Write(recordBatch)
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
		case schemapb.DataType_BinaryVector:
			rows := fieldData.GetDataRows().([]byte)
			if dim != fieldData.(*storage.BinaryVectorFieldData).Dim {
				panic(fmt.Sprintf("dim mis-match: %d, %d", dim, fieldData.(*storage.BinaryVectorFieldData).Dim))
			}
			const rowBytes = dim / 8
			chunked := lo.Chunk(rows, rowBytes)
			chunkedRows := make([][rowBytes]byte, len(chunked))
			for i, innerSlice := range chunked {
				copy(chunkedRows[i][:], innerSlice)
			}
			data = chunkedRows
		case schemapb.DataType_FloatVector:
			rows := fieldData.GetDataRows().([]float32)
			if dim != fieldData.(*storage.FloatVectorFieldData).Dim {
				panic(fmt.Sprintf("dim mis-match: %d, %d", dim, fieldData.(*storage.FloatVectorFieldData).Dim))
			}
			chunked := lo.Chunk(rows, dim)
			chunkedRows := make([][dim]float32, len(chunked))
			for i, innerSlice := range chunked {
				copy(chunkedRows[i][:], innerSlice)
			}
			data = chunkedRows
		case schemapb.DataType_Float16Vector:
			rows := insertData.Data[fieldID].GetDataRows().([]byte)
			if dim != fieldData.(*storage.Float16VectorFieldData).Dim {
				panic(fmt.Sprintf("dim mis-match: %d, %d", dim, fieldData.(*storage.Float16VectorFieldData).Dim))
			}
			const rowBytes = dim * 2
			chunked := lo.Chunk(rows, rowBytes)
			chunkedRows := make([][rowBytes]byte, len(chunked))
			for i, innerSlice := range chunked {
				copy(chunkedRows[i][:], innerSlice)
			}
			data = chunkedRows
		case schemapb.DataType_BFloat16Vector:
			rows := insertData.Data[fieldID].GetDataRows().([]byte)
			if dim != fieldData.(*storage.BFloat16VectorFieldData).Dim {
				panic(fmt.Sprintf("dim mis-match: %d, %d", dim, fieldData.(*storage.BFloat16VectorFieldData).Dim))
			}
			const rowBytes = dim * 2
			chunked := lo.Chunk(rows, rowBytes)
			chunkedRows := make([][rowBytes]byte, len(chunked))
			for i, innerSlice := range chunked {
				copy(chunkedRows[i][:], innerSlice)
			}
			data = chunkedRows
		case schemapb.DataType_SparseFloatVector:
			data = insertData.Data[fieldID].(*storage.SparseFloatVectorFieldData).GetContents()
		case schemapb.DataType_Int8Vector:
			rows := insertData.Data[fieldID].GetDataRows().([]int8)
			if dim != fieldData.(*storage.Int8VectorFieldData).Dim {
				panic(fmt.Sprintf("dim mis-match: %d, %d", dim, fieldData.(*storage.Int8VectorFieldData).Dim))
			}
			chunked := lo.Chunk(rows, dim)
			chunkedRows := make([][dim]int8, len(chunked))
			for i, innerSlice := range chunked {
				copy(chunkedRows[i][:], innerSlice)
			}
			data = chunkedRows
		default:
			data = insertData.Data[fieldID].GetDataRows()
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

	rows, err := testutil.CreateInsertDataRowsForJSON(schema, insertData)
	assert.NoError(t, err)

	jsonBytes, err := json.Marshal(rows)
	assert.NoError(t, err)

	err = os.WriteFile(filePath, jsonBytes, 0o644) // nolint
	assert.NoError(t, err)
}

func GenerateCSVFile(t *testing.T, filePath string, schema *schemapb.CollectionSchema, count int) rune {
	insertData, err := testutil.CreateInsertData(schema, count)
	assert.NoError(t, err)

	sep := ','
	nullkey := ""

	csvData, err := testutil.CreateInsertDataForCSV(schema, insertData, nullkey)
	assert.NoError(t, err)

	wf, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o666)
	assert.NoError(t, err)

	writer := csv.NewWriter(wf)
	writer.Comma = sep
	writer.WriteAll(csvData)
	writer.Flush()
	assert.NoError(t, err)

	return sep
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

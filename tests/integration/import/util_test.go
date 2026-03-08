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
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/sbinet/npyio"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	pq "github.com/milvus-io/milvus/internal/util/importutilv2/parquet"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/tests/integration/cluster"
)

const dim = 128

func CheckLogID(fieldBinlogs []*datapb.FieldBinlog) error {
	for _, fieldBinlog := range fieldBinlogs {
		for _, l := range fieldBinlog.GetBinlogs() {
			if l.GetLogID() == 0 {
				return errors.New("unexpected log id 0")
			}
		}
	}
	return nil
}

func GenerateParquetFile(c *cluster.MiniClusterV3, schema *schemapb.CollectionSchema, numRows int) (string, error) {
	_, filePath, err := GenerateParquetFileAndReturnInsertData(c, schema, numRows)
	return filePath, err
}

func GenerateParquetFileAndReturnInsertData(c *cluster.MiniClusterV3, schema *schemapb.CollectionSchema, numRows int) (*storage.InsertData, string, error) {
	insertData, err := testutil.CreateInsertData(schema, numRows)
	if err != nil {
		panic(err)
	}

	buf, err := searilizeParquetFile(schema, insertData, numRows)
	if err != nil {
		panic(err)
	}

	filePath := path.Join(c.RootPath(), "parquet", uuid.New().String()+".parquet")
	if err := c.ChunkManager.Write(context.Background(), filePath, buf.Bytes()); err != nil {
		return nil, "", err
	}
	return insertData, filePath, err
}

func searilizeParquetFile(schema *schemapb.CollectionSchema, insertData *storage.InsertData, numRows int) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 10240))

	pqSchema, err := pq.ConvertToArrowSchemaForUT(schema, false)
	if err != nil {
		return nil, err
	}
	fw, err := pqarrow.NewFileWriter(pqSchema, buf, parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(int64(numRows))), pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, err
	}
	defer fw.Close()

	columns, err := testutil.BuildArrayData(schema, insertData, false)
	if err != nil {
		return nil, err
	}
	recordBatch := array.NewRecord(pqSchema, columns, int64(numRows))
	if err := fw.Write(recordBatch); err != nil {
		return nil, err
	}
	return buf, nil
}

func GenerateNumpyFiles(c *cluster.MiniClusterV3, schema *schemapb.CollectionSchema, rowCount int) (*internalpb.ImportFile, error) {
	writeFn := func(path string, data interface{}) error {
		buf := bytes.NewBuffer(make([]byte, 0, 10240))
		if err := npyio.Write(buf, data); err != nil {
			return err
		}
		return c.ChunkManager.Write(context.Background(), path, buf.Bytes())
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
		path := path.Join(c.RootPath(), "numpy", uuid.New().String(), field.GetName()+".npy")

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

		if err := writeFn(path, data); err != nil {
			panic(err)
		}
		paths = append(paths, path)
	}
	return &internalpb.ImportFile{
		Paths: paths,
	}, nil
}

func GenerateJSONFile(t *testing.T, c *cluster.MiniClusterV3, schema *schemapb.CollectionSchema, count int) string {
	insertData, err := testutil.CreateInsertData(schema, count)
	assert.NoError(t, err)

	rows, err := testutil.CreateInsertDataRowsForJSON(schema, insertData)
	assert.NoError(t, err)

	jsonBytes, err := json.Marshal(rows)
	assert.NoError(t, err)

	filePath := path.Join(c.RootPath(), "json", uuid.New().String()+".json")

	if err = c.ChunkManager.Write(context.Background(), filePath, jsonBytes); err != nil {
		panic(err)
	}
	return filePath
}

func GenerateCSVFile(t *testing.T, c *cluster.MiniClusterV3, schema *schemapb.CollectionSchema, count int) (string, rune) {
	filePath := path.Join(c.RootPath(), "csv", uuid.New().String()+".csv")

	insertData, err := testutil.CreateInsertData(schema, count)
	assert.NoError(t, err)

	sep := ','
	nullkey := ""

	csvData, err := testutil.CreateInsertDataForCSV(schema, insertData, nullkey)
	assert.NoError(t, err)

	buf := bytes.NewBuffer(make([]byte, 0, 10240))
	writer := csv.NewWriter(buf)
	writer.Comma = sep
	writer.WriteAll(csvData)
	writer.Flush()
	assert.NoError(t, err)

	if err = c.ChunkManager.Write(context.Background(), filePath, buf.Bytes()); err != nil {
		panic(err)
	}
	return filePath, sep
}

func WaitForImportDone(ctx context.Context, c *cluster.MiniClusterV3, jobID string) error {
	for {
		resp, err := c.ProxyClient.GetImportProgress(ctx, &internalpb.GetImportProgressRequest{
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

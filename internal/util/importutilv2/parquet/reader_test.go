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
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"

	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/parquet"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"golang.org/x/exp/slices"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type ReaderSuite struct {
	suite.Suite

	numRows     int
	pkDataType  schemapb.DataType
	vecDataType schemapb.DataType
}

func (s *ReaderSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
}

func (s *ReaderSuite) SetupTest() {
	// default suite params
	s.numRows = 100
	s.pkDataType = schemapb.DataType_Int64
	s.vecDataType = schemapb.DataType_FloatVector
}

func randomString(length int) string {
	letterRunes := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, length)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func writeParquet(w io.Writer, schema *schemapb.CollectionSchema, numRows int) (*storage.InsertData, error) {
	pqSchema, err := ConvertToArrowSchema(schema)
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

	columns, err := testutil.BuildArrayData(schema, insertData)
	if err != nil {
		return nil, err
	}

	recordBatch := array.NewRecord(pqSchema, columns, int64(numRows))
	err = fw.Write(recordBatch)
	if err != nil {
		return nil, err
	}

	return insertData, nil
}

func (s *ReaderSuite) run(dataType schemapb.DataType, elemType schemapb.DataType) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     s.pkDataType,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "max_length",
						Value: "256",
					},
				},
			},
			{
				FieldID:  101,
				Name:     "vec",
				DataType: s.vecDataType,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "8",
					},
				},
			},
			{
				FieldID:     102,
				Name:        dataType.String(),
				DataType:    dataType,
				ElementType: elemType,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "max_length",
						Value: "256",
					},
				},
			},
		},
	}

	filePath := fmt.Sprintf("/tmp/test_%d_reader.parquet", rand.Int())
	defer os.Remove(filePath)
	wf, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o666)
	assert.NoError(s.T(), err)
	insertData, err := writeParquet(wf, schema, s.numRows)
	assert.NoError(s.T(), err)

	ctx := context.Background()
	f := storage.NewChunkManagerFactory("local", storage.RootPath("/tmp/milvus_test/test_parquet_reader/"))
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(s.T(), err)
	reader, err := NewReader(ctx, cm, schema, filePath, 64*1024*1024)
	s.NoError(err)

	checkFn := func(actualInsertData *storage.InsertData, offsetBegin, expectRows int) {
		expectInsertData := insertData
		for fieldID, data := range actualInsertData.Data {
			s.Equal(expectRows, data.RowNum())
			fieldDataType := typeutil.GetField(schema, fieldID).GetDataType()
			for i := 0; i < expectRows; i++ {
				expect := expectInsertData.Data[fieldID].GetRow(i + offsetBegin)
				actual := data.GetRow(i)
				if fieldDataType == schemapb.DataType_Array {
					s.True(slices.Equal(expect.(*schemapb.ScalarField).GetIntData().GetData(), actual.(*schemapb.ScalarField).GetIntData().GetData()))
				} else {
					s.Equal(expect, actual)
				}
			}
		}
	}

	res, err := reader.Read()
	s.NoError(err)
	checkFn(res, 0, s.numRows)
}

func (s *ReaderSuite) failRun(dt schemapb.DataType, isDynamic bool) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     s.pkDataType,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "max_length",
						Value: "256",
					},
				},
			},
			{
				FieldID:  101,
				Name:     "vec",
				DataType: s.vecDataType,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "8",
					},
				},
			},
			{
				FieldID:     102,
				Name:        dt.String(),
				DataType:    dt,
				ElementType: schemapb.DataType_Int32,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "max_length",
						Value: "256",
					},
				},
				IsDynamic: isDynamic,
			},
		},
	}

	filePath := fmt.Sprintf("/tmp/test_%d_reader.parquet", rand.Int())
	defer os.Remove(filePath)
	wf, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o666)
	assert.NoError(s.T(), err)
	_, err = writeParquet(wf, schema, s.numRows)
	assert.NoError(s.T(), err)

	ctx := context.Background()
	f := storage.NewChunkManagerFactory("local", storage.RootPath("/tmp/milvus_test/test_parquet_reader/"))
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(s.T(), err)
	reader, err := NewReader(ctx, cm, schema, filePath, 64*1024*1024)
	s.NoError(err)

	_, err = reader.Read()
	s.Error(err)
}

func (s *ReaderSuite) TestReadScalarFields() {
	s.run(schemapb.DataType_Bool, schemapb.DataType_None)
	s.run(schemapb.DataType_Int8, schemapb.DataType_None)
	s.run(schemapb.DataType_Int16, schemapb.DataType_None)
	s.run(schemapb.DataType_Int32, schemapb.DataType_None)
	s.run(schemapb.DataType_Int64, schemapb.DataType_None)
	s.run(schemapb.DataType_Float, schemapb.DataType_None)
	s.run(schemapb.DataType_Double, schemapb.DataType_None)
	s.run(schemapb.DataType_String, schemapb.DataType_None)
	s.run(schemapb.DataType_VarChar, schemapb.DataType_None)
	s.run(schemapb.DataType_JSON, schemapb.DataType_None)

	s.run(schemapb.DataType_Array, schemapb.DataType_Bool)
	s.run(schemapb.DataType_Array, schemapb.DataType_Int8)
	s.run(schemapb.DataType_Array, schemapb.DataType_Int16)
	s.run(schemapb.DataType_Array, schemapb.DataType_Int32)
	s.run(schemapb.DataType_Array, schemapb.DataType_Int64)
	s.run(schemapb.DataType_Array, schemapb.DataType_Float)
	s.run(schemapb.DataType_Array, schemapb.DataType_Double)
	s.run(schemapb.DataType_Array, schemapb.DataType_String)

	s.failRun(schemapb.DataType_JSON, true)
}

func (s *ReaderSuite) TestStringPK() {
	s.pkDataType = schemapb.DataType_VarChar
	s.run(schemapb.DataType_Int32, schemapb.DataType_None)
}

func (s *ReaderSuite) TestVector() {
	s.vecDataType = schemapb.DataType_BinaryVector
	s.run(schemapb.DataType_Int32, schemapb.DataType_None)
	s.vecDataType = schemapb.DataType_FloatVector
	s.run(schemapb.DataType_Int32, schemapb.DataType_None)
	s.vecDataType = schemapb.DataType_Float16Vector
	s.run(schemapb.DataType_Int32, schemapb.DataType_None)
	s.vecDataType = schemapb.DataType_BFloat16Vector
	s.run(schemapb.DataType_Int32, schemapb.DataType_None)
	s.vecDataType = schemapb.DataType_SparseFloatVector
	s.run(schemapb.DataType_Int32, schemapb.DataType_None)
}

func TestUtil(t *testing.T) {
	suite.Run(t, new(ReaderSuite))
}

package csv

import (
	"context"
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type ReaderSuite struct {
	suite.Suite

	numRows     int
	pkDataType  schemapb.DataType
	vecDataType schemapb.DataType
}

func (suite *ReaderSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
}

func (suite *ReaderSuite) SetupTest() {
	suite.numRows = 10
	suite.pkDataType = schemapb.DataType_Int64
	suite.vecDataType = schemapb.DataType_FloatVector
}

func (suite *ReaderSuite) run(dataType schemapb.DataType, elemType schemapb.DataType) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     suite.pkDataType,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.MaxLengthKey,
						Value: "128",
					},
				},
			},
			{
				FieldID:  101,
				Name:     "vec",
				DataType: suite.vecDataType,
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
						Key:   common.MaxLengthKey,
						Value: "128",
					},
				},
			},
		},
	}

	// generate csv data
	insertData, err := testutil.CreateInsertData(schema, suite.numRows)
	suite.NoError(err)
	csvData, err := testutil.CreateInsertDataForCSV(schema, insertData)
	suite.NoError(err)

	// write to csv file
	sep := '\t'
	filePath := fmt.Sprintf("/tmp/test_%d_reader.csv", rand.Int())
	defer os.Remove(filePath)
	wf, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o666)
	assert.NoError(suite.T(), err)
	writer := csv.NewWriter(wf)
	writer.Comma = sep
	writer.WriteAll(csvData)
	suite.NoError(err)

	// read from csv file
	ctx := context.Background()
	f := storage.NewChunkManagerFactory("local", storage.RootPath("/tmp/milvus_test/test_csv_reader/"))
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	suite.NoError(err)

	// check reader separate fields by '\t'
	wrongSep := ','
	_, err = NewReader(ctx, cm, schema, filePath, 64*1024*1024, wrongSep)
	suite.Error(err)
	suite.Contains(err.Error(), "value of field is missed: ")

	// check data
	reader, err := NewReader(ctx, cm, schema, filePath, 64*1024*1024, sep)
	suite.NoError(err)

	checkFn := func(actualInsertData *storage.InsertData, offsetBegin, expectRows int) {
		expectInsertData := insertData
		for fieldID, data := range actualInsertData.Data {
			suite.Equal(expectRows, data.RowNum())
			for i := 0; i < expectRows; i++ {
				expect := expectInsertData.Data[fieldID].GetRow(i + offsetBegin)
				actual := data.GetRow(i)
				suite.Equal(expect, actual)
			}
		}
	}

	res, err := reader.Read()
	suite.NoError(err)
	checkFn(res, 0, suite.numRows)
}

func (suite *ReaderSuite) TestReadScalarFields() {
	suite.run(schemapb.DataType_Bool, schemapb.DataType_None)
	suite.run(schemapb.DataType_Int8, schemapb.DataType_None)
	suite.run(schemapb.DataType_Int16, schemapb.DataType_None)
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None)
	suite.run(schemapb.DataType_Int64, schemapb.DataType_None)
	suite.run(schemapb.DataType_Float, schemapb.DataType_None)
	suite.run(schemapb.DataType_Double, schemapb.DataType_None)
	suite.run(schemapb.DataType_String, schemapb.DataType_None)
	suite.run(schemapb.DataType_VarChar, schemapb.DataType_None)
	suite.run(schemapb.DataType_JSON, schemapb.DataType_None)

	suite.run(schemapb.DataType_Array, schemapb.DataType_Bool)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int8)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int16)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int32)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int64)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Float)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Double)
	suite.run(schemapb.DataType_Array, schemapb.DataType_String)
}

func (suite *ReaderSuite) TestStringPK() {
	suite.pkDataType = schemapb.DataType_VarChar
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None)
}

func (suite *ReaderSuite) TestVector() {
	suite.vecDataType = schemapb.DataType_BinaryVector
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None)
	suite.vecDataType = schemapb.DataType_FloatVector
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None)
	suite.vecDataType = schemapb.DataType_Float16Vector
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None)
	suite.vecDataType = schemapb.DataType_BFloat16Vector
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None)
	suite.vecDataType = schemapb.DataType_SparseFloatVector
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None)
}

func TestUtil(t *testing.T) {
	suite.Run(t, new(ReaderSuite))
}

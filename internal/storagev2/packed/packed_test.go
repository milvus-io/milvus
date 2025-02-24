// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package packed

import (
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/stretchr/testify/suite"
	"golang.org/x/exp/rand"

	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestPackedReadAndWrite(t *testing.T) {
	suite.Run(t, new(PackedTestSuite))
}

type PackedTestSuite struct {
	suite.Suite
	schema            *arrow.Schema
	rec               arrow.Record
	localDataRootPath string
}

func (suite *PackedTestSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *PackedTestSuite) SetupTest() {
	initcore.InitLocalArrowFileSystem("/tmp")
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int32},
		{Name: "b", Type: arrow.PrimitiveTypes.Int64},
		{Name: "c", Type: arrow.BinaryTypes.String},
	}, nil)
	suite.schema = schema

	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for idx := range schema.Fields() {
		switch idx {
		case 0:
			b.Field(idx).(*array.Int32Builder).AppendValues(
				[]int32{int32(1), int32(2), int32(3)}, nil,
			)
		case 1:
			b.Field(idx).(*array.Int64Builder).AppendValues(
				[]int64{int64(4), int64(5), int64(6)}, nil,
			)
		case 2:
			b.Field(idx).(*array.StringBuilder).AppendValues(
				[]string{"a", "b", "c"}, nil,
			)
		}
	}
	rec := b.NewRecord()
	suite.rec = rec
}

func (suite *PackedTestSuite) TestPackedOneFile() {
	batches := 100

	paths := []string{"/tmp/100"}
	columnGroups := [][]int{{0, 1, 2}}
	bufferSize := int64(10 * 1024 * 1024) // 10MB
	multiPartUploadSize := int64(0)
	pw, err := NewPackedWriter(paths, suite.schema, bufferSize, multiPartUploadSize, columnGroups)
	suite.NoError(err)
	for i := 0; i < batches; i++ {
		err = pw.WriteRecordBatch(suite.rec)
		suite.NoError(err)
	}
	err = pw.Close()
	suite.NoError(err)

	reader, err := NewPackedReader(paths, suite.schema, bufferSize)
	suite.NoError(err)
	rr, err := reader.ReadNext()
	suite.NoError(err)
	defer rr.Release()
	suite.Equal(int64(3*batches), rr.NumRows())
}

func (suite *PackedTestSuite) TestPackedMultiFiles() {
	batches := 1000

	b := array.NewRecordBuilder(memory.DefaultAllocator, suite.schema)
	strLen := 1000
	arrLen := 30
	defer b.Release()
	for idx := range suite.schema.Fields() {
		switch idx {
		case 0:
			values := make([]int32, arrLen)
			for i := 0; i < arrLen; i++ {
				values[i] = int32(i + 1)
			}
			b.Field(idx).(*array.Int32Builder).AppendValues(values, nil)
		case 1:
			values := make([]int64, arrLen)
			for i := 0; i < arrLen; i++ {
				values[i] = int64(i + 1)
			}
			b.Field(idx).(*array.Int64Builder).AppendValues(values, nil)
		case 2:
			values := make([]string, arrLen)
			for i := 0; i < arrLen; i++ {
				values[i] = randomString(strLen)
			}
			b.Field(idx).(*array.StringBuilder).AppendValues(values, nil)
		}
	}
	rec := b.NewRecord()
	defer rec.Release()
	paths := []string{"/tmp/100", "/tmp/101"}
	columnGroups := [][]int{{2}, {0, 1}}
	bufferSize := int64(10 * 1024 * 1024) // 10MB
	multiPartUploadSize := int64(0)
	pw, err := NewPackedWriter(paths, suite.schema, bufferSize, multiPartUploadSize, columnGroups)
	suite.NoError(err)
	for i := 0; i < batches; i++ {
		err = pw.WriteRecordBatch(rec)
		suite.NoError(err)
	}
	err = pw.Close()
	suite.NoError(err)

	reader, err := NewPackedReader(paths, suite.schema, bufferSize)
	suite.NoError(err)
	var rows int64 = 0
	var rr arrow.Record
	for {
		rr, err = reader.ReadNext()
		suite.NoError(err)
		if rr == nil {
			// end of file
			break
		}

		rows += rr.NumRows()
	}

	suite.Equal(int64(arrLen*batches), rows)
}

func randomString(length int) string {
	const charset = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	result := make([]byte, length)
	for i := range result {
		result[i] = charset[rand.Intn(len(charset))]
	}
	return string(result)
}

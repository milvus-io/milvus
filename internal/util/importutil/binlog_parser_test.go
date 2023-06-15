// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package importutil

import (
	"context"
	"math"
	"path"
	"strconv"
	"testing"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/stretchr/testify/assert"
)

func Test_BinlogParserNew(t *testing.T) {
	ctx := context.Background()

	// nil schema
	parser, err := NewBinlogParser(ctx, nil, 1024, nil, nil, nil, 0, math.MaxUint64)
	assert.Nil(t, parser)
	assert.Error(t, err)

	collectionInfo, err := NewCollectionInfo(sampleSchema(), 2, []int64{1})
	assert.NoError(t, err)

	// nil chunkmanager
	parser, err = NewBinlogParser(ctx, collectionInfo, 1024, nil, nil, nil, 0, math.MaxUint64)
	assert.Nil(t, parser)
	assert.Error(t, err)

	// nil flushfunc
	parser, err = NewBinlogParser(ctx, collectionInfo, 1024, &MockChunkManager{}, nil, nil, 0, math.MaxUint64)
	assert.Nil(t, parser)
	assert.Error(t, err)

	// succeed
	flushFunc := func(fields BlockData, shardID int, partID int64) error {
		return nil
	}
	parser, err = NewBinlogParser(ctx, collectionInfo, 1024, &MockChunkManager{}, flushFunc, nil, 0, math.MaxUint64)
	assert.NotNil(t, parser)
	assert.NoError(t, err)

	// tsStartPoint larger than tsEndPoint
	parser, err = NewBinlogParser(ctx, collectionInfo, 1024, &MockChunkManager{}, flushFunc, nil, 2, 1)
	assert.Nil(t, parser)
	assert.Error(t, err)
}

func Test_BinlogParserConstructHolders(t *testing.T) {
	ctx := context.Background()

	flushFunc := func(fields BlockData, shardID int, partID int64) error {
		return nil
	}

	chunkManager := &MockChunkManager{
		listResult: make(map[string][]string),
	}

	insertPath := "insertPath"
	deltaPath := "deltaPath"

	// the first segment has 12 fields, each field has 2 binlog files
	seg1Files := []string{
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/0/435978159903735800",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/1/435978159903735801",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/102/435978159903735802",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/103/435978159903735803",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/104/435978159903735804",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/105/435978159903735805",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/106/435978159903735806",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/107/435978159903735807",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/108/435978159903735808",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/109/435978159903735809",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/110/435978159903735810",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/111/435978159903735811",

		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/0/425978159903735800",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/1/425978159903735801",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/102/425978159903735802",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/103/425978159903735803",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/104/425978159903735804",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/105/425978159903735805",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/106/425978159903735806",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/107/425978159903735807",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/108/425978159903735808",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/109/425978159903735809",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/110/425978159903735810",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/111/425978159903735811",
	}

	// the second segment has 12 fields, each field has 1 binlog file
	seg2Files := []string{
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483009/0/435978159903735811",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483009/1/435978159903735812",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483009/102/435978159903735802",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483009/103/435978159903735803",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483009/104/435978159903735804",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483009/105/435978159903735805",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483009/106/435978159903735806",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483009/107/435978159903735807",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483009/108/435978159903735808",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483009/109/435978159903735809",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483009/110/435978159903735810",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483009/111/435978159903735811",
	}

	chunkManager.listResult[insertPath] = append(chunkManager.listResult[insertPath], seg1Files...)
	chunkManager.listResult[insertPath] = append(chunkManager.listResult[insertPath], seg2Files...)

	// the segment has a delta log file
	chunkManager.listResult[deltaPath] = []string{
		"backup/bak1/data/delta_log/435978159196147009/435978159196147010/435978159261483009/434574382554415105",
	}

	collectionInfo, err := NewCollectionInfo(sampleSchema(), 2, []int64{1})
	assert.NoError(t, err)

	parser, err := NewBinlogParser(ctx, collectionInfo, 1024, chunkManager, flushFunc, nil, 0, math.MaxUint64)
	assert.NotNil(t, parser)
	assert.NoError(t, err)

	holders, err := parser.constructSegmentHolders(insertPath, deltaPath)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(holders))

	// verify the first segment
	holder := holders[0]
	assert.Equal(t, int64(435978159261483008), holder.segmentID)
	assert.Equal(t, 12, len(holder.fieldFiles))
	for i := 0; i < 12; i++ {
		fieldPath := path.Dir(seg1Files[i])
		fieldStrID := path.Base(fieldPath)
		fieldID, _ := strconv.ParseInt(fieldStrID, 10, 64)
		logFiles, ok := holder.fieldFiles[fieldID]
		assert.True(t, ok)
		assert.Equal(t, 2, len(logFiles))

		// verify logs under each field is sorted
		log1 := logFiles[0]
		logID1 := path.Base(log1)
		ID1, _ := strconv.ParseInt(logID1, 10, 64)
		log2 := logFiles[1]
		logID2 := path.Base(log2)
		ID2, _ := strconv.ParseInt(logID2, 10, 64)
		assert.LessOrEqual(t, ID1, ID2)
	}
	assert.Equal(t, 0, len(holder.deltaFiles))

	// verify the second segment
	holder = holders[1]
	assert.Equal(t, int64(435978159261483009), holder.segmentID)
	assert.Equal(t, len(seg2Files), len(holder.fieldFiles))
	for i := 0; i < len(seg2Files); i++ {
		fieldPath := path.Dir(seg2Files[i])
		fieldStrID := path.Base(fieldPath)
		fieldID, _ := strconv.ParseInt(fieldStrID, 10, 64)
		logFiles, ok := holder.fieldFiles[fieldID]
		assert.True(t, ok)
		assert.Equal(t, 1, len(logFiles))
		assert.Equal(t, seg2Files[i], logFiles[0])
	}
	assert.Equal(t, 1, len(holder.deltaFiles))
	assert.Equal(t, chunkManager.listResult[deltaPath][0], holder.deltaFiles[0])
}

func Test_BinlogParserConstructHoldersFailed(t *testing.T) {
	ctx := context.Background()

	flushFunc := func(fields BlockData, shardID int, partID int64) error {
		return nil
	}

	chunkManager := &MockChunkManager{
		listErr:    errors.New("error"),
		listResult: make(map[string][]string),
	}

	collectionInfo, err := NewCollectionInfo(sampleSchema(), 2, []int64{1})
	assert.NoError(t, err)

	parser, err := NewBinlogParser(ctx, collectionInfo, 1024, chunkManager, flushFunc, nil, 0, math.MaxUint64)
	assert.NotNil(t, parser)
	assert.NoError(t, err)

	insertPath := "insertPath"
	deltaPath := "deltaPath"

	// chunkManager return error
	holders, err := parser.constructSegmentHolders(insertPath, deltaPath)
	assert.Error(t, err)
	assert.Nil(t, holders)

	// parse field id error(insert log)
	chunkManager.listErr = nil
	chunkManager.listResult[insertPath] = []string{
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/illegal/435978159903735811",
	}
	holders, err = parser.constructSegmentHolders(insertPath, deltaPath)
	assert.Error(t, err)
	assert.Nil(t, holders)

	// parse segment id error(insert log)
	chunkManager.listResult[insertPath] = []string{
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/illegal/0/435978159903735811",
	}
	holders, err = parser.constructSegmentHolders(insertPath, deltaPath)
	assert.Error(t, err)
	assert.Nil(t, holders)

	// parse segment id error(delta log)
	chunkManager.listResult[insertPath] = []string{}
	chunkManager.listResult[deltaPath] = []string{
		"backup/bak1/data/delta_log/435978159196147009/435978159196147010/illegal/434574382554415105",
	}
	holders, err = parser.constructSegmentHolders(insertPath, deltaPath)
	assert.Error(t, err)
	assert.Nil(t, holders)
}

func Test_BinlogParserParseFilesFailed(t *testing.T) {
	ctx := context.Background()

	flushFunc := func(fields BlockData, shardID int, partID int64) error {
		return nil
	}

	collectionInfo, err := NewCollectionInfo(sampleSchema(), 2, []int64{1})
	assert.NoError(t, err)

	parser, err := NewBinlogParser(ctx, collectionInfo, 1024, &MockChunkManager{}, flushFunc, nil, 0, math.MaxUint64)
	assert.NotNil(t, parser)
	assert.NoError(t, err)

	err = parser.parseSegmentFiles(nil)
	assert.Error(t, err)

	parser.collectionInfo = nil
	err = parser.parseSegmentFiles(&SegmentFilesHolder{})
	assert.Error(t, err)
}

func Test_BinlogParserParse(t *testing.T) {
	ctx := context.Background()

	flushFunc := func(fields BlockData, shardID int, partID int64) error {
		return nil
	}

	chunkManager := &MockChunkManager{}

	schema := &schemapb.CollectionSchema{
		Name:        "schema",
		Description: "schema",
		AutoID:      true,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      101,
				Name:         "id",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
		},
	}
	collectionInfo, err := NewCollectionInfo(schema, 2, []int64{1})
	assert.NoError(t, err)

	parser, err := NewBinlogParser(ctx, collectionInfo, 1024, chunkManager, flushFunc, nil, 0, math.MaxUint64)
	assert.NotNil(t, parser)
	assert.NoError(t, err)

	// zero paths
	err = parser.Parse(nil)
	assert.Error(t, err)

	// one empty path
	paths := []string{
		"insertPath",
	}
	err = parser.Parse(paths)
	assert.NoError(t, err)

	// two empty paths
	paths = append(paths, "deltaPath")
	err = parser.Parse(paths)
	assert.NoError(t, err)

	// wrong path
	chunkManager.listResult = make(map[string][]string)
	chunkManager.listResult["insertPath"] = []string{
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/illegal/101/435978159903735811",
	}
	err = parser.Parse(paths)
	assert.Error(t, err)

	// file not found
	chunkManager.listResult["insertPath"] = []string{
		"123/0/a",
		"123/1/a",
		"123/101/a",
	}
	err = parser.Parse(paths)
	assert.Error(t, err)

	// progress
	rowCount := 100
	fieldsData := createFieldsData(sampleSchema(), rowCount)
	chunkManager.listResult["deltaPath"] = []string{}
	chunkManager.listResult["insertPath"] = []string{
		"123/0/a",
		"123/1/a",
		"123/102/a",
		"123/103/a",
		"123/104/a",
		"123/105/a",
		"123/106/a",
		"123/107/a",
		"123/108/a",
		"123/109/a",
		"123/110/a",
		"123/111/a",
		"123/112/a",
	}
	chunkManager.readBuf = map[string][]byte{
		"123/0/a":   createBinlogBuf(t, schemapb.DataType_Int64, fieldsData[106].([]int64)),
		"123/1/a":   createBinlogBuf(t, schemapb.DataType_Int64, fieldsData[106].([]int64)),
		"123/102/a": createBinlogBuf(t, schemapb.DataType_Bool, fieldsData[102].([]bool)),
		"123/103/a": createBinlogBuf(t, schemapb.DataType_Int8, fieldsData[103].([]int8)),
		"123/104/a": createBinlogBuf(t, schemapb.DataType_Int16, fieldsData[104].([]int16)),
		"123/105/a": createBinlogBuf(t, schemapb.DataType_Int32, fieldsData[105].([]int32)),
		"123/106/a": createBinlogBuf(t, schemapb.DataType_Int64, fieldsData[106].([]int64)), // this is primary key
		"123/107/a": createBinlogBuf(t, schemapb.DataType_Float, fieldsData[107].([]float32)),
		"123/108/a": createBinlogBuf(t, schemapb.DataType_Double, fieldsData[108].([]float64)),
		"123/109/a": createBinlogBuf(t, schemapb.DataType_VarChar, fieldsData[109].([]string)),
		"123/110/a": createBinlogBuf(t, schemapb.DataType_BinaryVector, fieldsData[110].([][]byte)),
		"123/111/a": createBinlogBuf(t, schemapb.DataType_FloatVector, fieldsData[111].([][]float32)),
		"123/112/a": createBinlogBuf(t, schemapb.DataType_JSON, fieldsData[112].([][]byte)),
	}

	callTime := 0
	updateProgress := func(percent int64) {
		assert.GreaterOrEqual(t, percent, int64(0))
		assert.LessOrEqual(t, percent, int64(100))
		callTime++
	}
	collectionInfo, err = NewCollectionInfo(sampleSchema(), 2, []int64{1})
	assert.NoError(t, err)
	parser, err = NewBinlogParser(ctx, collectionInfo, 1024, chunkManager, flushFunc, updateProgress, 0, math.MaxUint64)
	assert.NotNil(t, parser)
	assert.NoError(t, err)

	err = parser.Parse(paths)
	assert.NoError(t, err)
	assert.Equal(t, 1, callTime)
}

func Test_BinlogParserSkipFlagFile(t *testing.T) {
	ctx := context.Background()

	flushFunc := func(fields BlockData, shardID int, partID int64) error {
		return nil
	}

	chunkManager := &MockChunkManager{
		listErr:    errors.New("error"),
		listResult: make(map[string][]string),
	}

	collectionInfo, err := NewCollectionInfo(sampleSchema(), 2, []int64{1})
	assert.NoError(t, err)

	parser, err := NewBinlogParser(ctx, collectionInfo, 1024, chunkManager, flushFunc, nil, 0, math.MaxUint64)
	assert.NotNil(t, parser)
	assert.NoError(t, err)

	insertPath := "insertPath"
	deltaPath := "deltaPath"

	// chunkManager return error
	holders, err := parser.constructSegmentHolders(insertPath, deltaPath)
	assert.Error(t, err)
	assert.Nil(t, holders)

	// parse field id error(insert log)
	chunkManager.listErr = nil
	chunkManager.listResult[insertPath] = []string{
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/0/435978159903735811",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/.DS_Store",
	}
	_, err = parser.constructSegmentHolders(insertPath, deltaPath)
	assert.NoError(t, err)
}

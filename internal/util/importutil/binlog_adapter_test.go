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
	"encoding/json"
	"math"
	"strconv"
	"testing"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/stretchr/testify/assert"
)

const (
	baseTimestamp = 43757345
)

func createDeltalogBuf(t *testing.T, deleteList interface{}, varcharType bool) []byte {
	deleteData := &storage.DeleteData{
		Pks:      make([]storage.PrimaryKey, 0),
		Tss:      make([]storage.Timestamp, 0),
		RowCount: 0,
	}

	if varcharType {
		deltaData := deleteList.([]string)
		assert.NotNil(t, deltaData)
		for i, id := range deltaData {
			deleteData.Pks = append(deleteData.Pks, storage.NewVarCharPrimaryKey(id))
			deleteData.Tss = append(deleteData.Tss, baseTimestamp+uint64(i))
			deleteData.RowCount++
		}
	} else {
		deltaData := deleteList.([]int64)
		assert.NotNil(t, deltaData)
		for i, id := range deltaData {
			deleteData.Pks = append(deleteData.Pks, storage.NewInt64PrimaryKey(id))
			deleteData.Tss = append(deleteData.Tss, baseTimestamp+uint64(i))
			deleteData.RowCount++
		}
	}

	deleteCodec := storage.NewDeleteCodec()
	blob, err := deleteCodec.Serialize(1, 1, 1, deleteData)
	assert.NoError(t, err)
	assert.NotNil(t, blob)

	return blob.Value
}

func Test_BinlogAdapterNew(t *testing.T) {
	ctx := context.Background()

	// nil schema
	adapter, err := NewBinlogAdapter(ctx, nil, 1024, 2048, nil, nil, 0, math.MaxUint64)
	assert.Nil(t, adapter)
	assert.Error(t, err)

	// too many partitions
	collectionInfo, err := NewCollectionInfo(sampleSchema(), 2, []int64{1})
	assert.NoError(t, err)
	collectionInfo.PartitionIDs = []int64{1, 2}
	adapter, err = NewBinlogAdapter(ctx, collectionInfo, 1024, 2048, nil, nil, 0, math.MaxUint64)
	assert.Nil(t, adapter)
	assert.Error(t, err)

	collectionInfo.PartitionIDs = []int64{1}
	// nil chunkmanager
	adapter, err = NewBinlogAdapter(ctx, collectionInfo, 1024, 2048, nil, nil, 0, math.MaxUint64)
	assert.Nil(t, adapter)
	assert.Error(t, err)

	// nil flushfunc
	adapter, err = NewBinlogAdapter(ctx, collectionInfo, 1024, 2048, &MockChunkManager{}, nil, 0, math.MaxUint64)
	assert.Nil(t, adapter)
	assert.Error(t, err)

	// succeed
	flushFunc := func(fields BlockData, shardID int, partID int64) error {
		return nil
	}
	adapter, err = NewBinlogAdapter(ctx, collectionInfo, 2048, 1024, &MockChunkManager{}, flushFunc, 0, math.MaxUint64)
	assert.NotNil(t, adapter)
	assert.NoError(t, err)

	// amend blockSize, blockSize should less than MaxSegmentSizeInMemory
	adapter, err = NewBinlogAdapter(ctx, collectionInfo, MaxSegmentSizeInMemory+1, 1024, &MockChunkManager{}, flushFunc, 0, math.MaxUint64)
	assert.NotNil(t, adapter)
	assert.NoError(t, err)
	assert.Equal(t, int64(MaxSegmentSizeInMemory), adapter.blockSize)
}

func Test_BinlogAdapterVerify(t *testing.T) {
	ctx := context.Background()

	collectionInfo, err := NewCollectionInfo(sampleSchema(), 2, []int64{1})
	assert.NoError(t, err)

	flushFunc := func(fields BlockData, shardID int, partID int64) error {
		return nil
	}

	adapter, err := NewBinlogAdapter(ctx, collectionInfo, 1024, 2048, &MockChunkManager{}, flushFunc, 0, math.MaxUint64)
	assert.NotNil(t, adapter)
	assert.NoError(t, err)

	// nil input
	err = adapter.verify(nil)
	assert.Error(t, err)

	// empty holder
	holder := &SegmentFilesHolder{}
	err = adapter.verify(holder)
	assert.Error(t, err)

	// row id field missed
	holder.fieldFiles = make(map[int64][]string)
	for i := int64(102); i <= 112; i++ {
		holder.fieldFiles[i] = make([]string, 0)
	}
	err = adapter.verify(holder)
	assert.Error(t, err)

	// timestamp field missed
	holder.fieldFiles[common.RowIDField] = []string{
		"a",
	}

	err = adapter.verify(holder)
	assert.Error(t, err)

	// binlog file count of each field must be equal
	holder.fieldFiles[common.TimeStampField] = []string{
		"a",
	}
	err = adapter.verify(holder)
	assert.Error(t, err)

	// succeed
	for i := int64(102); i <= 112; i++ {
		holder.fieldFiles[i] = []string{
			"a",
		}
	}
	err = adapter.verify(holder)
	assert.NoError(t, err)
}

func Test_BinlogAdapterReadDeltalog(t *testing.T) {
	ctx := context.Background()

	deleteItems := []int64{1001, 1002, 1003}
	buf := createDeltalogBuf(t, deleteItems, false)
	chunkManager := &MockChunkManager{
		readBuf: map[string][]byte{
			"dummy": buf,
		},
	}

	collectionInfo, err := NewCollectionInfo(sampleSchema(), 2, []int64{1})
	assert.NoError(t, err)

	flushFunc := func(fields BlockData, shardID int, partID int64) error {
		return nil
	}

	adapter, err := NewBinlogAdapter(ctx, collectionInfo, 1024, 2048, chunkManager, flushFunc, 0, math.MaxUint64)
	assert.NotNil(t, adapter)
	assert.NoError(t, err)

	// succeed
	deleteLogs, err := adapter.readDeltalog("dummy")
	assert.NoError(t, err)
	assert.Equal(t, len(deleteItems), len(deleteLogs))

	// failed to init BinlogFile
	adapter.chunkManager = nil
	deleteLogs, err = adapter.readDeltalog("dummy")
	assert.Error(t, err)
	assert.Nil(t, deleteLogs)

	// failed to open binlog file
	chunkManager.readErr = errors.New("error")
	adapter.chunkManager = chunkManager
	deleteLogs, err = adapter.readDeltalog("dummy")
	assert.Error(t, err)
	assert.Nil(t, deleteLogs)
}

func Test_BinlogAdapterDecodeDeleteLogs(t *testing.T) {
	ctx := context.Background()

	deleteItems := []int64{1001, 1002, 1003, 1004, 1005}
	buf := createDeltalogBuf(t, deleteItems, false)
	chunkManager := &MockChunkManager{
		readBuf: map[string][]byte{
			"dummy": buf,
		},
	}

	collectionInfo, err := NewCollectionInfo(sampleSchema(), 2, []int64{1})
	assert.NoError(t, err)

	flushFunc := func(fields BlockData, shardID int, partID int64) error {
		return nil
	}

	adapter, err := NewBinlogAdapter(ctx, collectionInfo, 1024, 2048, chunkManager, flushFunc, 0, math.MaxUint64)
	assert.NotNil(t, adapter)
	assert.NoError(t, err)

	holder := &SegmentFilesHolder{
		deltaFiles: []string{
			"dummy",
		},
	}

	// use timetamp to filter the no.1 and no.2 deletions
	adapter.tsEndPoint = baseTimestamp + 1
	deletions, err := adapter.decodeDeleteLogs(holder)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(deletions))

	// wrong data type of delta log
	chunkManager.readBuf = map[string][]byte{
		"dummy": createDeltalogBuf(t, []string{"1001", "1002"}, true),
	}

	adapter, err = NewBinlogAdapter(ctx, collectionInfo, 1024, 2048, chunkManager, flushFunc, 0, math.MaxUint64)
	assert.NotNil(t, adapter)
	assert.NoError(t, err)

	adapter.tsEndPoint = baseTimestamp
	deletions, err = adapter.decodeDeleteLogs(holder)
	assert.Error(t, err)
	assert.Nil(t, deletions)
}

func Test_BinlogAdapterDecodeDeleteLog(t *testing.T) {
	ctx := context.Background()

	collectionInfo, err := NewCollectionInfo(sampleSchema(), 2, []int64{1})
	assert.NoError(t, err)

	flushFunc := func(fields BlockData, shardID int, partID int64) error {
		return nil
	}

	adapter, err := NewBinlogAdapter(ctx, collectionInfo, 1024, 2048, &MockChunkManager{}, flushFunc, 0, math.MaxUint64)
	assert.NotNil(t, adapter)
	assert.NoError(t, err)

	// v2.1 format
	st := &storage.DeleteLog{
		Pk: &storage.Int64PrimaryKey{
			Value: 100,
		},
		Ts:     uint64(450000),
		PkType: 5,
	}

	m, _ := json.Marshal(st)

	del, err := adapter.decodeDeleteLog(string(m))
	assert.NoError(t, err)
	assert.NotNil(t, del)
	assert.True(t, del.Pk.EQ(st.Pk))
	assert.Equal(t, st.Ts, del.Ts)
	assert.Equal(t, st.PkType, del.PkType)

	// v2.0 format
	del, err = adapter.decodeDeleteLog("")
	assert.Nil(t, del)
	assert.Error(t, err)

	del, err = adapter.decodeDeleteLog("a,b")
	assert.Nil(t, del)
	assert.Error(t, err)

	del, err = adapter.decodeDeleteLog("5,b")
	assert.Nil(t, del)
	assert.Error(t, err)

	del, err = adapter.decodeDeleteLog("5,1000")
	assert.NoError(t, err)
	assert.NotNil(t, del)
	assert.True(t, del.Pk.EQ(&storage.Int64PrimaryKey{
		Value: 5,
	}))
	tt, _ := strconv.ParseUint("1000", 10, 64)
	assert.Equal(t, del.Ts, tt)
	assert.Equal(t, del.PkType, int64(schemapb.DataType_Int64))
}

func Test_BinlogAdapterReadDeltalogs(t *testing.T) {
	ctx := context.Background()

	deleteItems := []int64{1001, 1002, 1003, 1004, 1005}
	buf := createDeltalogBuf(t, deleteItems, false)
	chunkManager := &MockChunkManager{
		readBuf: map[string][]byte{
			"dummy": buf,
		},
	}

	collectionInfo, err := NewCollectionInfo(sampleSchema(), 2, []int64{1})
	assert.NoError(t, err)

	flushFunc := func(fields BlockData, shardID int, partID int64) error {
		return nil
	}

	adapter, err := NewBinlogAdapter(ctx, collectionInfo, 1024, 2048, chunkManager, flushFunc, 0, math.MaxUint64)
	assert.NotNil(t, adapter)
	assert.NoError(t, err)

	holder := &SegmentFilesHolder{
		deltaFiles: []string{
			"dummy",
		},
	}

	// 1. int64 primary key, succeed, return the no.1 and no.2 deletion
	t.Run("int64 primary key succeed", func(t *testing.T) {
		adapter.tsEndPoint = baseTimestamp + 1
		intDeletions, strDeletions, err := adapter.readDeltalogs(holder)
		assert.NoError(t, err)
		assert.Nil(t, strDeletions)
		assert.NotNil(t, intDeletions)

		ts, ok := intDeletions[deleteItems[0]]
		assert.True(t, ok)
		assert.Equal(t, uint64(baseTimestamp), ts)

		ts, ok = intDeletions[deleteItems[1]]
		assert.True(t, ok)
		assert.Equal(t, uint64(baseTimestamp+1), ts)
	})

	// 2. varchar primary key, succeed, return the no.1 and no.2 deletetion
	t.Run("varchar primary key succeed", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name: "schema",
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      101,
					Name:         "ID",
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_VarChar,
				},
			},
		}
		collectionInfo.resetSchema(schema)

		chunkManager.readBuf = map[string][]byte{
			"dummy": createDeltalogBuf(t, []string{"1001", "1002"}, true),
		}

		adapter, err = NewBinlogAdapter(ctx, collectionInfo, 1024, 2048, chunkManager, flushFunc, 0, math.MaxUint64)
		assert.NotNil(t, adapter)
		assert.NoError(t, err)

		// 2.1 all deletion have been filtered out
		adapter.tsStartPoint = baseTimestamp + 2
		intDeletions, strDeletions, err := adapter.readDeltalogs(holder)
		assert.NoError(t, err)
		assert.Nil(t, intDeletions)
		assert.Nil(t, strDeletions)

		// 2.2 filter the no.1 and no.2 deletion
		adapter.tsStartPoint = 0
		adapter.tsEndPoint = baseTimestamp + 1
		intDeletions, strDeletions, err = adapter.readDeltalogs(holder)
		assert.NoError(t, err)
		assert.Nil(t, intDeletions)
		assert.NotNil(t, strDeletions)

		ts, ok := strDeletions["1001"]
		assert.True(t, ok)
		assert.Equal(t, uint64(baseTimestamp), ts)

		ts, ok = strDeletions["1002"]
		assert.True(t, ok)
		assert.Equal(t, uint64(baseTimestamp+1), ts)
	})

	// 3. unsupported primary key type
	t.Run("unsupported primary key type", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name: "schema",
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      101,
					Name:         "ID",
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_Float,
				},
			},
		}
		collectionInfo.resetSchema(schema)

		adapter, err = NewBinlogAdapter(ctx, collectionInfo, 1024, 2048, chunkManager, flushFunc, 0, math.MaxUint64)
		assert.NotNil(t, adapter)
		assert.NoError(t, err)

		adapter.tsEndPoint = baseTimestamp + 1
		intDeletions, strDeletions, err := adapter.readDeltalogs(holder)
		assert.Error(t, err)
		assert.Nil(t, intDeletions)
		assert.Nil(t, strDeletions)
	})
}

func Test_BinlogAdapterReadTimestamp(t *testing.T) {
	ctx := context.Background()

	collectionInfo, err := NewCollectionInfo(sampleSchema(), 2, []int64{1})
	assert.NoError(t, err)

	flushFunc := func(fields BlockData, shardID int, partID int64) error {
		return nil
	}
	adapter, err := NewBinlogAdapter(ctx, collectionInfo, 1024, 2048, &MockChunkManager{}, flushFunc, 0, math.MaxUint64)
	assert.NotNil(t, adapter)
	assert.NoError(t, err)

	// new BinglogFile error
	adapter.chunkManager = nil
	ts, err := adapter.readTimestamp("dummy")
	assert.Nil(t, ts)
	assert.Error(t, err)

	// open binlog file error
	chunkManager := &MockChunkManager{
		readBuf: make(map[string][]byte),
	}
	adapter.chunkManager = chunkManager
	ts, err = adapter.readTimestamp("dummy")
	assert.Nil(t, ts)
	assert.Error(t, err)

	// succeed
	rowCount := 10
	fieldsData := createFieldsData(sampleSchema(), rowCount)
	chunkManager.readBuf["dummy"] = createBinlogBuf(t, schemapb.DataType_Int64, fieldsData[106].([]int64))
	ts, err = adapter.readTimestamp("dummy")
	assert.NoError(t, err)
	assert.NotNil(t, ts)
	assert.Equal(t, rowCount, len(ts))
}

func Test_BinlogAdapterReadPrimaryKeys(t *testing.T) {
	ctx := context.Background()

	collectionInfo, err := NewCollectionInfo(sampleSchema(), 2, []int64{1})
	assert.NoError(t, err)

	flushFunc := func(fields BlockData, shardID int, partID int64) error {
		return nil
	}
	adapter, err := NewBinlogAdapter(ctx, collectionInfo, 1024, 2048, &MockChunkManager{}, flushFunc, 0, math.MaxUint64)
	assert.NotNil(t, adapter)
	assert.NoError(t, err)

	// new BinglogFile error
	adapter.chunkManager = nil
	intList, strList, err := adapter.readPrimaryKeys("dummy")
	assert.Nil(t, intList)
	assert.Nil(t, strList)
	assert.Error(t, err)

	// open binlog file error
	chunkManager := &MockChunkManager{
		readBuf: make(map[string][]byte),
	}
	adapter.chunkManager = chunkManager
	intList, strList, err = adapter.readPrimaryKeys("dummy")
	assert.Nil(t, intList)
	assert.Nil(t, strList)
	assert.Error(t, err)

	// wrong primary key type
	rowCount := 10
	fieldsData := createFieldsData(sampleSchema(), rowCount)
	chunkManager.readBuf["dummy"] = createBinlogBuf(t, schemapb.DataType_Bool, fieldsData[102].([]bool))

	adapter.collectionInfo.PrimaryKey.DataType = schemapb.DataType_Bool
	intList, strList, err = adapter.readPrimaryKeys("dummy")
	assert.Nil(t, intList)
	assert.Nil(t, strList)
	assert.Error(t, err)

	// succeed int64
	adapter.collectionInfo.PrimaryKey.DataType = schemapb.DataType_Int64
	chunkManager.readBuf["dummy"] = createBinlogBuf(t, schemapb.DataType_Int64, fieldsData[106].([]int64))
	intList, strList, err = adapter.readPrimaryKeys("dummy")
	assert.NotNil(t, intList)
	assert.Nil(t, strList)
	assert.NoError(t, err)
	assert.Equal(t, rowCount, len(intList))

	// succeed varchar
	adapter.collectionInfo.PrimaryKey.DataType = schemapb.DataType_VarChar
	chunkManager.readBuf["dummy"] = createBinlogBuf(t, schemapb.DataType_VarChar, fieldsData[109].([]string))
	intList, strList, err = adapter.readPrimaryKeys("dummy")
	assert.Nil(t, intList)
	assert.NotNil(t, strList)
	assert.NoError(t, err)
	assert.Equal(t, rowCount, len(strList))
}

func Test_BinlogAdapterShardListInt64(t *testing.T) {
	ctx := context.Background()

	flushFunc := func(fields BlockData, shardID int, partID int64) error {
		return nil
	}

	shardNum := int32(2)
	collectionInfo, err := NewCollectionInfo(sampleSchema(), shardNum, []int64{1})
	assert.NoError(t, err)

	adapter, err := NewBinlogAdapter(ctx, collectionInfo, 1024, 2048, &MockChunkManager{}, flushFunc, 0, math.MaxUint64)
	assert.NotNil(t, adapter)
	assert.NoError(t, err)

	fieldsData := createFieldsData(sampleSchema(), 0)
	shardsData := createShardsData(sampleSchema(), fieldsData, shardNum, []int64{1})

	// wrong input
	shardList, err := adapter.getShardingListByPrimaryInt64([]int64{1}, []int64{1, 2}, shardsData, map[int64]uint64{})
	assert.Nil(t, shardList)
	assert.Error(t, err)

	// succeed
	// 5 ids, delete two items, the ts end point is 25, there shardList should be [-1, 0, 1, -1, -1]
	adapter.tsEndPoint = 30
	idList := []int64{1, 2, 3, 4, 5}
	tsList := []int64{10, 20, 30, 40, 50}
	deletion := map[int64]uint64{
		1: 23,
		4: 36,
	}
	shardList, err = adapter.getShardingListByPrimaryInt64(idList, tsList, shardsData, deletion)
	assert.NoError(t, err)
	assert.NotNil(t, shardList)
	correctShardList := []int32{-1, 0, 1, -1, -1}
	assert.Equal(t, len(correctShardList), len(shardList))
	for i := 0; i < len(shardList); i++ {
		assert.Equal(t, correctShardList[i], shardList[i])
	}
}

func Test_BinlogAdapterShardListVarchar(t *testing.T) {
	ctx := context.Background()

	flushFunc := func(fields BlockData, shardID int, partID int64) error {
		return nil
	}

	shardNum := int32(2)
	collectionInfo, err := NewCollectionInfo(strKeySchema(), shardNum, []int64{1})
	assert.NoError(t, err)

	adapter, err := NewBinlogAdapter(ctx, collectionInfo, 1024, 2048, &MockChunkManager{}, flushFunc, 0, math.MaxUint64)
	assert.NotNil(t, adapter)
	assert.NoError(t, err)

	fieldsData := createFieldsData(strKeySchema(), 0)
	shardsData := createShardsData(strKeySchema(), fieldsData, shardNum, []int64{1})
	// wrong input
	shardList, err := adapter.getShardingListByPrimaryVarchar([]string{"1"}, []int64{1, 2}, shardsData, map[string]uint64{})
	assert.Nil(t, shardList)
	assert.Error(t, err)

	// succeed
	// 5 ids, delete two items, the ts end point is 25, there shardList should be [-1, 1, 1, -1, -1]
	adapter.tsEndPoint = 30
	idList := []string{"1", "2", "3", "4", "5"}
	tsList := []int64{10, 20, 30, 40, 50}
	deletion := map[string]uint64{
		"1": 23,
		"4": 36,
	}
	shardList, err = adapter.getShardingListByPrimaryVarchar(idList, tsList, shardsData, deletion)
	assert.NoError(t, err)
	assert.NotNil(t, shardList)
	correctShardList := []int32{-1, 1, 1, -1, -1}
	assert.Equal(t, len(correctShardList), len(shardList))
	for i := 0; i < len(shardList); i++ {
		assert.Equal(t, correctShardList[i], shardList[i])
	}
}

func Test_BinlogAdapterReadInt64PK(t *testing.T) {
	ctx := context.Background()

	chunkManager := &MockChunkManager{}

	flushCounter := 0
	flushRowCount := 0
	partitionID := int64(1)
	flushFunc := func(fields BlockData, shardID int, partID int64) error {
		assert.Equal(t, partitionID, partID)
		flushCounter++
		rowCount := 0
		for _, v := range fields {
			rowCount = v.RowNum()
			break
		}
		flushRowCount += rowCount
		for _, v := range fields {
			assert.Equal(t, rowCount, v.RowNum())
		}
		return nil
	}

	shardNum := int32(2)
	collectionInfo, err := NewCollectionInfo(sampleSchema(), shardNum, []int64{partitionID})
	assert.NoError(t, err)

	adapter, err := NewBinlogAdapter(ctx, collectionInfo, 1024, 2048, chunkManager, flushFunc, 0, math.MaxUint64)
	assert.NotNil(t, adapter)
	assert.NoError(t, err)
	adapter.tsEndPoint = baseTimestamp + 1

	// nil holder
	err = adapter.Read(nil)
	assert.Error(t, err)

	// verify failed
	holder := &SegmentFilesHolder{}
	err = adapter.Read(holder)
	assert.Error(t, err)

	// failed to read delta log
	holder.fieldFiles = map[int64][]string{
		int64(0):   {"0_insertlog"},
		int64(1):   {"1_insertlog"},
		int64(102): {"102_insertlog"},
		int64(103): {"103_insertlog"},
		int64(104): {"104_insertlog"},
		int64(105): {"105_insertlog"},
		int64(106): {"106_insertlog"},
		int64(107): {"107_insertlog"},
		int64(108): {"108_insertlog"},
		int64(109): {"109_insertlog"},
		int64(110): {"110_insertlog"},
		int64(111): {"111_insertlog"},
		int64(112): {"112_insertlog"},
	}
	holder.deltaFiles = []string{"deltalog"}
	err = adapter.Read(holder)
	assert.Error(t, err)

	// prepare binlog data
	rowCount := 1000
	fieldsData := createFieldsData(sampleSchema(), rowCount)
	deletedItems := []int64{41, 51, 100, 400, 600}

	chunkManager.readBuf = map[string][]byte{
		"102_insertlog": createBinlogBuf(t, schemapb.DataType_Bool, fieldsData[102].([]bool)),
		"103_insertlog": createBinlogBuf(t, schemapb.DataType_Int8, fieldsData[103].([]int8)),
		"104_insertlog": createBinlogBuf(t, schemapb.DataType_Int16, fieldsData[104].([]int16)),
		"105_insertlog": createBinlogBuf(t, schemapb.DataType_Int32, fieldsData[105].([]int32)),
		"106_insertlog": createBinlogBuf(t, schemapb.DataType_Int64, fieldsData[106].([]int64)), // this is primary key
		"107_insertlog": createBinlogBuf(t, schemapb.DataType_Float, fieldsData[107].([]float32)),
		"108_insertlog": createBinlogBuf(t, schemapb.DataType_Double, fieldsData[108].([]float64)),
		"109_insertlog": createBinlogBuf(t, schemapb.DataType_VarChar, fieldsData[109].([]string)),
		"110_insertlog": createBinlogBuf(t, schemapb.DataType_BinaryVector, fieldsData[110].([][]byte)),
		"111_insertlog": createBinlogBuf(t, schemapb.DataType_FloatVector, fieldsData[111].([][]float32)),
		"112_insertlog": createBinlogBuf(t, schemapb.DataType_JSON, fieldsData[112].([][]byte)),
		"deltalog":      createDeltalogBuf(t, deletedItems, false),
	}

	// failed to read primary keys
	err = adapter.Read(holder)
	assert.Error(t, err)

	// failed to read timestamp field
	chunkManager.readBuf["0_insertlog"] = createBinlogBuf(t, schemapb.DataType_Int64, fieldsData[0].([]int64))
	err = adapter.Read(holder)
	assert.Error(t, err)

	// succeed flush
	chunkManager.readBuf["1_insertlog"] = createBinlogBuf(t, schemapb.DataType_Int64, fieldsData[1].([]int64))

	adapter.tsEndPoint = baseTimestamp + uint64(499) // 4 entities deleted, 500 entities excluded
	err = adapter.Read(holder)
	assert.NoError(t, err)
	assert.Equal(t, shardNum, int32(flushCounter))
	assert.Equal(t, rowCount-4-500, flushRowCount)
}

func Test_BinlogAdapterReadVarcharPK(t *testing.T) {
	ctx := context.Background()

	chunkManager := &MockChunkManager{}

	flushCounter := 0
	flushRowCount := 0
	partitionID := int64(1)
	flushFunc := func(fields BlockData, shardID int, partID int64) error {
		assert.Equal(t, partitionID, partID)
		flushCounter++
		rowCount := 0
		for _, v := range fields {
			rowCount = v.RowNum()
			break
		}
		flushRowCount += rowCount
		for _, v := range fields {
			assert.Equal(t, rowCount, v.RowNum())
		}
		return nil
	}

	// prepare data
	holder := &SegmentFilesHolder{}
	holder.fieldFiles = map[int64][]string{
		int64(0):   {"0_insertlog"},
		int64(1):   {"1_insertlog"},
		int64(101): {"101_insertlog"},
		int64(102): {"102_insertlog"},
		int64(103): {"103_insertlog"},
		int64(104): {"104_insertlog"},
		int64(105): {"105_insertlog"},
		int64(106): {"106_insertlog"},
	}
	holder.deltaFiles = []string{"deltalog"}

	rowIDData := make([]int64, 0)
	timestampData := make([]int64, 0)
	pkData := make([]string, 0)
	int32Data := make([]int32, 0)
	floatData := make([]float32, 0)
	varcharData := make([]string, 0)
	boolData := make([]bool, 0)
	floatVecData := make([][]float32, 0)

	boolFunc := func(i int) bool {
		return i%3 != 0
	}

	rowCount := 1000
	for i := 0; i < rowCount; i++ {
		rowIDData = append(rowIDData, int64(i))
		timestampData = append(timestampData, baseTimestamp+int64(i))
		pkData = append(pkData, strconv.Itoa(i)) // primary key
		int32Data = append(int32Data, int32(i%1000))
		floatData = append(floatData, float32(i/2))
		varcharData = append(varcharData, "no."+strconv.Itoa(i))
		boolData = append(boolData, boolFunc(i))
		floatVecData = append(floatVecData, []float32{float32(i / 2), float32(i / 4), float32(i / 5), float32(i / 8)}) // dim = 4
	}

	deletedItems := []string{"1", "100", "999"}

	chunkManager.readBuf = map[string][]byte{
		"0_insertlog":   createBinlogBuf(t, schemapb.DataType_Int64, rowIDData),
		"1_insertlog":   createBinlogBuf(t, schemapb.DataType_Int64, timestampData),
		"101_insertlog": createBinlogBuf(t, schemapb.DataType_VarChar, pkData),
		"102_insertlog": createBinlogBuf(t, schemapb.DataType_Int32, int32Data),
		"103_insertlog": createBinlogBuf(t, schemapb.DataType_Float, floatData),
		"104_insertlog": createBinlogBuf(t, schemapb.DataType_VarChar, varcharData),
		"105_insertlog": createBinlogBuf(t, schemapb.DataType_Bool, boolData),
		"106_insertlog": createBinlogBuf(t, schemapb.DataType_FloatVector, floatVecData),
		"deltalog":      createDeltalogBuf(t, deletedItems, true),
	}

	// succeed
	shardNum := int32(3)
	collectionInfo, err := NewCollectionInfo(strKeySchema(), shardNum, []int64{partitionID})
	assert.NoError(t, err)

	adapter, err := NewBinlogAdapter(ctx, collectionInfo, 1024, 2048, chunkManager, flushFunc, 0, math.MaxUint64)
	assert.NotNil(t, adapter)
	assert.NoError(t, err)

	adapter.tsEndPoint = baseTimestamp + uint64(499) // 3 entities deleted, 500 entities excluded, the "999" is excluded, so totally 502 entities skipped
	err = adapter.Read(holder)
	assert.NoError(t, err)
	assert.Equal(t, shardNum, int32(flushCounter))
	assert.Equal(t, rowCount-502, flushRowCount)
}

func Test_BinlogAdapterDispatch(t *testing.T) {
	ctx := context.Background()

	flushFunc := func(fields BlockData, shardID int, partID int64) error {
		return nil
	}
	shardNum := int32(3)
	collectionInfo, err := NewCollectionInfo(sampleSchema(), shardNum, []int64{1})
	assert.NoError(t, err)

	adapter, err := NewBinlogAdapter(ctx, collectionInfo, 1024, 2048, &MockChunkManager{}, flushFunc, 0, math.MaxUint64)
	assert.NotNil(t, adapter)
	assert.NoError(t, err)

	// prepare empty in-memory segments data
	partitionID := int64(1)
	fieldsData := createFieldsData(sampleSchema(), 0)
	shardsData := createShardsData(sampleSchema(), fieldsData, shardNum, []int64{partitionID})

	shardList := []int32{0, -1, 1}
	t.Run("dispatch bool data", func(t *testing.T) {
		fieldID := int64(102)
		// row count mismatch
		err = adapter.dispatchBoolToShards([]bool{true}, shardsData, shardList, fieldID)
		assert.Error(t, err)
		for _, shardData := range shardsData {
			assert.Equal(t, 0, shardData[partitionID][fieldID].RowNum())
		}

		// illegal shard ID
		err = adapter.dispatchBoolToShards([]bool{true}, shardsData, []int32{9}, fieldID)
		assert.Error(t, err)

		// succeed
		err = adapter.dispatchBoolToShards([]bool{true, false, false}, shardsData, shardList, fieldID)
		assert.NoError(t, err)
		assert.Equal(t, 1, shardsData[0][partitionID][fieldID].RowNum())
		assert.Equal(t, 1, shardsData[1][partitionID][fieldID].RowNum())
		assert.Equal(t, 0, shardsData[2][partitionID][fieldID].RowNum())
	})

	t.Run("dispatch int8 data", func(t *testing.T) {
		fieldID := int64(103)
		// row count mismatch
		err = adapter.dispatchInt8ToShards([]int8{1, 2, 3, 4}, shardsData, shardList, fieldID)
		assert.Error(t, err)
		for _, segment := range shardsData {
			assert.Equal(t, 0, segment[partitionID][fieldID].RowNum())
		}

		// illegal shard ID
		err = adapter.dispatchInt8ToShards([]int8{1}, shardsData, []int32{9}, fieldID)
		assert.Error(t, err)

		// succeed
		err = adapter.dispatchInt8ToShards([]int8{1, 2, 3}, shardsData, shardList, fieldID)
		assert.NoError(t, err)
		assert.Equal(t, 1, shardsData[0][partitionID][fieldID].RowNum())
		assert.Equal(t, 1, shardsData[1][partitionID][fieldID].RowNum())
		assert.Equal(t, 0, shardsData[2][partitionID][fieldID].RowNum())
	})

	t.Run("dispatch int16 data", func(t *testing.T) {
		fieldID := int64(104)
		// row count mismatch
		err = adapter.dispatchInt16ToShards([]int16{1, 2, 3, 4}, shardsData, shardList, fieldID)
		assert.Error(t, err)
		for _, shardData := range shardsData {
			assert.Equal(t, 0, shardData[partitionID][fieldID].RowNum())
		}

		// illegal shard ID
		err = adapter.dispatchInt16ToShards([]int16{1}, shardsData, []int32{9}, fieldID)
		assert.Error(t, err)

		// succeed
		err = adapter.dispatchInt16ToShards([]int16{1, 2, 3}, shardsData, shardList, fieldID)
		assert.NoError(t, err)
		assert.Equal(t, 1, shardsData[0][partitionID][fieldID].RowNum())
		assert.Equal(t, 1, shardsData[1][partitionID][fieldID].RowNum())
		assert.Equal(t, 0, shardsData[2][partitionID][fieldID].RowNum())
	})

	t.Run("dispatch int32 data", func(t *testing.T) {
		fieldID := int64(105)
		// row count mismatch
		err = adapter.dispatchInt32ToShards([]int32{1, 2, 3, 4}, shardsData, shardList, fieldID)
		assert.Error(t, err)
		for _, shardData := range shardsData {
			assert.Equal(t, 0, shardData[partitionID][fieldID].RowNum())
		}

		// illegal shard ID
		err = adapter.dispatchInt32ToShards([]int32{1}, shardsData, []int32{9}, fieldID)
		assert.Error(t, err)

		// succeed
		err = adapter.dispatchInt32ToShards([]int32{1, 2, 3}, shardsData, shardList, fieldID)
		assert.NoError(t, err)
		assert.Equal(t, 1, shardsData[0][partitionID][fieldID].RowNum())
		assert.Equal(t, 1, shardsData[1][partitionID][fieldID].RowNum())
		assert.Equal(t, 0, shardsData[2][partitionID][fieldID].RowNum())
	})

	t.Run("dispatch int64 data", func(t *testing.T) {
		fieldID := int64(106)
		// row count mismatch
		err = adapter.dispatchInt64ToShards([]int64{1, 2, 3, 4}, shardsData, shardList, fieldID)
		assert.Error(t, err)
		for _, shardData := range shardsData {
			assert.Equal(t, 0, shardData[partitionID][fieldID].RowNum())
		}

		// illegal shard ID
		err = adapter.dispatchInt64ToShards([]int64{1}, shardsData, []int32{9}, fieldID)
		assert.Error(t, err)

		// succeed
		err = adapter.dispatchInt64ToShards([]int64{1, 2, 3}, shardsData, shardList, fieldID)
		assert.NoError(t, err)
		assert.Equal(t, 1, shardsData[0][partitionID][fieldID].RowNum())
		assert.Equal(t, 1, shardsData[1][partitionID][fieldID].RowNum())
		assert.Equal(t, 0, shardsData[2][partitionID][fieldID].RowNum())
	})

	t.Run("dispatch float data", func(t *testing.T) {
		fieldID := int64(107)
		// row count mismatch
		err = adapter.dispatchFloatToShards([]float32{1, 2, 3, 4}, shardsData, shardList, fieldID)
		assert.Error(t, err)
		for _, shardData := range shardsData {
			assert.Equal(t, 0, shardData[partitionID][fieldID].RowNum())
		}

		// illegal shard ID
		err = adapter.dispatchFloatToShards([]float32{1}, shardsData, []int32{9}, fieldID)
		assert.Error(t, err)

		// succeed
		err = adapter.dispatchFloatToShards([]float32{1, 2, 3}, shardsData, shardList, fieldID)
		assert.NoError(t, err)
		assert.Equal(t, 1, shardsData[0][partitionID][fieldID].RowNum())
		assert.Equal(t, 1, shardsData[1][partitionID][fieldID].RowNum())
		assert.Equal(t, 0, shardsData[2][partitionID][fieldID].RowNum())
	})

	t.Run("dispatch double data", func(t *testing.T) {
		fieldID := int64(108)
		// row count mismatch
		err = adapter.dispatchDoubleToShards([]float64{1, 2, 3, 4}, shardsData, shardList, fieldID)
		assert.Error(t, err)
		for _, shardData := range shardsData {
			assert.Equal(t, 0, shardData[partitionID][fieldID].RowNum())
		}

		// illegal shard ID
		err = adapter.dispatchDoubleToShards([]float64{1}, shardsData, []int32{9}, fieldID)
		assert.Error(t, err)

		// succeed
		err = adapter.dispatchDoubleToShards([]float64{1, 2, 3}, shardsData, shardList, fieldID)
		assert.NoError(t, err)
		assert.Equal(t, 1, shardsData[0][partitionID][fieldID].RowNum())
		assert.Equal(t, 1, shardsData[1][partitionID][fieldID].RowNum())
		assert.Equal(t, 0, shardsData[2][partitionID][fieldID].RowNum())
	})

	t.Run("dispatch varchar data", func(t *testing.T) {
		fieldID := int64(109)
		// row count mismatch
		err = adapter.dispatchVarcharToShards([]string{"a", "b", "c", "d"}, shardsData, shardList, fieldID)
		assert.Error(t, err)
		for _, shardData := range shardsData {
			assert.Equal(t, 0, shardData[partitionID][fieldID].RowNum())
		}

		// illegal shard ID
		err = adapter.dispatchVarcharToShards([]string{"a"}, shardsData, []int32{9}, fieldID)
		assert.Error(t, err)

		// succeed
		err = adapter.dispatchVarcharToShards([]string{"a", "b", "c"}, shardsData, shardList, fieldID)
		assert.NoError(t, err)
		assert.Equal(t, 1, shardsData[0][partitionID][fieldID].RowNum())
		assert.Equal(t, 1, shardsData[1][partitionID][fieldID].RowNum())
		assert.Equal(t, 0, shardsData[2][partitionID][fieldID].RowNum())
	})

	t.Run("dispatch JSON data", func(t *testing.T) {
		fieldID := int64(112)
		// row count mismatch
		data := [][]byte{[]byte("{\"x\": 3, \"y\": 10.5}"), []byte("{\"y\": true}"), []byte("{\"z\": \"hello\"}"), []byte("{}")}
		err = adapter.dispatchBytesToShards(data, shardsData, shardList, fieldID)
		assert.Error(t, err)
		for _, shardData := range shardsData {
			assert.Equal(t, 0, shardData[partitionID][fieldID].RowNum())
		}

		// illegal shard ID
		err = adapter.dispatchBytesToShards(data, shardsData, []int32{9, 1, 0, 2}, fieldID)
		assert.Error(t, err)

		// succeed
		err = adapter.dispatchBytesToShards([][]byte{[]byte("{}"), []byte("{}"), []byte("{}")}, shardsData, shardList, fieldID)
		assert.NoError(t, err)
		assert.Equal(t, 1, shardsData[0][partitionID][fieldID].RowNum())
		assert.Equal(t, 1, shardsData[1][partitionID][fieldID].RowNum())
		assert.Equal(t, 0, shardsData[2][partitionID][fieldID].RowNum())
	})

	t.Run("dispatch binary vector data", func(t *testing.T) {
		fieldID := int64(110)
		// row count mismatch
		err = adapter.dispatchBinaryVecToShards([]byte{1, 2, 3, 4}, 16, shardsData, shardList, fieldID)
		assert.Error(t, err)
		for _, shardData := range shardsData {
			assert.Equal(t, 0, shardData[partitionID][fieldID].RowNum())
		}

		// illegal shard ID
		err = adapter.dispatchBinaryVecToShards([]byte{1, 2}, 16, shardsData, []int32{9}, fieldID)
		assert.Error(t, err)

		// dimension mismatch
		err = adapter.dispatchBinaryVecToShards([]byte{1}, 8, shardsData, []int32{0}, fieldID)
		assert.Error(t, err)

		// succeed
		err = adapter.dispatchBinaryVecToShards([]byte{1, 2, 3, 4, 5, 6}, 16, shardsData, shardList, fieldID)
		assert.NoError(t, err)
		assert.Equal(t, 1, shardsData[0][partitionID][fieldID].RowNum())
		assert.Equal(t, 1, shardsData[1][partitionID][fieldID].RowNum())
		assert.Equal(t, 0, shardsData[2][partitionID][fieldID].RowNum())
	})

	t.Run("dispatch float vector data", func(t *testing.T) {
		fieldID := int64(111)
		// row count mismatch
		err = adapter.dispatchFloatVecToShards([]float32{1, 2, 3, 4}, 4, shardsData, shardList, fieldID)
		assert.Error(t, err)
		for _, shardData := range shardsData {
			assert.Equal(t, 0, shardData[partitionID][fieldID].RowNum())
		}

		// illegal shard ID
		err = adapter.dispatchFloatVecToShards([]float32{1, 2, 3, 4}, 4, shardsData, []int32{9}, fieldID)
		assert.Error(t, err)

		// dimension mismatch
		err = adapter.dispatchFloatVecToShards([]float32{1, 2, 3, 4, 5}, 5, shardsData, []int32{0}, fieldID)
		assert.Error(t, err)

		// succeed
		err = adapter.dispatchFloatVecToShards([]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, 4, shardsData, shardList, fieldID)
		assert.NoError(t, err)
		assert.Equal(t, 1, shardsData[0][partitionID][fieldID].RowNum())
		assert.Equal(t, 1, shardsData[1][partitionID][fieldID].RowNum())
		assert.Equal(t, 0, shardsData[2][partitionID][fieldID].RowNum())
	})
}

func Test_BinlogAdapterVerifyField(t *testing.T) {
	ctx := context.Background()

	shardNum := int32(2)
	partitionID := int64(1)
	fieldsData := createFieldsData(sampleSchema(), 0)
	shardsData := createShardsData(sampleSchema(), fieldsData, shardNum, []int64{partitionID})

	flushFunc := func(fields BlockData, shardID int, partID int64) error {
		return nil
	}
	collectionInfo, err := NewCollectionInfo(sampleSchema(), shardNum, []int64{1})
	assert.NoError(t, err)

	adapter, err := NewBinlogAdapter(ctx, collectionInfo, 1024, 2048, &MockChunkManager{}, flushFunc, 0, math.MaxUint64)
	assert.NotNil(t, adapter)
	assert.NoError(t, err)

	err = adapter.verifyField(103, shardsData)
	assert.NoError(t, err)
	err = adapter.verifyField(999999, shardsData)
	assert.Error(t, err)

	err = adapter.readInsertlog(999999, "dummy", shardsData, []int32{1})
	assert.Error(t, err)
}

func Test_BinlogAdapterReadInsertlog(t *testing.T) {
	ctx := context.Background()

	shardNum := int32(2)
	partitionID := int64(1)
	fieldsData := createFieldsData(sampleSchema(), 0)
	shardsData := createShardsData(sampleSchema(), fieldsData, shardNum, []int64{partitionID})

	flushFunc := func(fields BlockData, shardID int, partID int64) error {
		return nil
	}
	collectionInfo, err := NewCollectionInfo(sampleSchema(), shardNum, []int64{1})
	assert.NoError(t, err)

	adapter, err := NewBinlogAdapter(ctx, collectionInfo, 1024, 2048, &MockChunkManager{}, flushFunc, 0, math.MaxUint64)
	assert.NotNil(t, adapter)
	assert.NoError(t, err)

	// new BinglogFile error
	adapter.chunkManager = nil
	err = adapter.readInsertlog(102, "dummy", shardsData, []int32{1})
	assert.Error(t, err)

	// open binlog file error
	chunkManager := &MockChunkManager{
		readBuf: make(map[string][]byte),
	}
	adapter.chunkManager = chunkManager
	err = adapter.readInsertlog(102, "dummy", shardsData, []int32{1})
	assert.Error(t, err)

	// verify field error
	err = adapter.readInsertlog(1, "dummy", shardsData, []int32{1})
	assert.Error(t, err)

	// prepare binlog data
	rowCount := 3
	fieldsData = createFieldsData(sampleSchema(), rowCount)

	failedFunc := func(fieldID int64, fieldName string, fieldType schemapb.DataType, wrongField int64, wrongType schemapb.DataType) {
		// row count mismatch
		chunkManager.readBuf[fieldName] = createBinlogBuf(t, fieldType, fieldsData[fieldID])
		err = adapter.readInsertlog(fieldID, fieldName, shardsData, []int32{1})
		assert.Error(t, err)

		// wrong file type
		chunkManager.readBuf[fieldName] = createBinlogBuf(t, wrongType, fieldsData[wrongField])
		err = adapter.readInsertlog(fieldID, fieldName, shardsData, []int32{0, 1, 1})
		assert.Error(t, err)
	}

	t.Run("failed to dispatch bool data", func(t *testing.T) {
		failedFunc(102, "bool", schemapb.DataType_Bool, 111, schemapb.DataType_FloatVector)
	})

	t.Run("failed to dispatch int8 data", func(t *testing.T) {
		failedFunc(103, "int8", schemapb.DataType_Int8, 102, schemapb.DataType_Bool)
	})

	t.Run("failed to dispatch int16 data", func(t *testing.T) {
		failedFunc(104, "int16", schemapb.DataType_Int16, 103, schemapb.DataType_Int8)
	})

	t.Run("failed to dispatch int32 data", func(t *testing.T) {
		failedFunc(105, "int32", schemapb.DataType_Int32, 104, schemapb.DataType_Int16)
	})

	t.Run("failed to dispatch int64 data", func(t *testing.T) {
		failedFunc(106, "int64", schemapb.DataType_Int64, 105, schemapb.DataType_Int32)
	})

	t.Run("failed to dispatch float data", func(t *testing.T) {
		failedFunc(107, "float", schemapb.DataType_Float, 106, schemapb.DataType_Int64)
	})

	t.Run("failed to dispatch double data", func(t *testing.T) {
		failedFunc(108, "double", schemapb.DataType_Double, 107, schemapb.DataType_Float)
	})

	t.Run("failed to dispatch varchar data", func(t *testing.T) {
		failedFunc(109, "varchar", schemapb.DataType_VarChar, 108, schemapb.DataType_Double)
	})

	t.Run("failed to dispatch JSON data", func(t *testing.T) {
		failedFunc(112, "JSON", schemapb.DataType_JSON, 109, schemapb.DataType_VarChar)
	})

	t.Run("failed to dispatch binvector data", func(t *testing.T) {
		failedFunc(110, "binvector", schemapb.DataType_BinaryVector, 112, schemapb.DataType_JSON)
	})

	t.Run("failed to dispatch floatvector data", func(t *testing.T) {
		failedFunc(111, "floatvector", schemapb.DataType_FloatVector, 110, schemapb.DataType_BinaryVector)
	})

	// succeed
	chunkManager.readBuf["int32"] = createBinlogBuf(t, schemapb.DataType_Int32, fieldsData[105].([]int32))
	err = adapter.readInsertlog(105, "int32", shardsData, []int32{0, 1, 1})
	assert.NoError(t, err)
}

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
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type hashFunc func(row map[int64]interface{}) (int64, int64)

func getHashFunc(schema *schemapb.CollectionSchema, chanNum, partitionNum int) (hashFunc, error) {
	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}
	partKeyField, err := typeutil.GetPartitionKeyFieldSchema(schema)
	if err != nil {
		return hashByChannels(int64(chanNum), pkField), nil
	}
	return hashByChannelsAndPartitionKey(int64(chanNum), int64(partitionNum), pkField, partKeyField), nil
}

func hashByChannels(channelNum int64, pkField *schemapb.FieldSchema) hashFunc {
	fn1 := hashFuncBasic(pkField.GetDataType())
	return func(row map[int64]interface{}) (int64, int64) {
		pk1 := row[pkField.GetFieldID()]
		p1 := fn1(pk1, channelNum)
		return p1, 0
	}
}

func hashByChannelsAndPartitionKey(channelNum, partitionNum int64,
	pkField, partField *schemapb.FieldSchema) hashFunc {
	fn1 := hashFuncBasic(pkField.GetDataType())
	fn2 := hashFuncBasic(partField.GetDataType())
	return func(row map[int64]interface{}) (int64, int64) {
		pk1 := row[pkField.GetFieldID()]
		pk2 := row[partField.GetFieldID()]
		p1 := fn1(pk1, channelNum)
		p2 := fn2(pk2, partitionNum)
		return p1, p2
	}
}

func hashFuncBasic(dataType schemapb.DataType) func(pk interface{}, shardNum int64) int64 {
	switch dataType {
	case schemapb.DataType_Int64:
		return func(pk interface{}, shardNum int64) int64 {
			hash, _ := typeutil.Hash32Int64(pk.(int64))
			return int64(hash) % shardNum
		}
	case schemapb.DataType_VarChar:
		return func(pk interface{}, shardNum int64) int64 {
			hash := typeutil.HashString2Uint32(pk.(string))
			return int64(hash) % shardNum
		}
	default:
		return nil
	}
}

func GetHashedRowsCount(task Task, insertData *storage.InsertData) (map[string]*datapb.PartitionRows, error) {
	hashFunc, err := getHashFunc(task.GetSchema(), len(task.GetVchannels()), len(task.GetPartitionIDs()))
	if err != nil {
		return nil, err
	}

	rowNum := GetInsertDataRowNum(insertData, task.GetSchema())
	pkField, err := typeutil.GetPrimaryFieldSchema(task.GetSchema())
	if err != nil {
		return nil, err
	}
	if pkField.GetAutoID() { // TODO: dyh, fix it, find a better way
		// gen fake auto id for preimport
		if insertData.Data[pkField.GetFieldID()] == nil || insertData.Data[pkField.GetFieldID()].RowNum() == 0 {
			switch pkField.GetDataType() {
			case schemapb.DataType_Int64:
				data := make([]int64, rowNum)
				for i := 0; i < rowNum; i++ {
					data[i] = int64(i)
				}
				insertData.Data[pkField.GetFieldID()] = &storage.Int64FieldData{Data: data}
			case schemapb.DataType_VarChar:
				data := make([]string, rowNum)
				for i := 0; i < rowNum; i++ {
					data[i] = fmt.Sprint(i)
				}
				insertData.Data[pkField.GetFieldID()] = &storage.StringFieldData{Data: data}
			}
		}
	}

	err = CheckRowsEqual(task.GetSchema(), insertData)
	if err != nil {
		return nil, err
	}

	hashRowsCount := make([][]int, len(task.GetVchannels()))
	for i := 0; i < len(task.GetVchannels()); i++ {
		hashRowsCount[i] = make([]int, len(task.GetPartitionIDs()))
	}
	for i := 0; i < rowNum; i++ {
		p1, p2 := hashFunc(insertData.GetRow(i))
		hashRowsCount[p1][p2]++
	}

	res := make(map[string]*datapb.PartitionRows)
	for _, channel := range task.GetVchannels() {
		res[channel] = &datapb.PartitionRows{
			PartitionRows: make(map[int64]int64),
		}
	}
	for i, partitionRows := range hashRowsCount {
		channel := task.GetVchannels()[i]
		for j, rowNum := range partitionRows {
			partition := task.GetPartitionIDs()[j]
			res[channel].PartitionRows[partition] = int64(rowNum)
		}
	}
	return res, nil
}

func GetHashedData(task Task, insertData *storage.InsertData) (HashedData, error) {
	hashFunc, err := getHashFunc(task.GetSchema(), len(task.GetVchannels()), len(task.GetPartitionIDs()))
	if err != nil {
		return nil, err
	}

	schema := typeutil.AppendSystemFields(task.GetSchema())
	res := make(HashedData, len(task.GetVchannels()))
	for i := range task.GetVchannels() {
		res[i] = make([]*storage.InsertData, len(task.GetPartitionIDs()))
		for j := range task.GetPartitionIDs() {
			res[i][j], err = storage.NewInsertData(schema)
			if err != nil {
				return nil, err
			}
		}
	}

	for i := 0; i < insertData.GetRowNum(); i++ {
		row := insertData.GetRow(i)
		p1, p2 := hashFunc(row)
		err = res[p1][p2].Append(row)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

func MergeHashedRowsCount(src, dst map[string]*datapb.PartitionRows) {
	for channel, partitionRows := range src {
		for partitionID, rowCount := range partitionRows.GetPartitionRows() {
			if dst[channel] == nil {
				dst[channel] = &datapb.PartitionRows{
					PartitionRows: make(map[int64]int64),
				}
			}
			dst[channel].PartitionRows[partitionID] += rowCount
		}
	}
}

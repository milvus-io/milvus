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
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type HashedData [][]*storage.InsertData // vchannel -> (partitionID -> InsertData)

func newHashData(schema *schemapb.CollectionSchema, channelNum, partitionNum int) (HashedData, error) {
	var err error
	res := make(HashedData, channelNum)
	for i := 0; i < channelNum; i++ {
		res[i] = make([]*storage.InsertData, partitionNum)
		for j := 0; j < partitionNum; j++ {
			res[i][j], err = storage.NewInsertData(schema)
			if err != nil {
				return nil, err
			}
		}
	}
	return res, nil
}

func HashData(task Task, rows *storage.InsertData) (HashedData, error) {
	var (
		schema       = typeutil.AppendSystemFields(task.GetSchema())
		channelNum   = len(task.GetVchannels())
		partitionNum = len(task.GetPartitionIDs())
	)

	fn, err := getHashFunc(task.GetSchema(), channelNum, partitionNum)
	if err != nil {
		return nil, err
	}

	res, err := newHashData(schema, channelNum, partitionNum)
	if err != nil {
		return nil, err
	}

	for i := 0; i < rows.GetRowNum(); i++ {
		row := rows.GetRow(i)
		p1, p2 := fn(row)
		err = res[p1][p2].Append(row)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

func GetRowsStats(task Task, rows *storage.InsertData) (map[string]*datapb.PartitionRows, error) {
	var (
		schema       = task.GetSchema()
		channelNum   = len(task.GetVchannels())
		partitionNum = len(task.GetPartitionIDs())
	)

	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}
	partKeyField, _ := typeutil.GetPartitionKeyFieldSchema(schema)

	hashRowsCount := make([][]int, channelNum)
	for i := 0; i < channelNum; i++ {
		hashRowsCount[i] = make([]int, partitionNum)
	}

	rowNum := GetInsertDataRowNum(rows, schema)
	if !pkField.GetAutoID() {
		fn, err := getHashFunc(schema, channelNum, partitionNum)
		if err != nil {
			return nil, err
		}
		for i := 0; i < rowNum; i++ {
			p1, p2 := fn(rows.GetRow(i))
			hashRowsCount[p1][p2]++
		}
	} else {
		id := int64(0)
		num := int64(channelNum)
		fn1 := hashByID()
		fn2 := hashByPartitionKey(int64(partitionNum), partKeyField)
		for i := 0; i < rowNum; i++ {
			p1, p2 := fn1(id, num), fn2(rows.GetRow(i))
			hashRowsCount[p1][p2]++
			id++
		}
	}

	res := make(map[string]*datapb.PartitionRows)
	for _, channel := range task.GetVchannels() {
		res[channel] = &datapb.PartitionRows{
			PartitionRows: make(map[int64]int64),
		}
	}
	for i, partitionRows := range hashRowsCount {
		channel := task.GetVchannels()[i]
		for j, n := range partitionRows {
			partition := task.GetPartitionIDs()[j]
			res[channel].PartitionRows[partition] = int64(n)
		}
	}
	return res, nil
}

func getHashFunc(schema *schemapb.CollectionSchema, chanNum, partitionNum int) (func(row map[int64]interface{}) (int64, int64), error) {
	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}
	partKeyField, _ := typeutil.GetPartitionKeyFieldSchema(schema)

	f1 := hashByVChannel(int64(chanNum), pkField)
	f2 := hashByPartitionKey(int64(partitionNum), partKeyField)

	return func(row map[int64]interface{}) (int64, int64) {
		return f1(row), f2(row)
	}, nil
}

func hashByVChannel(channelNum int64, pkField *schemapb.FieldSchema) func(row map[int64]interface{}) int64 {
	if channelNum == 1 || pkField == nil {
		return func(_ map[int64]interface{}) int64 {
			return 0
		}
	}
	fn := hashFunc(pkField.GetDataType())
	return func(row map[int64]interface{}) int64 {
		pk := row[pkField.GetFieldID()]
		p := fn(pk, channelNum)
		return p
	}
}

func hashByPartitionKey(partitionNum int64, partField *schemapb.FieldSchema) func(row map[int64]interface{}) int64 {
	if partitionNum == 1 {
		return func(_ map[int64]interface{}) int64 {
			return 0
		}
	}
	fn := hashFunc(partField.GetDataType())
	return func(row map[int64]interface{}) int64 {
		pk := row[partField.GetFieldID()]
		p := fn(pk, partitionNum)
		return p
	}
}

func hashByID() func(id int64, shardNum int64) int64 {
	return func(id int64, shardNum int64) int64 {
		hash, _ := typeutil.Hash32Int64(id)
		return int64(hash) % shardNum
	}
}

func hashFunc(dataType schemapb.DataType) func(data any, shardNum int64) int64 {
	switch dataType {
	case schemapb.DataType_Int64:
		return func(data any, shardNum int64) int64 {
			hash, _ := typeutil.Hash32Int64(data.(int64))
			return int64(hash) % shardNum
		}
	case schemapb.DataType_VarChar:
		return func(data any, shardNum int64) int64 {
			hash := typeutil.HashString2Uint32(data.(string))
			return int64(hash) % shardNum
		}
	default:
		return nil
	}
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

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
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type HashedData [][]*storage.InsertData // [vchannelIndex][partitionIndex]*storage.InsertData

func newHashedData(schema *schemapb.CollectionSchema, channelNum, partitionNum int) (HashedData, error) {
	var err error
	res := make(HashedData, channelNum)
	for i := 0; i < channelNum; i++ {
		res[i] = make([]*storage.InsertData, partitionNum)
		for j := 0; j < partitionNum; j++ {
			res[i][j], err = storage.NewInsertDataWithFunctionOutputField(schema)
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

	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}
	partKeyField, _ := typeutil.GetPartitionKeyFieldSchema(schema)

	id1 := pkField.GetFieldID()
	id2 := partKeyField.GetFieldID()

	f1 := hashByVChannel(int64(channelNum), pkField)
	f2 := hashByPartition(int64(partitionNum), partKeyField)

	res, err := newHashedData(schema, channelNum, partitionNum)
	if err != nil {
		return nil, err
	}

	for i := 0; i < rows.GetRowNum(); i++ {
		row := rows.GetRow(i)
		p1, p2 := f1(row[id1]), f2(row[id2])
		err = res[p1][p2].Append(row)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

func HashDeleteData(task Task, delData *storage.DeleteData) ([]*storage.DeleteData, error) {
	var (
		schema     = typeutil.AppendSystemFields(task.GetSchema())
		channelNum = len(task.GetVchannels())
	)

	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}

	f1 := hashByVChannel(int64(channelNum), pkField)

	res := make([]*storage.DeleteData, channelNum)
	for i := 0; i < channelNum; i++ {
		res[i] = storage.NewDeleteData(nil, nil)
	}

	for i := 0; i < int(delData.RowCount); i++ {
		pk := delData.Pks[i]
		ts := delData.Tss[i]
		p := f1(pk.GetValue())
		res[p].Append(pk, ts)
	}
	return res, nil
}

func GetRowsStats(task Task, rows *storage.InsertData) (map[string]*datapb.PartitionImportStats, error) {
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

	id1 := pkField.GetFieldID()
	id2 := partKeyField.GetFieldID()

	hashRowsCount := make([][]int, channelNum)
	hashDataSize := make([][]int, channelNum)
	for i := 0; i < channelNum; i++ {
		hashRowsCount[i] = make([]int, partitionNum)
		hashDataSize[i] = make([]int, partitionNum)
	}

	rowNum := GetInsertDataRowCount(rows, schema)
	if pkField.GetAutoID() {
		id := int64(0)
		num := int64(channelNum)
		fn1 := hashByID()
		fn2 := hashByPartition(int64(partitionNum), partKeyField)
		rows.Data = lo.PickBy(rows.Data, func(fieldID int64, _ storage.FieldData) bool {
			return fieldID != pkField.GetFieldID()
		})
		for i := 0; i < rowNum; i++ {
			p1, p2 := fn1(id, num), fn2(rows.GetRow(i)[id2])
			hashRowsCount[p1][p2]++
			hashDataSize[p1][p2] += rows.GetRowSize(i)
			id++
		}
	} else {
		f1 := hashByVChannel(int64(channelNum), pkField)
		f2 := hashByPartition(int64(partitionNum), partKeyField)
		for i := 0; i < rowNum; i++ {
			row := rows.GetRow(i)
			p1, p2 := f1(row[id1]), f2(row[id2])
			hashRowsCount[p1][p2]++
			hashDataSize[p1][p2] += rows.GetRowSize(i)
		}
	}

	res := make(map[string]*datapb.PartitionImportStats)
	for _, channel := range task.GetVchannels() {
		res[channel] = &datapb.PartitionImportStats{
			PartitionRows:     make(map[int64]int64),
			PartitionDataSize: make(map[int64]int64),
		}
	}
	for i := range hashRowsCount {
		channel := task.GetVchannels()[i]
		for j := range hashRowsCount[i] {
			partition := task.GetPartitionIDs()[j]
			res[channel].PartitionRows[partition] = int64(hashRowsCount[i][j])
			res[channel].PartitionDataSize[partition] = int64(hashDataSize[i][j])
		}
	}
	return res, nil
}

func GetDeleteStats(task Task, delData *storage.DeleteData) (map[string]*datapb.PartitionImportStats, error) {
	var (
		schema     = typeutil.AppendSystemFields(task.GetSchema())
		channelNum = len(task.GetVchannels())
	)

	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}

	f1 := hashByVChannel(int64(channelNum), pkField)

	hashRowsCount := make([][]int, channelNum)
	hashDataSize := make([][]int, channelNum)
	for i := 0; i < channelNum; i++ {
		hashRowsCount[i] = make([]int, 1)
		hashDataSize[i] = make([]int, 1)
	}

	for i := 0; i < int(delData.RowCount); i++ {
		pk := delData.Pks[i]
		p := f1(pk.GetValue())
		hashRowsCount[p][0]++
		hashDataSize[p][0] += int(pk.Size()) + 8 // pk + ts
	}

	res := make(map[string]*datapb.PartitionImportStats)
	for i := range hashRowsCount {
		channel := task.GetVchannels()[i]
		partition := task.GetPartitionIDs()[0]
		res[channel] = &datapb.PartitionImportStats{
			PartitionRows:     make(map[int64]int64),
			PartitionDataSize: make(map[int64]int64),
		}
		res[channel].PartitionRows[partition] = int64(hashRowsCount[i][0])
		res[channel].PartitionDataSize[partition] = int64(hashDataSize[i][0])
	}

	return res, nil
}

func hashByVChannel(channelNum int64, pkField *schemapb.FieldSchema) func(pk any) int64 {
	if channelNum == 1 || pkField == nil {
		return func(_ any) int64 {
			return 0
		}
	}
	switch pkField.GetDataType() {
	case schemapb.DataType_Int64:
		return func(pk any) int64 {
			hash, _ := typeutil.Hash32Int64(pk.(int64))
			return int64(hash) % channelNum
		}
	case schemapb.DataType_VarChar:
		return func(pk any) int64 {
			hash := typeutil.HashString2Uint32(pk.(string))
			return int64(hash) % channelNum
		}
	default:
		return nil
	}
}

func hashByPartition(partitionNum int64, partField *schemapb.FieldSchema) func(key any) int64 {
	if partitionNum == 1 {
		return func(_ any) int64 {
			return 0
		}
	}
	switch partField.GetDataType() {
	case schemapb.DataType_Int64:
		return func(key any) int64 {
			hash, _ := typeutil.Hash32Int64(key.(int64))
			return int64(hash) % partitionNum
		}
	case schemapb.DataType_VarChar:
		return func(key any) int64 {
			hash := typeutil.HashString2Uint32(key.(string))
			return int64(hash) % partitionNum
		}
	default:
		return nil
	}
}

func hashByID() func(id int64, shardNum int64) int64 {
	return func(id int64, shardNum int64) int64 {
		hash, _ := typeutil.Hash32Int64(id)
		return int64(hash) % shardNum
	}
}

func MergeHashedStats(src, dst map[string]*datapb.PartitionImportStats) {
	for channel, partitionStats := range src {
		for partitionID := range partitionStats.GetPartitionRows() {
			if dst[channel] == nil {
				dst[channel] = &datapb.PartitionImportStats{
					PartitionRows:     make(map[int64]int64),
					PartitionDataSize: make(map[int64]int64),
				}
			}
			dst[channel].PartitionRows[partitionID] += partitionStats.GetPartitionRows()[partitionID]
			dst[channel].PartitionDataSize[partitionID] += partitionStats.GetPartitionDataSize()[partitionID]
		}
	}
}

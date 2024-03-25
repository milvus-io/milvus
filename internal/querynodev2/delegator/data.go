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

package delegator

import (
	bloom "github.com/bits-and-blooms/bloom/v3"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"go.uber.org/zap"
)

// InsertData
type InsertData struct {
	RowIDs        []int64
	PrimaryKeys   []storage.PrimaryKey
	Timestamps    []uint64
	InsertRecord  *segcorepb.InsertRecord
	Data          []*storage.InsertData
	PkData        []storage.FieldData
	TsData        []*storage.Int64FieldData
	StartPosition *msgpb.MsgPosition
	SegmentID     int64
	PartitionID   int64
	RowNum        int64
	// BF for primary keys in this data batch
	batchBF *storage.PkStatistics
}

func (id *InsertData) GeneratePkStats() {
	id.batchBF = &storage.PkStatistics{
		PkFilter: bloom.NewWithEstimates(uint(id.InsertRecord.GetNumRows()), paramtable.Get().CommonCfg.MaxBloomFalsePositive.GetAsFloat()),
	}

	buf := make([]byte, 8)
	for _, pk := range id.PrimaryKeys {
		id.batchBF.UpdateMinMax(pk)
		switch pk.Type() {
		case schemapb.DataType_Int64:
			int64Value := pk.(*storage.Int64PrimaryKey).Value
			common.Endian.PutUint64(buf, uint64(int64Value))
			id.batchBF.PkFilter.Add(buf)
		case schemapb.DataType_VarChar:
			stringValue := pk.(*storage.VarCharPrimaryKey).Value
			id.batchBF.PkFilter.AddString(stringValue)
		default:
			log.Error("failed to update bloomfilter", zap.Any("PK type", pk.Type()))
			panic("failed to update bloomfilter")
		}
	}
}

func (id *InsertData) PkExist(pk storage.PrimaryKey, ts uint64) bool {
	if !id.batchBF.PkExist(pk) {
		return false
	}

	for batchIdx, timestamp := range id.Timestamps {
		if ts <= timestamp {
			continue
		}
		targetPK := id.PrimaryKeys[batchIdx]

		if pk.EQ(targetPK) {
			return true
		}
	}
	return false
}

type DeleteData struct {
	PartitionID int64
	PrimaryKeys []storage.PrimaryKey
	Timestamps  []uint64
	RowCount    int64
}

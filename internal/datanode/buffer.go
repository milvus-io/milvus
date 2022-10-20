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

package datanode

import (
	"errors"
	"math"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
)

// BufferData buffers insert data, monitoring buffer size and limit
// size and limit both indicate numOfRows
type BufferData struct {
	buffer *InsertData
	size   int64
	limit  int64
	tsFrom Timestamp
	tsTo   Timestamp
}

func (bd *BufferData) effectiveCap() int64 {
	return bd.limit - bd.size
}

func (bd *BufferData) updateSize(no int64) {
	bd.size += no
}

// updateTimeRange update BufferData tsFrom, tsTo range according to input time range
func (bd *BufferData) updateTimeRange(tr TimeRange) {
	if tr.timestampMin < bd.tsFrom {
		bd.tsFrom = tr.timestampMin
	}
	if tr.timestampMax > bd.tsTo {
		bd.tsTo = tr.timestampMax
	}
}

// DelDataBuf buffers delete data, monitoring buffer size and limit
// size and limit both indicate numOfRows
type DelDataBuf struct {
	datapb.Binlog
	delData *DeleteData
}

func (ddb *DelDataBuf) updateSize(size int64) {
	ddb.EntriesNum += size
}

func (ddb *DelDataBuf) updateTimeRange(tr TimeRange) {
	if tr.timestampMin < ddb.TimestampFrom {
		ddb.TimestampFrom = tr.timestampMin
	}
	if tr.timestampMax > ddb.TimestampTo {
		ddb.TimestampTo = tr.timestampMax
	}
}

func (ddb *DelDataBuf) updateFromBuf(buf *DelDataBuf) {
	ddb.updateSize(buf.EntriesNum)

	tr := TimeRange{timestampMax: buf.TimestampTo, timestampMin: buf.TimestampFrom}
	ddb.updateTimeRange(tr)

	ddb.delData.Pks = append(ddb.delData.Pks, buf.delData.Pks...)
	ddb.delData.Tss = append(ddb.delData.Tss, buf.delData.Tss...)
}

// newBufferData needs an input dimension to calculate the limit of this buffer
//
// `limit` is the segment numOfRows a buffer can buffer at most.
//
// For a float32 vector field:
//  limit = 16 * 2^20 Byte [By default] / (dimension * 4 Byte)
//
// For a binary vector field:
//  limit = 16 * 2^20 Byte [By default]/ (dimension / 8 Byte)
//
// But since the buffer of binary vector fields is larger than the float32 one
//   with the same dimension, newBufferData takes the smaller buffer limit
//   to fit in both types of vector fields
//
// * This need to change for string field support and multi-vector fields support.
func newBufferData(dimension int64) (*BufferData, error) {
	if dimension == 0 {
		return nil, errors.New("Invalid dimension")
	}

	limit := Params.DataNodeCfg.FlushInsertBufferSize / (dimension * 4)

	//TODO::xige-16 eval vec and string field
	return &BufferData{
		buffer: &InsertData{Data: make(map[UniqueID]storage.FieldData)},
		size:   0,
		limit:  limit,
		tsFrom: math.MaxUint64,
		tsTo:   0}, nil
}

func newDelDataBuf() *DelDataBuf {
	return &DelDataBuf{
		delData: &DeleteData{},
		Binlog: datapb.Binlog{
			EntriesNum:    0,
			TimestampFrom: math.MaxUint64,
			TimestampTo:   0,
		},
	}
}

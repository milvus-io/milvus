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

package syncmgr

import (
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// SyncPack is the struct contains buffer sync data.
type SyncPack struct {
	// data
	SyncDataPack

	// statistics
	startPosition *msgpb.MsgPosition
	checkpoint    *msgpb.MsgPosition
	dataSource    string
	isDrop        bool
	// metadata
	channelName string
	// error handler function
	errHandler func(err error)
}

// SyncDataPack is used to make a Consume semantics for SyncPack's ConsumeData function
type SyncDataPack struct {
	insertData   []*storage.InsertData
	deltaData    *storage.DeleteData
	bm25Stats    map[int64]*storage.BM25Stats
	tsFrom       typeutil.Timestamp
	tsTo         typeutil.Timestamp
	batchRows    int64 // batchRows is the row number of this sync task,not the total num of rows of segment
	isFlush      bool
	collectionID int64
	partitionID  int64
	segmentID    int64
	level        datapb.SegmentLevel

	isConsumed bool // flag to indicate if the data is consumed, some big field should be gced as soon as possible
}

// ConsumeData is used to consume the data in SyncPack, and return a SyncDataPack for fast gc the big data field
// It can only be consumed once, and will panic if consumed twice
func (p *SyncPack) ConsumeData() *SyncDataPack {
	if p.isConsumed {
		panic("SyncDataPack data is alread consumed")
	}
	data := (*p).SyncDataPack
	p.isConsumed = true
	p.insertData = nil
	p.deltaData = nil
	p.bm25Stats = nil
	return &data
}

func (p *SyncPack) WithInsertData(insertData []*storage.InsertData) *SyncPack {
	p.insertData = lo.Filter(insertData, func(inData *storage.InsertData, _ int) bool {
		return inData != nil
	})
	return p
}

func (p *SyncPack) WithDeleteData(deltaData *storage.DeleteData) *SyncPack {
	p.deltaData = deltaData
	return p
}

func (p *SyncPack) WithBM25Stats(stats map[int64]*storage.BM25Stats) *SyncPack {
	p.bm25Stats = stats
	return p
}

func (p *SyncPack) WithStartPosition(start *msgpb.MsgPosition) *SyncPack {
	p.startPosition = start
	return p
}

func (p *SyncPack) WithCheckpoint(cp *msgpb.MsgPosition) *SyncPack {
	p.checkpoint = cp
	return p
}

func (p *SyncPack) WithCollectionID(collID int64) *SyncPack {
	p.collectionID = collID
	return p
}

func (p *SyncPack) WithPartitionID(partID int64) *SyncPack {
	p.partitionID = partID
	return p
}

func (p *SyncPack) WithSegmentID(segID int64) *SyncPack {
	p.segmentID = segID
	return p
}

func (p *SyncPack) WithChannelName(chanName string) *SyncPack {
	p.channelName = chanName
	return p
}

func (p *SyncPack) WithTimeRange(from, to typeutil.Timestamp) *SyncPack {
	p.tsFrom, p.tsTo = from, to
	return p
}

func (p *SyncPack) WithFlush() *SyncPack {
	p.isFlush = true
	return p
}

func (p *SyncPack) WithDrop() *SyncPack {
	p.isDrop = true
	return p
}

func (p *SyncPack) WithBatchRows(batchRows int64) *SyncPack {
	p.batchRows = batchRows
	return p
}

func (p *SyncPack) WithLevel(level datapb.SegmentLevel) *SyncPack {
	p.level = level
	return p
}

func (p *SyncPack) WithErrorHandler(handler func(err error)) *SyncPack {
	p.errHandler = handler
	return p
}

func (p *SyncPack) WithDataSource(source string) *SyncPack {
	p.dataSource = source
	return p
}

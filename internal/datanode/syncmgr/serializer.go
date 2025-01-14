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
	"context"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// Serializer is the interface for storage/storageV2 implementation to encoding
// WriteBuffer into sync task.
type Serializer interface {
	EncodeBuffer(ctx context.Context, pack *SyncPack) (Task, error)
}

// SyncPack is the struct contains buffer sync data.
type SyncPack struct {
	metacache  metacache.MetaCache
	metawriter MetaWriter
	// data
	insertData []*storage.InsertData
	deltaData  *storage.DeleteData
	// statistics
	tsFrom        typeutil.Timestamp
	tsTo          typeutil.Timestamp
	startPosition *msgpb.MsgPosition
	checkpoint    *msgpb.MsgPosition
	batchSize     int64 // batchSize is the row number of this sync task,not the total num of rows of segemnt
	dataSource    string
	isFlush       bool
	isDrop        bool
	// metadata
	collectionID int64
	partitionID  int64
	segmentID    int64
	channelName  string
	level        datapb.SegmentLevel
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

func (p *SyncPack) WithBatchSize(batchSize int64) *SyncPack {
	p.batchSize = batchSize
	return p
}

func (p *SyncPack) WithLevel(level datapb.SegmentLevel) *SyncPack {
	p.level = level
	return p
}

func (p *SyncPack) WithDataSource(source string) *SyncPack {
	p.dataSource = source
	return p
}

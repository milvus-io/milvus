// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package datanode

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
)

func TestFlowGraphDDNode_Operate(t *testing.T) {
	const ctxTimeInMillisecond = 2000
	const closeWithDeadline = true
	var ctx context.Context

	if closeWithDeadline {
		var cancel context.CancelFunc
		d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
		ctx, cancel = context.WithDeadline(context.Background(), d)
		defer cancel()
	} else {
		ctx = context.Background()
	}

	inFlushCh := make(chan *flushMsg, 10)
	defer close(inFlushCh)

	testPath := "/test/datanode/root/meta"
	err := clearEtcd(testPath)
	require.NoError(t, err)
	Params.MetaRootPath = testPath

	// Params.FlushDdBufferSize = 4
	replica := newReplica()
	ddNode := newDDNode(ctx, newBinlogMeta(), inFlushCh, replica, NewAllocatorFactory())

	collID := UniqueID(0)
	collName := "col-test-0"
	// create collection
	createCollReq := internalpb.CreateCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_CreateCollection,
			MsgID:     1,
			Timestamp: 1,
			SourceID:  1,
		},
		CollectionID:   collID,
		Schema:         make([]byte, 0),
		CollectionName: collName,
		DbName:         "DbName",
		DbID:           UniqueID(0),
	}
	createCollMsg := msgstream.CreateCollectionMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: Timestamp(1),
			EndTimestamp:   Timestamp(1),
			HashValues:     []uint32{uint32(0)},
		},
		CreateCollectionRequest: createCollReq,
	}

	// drop collection
	dropCollReq := internalpb.DropCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DropCollection,
			MsgID:     2,
			Timestamp: 2,
			SourceID:  2,
		},
		CollectionID:   collID,
		CollectionName: collName,
		DbName:         "DbName",
		DbID:           UniqueID(0),
	}
	dropCollMsg := msgstream.DropCollectionMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: Timestamp(2),
			EndTimestamp:   Timestamp(2),
			HashValues:     []uint32{uint32(0)},
		},
		DropCollectionRequest: dropCollReq,
	}

	partitionID := UniqueID(100)
	partitionName := "partition-test-0"
	// create partition
	createPartitionReq := internalpb.CreatePartitionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_CreatePartition,
			MsgID:     3,
			Timestamp: 3,
			SourceID:  3,
		},
		CollectionID:   collID,
		PartitionID:    partitionID,
		CollectionName: collName,
		PartitionName:  partitionName,
		DbName:         "DbName",
		DbID:           UniqueID(0),
	}
	createPartitionMsg := msgstream.CreatePartitionMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: Timestamp(3),
			EndTimestamp:   Timestamp(3),
			HashValues:     []uint32{uint32(0)},
		},
		CreatePartitionRequest: createPartitionReq,
	}

	// drop partition
	dropPartitionReq := internalpb.DropPartitionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DropPartition,
			MsgID:     4,
			Timestamp: 4,
			SourceID:  4,
		},
		CollectionID:   collID,
		PartitionID:    partitionID,
		CollectionName: collName,
		PartitionName:  partitionName,
		DbName:         "DbName",
		DbID:           UniqueID(0),
	}
	dropPartitionMsg := msgstream.DropPartitionMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: Timestamp(4),
			EndTimestamp:   Timestamp(4),
			HashValues:     []uint32{uint32(0)},
		},
		DropPartitionRequest: dropPartitionReq,
	}

	replica.addSegment(1, collID, partitionID, "insert-01")
	inFlushCh <- &flushMsg{
		msgID:        5,
		timestamp:    5,
		segmentID:    UniqueID(1),
		collectionID: collID,
	}

	startPos := []*internalpb.MsgPosition{
		{
			ChannelName: "aaa",
			MsgID:       make([]byte, 0),
			Timestamp:   0,
		},
	}

	tsMessages := make([]msgstream.TsMsg, 0)
	tsMessages = append(tsMessages, msgstream.TsMsg(&createCollMsg))
	tsMessages = append(tsMessages, msgstream.TsMsg(&dropCollMsg))
	tsMessages = append(tsMessages, msgstream.TsMsg(&createPartitionMsg))
	tsMessages = append(tsMessages, msgstream.TsMsg(&dropPartitionMsg))
	msgStream := flowgraph.GenerateMsgStreamMsg(tsMessages, Timestamp(0), Timestamp(3),
		startPos, startPos)
	var inMsg Msg = msgStream
	ddNode.Operate([]Msg{inMsg})
}

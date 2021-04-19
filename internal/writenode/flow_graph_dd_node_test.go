package writenode

import (
	"context"
	"testing"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
	"github.com/zilliztech/milvus-distributed/internal/util/flowgraph"
)

func TestFlowGraphDDNode_Operate(t *testing.T) {
	const ctxTimeInMillisecond = 2000
	const closeWithDeadline = false
	var ctx context.Context

	if closeWithDeadline {
		var cancel context.CancelFunc
		d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
		ctx, cancel = context.WithDeadline(context.Background(), d)
		defer cancel()
	} else {
		ctx = context.Background()
	}

	startMaster(ctx)
	Params.FlushDdBufSize = 4

	ddNode := newDDNode(ctx)

	colID := UniqueID(0)
	colName := "col-test-0"
	// create collection
	createColReq := internalpb.CreateCollectionRequest{
		MsgType:      internalpb.MsgType_kCreateCollection,
		CollectionID: colID,
		ReqID:        1,
		Timestamp:    1,
		ProxyID:      1,
		Schema:       &commonpb.Blob{},
	}
	createColMsg := msgstream.CreateCollectionMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: Timestamp(1),
			EndTimestamp:   Timestamp(1),
			HashValues:     []uint32{uint32(0)},
		},
		CreateCollectionRequest: createColReq,
	}

	// drop collection
	dropColReq := internalpb.DropCollectionRequest{
		MsgType:        internalpb.MsgType_kDropCollection,
		CollectionID:   colID,
		ReqID:          2,
		Timestamp:      2,
		ProxyID:        2,
		CollectionName: &servicepb.CollectionName{CollectionName: colName},
	}
	dropColMsg := msgstream.DropCollectionMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: Timestamp(2),
			EndTimestamp:   Timestamp(2),
			HashValues:     []uint32{uint32(0)},
		},
		DropCollectionRequest: dropColReq,
	}

	partitionID := UniqueID(100)
	partitionTag := "partition-test-0"
	// create partition
	createPartitionReq := internalpb.CreatePartitionRequest{
		MsgType:      internalpb.MsgType_kCreatePartition,
		CollectionID: colID,
		PartitionID:  partitionID,
		ReqID:        3,
		Timestamp:    3,
		ProxyID:      3,
		PartitionName: &servicepb.PartitionName{
			CollectionName: colName,
			Tag:            partitionTag,
		},
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
		MsgType:      internalpb.MsgType_kDropPartition,
		CollectionID: colID,
		PartitionID:  partitionID,
		ReqID:        4,
		Timestamp:    4,
		ProxyID:      4,
		PartitionName: &servicepb.PartitionName{
			CollectionName: colName,
			Tag:            partitionTag,
		},
	}
	dropPartitionMsg := msgstream.DropPartitionMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: Timestamp(4),
			EndTimestamp:   Timestamp(4),
			HashValues:     []uint32{uint32(0)},
		},
		DropPartitionRequest: dropPartitionReq,
	}

	tsMessages := make([]msgstream.TsMsg, 0)
	tsMessages = append(tsMessages, msgstream.TsMsg(&createColMsg))
	tsMessages = append(tsMessages, msgstream.TsMsg(&dropColMsg))
	tsMessages = append(tsMessages, msgstream.TsMsg(&createPartitionMsg))
	tsMessages = append(tsMessages, msgstream.TsMsg(&dropPartitionMsg))
	msgStream := flowgraph.GenerateMsgStreamMsg(tsMessages, Timestamp(0), Timestamp(3))
	var inMsg Msg = msgStream
	ddNode.Operate([]*Msg{&inMsg})
}

package proxy

import (
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type insertTask struct {
	baseTask
	// SegIdAssigner, RowIdAllocator
	rowBatch        *servicepb.RowBatch
	resultChan      chan *servicepb.IntegerRangeResponse
	pulsarMsgStream *msgstream.PulsarMsgStream
}

func (it *insertTask) PreExecute() error {
	return nil
}

func (it *insertTask) Execute() error {
	ts := it.GetTs()
	insertRequest := internalpb.InsertRequest{
		ReqType:        internalpb.ReqType_kInsert,
		ReqId:          it.ReqId,
		CollectionName: it.rowBatch.CollectionName,
		PartitionTag:   it.rowBatch.PartitionTag,
		SegmentId:      1, // TODO: use SegIdAssigner instead
		// TODO: ChannelID
		ProxyId:    it.ProxyId,
		Timestamps: []typeutil.Timestamp{ts},
		RowIds:     []int64{1}, // TODO: use RowIdAllocator instead
		RowData:    it.rowBatch.RowData,
	}
	pulsarInsertTask := msgstream.InsertTask{
		InsertRequest: insertRequest,
	}
	var tsMsg msgstream.TsMsg = &pulsarInsertTask
	msgPack := &msgstream.MsgPack{
		BeginTs: ts,
		EndTs:   ts,
	}
	msgPack.Msgs = append(msgPack.Msgs, &tsMsg)
	it.pulsarMsgStream.Produce(msgPack)
	return nil
}

func (it *insertTask) PostExecute() error {
	return nil
}

func (it *insertTask) WaitToFinish() error {
	return nil
}

func (it *insertTask) Notify() error {
	return nil
}

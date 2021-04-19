package proxy

import (
	"context"
	"errors"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
	"log"
)

type task interface {
	Id() UniqueID // return ReqId
	Type() internalpb.MsgType
	BeginTs() Timestamp
	EndTs() Timestamp
	SetTs(ts Timestamp)
	PreExecute() error
	Execute() error
	PostExecute() error
	WaitToFinish() error
	Notify(err error)
}

type baseInsertTask = msgstream.InsertMsg

type InsertTask struct {
	baseInsertTask
	ts                    Timestamp
	done                  chan error
	resultChan            chan *servicepb.IntegerRangeResponse
	manipulationMsgStream *msgstream.PulsarMsgStream
	ctx                   context.Context
	cancel                context.CancelFunc
}

func (it *InsertTask) SetTs(ts Timestamp) {
	it.ts = ts
}

func (it *InsertTask) BeginTs() Timestamp {
	return it.ts
}

func (it *InsertTask) EndTs() Timestamp {
	return it.ts
}

func (it *InsertTask) Id() UniqueID {
	return it.ReqId
}

func (it *InsertTask) Type() internalpb.MsgType {
	return it.MsgType
}

func (it *InsertTask) PreExecute() error {
	return nil
}

func (it *InsertTask) Execute() error {
	var tsMsg msgstream.TsMsg = it
	msgPack := &msgstream.MsgPack{
		BeginTs: it.BeginTs(),
		EndTs:   it.EndTs(),
		Msgs:    make([]*msgstream.TsMsg, 1),
	}
	msgPack.Msgs[0] = &tsMsg
	it.manipulationMsgStream.Produce(msgPack)
	return nil
}

func (it *InsertTask) PostExecute() error {
	return nil
}

func (it *InsertTask) WaitToFinish() error {
	defer it.cancel()
	for {
		select {
		case err := <-it.done:
			return err
		case <-it.ctx.Done():
			log.Print("wait to finish failed, timeout!")
			return errors.New("wait to finish failed, timeout!")
		}
	}
}

func (it *InsertTask) Notify(err error) {
	it.done <- err
}

type CreateCollectionTask struct {
	internalpb.CreateCollectionRequest
	masterClient masterpb.MasterClient
	done chan error
	resultChan chan *commonpb.Status
	ctx context.Context
	cancel context.CancelFunc
}

func (cct *CreateCollectionTask) Id() UniqueID {
	return cct.ReqId
}

func (cct *CreateCollectionTask) Type() internalpb.MsgType {
	return cct.MsgType
}

func (cct *CreateCollectionTask) BeginTs() Timestamp {
	return cct.Timestamp
}

func (cct *CreateCollectionTask) EndTs() Timestamp {
	return cct.Timestamp
}

func (cct *CreateCollectionTask) SetTs(ts Timestamp) {
	cct.Timestamp = ts
}

func (cct *CreateCollectionTask) PreExecute() error {
	return nil
}

func (cct *CreateCollectionTask) Execute() error {
	resp, err := cct.masterClient.CreateCollection(cct.ctx, &cct.CreateCollectionRequest)
	if err != nil {
		log.Printf("create collection failed, error= %v", err)
		cct.resultChan <- &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason: err.Error(),
		}
	} else {
		cct.resultChan <- resp
	}
	return err
}

func (cct *CreateCollectionTask) PostExecute() error {
	return nil
}

func (cct *CreateCollectionTask) WaitToFinish() error {
	defer cct.cancel()
	for {
		select {
		case err := <- cct.done:
			return err
		case <- cct.ctx.Done():
			log.Print("wait to finish failed, timeout!")
			return errors.New("wait to finish failed, timeout!")
		}
	}
}

func (cct *CreateCollectionTask) Notify(err error) {
	cct.done <- err
}

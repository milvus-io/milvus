package proxy

import (
	"context"
	"errors"
	"log"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
)

type task interface {
	ID() UniqueID // return ReqID
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

type BaseInsertTask = msgstream.InsertMsg

type InsertTask struct {
	BaseInsertTask
	ts                    Timestamp
	done                  chan error
	result                *servicepb.IntegerRangeResponse
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

func (it *InsertTask) ID() UniqueID {
	return it.ReqID
}

func (it *InsertTask) Type() internalpb.MsgType {
	return it.MsgType
}

func (it *InsertTask) PreExecute() error {
	return nil
}

func (it *InsertTask) Execute() error {
	var tsMsg msgstream.TsMsg = &it.BaseInsertTask
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
			return errors.New("wait to finish failed, timeout")
		}
	}
}

func (it *InsertTask) Notify(err error) {
	it.done <- err
}

type CreateCollectionTask struct {
	internalpb.CreateCollectionRequest
	masterClient masterpb.MasterClient
	done         chan error
	result       *commonpb.Status
	ctx          context.Context
	cancel       context.CancelFunc
}

func (cct *CreateCollectionTask) ID() UniqueID {
	return cct.ReqID
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
		cct.result = &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}
	} else {
		cct.result = resp
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
		case err := <-cct.done:
			return err
		case <-cct.ctx.Done():
			log.Print("wait to finish failed, timeout!")
			return errors.New("wait to finish failed, timeout")
		}
	}
}

func (cct *CreateCollectionTask) Notify(err error) {
	cct.done <- err
}

type DropCollectionTask struct {
	internalpb.DropCollectionRequest
	masterClient masterpb.MasterClient
	done         chan error
	result       *commonpb.Status
	ctx          context.Context
	cancel       context.CancelFunc
}

func (dct *DropCollectionTask) ID() UniqueID {
	return dct.ReqID
}

func (dct *DropCollectionTask) Type() internalpb.MsgType {
	return dct.MsgType
}

func (dct *DropCollectionTask) BeginTs() Timestamp {
	return dct.Timestamp
}

func (dct *DropCollectionTask) EndTs() Timestamp {
	return dct.Timestamp
}

func (dct *DropCollectionTask) SetTs(ts Timestamp) {
	dct.Timestamp = ts
}

func (dct *DropCollectionTask) PreExecute() error {
	return nil
}

func (dct *DropCollectionTask) Execute() error {
	resp, err := dct.masterClient.DropCollection(dct.ctx, &dct.DropCollectionRequest)
	if err != nil {
		log.Printf("drop collection failed, error= %v", err)
		dct.result = &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}
	} else {
		dct.result = resp
	}
	return err
}

func (dct *DropCollectionTask) PostExecute() error {
	return nil
}

func (dct *DropCollectionTask) WaitToFinish() error {
	defer dct.cancel()
	for {
		select {
		case err := <-dct.done:
			return err
		case <-dct.ctx.Done():
			log.Print("wait to finish failed, timeout!")
			return errors.New("wait to finish failed, timeout")
		}
	}
}

func (dct *DropCollectionTask) Notify(err error) {
	dct.done <- err
}

type QueryTask struct {
	internalpb.SearchRequest
	queryMsgStream *msgstream.PulsarMsgStream
	done           chan error
	resultBuf      chan []*internalpb.SearchResult
	result         *servicepb.QueryResult
	ctx            context.Context
	cancel         context.CancelFunc
}

func (qt *QueryTask) ID() UniqueID {
	return qt.ReqID
}

func (qt *QueryTask) Type() internalpb.MsgType {
	return qt.MsgType
}

func (qt *QueryTask) BeginTs() Timestamp {
	return qt.Timestamp
}

func (qt *QueryTask) EndTs() Timestamp {
	return qt.Timestamp
}

func (qt *QueryTask) SetTs(ts Timestamp) {
	qt.Timestamp = ts
}

func (qt *QueryTask) PreExecute() error {
	return nil
}

func (qt *QueryTask) Execute() error {
	var tsMsg msgstream.TsMsg = &msgstream.SearchMsg{
		SearchRequest: qt.SearchRequest,
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: qt.Timestamp,
			EndTimestamp:   qt.Timestamp,
		},
	}
	msgPack := &msgstream.MsgPack{
		BeginTs: qt.Timestamp,
		EndTs:   qt.Timestamp,
		Msgs:    make([]*msgstream.TsMsg, 1),
	}
	msgPack.Msgs[0] = &tsMsg
	qt.queryMsgStream.Produce(msgPack)
	return nil
}

func (qt *QueryTask) PostExecute() error {
	return nil
}

func (qt *QueryTask) WaitToFinish() error {
	defer qt.cancel()
	for {
		select {
		case err := <-qt.done:
			return err
		case <-qt.ctx.Done():
			log.Print("wait to finish failed, timeout!")
			return errors.New("wait to finish failed, timeout")
		}
	}
}

func (qt *QueryTask) Notify(err error) {
	defer qt.cancel()
	defer func() {
		qt.done <- err
	}()
	for {
		select {
		case <-qt.ctx.Done():
			log.Print("wait to finish failed, timeout!")
			return
		case searchResults := <-qt.resultBuf:
			rlen := len(searchResults) // query num
			if rlen <= 0 {
				qt.result = &servicepb.QueryResult{}
				return
			}
			n := len(searchResults[0].Hits) // n
			if n <= 0 {
				qt.result = &servicepb.QueryResult{}
				return
			}
			k := len(searchResults[0].Hits[0].IDs) // k
			queryResult := &servicepb.QueryResult{
				Status: &commonpb.Status{
					ErrorCode: 0,
				},
			}
			// reduce by score, TODO: use better algorithm
			// use merge-sort here, the number of ways to merge is `rlen`
			// in this process, we must make sure:
			//		len(queryResult.Hits) == n
			//		len(queryResult.Hits[i].Ids) == k for i in range(n)
			for i := 0; i < n; n++ { // n
				locs := make([]int, rlen)
				hits := &servicepb.Hits{}
				for j := 0; j < k; j++ { // k
					choice, maxScore := 0, float32(0)
					for q, loc := range locs { // query num, the number of ways to merge
						score := func(score *servicepb.Score) float32 {
							// TODO: get score of root
							return 0.0
						}(searchResults[q].Hits[i].Scores[loc])
						if score > maxScore {
							choice = q
							maxScore = score
						}
					}
					choiceOffset := locs[choice]
					hits.IDs = append(hits.IDs, searchResults[choice].Hits[i].IDs[choiceOffset])
					hits.RowData = append(hits.RowData, searchResults[choice].Hits[i].RowData[choiceOffset])
					hits.Scores = append(hits.Scores, searchResults[choice].Hits[i].Scores[choiceOffset])
					locs[choice]++
				}
				queryResult.Hits = append(queryResult.Hits, hits)
			}
			qt.result = queryResult
		}
	}
}

type HasCollectionTask struct {
	internalpb.HasCollectionRequest
	masterClient masterpb.MasterClient
	done         chan error
	result       *servicepb.BoolResponse
	ctx          context.Context
	cancel       context.CancelFunc
}

func (hct *HasCollectionTask) ID() UniqueID {
	return hct.ReqID
}

func (hct *HasCollectionTask) Type() internalpb.MsgType {
	return hct.MsgType
}

func (hct *HasCollectionTask) BeginTs() Timestamp {
	return hct.Timestamp
}

func (hct *HasCollectionTask) EndTs() Timestamp {
	return hct.Timestamp
}

func (hct *HasCollectionTask) SetTs(ts Timestamp) {
	hct.Timestamp = ts
}

func (hct *HasCollectionTask) PreExecute() error {
	return nil
}

func (hct *HasCollectionTask) Execute() error {
	resp, err := hct.masterClient.HasCollection(hct.ctx, &hct.HasCollectionRequest)
	if err != nil {
		log.Printf("has collection failed, error= %v", err)
		hct.result = &servicepb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    "internal error",
			},
			Value: false,
		}
	} else {
		hct.result = resp
	}
	return err
}

func (hct *HasCollectionTask) PostExecute() error {
	return nil
}

func (hct *HasCollectionTask) WaitToFinish() error {
	defer hct.cancel()
	for {
		select {
		case err := <-hct.done:
			return err
		case <-hct.ctx.Done():
			log.Print("wait to finish failed, timeout!")
			return errors.New("wait to finish failed, timeout")
		}
	}
}

func (hct *HasCollectionTask) Notify(err error) {
	hct.done <- err
}

type DescribeCollectionTask struct {
	internalpb.DescribeCollectionRequest
	masterClient masterpb.MasterClient
	done         chan error
	result       *servicepb.CollectionDescription
	ctx          context.Context
	cancel       context.CancelFunc
}

func (dct *DescribeCollectionTask) ID() UniqueID {
	return dct.ReqID
}

func (dct *DescribeCollectionTask) Type() internalpb.MsgType {
	return dct.MsgType
}

func (dct *DescribeCollectionTask) BeginTs() Timestamp {
	return dct.Timestamp
}

func (dct *DescribeCollectionTask) EndTs() Timestamp {
	return dct.Timestamp
}

func (dct *DescribeCollectionTask) SetTs(ts Timestamp) {
	dct.Timestamp = ts
}

func (dct *DescribeCollectionTask) PreExecute() error {
	return nil
}

func (dct *DescribeCollectionTask) Execute() error {
	resp, err := dct.masterClient.DescribeCollection(dct.ctx, &dct.DescribeCollectionRequest)
	if err != nil {
		log.Printf("describe collection failed, error= %v", err)
		dct.result = &servicepb.CollectionDescription{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    "internal error",
			},
		}
	} else {
		dct.result = resp
	}
	return err
}

func (dct *DescribeCollectionTask) PostExecute() error {
	return nil
}

func (dct *DescribeCollectionTask) WaitToFinish() error {
	defer dct.cancel()
	for {
		select {
		case err := <-dct.done:
			return err
		case <-dct.ctx.Done():
			log.Print("wait to finish failed, timeout!")
			return errors.New("wait to finish failed, timeout")
		}
	}
}

func (dct *DescribeCollectionTask) Notify(err error) {
	dct.done <- err
}

type ShowCollectionsTask struct {
	internalpb.ShowCollectionRequest
	masterClient masterpb.MasterClient
	done         chan error
	result       *servicepb.StringListResponse
	ctx          context.Context
	cancel       context.CancelFunc
}

func (sct *ShowCollectionsTask) ID() UniqueID {
	return sct.ReqID
}

func (sct *ShowCollectionsTask) Type() internalpb.MsgType {
	return sct.MsgType
}

func (sct *ShowCollectionsTask) BeginTs() Timestamp {
	return sct.Timestamp
}

func (sct *ShowCollectionsTask) EndTs() Timestamp {
	return sct.Timestamp
}

func (sct *ShowCollectionsTask) SetTs(ts Timestamp) {
	sct.Timestamp = ts
}

func (sct *ShowCollectionsTask) PreExecute() error {
	return nil
}

func (sct *ShowCollectionsTask) Execute() error {
	resp, err := sct.masterClient.ShowCollections(sct.ctx, &sct.ShowCollectionRequest)
	if err != nil {
		log.Printf("show collections failed, error= %v", err)
		sct.result = &servicepb.StringListResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    "internal error",
			},
		}
	} else {
		sct.result = resp
	}
	return err
}

func (sct *ShowCollectionsTask) PostExecute() error {
	return nil
}

func (sct *ShowCollectionsTask) WaitToFinish() error {
	defer sct.cancel()
	for {
		select {
		case err := <-sct.done:
			return err
		case <-sct.ctx.Done():
			log.Print("wait to finish failed, timeout!")
			return errors.New("wait to finish failed, timeout")
		}
	}
}

func (sct *ShowCollectionsTask) Notify(err error) {
	sct.done <- err
}

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

package allocator

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	msc "github.com/zilliztech/milvus-distributed/internal/distributed/masterservice/client"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/types"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type Timestamp = typeutil.Timestamp

const (
	tsCountPerRPC = 2 << 15
)

type TimestampAllocator struct {
	Allocator

	masterAddress string
	masterClient  types.MasterService

	countPerRPC uint32
	lastTsBegin Timestamp
	lastTsEnd   Timestamp
	PeerID      UniqueID
}

func NewTimestampAllocator(ctx context.Context, masterAddr string) (*TimestampAllocator, error) {
	ctx1, cancel := context.WithCancel(ctx)
	a := &TimestampAllocator{
		Allocator: Allocator{
			Ctx:        ctx1,
			CancelFunc: cancel,
			Role:       "TimestampAllocator",
		},
		masterAddress: masterAddr,
		countPerRPC:   tsCountPerRPC,
	}
	a.TChan = &Ticker{
		UpdateInterval: time.Second,
	}
	a.Allocator.SyncFunc = a.syncTs
	a.Allocator.ProcessFunc = a.processFunc
	a.Allocator.CheckSyncFunc = a.checkSyncFunc
	a.Allocator.PickCanDoFunc = a.pickCanDoFunc
	a.Init()
	return a, nil
}

func (ta *TimestampAllocator) Start() error {
	var err error
	ta.masterClient, err = msc.NewClient(ta.masterAddress, 20*time.Second)
	if err != nil {
		panic(err)
	}

	if err = ta.masterClient.Init(); err != nil {
		panic(err)
	}

	if err = ta.masterClient.Start(); err != nil {
		panic(err)
	}
	return ta.Allocator.Start()
}

func (ta *TimestampAllocator) checkSyncFunc(timeout bool) bool {
	return timeout || len(ta.ToDoReqs) > 0
}

func (ta *TimestampAllocator) pickCanDoFunc() {
	total := uint32(ta.lastTsEnd - ta.lastTsBegin)
	need := uint32(0)
	idx := 0
	for _, req := range ta.ToDoReqs {
		tReq := req.(*TSORequest)
		need += tReq.count
		if need <= total {
			ta.CanDoReqs = append(ta.CanDoReqs, req)
			idx++
		} else {
			break
		}
	}
	ta.ToDoReqs = ta.ToDoReqs[idx:]
	log.Debug("TimestampAllocator pickCanDoFunc",
		zap.Any("need", need),
		zap.Any("total", total),
		zap.Any("remainReqCnt", len(ta.ToDoReqs)))
}

func (ta *TimestampAllocator) gatherReqTsCount() uint32 {
	need := uint32(0)
	for _, req := range ta.ToDoReqs {
		tReq := req.(*TSORequest)
		need += tReq.count
	}
	return need
}

func (ta *TimestampAllocator) syncTs() (bool, error) {
	need := ta.gatherReqTsCount()
	if need < ta.countPerRPC {
		need = ta.countPerRPC
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	req := &masterpb.AllocTimestampRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_RequestTSO,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  ta.PeerID,
		},
		Count: need,
	}

	resp, err := ta.masterClient.AllocTimestamp(ctx, req)
	defer cancel()

	if err != nil {
		return false, fmt.Errorf("syncTimestamp Failed:%w", err)
	}
	ta.lastTsBegin = resp.GetTimestamp()
	ta.lastTsEnd = ta.lastTsBegin + uint64(resp.GetCount())
	return true, nil
}

func (ta *TimestampAllocator) processFunc(req Request) error {
	tsoRequest := req.(*TSORequest)
	tsoRequest.timestamp = ta.lastTsBegin
	ta.lastTsBegin++
	return nil
}

func (ta *TimestampAllocator) AllocOne() (Timestamp, error) {
	ret, err := ta.Alloc(1)
	if err != nil {
		return 0, err
	}
	return ret[0], nil
}

func (ta *TimestampAllocator) Alloc(count uint32) ([]Timestamp, error) {
	req := &TSORequest{
		BaseRequest: BaseRequest{Done: make(chan error), Valid: false},
	}
	req.count = count
	ta.Reqs <- req
	if err := req.Wait(); err != nil {
		return nil, fmt.Errorf("alloc time stamp request failed: %s", err)
	}

	start, count := req.timestamp, req.count
	var ret []Timestamp
	for i := uint32(0); i < count; i++ {
		ret = append(ret, start+uint64(i))
	}
	return ret, nil
}

func (ta *TimestampAllocator) ClearCache() {

}

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

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	idCountPerRPC = 200000
)

// UniqueID is alias of typeutil.UniqueID
type UniqueID = typeutil.UniqueID

type idAllocatorInterface interface {
	AllocID(ctx context.Context, req *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error)
}

// IDAllocator allocate Unique and monotonically increasing IDs from Root Coord.
// It could also batch allocate for less root coord server access
type IDAllocator struct {
	Allocator

	idAllocator idAllocatorInterface

	countPerRPC uint32

	idStart UniqueID
	idEnd   UniqueID

	PeerID UniqueID
}

// NewIDAllocator creates an ID Allocator allocate Unique and monotonically increasing IDs from Root Coord.
func NewIDAllocator(ctx context.Context, idAlloctor idAllocatorInterface, peerID UniqueID) (*IDAllocator, error) {
	ctx1, cancel := context.WithCancel(ctx)
	a := &IDAllocator{
		Allocator: Allocator{
			Ctx:        ctx1,
			CancelFunc: cancel,
			Role:       "IDAllocator",
		},
		countPerRPC: idCountPerRPC,
		idAllocator: idAlloctor,
		PeerID:      peerID,
	}
	a.TChan = &EmptyTicker{}
	a.Allocator.SyncFunc = a.syncID
	a.Allocator.ProcessFunc = a.processFunc
	a.Allocator.CheckSyncFunc = a.checkSyncFunc
	a.Allocator.PickCanDoFunc = a.pickCanDoFunc
	a.Init()
	return a, nil
}

// Start creates some working goroutines of IDAllocator.
func (ia *IDAllocator) Start() error {
	return ia.Allocator.Start()
}

func (ia *IDAllocator) gatherReqIDCount() uint32 {
	need := uint32(0)
	for _, req := range ia.ToDoReqs {
		tReq := req.(*IDRequest)
		need += tReq.count
	}
	return need
}

func (ia *IDAllocator) syncID() (bool, error) {

	need := ia.gatherReqIDCount()
	if need < ia.countPerRPC {
		need = ia.countPerRPC
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	req := &rootcoordpb.AllocIDRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_RequestID,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  ia.PeerID,
		},
		Count: need,
	}
	resp, err := ia.idAllocator.AllocID(ctx, req)

	cancel()
	if err != nil {
		return false, fmt.Errorf("syncID Failed:%w", err)
	}
	ia.idStart = resp.GetID()
	ia.idEnd = ia.idStart + int64(resp.GetCount())
	return true, nil
}

func (ia *IDAllocator) checkSyncFunc(timeout bool) bool {
	return timeout || len(ia.ToDoReqs) > 0
}

func (ia *IDAllocator) pickCanDoFunc() {
	total := uint32(ia.idEnd - ia.idStart)
	need := uint32(0)
	idx := 0
	for _, req := range ia.ToDoReqs {
		iReq := req.(*IDRequest)
		need += iReq.count
		if need <= total {
			ia.CanDoReqs = append(ia.CanDoReqs, req)
			idx++
		} else {
			break
		}
	}
	ia.ToDoReqs = ia.ToDoReqs[idx:]
	log.Debug("IDAllocator pickCanDoFunc",
		zap.Any("need", need),
		zap.Any("total", total),
		zap.Any("remainReqCnt", len(ia.ToDoReqs)))
}

func (ia *IDAllocator) processFunc(req Request) error {
	idRequest := req.(*IDRequest)
	idRequest.id = ia.idStart
	ia.idStart += int64(idRequest.count)
	return nil
}

// AllocOne allocates one id.
func (ia *IDAllocator) AllocOne() (UniqueID, error) {
	ret, _, err := ia.Alloc(1)
	if err != nil {
		return 0, err
	}
	return ret, nil
}

// Alloc allocates the id of the count number.
func (ia *IDAllocator) Alloc(count uint32) (UniqueID, UniqueID, error) {
	req := &IDRequest{BaseRequest: BaseRequest{Done: make(chan error), Valid: false}}

	req.count = count
	ia.Reqs <- req
	if err := req.Wait(); err != nil {
		return 0, 0, err
	}

	start, count := req.id, req.count
	return start, start + int64(count), nil
}

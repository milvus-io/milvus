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

package allocator

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
)

const (
	idCountPerRPC = 200000
)

// IDAllocator allocates Unique and monotonically increasing IDs from Root Coord.
// It could also batch allocate for less root coord server access
type IDAllocator struct {
	CachedAllocator

	remoteAllocator remoteInterface

	countPerRPC uint32

	idStart UniqueID
	idEnd   UniqueID

	PeerID UniqueID
}

// NewIDAllocator creates an ID Allocator allocate Unique and monotonically increasing IDs from RootCoord.
func NewIDAllocator(ctx context.Context, remoteAllocator remoteInterface, peerID UniqueID) (*IDAllocator, error) {
	ctx1, cancel := context.WithCancel(ctx)
	a := &IDAllocator{
		CachedAllocator: CachedAllocator{
			Ctx:        ctx1,
			CancelFunc: cancel,
			Role:       "IDAllocator",
		},
		countPerRPC:     idCountPerRPC,
		remoteAllocator: remoteAllocator,
		PeerID:          peerID,
	}
	a.TChan = &EmptyTicker{}
	a.CachedAllocator.SyncFunc = a.syncID
	a.CachedAllocator.ProcessFunc = a.processFunc
	a.CachedAllocator.CheckSyncFunc = a.checkSyncFunc
	a.CachedAllocator.PickCanDoFunc = a.pickCanDoFunc
	a.Init()
	return a, nil
}

// Start creates some working goroutines of IDAllocator.
func (ia *IDAllocator) Start() error {
	return ia.CachedAllocator.Start()
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
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_RequestID),
			commonpbutil.WithSourceID(ia.PeerID),
		),
		Count: need,
	}
	resp, err := ia.remoteAllocator.AllocID(ctx, req)

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
	if ia.closed() {
		return 0, 0, errors.New("fail to allocate ID, closed allocator")
	}
	req := &IDRequest{BaseRequest: BaseRequest{Done: make(chan error), Valid: false}}

	req.count = count
	ia.Reqs <- req
	if err := req.Wait(); err != nil {
		return 0, 0, err
	}

	start, count := req.id, req.count
	return start, start + int64(count), nil
}

// preventing alloc from a closed allocator stucking forever
func (ia *IDAllocator) closed() bool {
	select {
	case <-ia.Ctx.Done():
		return true
	default:
		return false
	}
}

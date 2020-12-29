package allocator

import (
	"context"
	"log"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

const (
	IDCountPerRPC = 200000
)

type UniqueID = typeutil.UniqueID

type IDAllocator struct {
	Allocator

	idStart UniqueID
	idEnd   UniqueID

	PeerID UniqueID
}

func NewIDAllocator(ctx context.Context, masterAddr string) (*IDAllocator, error) {

	ctx1, cancel := context.WithCancel(ctx)
	a := &IDAllocator{
		Allocator: Allocator{reqs: make(chan request, maxConcurrentRequests),
			ctx:           ctx1,
			cancel:        cancel,
			masterAddress: masterAddr,
			countPerRPC:   IDCountPerRPC,
		},
	}
	a.tChan = &emptyTicker{}
	a.Allocator.syncFunc = a.syncID
	a.Allocator.processFunc = a.processFunc
	a.Allocator.checkSyncFunc = a.checkSyncFunc
	a.Allocator.pickCanDoFunc = a.pickCanDoFunc
	a.init()
	return a, nil
}

func (ia *IDAllocator) syncID() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	req := &internalpb.IDRequest{
		PeerID: ia.PeerID,
		Role:   internalpb.PeerRole_Proxy,
		Count:  ia.countPerRPC,
	}
	resp, err := ia.masterClient.AllocID(ctx, req)

	cancel()
	if err != nil {
		log.Println("syncID Failed!!!!!")
		return false
	}
	ia.idStart = resp.GetID()
	ia.idEnd = ia.idStart + int64(resp.GetCount())
	return true
}

func (ia *IDAllocator) checkSyncFunc(timeout bool) bool {
	return timeout || len(ia.toDoReqs) > 0
}

func (ia *IDAllocator) pickCanDoFunc() {
	total := uint32(ia.idEnd - ia.idStart)
	need := uint32(0)
	idx := 0
	for _, req := range ia.toDoReqs {
		iReq := req.(*idRequest)
		need += iReq.count
		if need <= total {
			ia.canDoReqs = append(ia.canDoReqs, req)
			idx++
		} else {
			break
		}
	}
	ia.toDoReqs = ia.toDoReqs[idx:]
}

func (ia *IDAllocator) processFunc(req request) error {
	idRequest := req.(*idRequest)
	idRequest.id = ia.idStart
	ia.idStart++
	return nil
}

func (ia *IDAllocator) AllocOne() (UniqueID, error) {
	ret, _, err := ia.Alloc(1)
	if err != nil {
		return 0, err
	}
	return ret, nil
}

func (ia *IDAllocator) Alloc(count uint32) (UniqueID, UniqueID, error) {
	req := &idRequest{baseRequest: baseRequest{done: make(chan error), valid: false}}

	req.count = count
	ia.reqs <- req
	req.Wait()

	if !req.IsValid() {
		return 0, 0, nil
	}
	start, count := req.id, req.count
	return start, start + int64(count), nil
}

package allocator

import (
	"context"
	"fmt"
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
	a.Allocator.checkFunc = a.checkFunc
	a.init()
	return a, nil
}

func (ia *IDAllocator) syncID() {
	fmt.Println("syncID")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	req := &internalpb.IDRequest{
		PeerID: 1,
		Role:   internalpb.PeerRole_Proxy,
		Count:  ia.countPerRPC,
	}
	resp, err := ia.masterClient.AllocID(ctx, req)

	cancel()
	if err != nil {
		log.Println("syncID Failed!!!!!")
		return
	}
	ia.idStart = resp.GetID()
	ia.idEnd = ia.idStart + int64(resp.GetCount())
}

func (ia *IDAllocator) checkFunc(timeout bool) bool {
	if timeout {
		return timeout
	}
	need := uint32(0)
	for _, req := range ia.toDoReqs {
		iReq := req.(*idRequest)
		need += iReq.count
	}
	return ia.idStart+int64(need) >= ia.idEnd
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

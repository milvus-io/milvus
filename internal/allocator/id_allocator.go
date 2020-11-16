package allocator

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
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
		Allocator: Allocator{reqs: make(chan request, maxMergeRequests),
			ctx:           ctx1,
			cancel:        cancel,
			masterAddress: masterAddr,
			countPerRPC:   maxMergeRequests,
		},
	}
	a.tChan = &emptyTicker{}
	a.Allocator.syncFunc = a.syncID
	a.Allocator.processFunc = a.processFunc
	return a, nil
}

func (ta *IDAllocator) syncID() {
	fmt.Println("syncID")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	req := &internalpb.IDRequest{
		PeerID: 1,
		Role:   internalpb.PeerRole_Proxy,
		Count:  ta.countPerRPC,
	}
	resp, err := ta.masterClient.AllocID(ctx, req)

	cancel()
	if err != nil {
		log.Panic("syncID Failed!!!!!")
		return
	}
	ta.idStart = resp.GetID()
	ta.idEnd = ta.idStart + int64(resp.GetCount())

}

func (ta *IDAllocator) processFunc(req request) {
	idRequest := req.(*idRequest)
	idRequest.id = 1
	fmt.Println("process ID")
}

func (ta *IDAllocator) AllocOne() (UniqueID, error) {
	ret, _, err := ta.Alloc(1)
	if err != nil {
		return 0, err
	}
	return ret, nil
}

func (ta *IDAllocator) Alloc(count uint32) (UniqueID, UniqueID, error) {
	req := &idRequest{baseRequest: baseRequest{done: make(chan error), valid: false}}

	req.count = count
	ta.reqs <- req
	req.Wait()

	if !req.IsValid() {
		return 0, 0, nil
	}
	start, count := req.id, req.count
	return start, start + int64(count), nil
}

package allocator

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

type IdAllocator struct {
	Allocator

	idStart int64
	idEnd   int64
}

func NewIdAllocator(ctx context.Context) (*IdAllocator, error) {
	ctx1, cancel := context.WithCancel(ctx)
	a := &IdAllocator{
		Allocator: Allocator{reqs: make(chan request, maxMergeRequests),
			ctx:    ctx1,
			cancel: cancel,
		},
	}
	a.tChan = &emptyTicker{}
	a.Allocator.syncFunc = a.syncId
	a.Allocator.processFunc = a.processFunc
	return a, nil
}

func (ta *IdAllocator) syncId() {
	fmt.Println("syncId")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	req := &internalpb.IdRequest{
		PeerId: 1,
		Role:   internalpb.PeerRole_Proxy,
		Count:  ta.countPerRpc,
	}
	resp, err := ta.masterClient.AllocId(ctx, req)

	cancel()
	if err != nil {
		log.Panic("syncId Failed!!!!!")
		return
	}
	ta.idStart = resp.GetId()
	ta.idEnd = ta.idStart + int64(resp.GetCount())

}

func (ta *IdAllocator) processFunc(req request) {
	idRequest := req.(*idRequest)
	idRequest.id = 1
	fmt.Println("process Id")
}

func (ta *IdAllocator) AllocOne() (int64, error) {
	ret, _, err := ta.Alloc(1)
	if err != nil {
		return 0, err
	}
	return ret, nil
}

func (ta *IdAllocator) Alloc(count uint32) (int64, int64, error) {
	req := &idRequest{baseRequest: baseRequest{done: make(chan error), valid: false}}

	req.count = count
	ta.reqs <- req
	req.Wait()

	if !req.IsValid() {
		return 0, 0, nil
	}
	start, count := int64(req.id), req.count
	return start, start + int64(count), nil
}

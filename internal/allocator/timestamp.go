package allocator

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type Timestamp = typeutil.Timestamp

const (
	tsCountPerRPC = 2 << 18 * 10
)

type TimestampAllocator struct {
	Allocator
	lastTsBegin Timestamp
	lastTsEnd   Timestamp
}

func NewTimestampAllocator(ctx context.Context, masterAddr string) (*TimestampAllocator, error) {
	ctx1, cancel := context.WithCancel(ctx)
	a := &TimestampAllocator{
		Allocator: Allocator{reqs: make(chan request, maxConcurrentRequests),
			ctx:           ctx1,
			cancel:        cancel,
			masterAddress: masterAddr,
			countPerRPC:   tsCountPerRPC,
		},
	}
	a.tChan = &ticker{
		updateInterval: time.Second,
	}
	a.Allocator.syncFunc = a.syncTs
	a.Allocator.processFunc = a.processFunc
	return a, nil
}

func (ta *TimestampAllocator) checkFunc(timeout bool) bool {
	if timeout {
		return true
	}
	need := uint32(0)
	for _, req := range ta.toDoReqs {
		iReq := req.(*tsoRequest)
		need += iReq.count
	}
	return ta.lastTsBegin+Timestamp(need) >= ta.lastTsEnd
}

func (ta *TimestampAllocator) syncTs() {
	fmt.Println("sync TS")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	req := &internalpb.TsoRequest{
		PeerID: 1,
		Role:   internalpb.PeerRole_Proxy,
		Count:  ta.countPerRPC,
	}
	resp, err := ta.masterClient.AllocTimestamp(ctx, req)
	log.Printf("resp: %v", resp)

	cancel()
	if err != nil {
		log.Panic("syncID Failed!!!!!")
		return
	}
	ta.lastTsBegin = resp.GetTimestamp()
	ta.lastTsEnd = ta.lastTsBegin + uint64(resp.GetCount())
}

func (ta *TimestampAllocator) processFunc(req request) error {
	tsoRequest := req.(*tsoRequest)
	tsoRequest.timestamp = ta.lastTsBegin
	ta.lastTsBegin++
	fmt.Println("process tso")
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
	req := &tsoRequest{
		baseRequest: baseRequest{done: make(chan error), valid: false},
	}
	req.count = count
	ta.reqs <- req
	fmt.Println("YYYYY ", len(ta.reqs))
	req.Wait()

	if !req.IsValid() {
		return nil, nil
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

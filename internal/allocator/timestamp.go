package allocator

import (
	"context"
	"log"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type Timestamp = typeutil.Timestamp

const (
	tsCountPerRPC = 2 << 15
)

type TimestampAllocator struct {
	Allocator
	lastTsBegin Timestamp
	lastTsEnd   Timestamp
	PeerID      UniqueID
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
	a.Allocator.checkSyncFunc = a.checkSyncFunc
	a.Allocator.pickCanDoFunc = a.pickCanDoFunc
	return a, nil
}

func (ta *TimestampAllocator) checkSyncFunc(timeout bool) bool {
	return timeout || len(ta.toDoReqs) > 0
}

func (ta *TimestampAllocator) pickCanDoFunc() {
	total := uint32(ta.lastTsEnd - ta.lastTsBegin)
	need := uint32(0)
	idx := 0
	for _, req := range ta.toDoReqs {
		tReq := req.(*tsoRequest)
		need += tReq.count
		if need <= total {
			ta.canDoReqs = append(ta.canDoReqs, req)
			idx++
		} else {
			break
		}
	}
	ta.toDoReqs = ta.toDoReqs[idx:]
}

func (ta *TimestampAllocator) syncTs() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	req := &internalpb.TsoRequest{
		PeerID: ta.PeerID,
		Role:   internalpb.PeerRole_Proxy,
		Count:  ta.countPerRPC,
	}
	resp, err := ta.masterClient.AllocTimestamp(ctx, req)

	cancel()
	if err != nil {
		log.Println("syncTimestamp Failed!!!!!")
		return false
	}
	ta.lastTsBegin = resp.GetTimestamp()
	ta.lastTsEnd = ta.lastTsBegin + uint64(resp.GetCount())
	return true
}

func (ta *TimestampAllocator) processFunc(req request) error {
	tsoRequest := req.(*tsoRequest)
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
	req := &tsoRequest{
		baseRequest: baseRequest{done: make(chan error), valid: false},
	}
	req.count = count
	ta.reqs <- req
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

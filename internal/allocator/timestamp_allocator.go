package allocator

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"

	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

type Timestamp = typeutil.Timestamp

const (
	tsCountPerRPC         = 2 << 18 * 10
	defaultUpdateInterval = 1000 * time.Millisecond
)

type TimestampAllocator struct {
	Allocator
	lastTsBegin Timestamp
	lastTsEnd   Timestamp
}

func NewTimestampAllocator(ctx context.Context) (*TimestampAllocator, error) {
	ctx1, cancel := context.WithCancel(ctx)
	a := &TimestampAllocator{
		Allocator: Allocator{reqs: make(chan request, maxMergeRequests),
			ctx:    ctx1,
			cancel: cancel,
		},
	}
	a.tChan = &ticker{
		updateInterval: time.Second,
	}
	a.Allocator.syncFunc = a.syncTs
	a.Allocator.processFunc = a.processFunc
	return a, nil
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

	cancel()
	if err != nil {
		log.Panic("syncID Failed!!!!!")
		return
	}
	ta.lastTsBegin = resp.GetTimestamp()
	ta.lastTsEnd = ta.lastTsBegin + uint64(resp.GetCount())
}

func (ta *TimestampAllocator) processFunc(req request) {
	if req == nil {
		fmt.Println("Occur nil!!!!")
		return
	}
	tsoRequest := req.(*tsoRequest)
	tsoRequest.timestamp = 1
	fmt.Println("process tso")
}

func (ta *TimestampAllocator) AllocOne() (Timestamp, error) {
	ret, err := ta.Alloc(1)
	if err != nil {
		return 0, err
	}
	return ret[0], nil
}

func (ta *TimestampAllocator) Alloc(count uint32) ([]Timestamp, error) {
	//req := tsoReqPool.Get().(*tsoRequest)
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

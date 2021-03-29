package allocator

import (
	"context"
	"sync"
	"time"

	"errors"
)

const (
	maxConcurrentRequests = 10000
)

type Request interface {
	Wait() error
	Notify(error)
}

type BaseRequest struct {
	Done  chan error
	Valid bool
}

func (req *BaseRequest) Wait() error {
	err := <-req.Done
	return err
}

func (req *BaseRequest) Notify(err error) {
	req.Done <- err
}

type IDRequest struct {
	BaseRequest
	id    UniqueID
	count uint32
}

type TSORequest struct {
	BaseRequest
	timestamp Timestamp
	count     uint32
}

type SyncRequest struct {
	BaseRequest
}

type TickerChan interface {
	Chan() <-chan time.Time
	Close()
	Init()
	Reset()
}

type EmptyTicker struct {
	tChan <-chan time.Time
}

func (t *EmptyTicker) Chan() <-chan time.Time {
	return t.tChan
}

func (t *EmptyTicker) Init() {
}

func (t *EmptyTicker) Reset() {
}

func (t *EmptyTicker) Close() {
}

type Ticker struct {
	ticker         *time.Ticker
	UpdateInterval time.Duration //
}

func (t *Ticker) Init() {
	t.ticker = time.NewTicker(t.UpdateInterval)
}

func (t *Ticker) Reset() {
	t.ticker.Reset(t.UpdateInterval)
}

func (t *Ticker) Close() {
	t.ticker.Stop()
}

func (t *Ticker) Chan() <-chan time.Time {
	return t.ticker.C
}

type Allocator struct {
	Ctx        context.Context
	CancelFunc context.CancelFunc

	wg sync.WaitGroup

	Reqs      chan Request
	ToDoReqs  []Request
	CanDoReqs []Request
	SyncReqs  []Request

	TChan         TickerChan
	ForceSyncChan chan Request

	SyncFunc    func() bool
	ProcessFunc func(req Request) error

	CheckSyncFunc func(timeout bool) bool
	PickCanDoFunc func()
}

func (ta *Allocator) Start() error {
	ta.TChan.Init()
	ta.wg.Add(1)
	go ta.mainLoop()
	return nil
}

func (ta *Allocator) Init() {
	ta.ForceSyncChan = make(chan Request, maxConcurrentRequests)
	ta.Reqs = make(chan Request, maxConcurrentRequests)
}

func (ta *Allocator) mainLoop() {
	defer ta.wg.Done()

	loopCtx, loopCancel := context.WithCancel(ta.Ctx)
	defer loopCancel()

	for {
		select {

		case first := <-ta.ForceSyncChan:
			ta.SyncReqs = append(ta.SyncReqs, first)
			pending := len(ta.ForceSyncChan)
			for i := 0; i < pending; i++ {
				ta.SyncReqs = append(ta.SyncReqs, <-ta.ForceSyncChan)
			}
			ta.sync(true)
			ta.finishSyncRequest()

		case <-ta.TChan.Chan():
			ta.pickCanDo()
			ta.finishRequest()
			if ta.sync(true) {
				ta.pickCanDo()
				ta.finishRequest()
			}
			ta.failRemainRequest()

		case first := <-ta.Reqs:
			ta.ToDoReqs = append(ta.ToDoReqs, first)
			pending := len(ta.Reqs)
			for i := 0; i < pending; i++ {
				ta.ToDoReqs = append(ta.ToDoReqs, <-ta.Reqs)
			}
			ta.pickCanDo()
			ta.finishRequest()
			if ta.sync(false) {
				ta.pickCanDo()
				ta.finishRequest()
			}
			ta.failRemainRequest()

		case <-loopCtx.Done():
			return
		}

	}
}

func (ta *Allocator) pickCanDo() {
	if ta.PickCanDoFunc == nil {
		return
	}
	ta.PickCanDoFunc()
}

func (ta *Allocator) sync(timeout bool) bool {
	if ta.SyncFunc == nil || ta.CheckSyncFunc == nil {
		ta.CanDoReqs = ta.ToDoReqs
		ta.ToDoReqs = ta.ToDoReqs[0:0]
		return true
	}
	if !timeout && len(ta.ToDoReqs) == 0 {
		return false
	}
	if !ta.CheckSyncFunc(timeout) {
		return false
	}

	ret := ta.SyncFunc()

	if !timeout {
		ta.TChan.Reset()
	}
	return ret
}

func (ta *Allocator) finishSyncRequest() {
	for _, req := range ta.SyncReqs {
		if req != nil {
			req.Notify(nil)
		}
	}
	ta.SyncReqs = ta.SyncReqs[0:0]
}

func (ta *Allocator) failRemainRequest() {
	for _, req := range ta.ToDoReqs {
		if req != nil {
			req.Notify(errors.New("failed: unexpected error"))
		}
	}
	ta.ToDoReqs = []Request{}
}

func (ta *Allocator) finishRequest() {
	for _, req := range ta.CanDoReqs {
		if req != nil {
			err := ta.ProcessFunc(req)
			req.Notify(err)
		}
	}
	ta.CanDoReqs = []Request{}
}

func (ta *Allocator) revokeRequest(err error) {
	n := len(ta.Reqs)
	for i := 0; i < n; i++ {
		req := <-ta.Reqs
		req.Notify(err)
	}
}

func (ta *Allocator) Close() {
	ta.CancelFunc()
	ta.wg.Wait()
	ta.TChan.Close()
	ta.revokeRequest(errors.New("closing"))
}

func (ta *Allocator) CleanCache() {
	req := &SyncRequest{BaseRequest: BaseRequest{Done: make(chan error), Valid: false}}
	ta.ForceSyncChan <- req
	_ = req.Wait()
}

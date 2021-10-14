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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
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
	UpdateInterval time.Duration
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

	SyncFunc    func() (bool, error)
	ProcessFunc func(req Request) error

	CheckSyncFunc func(timeout bool) bool
	PickCanDoFunc func()
	SyncErr       error
	Role          string
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
		ta.ToDoReqs = nil
		return true
	}
	if !timeout && len(ta.ToDoReqs) == 0 {
		return false
	}
	if !ta.CheckSyncFunc(timeout) {
		return false
	}

	var ret bool
	ret, ta.SyncErr = ta.SyncFunc()

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
	ta.SyncReqs = nil
}

func (ta *Allocator) failRemainRequest() {
	var err error
	if ta.SyncErr != nil {
		err = fmt.Errorf("%s failRemainRequest err:%w", ta.Role, ta.SyncErr)
	} else {
		errMsg := fmt.Sprintf("%s failRemainRequest unexpected error", ta.Role)
		err = errors.New(errMsg)
	}
	if len(ta.ToDoReqs) > 0 {
		log.Debug("Allocator has some reqs to fail",
			zap.Any("Role", ta.Role),
			zap.Any("reqLen", len(ta.ToDoReqs)))
	}
	for _, req := range ta.ToDoReqs {
		if req != nil {
			req.Notify(err)
		}
	}
	ta.ToDoReqs = nil
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
	errMsg := fmt.Sprintf("%s is closing", ta.Role)
	ta.revokeRequest(errors.New(errMsg))
}

func (ta *Allocator) CleanCache() {
	req := &SyncRequest{
		BaseRequest: BaseRequest{
			Done:  make(chan error),
			Valid: false,
		},
	}
	ta.ForceSyncChan <- req
	_ = req.Wait()
}

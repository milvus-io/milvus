package allocator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"google.golang.org/grpc"
)

const (
	maxMergeRequests = 10000
)

type request interface {
	Wait()
	Notify(error)
	IsValid() bool
}

type baseRequest struct {
	done  chan error
	valid bool
}

func (req *baseRequest) Wait() {
	err := <-req.done
	req.valid = err == nil
}

func (req *baseRequest) IsValid() bool {
	return req.valid
}

func (req *baseRequest) Notify(err error) {
	req.done <- err
}

type idRequest struct {
	baseRequest
	id    UniqueID
	count uint32
}

func (req *idRequest) Wait() {
	req.baseRequest.Wait()
}

type tsoRequest struct {
	baseRequest
	timestamp Timestamp
	count     uint32
}

func (req *tsoRequest) Wait() {
	req.baseRequest.Wait()
}

type tickerChan interface {
	Chan() <-chan time.Time
	Close()
	Init()
	Reset()
}

type emptyTicker struct {
	tChan <-chan time.Time
}

func (t *emptyTicker) Chan() <-chan time.Time {
	return t.tChan
}

func (t *emptyTicker) Init() {
}

func (t *emptyTicker) Reset() {
}

func (t *emptyTicker) Close() {
}

type ticker struct {
	ticker         *time.Ticker
	updateInterval time.Duration //
}

func (t *ticker) Init() {
	t.ticker = time.NewTicker(t.updateInterval)
}

func (t *ticker) Reset() {
	t.ticker.Reset(t.updateInterval)
}

func (t *ticker) Close() {
	t.ticker.Stop()
}

func (t *ticker) Chan() <-chan time.Time {
	return t.ticker.C
}

type Allocator struct {
	reqs chan request

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	masterAddress string
	masterConn    *grpc.ClientConn
	masterClient  masterpb.MasterClient
	countPerRPC   uint32

	tChan       tickerChan
	syncFunc    func()
	processFunc func(req request)
}

func (ta *Allocator) Start() error {
	err := ta.connectMaster()
	if err != nil {
		panic("connect to master failed")
	}
	ta.tChan.Init()
	ta.wg.Add(1)
	go ta.mainLoop()
	return nil
}

func (ta *Allocator) connectMaster() error {
	log.Printf("Connected to master, master_addr=%s", ta.masterAddress)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, ta.masterAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Printf("Connect to master failed, error= %v", err)
		return err
	}
	log.Printf("Connected to master, master_addr=%s", ta.masterAddress)
	ta.masterConn = conn
	ta.masterClient = masterpb.NewMasterClient(conn)
	return nil
}

func (ta *Allocator) mainLoop() {
	defer ta.wg.Done()

	loopCtx, loopCancel := context.WithCancel(ta.ctx)
	defer loopCancel()

	defaultSize := maxMergeRequests + 1
	reqs := make([]request, defaultSize)
	for {
		select {
		case <-ta.tChan.Chan():
			ta.sync()
		case first := <-ta.reqs:
			pendingPlus1 := len(ta.reqs) + 1
			reqs[0] = first
			for i := 1; i < pendingPlus1; i++ {
				reqs[i] = <-ta.reqs
			}
			ta.finishRequest(reqs[:pendingPlus1])

		case <-loopCtx.Done():
			return
		}

	}
}

func (ta *Allocator) sync() {
	if ta.syncFunc != nil {
		ta.syncFunc()
		ta.tChan.Reset()
		fmt.Println("synced")
	}
}

func (ta *Allocator) finishRequest(reqs []request) {
	for i := 0; i < len(reqs); i++ {
		ta.processFunc(reqs[i])
		if reqs[i] != nil {
			reqs[i].Notify(nil)
		}
	}
}

func (ta *Allocator) revokeRequest(err error) {
	n := len(ta.reqs)
	for i := 0; i < n; i++ {
		req := <-ta.reqs
		req.Notify(err)
	}
}

func (ta *Allocator) Close() {
	ta.cancel()
	ta.wg.Wait()
	ta.tChan.Close()
	ta.revokeRequest(errors.New("closing"))
}

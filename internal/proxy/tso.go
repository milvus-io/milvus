package proxy

import (
	"context"
	"fmt"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proxy/mock"
	"log"
	"sync"
	"time"
)

// tsCountPerRPC is the count of timestamp requested from master per RPC
const tsCountPerRPC = 2 << 18 * 10

// defaultUpdateInterval is the interval between requesting a batch timestamps from master
const defaultUpdateInterval = time.Millisecond * 1000

// Oracle is the interface that provides strictly ascending timestamps.
type Oracle interface {
	GetTimestamp(ctx context.Context, count uint32) (uint64, error)
	Close()
}

type tsWithTTL struct {
	ts         uint64
	count      uint64
	expireTime time.Time
}

func (ts *tsWithTTL) IsExpired() bool {
	now := time.Now()
	return now.Sub(ts.expireTime) >= 0
}

func (ts *tsWithTTL) CanAllocTs(count uint32) bool {
	return !ts.IsExpired() && ts.count >= uint64(count)
}

// MatserOracle implement Oracle interface, proving strictly ascending timestamps.
// It request and cache a batch timestamps from master, and updating periodically.
type MatserOracle struct {
	c      *mock.TSOClient
	lastTs *tsWithTTL
	quit   chan struct{}
	mux    sync.RWMutex
}

func NewMasterTSO(client *mock.TSOClient) (Oracle, error) {
	o := &MatserOracle{
		c: client,
		lastTs: &tsWithTTL{
			ts:         0,
			count:      0,
			expireTime: time.Time{},
		},
		quit: make(chan struct{}),
	}
	go o.UpdateLastTs(defaultUpdateInterval)
	return o, nil
}

func (o *MatserOracle) UpdateLastTs(updateInterval time.Duration) {
	tick := time.NewTicker(updateInterval)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			// Update ts
			ctx := context.TODO()
			ts, count, tw, err := o.c.GetTimeStamp(ctx, tsCountPerRPC)
			if err != nil {
				break
			} else {
				o.SetTs(ts, count, tw)
			}
		case <-o.quit:
			return
		}
	}
}

func (o *MatserOracle) SetTs(ts uint64, count uint64, timeWindow time.Duration) {
	o.mux.Lock()
	defer o.mux.Unlock()
	if ts > o.lastTs.ts || o.lastTs.ts == 0 {
		o.lastTs.ts = ts
		o.lastTs.count = count
		o.lastTs.expireTime = time.Now().Add(timeWindow)
	}
}

func (o *MatserOracle) GetTimestamp(ctx context.Context, count uint32) (uint64, error) {
	// TODO: add context deadline
	if count > tsCountPerRPC {
		return 0, errors.New("Can't alloc too large count timestamps, count must less than " + fmt.Sprintf("%v", tsCountPerRPC))
	}
	maxRetry := 10
	for i := 0; i < maxRetry; i++ {
		o.mux.RLock()
		retry := !o.lastTs.CanAllocTs(count)
		o.mux.RUnlock()
		if retry {
			// wait for timestamp updated
			log.Printf("MasterOracle GetTimeStamp, retry count: %v", i+1)
			time.Sleep(time.Millisecond * 100)
			continue
		}
		break
	}
	o.mux.Lock()
	defer o.mux.Unlock()
	// TimeStamp has not been update while retry `maxRetry` times
	if !o.lastTs.CanAllocTs(count) {
		return 0, errors.New("MasterOracle GetTimeStamp failed, exceeds max retry times")
	}
	ts := o.lastTs.ts
	o.lastTs.ts += uint64(count)
	o.lastTs.count -= uint64(count)
	return ts, nil
}

func (o *MatserOracle) Close() {
	close(o.quit)
}

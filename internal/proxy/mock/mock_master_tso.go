package mock

import (
	"context"
	"sync"
	"time"
)

const timeWindow = time.Second

type TSOClient struct {
	lastTs uint64
	mux    sync.Mutex
}

// window is 1000ms default
func (c *TSOClient) GetTimeStamp(ctx context.Context, n uint64) (ts uint64, count uint64, window time.Duration, err error) {
	c.mux.Lock()
	defer c.mux.Unlock()
	ts = c.lastTs
	c.lastTs += n
	return ts, n, timeWindow, nil
}

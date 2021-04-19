package mock

import (
	"context"
	"sync"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

const timeWindow = time.Second

type Timestamp = typeutil.Timestamp

type TSOClient struct {
	lastTs Timestamp
	mux    sync.Mutex
}

func (c *TSOClient) GetTimeStamp(ctx context.Context, n Timestamp) (ts Timestamp, count uint64, window time.Duration, err error) {
	c.mux.Lock()
	defer c.mux.Unlock()
	ts = c.lastTs
	c.lastTs += n
	return ts, n, timeWindow, nil
}

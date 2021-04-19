package proxy

import (
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
	"sync"
)

type requestScheduler struct {
	//definitions requestQueue

	//manipulations requestQueue
	manipulationsChan chan *manipulationReq // manipulation queue
	mTimestamp        typeutil.Timestamp
	mTimestampMux     sync.Mutex

	//queries requestQueue
	queryChan     chan *queryReq
	qTimestamp    typeutil.Timestamp
	qTimestampMux sync.Mutex
}

// @param selection
// bit_0 = 1: select definition queue
// bit_1 = 1: select manipulation queue
// bit_2 = 1: select query queue
// example: if mode = 3, then both definition and manipulation queues are selected
func (rs *requestScheduler) AreRequestsDelivered(ts typeutil.Timestamp, selection uint32) bool {
	r1 := func() bool {
		if selection&uint32(2) == 0 {
			return true
		}
		rs.mTimestampMux.Lock()
		defer rs.mTimestampMux.Unlock()
		if rs.mTimestamp >= ts {
			return true
		}
		if len(rs.manipulationsChan) == 0 {
			return true
		}
		return false
	}()

	r2 := func() bool {
		if selection&uint32(4) == 0 {
			return true
		}
		rs.qTimestampMux.Lock()
		defer rs.qTimestampMux.Unlock()
		if rs.qTimestamp >= ts {
			return true
		}
		if len(rs.queryChan) == 0 {
			return true
		}
		return false
	}()

	return r1 && r2
}

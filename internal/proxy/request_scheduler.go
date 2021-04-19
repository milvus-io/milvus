package proxy

import "sync"

type requestScheduler struct {
	//definitions requestQueue

	//manipulations requestQueue
	manipulationsChan chan *manipulationReq // manipulation queue
	m_timestamp       Timestamp
	m_timestamp_mux   sync.Mutex

	//queries requestQueue
	queryChan       chan *queryReq
	q_timestamp     Timestamp
	q_timestamp_mux sync.Mutex
}

// @param selection
// bit_0 = 1: select definition queue
// bit_1 = 1: select manipulation queue
// bit_2 = 1: select query queue
// example: if mode = 3, then both definition and manipulation queues are selected
func (rs *requestScheduler) AreRequestsDelivered(ts Timestamp, selection uint32) bool {
	r1 := func() bool {
		if selection&uint32(2) == 0 {
			return true
		}
		rs.m_timestamp_mux.Lock()
		defer rs.m_timestamp_mux.Unlock()
		if rs.m_timestamp >= ts {
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
		rs.q_timestamp_mux.Lock()
		defer rs.q_timestamp_mux.Unlock()
		if rs.q_timestamp >= ts {
			return true
		}
		if len(rs.queryChan) == 0 {
			return true
		}
		return false
	}()

	return r1 && r2
}

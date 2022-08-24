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

package querynode

import (
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/util/metricsinfo"
)

// readTaskQueueType is the queue type in taskScheduler
type readTaskQueueType int32

const (
	unsolvedQueueType readTaskQueueType = iota
	readyQueueType
	receiveQueueType
	executeQueueType
)

// queueTime counts average queue latency.
type queueTime struct {
	totalDuration time.Duration
	count         int64
}

// readTaskCounter counts readTask by readTaskQueueType.
type readTaskCounter struct {
	sync.Mutex
	searchNQCounter   map[readTaskQueueType]int64 // search nq in queue
	queryTasksCounter map[readTaskQueueType]int64 // query task in queue

	searchQueueTime *queueTime
	queryQueueTime  *queueTime
}

// newReadTaskCounter returns a new readTaskCounter.
func newReadTaskCounter() *readTaskCounter {
	return &readTaskCounter{
		searchNQCounter:   make(map[readTaskQueueType]int64),
		queryTasksCounter: make(map[readTaskQueueType]int64),
		searchQueueTime:   &queueTime{},
		queryQueueTime:    &queueTime{},
	}
}

// add would increase the readTask counter.
func (r *readTaskCounter) add(t readTask, rtQueueType readTaskQueueType) {
	r.Lock()
	defer r.Unlock()
	if st, ok := t.(*searchTask); ok {
		r.searchNQCounter[rtQueueType] += st.NQ
	} else if _, ok = t.(*queryTask); ok {
		r.queryTasksCounter[rtQueueType]++
	}
}

// add would decrease the readTask counter.
func (r *readTaskCounter) sub(t readTask, rtQueueType readTaskQueueType) {
	r.Lock()
	defer r.Unlock()
	if st, ok := t.(*searchTask); ok {
		r.searchNQCounter[rtQueueType] -= st.NQ
	} else if _, ok = t.(*queryTask); ok {
		r.queryTasksCounter[rtQueueType]--
	}
}

// increaseQueueTime increases search or query queue duration.
func (r *readTaskCounter) increaseQueueTime(t readTask) {
	r.Lock()
	defer r.Unlock()
	if st, ok := t.(*searchTask); ok {
		r.searchQueueTime.totalDuration += st.queueDur - st.waitTsDur // ignore tSafe waiting duration
		// TODO: increase nq or 1(request)?
		r.searchQueueTime.count++
	} else if qt, ok := t.(*queryTask); ok {
		r.queryQueueTime.totalDuration += qt.queueDur - qt.waitTsDur
		r.queryQueueTime.count++
	}
}

// resetQueueTime resets searchQueueTime and queryQueueTime of readTaskCounter.
func (r *readTaskCounter) resetQueueTime() {
	r.Lock()
	defer r.Unlock()
	r.searchQueueTime = &queueTime{}
	r.queryQueueTime = &queueTime{}
}

// getSearchNQInQueue returns search nq count.
func (r *readTaskCounter) getSearchNQInQueue() metricsinfo.ReadInfoInQueue {
	r.Lock()
	defer r.Unlock()
	var avgDuration time.Duration
	if r.searchQueueTime.count > 0 {
		avgDuration = r.searchQueueTime.totalDuration / time.Duration(r.searchQueueTime.count)
	}
	return metricsinfo.ReadInfoInQueue{
		UnsolvedQueue:    r.searchNQCounter[unsolvedQueueType],
		ReadyQueue:       r.searchNQCounter[readyQueueType],
		ReceiveChan:      r.searchNQCounter[receiveQueueType],
		ExecuteChan:      r.searchNQCounter[executeQueueType],
		AvgQueueDuration: avgDuration,
	}
}

// getSearchNQInQueue returns query tasks count.
func (r *readTaskCounter) getQueryTasksInQueue() metricsinfo.ReadInfoInQueue {
	r.Lock()
	defer r.Unlock()
	var avgDuration time.Duration
	if r.queryQueueTime.count > 0 {
		avgDuration = r.queryQueueTime.totalDuration / time.Duration(r.queryQueueTime.count)
	}
	return metricsinfo.ReadInfoInQueue{
		UnsolvedQueue:    r.queryTasksCounter[unsolvedQueueType],
		ReadyQueue:       r.queryTasksCounter[readyQueueType],
		ReceiveChan:      r.queryTasksCounter[receiveQueueType],
		ExecuteChan:      r.queryTasksCounter[executeQueueType],
		AvgQueueDuration: avgDuration,
	}
}

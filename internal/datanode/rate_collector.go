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

package datanode

import (
	"sync"

	"github.com/milvus-io/milvus/internal/util/ratelimitutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// rateCollector helps to collect and calculate values (like rate, timeTick and etc...).
type rateCollector struct {
	*ratelimitutil.RateCollector

	flowGraphTtMu sync.Mutex
	flowGraphTt   map[string]Timestamp
}

// newRateCollector returns a new rateCollector.
func newRateCollector() (*rateCollector, error) {
	rc, err := ratelimitutil.NewRateCollector(ratelimitutil.DefaultWindow, ratelimitutil.DefaultGranularity)
	if err != nil {
		return nil, err
	}
	return &rateCollector{
		RateCollector: rc,
		flowGraphTt:   make(map[string]Timestamp),
	}, nil
}

// updateFlowGraphTt updates rateCollector's flow graph time tick.
func (r *rateCollector) updateFlowGraphTt(channel string, t Timestamp) {
	r.flowGraphTtMu.Lock()
	defer r.flowGraphTtMu.Unlock()
	r.flowGraphTt[channel] = t
}

// removeFlowGraphChannel removes channel from flowGraphTt.
func (r *rateCollector) removeFlowGraphChannel(channel string) {
	r.flowGraphTtMu.Lock()
	defer r.flowGraphTtMu.Unlock()
	delete(r.flowGraphTt, channel)
}

// getMinFlowGraphTt returns the minimal time tick of flow graphs.
func (r *rateCollector) getMinFlowGraphTt() Timestamp {
	r.flowGraphTtMu.Lock()
	defer r.flowGraphTtMu.Unlock()
	minTt := typeutil.MaxTimestamp
	for _, t := range r.flowGraphTt {
		if minTt > t {
			minTt = t
		}
	}
	return minTt
}

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

	"github.com/milvus-io/milvus/internal/util/ratelimitutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// rateCollector helps to collect and calculate values (like rate, timeTick and etc...).
type rateCollector struct {
	*ratelimitutil.RateCollector

	rtCounter *readTaskCounter

	tSafesMu sync.Mutex
	tSafes   map[Channel]Timestamp
}

// newRateCollector returns a new rateCollector.
func newRateCollector() (*rateCollector, error) {
	rc, err := ratelimitutil.NewRateCollector(ratelimitutil.DefaultWindow, ratelimitutil.DefaultGranularity)
	if err != nil {
		return nil, err
	}
	return &rateCollector{
		RateCollector: rc,
		rtCounter:     newReadTaskCounter(),
		tSafes:        make(map[Channel]Timestamp),
	}, nil
}

// updateTSafe updates rateCollector's flow graph tSafe.
func (r *rateCollector) updateTSafe(c Channel, t Timestamp) {
	r.tSafesMu.Lock()
	defer r.tSafesMu.Unlock()
	r.tSafes[c] = t
}

// removeTSafeChannel removes channel from tSafes.
func (r *rateCollector) removeTSafeChannel(c string) {
	r.tSafesMu.Lock()
	defer r.tSafesMu.Unlock()
	delete(r.tSafes, c)
}

// getMinTSafe returns the minimal tSafe of flow graphs.
func (r *rateCollector) getMinTSafe() Timestamp {
	r.tSafesMu.Lock()
	defer r.tSafesMu.Unlock()
	minTt := typeutil.MaxTimestamp
	for _, t := range r.tSafes {
		if minTt > t {
			minTt = t
		}
	}
	return minTt
}

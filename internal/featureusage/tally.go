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

package featureusage

const (
	firstTallyFeature = FeatureEfOmitted
	lastTallyFeature  = FeatureRetrievalElementLevel
	numTally          = int(lastTallyFeature-firstTallyFeature) + 1
)

// Tally accumulates per-unit counts for one user request, for the counters
// whose unit is smaller than the request: a search parameter distribution is
// counted once per ANN subrequest, so a hybrid search with three subrequests
// adds three. FeatureSet is the request-level counterpart that deduplicates.
//
// A Tally covers only the per-subrequest counters (firstTallyFeature through
// lastTallyFeature), 27 uint16 slots, 54 bytes on the task that owns it.
// Filling it allocates nothing, and it is flushed once, like a FeatureSet, so
// the retries and the internal re-runs that do not count a request do not
// count its units either. A slot saturates rather than wraps; one request
// never has anywhere near 65535 subrequests.
type Tally [numTally]uint16

// Add records one unit of f. A feature outside the per-subrequest range is
// ignored.
func (t *Tally) Add(f Feature) {
	if t == nil || f < firstTallyFeature || f > lastTallyFeature {
		return
	}
	if i := f - firstTallyFeature; t[i] < ^uint16(0) {
		t[i]++
	}
}

// Count returns how many units of f were recorded.
func (t *Tally) Count(f Feature) uint32 {
	if t == nil || f < firstTallyFeature || f > lastTallyFeature {
		return 0
	}
	return uint32(t[f-firstTallyFeature])
}

// HitAll moves every recorded count into the process counters, once each.
func (t *Tally) HitAll() {
	if t == nil || !enabled.Load() {
		return
	}
	for i, n := range t {
		if n > 0 {
			defaultCounters.HitN(firstTallyFeature+Feature(i), int64(n))
		}
	}
}

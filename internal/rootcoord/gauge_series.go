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

package rootcoord

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// gaugeSeries keeps a gauge that is rewritten from scratch every collection
// round free of series nobody sets any more.
//
// A collection's gauge is set under its current name while the collection is
// reported. Once the collection is dropped, renamed or released it is no longer
// reported, and nothing else deletes the series, which would then show its last
// value forever. So the series a round sets are recorded, and when the round
// completes those the previous round set but this one did not are deleted.
type gaugeSeries struct {
	gauge  *prometheus.GaugeVec
	labels map[string][]string // series key -> label values of every series set and not deleted
}

func newGaugeSeries(gauge *prometheus.GaugeVec) *gaugeSeries {
	return &gaugeSeries{gauge: gauge, labels: make(map[string][]string)}
}

// newRound starts a round of setting the gauge.
func (s *gaugeSeries) newRound() *gaugeRound {
	return &gaugeRound{series: s, labels: make(map[string][]string)}
}

// gaugeRound is one round of setting a gaugeSeries. A round that is abandoned
// without complete deletes nothing; what it set is deleted by the next round
// that completes without setting it.
type gaugeRound struct {
	series *gaugeSeries
	labels map[string][]string
}

func (r *gaugeRound) set(value float64, labelValues ...string) {
	r.series.gauge.WithLabelValues(labelValues...).Set(value)
	key := strings.Join(labelValues, "\x00")
	r.labels[key] = labelValues
	r.series.labels[key] = labelValues
}

// complete deletes the series set before this round and not by it.
func (r *gaugeRound) complete() {
	for key, labelValues := range r.series.labels {
		if _, ok := r.labels[key]; !ok {
			r.series.gauge.DeleteLabelValues(labelValues...)
		}
	}
	r.series.labels = r.labels
}

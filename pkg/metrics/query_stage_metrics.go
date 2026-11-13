// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// Stage labels are fixed at instrumentation sites, never derived from requests.
var (
	QueryStageDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: milvusNamespace, Subsystem: "qv", Name: "stage_duration_seconds",
		Help:    "Wall time of a query/load stage; stages have their own request, view, segment or batch population and must not be summed across populations.",
		Buckets: []float64{0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 120, 300, 600},
	}, []string{"component", "operation", "stage", "result"})
	QueryStageInflight = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: milvusNamespace, Subsystem: "qv", Name: "stage_inflight",
		Help: "Operations currently inside a query/load stage, including blocked operations.",
	}, []string{"component", "operation", "stage"})
	QueryStageItems = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: milvusNamespace, Subsystem: "qv", Name: "stage_items_total",
		Help: "Items handled by a query/load stage; kind identifies the counted unit.",
	}, []string{"component", "operation", "stage", "kind"})
	QueryRequestStageDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: milvusNamespace, Subsystem: "qv", Name: "request_stage_duration_seconds",
		Help:    "Logical DQL request stages recorded together at completion; total=readiness+execution+retry_wait. Path describes load state, not cache warmth.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 120, 300, 600},
	}, []string{"operation", "path", "latency_class", "stage", "result"})
)

var queryFlushOldest struct {
	sync.RWMutex
	provider func() float64
}

func SetQueryFlushOldestProvider(provider func() float64) {
	queryFlushOldest.Lock()
	queryFlushOldest.provider = provider
	queryFlushOldest.Unlock()
}

var QueryFlushOldestPending = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
	Namespace: milvusNamespace, Subsystem: "qv", Name: "flush_oldest_pending_seconds",
	Help: "Age of the oldest unclaimed Coord dirty shard event, including batch hold and same-shard serialization.",
}, func() float64 {
	queryFlushOldest.RLock()
	provider := queryFlushOldest.provider
	queryFlushOldest.RUnlock()
	if provider == nil {
		return 0
	}
	return provider()
})

func registerQueryStages(r prometheus.Registerer) {
	r.MustRegister(QueryFlushOldestPending)
	r.MustRegister(QueryStageDuration, QueryStageInflight, QueryStageItems, QueryRequestStageDuration)
}

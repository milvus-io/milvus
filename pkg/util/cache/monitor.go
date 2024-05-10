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

package cache

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// RegisterLRUCacheMetrics registers metrics for LRU cache.
func RegisterLRUCacheMetrics[K comparable, V any](c Cache[K, V], namespace string, subsystem string, prefix string, constLabel prometheus.Labels) {
	newGaugeFunc := func(name string, help string, valueFunc func() float64) prometheus.Collector {
		return prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Subsystem:   subsystem,
				Name:        fmt.Sprintf("%s_%s", prefix, name),
				Help:        help,
				ConstLabels: constLabel,
			},
			valueFunc,
		)
	}

	ms := []prometheus.Collector{
		newGaugeFunc("hit_total", "cache hit count", func() float64 {
			return float64(c.Stats().HitCount.Load())
		}),
		newGaugeFunc("miss_total", "cache miss count", func() float64 {
			return float64(c.Stats().MissCount.Load())
		}),
		newGaugeFunc("size_bytes", "cache size", func() float64 {
			return float64(c.Size())
		}),
		newGaugeFunc("capacity_bytes", "cache capacity", func() float64 {
			return float64(c.Capacity())
		}),
		newGaugeFunc("load_success_total", "cache load success count", func() float64 {
			return float64(c.Stats().LoadSuccessCount.Load())
		}),
		newGaugeFunc("load_fail_total", "cache load fail count", func() float64 {
			return float64(c.Stats().LoadFailCount.Load())
		}),
		newGaugeFunc("load_duration", "cache load duration", func() float64 {
			return float64(c.Stats().TotalLoadDuration.Load() * 1000)
		}),
		newGaugeFunc("evict_total", "cache evict count", func() float64 {
			return float64(c.Stats().EvictionCount.Load())
		}),
		newGaugeFunc("finalize_total", "cache finalize total", func() float64 {
			return float64(c.Stats().TotalFinalizeCount.Load())
		}),
		newGaugeFunc("finalize_duration", "cache finalize duration", func() float64 {
			return float64(c.Stats().TotalFinalizeDuration.Load() * 1000)
		}),
		newGaugeFunc("reload_total", "cache reload total", func() float64 {
			return float64(c.Stats().TotalReloadCount.Load())
		}),
		newGaugeFunc("reload_duration", "cache reload duration", func() float64 {
			return float64(c.Stats().TotalReloadDuration.Load() * 1000)
		}),
	}

	metrics.GetRegisterer().MustRegister(ms...)
}

// WIP: this function is a showcase of how to use prometheus, do not use it in production.
func PrometheusCacheMonitor[K comparable, V any](c Cache[K, V], namespace, subsystem string) {
	hitRate := prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "cache_hitrate",
			Help:      "hit rate equals hitcount / (hitcount + misscount)",
		},
		func() float64 {
			hit := float64(c.Stats().HitCount.Load())
			miss := float64(c.Stats().MissCount.Load())
			return hit / (hit + miss)
		})
	// TODO: adding more metrics.
	prometheus.MustRegister(hitRate)
}

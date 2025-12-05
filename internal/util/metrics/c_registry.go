/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package metrics

/*
#cgo pkg-config: milvus_core

#include <stdlib.h>
#include "segcore/metrics_c.h"
#include "monitor/monitor_c.h"
#include "monitor/jemalloc_stats_c.h"

*/
import "C"

import (
	"sort"
	"strings"
	"sync"
	"unsafe"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v2/log"
)

// metricSorter is a sortable slice of *dto.Metric.
type metricSorter []*dto.Metric

func (s metricSorter) Len() int {
	return len(s)
}

func (s metricSorter) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s metricSorter) Less(i, j int) bool {
	if len(s[i].Label) != len(s[j].Label) {
		// This should not happen. The metrics are
		// inconsistent. However, we have to deal with the fact, as
		// people might use custom collectors or metric family injection
		// to create inconsistent metrics. So let's simply compare the
		// number of labels in this case. That will still yield
		// reproducible sorting.
		return len(s[i].Label) < len(s[j].Label)
	}
	for n, lp := range s[i].Label {
		vi := lp.GetValue()
		vj := s[j].Label[n].GetValue()
		if vi != vj {
			return vi < vj
		}
	}

	// We should never arrive here. Multiple metrics with the same
	// label set in the same scrape will lead to undefined ingestion
	// behavior. However, as above, we have to provide stable sorting
	// here, even for inconsistent metrics. So sort equal metrics
	// by their timestamp, with missing timestamps (implying "now")
	// coming last.
	if s[i].TimestampMs == nil {
		return false
	}
	if s[j].TimestampMs == nil {
		return true
	}
	return s[i].GetTimestampMs() < s[j].GetTimestampMs()
}

// NormalizeMetricFamilies returns a MetricFamily slice with empty
// MetricFamilies pruned and the remaining MetricFamilies sorted by name within
// the slice, with the contained Metrics sorted within each MetricFamily.
func NormalizeMetricFamilies(metricFamiliesByName map[string]*dto.MetricFamily) []*dto.MetricFamily {
	for _, mf := range metricFamiliesByName {
		sort.Sort(metricSorter(mf.Metric))
	}
	names := make([]string, 0, len(metricFamiliesByName))
	for name, mf := range metricFamiliesByName {
		if len(mf.Metric) > 0 {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	result := make([]*dto.MetricFamily, 0, len(names))
	for _, name := range names {
		result = append(result, metricFamiliesByName[name])
	}
	return result
}

func NewCRegistry() *CRegistry {
	return &CRegistry{
		Registry: prometheus.NewRegistry(),
	}
}

// only re-write the implementation of Gather()
type CRegistry struct {
	*prometheus.Registry
	mtx sync.RWMutex
}

// Gather implements Gatherer.
func (r *CRegistry) Gather() (res []*dto.MetricFamily, err error) {
	var parser expfmt.TextParser

	r.mtx.RLock()
	defer r.mtx.RUnlock()

	cMetricsStr := C.GetKnowhereMetrics()
	metricsStr := C.GoString(cMetricsStr)
	C.free(unsafe.Pointer(cMetricsStr))

	out, err := parser.TextToMetricFamilies(strings.NewReader(metricsStr))
	if err != nil {
		log.Error("fail to parse knowhere prometheus metrics", zap.Error(err))
		return
	}

	cMetricsStr = C.GetCoreMetrics()
	metricsStr = C.GoString(cMetricsStr)
	C.free(unsafe.Pointer(cMetricsStr))

	out1, err := parser.TextToMetricFamilies(strings.NewReader(metricsStr))
	if err != nil {
		log.Error("fail to parse storage prometheus metrics", zap.Error(err))
		return
	}

	maps.Copy(out, out1)

	// Add jemalloc stats metrics
	jemallocMetrics := gatherJemallocMetrics()
	for name, mf := range jemallocMetrics {
		out[name] = mf
	}

	res = NormalizeMetricFamilies(out)
	return
}

// gatherJemallocMetrics collects jemalloc stats and returns them as metric families.
func gatherJemallocMetrics() map[string]*dto.MetricFamily {
	result := make(map[string]*dto.MetricFamily)

	cStats := C.GetJemallocStats()
	if !bool(cStats.success) {
		log.Warn("failed to get jemalloc stats")
		return result
	}

	gaugeType := dto.MetricType_GAUGE

	// Helper function to create a gauge metric family
	createGaugeFamily := func(name, help string, value float64) *dto.MetricFamily {
		return &dto.MetricFamily{
			Name: proto.String(name),
			Help: proto.String(help),
			Type: &gaugeType,
			Metric: []*dto.Metric{
				{
					Gauge: &dto.Gauge{
						Value: proto.Float64(value),
					},
				},
			},
		}
	}

	// Define all jemalloc metrics
	metrics := []struct {
		name  string
		help  string
		value uint64
	}{
		{"milvus_jemalloc_allocated_bytes", "Total number of bytes allocated by the application", uint64(cStats.allocated)},
		{"milvus_jemalloc_active_bytes", "Total number of bytes in active pages allocated by the application", uint64(cStats.active)},
		{"milvus_jemalloc_metadata_bytes", "Total number of bytes dedicated to jemalloc metadata", uint64(cStats.metadata)},
		{"milvus_jemalloc_resident_bytes", "Total number of bytes in physically resident data pages mapped by the allocator", uint64(cStats.resident)},
		{"milvus_jemalloc_mapped_bytes", "Total number of bytes in virtual memory mappings", uint64(cStats.mapped)},
		{"milvus_jemalloc_retained_bytes", "Total number of bytes in retained virtual memory mappings", uint64(cStats.retained)},
	}

	for _, m := range metrics {
		result[m.name] = createGaugeFamily(m.name, m.help, float64(m.value))
	}

	return result
}

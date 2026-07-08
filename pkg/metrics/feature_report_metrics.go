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

package metrics

import (
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	featureReportSourceGo = "go"
	featureLabelName      = "feature"
	sourceLabelName       = "source"
)

var (
	featureReportTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Name:      "feature_report_total",
			Help:      "Count of feature reports emitted.",
		},
		[]string{featureLabelName, sourceLabelName},
	)

	// FeatureHybridSearch marks the hybrid search feature path.
	FeatureHybridSearch = newFeatureReporter("hybrid_search")

	// FeaturePartitionKey marks the partition key feature path.
	FeaturePartitionKey = newFeatureReporter("partition_key")

	// FeatureDynamicField marks the dynamic field feature path.
	FeatureDynamicField = newFeatureReporter("dynamic_field")

	// FeatureBM25Function marks the BM25 function feature path.
	FeatureBM25Function = newFeatureReporter("bm25_function")

	// FeatureResourceGroup marks the resource group feature path.
	FeatureResourceGroup = newFeatureReporter("resource_group")

	// FeatureBulkImport marks the bulk import feature path.
	FeatureBulkImport = newFeatureReporter("bulk_import")

	featureReportInterval = time.Hour
)

// FeatureReporter records throttled reports for one feature. It must not be
// copied after first use.
type FeatureReporter struct {
	nextAllowedNanos atomic.Int64
	_                [64]byte

	name string
}

func newFeatureReporter(name string) *FeatureReporter {
	return &FeatureReporter{name: name}
}

// Name returns the Prometheus label value for the feature.
func (r *FeatureReporter) Name() string {
	if r == nil {
		return ""
	}
	return r.name
}

// Record emits a Go-side report if the feature's per-process reporting interval
// has elapsed.
func (r *FeatureReporter) Record() bool {
	return r.recordAt(time.Now())
}

func (r *FeatureReporter) recordAt(now time.Time) bool {
	if r == nil || r.name == "" {
		return false
	}

	nowNanos := now.UnixNano()
	nextNanos := now.Add(featureReportInterval).UnixNano()

	for {
		old := r.nextAllowedNanos.Load()
		if nowNanos < old {
			return false
		}
		if r.nextAllowedNanos.CompareAndSwap(old, nextNanos) {
			featureReportTotal.WithLabelValues(r.name, featureReportSourceGo).Inc()
			return true
		}
	}
}

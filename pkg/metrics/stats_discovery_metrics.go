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
	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Discovery metrics have fixed, bounded labels; no segment or collection IDs.
var (
	StatsDiscoveryPending = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: milvusNamespace, Subsystem: typeutil.DataCoordRole,
		Name: "stats_discovery_pending", Help: "Pending keys (including retries/in-flight), collection scopes and global scope.",
	}, []string{"scope"})
	StatsDiscoveryOldestAge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: milvusNamespace, Subsystem: typeutil.DataCoordRole,
		Name: "stats_discovery_oldest_dirty_age_seconds", Help: "Age of the oldest unresolved key or reconciliation scope.",
	})
	StatsDiscoveryOverflow = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: milvusNamespace, Subsystem: typeutil.DataCoordRole,
		Name: "stats_discovery_overflow_total", Help: "Dirty state promotions due to segment or collection capacity limits.",
	})
	StatsDiscoveryChecks = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: milvusNamespace, Subsystem: typeutil.DataCoordRole,
		Name: "stats_discovery_checks_total", Help: "Stats discovery checks by result, mode and subjob.",
	}, []string{"result", "mode", "subjob"})
	StatsDiscoveryScannedSegments = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: milvusNamespace, Subsystem: typeutil.DataCoordRole,
		Name: "stats_discovery_scanned_entries_total", Help: "Metadata entries examined by reconciliation, including tombstones.",
	})
	StatsDiscoveryScanDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: milvusNamespace, Subsystem: typeutil.DataCoordRole,
		Name: "stats_discovery_scan_duration_seconds", Help: "Completed scan duration including backpressure.",
		Buckets: prometheus.ExponentialBuckets(0.1, 4, 9),
	})
	StatsDiscoveryDelay = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: milvusNamespace, Subsystem: typeutil.DataCoordRole,
		Name: "stats_discovery_delay_seconds", Help: "Time from admitted dirty key to task submission; excludes pre-admission scan delay.",
		Buckets: prometheus.ExponentialBuckets(0.01, 4, 10),
	})
)

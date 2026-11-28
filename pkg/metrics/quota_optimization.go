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

var (
	DataCoordQuotaCacheWork        = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: milvusNamespace, Subsystem: typeutil.DataCoordRole, Name: "quota_cache_work_total", Help: "Collections recomputed and segments scanned by the quota statistics cache."}, []string{"kind"})
	DataCoordQuotaCachePending     = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: milvusNamespace, Subsystem: typeutil.DataCoordRole, Name: "quota_cache_pending", Help: "Collections awaiting statistics refresh or local consumer acknowledgement."}, []string{"kind"})
	RootCoordQuotaCalculatedScopes = prometheus.NewCounter(prometheus.CounterOpts{Namespace: milvusNamespace, Subsystem: typeutil.RootCoordRole, Name: "quota_calculated_scopes_total", Help: "Scopes recomputed by local quota calculation."})
	RootCoordQuotaSync             = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: milvusNamespace, Subsystem: typeutil.RootCoordRole, Name: "quota_sync_total", Help: "Per-proxy quota synchronization outcomes."}, []string{"mode", "result"})
	RootCoordQuotaUnsynced         = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: milvusNamespace, Subsystem: typeutil.RootCoordRole, Name: "quota_unsynced_proxies", Help: "Proxies not confirmed at the current quota version in the last reconciliation."})
	RootCoordQuotaSnapshotBytes    = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: milvusNamespace, Subsystem: typeutil.RootCoordRole, Name: "quota_snapshot_bytes", Help: "Encoded full quota snapshot size before per-proxy routing headers, once per constructed payload.", Buckets: prometheus.ExponentialBuckets(128, 4, 12)}, []string{"mode"})
)

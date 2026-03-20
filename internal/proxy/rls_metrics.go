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

package proxy

import (
	"strconv"
	"time"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// RLSMetricsRecorder provides helper functions for recording RLS metrics
type RLSMetricsRecorder struct{}

// NewRLSMetricsRecorder creates a new RLS metrics recorder
func NewRLSMetricsRecorder() *RLSMetricsRecorder {
	return &RLSMetricsRecorder{}
}

// Global metrics recorder for convenience
var globalRLSMetrics = NewRLSMetricsRecorder()

// GetRLSMetrics returns the global RLS metrics recorder
func GetRLSMetrics() *RLSMetricsRecorder {
	return globalRLSMetrics
}

func nodeID() string {
	return strconv.FormatInt(paramtable.GetNodeID(), 10)
}

// RecordPolicyEvaluation records an RLS policy evaluation event
func (r *RLSMetricsRecorder) RecordPolicyEvaluation(opType, status, dbName, collectionName string) {
	metrics.ProxyRLSEvaluationTotal.WithLabelValues(nodeID(), opType, status, dbName, collectionName).Inc()
}

// RecordEvaluationLatency records the latency of an RLS policy evaluation
func (r *RLSMetricsRecorder) RecordEvaluationLatency(opType, dbName, collectionName string, latencyMs float64) {
	metrics.ProxyRLSEvaluationLatency.WithLabelValues(nodeID(), opType, dbName, collectionName).Observe(latencyMs)
}

// RecordEvaluationDuration records the duration of an RLS policy evaluation
func (r *RLSMetricsRecorder) RecordEvaluationDuration(opType, dbName, collectionName string, duration time.Duration) {
	r.RecordEvaluationLatency(opType, dbName, collectionName, float64(duration.Milliseconds()))
}

// RecordCacheHit records an RLS cache hit
func (r *RLSMetricsRecorder) RecordCacheHit() {
	metrics.ProxyRLSCacheOps.WithLabelValues(nodeID(), metrics.CacheHitLabel).Inc()
}

// RecordCacheMiss records an RLS cache miss
func (r *RLSMetricsRecorder) RecordCacheMiss() {
	metrics.ProxyRLSCacheOps.WithLabelValues(nodeID(), metrics.CacheMissLabel).Inc()
}

// RecordAccessDenied records an access denied event by RLS policy
func (r *RLSMetricsRecorder) RecordAccessDenied(opType, dbName, collectionName string) {
	metrics.ProxyRLSAccessDenied.WithLabelValues(nodeID(), opType, dbName, collectionName).Inc()
}

// SetActivePolicies sets the number of active RLS policies for a collection
func (r *RLSMetricsRecorder) SetActivePolicies(dbName, collectionName string, count int) {
	metrics.ProxyRLSActivePolicies.WithLabelValues(nodeID(), dbName, collectionName).Set(float64(count))
}

// RecordPolicyLoadFailure records a failure when loading RLS policies
func (r *RLSMetricsRecorder) RecordPolicyLoadFailure(loadType string) {
	metrics.ProxyRLSLoadFailures.WithLabelValues(nodeID(), "policy_"+loadType).Inc()
}

// RecordUserTagsLoadFailure records a failure when loading user tags
func (r *RLSMetricsRecorder) RecordUserTagsLoadFailure() {
	metrics.ProxyRLSLoadFailures.WithLabelValues(nodeID(), "user_tags").Inc()
}

// RLSOpType constants for metrics labels
const (
	RLSOpQuery  = "query"
	RLSOpSearch = "search"
	RLSOpInsert = "insert"
	RLSOpDelete = "delete"
	RLSOpUpsert = "upsert"
)

// RLSStatusSuccess indicates successful RLS evaluation
const RLSStatusSuccess = "success"

// RLSStatusFail indicates failed RLS evaluation
const RLSStatusFail = "fail"

// RLSStatusSkipped indicates RLS was skipped (disabled or no policies)
const RLSStatusSkipped = "skipped"

// RLSStatusDenied indicates access was denied by RLS
const RLSStatusDenied = "denied"

// RLSEvaluationTimer is a helper for timing RLS evaluations
type RLSEvaluationTimer struct {
	startTime      time.Time
	opType         string
	dbName         string
	collectionName string
}

// StartRLSEvaluationTimer starts a timer for RLS evaluation
func StartRLSEvaluationTimer(opType, dbName, collectionName string) *RLSEvaluationTimer {
	return &RLSEvaluationTimer{
		startTime:      time.Now(),
		opType:         opType,
		dbName:         dbName,
		collectionName: collectionName,
	}
}

// Stop stops the timer and records the latency
func (t *RLSEvaluationTimer) Stop(status string) {
	duration := time.Since(t.startTime)
	GetRLSMetrics().RecordEvaluationDuration(t.opType, t.dbName, t.collectionName, duration)
	GetRLSMetrics().RecordPolicyEvaluation(t.opType, status, t.dbName, t.collectionName)
}

// StopSuccess stops the timer and records success
func (t *RLSEvaluationTimer) StopSuccess() {
	t.Stop(RLSStatusSuccess)
}

// StopFail stops the timer and records failure
func (t *RLSEvaluationTimer) StopFail() {
	t.Stop(RLSStatusFail)
}

// StopSkipped stops the timer and records skipped
func (t *RLSEvaluationTimer) StopSkipped() {
	t.Stop(RLSStatusSkipped)
}

// StopDenied stops the timer and records access denied
func (t *RLSEvaluationTimer) StopDenied() {
	t.Stop(RLSStatusDenied)
	GetRLSMetrics().RecordAccessDenied(t.opType, t.dbName, t.collectionName)
}

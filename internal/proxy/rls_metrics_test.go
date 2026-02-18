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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRLSMetricsRecorder(t *testing.T) {
	recorder := NewRLSMetricsRecorder()
	assert.NotNil(t, recorder)
}

func TestGetRLSMetrics(t *testing.T) {
	metrics := GetRLSMetrics()
	assert.NotNil(t, metrics)

	// Test that it returns the global singleton
	metrics2 := GetRLSMetrics()
	assert.Equal(t, metrics, metrics2)
}

func TestRLSMetricsRecordPolicyEvaluation(t *testing.T) {
	recorder := NewRLSMetricsRecorder()

	// Should not panic when recording metrics
	recorder.RecordPolicyEvaluation(RLSOpQuery, RLSStatusSuccess, "testdb", "testcoll")
	recorder.RecordPolicyEvaluation(RLSOpSearch, RLSStatusFail, "testdb", "testcoll")
	recorder.RecordPolicyEvaluation(RLSOpInsert, RLSStatusSkipped, "testdb", "testcoll")
	recorder.RecordPolicyEvaluation(RLSOpDelete, RLSStatusDenied, "testdb", "testcoll")
	recorder.RecordPolicyEvaluation(RLSOpUpsert, RLSStatusSuccess, "testdb", "testcoll")
}

func TestRLSMetricsRecordEvaluationLatency(t *testing.T) {
	recorder := NewRLSMetricsRecorder()

	// Should not panic when recording latency
	recorder.RecordEvaluationLatency(RLSOpQuery, "testdb", "testcoll", 10.5)
	recorder.RecordEvaluationLatency(RLSOpSearch, "testdb", "testcoll", 0.5)
}

func TestRLSMetricsRecordEvaluationDuration(t *testing.T) {
	recorder := NewRLSMetricsRecorder()

	// Should not panic when recording duration
	recorder.RecordEvaluationDuration(RLSOpQuery, "testdb", "testcoll", 10*time.Millisecond)
	recorder.RecordEvaluationDuration(RLSOpSearch, "testdb", "testcoll", 500*time.Microsecond)
}

func TestRLSMetricsRecordCacheStats(t *testing.T) {
	recorder := NewRLSMetricsRecorder()

	// Should not panic when recording cache stats
	recorder.RecordCacheHit()
	recorder.RecordCacheMiss()
}

func TestRLSMetricsRecordAccessDenied(t *testing.T) {
	recorder := NewRLSMetricsRecorder()

	// Should not panic when recording access denied
	recorder.RecordAccessDenied(RLSOpQuery, "testdb", "testcoll")
}

func TestRLSMetricsSetActivePolicies(t *testing.T) {
	recorder := NewRLSMetricsRecorder()

	// Should not panic when setting active policies count
	recorder.SetActivePolicies("testdb", "testcoll", 5)
	recorder.SetActivePolicies("testdb", "testcoll", 0)
}

func TestRLSEvaluationTimer(t *testing.T) {
	timer := StartRLSEvaluationTimer(RLSOpQuery, "testdb", "testcoll")
	assert.NotNil(t, timer)
	assert.Equal(t, RLSOpQuery, timer.opType)
	assert.Equal(t, "testdb", timer.dbName)
	assert.Equal(t, "testcoll", timer.collectionName)

	// Small delay to ensure time measurement works
	time.Sleep(1 * time.Millisecond)

	// Stop should not panic
	timer.Stop(RLSStatusSuccess)
}

func TestRLSEvaluationTimerStopMethods(t *testing.T) {
	// Test StopSuccess
	timer1 := StartRLSEvaluationTimer(RLSOpQuery, "testdb", "testcoll")
	timer1.StopSuccess()

	// Test StopFail
	timer2 := StartRLSEvaluationTimer(RLSOpSearch, "testdb", "testcoll")
	timer2.StopFail()

	// Test StopSkipped
	timer3 := StartRLSEvaluationTimer(RLSOpInsert, "testdb", "testcoll")
	timer3.StopSkipped()

	// Test StopDenied
	timer4 := StartRLSEvaluationTimer(RLSOpDelete, "testdb", "testcoll")
	timer4.StopDenied()
}

func TestRLSOpTypeConstants(t *testing.T) {
	assert.Equal(t, "query", RLSOpQuery)
	assert.Equal(t, "search", RLSOpSearch)
	assert.Equal(t, "insert", RLSOpInsert)
	assert.Equal(t, "delete", RLSOpDelete)
	assert.Equal(t, "upsert", RLSOpUpsert)
}

func TestRLSStatusConstants(t *testing.T) {
	assert.Equal(t, "success", RLSStatusSuccess)
	assert.Equal(t, "fail", RLSStatusFail)
	assert.Equal(t, "skipped", RLSStatusSkipped)
	assert.Equal(t, "denied", RLSStatusDenied)
}

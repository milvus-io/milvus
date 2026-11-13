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

// Package stage records bounded-cardinality query lifecycle metrics.
// Recorders belong at package scope; request identifiers never become metric labels.
package stage

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Result is a bounded telemetry outcome, independent of application error policy.
type Result uint8

const (
	Success Result = iota
	Error
	Canceled
	Timeout
	Superseded
	NotReady
	resultCount
)

var resultNames = [resultCount]string{"success", "error", "canceled", "timeout", "superseded", "not_ready"}

func (r Result) String() string { return resultNames[r] }

// Outcome only describes an observed result. It never changes application
// errors, retries, or wire codes. Callers with embedded statuses must unwrap them.
func Outcome(err error) Result {
	if err == nil {
		return Success
	}
	if errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded {
		return Timeout
	}
	if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled ||
		errors.Is(err, merr.ErrRequestCanceled) || errors.Is(err, merr.ErrSegcoreFollyCancel) {
		return Canceled
	}
	// Remote errors reconstructed from Status retain these numeric codes but
	// no longer wrap Go's context sentinels.
	switch merr.Code(err) {
	case merr.CanceledCode:
		return Canceled
	case merr.TimeoutCode:
		return Timeout
	}
	return Error
}

// Recorder is safe for concurrent use. Reuse it across operations with the same labels.
type Recorder struct {
	durations    [resultCount]prometheus.Observer
	once         [resultCount]sync.Once
	labels       [3]string
	inflight     prometheus.Gauge
	inflightOnce sync.Once
}

// New accepts fixed labels from an instrumentation site, never request identifiers.
func New(component, operation, name string) *Recorder {
	return &Recorder{labels: [3]string{component, operation, name}}
}

// Timer belongs to one operation. It must not be copied or ended concurrently.
type Timer struct {
	recorder *Recorder
	started  time.Time
}

// Begin starts measuring an operation and increments its inflight gauge.
func (r *Recorder) Begin() Timer {
	r.inflightOnce.Do(func() { r.inflight = metrics.QueryStageInflight.WithLabelValues(r.labels[0], r.labels[1], r.labels[2]) })
	r.inflight.Inc()
	return Timer{recorder: r, started: time.Now()}
}

// End is idempotent on this timer. Do not copy an active timer to another owner.
func (t *Timer) End(err error)       { t.EndResult(Outcome(err)) }
func (t *Timer) EndError(err *error) { t.End(*err) }
func (t *Timer) EndResult(result Result) {
	if t.recorder == nil {
		return
	}
	t.recorder.Observe(time.Since(t.started), result)
	t.recorder.inflight.Dec()
	t.recorder = nil
}

// Observe records a completed interval measured by an existing lifecycle owner.
// It does not create an inflight series: live intervals require Begin.
func (r *Recorder) Observe(d time.Duration, result Result) {
	r.once[result].Do(func() {
		r.durations[result] = metrics.QueryStageDuration.WithLabelValues(r.labels[0], r.labels[1], r.labels[2], result.String())
	})
	r.durations[result].Observe(max(0, d.Seconds()))
}

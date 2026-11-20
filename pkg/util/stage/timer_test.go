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

package stage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestOutcome(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want Result
	}{
		{merr.Error(merr.Status(context.Canceled)), Canceled},
		{merr.Error(merr.Status(context.DeadlineExceeded)), Timeout},
		{nil, Success},
		{errors.New("failure"), Error},
		{context.Canceled, Canceled},
		{fmt.Errorf("wrapped: %w", context.DeadlineExceeded), Timeout},
		{status.Error(codes.Canceled, "cancel"), Canceled},
		{status.Error(codes.DeadlineExceeded, "timeout"), Timeout},
		{merr.WrapErrRequestCanceled("operator", "stop"), Canceled},
		{merr.Error(merr.Status(merr.WrapErrRequestCanceled("operator", "stop"))), Canceled},
		{merr.ErrSegcoreFollyCancel, Canceled},
		{merr.Error(merr.Status(merr.ErrSegcoreFollyCancel)), Canceled},
	} {
		require.Equal(t, tc.want, Outcome(tc.err))
	}
}

func TestTimerBalancesEveryOutcomeAndIsIdempotent(t *testing.T) {
	r := New("test", "timer", "balanced")
	for result := Success; result < resultCount; result++ {
		var before dto.Metric
		observer, err := metrics.QueryStageDuration.GetMetricWithLabelValues("test", "timer", "balanced", result.String())
		require.NoError(t, err)
		metric := observer.(prometheus.Metric)
		require.NoError(t, metric.Write(&before))
		timer := r.Begin()
		require.Equal(t, float64(1), testutil.ToFloat64(r.inflight))
		timer.EndResult(result)
		timer.EndResult(result)
		require.Zero(t, testutil.ToFloat64(r.inflight))
		var after dto.Metric
		require.NoError(t, metric.Write(&after))
		require.Equal(t, before.GetHistogram().GetSampleCount()+1, after.GetHistogram().GetSampleCount())
	}
}

func BenchmarkTimer(b *testing.B) {
	r := New("test", "benchmark", "timer")
	r.Observe(time.Nanosecond, Success)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := r.Begin()
		t.End(nil)
	}
}

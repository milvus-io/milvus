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
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	executeDQLRetryRateLimit = "rate_limit"
	executeDQLRetryQueueFull = "queue_full"
)

func (b *ExecuteDQLTask) startAttempt(ctx context.Context, index int, retryReason string) bool {
	b.mu.Lock()
	if b.results == nil || contextutil.ErrOrDeadlineExceeded(ctx) != nil {
		b.mu.Unlock()
		return false
	}
	metrics.ProxyExecuteDQLAttemptsTotal.WithLabelValues(b.nodeID).Inc()
	if retryReason != "" {
		metrics.ProxyExecuteDQLRetriesTotal.WithLabelValues(b.nodeID, retryReason).Inc()
	}
	b.mu.Unlock()
	if retryReason != "" && mlog.LevelEnabled(mlog.DebugLevel) {
		mlog.RatedDebug(ctx, 10, "ExecuteDQL subrequest retry",
			mlog.Int("index", index), mlog.String("reason", retryReason))
	}
	return true
}

func logExecuteDQLRejection(ctx context.Context, index int, reason string) {
	if mlog.LevelEnabled(mlog.DebugLevel) {
		mlog.RatedDebug(ctx, 10, "ExecuteDQL subrequest rejected",
			mlog.Int("index", index), mlog.String("reason", reason))
	}
}

func executeDQLResultLabel(status *commonpb.Status) (string, int32) {
	code := merr.Code(merr.Error(status))
	switch code {
	case 0:
		return "success", code
	case merr.TimeoutCode:
		return "timeout", code
	case merr.CanceledCode, merr.Code(merr.ErrRequestCanceled):
		return "cancelled", code //nolint:misspell // Preserve the documented cancellation label.
	default:
		return "failed", code
	}
}

func traceExecuteDQLCompletion(ctx context.Context, index int, result string, code int32, duration time.Duration) {
	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.AddEvent("ExecuteDQL subrequest finished", trace.WithAttributes(
			attribute.Int("index", index),
			attribute.String("result", result),
			attribute.Int("code", int(code)),
			attribute.Int64("duration_ms", duration.Milliseconds()),
		))
	}
}

func logExecuteDQLCompletion(ctx context.Context, index int, result string, code int32, duration time.Duration) {
	if mlog.LevelEnabled(mlog.DebugLevel) {
		mlog.RatedDebug(ctx, 10, "ExecuteDQL subrequest finished",
			mlog.Int("index", index), mlog.String("result", result), mlog.Int32("code", code),
			mlog.Duration("duration", duration))
	}
}

func observeExecuteDQLBatch(ctx context.Context, nodeID string, startedAt time.Time, size int, results []*milvuspb.DQLResult, err error) {
	outcome := "batch_failed"
	succeeded := 0
	if err == nil {
		for _, result := range results {
			if merr.Ok(result.GetStatus()) {
				succeeded++
			}
		}
		switch succeeded {
		case len(results):
			outcome = "all_success"
		case 0:
			outcome = "all_failed"
		default:
			outcome = "partial_success"
		}
	}
	metrics.ProxyExecuteDQLBatchesTotal.WithLabelValues(nodeID, outcome).Inc()

	duration := time.Since(startedAt)
	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(
			attribute.String("result", outcome),
			attribute.Int("code", int(merr.Code(err))),
			attribute.Int("size", size),
			attribute.Int("succeeded", succeeded),
			attribute.Int64("duration_ms", duration.Milliseconds()),
		)
	}
	if mlog.LevelEnabled(mlog.DebugLevel) {
		mlog.RatedDebug(ctx, 10, "ExecuteDQL batch finished",
			mlog.String("result", outcome), mlog.Int32("code", merr.Code(err)),
			mlog.Int("size", size), mlog.Int("succeeded", succeeded), mlog.Duration("duration", duration))
	}
}

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
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	multiSearchRetryRateLimit = "rate_limit"
	multiSearchRetryQueueFull = "queue_full"
)

// Progress is protected by the batch lock until finish transfers ownership.
type multiSearchItemProgress struct {
	completedAt  time.Time
	backoffStart time.Time
	backoff      time.Duration
	attempts     int
	rateLimited  int
	queueFull    int
	deferrals    int
}

func (b *multiSearchBatch) startAttempt(ctx context.Context, index int, retryReason string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.progress == nil || multiSearchContextErr(ctx) != nil {
		return false
	}
	item := &b.progress[index]
	item.attempts++
	if retryReason != "" {
		item.finishBackoff(time.Now())
		metrics.ProxyMultiSearchRetriesTotal.WithLabelValues(b.nodeID, retryReason).Inc()
	}
	return true
}

func (b *multiSearchBatch) recordRejection(index int, reason string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.progress == nil {
		return
	}
	item := &b.progress[index]
	switch reason {
	case multiSearchRetryRateLimit:
		item.rateLimited++
	case multiSearchRetryQueueFull:
		item.queueFull++
	}
}

func (b *multiSearchBatch) startBackoff(ctx context.Context, index int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.progress != nil && multiSearchContextErr(ctx) == nil {
		item := &b.progress[index]
		item.backoffStart = time.Now()
		item.deferrals++
	}
}

func (p *multiSearchItemProgress) finishBackoff(now time.Time) {
	if !p.backoffStart.IsZero() {
		p.backoff += now.Sub(p.backoffStart)
		p.backoffStart = time.Time{}
	}
}

func (p multiSearchItemProgress) observe(ctx context.Context, nodeID string, index int, batchStart time.Time, status *commonpb.Status) {
	code := merr.Code(merr.Error(status))
	result := "failed"
	switch code {
	case 0:
		result = "success"
	case merr.TimeoutCode:
		result = "timeout"
	case merr.CanceledCode, merr.Code(merr.ErrRequestCanceled):
		result = "cancelled"
	}
	metrics.ProxyMultiSearchSubrequestsTotal.WithLabelValues(nodeID, result).Inc()

	duration := p.completedAt.Sub(batchStart)
	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.AddEvent("MultiSearch subrequest finished", trace.WithAttributes(
			attribute.Int("index", index),
			attribute.String("result", result),
			attribute.Int("code", int(code)),
			attribute.Int("attempts", p.attempts),
			attribute.Int("rate_limited_attempts", p.rateLimited),
			attribute.Int("queue_full_attempts", p.queueFull),
			attribute.Int("deferrals", p.deferrals),
			attribute.Int64("backoff_ms", p.backoff.Milliseconds()),
			attribute.Int64("duration_ms", duration.Milliseconds()),
		))
	}
	if mlog.LevelEnabled(mlog.DebugLevel) {
		mlog.RatedDebug(ctx, 10, "MultiSearch subrequest finished",
			mlog.Int("index", index), mlog.String("result", result), mlog.Int32("code", code),
			mlog.Int("attempts", p.attempts), mlog.Int("rateLimitedAttempts", p.rateLimited),
			mlog.Int("queueFullAttempts", p.queueFull), mlog.Int("deferrals", p.deferrals),
			mlog.Duration("backoff", p.backoff), mlog.Duration("duration", duration))
	}
}

func observeMultiSearchBatch(ctx context.Context, nodeID string, startedAt time.Time, size int, results []*milvuspb.SearchResults, err error) {
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
	metrics.ProxyMultiSearchBatchesTotal.WithLabelValues(nodeID, outcome).Inc()

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
		mlog.RatedDebug(ctx, 10, "MultiSearch batch finished",
			mlog.String("result", outcome), mlog.Int32("code", merr.Code(err)),
			mlog.Int("size", size), mlog.Int("succeeded", succeeded), mlog.Duration("duration", duration))
	}
}

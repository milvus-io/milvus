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

package datacoord

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

type manifestIndexReadRequest struct {
	segment *SegmentInfo
	attempt int
	ready   time.Time
}

type manifestIndexReadResult struct {
	request manifestIndexReadRequest
	entries []packed.ManifestIndexInfo
	err     error
}

// manifestIndexReader delivers each segment's final read result in completion
// order. The caller alone drives scheduling through next; native reads and
// callbacks use this reader's ManifestIOContext executor, with no extra workers.
// The caller must close the reader, including after a metadata installation error.
type manifestIndexReader struct {
	io         *packed.ManifestIOContext
	ctx        context.Context
	cancel     context.CancelFunc
	submitRead func(context.Context, *SegmentInfo, func([]packed.ManifestIndexInfo, error)) error
	remaining  []*SegmentInfo
	limit      int
	// outstanding counts submitted attempts whose results have not been consumed,
	// including immediate rejections. It is mutated only by next/close.
	outstanding int
	results     chan manifestIndexReadResult
	retries     []manifestIndexReadRequest
	timer       *time.Timer
}

func (m *meta) newManifestIndexReader(ctx context.Context, segments []*SegmentInfo, concurrency int, config *indexpb.StorageConfig) *manifestIndexReader {
	ctx, cancel := context.WithCancel(ctx)
	limit := max(1, concurrency)
	io := packed.NewManifestIOContext(limit)
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	return &manifestIndexReader{
		ctx: ctx, cancel: cancel, remaining: segments, limit: limit, io: io,
		results: make(chan manifestIndexReadResult, limit), timer: timer,
		submitRead: func(ctx context.Context, segment *SegmentInfo, complete func([]packed.ManifestIndexInfo, error)) error {
			return m.submitManifestIndexRead(ctx, io, segment.GetManifestPath(), config, complete)
		},
	}
}

// next returns nil, nil at exhaustion. A result may contain a terminal read
// error, which the recovery layer interprets (e.g. an absent dropped manifest).
// A context error stops iteration; close still drains outstanding results.
func (r *manifestIndexReader) next() (*manifestIndexReadResult, error) {
	for {
		if err := r.ctx.Err(); err != nil {
			return nil, err
		}
		wake := r.submitReady()
		if r.outstanding == 0 && len(r.retries) == 0 {
			return nil, nil
		}
		select {
		case <-r.ctx.Done():
			return nil, r.ctx.Err()
		case <-wake:
			continue
		case result := <-r.results:
			r.timer.Stop()
			r.outstanding--
			if r.scheduleRetry(result) {
				continue
			}
			return &result, nil
		}
	}
}

// submitReady maintains one bounded window: outstanding results plus delayed
// retries never exceed limit. Thus callbacks can always enqueue their result
// even while this caller is waiting for shared admission or installing metadata.
func (r *manifestIndexReader) submitReady() <-chan time.Time {
	for i := 0; i < len(r.retries); {
		if time.Now().Before(r.retries[i].ready) {
			i++
			continue
		}
		request := r.retries[i]
		r.retries = append(r.retries[:i], r.retries[i+1:]...)
		r.submit(request)
	}
	for len(r.remaining) > 0 && r.outstanding+len(r.retries) < r.limit && r.ctx.Err() == nil {
		r.submit(manifestIndexReadRequest{segment: r.remaining[0]})
		r.remaining = r.remaining[1:]
	}
	if len(r.retries) == 0 {
		return nil
	}
	earliest := r.retries[0].ready
	for _, request := range r.retries[1:] {
		if request.ready.Before(earliest) {
			earliest = request.ready
		}
	}
	r.timer.Reset(time.Until(earliest))
	return r.timer.C
}

func (r *manifestIndexReader) submit(request manifestIndexReadRequest) {
	request.attempt++
	r.outstanding++
	complete := func(entries []packed.ManifestIndexInfo, err error) {
		r.results <- manifestIndexReadResult{request, entries, err}
	}
	if err := r.submitRead(r.ctx, request.segment, complete); err != nil {
		complete(nil, err) // A rejected submission never invokes its callback.
	}
}

func (r *manifestIndexReader) scheduleRetry(result manifestIndexReadResult) bool {
	if result.err == nil || result.request.attempt >= 3 || !retry.IsRecoverable(result.err) || merr.GetErrorType(result.err) == merr.InputError {
		return false
	}
	delay := 200 * time.Millisecond * time.Duration(1<<(result.request.attempt-1))
	if deadline, ok := r.ctx.Deadline(); ok && time.Until(deadline) < delay {
		return false
	}
	mlog.RatedWarn(r.ctx, 1, "retry manifest index read",
		mlog.FieldSegmentID(result.request.segment.GetID()), mlog.Int("attempt", result.request.attempt),
		mlog.Duration("delay", delay), mlog.Err(result.err))
	result.request.ready = time.Now().Add(delay)
	r.retries = append(r.retries, result.request)
	return true
}

func (r *manifestIndexReader) close() {
	r.cancel()
	r.timer.Stop()
	// Delayed retries were never submitted and need no callback. Accepted reads
	// each deliver one terminal result even after cancellation.
	for r.outstanding > 0 {
		<-r.results
		r.outstanding--
	}
	r.io.Close()
}

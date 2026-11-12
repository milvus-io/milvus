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

package coordview

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/stage"
)

const (
	waitShard = iota
	waitBatch
	waitEligible
)

var (
	flushPending = stage.New("coord", "flush", "pending_total")
	flushWaits   = [3]*stage.Recorder{
		stage.New("coord", "flush", "same_shard_wait"),
		stage.New("coord", "flush", "batch_hold"),
		stage.New("coord", "flush", "eligible_wait"),
	}
	flushSave      = stage.New("coord", "flush", "catalog_save")
	flushCallbacks = stage.New("coord", "flush", "after_persist")
	flushSync      = stage.New("coord", "flush", "sync_enqueue")
	flushTotal     = stage.New("coord", "flush", "execute")
)

type pendingTiming struct {
	created   time.Time
	since     time.Time
	reason    int
	durations [3]time.Duration
	total     stage.Timer
	finished  bool
}

func newPendingTiming() pendingTiming {
	now := time.Now()
	return pendingTiming{created: now, since: now, reason: waitEligible, total: flushPending.Begin()}
}

func (p *pendingTiming) change(reason int) {
	now := time.Now()
	p.durations[p.reason] += now.Sub(p.since)
	p.since, p.reason = now, reason
}

func (p *pendingTiming) finish(err error) {
	if p.finished {
		return
	}
	p.change(p.reason)
	p.finished = true
	for i, duration := range p.durations {
		flushWaits[i].Observe(duration, stage.Outcome(err))
	}
	p.total.End(err)
}

func (s *DirtyViewFlushScheduler) waitReason(shard qviews.ShardID) int {
	if _, ok := s.inflight[shard]; ok {
		return waitShard
	}
	if _, ok := s.held[shard]; ok {
		return waitBatch
	}
	return waitEligible
}

// Called only by the metrics collector, never from the scheduling hot path.
func (s *DirtyViewFlushScheduler) oldestPendingAge() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var oldest time.Time
	for _, pending := range s.pending {
		if oldest.IsZero() || pending.timing.created.Before(oldest) {
			oldest = pending.timing.created
		}
	}
	if oldest.IsZero() {
		return 0
	}
	return time.Since(oldest).Seconds()
}

func (s *DirtyViewFlushScheduler) finishPending() {
	for _, pending := range s.pending {
		pending.timing.finish(context.Canceled)
	}
}

var flushPack = stage.New("coord", "flush", "pack")

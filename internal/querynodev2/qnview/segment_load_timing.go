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

package qnview

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/stage"
)

const (
	segmentLoadTimingLogInterval = 5 * time.Second
	segmentLoadTimingLogBatch    = 2000
)

var sqnSegmentLoadTimingStats = segmentLoadTimingStats{
	interval:  segmentLoadTimingLogInterval,
	batchSize: segmentLoadTimingLogBatch,
}

type segmentLoadTimingSample struct {
	total           time.Duration
	reserveResource time.Duration
	physicalLoad    time.Duration
	physicalDetail  segments.PhysicalLoadTiming
	releaseResource time.Duration
	onLoaded        time.Duration
	failed          bool
	result          stage.Result
}

type durationStats struct {
	total time.Duration
	max   time.Duration
}

func (s *durationStats) add(duration time.Duration) {
	s.total += duration
	s.max = max(s.max, duration)
}

func (s durationStats) average(count int64) time.Duration {
	if count == 0 {
		return 0
	}
	return s.total / time.Duration(count)
}

type physicalLoadDetailStats struct {
	newSegment         durationStats
	loadSegment        durationStats
	sealedLoad         durationStats
	sealedPrepare      durationStats
	sealedLoadPoolWait durationStats
	localSegmentLoad   durationStats
	cSegmentLoad       durationStats
	syncJSONStats      durationStats
	sealedPostLoad     durationStats
	deltaLogs          durationStats
	pkCandidate        durationStats
}

func (s *physicalLoadDetailStats) add(timing segments.PhysicalLoadTiming) {
	s.newSegment.add(timing.NewSegment)
	s.loadSegment.add(timing.LoadSegment)
	s.sealedLoad.add(timing.SealedLoad)
	s.sealedPrepare.add(timing.SealedPrepare)
	s.sealedLoadPoolWait.add(timing.SealedLoadPoolWait)
	s.localSegmentLoad.add(timing.LocalSegmentLoad)
	s.cSegmentLoad.add(timing.CSegmentLoad)
	s.syncJSONStats.add(timing.SyncJSONStats)
	s.sealedPostLoad.add(timing.SealedPostLoad)
	s.deltaLogs.add(timing.DeltaLogs)
	s.pkCandidate.add(timing.PKCandidate)
}

type segmentLoadTimingSnapshot struct {
	count          int64
	failed         int64
	windowDuration time.Duration

	total           durationStats
	reserveResource durationStats
	physicalLoad    durationStats
	physicalDetail  physicalLoadDetailStats
	releaseResource durationStats
	onLoaded        durationStats
}

type segmentLoadTimingStats struct {
	mu         sync.Mutex
	interval   time.Duration
	batchSize  int64
	windowFrom time.Time

	count  int64
	failed int64

	total           durationStats
	reserveResource durationStats
	physicalLoad    durationStats
	physicalDetail  physicalLoadDetailStats
	releaseResource durationStats
	onLoaded        durationStats
}

func recordSQNSegmentLoadTiming(ctx context.Context, sample segmentLoadTimingSample) {
	observeSegmentLoadAttempt(sample)
	snapshot, ok := sqnSegmentLoadTimingStats.add(time.Now(), sample)
	if !ok {
		return
	}
	logSQNSegmentLoadTiming(ctx, snapshot)
}

// All stages have one observation per completed scheduler attempt, including
// zero for stages that were not reached. Every observation uses the final load
// outcome. This keeps sum/count comparisons valid across early failures and the
// final partial logging batch. Parent and child intervals must not be added.
var segmentLoadAttemptStages = [...]struct {
	recorder *stage.Recorder
	duration func(segmentLoadTimingSample) time.Duration
}{
	{stage.New("queryNode", "segment_load_attempt", "total"), func(s segmentLoadTimingSample) time.Duration { return s.total }},
	{stage.New("queryNode", "segment_load_attempt", "reserve_resource"), func(s segmentLoadTimingSample) time.Duration { return s.reserveResource }},
	{stage.New("queryNode", "segment_load_attempt", "physical_load"), func(s segmentLoadTimingSample) time.Duration { return s.physicalLoad }},
	{stage.New("queryNode", "segment_load_attempt", "new_segment"), func(s segmentLoadTimingSample) time.Duration { return s.physicalDetail.NewSegment }},
	{stage.New("queryNode", "segment_load_attempt", "load_segment"), func(s segmentLoadTimingSample) time.Duration { return s.physicalDetail.LoadSegment }},
	{stage.New("queryNode", "segment_load_attempt", "sealed_load"), func(s segmentLoadTimingSample) time.Duration { return s.physicalDetail.SealedLoad }},
	{stage.New("queryNode", "segment_load_attempt", "sealed_prepare"), func(s segmentLoadTimingSample) time.Duration { return s.physicalDetail.SealedPrepare }},
	{stage.New("queryNode", "segment_load_attempt", "load_pool_queue"), func(s segmentLoadTimingSample) time.Duration { return s.physicalDetail.SealedLoadPoolWait }},
	{stage.New("queryNode", "segment_load_attempt", "local_segment_load"), func(s segmentLoadTimingSample) time.Duration { return s.physicalDetail.LocalSegmentLoad }},
	{stage.New("queryNode", "segment_load_attempt", "csegment_load"), func(s segmentLoadTimingSample) time.Duration { return s.physicalDetail.CSegmentLoad }},
	{stage.New("queryNode", "segment_load_attempt", "sync_json_stats"), func(s segmentLoadTimingSample) time.Duration { return s.physicalDetail.SyncJSONStats }},
	{stage.New("queryNode", "segment_load_attempt", "sealed_post_load"), func(s segmentLoadTimingSample) time.Duration { return s.physicalDetail.SealedPostLoad }},
	{stage.New("queryNode", "segment_load_attempt", "delta_logs"), func(s segmentLoadTimingSample) time.Duration { return s.physicalDetail.DeltaLogs }},
	{stage.New("queryNode", "segment_load_attempt", "pk_candidate"), func(s segmentLoadTimingSample) time.Duration { return s.physicalDetail.PKCandidate }},
	{stage.New("queryNode", "segment_load_attempt", "release_resource"), func(s segmentLoadTimingSample) time.Duration { return s.releaseResource }},
	{stage.New("queryNode", "segment_load_attempt", "on_loaded"), func(s segmentLoadTimingSample) time.Duration { return s.onLoaded }},
}

func observeSegmentLoadAttempt(sample segmentLoadTimingSample) {
	for _, metric := range segmentLoadAttemptStages {
		metric.recorder.Observe(metric.duration(sample), sample.result)
	}
}

func (s *segmentLoadTimingStats) add(now time.Time, sample segmentLoadTimingSample) (segmentLoadTimingSnapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.windowFrom.IsZero() {
		s.windowFrom = now
	}
	s.count++
	if sample.failed {
		s.failed++
	}
	s.total.add(sample.total)
	s.reserveResource.add(sample.reserveResource)
	s.physicalLoad.add(sample.physicalLoad)
	s.physicalDetail.add(sample.physicalDetail)
	s.releaseResource.add(sample.releaseResource)
	s.onLoaded.add(sample.onLoaded)

	if s.count < s.batchSize && now.Sub(s.windowFrom) < s.interval {
		return segmentLoadTimingSnapshot{}, false
	}
	snapshot := segmentLoadTimingSnapshot{
		count:           s.count,
		failed:          s.failed,
		windowDuration:  max(now.Sub(s.windowFrom), time.Duration(0)),
		total:           s.total,
		reserveResource: s.reserveResource,
		physicalLoad:    s.physicalLoad,
		physicalDetail:  s.physicalDetail,
		releaseResource: s.releaseResource,
		onLoaded:        s.onLoaded,
	}
	s.reset()
	return snapshot, true
}

func (s *segmentLoadTimingStats) reset() {
	s.windowFrom = time.Time{}
	s.count = 0
	s.failed = 0
	s.total = durationStats{}
	s.reserveResource = durationStats{}
	s.physicalLoad = durationStats{}
	s.physicalDetail = physicalLoadDetailStats{}
	s.releaseResource = durationStats{}
	s.onLoaded = durationStats{}
}

func logSQNSegmentLoadTiming(ctx context.Context, snapshot segmentLoadTimingSnapshot) {
	mlog.Info(
		ctx, "[SN recovery] SQN segment load timing",
		mlog.String("phase", "sqn_segment_load_timing"),
		mlog.String("component", "queryNode"),
		mlog.Int64("count", snapshot.count),
		mlog.Int64("failed", snapshot.failed),
		mlog.Duration("windowDuration", snapshot.windowDuration),
		mlog.Duration("avgTotal", snapshot.total.average(snapshot.count)),
		mlog.Duration("maxTotal", snapshot.total.max),
		mlog.Duration("avgReserveResource", snapshot.reserveResource.average(snapshot.count)),
		mlog.Duration("maxReserveResource", snapshot.reserveResource.max),
		mlog.Duration("avgPhysicalLoad", snapshot.physicalLoad.average(snapshot.count)),
		mlog.Duration("maxPhysicalLoad", snapshot.physicalLoad.max),
		mlog.Duration("avgPhysicalNewSegment", snapshot.physicalDetail.newSegment.average(snapshot.count)),
		mlog.Duration("maxPhysicalNewSegment", snapshot.physicalDetail.newSegment.max),
		mlog.Duration("avgPhysicalLoadSegment", snapshot.physicalDetail.loadSegment.average(snapshot.count)),
		mlog.Duration("maxPhysicalLoadSegment", snapshot.physicalDetail.loadSegment.max),
		mlog.Duration("avgPhysicalSealedLoad", snapshot.physicalDetail.sealedLoad.average(snapshot.count)),
		mlog.Duration("maxPhysicalSealedLoad", snapshot.physicalDetail.sealedLoad.max),
		mlog.Duration("avgPhysicalSealedPrepare", snapshot.physicalDetail.sealedPrepare.average(snapshot.count)),
		mlog.Duration("maxPhysicalSealedPrepare", snapshot.physicalDetail.sealedPrepare.max),
		mlog.Duration("avgPhysicalLoadPoolWait", snapshot.physicalDetail.sealedLoadPoolWait.average(snapshot.count)),
		mlog.Duration("maxPhysicalLoadPoolWait", snapshot.physicalDetail.sealedLoadPoolWait.max),
		mlog.Duration("avgPhysicalLocalSegmentLoad", snapshot.physicalDetail.localSegmentLoad.average(snapshot.count)),
		mlog.Duration("maxPhysicalLocalSegmentLoad", snapshot.physicalDetail.localSegmentLoad.max),
		mlog.Duration("avgPhysicalCSegmentLoad", snapshot.physicalDetail.cSegmentLoad.average(snapshot.count)),
		mlog.Duration("maxPhysicalCSegmentLoad", snapshot.physicalDetail.cSegmentLoad.max),
		mlog.Duration("avgPhysicalSyncJSONStats", snapshot.physicalDetail.syncJSONStats.average(snapshot.count)),
		mlog.Duration("maxPhysicalSyncJSONStats", snapshot.physicalDetail.syncJSONStats.max),
		mlog.Duration("avgPhysicalPostLoad", snapshot.physicalDetail.sealedPostLoad.average(snapshot.count)),
		mlog.Duration("maxPhysicalPostLoad", snapshot.physicalDetail.sealedPostLoad.max),
		mlog.Duration("avgPhysicalDeltaLogs", snapshot.physicalDetail.deltaLogs.average(snapshot.count)),
		mlog.Duration("maxPhysicalDeltaLogs", snapshot.physicalDetail.deltaLogs.max),
		mlog.Duration("avgPhysicalPKCandidate", snapshot.physicalDetail.pkCandidate.average(snapshot.count)),
		mlog.Duration("maxPhysicalPKCandidate", snapshot.physicalDetail.pkCandidate.max),
		mlog.Duration("avgReleaseResource", snapshot.releaseResource.average(snapshot.count)),
		mlog.Duration("maxReleaseResource", snapshot.releaseResource.max),
		mlog.Duration("avgOnLoaded", snapshot.onLoaded.average(snapshot.count)),
		mlog.Duration("maxOnLoaded", snapshot.onLoaded.max),
	)
}

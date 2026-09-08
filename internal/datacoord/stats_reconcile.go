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

package datacoord

import (
	"context"
	"fmt"
	"iter"
	"time"

	"github.com/cockroachdb/errors"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type statsSubmitResult string

const (
	statsSubmitted   statsSubmitResult = "submitted"
	statsExisting    statsSubmitResult = "existing"
	statsNotNeeded   statsSubmitResult = "not_needed"
	statsDeferred    statsSubmitResult = "deferred"
	statsWouldSubmit statsSubmitResult = "would_submit"
)

type statsDiscoveryOptions struct {
	mode                                                             string
	maxPending, maxCollections, scanBatchSize                        int
	scanInterval, reconcileInterval, retryInterval, retryMaxInterval time.Duration
}

func getStatsDiscoveryOptions() statsDiscoveryOptions {
	p := &Params.DataCoordCfg
	return statsDiscoveryOptions{
		mode: p.StatsDiscoveryMode.GetValue(),
		// Implementation budgets, not independent operator tuning knobs.
		maxPending:        4096,
		maxCollections:    128,
		scanBatchSize:     128,
		scanInterval:      100 * time.Millisecond,
		reconcileInterval: p.StatsDiscoveryReconcileInterval.GetAsDuration(time.Second),
		retryInterval:     time.Second,
		retryMaxInterval:  30 * time.Second,
	}
}

type statsScanCursor struct {
	collectionID                 int64
	generation                   uint64
	next                         func() (int64, int64, bool)
	stop                         func()
	started                      time.Time
	segmentID, segmentCollection int64
	subjob                       int
	hasSegment                   bool
}

// rangeStatsSegments snapshots identities under segMu before yielding. The
// iterator may pause between batches, so it must not retain a metadata lock.
// Publication notifications cover changes after the identity snapshot.
func (m *meta) rangeStatsSegments(collectionID int64) iter.Seq2[int64, int64] {
	return func(yield func(int64, int64) bool) {
		m.segMu.RLock()
		segments := m.segments.segments
		if collectionID != 0 {
			segments = m.segments.secondaryIndexes.coll2Segments[collectionID]
		}
		identities := make([][2]int64, 0, len(segments))
		for id, segment := range segments {
			identities = append(identities, [2]int64{segment.GetCollectionID(), id})
		}
		m.segMu.RUnlock()
		for _, identity := range identities {
			if !yield(identity[0], identity[1]) {
				return
			}
		}
	}
}

type statsFieldRules struct {
	collection             *collectionInfo
	textFields, jsonFields []int64
	resources              []*internalpb.FileResourceInfo
	resourcesLoaded        bool
}

func (si *statsInspector) reconcileStats(key statsReconcileKey, rules map[int64]statsFieldRules) (statsSubmitResult, error) {
	if err := si.ctx.Err(); err != nil {
		return statsDeferred, err
	}
	segment := si.mt.GetHealthySegment(si.ctx, key.segmentID)
	if segment == nil {
		return statsNotNeeded, nil
	}
	collection := si.mt.GetCollection(segment.GetCollectionID())
	if collection == nil {
		if si.handler == nil || si.discoveryOptions.mode == "shadow" {
			return statsDeferred, nil
		}
		// A cache miss is not proof of deletion. Consult the existing metadata
		// loader, with bounded time; only a confirmed not-found ends discovery.
		ctx, cancel := context.WithTimeout(si.ctx, 10*time.Second)
		var err error
		collection, err = si.handler.GetCollection(ctx, segment.GetCollectionID())
		cancel()
		if errors.Is(err, merr.ErrCollectionNotFound) {
			return statsNotNeeded, nil
		}
		if err != nil || collection == nil {
			return statsDeferred, err
		}
	}
	fields, ok := rules[collection.ID]
	if !ok || fields.collection != collection {
		fields = statsFieldRules{collection: collection}
		for _, field := range collection.Schema.GetFields() {
			if typeutil.IsMatchEnabled(field) {
				fields.textFields = append(fields.textFields, field.GetFieldID())
			}
			if typeutil.IsJSONType(field.GetDataType()) {
				fields.jsonFields = append(fields.jsonFields, field.GetFieldID())
			}
		}
		rules[collection.ID] = fields
	}
	switch key.subjob {
	case indexpb.StatsSubJob_TextIndexJob:
		if !needDoTextIndex(segment, fields.textFields, collection.IsExternal()) {
			return statsNotNeeded, nil
		}
	case indexpb.StatsSubJob_JsonKeyIndexJob:
		if jsonShreddingDisabledByDeprecatedConfig() || !Params.CommonCfg.EnabledJSONKeyStats.GetAsBool() ||
			(collection.IsExternal() && !canBuildExternalJSONKeyIndex(segment)) ||
			!needDoJSONKeyIndex(segment, fields.jsonFields, collection.IsExternal()) {
			return statsNotNeeded, nil
		}
	default:
		return statsNotNeeded, nil
	}
	if si.mt.statsTaskMeta.HasStatsTask(key.segmentID, key.subjob) {
		return statsExisting, nil
	}
	if !si.canSubmitStatsTask(key.subjob) {
		return statsDeferred, nil
	}
	if si.discoveryOptions.mode == "shadow" {
		return statsWouldSubmit, nil // no resource RPC, ID allocation, persist or enqueue.
	}
	var resources []*internalpb.FileResourceInfo
	if key.subjob == indexpb.StatsSubJob_TextIndexJob &&
		fileresource.IsRefMode(Params.CommonCfg.DNFileResourceMode.GetValue()) &&
		len(collection.Schema.GetFileResourceIds()) > 0 {
		if !fields.resourcesLoaded {
			ctx, cancel := context.WithTimeout(si.ctx, 10*time.Second)
			var err error
			fields.resources, err = si.mt.GetFileResources(ctx, collection.Schema.GetFileResourceIds()...)
			cancel()
			if err != nil {
				return statsDeferred, err
			}
			fields.resourcesLoaded = true
			rules[collection.ID] = fields
		}
		if si.mt.GetCollection(collection.ID) != collection {
			return statsDeferred, nil // resource IDs belong to an obsolete schema.
		}
		resources = fields.resources
	}
	return si.submitStatsTask(key.segmentID, key.segmentID, key.subjob, true, resources)
}

// advanceStatsScans shares one budget across at most four suspended iterators.
// Per-collection queue quotas and round-robin cursors stop a large collection
// from blocking smaller collection scans. A full queue retains the current ID.
func (si *statsInspector) advanceStatsScans(cursors *[]*statsScanCursor, round *int) {
	q := si.discovery
	for len(*cursors) < 4 {
		id, gen, ok := q.beginScan()
		if !ok {
			break
		}
		next, stop := iter.Pull2(si.mt.rangeStatsSegments(id))
		*cursors = append(*cursors, &statsScanCursor{
			collectionID: id, generation: gen, next: next, stop: stop, started: time.Now(),
		})
	}
	jobs := [...]indexpb.StatsSubJob{indexpb.StatsSubJob_TextIndexJob, indexpb.StatsSubJob_JsonKeyIndexJob}
	for budget := 0; budget < si.discoveryOptions.scanBatchSize && len(*cursors) > 0; budget++ {
		if si.ctx.Err() != nil {
			return
		}
		*round %= len(*cursors)
		cursor := (*cursors)[*round]
		if !cursor.hasSegment {
			collectionID, segmentID, ok := cursor.next()
			if !ok {
				cursor.stop()
				q.finishScan(cursor.collectionID, cursor.generation)
				metrics.StatsDiscoveryScanDuration.Observe(time.Since(cursor.started).Seconds())
				copy((*cursors)[*round:], (*cursors)[*round+1:])
				(*cursors)[len(*cursors)-1] = nil
				*cursors = (*cursors)[:len(*cursors)-1]
				continue
			}
			metrics.StatsDiscoveryScannedSegments.Inc()
			if segmentID == 0 {
				*round++
				continue
			}
			cursor.segmentCollection, cursor.segmentID = collectionID, segmentID
			cursor.subjob, cursor.hasSegment = 0, true
		}
		for cursor.subjob < len(jobs) {
			key := statsReconcileKey{cursor.segmentID, jobs[cursor.subjob]}
			if !q.enqueue(cursor.segmentCollection, key, time.Now(), false) {
				break
			}
			cursor.subjob++
		}
		cursor.hasSegment = cursor.subjob != len(jobs)
		*round++
	}
}

func (si *statsInspector) statsDiscoveryLoop() {
	defer si.loopWg.Done()
	opts, q := si.discoveryOptions, si.discovery
	scanTicker := time.NewTicker(opts.scanInterval)
	reconcileTicker := time.NewTicker(opts.reconcileInterval)
	metricsTicker := time.NewTicker(time.Second)
	timer := time.NewTimer(0)
	defer scanTicker.Stop()
	defer reconcileTicker.Stop()
	defer metricsTicker.Stop()
	defer timer.Stop()
	var cursors []*statsScanCursor
	defer func() {
		for _, cursor := range cursors {
			cursor.stop()
		}
	}()
	round := 0
	q.requestScan(0, false)
	for {
		select {
		case <-si.ctx.Done():
			return
		case <-q.wake:
		case <-timer.C:
		case <-scanTicker.C:
			si.advanceStatsScans(&cursors, &round)
		case <-reconcileTicker.C:
			q.requestScan(0, false)
		case <-metricsTicker.C:
			q.updateMetrics()
		}
		if si.ctx.Err() != nil {
			return
		}
		si.processStatsDiscoveryBatch()
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(q.delay(time.Now()))
	}
}

// A soft time slice bounds local processing between select iterations. A single
// metadata/resource RPC can exceed it, but remains context-cancellable.
func (si *statsInspector) processStatsDiscoveryBatch() {
	opts, q := si.discoveryOptions, si.discovery
	rules := make(map[int64]statsFieldRules)
	start := time.Now()
	for n := 0; n < 64 && si.ctx.Err() == nil && time.Since(start) < 10*time.Millisecond; n++ {
		work, ok := q.pop(time.Now())
		if !ok {
			break
		}
		result, err := si.reconcileStats(work.key, rules)
		if err != nil {
			mlog.RatedWarn(si.ctx, rate.Limit(1), "stats discovery deferred after failure",
				mlog.FieldSegmentID(work.key.segmentID), mlog.Err(err))
		}
		metrics.StatsDiscoveryChecks.WithLabelValues(string(result), opts.mode, work.key.subjob.String()).Inc()
		if result == statsSubmitted {
			metrics.StatsDiscoveryDelay.Observe(time.Since(work.firstDirty).Seconds())
		}
		q.complete(work, result == statsDeferred || err != nil, time.Now(), opts.retryInterval, opts.retryMaxInterval)
	}
}

func (si *statsInspector) watchStatsDiscoveryConfig() {
	q := si.discovery
	for _, item := range []*paramtable.ParamItem{
		&Params.CommonCfg.EnabledJSONKeyStats, &Params.DataCoordCfg.JSONStatsTriggerCount,
		&Params.CommonCfg.DNFileResourceMode,
	} {
		keys := append([]string{item.Key}, item.FallbackKeys...)
		for _, key := range keys {
			handler := config.NewHandler(fmt.Sprintf("stats-discovery-%p", q), func(*config.Event) { q.requestScan(0, true) })
			Params.Watch(key, handler)
			si.discoveryUnwatch = append(si.discoveryUnwatch, func() { Params.Unwatch(key, handler) })
		}
	}
}

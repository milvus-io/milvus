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

package delegator

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

var errGrowingSourceProviderClosed = errors.New("growing source provider is closed")

type delegatorGrowingSourceProvider struct {
	segmentManager  segments.SegmentManager
	waitFence       func(context.Context, uint64) error
	currentTSafe    func() uint64
	mu              sync.Mutex
	cond            *sync.Cond
	closing         bool
	deactivated     bool
	registration    *syncmgr.GrowingSourceRegistration
	active          int
	retained        map[int64]*retainedGrowingFlushSource
	releaseAllowed  map[int64]uint64
	releasePrepared map[int64]int64
	handoffOnly     bool
	handoffAllowed  map[int64]struct{}
}

func newDelegatorGrowingSourceProvider(segmentManager segments.SegmentManager, waitFence func(context.Context, uint64) error, currentTSafe ...func() uint64) *delegatorGrowingSourceProvider {
	provider := &delegatorGrowingSourceProvider{
		segmentManager:  segmentManager,
		waitFence:       waitFence,
		retained:        make(map[int64]*retainedGrowingFlushSource),
		releaseAllowed:  make(map[int64]uint64),
		releasePrepared: make(map[int64]int64),
		handoffAllowed:  make(map[int64]struct{}),
	}
	if len(currentTSafe) > 0 {
		provider.currentTSafe = currentTSafe[0]
	}
	provider.cond = sync.NewCond(&provider.mu)
	return provider
}

func (p *delegatorGrowingSourceProvider) SetRegistration(registration *syncmgr.GrowingSourceRegistration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.registration = registration
}

func (p *delegatorGrowingSourceProvider) GetGrowingFlushSource(segmentID int64, targetOffset int64, endPos *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
	if !p.acquireLease(segmentID) {
		return nil, syncmgr.GrowingSourceUnavailable
	}
	segment := p.segmentManager.GetGrowing(segmentID)
	retained := false
	if segment == nil {
		var ok bool
		segment, ok = p.getRetained(segmentID)
		if !ok {
			p.releaseLease()
			if p.activeProviderBehind(endPos) {
				return nil, syncmgr.GrowingSourcePending
			}
			return nil, syncmgr.GrowingSourceUnavailable
		}
		retained = true
	} else if p.isDeactivated() {
		p.releaseLease()
		return nil, syncmgr.GrowingSourceUnavailable
	}
	if err := segment.PinIfNotReleased(); err != nil {
		p.releaseLease()
		return nil, syncmgr.GrowingSourceUnavailable
	}
	source := &delegatorGrowingFlushSource{segmentID: segmentID, segment: segment, provider: p, targetOffset: targetOffset, retained: retained}
	if p.currentOffset(segment) < targetOffset {
		return source, syncmgr.GrowingSourcePending
	}
	return source, syncmgr.GrowingSourceUsable
}

func (p *delegatorGrowingSourceProvider) activeProviderBehind(endPos *msgpb.MsgPosition) bool {
	if endPos == nil || endPos.GetTimestamp() == 0 || p.currentTSafe == nil {
		return false
	}
	p.mu.Lock()
	closing := p.closing
	deactivated := p.deactivated
	p.mu.Unlock()
	return !closing && !deactivated && p.currentTSafe() < endPos.GetTimestamp()
}

func (p *delegatorGrowingSourceProvider) PrepareGrowingSourceReleaseHandoff(ctx context.Context, fenceTs uint64, segments []syncmgr.GrowingSourceReleaseHandoffSegment) error {
	if p.isDeactivated() {
		return p.prepareDeactivatedGrowingSourceReleaseHandoff(fenceTs, segments)
	}
	handoffSnapshot := p.enterHandoffOnly(segments)
	if p.waitFence != nil && fenceTs > 0 {
		if err := p.waitFence(ctx, fenceTs); err != nil {
			p.rollbackHandoffOnly(handoffSnapshot)
			return err
		}
	}
	snapshot := p.snapshotRetained(segments)
	allowedSegments := make([]syncmgr.GrowingSourceReleaseHandoffSegment, 0, len(segments))
	preparedSegments := make([]syncmgr.GrowingSourceReleaseHandoffSegment, 0, len(segments))
	for _, segment := range segments {
		allowedSegments = append(allowedSegments, segment)
		if segment.TargetOffset <= 0 {
			continue
		}
		if err := p.registerRetained(segment.SegmentID, segment.TargetOffset); err != nil {
			if errors.Is(err, merr.ErrSegmentNotFound) {
				continue
			}
			p.rollbackRetained(snapshot)
			p.rollbackHandoffOnly(handoffSnapshot)
			return err
		}
		preparedSegments = append(preparedSegments, segment)
	}
	p.markReleaseAllowed(fenceTs, allowedSegments)
	p.markReleasePrepared(fenceTs, preparedSegments)
	return nil
}

func (p *delegatorGrowingSourceProvider) prepareDeactivatedGrowingSourceReleaseHandoff(fenceTs uint64, segments []syncmgr.GrowingSourceReleaseHandoffSegment) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, segment := range segments {
		retained, ok := p.retained[segment.SegmentID]
		if !ok {
			continue
		}
		if segment.TargetOffset > retained.targetOffset {
			continue
		}
		if current, ok := p.releaseAllowed[segment.SegmentID]; !ok || current < fenceTs {
			p.releaseAllowed[segment.SegmentID] = fenceTs
		}
		if segment.TargetOffset <= 0 {
			continue
		}
		if current, ok := p.releasePrepared[segment.SegmentID]; !ok || current < segment.TargetOffset {
			p.releasePrepared[segment.SegmentID] = segment.TargetOffset
		}
	}
	return nil
}

func (p *delegatorGrowingSourceProvider) registerRetained(segmentID int64, targetOffset int64) error {
	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		return errGrowingSourceProviderClosed
	}
	if retained, ok := p.retained[segmentID]; ok {
		if retained.targetOffset < targetOffset {
			retained.targetOffset = targetOffset
		}
		p.mu.Unlock()
		return nil
	}
	p.mu.Unlock()

	segment := p.segmentManager.GetGrowing(segmentID)
	if segment == nil {
		return merr.WrapErrSegmentNotFound(segmentID)
	}
	if err := segment.PinIfNotReleased(); err != nil {
		return err
	}
	currentOffset := p.currentOffset(segment)
	if currentOffset < targetOffset {
		segment.Unpin()
		return merr.WrapErrServiceInternalMsg("growing-source segment %d is behind target offset, current=%d target=%d", segmentID, currentOffset, targetOffset)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closing {
		segment.Unpin()
		return errGrowingSourceProviderClosed
	}
	if retained, ok := p.retained[segmentID]; ok {
		if retained.targetOffset < targetOffset {
			retained.targetOffset = targetOffset
		}
		segment.Unpin()
		return nil
	}
	p.retained[segmentID] = &retainedGrowingFlushSource{
		segment:      segment,
		targetOffset: targetOffset,
	}
	return nil
}

type retainedSnapshot struct {
	existed         bool
	source          *retainedGrowingFlushSource
	targetOffset    int64
	committedOffset int64
	detached        bool
}

func (p *delegatorGrowingSourceProvider) snapshotRetained(segments []syncmgr.GrowingSourceReleaseHandoffSegment) map[int64]retainedSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()

	snapshot := make(map[int64]retainedSnapshot, len(segments))
	for _, segment := range segments {
		if _, ok := snapshot[segment.SegmentID]; ok {
			continue
		}
		retained, existed := p.retained[segment.SegmentID]
		entry := retainedSnapshot{existed: existed, source: retained}
		if existed {
			entry.targetOffset = retained.targetOffset
			entry.committedOffset = retained.committedOffset
			entry.detached = retained.detached
		}
		snapshot[segment.SegmentID] = entry
	}
	return snapshot
}

func (p *delegatorGrowingSourceProvider) rollbackRetained(snapshot map[int64]retainedSnapshot) {
	var toUnpin []segments.Segment
	p.mu.Lock()
	for segmentID, entry := range snapshot {
		current, exists := p.retained[segmentID]
		if entry.existed {
			p.retained[segmentID] = entry.source
			entry.source.targetOffset = entry.targetOffset
			entry.source.committedOffset = entry.committedOffset
			entry.source.detached = entry.detached
			if exists && current != entry.source {
				toUnpin = append(toUnpin, current.segment)
			}
			continue
		}
		if exists {
			delete(p.retained, segmentID)
			toUnpin = append(toUnpin, current.segment)
		}
	}
	p.mu.Unlock()
	for _, segment := range toUnpin {
		segment.Unpin()
	}
}

type handoffSnapshot struct {
	enabled bool
	allowed map[int64]struct{}
}

func (p *delegatorGrowingSourceProvider) enterHandoffOnly(segments []syncmgr.GrowingSourceReleaseHandoffSegment) handoffSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()

	snapshot := handoffSnapshot{
		enabled: p.handoffOnly,
		allowed: make(map[int64]struct{}, len(p.handoffAllowed)),
	}
	for segmentID := range p.handoffAllowed {
		snapshot.allowed[segmentID] = struct{}{}
	}

	p.handoffOnly = true
	for _, segment := range segments {
		p.handoffAllowed[segment.SegmentID] = struct{}{}
	}
	return snapshot
}

func (p *delegatorGrowingSourceProvider) rollbackHandoffOnly(snapshot handoffSnapshot) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.handoffOnly = snapshot.enabled
	p.handoffAllowed = snapshot.allowed
}

func (p *delegatorGrowingSourceProvider) markReleaseAllowed(fenceTs uint64, segments []syncmgr.GrowingSourceReleaseHandoffSegment) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, segment := range segments {
		if current, ok := p.releaseAllowed[segment.SegmentID]; !ok || current < fenceTs {
			p.releaseAllowed[segment.SegmentID] = fenceTs
		}
	}
}

func (p *delegatorGrowingSourceProvider) markReleasePrepared(fenceTs uint64, segments []syncmgr.GrowingSourceReleaseHandoffSegment) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, segment := range segments {
		if current, ok := p.releaseAllowed[segment.SegmentID]; !ok || current < fenceTs {
			p.releaseAllowed[segment.SegmentID] = fenceTs
		}
		if current, ok := p.releasePrepared[segment.SegmentID]; !ok || current < segment.TargetOffset {
			p.releasePrepared[segment.SegmentID] = segment.TargetOffset
		}
	}
}

func (p *delegatorGrowingSourceProvider) IsReleaseAllowed(segmentID int64, checkpointTs uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.isReleaseAllowedLocked(segmentID, checkpointTs)
}

func (p *delegatorGrowingSourceProvider) IsReleasePrepared(segmentID int64, checkpointTs uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.releasePrepared[segmentID]; ok {
		return p.isReleaseAllowedLocked(segmentID, checkpointTs)
	}
	return false
}

func (p *delegatorGrowingSourceProvider) isReleaseAllowedLocked(segmentID int64, checkpointTs uint64) bool {
	fenceTs, ok := p.releaseAllowed[segmentID]
	if !ok {
		return false
	}
	return fenceTs == 0 || checkpointTs == 0 || checkpointTs <= fenceTs
}

func (p *delegatorGrowingSourceProvider) ClearReleasePrepared(segmentID int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.releasePrepared, segmentID)
	delete(p.releaseAllowed, segmentID)
	delete(p.handoffAllowed, segmentID)
}

func (p *delegatorGrowingSourceProvider) ReleasePreparedSegments() []int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	segments := make([]int64, 0, len(p.releasePrepared))
	for segmentID := range p.releasePrepared {
		segments = append(segments, segmentID)
	}
	return segments
}

func (p *delegatorGrowingSourceProvider) MarkReleaseDetached(segmentID int64) {
	p.mu.Lock()
	retained, ok := p.retained[segmentID]
	if !ok {
		p.mu.Unlock()
		return
	}
	retained.detached = true
	registration, released := p.tryReleaseRetainedLocked(segmentID, retained)
	p.mu.Unlock()
	p.releaseRetained(registration, released)
}

func (p *delegatorGrowingSourceProvider) getRetained(segmentID int64) (segments.Segment, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	retained, ok := p.retained[segmentID]
	if !ok {
		return nil, false
	}
	return retained.segment, true
}

func (p *delegatorGrowingSourceProvider) releaseRetainedIfComplete(segmentID int64, targetOffset int64) {
	p.mu.Lock()
	retained, ok := p.retained[segmentID]
	if !ok {
		p.mu.Unlock()
		return
	}
	if retained.committedOffset < targetOffset {
		retained.committedOffset = targetOffset
	}
	registration, released := p.tryReleaseRetainedLocked(segmentID, retained)
	p.mu.Unlock()
	p.releaseRetained(registration, released)
}

func (p *delegatorGrowingSourceProvider) tryReleaseRetainedLocked(segmentID int64, retained *retainedGrowingFlushSource) (*syncmgr.GrowingSourceRegistration, *retainedGrowingFlushSource) {
	if retained == nil ||
		!retained.detached ||
		retained.committedOffset < retained.targetOffset {
		return nil, nil
	}
	delete(p.retained, segmentID)
	return p.unregisterIfInactiveLocked(), retained
}

func (p *delegatorGrowingSourceProvider) releaseRetained(registration *syncmgr.GrowingSourceRegistration, retained *retainedGrowingFlushSource) {
	if retained == nil {
		syncmgr.DefaultGrowingSourceRegistry().Unregister(registration)
		return
	}
	// Drop the retained pin first so Release can drain, then route through the
	// segment manager's managed release path. The segment was already removed
	// from the active maps by Detach, so release() will reconcile the
	// on-releasing set, the segment gauge and the release callback. Calling
	// segment.Release() directly here would leak the on-releasing set entry and
	// keep Exist() reporting the segment forever.
	retained.segment.Unpin()
	p.segmentManager.ReleaseDetached(context.Background(), retained.segment)
	syncmgr.DefaultGrowingSourceRegistry().Unregister(registration)
}

func (p *delegatorGrowingSourceProvider) acquireLease(segmentID int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closing {
		return false
	}
	if p.handoffOnly {
		_, allowed := p.handoffAllowed[segmentID]
		_, retained := p.retained[segmentID]
		if !allowed && !retained {
			return false
		}
	}
	p.active++
	return true
}

func (p *delegatorGrowingSourceProvider) releaseLease() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.active > 0 {
		p.active--
	}
	if p.closing && p.active == 0 {
		p.cond.Broadcast()
	}
}

func (p *delegatorGrowingSourceProvider) isDeactivated() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.deactivated
}

func (p *delegatorGrowingSourceProvider) Deactivate() {
	p.mu.Lock()
	p.deactivated = true
	registration := p.unregisterIfInactiveLocked()
	p.mu.Unlock()
	syncmgr.DefaultGrowingSourceRegistry().Unregister(registration)
}

func (p *delegatorGrowingSourceProvider) Close() {
	p.mu.Lock()
	p.closing = true
	for p.active > 0 {
		p.cond.Wait()
	}
	retained := p.retained
	p.retained = make(map[int64]*retainedGrowingFlushSource)
	p.releaseAllowed = make(map[int64]uint64)
	p.releasePrepared = make(map[int64]int64)
	p.handoffOnly = false
	p.handoffAllowed = make(map[int64]struct{})
	registration := p.registration
	p.registration = nil
	p.mu.Unlock()
	for _, source := range retained {
		source.segment.Unpin()
	}
	syncmgr.DefaultGrowingSourceRegistry().Unregister(registration)
}

func (p *delegatorGrowingSourceProvider) unregisterIfInactiveLocked() *syncmgr.GrowingSourceRegistration {
	if !p.deactivated || len(p.retained) > 0 {
		return nil
	}
	registration := p.registration
	p.registration = nil
	p.releaseAllowed = make(map[int64]uint64)
	p.releasePrepared = make(map[int64]int64)
	p.handoffOnly = false
	p.handoffAllowed = make(map[int64]struct{})
	return registration
}

func (p *delegatorGrowingSourceProvider) currentOffset(segment segments.Segment) int64 {
	if segment == nil {
		return 0
	}
	return segment.InsertCount()
}

type retainedGrowingFlushSource struct {
	segment         segments.Segment
	targetOffset    int64
	committedOffset int64
	detached        bool
}

type delegatorGrowingFlushSource struct {
	segmentID    int64
	segment      segments.Segment
	provider     *delegatorGrowingSourceProvider
	targetOffset int64
	retained     bool
	once         sync.Once
}

func (s *delegatorGrowingFlushSource) CurrentOffset() int64 {
	if s.provider != nil {
		return s.provider.currentOffset(s.segment)
	}
	if s.segment == nil {
		return 0
	}
	return s.segment.InsertCount()
}

func (s *delegatorGrowingFlushSource) FlushGrowingData(ctx context.Context, startOffset, endOffset int64, config *syncmgr.GrowingFlushConfig) (*syncmgr.GrowingFlushResult, error) {
	result, err := s.segment.FlushData(ctx, startOffset, endOffset, &segments.FlushConfig{
		SegmentBasePath:         config.SegmentBasePath,
		PartitionBasePath:       config.PartitionBasePath,
		CollectionID:            config.CollectionID,
		PartitionID:             config.PartitionID,
		Schema:                  config.Schema,
		TextFieldIDs:            config.TextFieldIDs,
		TextLobPaths:            config.TextLobPaths,
		TextInlineThreshold:     config.TextInlineThreshold,
		TextMaxLobFileBytes:     config.TextMaxLobFileBytes,
		TextFlushThresholdBytes: config.TextFlushThresholdBytes,
		BM25FieldIDs:            config.BM25FieldIDs,
		BM25StatsLogIDs:         config.BM25StatsLogIDs,
		WriteMergedBM25Stats:    config.WriteMergedBM25Stats,
		ReadVersion:             config.ReadVersion,
		WriterFormat:            config.WriterFormat,
		SchemaBasedPattern:      config.SchemaBasedPattern,
		SchemaBasedFormats:      config.SchemaBasedFormats,
		AllowedFieldIDs:         config.AllowedFieldIDs,
		ColumnGroups:            config.ColumnGroups,
	})
	if err != nil || result == nil {
		return nil, err
	}
	return &syncmgr.GrowingFlushResult{
		ManifestPath:           result.ManifestPath,
		NumRows:                result.NumRows,
		TimestampFrom:          result.TimestampFrom,
		TimestampTo:            result.TimestampTo,
		FlushedFieldIDs:        result.FlushedFieldIDs,
		ColumnGroupMemorySizes: result.ColumnGroupMemorySizes,
		FieldNullCounts:        result.FieldNullCounts,
		BM25Stats:              result.BM25Stats,
	}, nil
}

// materializedFieldIDsProvider is the capability a source segment must expose
// for the flush layout to be trimmed to its materialized columns.
type materializedFieldIDsProvider interface {
	MaterializedFieldIDs(ctx context.Context) ([]int64, error)
}

func (s *delegatorGrowingFlushSource) MaterializedFieldIDs(ctx context.Context) ([]int64, error) {
	provider, ok := s.segment.(materializedFieldIDsProvider)
	if !ok {
		return nil, merr.WrapErrServiceInternalMsg("growing flush source segment does not expose materialized field ids")
	}
	return provider.MaterializedFieldIDs(ctx)
}

func (s *delegatorGrowingFlushSource) Release() {
	s.once.Do(func() {
		if s.segment != nil {
			s.segment.Unpin()
		}
		if s.provider != nil {
			s.provider.releaseLease()
		}
	})
}

func (s *delegatorGrowingFlushSource) CommitGrowingFlush(targetOffset int64) {
	if s.retained && s.provider != nil {
		s.provider.releaseRetainedIfComplete(s.segmentID, targetOffset)
	}
}

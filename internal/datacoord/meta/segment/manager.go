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

package segment

import (
	"context"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ SegmentManager = (*segmentManager)(nil)

// segmentManager is the implementation of SegmentManager.
type segmentManager struct {
	catalog metastore.DataCoordCatalog

	mut              lock.RWMutex
	segments         map[typeutil.UniqueID]*SegmentInfo
	secondaryIndexes segmentInfoIndexes
	// map[compaction_from] => compact_to
	compactionTo map[typeutil.UniqueID]typeutil.UniqueID // map the compact relation, value is the segment which `CompactFrom` contains key.
	// A segment can be compacted to only one segment finally in meta.
}

type segmentInfoIndexes struct {
	coll2Segments    map[typeutil.UniqueID]map[typeutil.UniqueID]*SegmentInfo
	channel2Segments map[string]map[typeutil.UniqueID]*SegmentInfo
}

// NewSegmentsInfo creates a `SegmentsInfo` instance, which makes sure internal map is initialized
// note that no mutex is wrapped so external concurrent control is needed
func NewSegmentManager(ctx context.Context, catalog metastore.DataCoordCatalog) (*segmentManager, error) {
	s := &segmentManager{
		segments: make(map[typeutil.UniqueID]*SegmentInfo),
		secondaryIndexes: segmentInfoIndexes{
			coll2Segments:    make(map[typeutil.UniqueID]map[typeutil.UniqueID]*SegmentInfo),
			channel2Segments: make(map[string]map[typeutil.UniqueID]*SegmentInfo),
		},
		compactionTo: make(map[typeutil.UniqueID]typeutil.UniqueID),
	}

	err := s.reloadFromKV(ctx)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *segmentManager) reloadFromKV(ctx context.Context) error {
	segments, err := s.catalog.ListSegments(ctx)
	if err != nil {
		return err
	}
	metrics.DataCoordNumCollections.WithLabelValues().Set(0)
	metrics.DataCoordNumSegments.Reset()
	numStoredRows := int64(0)
	for _, segment := range segments {
		// segments from catalog.ListSegments will not have logPath
		s.setSegment(segment.ID, NewSegmentInfo(segment))
		metrics.DataCoordNumSegments.WithLabelValues(segment.GetState().String(), segment.GetLevel().String()).Inc()
		if segment.State == commonpb.SegmentState_Flushed {
			numStoredRows += segment.NumOfRows

			insertFileNum := 0
			for _, fieldBinlog := range segment.GetBinlogs() {
				insertFileNum += len(fieldBinlog.GetBinlogs())
			}
			metrics.FlushedSegmentFileNum.WithLabelValues(metrics.InsertFileLabel).Observe(float64(insertFileNum))

			statFileNum := 0
			for _, fieldBinlog := range segment.GetStatslogs() {
				statFileNum += len(fieldBinlog.GetBinlogs())
			}
			metrics.FlushedSegmentFileNum.WithLabelValues(metrics.StatFileLabel).Observe(float64(statFileNum))

			deleteFileNum := 0
			for _, filedBinlog := range segment.GetDeltalogs() {
				deleteFileNum += len(filedBinlog.GetBinlogs())
			}
			metrics.FlushedSegmentFileNum.WithLabelValues(metrics.DeleteFileLabel).Observe(float64(deleteFileNum))
		}
	}
	return nil
}

// AddSegment records segment info, persisting info into kv store
func (s *segmentManager) AddSegment(ctx context.Context, segment *SegmentInfo) error {
	log := log.Ctx(ctx)
	log.Info("meta update: adding segment - Start", zap.Int64("segmentID", segment.GetID()))
	s.mut.Lock()
	defer s.mut.Unlock()
	if err := s.catalog.AddSegment(ctx, segment.SegmentInfo); err != nil {
		log.Error("meta update: adding segment failed",
			zap.Int64("segmentID", segment.GetID()),
			zap.Error(err))
		return err
	}
	s.setSegment(segment.GetID(), segment)

	metrics.DataCoordNumSegments.WithLabelValues(segment.GetState().String(), segment.GetLevel().String()).Inc()
	log.Info("meta update: adding segment - complete", zap.Int64("segmentID", segment.GetID()))
	return nil
}

// GetSegment returns SegmentInfo
// the logPath in meta is empty
func (s *segmentManager) GetSegmentByID(segmentID typeutil.UniqueID, filters ...SegmentFilter) *SegmentInfo {
	s.mut.RLock()
	defer s.mut.RUnlock()
	return s.getSegmentByID(segmentID, filters...)
}

func (s *segmentManager) GetSegments(filters ...SegmentFilter) []*SegmentInfo {
	s.mut.RLock()
	defer s.mut.RUnlock()
	return s.GetSegments(filters...)
}

func (s *segmentManager) getSegments(filters ...SegmentFilter) []*SegmentInfo {
	criterion := &segmentCriterion{}
	for _, filter := range filters {
		filter.AddFilter(criterion)
	}

	// apply criterion
	candidates := s.getCandidates(criterion)
	var result []*SegmentInfo
	for _, segment := range candidates {
		if criterion.Match(segment) {
			result = append(result, segment)
		}
	}
	return result
}

// UpdateSegment perform single segment update operations.
// it shall update catalog first(if needed), then update the memory state.
func (s *segmentManager) UpdateSegment(ctx context.Context, segmentID int64, operators ...SegmentOperator) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.updateSegment(ctx, segmentID, operators...)
}

func (s *segmentManager) updateSegment(ctx context.Context, segmentID int64, operators ...SegmentOperator) error {
	info := s.getSegmentByID(segmentID)
	if info == nil {
		log.Warn("meta update: UpdateSegment - segment not found",
			zap.Int64("segmentID", segmentID))

		return merr.WrapErrSegmentNotFound(segmentID)
	}

	needDeepClone := false
	for _, operator := range operators {
		if operator.IsMetaUpdate() {
			needDeepClone = true
			break
		}
	}

	// Persist segment updates first.
	var cloned *SegmentInfo
	if needDeepClone {
		cloned = info.Clone()
	} else {
		cloned = info.ShadowClone()
	}

	var updated bool
	for _, operator := range operators {
		updated = updated || operator.UpdateSegment(cloned)
	}

	if !updated {
		log.Warn("meta update:UpdateSegmnt skipped, no update",
			zap.Int64("segmentID", segmentID),
		)
		return nil
	}

	if err := s.catalog.AlterSegments(ctx, []*datapb.SegmentInfo{cloned.SegmentInfo}); err != nil {
		log.Warn("meta update: update segment - failed to alter segments",
			zap.Int64("segmentID", segmentID),
			zap.Error(err))
		return err
	}
	// Update in-memory meta.
	s.setSegment(segmentID, cloned)

	log.Info("meta update: update segment - complete",
		zap.Int64("segmentID", segmentID))
	return nil
}

// UpdateSegmentsInfo update segment infos
// will exec all operators, and update all changed segments
func (s *segmentManager) UpdateSegmentsInfo(ctx context.Context, operators ...UpdateOperator) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	updatePack := &updateSegmentPack{
		meta:       s,
		segments:   make(map[int64]*SegmentInfo),
		increments: make(map[int64]metastore.BinlogsIncrement),
		metricMutation: &segMetricMutation{
			stateChange: make(map[string]map[string]int),
		},
	}

	for _, operator := range operators {
		ok := operator(updatePack)
		if !ok {
			return nil
		}
	}

	segments := lo.MapToSlice(updatePack.segments, func(_ int64, segment *SegmentInfo) *datapb.SegmentInfo { return segment.SegmentInfo })
	increments := lo.Values(updatePack.increments)

	if err := s.catalog.AlterSegments(ctx, segments, increments...); err != nil {
		log.Error("meta update: update flush segments info - failed to store flush segment info into Etcd",
			zap.Error(err))
		return err
	}
	// Apply metric mutation after a successful meta update.
	updatePack.metricMutation.commit()
	// update memory status
	for id, seg := range updatePack.segments {
		s.setSegment(id, seg)
	}
	log.Info("meta update: update flush segments info - update flush segments info successfully")
	return nil
}

func (s *segmentManager) getSegmentByID(segmentID typeutil.UniqueID, filters ...SegmentFilter) *SegmentInfo {
	segment, ok := s.segments[segmentID]
	if !ok {
		return nil
	}
	for _, filter := range filters {
		if !filter.Match(segment) {
			return nil
		}
	}
	return segment
}

func (s *segmentManager) UpdateSegmentsIf(ctx context.Context, condition Condition, actions ...SegmentOperator) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	// check condition first
	if !condition.satisfy(s) {
		return merr.WrapErrServiceInternal("condition not satified")
	}

	segments := condition.targets()

	for _, segmentID := range segments {
		// use in-memory (shallow-clone) action only for now.
		// otherwise, a method providing batch update shall be added.
		if err := s.updateSegment(ctx, segmentID, actions...); err != nil {
			return err
		}
	}

	return nil
}

// // SetSegment sets SegmentInfo with segmentID, perform overwrite if already exists
// // set the logPath of segment in meta empty, to save space
// // if segment has logPath, make it empty
func (s *segmentManager) setSegment(segmentID typeutil.UniqueID, segment *SegmentInfo) {
	if segment, ok := s.segments[segmentID]; ok {
		// Remove old segment compact to relation first.
		s.deleteCompactTo(segment)
		s.removeSecondaryIndex(segment)
	}
	s.segments[segmentID] = segment
	s.addSecondaryIndex(segment)
	s.addCompactTo(segment)
}

// DropSegment deletes provided segmentID
// no extra method is taken when segmentID not exists
func (s *segmentManager) DropSegment(ctx context.Context, segmentID typeutil.UniqueID) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	// check whether segment exists
	segment, ok := s.segments[segmentID]
	if !ok {
		// segment not found, print warning message and return nil
		log.Warn("meta update: dropping segment failed - segment not found",
			zap.Int64("segmentID", segmentID))
		return nil
	}

	if err := s.catalog.DropSegment(ctx, segment.SegmentInfo); err != nil {
		log.Warn("meta update: dropping segment failed",
			zap.Int64("segmentID", segmentID),
			zap.Error(err))
		return err
	}
	metrics.DataCoordNumSegments.WithLabelValues(segment.GetState().String(), segment.GetLevel().String()).Dec()

	s.deleteCompactTo(segment)
	s.removeSecondaryIndex(segment)
	delete(s.segments, segmentID)
	log.Info("meta update: dropping segment - complete",
		zap.Int64("segmentID", segmentID))
	return nil
}

func (s *segmentManager) getCandidates(criterion *segmentCriterion) map[typeutil.UniqueID]*SegmentInfo {
	// segment ids specified
	if criterion.segmentIDs != nil {
		return s.getCandidatesByIDs(criterion)
	}

	// use collection index
	if criterion.collectionID > 0 {
		return s.getCandidatesByCollection(criterion)
	}

	// use channel secondary index
	if criterion.channel != "" {
		return s.getCandidatesByChannel(criterion)
	}

	// scan all segments
	return s.segments
}

func (s *segmentManager) getCandidatesByIDs(criterion *segmentCriterion) map[typeutil.UniqueID]*SegmentInfo {
	segments := lo.FilterMap(criterion.segmentIDs.Collect(), func(segID int64, _ int) (*SegmentInfo, bool) {
		seg, ok := s.segments[segID]
		return seg, ok
	})

	// add collection id filter to filters
	if criterion.collectionID > 0 {
		criterion.others = append(criterion.others, WithSegmentFilterFunc(func(si *SegmentInfo) bool {
			return si.GetCollectionID() == criterion.collectionID
		}))
	}

	// add channel filter to filters
	if criterion.channel != "" {
		criterion.others = append(criterion.others, WithSegmentFilterFunc(func(si *SegmentInfo) bool {
			return si.GetInsertChannel() == criterion.channel
		}))
	}

	return lo.SliceToMap(segments, func(segment *SegmentInfo) (int64, *SegmentInfo) { return segment.GetID(), segment })
}

func (s *segmentManager) getCandidatesByCollection(criterion *segmentCriterion) map[typeutil.UniqueID]*SegmentInfo {
	collSegments, ok := s.secondaryIndexes.coll2Segments[criterion.collectionID]
	if !ok {
		return nil
	}

	// both collection id and channel are filters of criterion
	if criterion.channel != "" {
		return lo.OmitBy(collSegments, func(k typeutil.UniqueID, v *SegmentInfo) bool {
			return v.InsertChannel != criterion.channel
		})
	}
	return collSegments
}

func (s *segmentManager) getCandidatesByChannel(criterion *segmentCriterion) map[typeutil.UniqueID]*SegmentInfo {
	channelSegments, ok := s.secondaryIndexes.channel2Segments[criterion.channel]
	if !ok {
		return nil
	}
	return channelSegments
}

func (s *segmentManager) addSecondaryIndex(segment *SegmentInfo) {
	collID := segment.GetCollectionID()
	channel := segment.GetInsertChannel()
	if _, ok := s.secondaryIndexes.coll2Segments[collID]; !ok {
		s.secondaryIndexes.coll2Segments[collID] = make(map[typeutil.UniqueID]*SegmentInfo)
	}
	s.secondaryIndexes.coll2Segments[collID][segment.ID] = segment

	if _, ok := s.secondaryIndexes.channel2Segments[channel]; !ok {
		s.secondaryIndexes.channel2Segments[channel] = make(map[typeutil.UniqueID]*SegmentInfo)
	}
	s.secondaryIndexes.channel2Segments[channel][segment.ID] = segment
}

func (s *segmentManager) removeSecondaryIndex(segment *SegmentInfo) {
	collID := segment.GetCollectionID()
	channel := segment.GetInsertChannel()
	if segments, ok := s.secondaryIndexes.coll2Segments[collID]; ok {
		delete(segments, segment.ID)
		if len(segments) == 0 {
			delete(s.secondaryIndexes.coll2Segments, collID)
		}
	}

	if segments, ok := s.secondaryIndexes.channel2Segments[channel]; ok {
		delete(segments, segment.ID)
		if len(segments) == 0 {
			delete(s.secondaryIndexes.channel2Segments, channel)
		}
	}
}

// addCompactTo adds the compact relation to the segment
func (s *segmentManager) addCompactTo(segment *SegmentInfo) {
	for _, from := range segment.GetCompactionFrom() {
		s.compactionTo[from] = segment.GetID()
	}
}

// deleteCompactTo deletes the compact relation to the segment
func (s *segmentManager) deleteCompactTo(segment *SegmentInfo) {
	for _, from := range segment.GetCompactionFrom() {
		delete(s.compactionTo, from)
	}
}

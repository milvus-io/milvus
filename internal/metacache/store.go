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

// Package metacache provides a centralized, in-memory store for segment and
// collection metadata that is shared across coordinators when running in
// MixCoord mode. It exposes a read-only MetaView (consumed by QueryCoord)
// and a read-write MetaStore (consumed by DataCoord) backed by the same
// underlying data, avoiding metadata duplication between coordinators.
package metacache

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// CollectionInfo holds the collection-level metadata tracked by the
// metacache.
type CollectionInfo struct {
	ID             int64
	Schema         *schemapb.CollectionSchema
	Partitions     []int64
	VChannelNames  []string
	Properties     map[string]string
	DatabaseName   string
	DatabaseID     int64
	StartPositions []*commonpb.KeyDataPair
	CreatedAt      uint64
}

func (c *CollectionInfo) IsExternal() bool {
	if c == nil || c.Schema == nil {
		return false
	}
	return typeutil.IsExternalCollection(c.Schema)
}

// MetaView is the read-only view over the metacache, used by components
// that only need to observe metadata (e.g. QueryCoord).
type MetaView interface {
	// Segments — read
	GetSegment(segmentID int64) (*datapb.SegmentInfo, bool)
	GetSegmentsByIDs(segmentIDs []int64) map[int64]*datapb.SegmentInfo
	GetAllSegments() map[int64]*datapb.SegmentInfo
	GetSegments(collectionID int64) map[int64]*datapb.SegmentInfo
	GetSegmentsByChannel(channel string) map[int64]*datapb.SegmentInfo

	// Collections — read
	GetCollection(collectionID int64) (*CollectionInfo, bool)
	GetCollections() []*CollectionInfo
	GetCollectionIDs() []int64
}

// MetaStore is the read-write metacache interface, used by components that
// own metadata mutation (e.g. DataCoord). MetaStore owns the catalog and
// provides all functionality directly — no catalog accessor is exposed.
type MetaStore interface {
	MetaView

	// Segments — write (in-memory only)
	PutSegment(segment *datapb.SegmentInfo)
	RemoveSegment(segmentID int64)
	LoadSegments(segments []*datapb.SegmentInfo)

	// Segments — catalog persistence (also updates in-memory map + logs)

	// Collections — write
	PutCollection(info *CollectionInfo)
	RemoveCollection(collectionID int64)

	// Channel checkpoints (includes timestamp comparison, logging, metrics)
	UpdateChannelCheckpoint(ctx context.Context, vChannel string, pos *msgpb.MsgPosition) error
	UpdateChannelCheckpoints(ctx context.Context, positions []*msgpb.MsgPosition) error
	MarkChannelCheckpointDropped(ctx context.Context, channel string) error
	DropChannelCheckpoint(ctx context.Context, vChannel string) error
	GetChannelCheckpoint(vChannel string) *msgpb.MsgPosition
	GetChannelCheckpoints() map[string]*msgpb.MsgPosition
	LoadChannelCheckpoints(checkpoints map[string]*msgpb.MsgPosition)

	// Channel lifecycle
	MarkChannelAdded(ctx context.Context, channel string) error
	ShouldDropChannel(ctx context.Context, channel string) bool
	ChannelExists(ctx context.Context, channel string) bool
	DropChannel(ctx context.Context, channel string) error

	// GC
	GcConfirm(ctx context.Context, collectionID, partitionID typeutil.UniqueID) bool

	// Loading
	LoadFromCatalog(ctx context.Context, collectionIDs []int64, normalize SegmentNormalizer) error
}

type metaStore struct {
	mu      sync.RWMutex
	catalog metastore.DataCoordCatalog

	// Segment storage + indexes
	segments   map[int64]*datapb.SegmentInfo
	segCollIdx map[int64]map[int64]struct{}
	segChanIdx map[string]map[int64]struct{}

	// Collection storage
	collections map[int64]*CollectionInfo

	// Channel checkpoints
	checkpoints map[string]*msgpb.MsgPosition
}

// NewMetaStore creates an empty, ready-to-use MetaStore that owns the
// given catalog.
func NewMetaStore(catalog metastore.DataCoordCatalog) MetaStore {
	return &metaStore{
		catalog:     catalog,
		segments:    make(map[int64]*datapb.SegmentInfo),
		segCollIdx:  make(map[int64]map[int64]struct{}),
		segChanIdx:  make(map[string]map[int64]struct{}),
		collections: make(map[int64]*CollectionInfo),
		checkpoints: make(map[string]*msgpb.MsgPosition),
	}
}

// InternalCatalog returns the catalog for sub-meta initialization.
// This is NOT part of the MetaStore interface — only available on the
// concrete type. Sub-metas (indexMeta, analyzeMeta, etc.) need direct
// catalog access for their own specialized operations.
func (s *metaStore) InternalCatalog() metastore.DataCoordCatalog {
	return s.catalog
}

// --- Segment read methods ---

func (s *metaStore) GetSegment(segmentID int64) (*datapb.SegmentInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	seg, ok := s.segments[segmentID]
	return seg, ok
}

func (s *metaStore) GetSegmentsByIDs(segmentIDs []int64) map[int64]*datapb.SegmentInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[int64]*datapb.SegmentInfo, len(segmentIDs))
	for _, id := range segmentIDs {
		if seg, ok := s.segments[id]; ok {
			result[id] = seg
		}
	}
	return result
}

func (s *metaStore) GetAllSegments() map[int64]*datapb.SegmentInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[int64]*datapb.SegmentInfo, len(s.segments))
	for id, seg := range s.segments {
		result[id] = seg
	}
	return result
}

func (s *metaStore) GetSegments(collectionID int64) map[int64]*datapb.SegmentInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids, ok := s.segCollIdx[collectionID]
	if !ok {
		return nil
	}
	result := make(map[int64]*datapb.SegmentInfo, len(ids))
	for id := range ids {
		result[id] = s.segments[id]
	}
	return result
}

func (s *metaStore) GetSegmentsByChannel(channel string) map[int64]*datapb.SegmentInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids, ok := s.segChanIdx[channel]
	if !ok {
		return nil
	}
	result := make(map[int64]*datapb.SegmentInfo, len(ids))
	for id := range ids {
		result[id] = s.segments[id]
	}
	return result
}

// --- Segment write methods (in-memory) ---

// PutSegment inserts or replaces a segment and emits the matching
// DataCoordNumSegments change.
func (s *metaStore) PutSegment(segment *datapb.SegmentInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.putSegmentLocked(segment)
}

// RemoveSegment deletes a segment and decrements DataCoordNumSegments.
func (s *metaStore) RemoveSegment(segmentID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.segments[segmentID]
	if !ok {
		return
	}
	s.removeSegmentIndexes(segmentID, old)
	delete(s.segments, segmentID)
	labels := segMetricLabels(old)
	metrics.DataCoordNumSegments.WithLabelValues(labels[:]...).Dec()
}

// putSegmentLocked is the single write path that keeps DataCoordNumSegments
// in sync with the in-memory segments. Caller must hold s.mu.
func (s *metaStore) putSegmentLocked(segment *datapb.SegmentInfo) {
	id := segment.GetID()
	if old, exists := s.segments[id]; exists {
		s.emitSegmentMetricTransition(old, segment)
		s.removeSegmentIndexes(id, old)
	} else {
		labels := segMetricLabels(segment)
		metrics.DataCoordNumSegments.WithLabelValues(labels[:]...).Inc()
	}
	s.segments[id] = segment
	s.addSegmentIndexes(id, segment)
}

func (s *metaStore) LoadSegments(segments []*datapb.SegmentInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, seg := range segments {
		id := seg.GetID()
		if old, exists := s.segments[id]; exists {
			s.removeSegmentIndexes(id, old)
		}
		s.segments[id] = seg
		s.addSegmentIndexes(id, seg)
	}
}

func (s *metaStore) addSegmentIndexes(id int64, seg *datapb.SegmentInfo) {
	collID := seg.GetCollectionID()
	if s.segCollIdx[collID] == nil {
		s.segCollIdx[collID] = make(map[int64]struct{})
	}
	s.segCollIdx[collID][id] = struct{}{}

	ch := seg.GetInsertChannel()
	if s.segChanIdx[ch] == nil {
		s.segChanIdx[ch] = make(map[int64]struct{})
	}
	s.segChanIdx[ch][id] = struct{}{}
}

func (s *metaStore) removeSegmentIndexes(id int64, seg *datapb.SegmentInfo) {
	collID := seg.GetCollectionID()
	if ids, ok := s.segCollIdx[collID]; ok {
		delete(ids, id)
		if len(ids) == 0 {
			delete(s.segCollIdx, collID)
		}
	}

	ch := seg.GetInsertChannel()
	if ids, ok := s.segChanIdx[ch]; ok {
		delete(ids, id)
		if len(ids) == 0 {
			delete(s.segChanIdx, ch)
		}
	}
}

func (s *metaStore) emitSegmentMetricTransition(old, updated *datapb.SegmentInfo) {
	oldLabels := segMetricLabels(old)
	newLabels := segMetricLabels(updated)
	if oldLabels != newLabels {
		metrics.DataCoordNumSegments.WithLabelValues(oldLabels[:]...).Dec()
		metrics.DataCoordNumSegments.WithLabelValues(newLabels[:]...).Inc()
	}
}

func sortLabel(sorted bool) string {
	if sorted {
		return "sorted"
	}
	return "unsorted"
}

const (
	segFormatLegacy  = "legacy"
	segFormatUnknown = "unknown"
	segFormatMixed   = "mixed"
)

func segFormatLabel(seg *datapb.SegmentInfo) string {
	if seg == nil {
		return segFormatUnknown
	}
	// Match DataCoord's SegmentInfo.EnsureStats: derive stats from the
	// binlogs when the segment carries none.
	stats := seg.GetStats()
	if stats == nil {
		stats = storage.BuildStatsFromFieldBinlogs(seg.GetBinlogs(), seg.GetStatslogs(), seg.GetBm25Statslogs(), seg.GetDeltalogs())
	}
	formats := stats.GetFormats()
	if len(formats) == 0 {
		if seg.GetStorageVersion() < storage.StorageV2 {
			return segFormatLegacy
		}
		return segFormatUnknown
	}
	if len(formats) > 1 {
		return segFormatMixed
	}
	return formats[0]
}

func segMetricLabels(seg *datapb.SegmentInfo) [5]string {
	return [5]string{
		seg.GetState().String(),
		seg.GetLevel().String(),
		sortLabel(seg.GetIsSorted()),
		fmt.Sprint(seg.GetStorageVersion()),
		segFormatLabel(seg),
	}
}

// --- Collection read methods ---

func (s *metaStore) GetCollection(collectionID int64) (*CollectionInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	info, ok := s.collections[collectionID]
	return info, ok
}

func (s *metaStore) GetCollections() []*CollectionInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*CollectionInfo, 0, len(s.collections))
	for _, info := range s.collections {
		result = append(result, info)
	}
	return result
}

func (s *metaStore) GetCollectionIDs() []int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]int64, 0, len(s.collections))
	for id := range s.collections {
		result = append(result, id)
	}
	return result
}

// --- Collection write methods ---

func (s *metaStore) PutCollection(info *CollectionInfo) {
	mlog.Info(context.Background(), "metaStore: put collection", mlog.Int64("collectionID", info.ID))
	s.mu.Lock()
	id := info.ID
	s.collections[id] = info
	numCollections := len(s.collections)
	s.mu.Unlock()
	metrics.DataCoordNumCollections.WithLabelValues().Set(float64(numCollections))
}

func (s *metaStore) RemoveCollection(collectionID int64) {
	mlog.Info(context.Background(), "metaStore: remove collection", mlog.Int64("collectionID", collectionID))
	s.mu.Lock()
	if _, ok := s.collections[collectionID]; !ok {
		s.mu.Unlock()
		return
	}
	delete(s.collections, collectionID)
	numCollections := len(s.collections)
	s.mu.Unlock()
	metrics.CleanupDataCoordWithCollectionID(collectionID)
	metrics.DataCoordNumCollections.WithLabelValues().Set(float64(numCollections))
}

// --- Channel checkpoint methods ---

// UpdateChannelCheckpoint saves a channel checkpoint if it is newer than the
// existing one. The timestamp comparison is done under RLock and the catalog
// write happens outside the lock to avoid holding it during I/O. This leaves
// a narrow TOCTOU window where two concurrent updates for the same channel
// could both pass the comparison and write; the last writer wins. In practice
// this is benign — checkpoint regression on a single channel is self-healing
// on the next update — and matches the original DC meta behavior.
// validChannelCheckpoint reports whether a position may be persisted. A
// WoodPecker WAL position carries no msgID, so it is exempt from that check —
// the datanode-side updater applies the same exemption.
func validChannelCheckpoint(pos *msgpb.MsgPosition) bool {
	if pos == nil || pos.GetChannelName() == "" {
		return false
	}
	return pos.GetMsgID() != nil || pos.GetWALName() == commonpb.WALName_WoodPecker
}

// checkpointAdvances reports whether pos moves the channel checkpoint forward.
// A position sharing the previous timestamp but carrying a different msgID
// still advances it: several messages can share one tick.
func checkpointAdvances(old, pos *msgpb.MsgPosition) bool {
	if old == nil {
		return true
	}
	if old.GetTimestamp() != pos.GetTimestamp() {
		return old.GetTimestamp() < pos.GetTimestamp()
	}
	return !bytes.Equal(old.GetMsgID(), pos.GetMsgID())
}

func (s *metaStore) UpdateChannelCheckpoint(ctx context.Context, vChannel string, pos *msgpb.MsgPosition) error {
	// Same validity rule as the batch path, so the two cannot drift.
	if pos == nil || (pos.GetMsgID() == nil && pos.GetWALName() != commonpb.WALName_WoodPecker) {
		return merr.WrapErrServiceInternalMsg("channelCP is nil, vChannel=%s", vChannel)
	}

	s.mu.RLock()
	oldPosition := s.checkpoints[vChannel]
	if !checkpointAdvances(oldPosition, pos) {
		s.mu.RUnlock()
		return nil
	}
	s.mu.RUnlock()

	if err := s.catalog.SaveChannelCheckpoint(ctx, vChannel, pos); err != nil {
		return err
	}
	s.mu.Lock()
	s.checkpoints[vChannel] = pos
	s.mu.Unlock()

	ts, _ := tsoutil.ParseTS(pos.Timestamp)
	mlog.Info(ctx, "UpdateChannelCheckpoint done",
		mlog.String("vChannel", vChannel),
		mlog.Uint64("ts", pos.GetTimestamp()),
		mlog.ByteString("msgID", pos.GetMsgID()),
		mlog.Time("time", ts))
	metrics.DataCoordCheckpointUnixSeconds.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), vChannel).
		Set(float64(ts.Unix()))
	return nil
}

func (s *metaStore) UpdateChannelCheckpoints(ctx context.Context, positions []*msgpb.MsgPosition) error {
	s.mu.RLock()
	var toUpdates []*msgpb.MsgPosition
	for _, pos := range positions {
		if !validChannelCheckpoint(pos) {
			mlog.Warn(ctx, "illegal channel cp", mlog.Any("pos", pos))
			continue
		}
		if checkpointAdvances(s.checkpoints[pos.GetChannelName()], pos) {
			toUpdates = append(toUpdates, pos)
		}
	}
	s.mu.RUnlock()

	if len(toUpdates) == 0 {
		return nil
	}

	if err := s.catalog.SaveChannelCheckpoints(ctx, toUpdates); err != nil {
		return err
	}

	s.mu.Lock()
	for _, pos := range toUpdates {
		s.checkpoints[pos.GetChannelName()] = pos
	}
	s.mu.Unlock()

	nodeID := fmt.Sprint(paramtable.GetNodeID())
	for _, pos := range toUpdates {
		channel := pos.GetChannelName()
		ts, _ := tsoutil.ParseTS(pos.Timestamp)
		mlog.Info(ctx, "UpdateChannelCheckpoint done",
			mlog.String("channel", channel),
			mlog.Uint64("ts", pos.GetTimestamp()),
			mlog.Time("time", ts))
		metrics.DataCoordCheckpointUnixSeconds.WithLabelValues(nodeID, channel).Set(float64(ts.Unix()))
	}
	return nil
}

func (s *metaStore) MarkChannelCheckpointDropped(ctx context.Context, channel string) error {
	cp := &msgpb.MsgPosition{
		ChannelName: channel,
		Timestamp:   funcutil.DroppedChannelCheckpointTimestamp,
	}

	if err := s.catalog.SaveChannelCheckpoints(ctx, []*msgpb.MsgPosition{cp}); err != nil {
		return err
	}

	s.mu.Lock()
	s.checkpoints[channel] = cp
	s.mu.Unlock()

	metrics.DataCoordCheckpointUnixSeconds.DeleteLabelValues(fmt.Sprint(paramtable.GetNodeID()), channel)
	return nil
}

func (s *metaStore) DropChannelCheckpoint(ctx context.Context, vChannel string) error {
	if err := s.catalog.DropChannelCheckpoint(ctx, vChannel); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.checkpoints, vChannel)
	s.mu.Unlock()

	metrics.DataCoordCheckpointUnixSeconds.DeleteLabelValues(fmt.Sprint(paramtable.GetNodeID()), vChannel)
	mlog.Info(ctx, "DropChannelCheckpoint done", mlog.String("vChannel", vChannel))
	return nil
}

func (s *metaStore) GetChannelCheckpoint(vChannel string) *msgpb.MsgPosition {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp, ok := s.checkpoints[vChannel]
	if !ok {
		return nil
	}
	return proto.Clone(cp).(*msgpb.MsgPosition)
}

func (s *metaStore) GetChannelCheckpoints() map[string]*msgpb.MsgPosition {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]*msgpb.MsgPosition, len(s.checkpoints))
	for ch, cp := range s.checkpoints {
		result[ch] = proto.Clone(cp).(*msgpb.MsgPosition)
	}
	return result
}

func (s *metaStore) LoadChannelCheckpoints(checkpoints map[string]*msgpb.MsgPosition) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ch, pos := range checkpoints {
		s.checkpoints[ch] = pos
	}
}

// --- Channel lifecycle methods ---

func (s *metaStore) MarkChannelAdded(ctx context.Context, channel string) error {
	return s.catalog.MarkChannelAdded(ctx, channel)
}

func (s *metaStore) ShouldDropChannel(ctx context.Context, channel string) bool {
	return s.catalog.ShouldDropChannel(ctx, channel)
}

func (s *metaStore) ChannelExists(ctx context.Context, channel string) bool {
	return s.catalog.ChannelExists(ctx, channel)
}

func (s *metaStore) DropChannel(ctx context.Context, channel string) error {
	return s.catalog.DropChannel(ctx, channel)
}

// --- File resource methods ---

// --- GC ---

func (s *metaStore) GcConfirm(ctx context.Context, collectionID, partitionID typeutil.UniqueID) bool {
	return s.catalog.GcConfirm(ctx, collectionID, partitionID)
}

// --- Loading ---

// SegmentNormalizer validates and rewrites a segment loaded from the catalog
// before it enters the store. It returns the segment to install, which may be
// the input itself.
type SegmentNormalizer func(segment *datapb.SegmentInfo) (*datapb.SegmentInfo, error)

// LoadFromCatalog loads segments and channel checkpoints. A non-nil normalize
// runs on every segment first; if it fails, no segment is installed.
func (s *metaStore) LoadFromCatalog(ctx context.Context, collectionIDs []int64, normalize SegmentNormalizer) error {
	record := timerecord.NewTimeRecorder("metaStore")

	pool := conc.NewPool[any](paramtable.Get().MetaStoreCfg.ReadConcurrency.GetAsInt())
	defer pool.Release()
	collectionSegments := make([][]*datapb.SegmentInfo, len(collectionIDs))
	futures := make([]*conc.Future[any], 0, len(collectionIDs))
	for i, collID := range collectionIDs {
		i := i
		collID := collID
		futures = append(futures, pool.Submit(func() (any, error) {
			segments, err := s.catalog.ListSegments(ctx, collID)
			if err != nil {
				return nil, err
			}
			collectionSegments[i] = segments
			return nil, nil
		}))
	}
	if err := conc.AwaitAll(futures...); err != nil {
		return err
	}
	if normalize != nil {
		for i, segments := range collectionSegments {
			normalized := make([]*datapb.SegmentInfo, 0, len(segments))
			for _, segment := range segments {
				seg, err := normalize(segment)
				if err != nil {
					return err
				}
				normalized = append(normalized, seg)
			}
			collectionSegments[i] = normalized
		}
	}
	for _, segments := range collectionSegments {
		s.LoadSegments(segments)
	}

	s.emitSegmentMetrics()

	channelCPs, err := s.catalog.ListChannelCheckpoint(ctx)
	if err != nil {
		return err
	}
	s.mu.Lock()
	for vChannel, pos := range channelCPs {
		pos.ChannelName = vChannel
		s.checkpoints[vChannel] = pos
	}
	s.mu.Unlock()

	s.emitCheckpointMetrics()

	s.mu.RLock()
	numSegments := len(s.segments)
	s.mu.RUnlock()

	mlog.Info(ctx, "metaStore LoadFromCatalog done",
		mlog.Int("numSegments", numSegments),
		mlog.Int("numCheckpoints", len(channelCPs)),
		mlog.Duration("duration", record.ElapseSpan()))
	return nil
}

func (s *metaStore) emitSegmentMetrics() {
	s.mu.RLock()
	defer s.mu.RUnlock()

	metrics.DataCoordNumSegments.Reset()
	for _, segment := range s.segments {
		labels := segMetricLabels(segment)
		metrics.DataCoordNumSegments.WithLabelValues(labels[:]...).Inc()

		if segment.State == commonpb.SegmentState_Flushed {
			stats := segment.GetStats()
			metrics.FlushedSegmentFileNum.WithLabelValues(metrics.InsertFileLabel).Observe(float64(stats.GetInsertBinlogCount()))

			statFileNum := 0
			for _, fieldBinlog := range segment.GetStatslogs() {
				statFileNum += len(fieldBinlog.GetBinlogs())
			}
			metrics.FlushedSegmentFileNum.WithLabelValues(metrics.StatFileLabel).Observe(float64(statFileNum))

			metrics.FlushedSegmentFileNum.WithLabelValues(metrics.DeleteFileLabel).Observe(float64(stats.GetDeltaBinlogCount()))
		}
	}
}

func (s *metaStore) emitCheckpointMetrics() {
	s.mu.RLock()
	defer s.mu.RUnlock()

	nodeID := fmt.Sprint(paramtable.GetNodeID())
	for vChannel, pos := range s.checkpoints {
		if !funcutil.IsDroppedChannelCheckpoint(pos) {
			ts, _ := tsoutil.ParseTS(pos.Timestamp)
			metrics.DataCoordCheckpointUnixSeconds.WithLabelValues(nodeID, vChannel).
				Set(float64(ts.Unix()))
		}
	}
}

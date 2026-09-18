// Package metacache provides a centralized, in-memory store for segment and
// collection metadata that is shared across coordinators when running in
// MixCoord mode. It exposes a read-only MetaView (consumed by QueryCoord)
// and a read-write MetaStore (consumed by DataCoord) backed by the same
// underlying data, avoiding metadata duplication between coordinators.
package metacache

import (
	"context"
	"fmt"
	"math"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
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

// MetaView is the read-only view over the metacache, used by components
// that only need to observe metadata (e.g. QueryCoord).
type MetaView interface {
	// Segments — read
	GetSegment(segmentID int64) (*datapb.SegmentInfo, bool)
	GetSegmentsByIDs(segmentIDs []int64) map[int64]*datapb.SegmentInfo
	GetAllSegments() map[int64]*datapb.SegmentInfo
	GetSegments(collectionID int64) map[int64]*datapb.SegmentInfo
	GetSegmentsByChannel(channel string) map[int64]*datapb.SegmentInfo
	GetSegmentsByState(states ...commonpb.SegmentState) map[int64]*datapb.SegmentInfo
	GetSegmentsByCollectionAndState(collectionID int64, states ...commonpb.SegmentState) map[int64]*datapb.SegmentInfo

	// Collections — read
	GetCollection(collectionID int64) (*CollectionInfo, bool)
	GetAllCollections() map[int64]*CollectionInfo
	GetCollectionsByDatabase(databaseID int64) map[int64]*CollectionInfo
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
	UpdateSegmentState(segmentID int64, newState commonpb.SegmentState)
	LoadSegments(segments []*datapb.SegmentInfo)

	// Segments — catalog persistence (also updates in-memory map + logs)
	AddSegment(ctx context.Context, segment *datapb.SegmentInfo) error
	AlterSegments(ctx context.Context, newSegments []*datapb.SegmentInfo, binlogs ...metastore.BinlogsIncrement) error
	SaveDroppedSegmentsInBatch(ctx context.Context, segments []*datapb.SegmentInfo) error
	DropSegment(ctx context.Context, segment *datapb.SegmentInfo) error
	ListSegments(ctx context.Context, collectionID int64) ([]*datapb.SegmentInfo, error)

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
	LoadFromCatalog(ctx context.Context, collectionIDs []int64) error
}

type metaStore struct {
	mu      sync.RWMutex
	catalog metastore.DataCoordCatalog

	// Segment storage + indexes
	segments    map[int64]*datapb.SegmentInfo
	segCollIdx  map[int64]map[int64]struct{}
	segChanIdx  map[string]map[int64]struct{}
	segStateIdx map[commonpb.SegmentState]map[int64]struct{}

	// Collection storage + indexes
	collections map[int64]*CollectionInfo
	collDBIdx   map[int64]map[int64]struct{}

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
		segStateIdx: make(map[commonpb.SegmentState]map[int64]struct{}),
		collections: make(map[int64]*CollectionInfo),
		collDBIdx:   make(map[int64]map[int64]struct{}),
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

func (s *metaStore) GetSegmentsByState(states ...commonpb.SegmentState) map[int64]*datapb.SegmentInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[int64]*datapb.SegmentInfo)
	for _, state := range states {
		if ids, ok := s.segStateIdx[state]; ok {
			for id := range ids {
				result[id] = s.segments[id]
			}
		}
	}
	return result
}

func (s *metaStore) GetSegmentsByCollectionAndState(collectionID int64, states ...commonpb.SegmentState) map[int64]*datapb.SegmentInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	collIDs, ok := s.segCollIdx[collectionID]
	if !ok {
		return nil
	}
	stateSet := make(map[commonpb.SegmentState]struct{}, len(states))
	for _, st := range states {
		stateSet[st] = struct{}{}
	}
	result := make(map[int64]*datapb.SegmentInfo)
	for id := range collIDs {
		seg := s.segments[id]
		if _, match := stateSet[seg.GetState()]; match {
			result[id] = seg
		}
	}
	return result
}

// --- Segment write methods (in-memory) ---

func (s *metaStore) PutSegment(segment *datapb.SegmentInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := segment.GetID()
	if old, exists := s.segments[id]; exists {
		s.removeSegmentIndexes(id, old)
	}
	s.segments[id] = segment
	s.addSegmentIndexes(id, segment)
}

func (s *metaStore) RemoveSegment(segmentID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.segments[segmentID]
	if !ok {
		return
	}
	s.removeSegmentIndexes(segmentID, old)
	delete(s.segments, segmentID)
}

func (s *metaStore) UpdateSegmentState(segmentID int64, newState commonpb.SegmentState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	seg, ok := s.segments[segmentID]
	if !ok {
		return
	}
	oldState := seg.GetState()
	if oldState == newState {
		return
	}
	if ids, ok := s.segStateIdx[oldState]; ok {
		delete(ids, segmentID)
		if len(ids) == 0 {
			delete(s.segStateIdx, oldState)
		}
	}
	updated := proto.Clone(seg).(*datapb.SegmentInfo)
	updated.State = newState
	s.segments[segmentID] = updated
	if s.segStateIdx[newState] == nil {
		s.segStateIdx[newState] = make(map[int64]struct{})
	}
	s.segStateIdx[newState][segmentID] = struct{}{}
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

	state := seg.GetState()
	if s.segStateIdx[state] == nil {
		s.segStateIdx[state] = make(map[int64]struct{})
	}
	s.segStateIdx[state][id] = struct{}{}
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

	state := seg.GetState()
	if ids, ok := s.segStateIdx[state]; ok {
		delete(ids, id)
		if len(ids) == 0 {
			delete(s.segStateIdx, state)
		}
	}
}

// --- Segment catalog persistence ---

func (s *metaStore) AddSegment(ctx context.Context, segment *datapb.SegmentInfo) error {
	if err := s.catalog.AddSegment(ctx, segment); err != nil {
		return err
	}
	s.PutSegment(segment)
	metrics.DataCoordNumSegments.WithLabelValues(
		segment.GetState().String(), segment.GetLevel().String(), sortLabel(segment.GetIsSorted()),
	).Inc()
	mlog.Info(ctx, "metaStore: segment added",
		mlog.Int64("segmentID", segment.GetID()),
		mlog.String("channel", segment.GetInsertChannel()))
	return nil
}

func (s *metaStore) AlterSegments(ctx context.Context, newSegments []*datapb.SegmentInfo, binlogs ...metastore.BinlogsIncrement) error {
	if err := s.catalog.AlterSegments(ctx, newSegments, binlogs...); err != nil {
		return err
	}
	s.upsertSegmentsLocked(newSegments)
	return nil
}

func (s *metaStore) SaveDroppedSegmentsInBatch(ctx context.Context, segments []*datapb.SegmentInfo) error {
	if err := s.catalog.SaveDroppedSegmentsInBatch(ctx, segments); err != nil {
		return err
	}
	s.upsertSegmentsLocked(segments)
	return nil
}

func (s *metaStore) upsertSegmentsLocked(segments []*datapb.SegmentInfo) {
	s.mu.Lock()
	for _, seg := range segments {
		id := seg.GetID()
		if old, exists := s.segments[id]; exists {
			s.emitSegmentMetricTransition(old, seg)
			s.removeSegmentIndexes(id, old)
		} else {
			metrics.DataCoordNumSegments.WithLabelValues(
				seg.GetState().String(), seg.GetLevel().String(), sortLabel(seg.GetIsSorted()),
			).Inc()
		}
		s.segments[id] = seg
		s.addSegmentIndexes(id, seg)
	}
	s.mu.Unlock()
}

func (s *metaStore) DropSegment(ctx context.Context, segment *datapb.SegmentInfo) error {
	if err := s.catalog.DropSegment(ctx, segment); err != nil {
		return err
	}
	s.RemoveSegment(segment.GetID())
	metrics.DataCoordNumSegments.WithLabelValues(
		segment.GetState().String(), segment.GetLevel().String(), sortLabel(segment.GetIsSorted()),
	).Dec()
	mlog.Info(ctx, "metaStore: segment dropped",
		mlog.Int64("segmentID", segment.GetID()))
	return nil
}

func (s *metaStore) emitSegmentMetricTransition(old, updated *datapb.SegmentInfo) {
	oldLabels := [3]string{old.GetState().String(), old.GetLevel().String(), sortLabel(old.GetIsSorted())}
	newLabels := [3]string{updated.GetState().String(), updated.GetLevel().String(), sortLabel(updated.GetIsSorted())}
	if oldLabels != newLabels {
		metrics.DataCoordNumSegments.WithLabelValues(oldLabels[0], oldLabels[1], oldLabels[2]).Dec()
		metrics.DataCoordNumSegments.WithLabelValues(newLabels[0], newLabels[1], newLabels[2]).Inc()
	}
}

func sortLabel(sorted bool) string {
	if sorted {
		return "sorted"
	}
	return "unsorted"
}

func (s *metaStore) ListSegments(ctx context.Context, collectionID int64) ([]*datapb.SegmentInfo, error) {
	return s.catalog.ListSegments(ctx, collectionID)
}

// --- Collection read methods ---

func (s *metaStore) GetCollection(collectionID int64) (*CollectionInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	info, ok := s.collections[collectionID]
	return info, ok
}

func (s *metaStore) GetAllCollections() map[int64]*CollectionInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[int64]*CollectionInfo, len(s.collections))
	for id, info := range s.collections {
		result[id] = info
	}
	return result
}

func (s *metaStore) GetCollectionsByDatabase(databaseID int64) map[int64]*CollectionInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids, ok := s.collDBIdx[databaseID]
	if !ok {
		return nil
	}
	result := make(map[int64]*CollectionInfo, len(ids))
	for id := range ids {
		result[id] = s.collections[id]
	}
	return result
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
	if old, exists := s.collections[id]; exists {
		s.removeCollectionIndex(id, old)
	}
	s.collections[id] = info
	s.addCollectionIndex(id, info)
	numCollections := len(s.collections)
	s.mu.Unlock()
	metrics.DataCoordNumCollections.WithLabelValues().Set(float64(numCollections))
}

func (s *metaStore) RemoveCollection(collectionID int64) {
	mlog.Info(context.Background(), "metaStore: remove collection", mlog.Int64("collectionID", collectionID))
	s.mu.Lock()
	old, ok := s.collections[collectionID]
	if !ok {
		s.mu.Unlock()
		return
	}
	s.removeCollectionIndex(collectionID, old)
	delete(s.collections, collectionID)
	numCollections := len(s.collections)
	s.mu.Unlock()
	metrics.CleanupDataCoordWithCollectionID(collectionID)
	metrics.DataCoordNumCollections.WithLabelValues().Set(float64(numCollections))
}

func (s *metaStore) addCollectionIndex(id int64, info *CollectionInfo) {
	dbID := info.DatabaseID
	if s.collDBIdx[dbID] == nil {
		s.collDBIdx[dbID] = make(map[int64]struct{})
	}
	s.collDBIdx[dbID][id] = struct{}{}
}

func (s *metaStore) removeCollectionIndex(id int64, info *CollectionInfo) {
	dbID := info.DatabaseID
	if ids, ok := s.collDBIdx[dbID]; ok {
		delete(ids, id)
		if len(ids) == 0 {
			delete(s.collDBIdx, dbID)
		}
	}
}

// --- Channel checkpoint methods ---

// UpdateChannelCheckpoint saves a channel checkpoint if it is newer than the
// existing one. The timestamp comparison is done under RLock and the catalog
// write happens outside the lock to avoid holding it during I/O. This leaves
// a narrow TOCTOU window where two concurrent updates for the same channel
// could both pass the comparison and write; the last writer wins. In practice
// this is benign — checkpoint regression on a single channel is self-healing
// on the next update — and matches the original DC meta behavior.
func (s *metaStore) UpdateChannelCheckpoint(ctx context.Context, vChannel string, pos *msgpb.MsgPosition) error {
	if pos == nil || pos.GetMsgID() == nil {
		return merr.WrapErrServiceInternalMsg("channelCP is nil, vChannel=%s", vChannel)
	}

	s.mu.RLock()
	oldPosition := s.checkpoints[vChannel]
	if oldPosition != nil && oldPosition.Timestamp >= pos.Timestamp {
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
		if pos == nil || pos.GetMsgID() == nil || pos.GetChannelName() == "" {
			mlog.Warn(ctx, "illegal channel cp", mlog.Any("pos", pos))
			continue
		}
		oldPosition := s.checkpoints[pos.GetChannelName()]
		if oldPosition == nil || oldPosition.Timestamp < pos.Timestamp {
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
		Timestamp:   math.MaxUint64,
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

func (s *metaStore) LoadFromCatalog(ctx context.Context, collectionIDs []int64) error {
	record := timerecord.NewTimeRecorder("metaStore")

	for _, collID := range collectionIDs {
		segments, err := s.catalog.ListSegments(ctx, collID)
		if err != nil {
			return err
		}
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
		metrics.DataCoordNumSegments.WithLabelValues(
			segment.GetState().String(),
			segment.GetLevel().String(),
			sortLabel(segment.GetIsSorted()),
		).Inc()

		if segment.State == commonpb.SegmentState_Flushed {
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
			for _, fieldBinlog := range segment.GetDeltalogs() {
				deleteFileNum += len(fieldBinlog.GetBinlogs())
			}
			metrics.FlushedSegmentFileNum.WithLabelValues(metrics.DeleteFileLabel).Observe(float64(deleteFileNum))
		}
	}
}

func (s *metaStore) emitCheckpointMetrics() {
	s.mu.RLock()
	defer s.mu.RUnlock()

	nodeID := fmt.Sprint(paramtable.GetNodeID())
	for vChannel, pos := range s.checkpoints {
		if pos.Timestamp != math.MaxUint64 {
			ts, _ := tsoutil.ParseTS(pos.Timestamp)
			metrics.DataCoordCheckpointUnixSeconds.WithLabelValues(nodeID, vChannel).
				Set(float64(ts.Unix()))
		}
	}
}

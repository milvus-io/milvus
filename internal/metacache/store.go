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

// IsExternal returns true when the collection schema references an external source or spec.
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
	VersionedSegments

	// Segments — write (in-memory only). PutSegment and RemoveSegment force the
	// write (see VersionedSegments); LoadSegments does not count the segments in
	// DataCoordNumSegments, its caller emits the counts.
	PutSegment(segment *datapb.SegmentInfo)
	RemoveSegment(segmentID int64)
	LoadSegments(segments []*datapb.SegmentInfo)

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
	ChannelExists(ctx context.Context, channel string) (bool, error)
	DropChannel(ctx context.Context, channel string) error

	// GC
	GcConfirm(ctx context.Context, collectionID, partitionID typeutil.UniqueID) bool

	// Loading
	LoadFromCatalog(ctx context.Context, collectionIDs []int64, normalize SegmentNormalizer) error
}

// VersionedSegments is the segment storage of the store for the owner of the
// segment metadata (DataCoord). Every live segment carries the version of the
// value persisted for it (the store revision of its record). Reads are
// lock-free and every write is a compare-and-swap of one segment, so writers
// of different segments never wait for each other and readers never block.
// A write with a version > 0 is rejected when the store already holds that or
// a newer version of the segment; a version <= 0 forces the write and keeps
// the stored version. A segment removed at a version > 0 leaves a tombstone
// with that version until PruneSegment, so stale writes of it are rejected.
// Every accepted write keeps DataCoordNumSegments in step, except LoadSegment.
type VersionedSegments interface {
	GetSegmentWithVersion(segmentID int64) (*datapb.SegmentInfo, int64, bool)
	SegmentVersion(segmentID int64) (int64, bool)
	NumSegments() int
	RangeSegments(fn func(segmentID int64, segment *datapb.SegmentInfo) bool)
	// RangeCollectionSegmentIDs and RangeChannelSegmentIDs visit the IDs
	// indexed for a collection or a channel. A segment removed meanwhile may
	// still be visited, so callers look the IDs up.
	RangeCollectionSegmentIDs(collectionID int64, fn func(segmentID int64) bool)
	RangeChannelSegmentIDs(channel string, fn func(segmentID int64) bool)

	// InstallSegment writes segment at version. LoadSegment is InstallSegment
	// without counting the segment, for a recovery that emits the counts of all
	// the segments it loads at once. Both return the live value they replaced
	// (the newer value kept when the write was rejected), nil when there was
	// none, and whether the write applied.
	InstallSegment(segmentID int64, segment *datapb.SegmentInfo, version int64) (*datapb.SegmentInfo, bool)
	LoadSegment(segmentID int64, segment *datapb.SegmentInfo, version int64) (*datapb.SegmentInfo, bool)
	// PublishSegments installs the persisted values of one mutation at their
	// versions (keyed by segment ID). A multi-segment mutation is installed
	// under the publication lock, so a ReadConsistent reader observes all of it
	// or none of it. merge, when not nil, adjusts every value that replaces a
	// live one.
	PublishSegments(segments []*datapb.SegmentInfo, versions map[int64]int64, merge SegmentMerge) []SegmentPublication
	// DropSegment removes a segment at version and returns the live value it
	// removed, nil when the removal did not apply. A version <= 0 leaves no
	// tombstone.
	DropSegment(segmentID int64, version int64) *datapb.SegmentInfo
	// PruneSegment removes the tombstone of a removed segment.
	PruneSegment(segmentID int64)
	// UpdateSegment replaces the live value of a segment with fn(current) and
	// keeps its version: an in-memory change that no persisted write owns. fn
	// may run more than once, must not modify current, and returns nil to leave
	// the segment unchanged. Returns the replaced and the new value, nil when
	// nothing changed.
	UpdateSegment(segmentID int64, fn func(current *datapb.SegmentInfo) *datapb.SegmentInfo) (old, updated *datapb.SegmentInfo)
	// ClearSegments removes every segment, tombstones included, for a recovery
	// attempt that loads them all again.
	ClearSegments()
	// ReadConsistent runs read without observing a half-installed
	// PublishSegments.
	ReadConsistent(read func())
}

// SegmentMerge adjusts next, the value a publication is about to install, to
// current, the live value it replaces. It may run more than once and must not
// modify current.
type SegmentMerge func(current, next *datapb.SegmentInfo)

// SegmentPublication is the outcome of one segment of PublishSegments.
type SegmentPublication struct {
	// Old is the live value before the publication (the newer value kept when
	// the publication was rejected), nil when the segment was not live.
	Old     *datapb.SegmentInfo
	Applied bool
}

type metaStore struct {
	mu      sync.RWMutex
	catalog metastore.DataCoordCatalog

	// Segment storage (see VersionedSegments); mu does not guard it. The
	// indexes map a collection or a channel to the IDs of its segments (see
	// segmentIndex).
	segments   *versionedMap[int64, *datapb.SegmentInfo]
	segCollIdx *segmentIndex[int64]
	segChanIdx *segmentIndex[string]
	// pubMu makes PublishSegments of several segments atomic for
	// ReadConsistent. It is held only around in-memory installs.
	pubMu sync.RWMutex

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
		segments:    newVersionedMap[int64, *datapb.SegmentInfo](),
		segCollIdx:  newSegmentIndex((*datapb.SegmentInfo).GetCollectionID),
		segChanIdx:  newSegmentIndex((*datapb.SegmentInfo).GetInsertChannel),
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
	return s.segments.Lookup(segmentID)
}

func (s *metaStore) GetSegmentsByIDs(segmentIDs []int64) map[int64]*datapb.SegmentInfo {
	result := make(map[int64]*datapb.SegmentInfo, len(segmentIDs))
	for _, id := range segmentIDs {
		if seg, ok := s.segments.Lookup(id); ok {
			result[id] = seg
		}
	}
	return result
}

func (s *metaStore) GetAllSegments() map[int64]*datapb.SegmentInfo {
	result := make(map[int64]*datapb.SegmentInfo, s.segments.Len())
	s.segments.Range(func(id int64, seg *datapb.SegmentInfo) bool {
		result[id] = seg
		return true
	})
	return result
}

// GetSegments returns the segments of a collection, nil when it has none.
func (s *metaStore) GetSegments(collectionID int64) map[int64]*datapb.SegmentInfo {
	ids, ok := s.segCollIdx.get(collectionID)
	if !ok {
		return nil
	}
	return s.lookupIndexed(ids, func(seg *datapb.SegmentInfo) bool {
		return seg.GetCollectionID() == collectionID
	})
}

// GetSegmentsByChannel returns the segments of a channel, nil when it has none.
func (s *metaStore) GetSegmentsByChannel(channel string) map[int64]*datapb.SegmentInfo {
	ids, ok := s.segChanIdx.get(channel)
	if !ok {
		return nil
	}
	return s.lookupIndexed(ids, func(seg *datapb.SegmentInfo) bool {
		return seg.GetInsertChannel() == channel
	})
}

// lookupIndexed returns the live segments of the indexed IDs that match, nil
// when none does: an indexed ID may belong to a segment removed meanwhile.
func (s *metaStore) lookupIndexed(ids *typeutil.ConcurrentMap[int64, struct{}], match func(*datapb.SegmentInfo) bool) map[int64]*datapb.SegmentInfo {
	var result map[int64]*datapb.SegmentInfo
	ids.Range(func(id int64, _ struct{}) bool {
		if seg, ok := s.segments.Lookup(id); ok && match(seg) {
			if result == nil {
				result = make(map[int64]*datapb.SegmentInfo, ids.Len())
			}
			result[id] = seg
		}
		return true
	})
	return result
}

func (s *metaStore) GetSegmentWithVersion(segmentID int64) (*datapb.SegmentInfo, int64, bool) {
	return s.segments.LookupWithVersion(segmentID)
}

func (s *metaStore) SegmentVersion(segmentID int64) (int64, bool) {
	return s.segments.Version(segmentID)
}

func (s *metaStore) NumSegments() int {
	return s.segments.Len()
}

func (s *metaStore) RangeSegments(fn func(segmentID int64, segment *datapb.SegmentInfo) bool) {
	s.segments.Range(fn)
}

func (s *metaStore) RangeCollectionSegmentIDs(collectionID int64, fn func(segmentID int64) bool) {
	if ids, ok := s.segCollIdx.get(collectionID); ok {
		ids.Range(func(id int64, _ struct{}) bool { return fn(id) })
	}
}

func (s *metaStore) RangeChannelSegmentIDs(channel string, fn func(segmentID int64) bool) {
	if ids, ok := s.segChanIdx.get(channel); ok {
		ids.Range(func(id int64, _ struct{}) bool { return fn(id) })
	}
}

// ReadConsistent runs read without observing a half-installed PublishSegments.
func (s *metaStore) ReadConsistent(read func()) {
	s.pubMu.RLock()
	defer s.pubMu.RUnlock()
	read()
}

// --- Segment write methods (in-memory) ---

// PutSegment inserts or replaces a segment, keeping its stored version, and
// emits the matching DataCoordNumSegments change.
func (s *metaStore) PutSegment(segment *datapb.SegmentInfo) {
	s.InstallSegment(segment.GetID(), segment, 0)
}

// RemoveSegment deletes a segment, leaving no tombstone, and decrements
// DataCoordNumSegments.
func (s *metaStore) RemoveSegment(segmentID int64) {
	s.DropSegment(segmentID, 0)
}

func (s *metaStore) LoadSegments(segments []*datapb.SegmentInfo) {
	for _, seg := range segments {
		s.LoadSegment(seg.GetID(), seg, 0)
	}
}

func (s *metaStore) InstallSegment(segmentID int64, segment *datapb.SegmentInfo, version int64) (*datapb.SegmentInfo, bool) {
	old, applied := s.LoadSegment(segmentID, segment, version)
	if applied {
		segmentMetricTransition(old, segment)
	}
	return old, applied
}

func (s *metaStore) LoadSegment(segmentID int64, segment *datapb.SegmentInfo, version int64) (*datapb.SegmentInfo, bool) {
	old, _, applied := s.segments.Swap(segmentID, version, func(*datapb.SegmentInfo, bool) *datapb.SegmentInfo {
		return segment
	})
	if applied {
		s.indexSegment(segmentID, old, segment)
	}
	return old, applied
}

func (s *metaStore) PublishSegments(segments []*datapb.SegmentInfo, versions map[int64]int64, merge SegmentMerge) []SegmentPublication {
	if len(segments) > 1 {
		s.pubMu.Lock()
		defer s.pubMu.Unlock()
	}
	results := make([]SegmentPublication, len(segments))
	for i, segment := range segments {
		id := segment.GetID()
		old, _, applied := s.segments.Swap(id, versions[id], func(current *datapb.SegmentInfo, live bool) *datapb.SegmentInfo {
			if live && merge != nil {
				merge(current, segment)
			}
			return segment
		})
		if applied {
			s.indexSegment(id, old, segment)
			segmentMetricTransition(old, segment)
		} else {
			// The store already holds this or a newer version.
			currentVersion, _ := s.segments.Version(id)
			mlog.Debug(context.TODO(), "stale segment publication ignored",
				mlog.FieldSegmentID(id), mlog.Int64("version", versions[id]), mlog.Int64("currentVersion", currentVersion))
		}
		results[i] = SegmentPublication{Old: old, Applied: applied}
	}
	return results
}

func (s *metaStore) DropSegment(segmentID int64, version int64) *datapb.SegmentInfo {
	// Erase first: a stale drop is rejected and must leave the indexes intact;
	// a drop over a tombstone still advances the tombstone version. Only the
	// drop that erases the live value counts it, so concurrent drops of one
	// segment decrement once.
	old := s.segments.Erase(segmentID, version)
	if old != nil {
		s.unindexSegment(segmentID, old)
		segmentMetricTransition(old, nil)
	}
	if version <= 0 {
		s.segments.Prune(segmentID)
	}
	return old
}

func (s *metaStore) PruneSegment(segmentID int64) {
	s.segments.Prune(segmentID)
}

func (s *metaStore) UpdateSegment(segmentID int64, fn func(current *datapb.SegmentInfo) *datapb.SegmentInfo) (old, updated *datapb.SegmentInfo) {
	// Transform may rerun fn after a lost race; the last run is the applied one.
	if !s.segments.Transform(segmentID, func(current *datapb.SegmentInfo) (*datapb.SegmentInfo, bool) {
		old, updated = current, fn(current)
		return updated, updated != nil
	}) {
		return nil, nil
	}
	segmentMetricTransition(old, updated)
	return old, updated
}

func (s *metaStore) ClearSegments() {
	s.segments.clear()
	s.segCollIdx.clear()
	s.segChanIdx.clear()
}

// indexSegment indexes segment, which replaced old (nil when the segment was
// not live). A segment's collection and channel only change in tests that
// rewrite it wholesale.
func (s *metaStore) indexSegment(segmentID int64, old, segment *datapb.SegmentInfo) {
	if old != nil && (old.GetCollectionID() != segment.GetCollectionID() || old.GetInsertChannel() != segment.GetInsertChannel()) {
		s.unindexSegment(segmentID, old)
	}
	s.segCollIdx.add(s.segments, segment.GetCollectionID(), segmentID)
	s.segChanIdx.add(s.segments, segment.GetInsertChannel(), segmentID)
}

func (s *metaStore) unindexSegment(segmentID int64, segment *datapb.SegmentInfo) {
	s.segCollIdx.remove(s.segments, segment.GetCollectionID(), segmentID)
	s.segChanIdx.remove(s.segments, segment.GetInsertChannel(), segmentID)
}

// segmentIndex maps a key (a collection or a channel) to the bucket of the
// IDs of its live segments. Adds and removes run without locks: an add checks
// the segment again after it lands, and a removal takes an emptied bucket out
// of the index, so the index neither keeps the IDs of removed segments nor
// the buckets of dropped collections and channels.
type segmentIndex[K comparable] struct {
	buckets *typeutil.ConcurrentMap[K, *typeutil.ConcurrentMap[int64, struct{}]]
	keyOf   func(*datapb.SegmentInfo) K
}

func newSegmentIndex[K comparable](keyOf func(*datapb.SegmentInfo) K) *segmentIndex[K] {
	return &segmentIndex[K]{
		buckets: typeutil.NewConcurrentMap[K, *typeutil.ConcurrentMap[int64, struct{}]](),
		keyOf:   keyOf,
	}
}

// indexes reports whether segmentID is a live segment of key.
func (idx *segmentIndex[K]) indexes(segments *versionedMap[int64, *datapb.SegmentInfo], key K, segmentID int64) bool {
	segment, ok := segments.Lookup(segmentID)
	return ok && idx.keyOf(segment) == key
}

// add adds segmentID to the bucket of key. A removal may take the bucket out
// of the index while the add lands in it; the add then goes again into the
// bucket the index holds now. A removal of the segment that ran before the add
// landed is undone by checking the segment afterwards: one that runs after
// the check finds the add.
func (idx *segmentIndex[K]) add(segments *versionedMap[int64, *datapb.SegmentInfo], key K, segmentID int64) {
	for {
		bucket, ok := idx.buckets.Get(key)
		if !ok {
			bucket, _ = idx.buckets.GetOrInsert(key, typeutil.NewConcurrentMap[int64, struct{}]())
		}
		bucket.Insert(segmentID, struct{}{})
		if current, ok := idx.buckets.Get(key); ok && current == bucket {
			break
		}
	}
	if !idx.indexes(segments, key, segmentID) {
		idx.remove(segments, key, segmentID)
	}
}

// remove removes segmentID from the bucket of key, and the bucket from the
// index once it is empty. The live segments added to the bucket while it was
// being taken out move into the bucket the index holds now; until then a
// reader of the index misses them, as it misses a segment whose install has
// not indexed it yet.
func (idx *segmentIndex[K]) remove(segments *versionedMap[int64, *datapb.SegmentInfo], key K, segmentID int64) {
	bucket, ok := idx.buckets.Get(key)
	if !ok {
		return
	}
	bucket.Remove(segmentID)
	if bucket.Len() > 0 || !idx.buckets.CompareAndDelete(key, bucket) {
		return
	}
	bucket.Range(func(id int64, _ struct{}) bool {
		if idx.indexes(segments, key, id) {
			idx.add(segments, key, id)
		}
		return true
	})
}

// get returns the bucket of key.
func (idx *segmentIndex[K]) get(key K) (*typeutil.ConcurrentMap[int64, struct{}], bool) {
	return idx.buckets.Get(key)
}

// clear removes every bucket.
func (idx *segmentIndex[K]) clear() {
	idx.buckets.Range(func(key K, _ *typeutil.ConcurrentMap[int64, struct{}]) bool {
		idx.buckets.Remove(key)
		return true
	})
}

// segmentMetricTransition updates DataCoordNumSegments for a segment replaced
// by updated. A nil old counts a new segment, a nil updated a removed one.
func segmentMetricTransition(old, updated *datapb.SegmentInfo) {
	switch {
	case old == nil && updated == nil:
	case old == nil:
		labels := segMetricLabels(updated)
		metrics.DataCoordNumSegments.WithLabelValues(labels[:]...).Inc()
	case updated == nil:
		labels := segMetricLabels(old)
		metrics.DataCoordNumSegments.WithLabelValues(labels[:]...).Dec()
	default:
		oldLabels, newLabels := segMetricLabels(old), segMetricLabels(updated)
		if oldLabels != newLabels {
			metrics.DataCoordNumSegments.WithLabelValues(oldLabels[:]...).Dec()
			metrics.DataCoordNumSegments.WithLabelValues(newLabels[:]...).Inc()
		}
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

// PutCollection replaces the entry wholesale. It is in-memory only and takes no
// ctx, like the other in-memory writers; its callers (meta.AddCollection /
// meta.DropCollection) do the logging with a real ctx.
func (s *metaStore) PutCollection(info *CollectionInfo) {
	s.mu.Lock()
	id := info.ID
	s.collections[id] = info
	// Set() under the lock: releasing first lets two concurrent writers commit
	// their counts out of order and strand the gauge at a stale value.
	metrics.DataCoordNumCollections.WithLabelValues().Set(float64(len(s.collections)))
	s.mu.Unlock()
}

func (s *metaStore) RemoveCollection(collectionID int64) {
	s.mu.Lock()
	if _, ok := s.collections[collectionID]; !ok {
		s.mu.Unlock()
		return
	}
	delete(s.collections, collectionID)
	metrics.DataCoordNumCollections.WithLabelValues().Set(float64(len(s.collections)))
	s.mu.Unlock()
	metrics.CleanupDataCoordWithCollectionID(collectionID)
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

func (s *metaStore) ChannelExists(ctx context.Context, channel string) (bool, error) {
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

	// Recovery retries (newMeta is wrapped in retry.Do) reuse this store,
	// because it is injected rather than rebuilt per attempt. Loading is
	// otherwise purely additive, so a failed attempt would leave its segments
	// behind -- and if the catalog's collection set shrank in between, those
	// orphans stay resolvable to QueryCoord forever. Start from empty so each
	// attempt reflects exactly what the catalog holds now.
	s.ClearSegments()
	s.mu.Lock()
	s.checkpoints = make(map[string]*msgpb.MsgPosition)
	s.mu.Unlock()

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

	numSegments := s.segments.Len()

	mlog.Info(ctx, "metaStore LoadFromCatalog done",
		mlog.Int("numSegments", numSegments),
		mlog.Int("numCheckpoints", len(channelCPs)),
		mlog.Duration("duration", record.ElapseSpan()))
	return nil
}

func (s *metaStore) emitSegmentMetrics() {
	metrics.DataCoordNumSegments.Reset()
	s.segments.Range(func(_ int64, segment *datapb.SegmentInfo) bool {
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
		return true
	})
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

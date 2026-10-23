package datacoord

import (
	"context"
	"math/rand"
	"slices"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// Segment writes on the optimistic segment store.
//
// The store is the source of truth: readers use the lock-free segment cache,
// and every write is a compare-and-swap of the stored values it is built on.
// A writer takes no lock of the segments it changes; writers of different
// segments run in parallel. It
//  1. reads the segments it changes and builds their new values on them: the
//     cached values on its first attempt, the stored values on every attempt
//     after a conflict (segmentReader);
//  2. persists them through the segment store, which compares each written
//     segment against the version it was read at (casSegmentStore), so a
//     write built on cached values commits only if they are the stored ones.
//     A conflict means another writer stored one of the segments since: the
//     write fails with ErrVersionConflict;
//  3. runs again from step 1 on a conflict (retrySegmentWrite), until it does
//     not conflict or ctx ends. A write that stored part of itself before
//     failing publishes that part (publishLanded);
//  4. publishes the persisted values with their new versions
//     (publishSegments). Publications of concurrent writers may arrive out of
//     order: the cache keeps the newer version.
//
// Lock order: segment manifest lock -> index keyLock(buildID) -> groupMu ->
// segments.pubMu.

const (
	// segmentWriteRetryMinBackoff and segmentWriteRetryMaxBackoff bound the
	// wait before a write that conflicted runs again; it doubles, jittered.
	segmentWriteRetryMinBackoff = time.Millisecond
	segmentWriteRetryMaxBackoff = time.Second
)

// retrySegmentWrite runs write, which builds a segment write from the segments
// r reads and commits it, until it does not fail with a version conflict, or
// ctx ends. The first run reads the cache; a run after a conflict reads the
// store, so it builds on what the conflicting writer stored whether or not it
// published it. Writes of one segment rarely conflict, so there is no attempt
// limit.
func (m *meta) retrySegmentWrite(ctx context.Context, write func(r *segmentReader) error) error {
	backoff := segmentWriteRetryMinBackoff
	fromStore := false
	for attempt := 1; ; attempt++ {
		r := &segmentReader{m: m, ctx: ctx, store: fromStore, reads: make(map[int64]segmentRead)}
		err := write(r)
		var partial *partialWriteError
		if errors.As(err, &partial) {
			m.publishLanded(partial.landed)
		}
		if r.err != nil {
			// A failed store read returns no segment, so the run did not
			// commit on it; its outcome is that failure.
			return r.err
		}
		if err == nil || !errors.Is(err, ErrVersionConflict) || ctx.Err() != nil {
			return err
		}
		fromStore = true
		mlog.Info(ctx, "meta update: segment meta moved under the write, rebuilding it from the store",
			mlog.Int("attempt", attempt), mlog.Err(err))
		timer := time.NewTimer(backoff/2 + time.Duration(rand.Int63n(int64(backoff/2)+1)))
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return err
		}
		backoff = min(2*backoff, segmentWriteRetryMaxBackoff)
	}
}

// store returns the segment store, defaulting to the catalog for a meta built
// without one (unit tests construct meta directly).
func (m *meta) store() segmentStore {
	m.segmentStoreOnce.Do(func() {
		if m.segmentStore == nil {
			m.segmentStore = newCatalogSegmentStore(func() metastore.DataCoordCatalog { return m.catalog })
		}
	})
	return m.segmentStore
}

// publishSegment installs the persisted value of one segment and returns the
// value it replaced.
func (m *meta) publishSegment(segment *SegmentInfo, version int64) *SegmentInfo {
	return m.publishSegments([]*SegmentInfo{segment}, map[int64]int64{segment.GetID(): version})[segment.GetID()]
}

// publishSegments installs the persisted values of a mutation and returns the
// values they replaced. A multi-segment mutation is installed atomically
// (SegmentsInfo.PublishSegments), so a consistent reader observes all
// of it or none of it.
func (m *meta) publishSegments(segments []*SegmentInfo, versions map[int64]int64) map[int64]*SegmentInfo {
	return m.segments.PublishSegments(segments, versions)
}

// publishLanded installs the segments a failed write stored before it failed
// (see partialWriteError), as a successful write publishes its own: the cache
// keeps matching the store, so the rebuilt write, and every other writer of
// those segments, builds on them.
func (m *meta) publishLanded(landed []landedSegment) {
	segments := make([]*SegmentInfo, 0, len(landed))
	versions := make(map[int64]int64, len(landed))
	for _, l := range landed {
		segments = append(segments, NewSegmentInfo(proto.Clone(l.segment).(*datapb.SegmentInfo)))
		versions[l.segment.GetID()] = l.version
	}
	m.publishSegments(segments, versions)
}

// selectSegmentsConsistent is SelectSegments for readers that must not observe
// a half-installed multi-segment mutation.
func (m *meta) selectSegmentsConsistent(ctx context.Context, filters ...SegmentFilter) []*SegmentInfo {
	var segments []*SegmentInfo
	m.segments.ReadConsistent(func() {
		segments = m.SelectSegments(ctx, filters...)
	})
	return segments
}

// runOperators builds the update pack of operators on the segments r reads.
func (m *meta) runOperators(r *segmentReader, operators []UpdateOperator) (*updateSegmentPack, error) {
	pack := m.newUpdateSegmentPack()
	pack.reader = r
	if err := pack.run(operators); err != nil {
		return nil, err
	}
	if err := r.failure(); err != nil {
		return nil, err
	}
	return pack, nil
}

// segmentReader reads the segments one run of a segment write builds on (see
// retrySegmentWrite): from the cache, or from the store once a run conflicted.
// The first read of a segment pins its value and version for the rest of the
// run, so every check of the write sees the value the write compares against.
//
// A store read of a segment whose record is still at its cached version
// returns the cached value, binlogs included; any other is read with its
// binlog KVs (segmentStore.ReadSegment). The values a run reads from the store
// are not installed in the cache: the values it stores are, once it commits.
type segmentReader struct {
	m     *meta
	ctx   context.Context
	store bool
	reads map[int64]segmentRead
	// err is the first failed store read. A failed read returns no segment,
	// so a write checks failure before it commits.
	err error
}

// get returns the value of a segment the run builds on and its version, nil
// and 0 when the segment does not exist. A store read locates the segment by
// its cached value: a segment the cache does not hold reads as absent, unless
// the write locates it (getAt).
func (r *segmentReader) get(segmentID int64) (*SegmentInfo, int64) {
	if read, ok := r.reads[segmentID]; ok {
		return read.segment, read.version
	}
	segment, version := r.m.segments.GetSegmentWithVersion(segmentID)
	if r.store && segment != nil {
		segment, version = r.read(segment.GetCollectionID(), segment.GetPartitionID(), segmentID, segment, version)
	}
	return r.pin(segmentID, segment, version)
}

// getAt is get for a segment the write knows the collection and partition of,
// such as one it creates: a store read finds it even when the cache does not
// hold it.
func (r *segmentReader) getAt(collectionID, partitionID, segmentID int64) (*SegmentInfo, int64) {
	if read, ok := r.reads[segmentID]; ok {
		return read.segment, read.version
	}
	segment, version := r.m.segments.GetSegmentWithVersion(segmentID)
	if r.store {
		segment, version = r.read(collectionID, partitionID, segmentID, segment, version)
	}
	return r.pin(segmentID, segment, version)
}

func (r *segmentReader) read(collectionID, partitionID, segmentID int64, cached *SegmentInfo, version int64) (*SegmentInfo, int64) {
	segment, version, err := r.m.store().ReadSegment(r.ctx, collectionID, partitionID, segmentID, cached, version)
	if err != nil {
		r.fail(err)
		return nil, 0
	}
	return segment, version
}

func (r *segmentReader) pin(segmentID int64, segment *SegmentInfo, version int64) (*SegmentInfo, int64) {
	r.reads[segmentID] = segmentRead{segment: segment, version: version}
	return segment, version
}

func (r *segmentReader) fail(err error) {
	if r.err == nil {
		r.err = err
	}
}

// failure returns the error of a failed store read of the run, which a write
// returns instead of committing.
func (r *segmentReader) failure() error {
	return r.err
}

// selectSegments returns the segments matching filters the run builds on,
// pinned like get. The cache answers with one consistent cut of it (see
// selectSegmentsConsistent). The store answers by scanning the collections of
// the segments the cache matches, so it also returns the segments a writer
// stored and has not published yet, such as the outputs of a compaction of
// the matched segments.
func (r *segmentReader) selectSegments(filters ...SegmentFilter) []*SegmentInfo {
	matches := func(segment *SegmentInfo) bool {
		if segment == nil {
			return false
		}
		for _, filter := range filters {
			if !filter.Match(segment) {
				return false
			}
		}
		return true
	}
	var selected []*SegmentInfo
	if !r.store {
		r.m.segments.ReadConsistent(func() {
			for _, candidate := range r.m.segments.GetSegmentsBySelector(filters...) {
				if segment, _ := r.get(candidate.GetID()); matches(segment) {
					selected = append(selected, segment)
				}
			}
		})
		return selected
	}
	var collectionIDs []int64
	for _, candidate := range r.m.segments.GetSegmentsBySelector(filters...) {
		collectionIDs = append(collectionIDs, candidate.GetCollectionID())
	}
	collectionIDs = lo.Uniq(collectionIDs)
	slices.Sort(collectionIDs)
	for _, collectionID := range collectionIDs {
		segments, versions, err := r.m.store().ScanSegments(r.ctx, collectionID, r.m.segments.GetSegmentWithVersion)
		if err != nil {
			r.fail(err)
			return nil
		}
		for i, stored := range segments {
			if _, ok := r.reads[stored.GetID()]; !ok {
				r.pin(stored.GetID(), stored, versions[i])
			}
			if segment, _ := r.get(stored.GetID()); matches(segment) {
				selected = append(selected, segment)
			}
		}
	}
	return selected
}

// withVersions adds to base the version of each of segments the run builds
// on, 0 for a segment that does not exist. It is the base of the segments a
// write creates under IDs no other write uses: an earlier run of the write may
// have stored them already.
func (r *segmentReader) withVersions(base map[int64]int64, segments ...*SegmentInfo) map[int64]int64 {
	for _, segment := range segments {
		_, base[segment.GetID()] = r.getAt(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
	}
	return base
}

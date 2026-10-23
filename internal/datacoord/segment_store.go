package datacoord

import (
	"context"
	"sync/atomic"

	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	datacoordkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/kv/txn"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// segmentStore persists the segment records of meta. Its methods mirror the
// segment part of metastore.DataCoordCatalog and additionally return the store
// version of every written segment, which meta keeps next to the cached value.
//
// Two implementations exist:
//   - casSegmentStore: segments live in an OptimisticTxnPersist in the
//     catalog's encoding (a record without binlogs plus per-field binlog KVs)
//     and every record write is a compare-and-swap against the version it was
//     built from, so writers of different segments never serialize on a
//     global lock. The non-segment actions of a composite write are encoded by
//     the catalog and committed in the same transaction. Used by the server.
//   - catalogSegmentStore: forwards to the catalog unchanged. Used when meta is
//     built without a persist (unit tests).
//
// base maps every segment a write changes to the store version of the value
// the write was built from (read with it, see segmentReader); a segment absent
// from base must not exist yet.
type segmentStore interface {
	// ListSegments lists the stored segments of a collection with their
	// versions (a metacache.SegmentLister).
	ListSegments(ctx context.Context, collectionID int64) ([]*datapb.SegmentInfo, []int64, error)
	// AddSegment persists a segment that must not exist yet.
	AddSegment(ctx context.Context, segment *datapb.SegmentInfo) (int64, error)
	// DropSegment removes a segment record.
	DropSegment(ctx context.Context, base map[int64]int64, segment *datapb.SegmentInfo) error
	// DropSegments removes segment records in one transaction.
	DropSegments(ctx context.Context, base map[int64]int64, segments []*datapb.SegmentInfo) error
	// AlterSegments persists new values of segments.
	AlterSegments(ctx context.Context, base map[int64]int64, segments []*datapb.SegmentInfo, binlogs ...metastore.BinlogsIncrement) (map[int64]int64, error)
	// SaveDroppedSegmentsInBatch persists dropped segments.
	SaveDroppedSegmentsInBatch(ctx context.Context, base map[int64]int64, segments []*datapb.SegmentInfo) (map[int64]int64, error)
	// Update applies a composite write with the catalog's atomicity contract.
	Update(ctx context.Context, base map[int64]int64, actions ...metastore.UpdateAction) (map[int64]int64, error)
	// ReadSegment reads the stored value of a segment, located by its
	// collection and partition, with its version; nil when it does not exist.
	// cached, at version, is the value the caller holds: a record still at
	// that version comes back as cached, binlogs included, without reading
	// them, as a write of the binlogs of a segment rewrites its record in the
	// same batch.
	ReadSegment(ctx context.Context, collectionID, partitionID, segmentID int64, cached *SegmentInfo, version int64) (*SegmentInfo, int64, error)
	// ScanSegments reads the stored segments of a collection with their
	// versions. cached returns the value the caller holds of a segment and
	// its version, as for ReadSegment.
	ScanSegments(ctx context.Context, collectionID int64, cached func(segmentID int64) (*SegmentInfo, int64)) ([]*SegmentInfo, []int64, error)
}

// ============================================================
// catalogSegmentStore
// ============================================================

type catalogSegmentStore struct {
	// catalog returns the catalog at call time, so tests that swap
	// meta.catalog after construction keep observing segment writes.
	catalog func() metastore.DataCoordCatalog
	version atomic.Int64
}

func newCatalogSegmentStore(catalog func() metastore.DataCoordCatalog) *catalogSegmentStore {
	return &catalogSegmentStore{catalog: catalog}
}

func (s *catalogSegmentStore) nextVersions(segments []*datapb.SegmentInfo) map[int64]int64 {
	versions := make(map[int64]int64, len(segments))
	for _, segment := range segments {
		versions[segment.GetID()] = s.version.Add(1)
	}
	return versions
}

func (s *catalogSegmentStore) ListSegments(ctx context.Context, collectionID int64) ([]*datapb.SegmentInfo, []int64, error) {
	segments, err := s.catalog().ListSegments(ctx, collectionID)
	if err != nil {
		return nil, nil, err
	}
	versions := make([]int64, len(segments))
	for i := range segments {
		versions[i] = s.version.Add(1)
	}
	return segments, versions, nil
}

func (s *catalogSegmentStore) AddSegment(ctx context.Context, segment *datapb.SegmentInfo) (int64, error) {
	if err := s.catalog().AddSegment(ctx, segment); err != nil {
		return 0, err
	}
	return s.version.Add(1), nil
}

func (s *catalogSegmentStore) DropSegment(ctx context.Context, _ map[int64]int64, segment *datapb.SegmentInfo) error {
	return s.catalog().DropSegment(ctx, segment)
}

// DropSegments drops the segments one by one: the catalog has no batch drop.
func (s *catalogSegmentStore) DropSegments(ctx context.Context, _ map[int64]int64, segments []*datapb.SegmentInfo) error {
	for _, segment := range segments {
		if err := s.catalog().DropSegment(ctx, segment); err != nil {
			return err
		}
	}
	return nil
}

func (s *catalogSegmentStore) AlterSegments(ctx context.Context, _ map[int64]int64, segments []*datapb.SegmentInfo, binlogs ...metastore.BinlogsIncrement) (map[int64]int64, error) {
	if err := s.catalog().AlterSegments(ctx, segments, binlogs...); err != nil {
		return nil, err
	}
	return s.nextVersions(segments), nil
}

func (s *catalogSegmentStore) SaveDroppedSegmentsInBatch(ctx context.Context, _ map[int64]int64, segments []*datapb.SegmentInfo) (map[int64]int64, error) {
	if err := s.catalog().SaveDroppedSegmentsInBatch(ctx, segments); err != nil {
		return nil, err
	}
	return s.nextVersions(segments), nil
}

func (s *catalogSegmentStore) Update(ctx context.Context, _ map[int64]int64, actions ...metastore.UpdateAction) (map[int64]int64, error) {
	if err := s.catalog().Update(ctx, actions...); err != nil {
		return nil, err
	}
	return s.nextVersions(segmentsOfActions(actions)), nil
}

// ReadSegment is not supported: no write against the catalog conflicts, so
// none reads the store back.
func (s *catalogSegmentStore) ReadSegment(context.Context, int64, int64, int64, *SegmentInfo, int64) (*SegmentInfo, int64, error) {
	return nil, 0, merr.WrapErrServiceInternalMsg("the catalog segment store does not read segments back")
}

// ScanSegments is not supported, see ReadSegment.
func (s *catalogSegmentStore) ScanSegments(context.Context, int64, func(int64) (*SegmentInfo, int64)) ([]*SegmentInfo, []int64, error) {
	return nil, nil, merr.WrapErrServiceInternalMsg("the catalog segment store does not read segments back")
}

func segmentsOfActions(actions []metastore.UpdateAction) []*datapb.SegmentInfo {
	var segments []*datapb.SegmentInfo
	for _, action := range actions {
		if entry, ok := action.Entry.(metastore.SegmentEntry); ok && entry.Segment != nil {
			segments = append(segments, entry.Segment)
		}
	}
	return segments
}

// ============================================================
// casSegmentStore
// ============================================================

// catalogUpdateEncoder encodes composite actions into ops without committing
// them (implemented by the kv DataCoord catalog).
type catalogUpdateEncoder interface {
	EncodeUpdate(ctx context.Context, actions ...metastore.UpdateAction) (*txn.Builder, error)
}

type casSegmentStore struct {
	persist OptimisticTxnPersist[string, *datapb.SegmentInfo]
	encoder catalogUpdateEncoder
	// codec keeps the persisted segments in the catalog encoding, so the
	// catalog (and an older DataCoord) reads what this store writes and the
	// other way round.
	codec datacoordkv.SegmentCodec
	// rootPath is the MetaKv root of the catalog keys; segment keys and the
	// raw ops encoded by the catalog are joined under it.
	rootPath string
	// inline holds the segments whose persisted record still carries its
	// binlogs: records written before this store kept the catalog encoding.
	// Their next write moves every binlog family out of the record.
	inline *typeutil.ConcurrentSet[int64]
}

func newCASSegmentStore(persist OptimisticTxnPersist[string, *datapb.SegmentInfo], catalog metastore.DataCoordCatalog, rootPath string) *casSegmentStore {
	encoder, _ := catalog.(catalogUpdateEncoder)
	chunkManagerRootPath := ""
	if kc, ok := catalog.(*datacoordkv.Catalog); ok {
		chunkManagerRootPath = kc.ChunkManagerRootPath
	}
	return &casSegmentStore{
		persist: persist,
		encoder: encoder,
		// Every segment this store writes keeps its binlogs in binlog KVs (an
		// inline record moves them out on its first write), so the GC
		// compatibility write of a dropped segment never applies.
		codec:    datacoordkv.NewSegmentCodec(chunkManagerRootPath, nil),
		rootPath: rootPath,
		inline:   typeutil.NewConcurrentSet[int64](),
	}
}

// join joins a key under the root the way the catalog's MetaKv does, keeping
// the trailing separator of a prefix (segment 3 must not cover segment 30).
func (s *casSegmentStore) join(key string) string {
	return util.GetPath(s.rootPath, key)
}

// ListSegments scans the segment records and the binlog KVs of a collection
// in one walk per prefix, like the catalog.
func (s *casSegmentStore) ListSegments(ctx context.Context, collectionID int64) ([]*datapb.SegmentInfo, []int64, error) {
	var records []*datapb.SegmentInfo
	var versions []int64
	var binlogs []map[int64][]*datapb.FieldBinlog
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		_, records, versions, err = s.persist.Scan(gctx, s.join(datacoordkv.SegmentRecordPrefixOf(collectionID)))
		return err
	})
	g.Go(func() error {
		var err error
		binlogs, err = s.loadBinlogs(gctx, datacoordkv.SegmentBinlogPrefixesOfCollection(collectionID))
		return err
	})
	if err := g.Wait(); err != nil {
		return nil, nil, err
	}

	inline := 0
	for _, record := range records {
		if datacoordkv.HasInlineBinlogs(record) {
			s.inline.Insert(record.GetID())
			inline++
		}
		s.codec.DecodeRecord(record)
	}
	if err := datacoordkv.FillSegmentBinlogs(records, binlogs); err != nil {
		return nil, nil, err
	}
	if inline > 0 {
		mlog.Info(ctx, "segment records still carry their binlogs; their next write moves them out", mlog.Int("numSegments", inline))
	}
	return records, versions, nil
}

// loadBinlogs reads the binlog KVs under prefixes (one prefix per family, in
// the order of datacoordkv.SegmentBinlogPrefixes) into one map per family,
// keyed by segment.
func (s *casSegmentStore) loadBinlogs(ctx context.Context, prefixes []string) ([]map[int64][]*datapb.FieldBinlog, error) {
	binlogs := make([]map[int64][]*datapb.FieldBinlog, len(prefixes))
	g, gctx := errgroup.WithContext(ctx)
	for i, prefix := range prefixes {
		family := make(map[int64][]*datapb.FieldBinlog)
		binlogs[i] = family
		g.Go(func() error {
			return s.persist.WalkRaw(gctx, s.join(prefix), func(key string, value []byte) error {
				segmentID, fieldBinlog, err := datacoordkv.DecodeFieldBinlog(key, value)
				if err != nil {
					return err
				}
				family[segmentID] = append(family[segmentID], fieldBinlog)
				return nil
			})
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return binlogs, nil
}

func (s *casSegmentStore) ReadSegment(ctx context.Context, collectionID, partitionID, segmentID int64, cached *SegmentInfo, version int64) (*SegmentInfo, int64, error) {
	key := s.join(datacoordkv.SegmentRecordKey(&datapb.SegmentInfo{ID: segmentID, CollectionID: collectionID, PartitionID: partitionID}))
	stored, err := s.persist.Get(ctx, []string{key})
	if err != nil {
		return nil, 0, err
	}
	current, ok := stored[key]
	if !ok {
		return nil, 0, nil
	}
	segment, err := s.decode(ctx, current.Value, current.Version, cached, version)
	if err != nil {
		return nil, 0, err
	}
	return segment, current.Version, nil
}

func (s *casSegmentStore) ScanSegments(ctx context.Context, collectionID int64, cached func(segmentID int64) (*SegmentInfo, int64)) ([]*SegmentInfo, []int64, error) {
	_, records, versions, err := s.persist.Scan(ctx, s.join(datacoordkv.SegmentRecordPrefixOf(collectionID)))
	if err != nil {
		return nil, nil, err
	}
	segments := make([]*SegmentInfo, len(records))
	for i, record := range records {
		value, version := cached(record.GetID())
		if segments[i], err = s.decode(ctx, record, versions[i], value, version); err != nil {
			return nil, nil, err
		}
	}
	return segments, versions, nil
}

// decode returns the segment a record stored at version holds: cached when
// it is the value of that version, otherwise the record stitched with its
// binlog KVs.
func (s *casSegmentStore) decode(ctx context.Context, record *datapb.SegmentInfo, version int64, cached *SegmentInfo, cachedVersion int64) (*SegmentInfo, error) {
	if cached != nil && version == cachedVersion {
		return cached, nil
	}
	if datacoordkv.HasInlineBinlogs(record) {
		s.inline.Insert(record.GetID())
	} else {
		s.inline.Remove(record.GetID())
	}
	binlogs, err := s.loadBinlogs(ctx, datacoordkv.SegmentBinlogPrefixesOf(record.GetCollectionID(), record.GetPartitionID(), record.GetID()))
	if err != nil {
		return nil, err
	}
	s.codec.DecodeRecord(record)
	if err := datacoordkv.FillSegmentBinlogs([]*datapb.SegmentInfo{record}, binlogs); err != nil {
		return nil, err
	}
	return NewSegmentInfo(record), nil
}

func (s *casSegmentStore) AddSegment(ctx context.Context, segment *datapb.SegmentInfo) (int64, error) {
	b := s.newTxn(ctx, nil)
	if err := b.write(ctx, metastore.ActionAdd, metastore.SegmentEntry{Segment: segment}, false); err != nil {
		return 0, err
	}
	versions, err := s.commit(b)
	if err != nil {
		return 0, err
	}
	return versions[segment.GetID()], nil
}

func (s *casSegmentStore) DropSegment(ctx context.Context, base map[int64]int64, segment *datapb.SegmentInfo) error {
	return s.DropSegments(ctx, base, []*datapb.SegmentInfo{segment})
}

// DropSegments deletes every record against its base version, with its
// binlog KVs. A transaction holds whole segments, as many as the op limit
// takes, so no segment is dropped in part. A version conflict fails its
// transaction and the ones after it.
func (s *casSegmentStore) DropSegments(ctx context.Context, base map[int64]int64, segments []*datapb.SegmentInfo) error {
	perTxn := len(segments)
	if limit := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.GetAsInt(); limit > 0 {
		perTxn = max(1, limit/(1+len(datacoordkv.SegmentBinlogPrefixes())))
	}
	for _, chunk := range lo.Chunk(segments, max(1, perTxn)) {
		b := s.newTxn(ctx, base)
		for _, segment := range chunk {
			b.drop(segment)
		}
		if _, err := s.commit(b); err != nil {
			return err
		}
	}
	return nil
}

// AlterSegments writes the segments in the catalog's AlterSegments encoding:
// each record plus the binlog increments of its segment.
func (s *casSegmentStore) AlterSegments(ctx context.Context, base map[int64]int64, segments []*datapb.SegmentInfo, binlogs ...metastore.BinlogsIncrement) (map[int64]int64, error) {
	if len(segments) == 0 && len(binlogs) == 0 {
		return map[int64]int64{}, nil
	}
	increments := lo.GroupBy(binlogs, func(b metastore.BinlogsIncrement) int64 { return b.Segment.GetID() })
	b := s.newTxn(ctx, base)
	for _, segment := range retiredLast(segments) {
		entry := metastore.SegmentEntry{Segment: segment, Binlogs: increments[segment.GetID()], AlterEncoding: true}
		delete(increments, segment.GetID())
		if err := b.write(ctx, metastore.ActionUpdate, entry, false); err != nil {
			return nil, err
		}
	}
	// The catalog persists the increments of segments it does not rewrite as
	// bare binlog KVs.
	for _, segmentIncrements := range increments {
		ops, err := datacoordkv.EncodeBinlogIncrements(segmentIncrements)
		if err != nil {
			return nil, err
		}
		b.txn.Group()
		for _, op := range ops {
			b.raw(op)
		}
	}
	return s.commit(b)
}

// SaveDroppedSegmentsInBatch rewrites the records alone, like the catalog.
func (s *casSegmentStore) SaveDroppedSegmentsInBatch(ctx context.Context, base map[int64]int64, segments []*datapb.SegmentInfo) (map[int64]int64, error) {
	if len(segments) == 0 {
		return map[int64]int64{}, nil
	}
	b := s.newTxn(ctx, base)
	for _, segment := range segments {
		if err := b.write(ctx, metastore.ActionUpdate, metastore.SegmentEntry{Segment: segment}, false); err != nil {
			return nil, err
		}
	}
	return s.commit(b)
}

// Update commits a composite write in one versioned transaction. Segment
// entries become versioned puts of their records with the binlog KVs of the
// catalog encoding; every other action is encoded by the catalog and composed
// as raw ops, in action order, keeping the catalog's commit markers so an
// over-limit write is flushed in the same order as catalog.Update's chunked
// fallback.
func (s *casSegmentStore) Update(ctx context.Context, base map[int64]int64, actions ...metastore.UpdateAction) (map[int64]int64, error) {
	b := s.newTxn(ctx, base)
	for _, action := range actions {
		entry, ok := action.Entry.(metastore.SegmentEntry)
		if !ok {
			if err := s.composeRaw(ctx, b, action); err != nil {
				return nil, err
			}
			continue
		}
		// A first Flush publication must never become visible before its
		// DataView: on the ordered fallback the record lands in the final
		// batch together with the commit-marked DataView.
		if err := b.write(ctx, action.Type, entry, publishesSegmentDataVersion(entry.Segment, actions)); err != nil {
			return nil, err
		}
	}
	if containsSegmentIndexAction(actions) {
		// A segment index removal and the manifest pointer that publishes or
		// retracts its artifact must land together.
		b.txn.AtomicOnly()
	}
	return s.commit(b)
}

// composeRaw appends the catalog encoding of a non-segment action to b.
func (s *casSegmentStore) composeRaw(ctx context.Context, b *segmentTxn, action metastore.UpdateAction) error {
	if s.encoder == nil {
		return merr.WrapErrServiceInternalMsg("segment store: catalog cannot encode %T", action.Entry)
	}
	builder, err := s.encoder.EncodeUpdate(ctx, action)
	if err != nil {
		return err
	}
	if builder.Guarded() {
		return merr.WrapErrServiceInternalMsg("segment store: conditional commit of %T is not supported", action.Entry)
	}
	b.txn.Group()
	for _, op := range builder.Ops() {
		b.raw(op)
	}
	return nil
}

// segmentTxn stages segment writes into a store transaction and remembers the
// op index of every segment record. base holds the version each segment
// record is compared against (see segmentStore).
type segmentTxn struct {
	store     *casSegmentStore
	txn       Txn[string, *datapb.SegmentInfo]
	base      map[int64]int64
	numOps    int
	segments  []*datapb.SegmentInfo
	positions []int
	// dropped marks the segments the transaction removes.
	dropped []bool
}

func (s *casSegmentStore) newTxn(ctx context.Context, base map[int64]int64) *segmentTxn {
	return &segmentTxn{store: s, txn: s.persist.Txn(ctx), base: base}
}

func (b *segmentTxn) raw(op txn.Op) {
	b.txn.Raw(RawOp{Key: b.store.join(op.Key), Value: op.Value, Delete: op.Delete, DeletePrefix: op.DeletePrefix, Commit: op.Commit})
	b.numOps++
}

func (b *segmentTxn) track(segment *datapb.SegmentInfo, dropped bool) {
	b.segments = append(b.segments, segment)
	b.positions = append(b.positions, b.numOps)
	b.dropped = append(b.dropped, dropped)
	b.numOps++
}

// write stages a segment action in the catalog encoding: the binlog KVs, then
// the versioned record, as one group, so on the ordered fallback they land in
// one batch and a record never lands ahead of its binlogs. Binlog KVs that do
// land ahead of their record (a segment whose ops exceed the batch limit on
// their own, or a commit-marked record) land in batches that compare the
// record's version and move it (see planBatches). A record still at the
// version of a cached value thus has the binlogs of that value (see
// segmentStore.ReadSegment).
//
// A segment whose persisted record still carries its binlogs gets every
// binlog family written out (keeping the caller's field removals): the record
// this write stores no longer holds them.
func (b *segmentTxn) write(ctx context.Context, t metastore.ActionType, e metastore.SegmentEntry, commit bool) error {
	if e.Segment != nil && b.store.inline.Contain(e.Segment.GetID()) {
		all := metastore.BinlogsIncrement{Segment: e.Segment}
		for _, increment := range e.Binlogs {
			all.DroppedBinlogFieldIDs = append(all.DroppedBinlogFieldIDs, increment.DroppedBinlogFieldIDs...)
		}
		e.Binlogs = []metastore.BinlogsIncrement{all}
		e.AlterEncoding = true
	}
	w, err := b.store.codec.EncodeAction(ctx, t, e)
	if err != nil {
		return err
	}
	b.txn.Group()
	for _, op := range w.Ops {
		b.raw(op)
	}
	b.txn.PutIfVersion(b.store.join(w.RecordKey), w.Record, b.base[e.Segment.GetID()], commit)
	b.track(e.Segment, false)
	return nil
}

// drop stages the removal of a segment, as one group: its versioned record
// first, then its binlog KVs, so on the ordered fallback the record never
// outlives them.
func (b *segmentTxn) drop(segment *datapb.SegmentInfo) {
	b.txn.Group()
	b.txn.DeleteIfVersion(b.store.join(datacoordkv.SegmentRecordKey(segment)), b.base[segment.GetID()])
	b.track(segment, true)
	for _, prefix := range datacoordkv.SegmentBinlogPrefixesOf(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID()) {
		b.raw(txn.Op{Key: prefix, DeletePrefix: true})
	}
}

// commit runs the transaction of b and maps the result of each segment record
// to the segment's version. A write that conflicts is built again from the
// stored values (see meta.retrySegmentWrite). A transaction split over the op
// limit that fails after its first batch reports the segments it stored with
// the error (see partialWriteError).
func (s *casSegmentStore) commit(b *segmentTxn) (map[int64]int64, error) {
	results, err := b.txn.Commit()
	if err != nil {
		if results == nil {
			return nil, err
		}
		var landed []landedSegment
		for i, segment := range b.segments {
			if version := results[b.positions[i]].Version; version > 0 && !b.dropped[i] {
				landed = append(landed, landedSegment{segment: segment, version: version})
				s.inline.Remove(segment.GetID())
			}
		}
		if len(landed) == 0 {
			return nil, err
		}
		return nil, &partialWriteError{err: err, landed: landed}
	}
	versions := make(map[int64]int64, len(b.segments))
	for i, segment := range b.segments {
		versions[segment.GetID()] = results[b.positions[i]].Version
		s.inline.Remove(segment.GetID())
	}
	return versions, nil
}

// partialWriteError reports a segment write that failed after part of it was
// stored: a transaction split over the op limit landed some batches before
// one failed. landed holds the segments those batches stored, with their new
// versions, for the cache to catch up with (see meta.publishLanded).
type partialWriteError struct {
	err    error
	landed []landedSegment
}

// landedSegment is a segment value a failed write stored, at version.
type landedSegment struct {
	segment *datapb.SegmentInfo
	version int64
}

func (e *partialWriteError) Error() string { return e.err.Error() }

func (e *partialWriteError) Unwrap() error { return e.err }

// publishesSegmentDataVersion identifies a first Flush publication: the
// segment carries the DataVersion of a DataView written by the same action set.
func publishesSegmentDataVersion(segment *datapb.SegmentInfo, actions []metastore.UpdateAction) bool {
	if segment.GetSealedAtDataVersion() == nil {
		return false
	}
	for _, action := range actions {
		if entry, ok := action.Entry.(metastore.DataViewEntry); ok &&
			entry.DataView.GetCollectionId() == segment.GetCollectionID() &&
			proto.Equal(entry.DataView.GetDataVersion(), segment.GetSealedAtDataVersion()) {
			return true
		}
	}
	return false
}

func containsSegmentIndexAction(actions []metastore.UpdateAction) bool {
	for _, action := range actions {
		if _, ok := action.Entry.(metastore.SegmentIndexEntry); ok {
			return true
		}
	}
	return false
}

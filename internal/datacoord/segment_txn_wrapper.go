package datacoord

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	datacoordkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/util/segmentutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

// BinlogIncrement names the FieldBinlogs that an Update writes as side-prefix
// KVs. Each FieldBinlog carries its full current content (the same convention
// BuildBinlogKvsWithLogID expects). Unchanged fields are omitted; their
// side-prefix KVs stay as-is. An empty increment is a state-only update.
type BinlogIncrement struct {
	Binlogs               []*datapb.FieldBinlog
	Deltalogs             []*datapb.FieldBinlog
	Statslogs             []*datapb.FieldBinlog
	Bm25Statslogs         []*datapb.FieldBinlog
	DroppedBinlogFieldIDs []int64
}

// IsEmpty reports whether the increment contains no FieldBinlogs.
func (i BinlogIncrement) IsEmpty() bool {
	return len(i.Binlogs) == 0 && len(i.Deltalogs) == 0 &&
		len(i.Statslogs) == 0 && len(i.Bm25Statslogs) == 0 &&
		len(i.DroppedBinlogFieldIDs) == 0
}

// Union merges another increment into this one. Caller is responsible for
// de-duplication (matching fieldID → last write wins, same as map ordering).
func (i *BinlogIncrement) Union(o BinlogIncrement) {
	i.Binlogs = append(i.Binlogs, o.Binlogs...)
	i.Deltalogs = append(i.Deltalogs, o.Deltalogs...)
	i.Statslogs = append(i.Statslogs, o.Statslogs...)
	i.Bm25Statslogs = append(i.Bm25Statslogs, o.Bm25Statslogs...)
	i.DroppedBinlogFieldIDs = append(i.DroppedBinlogFieldIDs, o.DroppedBinlogFieldIDs...)
}

// SegmentTxnWrapper adapts the bytes-only OptimisticTxnPersist to segment-typed
// operations. It owns SegmentInfo marshaling and the "strip binlog fields from
// the segment proto + carry binlog KVs as sibling writes" wire-compat policy,
// so every persisted segment record matches the legacy on-disk shape: binlogs
// live as separate KVs under their side prefixes, and the segment proto is
// stored without them.
//
// Writers pass the fully-stitched in-memory SegmentInfo (the way it looks with
// binlogs populated from a cache lookup). The wrapper strips the proto and
// stages binlog KVs before the main segment record in the same ordered logical
// write. The underlying persist may split that write into bounded atomic
// batches, so callers must handle ErrPartialCommit.
type SegmentTxnWrapper struct {
	inner        OptimisticTxnPersist
	metaRootPath string
}

func NewSegmentTxnWrapper(inner OptimisticTxnPersist) *SegmentTxnWrapper {
	return &SegmentTxnWrapper{inner: inner}
}

func (w *SegmentTxnWrapper) WithMetaRootPath(metaRootPath string) *SegmentTxnWrapper {
	cloned := *w
	cloned.metaRootPath = strings.TrimSuffix(metaRootPath, "/")
	return &cloned
}

func (w *SegmentTxnWrapper) Txn(ctx context.Context) *SegmentTxn {
	return &SegmentTxn{inner: w.inner.Txn(ctx), metaRootPath: w.metaRootPath}
}

// Scan reads all segment records under the given prefix and returns them
// unmarshaled. Binlog fields are NOT stitched here — callers that need
// stitched binlogs do that separately via meta.reloadFromKV.
func (w *SegmentTxnWrapper) Scan(ctx context.Context, prefix string) ([]*datapb.SegmentInfo, []int64, error) {
	_, values, versions, err := w.inner.Scan(ctx, prefix)
	if err != nil {
		return nil, nil, err
	}
	segments := make([]*datapb.SegmentInfo, 0, len(values))
	keptVers := make([]int64, 0, len(values))
	for i, v := range values {
		seg := &datapb.SegmentInfo{}
		if err := proto.Unmarshal(v, seg); err != nil {
			return nil, nil, merr.WrapErrDataIntegrity(err, "unmarshal SegmentInfo")
		}
		segments = append(segments, seg)
		keptVers = append(keptVers, versions[i])
	}
	return segments, keptVers, nil
}

// ScanRaw exposes the underlying bytes-only scan for callers that need to read
// non-segment data under a prefix (e.g. legacy binlog KVs during reloadFromKV).
func (w *SegmentTxnWrapper) ScanRaw(ctx context.Context, prefix string) (keys []string, values [][]byte, versions []int64, err error) {
	return w.inner.Scan(ctx, prefix)
}

// SegmentTxn accepts typed SegmentInfo operations. For each operation it
// stages side-prefix binlog KVs before the main segment record, so a committed
// prefix never publishes a new segment record before its auxiliary writes.
type SegmentTxn struct {
	inner        Txn
	metaRootPath string
	// mainIdx records, for each typed segment op, the index of its main op
	// inside inner. Binlog Put/Remove ops take the slots in between; the
	// mapping lets Commit return one result per typed op in add order.
	mainIdx      []int
	mainSegments []*datapb.SegmentInfo
	count        int
}

// SegmentTxnResult is one entry per typed segment op, in add order.
type SegmentTxnResult struct {
	Segment *datapb.SegmentInfo
	Version int64
}

// Insert stages a new segment record plus every side-prefix binlog KV derived
// from seg's binlog fields. Fails the commit
// (ErrKeyAlreadyExists) if the segment key is already present.
func (t *SegmentTxn) Insert(key string, seg *datapb.SegmentInfo) error {
	value, binlogKvs, removals, err := t.buildSegmentWrite(seg, BinlogIncrement{
		Binlogs:       seg.GetBinlogs(),
		Deltalogs:     seg.GetDeltalogs(),
		Statslogs:     seg.GetStatslogs(),
		Bm25Statslogs: seg.GetBm25Statslogs(),
	})
	if err != nil {
		return err
	}
	for k, v := range binlogKvs {
		t.inner.Put(k, v)
		t.recordAux()
	}
	for _, k := range removals {
		t.inner.Remove(k)
		t.recordAux()
	}
	t.inner.Insert(key, value)
	t.recordMain(seg)
	return nil
}

// Update stages an overwrite of a segment record plus an explicit list of
// binlog KVs to rewrite, CAS-gated by expectedVersion (the etcd
// ModRevision the caller read when staging). On version mismatch, Commit
// returns ErrCASFailed; callers retry by re-reading the cache entry and
// re-staging.
//
// inc carries the FieldBinlogs the caller explicitly wants persisted — each
// one's full current content will be Put under its side-prefix key. Unchanged
// FieldBinlogs aren't in inc and their KVs aren't touched. An empty inc is a
// state-only update that rewrites only the segment record.
//
// seg MUST be the fully-stitched post-mutation SegmentInfo (for the segment
// record write); binlog fields in seg are stripped from the persisted proto.
func (t *SegmentTxn) Update(key string, seg *datapb.SegmentInfo, expectedVersion int64, inc BinlogIncrement) error {
	// A legacy segment can still have its binlogs embedded in the segment record
	// and no side-prefix KVs after a rolling upgrade. Preserve Catalog's
	// write-on-drop compatibility behavior so rewriting the stripped Dropped
	// record never makes those logs unreachable to GC.
	if seg.GetState() == commonpb.SegmentState_Dropped && seg.GetManifestPath() == "" {
		inc.Binlogs = seg.GetBinlogs()
		inc.Deltalogs = seg.GetDeltalogs()
		inc.Statslogs = seg.GetStatslogs()
		inc.Bm25Statslogs = seg.GetBm25Statslogs()
	}
	value, binlogKvs, removals, err := t.buildSegmentWrite(seg, inc)
	if err != nil {
		return err
	}
	for k, v := range binlogKvs {
		t.inner.Put(k, v)
		t.recordAux()
	}
	for _, k := range removals {
		t.inner.Remove(k)
		t.recordAux()
	}
	t.inner.Update(key, value, expectedVersion)
	t.recordMain(seg)
	return nil
}

// Delete stages removal of the segment record and its binlog KVs. Fails
// (ErrKeyNotFound) if the segment key is missing.
func (t *SegmentTxn) Delete(key string, seg *datapb.SegmentInfo) {
	for _, k := range t.segmentBinlogKeys(seg) {
		t.inner.Remove(k)
		t.recordAux()
	}
	t.inner.Delete(key)
	t.recordMain(nil)
}

// RawTxn returns the underlying bytes-only transaction for call sites that
// need to stage non-segment operations (e.g. channel CP writes) alongside
// segment ops in the same ordered logical commit.
func (t *SegmentTxn) RawTxn() Txn { return t.inner }

// Commit executes the underlying logical write. Returned results are in add
// order of the typed ops; binlog Put/Remove ops are not reported. On
// ErrPartialCommit, results still preserve typed-op order; a main segment
// record outside the committed prefix has Version == 0. Segment results keep
// the fully stitched SegmentInfo supplied to Insert/Update rather than the
// stripped proto persisted in the main key.
func (t *SegmentTxn) Commit() ([]SegmentTxnResult, error) {
	raws, err := t.inner.Commit()
	if err != nil && !errors.Is(err, ErrPartialCommit) {
		return nil, err
	}
	results := make([]SegmentTxnResult, 0, len(t.mainIdx))
	for i, idx := range t.mainIdx {
		if idx >= len(raws) {
			break
		}
		r := raws[idx]
		out := SegmentTxnResult{Version: r.Version}
		if t.mainSegments[i] != nil {
			out.Segment = proto.Clone(t.mainSegments[i]).(*datapb.SegmentInfo)
		}
		results = append(results, out)
	}
	return results, err
}

func (t *SegmentTxn) recordMain(seg *datapb.SegmentInfo) {
	t.mainIdx = append(t.mainIdx, t.count)
	if seg == nil {
		t.mainSegments = append(t.mainSegments, nil)
	} else {
		t.mainSegments = append(t.mainSegments, proto.Clone(seg).(*datapb.SegmentInfo))
	}
	t.count++
}

func (t *SegmentTxn) recordAux() { t.count++ }

// buildSegmentWrite produces the stripped segment proto bytes plus one
// side-prefix KV per FieldBinlog in the increment. Each passed FieldBinlog is
// persisted verbatim under its side-prefix key.
func (t *SegmentTxn) buildSegmentWrite(seg *datapb.SegmentInfo, inc BinlogIncrement) ([]byte, map[string][]byte, []string, error) {
	stripped := proto.Clone(seg).(*datapb.SegmentInfo)
	datacoordkv.ResetBinlogFields(stripped)
	if seg.GetManifestPath() == "" {
		segmentutil.ReCalcRowCount(seg, stripped)
	}
	// Match Catalog's compact segment-record wire format. Runtime paths are
	// reconstructed after reload; persisting absolute paths here would restore
	// the metadata amplification this wrapper is meant to avoid.
	metautil.ExtractTextLogFilenames(stripped.GetTextStatsLogs())
	metautil.ExtractJSONKeyStatsRelativePaths(stripped.GetJsonKeyStats())
	value, err := proto.Marshal(stripped)
	if err != nil {
		return nil, nil, nil, merr.WrapErrSerializationFailed(err, "marshal SegmentInfo")
	}
	// Manifest-backed V3 segments persist their log paths in LOON. Keep the
	// existing catalog contract: their SegmentInfo record is still stripped,
	// but no legacy per-FieldBinlog side-prefix KVs are written or removed.
	if seg.GetManifestPath() != "" || inc.IsEmpty() {
		return value, nil, nil, nil
	}
	removals := t.segmentDroppedBinlogKeys(seg, inc.DroppedBinlogFieldIDs)
	kvs, err := datacoordkv.BuildBinlogKvsWithLogID(
		seg.GetCollectionID(), seg.GetPartitionID(), seg.GetID(),
		datacoordkv.CloneLogs(inc.Binlogs),
		datacoordkv.CloneLogs(inc.Deltalogs),
		datacoordkv.CloneLogs(inc.Statslogs),
		datacoordkv.CloneLogs(inc.Bm25Statslogs),
	)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(kvs) == 0 {
		return value, nil, removals, nil
	}
	binlogPuts := make(map[string][]byte, len(kvs))
	for k, v := range kvs {
		binlogPuts[t.joinMetaRootPath(k)] = []byte(v)
	}
	return value, binlogPuts, removals, nil
}

func (t *SegmentTxn) segmentDroppedBinlogKeys(seg *datapb.SegmentInfo, fieldIDs []int64) []string {
	keys := make([]string, 0, len(fieldIDs))
	for _, fieldID := range fieldIDs {
		keys = append(keys, t.joinMetaRootPath(fmt.Sprintf("%s/%d/%d/%d/%d",
			datacoordkv.SegmentBinlogPathPrefix,
			seg.GetCollectionID(),
			seg.GetPartitionID(),
			seg.GetID(),
			fieldID)))
	}
	return keys
}

// segmentBinlogKeys enumerates every side-prefix binlog KV key for the segment.
func (t *SegmentTxn) segmentBinlogKeys(seg *datapb.SegmentInfo) []string {
	keys := make([]string, 0)
	add := func(prefix string, logs []*datapb.FieldBinlog) {
		for _, fb := range logs {
			keys = append(keys, t.joinMetaRootPath(fmt.Sprintf("%s/%d/%d/%d/%d",
				prefix, seg.GetCollectionID(), seg.GetPartitionID(), seg.GetID(), fb.GetFieldID())))
		}
	}
	add(datacoordkv.SegmentBinlogPathPrefix, seg.GetBinlogs())
	add(datacoordkv.SegmentDeltalogPathPrefix, seg.GetDeltalogs())
	add(datacoordkv.SegmentStatslogPathPrefix, seg.GetStatslogs())
	add(datacoordkv.SegmentBM25logPathPrefix, seg.GetBm25Statslogs())
	return keys
}

func (t *SegmentTxn) joinMetaRootPath(key string) string {
	if t.metaRootPath == "" {
		return key
	}
	return t.metaRootPath + "/" + key
}

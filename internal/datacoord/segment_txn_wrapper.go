package datacoord

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	datacoordkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
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
// emits binlog KVs in the same atomic backend transaction.
type SegmentTxnWrapper struct {
	inner OptimisticTxnPersist
}

func NewSegmentTxnWrapper(inner OptimisticTxnPersist) *SegmentTxnWrapper {
	return &SegmentTxnWrapper{inner: inner}
}

func (w *SegmentTxnWrapper) Txn(ctx context.Context) *SegmentTxn {
	return &SegmentTxn{inner: w.inner.Txn(ctx)}
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
			return nil, nil, fmt.Errorf("unmarshal SegmentInfo: %w", err)
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

// SegmentTxn accepts typed SegmentInfo ops. On Commit, the stripped segment
// record and all its binlog KVs persist atomically in a single backend txn.
type SegmentTxn struct {
	inner Txn
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

// Insert stages an atomic write of a new segment record plus every side-prefix
// binlog KV derived from seg's binlog fields. Fails the commit
// (ErrKeyAlreadyExists) if the segment key is already present.
func (t *SegmentTxn) Insert(key string, seg *datapb.SegmentInfo) error {
	value, binlogKvs, removals, err := buildSegmentWrite(seg, BinlogIncrement{
		Binlogs:       seg.GetBinlogs(),
		Deltalogs:     seg.GetDeltalogs(),
		Statslogs:     seg.GetStatslogs(),
		Bm25Statslogs: seg.GetBm25Statslogs(),
	})
	if err != nil {
		return err
	}
	t.inner.Insert(key, value)
	t.recordMain(seg)
	for k, v := range binlogKvs {
		t.inner.Put(k, v)
		t.recordAux()
	}
	for _, k := range removals {
		t.inner.Remove(k)
		t.recordAux()
	}
	return nil
}

// Update stages an atomic overwrite of a segment record plus an explicit list
// of binlog KVs to rewrite, CAS-gated by expectedVersion (the etcd
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
	value, binlogKvs, removals, err := buildSegmentWrite(seg, inc)
	if err != nil {
		return err
	}
	t.inner.Update(key, value, expectedVersion)
	t.recordMain(seg)
	for k, v := range binlogKvs {
		t.inner.Put(k, v)
		t.recordAux()
	}
	for _, k := range removals {
		t.inner.Remove(k)
		t.recordAux()
	}
	return nil
}

// Delete removes the segment record and its binlog KVs atomically.
// Fails (ErrKeyNotFound) if the segment key is missing.
func (t *SegmentTxn) Delete(key string, seg *datapb.SegmentInfo) {
	t.inner.Delete(key)
	t.recordMain(nil)
	for _, k := range segmentBinlogKeys(seg) {
		t.inner.Remove(k)
		t.recordAux()
	}
}

// RawTxn returns the underlying bytes-only transaction for call sites that
// need to stage non-segment operations (e.g. channel CP writes) alongside
// segment ops in the same atomic commit.
func (t *SegmentTxn) RawTxn() Txn { return t.inner }

// Commit executes the underlying atomic transaction. Returned results are in
// add order of the typed ops; binlog Put/Remove ops are not reported. Segment
// results keep the fully stitched SegmentInfo supplied to Insert/Update rather
// than the stripped proto persisted in the main segment key.
func (t *SegmentTxn) Commit() ([]SegmentTxnResult, error) {
	raws, err := t.inner.Commit()
	if err != nil {
		return nil, err
	}
	results := make([]SegmentTxnResult, 0, len(t.mainIdx))
	for i, idx := range t.mainIdx {
		r := raws[idx]
		out := SegmentTxnResult{Version: r.Version}
		if t.mainSegments[i] != nil {
			out.Segment = proto.Clone(t.mainSegments[i]).(*datapb.SegmentInfo)
		}
		results = append(results, out)
	}
	return results, nil
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
func buildSegmentWrite(seg *datapb.SegmentInfo, inc BinlogIncrement) ([]byte, map[string][]byte, []string, error) {
	stripped := proto.Clone(seg).(*datapb.SegmentInfo)
	datacoordkv.ResetBinlogFields(stripped)
	value, err := proto.Marshal(stripped)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("marshal SegmentInfo: %w", err)
	}
	if inc.IsEmpty() {
		return value, nil, nil, nil
	}
	removals := segmentDroppedBinlogKeys(seg, inc.DroppedBinlogFieldIDs)
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
		binlogPuts[k] = []byte(v)
	}
	return value, binlogPuts, removals, nil
}

func segmentDroppedBinlogKeys(seg *datapb.SegmentInfo, fieldIDs []int64) []string {
	keys := make([]string, 0, len(fieldIDs))
	for _, fieldID := range fieldIDs {
		keys = append(keys, fmt.Sprintf("%s/%d/%d/%d/%d",
			datacoordkv.SegmentBinlogPathPrefix,
			seg.GetCollectionID(),
			seg.GetPartitionID(),
			seg.GetID(),
			fieldID))
	}
	return keys
}

// segmentBinlogKeys enumerates every side-prefix binlog KV key for the segment.
func segmentBinlogKeys(seg *datapb.SegmentInfo) []string {
	keys := make([]string, 0)
	add := func(prefix string, logs []*datapb.FieldBinlog) {
		for _, fb := range logs {
			keys = append(keys, fmt.Sprintf("%s/%d/%d/%d/%d",
				prefix, seg.GetCollectionID(), seg.GetPartitionID(), seg.GetID(), fb.GetFieldID()))
		}
	}
	add(datacoordkv.SegmentBinlogPathPrefix, seg.GetBinlogs())
	add(datacoordkv.SegmentDeltalogPathPrefix, seg.GetDeltalogs())
	add(datacoordkv.SegmentStatslogPathPrefix, seg.GetStatslogs())
	add(datacoordkv.SegmentBM25logPathPrefix, seg.GetBm25Statslogs())
	return keys
}

package datacoord

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tikv"
)

const testSegmentStoreRoot = "by-dev/meta"

// newCASTestMeta builds a meta whose segments live in an in-memory optimistic
// persist, so writes go through casSegmentStore.
func newCASTestMeta(t *testing.T) (*meta, *memPersist[string, *datapb.SegmentInfo]) {
	persist := NewOptimisticTxnMemoryPersist[string, *datapb.SegmentInfo](&SegmentInfoMarshaler{}).(*memPersist[string, *datapb.SegmentInfo])
	m := &meta{
		ctx:      context.Background(),
		catalog:  datacoord.NewCatalog(NewMetaMemoryKV(), "", ""),
		segments: NewSegmentsInfo(metacache.NewMetaStore(nil)),
	}
	m.segmentStore = newCASSegmentStore(persist, m.catalog, testSegmentStoreRoot)
	return m, persist
}

func newFlushedTestSegment(id int64) *SegmentInfo {
	return NewSegmentInfo(&datapb.SegmentInfo{
		ID:            id,
		CollectionID:  1,
		PartitionID:   2,
		InsertChannel: "ch-1",
		State:         commonpb.SegmentState_Flushed,
		NumOfRows:     10,
	})
}

func segmentRecordKey(collectionID, partitionID, segmentID int64) string {
	return datacoord.SegmentRecordKey(&datapb.SegmentInfo{ID: segmentID, CollectionID: collectionID, PartitionID: partitionID})
}

func storedSegmentKey(id int64) string {
	return testSegmentStoreRoot + "/" + segmentRecordKey(1, 2, id)
}

// cachedVersion is the store version of the cached value of a segment.
func cachedVersion(m *meta, segmentID int64) (int64, bool) {
	return m.segments.store.SegmentVersion(segmentID)
}

// cacheReads is the reader of the first run of a segment write.
func cacheReads(m *meta) *segmentReader {
	return &segmentReader{m: m, ctx: context.Background(), reads: make(map[int64]segmentRead)}
}

// newCASTestMetaOn builds another meta over persist, the store of m, with a
// cache recovered from it: another DataCoord writing the same store, whose
// writes the cache of m misses.
func newCASTestMetaOn(t *testing.T, m *meta, persist OptimisticTxnPersist[string, *datapb.SegmentInfo]) *meta {
	other := &meta{
		ctx:      context.Background(),
		catalog:  m.catalog,
		segments: NewSegmentsInfo(metacache.NewMetaStore(nil)),
	}
	other.segmentStore = newCASSegmentStore(persist, m.catalog, testSegmentStoreRoot)
	segments, versions, err := other.store().ListSegments(context.Background(), 1)
	require.NoError(t, err)
	for i, segment := range segments {
		other.segments.SetSegment(segment.GetID(), NewSegmentInfo(segment), versions[i])
	}
	return other
}

func TestOptimisticTxnPutIfVersion(t *testing.T) {
	ctx := context.Background()
	persist := NewOptimisticTxnMemoryPersist[string, *datapb.SegmentInfo](&SegmentInfoMarshaler{})

	txn := persist.Txn(ctx)
	txn.PutIfVersion("k", &datapb.SegmentInfo{ID: 1}, 0, false)
	results, err := txn.Commit()
	require.NoError(t, err)
	v1 := results[0].Version

	// Version 0 means the key must be absent.
	txn = persist.Txn(ctx)
	txn.PutIfVersion("k", &datapb.SegmentInfo{ID: 1, NumOfRows: 1}, 0, false)
	_, err = txn.Commit()
	require.ErrorIs(t, err, ErrVersionConflict)

	txn = persist.Txn(ctx)
	txn.PutIfVersion("k", &datapb.SegmentInfo{ID: 1, NumOfRows: 1}, v1, false)
	results, err = txn.Commit()
	require.NoError(t, err)
	require.Greater(t, results[0].Version, v1)

	// The old version is stale now.
	txn = persist.Txn(ctx)
	txn.DeleteIfVersion("k", v1)
	_, err = txn.Commit()
	require.ErrorIs(t, err, ErrVersionConflict)
}

func TestOptimisticTxnPlanBatches(t *testing.T) {
	ops := &txnOps[string, *datapb.SegmentInfo]{}
	ops.Raw(RawOp{Key: "a"})
	ops.Raw(RawOp{Key: "marker", Commit: true})
	ops.Raw(RawOp{Key: "b"})
	ops.Raw(RawOp{Key: "c"})

	batches, err := ops.planBatches(10)
	require.NoError(t, err)
	require.Equal(t, [][]int{{0, 1, 2, 3}}, batches)

	// Over the limit: non-commit ops flush first in recorded order, the
	// commit-marked op lands alone in the final batch.
	batches, err = ops.planBatches(2)
	require.NoError(t, err)
	require.Equal(t, [][]int{{0, 2}, {3}, {1}}, batches)

	ops.AtomicOnly()
	_, err = ops.planBatches(2)
	require.Error(t, err)

	// A group is not split unless it exceeds the limit on its own; the next
	// group fills the rest of its last batch.
	grouped := &txnOps[string, *datapb.SegmentInfo]{}
	for _, size := range []int{2, 3, 1} {
		grouped.Group()
		for i := 0; i < size; i++ {
			grouped.Raw(RawOp{Key: fmt.Sprint(len(grouped.ops))})
		}
	}
	batches, err = grouped.planBatches(3)
	require.NoError(t, err)
	require.Equal(t, [][]int{{0, 1}, {2, 3, 4}, {5}}, batches)
	batches, err = grouped.planBatches(2)
	require.NoError(t, err)
	require.Equal(t, [][]int{{0, 1}, {2, 3}, {4, 5}}, batches)
}

func TestCASSegmentStoreWritesCatalogEncodingAgainstCachedVersions(t *testing.T) {
	ctx := context.Background()
	m, persist := newCASTestMeta(t)

	segment := newFlushedTestSegment(100)
	segment.Binlogs = []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []*datapb.Binlog{{LogID: 7, EntriesNum: 10}}}}
	require.NoError(t, m.AddSegment(ctx, segment))
	version, ok := cachedVersion(m, 100)
	require.True(t, ok)
	stored := persist.data[storedSegmentKey(100)]
	require.NotNil(t, stored)
	require.Equal(t, version, stored.version)
	// Like the catalog, the record carries no binlogs: they live in per-field
	// KVs next to it, and the cache keeps the full segment.
	require.Empty(t, stored.value.GetBinlogs())
	binlogKey := testSegmentStoreRoot + "/datacoord-meta/binlog/1/2/100/1"
	require.Contains(t, persist.raw, binlogKey)
	require.Len(t, m.GetSegment(ctx, 100).GetBinlogs(), 1)

	require.NoError(t, m.UpdateSegmentsInfo(ctx, UpdateStatusOperator(100, commonpb.SegmentState_Dropped)))
	newVersion, ok := cachedVersion(m, 100)
	require.True(t, ok)
	require.Greater(t, newVersion, version)
	require.Equal(t, commonpb.SegmentState_Dropped, persist.data[storedSegmentKey(100)].value.GetState())
	require.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(ctx, 100).GetState())

	require.NoError(t, m.DropSegment(ctx, 100))
	require.Nil(t, m.GetSegment(ctx, 100))
	require.NotContains(t, persist.data, storedSegmentKey(100))
	require.NotContains(t, persist.raw, binlogKey, "a drop removes the binlog KVs with the record")
}

// A write that conflicts with a value another writer stored but has not
// published yet runs again on the stored value: it does not wait for the
// publication, and the late publication does not move the cache back.
func TestSegmentWriteReadsTheStoreAfterAConflict(t *testing.T) {
	ctx := context.Background()
	m, persist := newCASTestMeta(t)
	hooked := &hookPersist{OptimisticTxnPersist: persist}
	m.segmentStore = newCASSegmentStore(hooked, m.catalog, testSegmentStoreRoot)
	require.NoError(t, m.AddSegment(ctx, newFlushedTestSegment(100)))

	hooked.afterCommit = func() {
		// The first write is stored and not published yet.
		require.NoError(t, m.UpdateSegment(100, SetMaxRowCount(42)))
	}
	require.NoError(t, m.UpdateSegmentsInfo(ctx, MutationsOperator(map[int64][]MutateFunc{
		100: {func(seg *datapb.SegmentInfo) bool {
			seg.IsSorted = true
			return true
		}},
	})))
	for _, segment := range []*datapb.SegmentInfo{m.GetSegment(ctx, 100).SegmentInfo, persist.data[storedSegmentKey(100)].value} {
		require.True(t, segment.GetIsSorted())
		require.EqualValues(t, 42, segment.GetMaxRowNum())
	}
	version, _ := cachedVersion(m, 100)
	require.Equal(t, persist.data[storedSegmentKey(100)].version, version)
}

// A write whose cached value misses binlogs another DataCoord stored conflicts,
// reads the segment with its binlog KVs from the store, and builds on them: it
// does not write the binlogs of its stale value back.
func TestSegmentWriteBuildsOnTheBinlogsTheCacheMissed(t *testing.T) {
	ctx := context.Background()
	m, persist := newCASTestMeta(t)
	binlog := func(logID int64) []*datapb.FieldBinlog {
		return []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []*datapb.Binlog{{LogID: logID, EntriesNum: 1}}}}
	}
	segment := newFlushedTestSegment(100)
	segment.Binlogs = binlog(1)
	require.NoError(t, m.AddSegment(ctx, segment))

	other := newCASTestMetaOn(t, m, persist)
	require.NoError(t, other.UpdateSegmentsInfo(ctx, AddBinlogsOperator(100, binlog(2), nil, nil, nil)))
	require.NoError(t, m.UpdateSegmentsInfo(ctx, AddBinlogsOperator(100, binlog(3), nil, nil, nil)))

	require.Equal(t, []int64{1, 2, 3}, logIDs(m.GetSegment(ctx, 100).GetBinlogs()))
	recovered := newCASTestMetaOn(t, m, persist)
	require.Equal(t, []int64{1, 2, 3}, logIDs(recovered.GetSegment(ctx, 100).GetBinlogs()))
}

// Two writes that change only the binlogs of a segment store equal records:
// the catalog encoding keeps binlogs out of the record. The write that
// conflicts must not take the other's record for its own earlier attempt, or
// it would overwrite the other's binlogs.
func TestSegmentWriteDoesNotTakeAnEqualRecordForItsOwn(t *testing.T) {
	ctx := context.Background()
	persist, cli, root := newTestEtcdPersist(t)
	m := &meta{
		ctx:      ctx,
		catalog:  datacoord.NewCatalog(NewMetaMemoryKV(), "", ""),
		segments: NewSegmentsInfo(metacache.NewMetaStore(nil)),
	}
	m.segmentStore = newCASSegmentStore(persist, m.catalog, root)
	binlog := func(logID int64) []*datapb.FieldBinlog {
		return []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []*datapb.Binlog{{LogID: logID, EntriesNum: 1}}}}
	}
	segment := newFlushedTestSegment(100)
	segment.Binlogs = binlog(1)
	require.NoError(t, m.AddSegment(ctx, segment))

	other := &meta{ctx: ctx, catalog: m.catalog, segments: NewSegmentsInfo(metacache.NewMetaStore(nil))}
	other.segmentStore = newCASSegmentStore(persist, m.catalog, root)
	segments, versions, err := other.store().ListSegments(ctx, 1)
	require.NoError(t, err)
	for i, stored := range segments {
		other.segments.SetSegment(stored.GetID(), NewSegmentInfo(stored), versions[i])
	}
	require.NoError(t, other.UpdateSegmentsInfo(ctx, AddBinlogsOperator(100, binlog(2), nil, nil, nil)))
	require.NoError(t, m.UpdateSegmentsInfo(ctx, AddBinlogsOperator(100, binlog(3), nil, nil, nil)))

	require.Equal(t, []int64{1, 2, 3}, logIDs(m.GetSegment(ctx, 100).GetBinlogs()))
	stored, _, err := other.store().ListSegments(ctx, 1)
	require.NoError(t, err)
	require.Len(t, stored, 1)
	require.Equal(t, []int64{1, 2, 3}, logIDs(stored[0].GetBinlogs()))
	_ = cli
}

// A write runs again on a value that may already hold its binlogs (a commit
// that stored part of itself before it failed, or whose response was lost):
// adding them again must not list them twice.
func TestAddBinlogsOperatorSkipsTheBinlogsTheSegmentLists(t *testing.T) {
	ctx := context.Background()
	m, _ := newCASTestMeta(t)
	segment := newFlushedTestSegment(100)
	segment.State = commonpb.SegmentState_Growing
	require.NoError(t, m.AddSegment(ctx, segment))
	binlogs := []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []*datapb.Binlog{{LogID: 7, EntriesNum: 1}}}}

	for range 2 {
		require.NoError(t, m.UpdateSegmentsInfo(ctx, AddBinlogsOperator(100, binlogs, nil, binlogs, nil)))
	}
	require.Equal(t, []int64{7}, logIDs(m.GetSegment(ctx, 100).GetBinlogs()))
	require.Equal(t, []int64{7}, logIDs(m.GetSegment(ctx, 100).GetDeltalogs()))
}

// The TiKV store keeps the contract of the etcd one: a write compares the
// version it was built from, and a write whose cached value misses binlogs
// another DataCoord stored rebuilds on the stored segment.
func TestCASSegmentStoreOnTiKV(t *testing.T) {
	ctx := context.Background()
	persist := NewOptimisticTxnTiKVPersist[string, *datapb.SegmentInfo](tikv.SetupLocalTxn(), &SegmentInfoMarshaler{})

	key := testSegmentStoreRoot + "/versioned/1"
	txn := persist.Txn(ctx)
	txn.PutIfVersion(key, &datapb.SegmentInfo{ID: 1}, 0, false)
	results, err := txn.Commit()
	require.NoError(t, err)
	v1 := results[0].Version
	require.Positive(t, v1)

	txn = persist.Txn(ctx)
	txn.PutIfVersion(key, &datapb.SegmentInfo{ID: 1, NumOfRows: 1}, 0, false)
	_, err = txn.Commit()
	require.ErrorIs(t, err, ErrVersionConflict)

	txn = persist.Txn(ctx)
	txn.PutIfVersion(key, &datapb.SegmentInfo{ID: 1, NumOfRows: 2}, v1, false)
	results, err = txn.Commit()
	require.NoError(t, err)
	v2 := results[0].Version
	require.Greater(t, v2, v1)
	// A read reports its snapshot timestamp, which bounds the commit
	// timestamp of the value it read.
	current, err := persist.Get(ctx, []string{key})
	require.NoError(t, err)
	require.GreaterOrEqual(t, current[key].Version, v2)
	require.EqualValues(t, 2, current[key].Value.GetNumOfRows())

	txn = persist.Txn(ctx)
	txn.DeleteIfVersion(key, v1)
	_, err = txn.Commit()
	require.ErrorIs(t, err, ErrVersionConflict)

	m := &meta{
		ctx:      ctx,
		catalog:  datacoord.NewCatalog(NewMetaMemoryKV(), "", ""),
		segments: NewSegmentsInfo(metacache.NewMetaStore(nil)),
	}
	m.segmentStore = newCASSegmentStore(persist, m.catalog, testSegmentStoreRoot)
	binlog := func(logID int64) []*datapb.FieldBinlog {
		return []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []*datapb.Binlog{{LogID: logID, EntriesNum: 1}}}}
	}
	segment := newFlushedTestSegment(100)
	segment.Binlogs = binlog(1)
	require.NoError(t, m.AddSegment(ctx, segment))

	other := newCASTestMetaOn(t, m, persist)
	require.NoError(t, other.UpdateSegmentsInfo(ctx, AddBinlogsOperator(100, binlog(2), nil, nil, nil)))
	require.NoError(t, m.UpdateSegmentsInfo(ctx, AddBinlogsOperator(100, binlog(3), nil, nil, nil)))

	require.Equal(t, []int64{1, 2, 3}, logIDs(m.GetSegment(ctx, 100).GetBinlogs()))
	recovered := newCASTestMetaOn(t, m, persist)
	require.Equal(t, []int64{1, 2, 3}, logIDs(recovered.GetSegment(ctx, 100).GetBinlogs()))

	require.NoError(t, m.DropSegment(ctx, 100))
	require.Nil(t, m.GetSegment(ctx, 100))
	require.Nil(t, newCASTestMetaOn(t, m, persist).GetSegment(ctx, 100))
}

// A store read of a segment whose record is still at the cached version
// returns the cached value without reading the binlog KVs; any other record
// is read with them.
func TestCASSegmentStoreReadSegment(t *testing.T) {
	ctx := context.Background()
	m, _ := newCASTestMeta(t)
	segment := newFlushedTestSegment(100)
	segment.Binlogs = []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []*datapb.Binlog{{LogID: 7, EntriesNum: 10}}}}
	require.NoError(t, m.AddSegment(ctx, segment))
	cached, version := m.segments.GetSegmentWithVersion(100)

	read, readVersion, err := m.store().ReadSegment(ctx, 1, 2, 100, cached, version)
	require.NoError(t, err)
	require.Same(t, cached, read)
	require.Equal(t, version, readVersion)

	read, readVersion, err = m.store().ReadSegment(ctx, 1, 2, 100, cached, version-1)
	require.NoError(t, err)
	require.NotSame(t, cached, read)
	require.Equal(t, version, readVersion)
	require.Equal(t, []int64{7}, logIDs(read.GetBinlogs()))

	read, readVersion, err = m.store().ReadSegment(ctx, 1, 2, 101, nil, 0)
	require.NoError(t, err)
	require.Nil(t, read)
	require.Zero(t, readVersion)
}

// failingGetPersist fails every read of a key of the wrapped persist.
type failingGetPersist struct {
	OptimisticTxnPersist[string, *datapb.SegmentInfo]
	err error
}

func (p *failingGetPersist) Get(context.Context, []string) (map[string]TxnResult[*datapb.SegmentInfo], error) {
	return nil, p.err
}

// A run that fails to read a segment from the store stops with that failure
// and stores nothing.
func TestSegmentWriteStopsAtAFailedStoreRead(t *testing.T) {
	ctx := context.Background()
	m, persist := newCASTestMeta(t)
	require.NoError(t, m.AddSegment(ctx, newFlushedTestSegment(100)))
	other := newCASTestMetaOn(t, m, persist)
	require.NoError(t, other.UpdateSegment(100, SetMaxRowCount(7)))

	expected := errors.New("mock read failure")
	m.segmentStore = newCASSegmentStore(&failingGetPersist{OptimisticTxnPersist: persist, err: expected}, m.catalog, testSegmentStoreRoot)
	require.ErrorIs(t, m.UpdateSegment(100, SetMaxRowCount(42)), expected)
	require.EqualValues(t, 7, persist.data[storedSegmentKey(100)].value.GetMaxRowNum())
}

// AddSegment of a segment the store holds and the cache does not, stored by a
// write whose outcome was lost, installs the stored segment.
func TestAddSegmentInstallsASegmentItFindsStored(t *testing.T) {
	ctx := context.Background()
	m, persist := newCASTestMeta(t)
	other := newCASTestMetaOn(t, m, persist)
	require.NoError(t, other.AddSegment(ctx, newFlushedTestSegment(100)))
	require.Nil(t, m.GetSegment(ctx, 100))

	require.NoError(t, m.AddSegment(ctx, newFlushedTestSegment(100)))
	require.NotNil(t, m.GetSegment(ctx, 100))
	version, _ := cachedVersion(m, 100)
	require.Equal(t, persist.data[storedSegmentKey(100)].version, version)
}

// DropSegment of a segment the store no longer holds, removed by a write whose
// outcome was lost, evicts it from the cache.
func TestDropSegmentEvictsASegmentItFindsRemoved(t *testing.T) {
	ctx := context.Background()
	m, persist := newCASTestMeta(t)
	require.NoError(t, m.AddSegment(ctx, newDroppedTestSegment(100)))
	other := newCASTestMetaOn(t, m, persist)
	require.NoError(t, other.DropSegment(ctx, 100))
	require.NotNil(t, m.GetSegment(ctx, 100))

	require.NoError(t, m.DropSegment(ctx, 100))
	require.Nil(t, m.GetSegment(ctx, 100))
}

func newDroppedTestSegment(id int64) *SegmentInfo {
	segment := newFlushedTestSegment(id)
	segment.State = commonpb.SegmentState_Dropped
	return segment
}

// failingCommitPersist fails every commit of the wrapped persist.
type failingCommitPersist struct {
	OptimisticTxnPersist[string, *datapb.SegmentInfo]
	err error
}

func (p *failingCommitPersist) Txn(ctx context.Context) Txn[string, *datapb.SegmentInfo] {
	return &failingCommitTxn{Txn: p.OptimisticTxnPersist.Txn(ctx), err: p.err}
}

type failingCommitTxn struct {
	Txn[string, *datapb.SegmentInfo]
	err error
}

func (t *failingCommitTxn) Commit() ([]TxnResult[*datapb.SegmentInfo], error) {
	return nil, t.err
}

func TestCASSegmentStoreComposesCatalogActions(t *testing.T) {
	ctx := context.Background()
	m, persist := newCASTestMeta(t)
	require.NoError(t, m.AddSegment(ctx, newFlushedTestSegment(100)))

	dataView := &viewpb.DataViewOfCollection{
		CollectionId: 1,
		DataVersion:  &viewpb.DataVersion{StreamingVersion: 3},
	}
	committed, err := m.UpdateSegmentsInfoAndDataView(ctx, dataView, SetSegmentIsInvisible(100, true))
	require.NoError(t, err)
	require.True(t, committed)
	require.True(t, persist.data[storedSegmentKey(100)].value.GetIsInvisible())

	// The DataView is written by the catalog encoding, under the root path,
	// in the same transaction as the segment record.
	b, err := m.catalog.(catalogUpdateEncoder).EncodeUpdate(ctx, metastore.SaveDataView(dataView))
	require.NoError(t, err)
	require.Len(t, b.Ops(), 1)
	value, ok := persist.raw[testSegmentStoreRoot+"/"+b.Ops()[0].Key]
	require.True(t, ok)
	require.Equal(t, b.Ops()[0].Value, value)
}

func TestCASSegmentStoreKeepsRuntimeFields(t *testing.T) {
	ctx := context.Background()
	m, _ := newCASTestMeta(t)
	require.NoError(t, m.AddSegment(ctx, newFlushedTestSegment(100)))

	exist, canDo := m.CheckAndSetSegmentsCompacting(ctx, []int64{100})
	require.True(t, exist)
	require.True(t, canDo)
	require.NoError(t, m.UpdateSegmentsInfo(ctx, UpdateSegmentLevelOperator(100, datapb.SegmentLevel_L2)))
	require.True(t, m.GetSegment(ctx, 100).isCompacting, "a persisted update must keep the compacting flag")
	require.Equal(t, datapb.SegmentLevel_L2, m.GetSegment(ctx, 100).GetLevel())
}

func TestCASSegmentStoreParallelWritersOfDifferentSegments(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	m, persist := newCASTestMeta(t)
	const n = 32
	for i := int64(0); i < n; i++ {
		require.NoError(t, m.AddSegment(ctx, newFlushedTestSegment(100+i)))
	}
	var wg sync.WaitGroup
	for i := int64(0); i < n; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			for round := 0; round < 5; round++ {
				assert.NoError(t, m.UpdateSegmentsInfo(ctx, MutationsOperator(map[int64][]MutateFunc{
					id: {func(seg *datapb.SegmentInfo) bool {
						seg.NumOfRows++
						return true
					}},
				})))
			}
		}(100 + i)
	}
	wg.Wait()
	for i := int64(0); i < n; i++ {
		require.EqualValues(t, 15, m.GetSegment(ctx, 100+i).GetNumOfRows())
		require.EqualValues(t, 15, persist.data[storedSegmentKey(100+i)].value.GetNumOfRows())
	}
}

func TestMutationsOperatorSkipsRejectedSegments(t *testing.T) {
	ctx := context.Background()
	m, persist := newCASTestMeta(t)
	require.NoError(t, m.AddSegment(ctx, newFlushedTestSegment(100)))
	require.NoError(t, m.AddSegment(ctx, newFlushedTestSegment(101)))
	before := persist.data[storedSegmentKey(101)].version

	require.NoError(t, m.UpdateSegmentsInfo(ctx, MutationsOperator(map[int64][]MutateFunc{
		100: {func(seg *datapb.SegmentInfo) bool { seg.NumOfRows = 99; return true }},
		101: {func(seg *datapb.SegmentInfo) bool { seg.NumOfRows = 99; return false }},
	})))
	require.EqualValues(t, 99, m.GetSegment(ctx, 100).GetNumOfRows())
	require.EqualValues(t, 10, m.GetSegment(ctx, 101).GetNumOfRows())
	require.Equal(t, before, persist.data[storedSegmentKey(101)].version, "a rejected segment is not rewritten")
}

func TestMergeFieldBinlogsDoesNotAliasInput(t *testing.T) {
	incoming := []*datapb.FieldBinlog{
		{FieldID: 1, Binlogs: []*datapb.Binlog{{LogID: 1}}},
		{FieldID: 1, Binlogs: []*datapb.Binlog{{LogID: 2}}},
	}
	first := mergeFieldBinlogs(nil, incoming)
	second := mergeFieldBinlogs(nil, incoming)
	require.Len(t, first, 1)
	require.Len(t, first[0].GetBinlogs(), 2)
	require.Len(t, second[0].GetBinlogs(), 2, "a rerun from the same input must produce the same result")
	require.Len(t, incoming[0].GetBinlogs(), 1, "the input is never modified")
}

func newTestEtcdPersist(t *testing.T) (OptimisticTxnPersist[string, *datapb.SegmentInfo], *clientv3.Client, string) {
	paramtable.Init()
	cli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	require.NoError(t, err)
	root := fmt.Sprintf("/test/optimistic-txn/%s/%d", t.Name(), time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = cli.Delete(context.Background(), root, clientv3.WithPrefix())
		_ = cli.Close()
	})
	return NewOptimisticTxnEtcdPersist[string, *datapb.SegmentInfo](cli, &SegmentInfoMarshaler{}), cli, root
}

func TestOptimisticTxnEtcdVersionedWrites(t *testing.T) {
	ctx := context.Background()
	persist, cli, root := newTestEtcdPersist(t)
	key := root + "/s/1"

	txn := persist.Txn(ctx)
	txn.PutIfVersion(key, &datapb.SegmentInfo{ID: 1}, 0, false)
	txn.Raw(RawOp{Key: root + "/marker", Value: "v1", Commit: true})
	results, err := txn.Commit()
	require.NoError(t, err)
	v1 := results[0].Version
	resp, err := cli.Get(ctx, root+"/marker")
	require.NoError(t, err)
	require.Equal(t, "v1", string(resp.Kvs[0].Value), "raw ops commit in the same transaction")

	// A stale version is a conflict, not a retry loop.
	txn = persist.Txn(ctx)
	txn.PutIfVersion(key, &datapb.SegmentInfo{ID: 1, NumOfRows: 1}, 0, false)
	_, err = txn.Commit()
	require.ErrorIs(t, err, ErrVersionConflict)

	txn = persist.Txn(ctx)
	txn.PutIfVersion(key, &datapb.SegmentInfo{ID: 1, NumOfRows: 2}, v1, false)
	results, err = txn.Commit()
	require.NoError(t, err)
	v2 := results[0].Version
	require.Greater(t, v2, v1)

	current, err := persist.Get(ctx, []string{key})
	require.NoError(t, err)
	require.Equal(t, v2, current[key].Version)
	require.EqualValues(t, 2, current[key].Value.GetNumOfRows())
}

// A versioned write whose response was lost is retried with the same expected
// version; finding its own value in the store, the retry succeeds instead of
// reporting a conflict.
func TestOptimisticTxnEtcdRetryAfterLostResponse(t *testing.T) {
	ctx := context.Background()
	persist, cli, root := newTestEtcdPersist(t)
	key := root + "/s/1"

	txn := persist.Txn(ctx)
	txn.PutIfVersion(key, &datapb.SegmentInfo{ID: 1}, 0, false)
	results, err := txn.Commit()
	require.NoError(t, err)
	v1 := results[0].Version

	value := &datapb.SegmentInfo{ID: 1, NumOfRows: 5}
	retried := persist.Txn(ctx)
	retried.PutIfVersion(key, value, v1, false)
	retried.Raw(RawOp{Key: root + "/marker", Value: "m", Commit: true})
	first, err := retried.Commit()
	require.NoError(t, err)

	again, err := retried.Commit()
	require.NoError(t, err)
	require.Equal(t, first[0].Version, again[0].Version)

	// Its raw ops changed since: the batch is not taken for applied, as the
	// stored versioned values may be another writer's.
	_, err = cli.Delete(ctx, root+"/marker")
	require.NoError(t, err)
	_, err = retried.Commit()
	require.ErrorIs(t, err, ErrVersionConflict)

	// A different value at a newer version is still a conflict.
	conflicting := persist.Txn(ctx)
	conflicting.PutIfVersion(key, &datapb.SegmentInfo{ID: 1, NumOfRows: 6}, v1, false)
	_, err = conflicting.Commit()
	require.ErrorIs(t, err, ErrVersionConflict)
}

func TestOptimisticTxnEtcdOrderedBatches(t *testing.T) {
	ctx := context.Background()
	persist, cli, root := newTestEtcdPersist(t)
	key := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.Key
	paramtable.Get().Save(key, "2")
	defer paramtable.Get().Reset(key)

	txn := persist.Txn(ctx)
	for i := 0; i < 5; i++ {
		txn.PutIfVersion(fmt.Sprintf("%s/s/%d", root, i), &datapb.SegmentInfo{ID: int64(i)}, 0, false)
	}
	txn.Raw(RawOp{Key: root + "/marker", Value: "done", Commit: true})
	results, err := txn.Commit()
	require.NoError(t, err)
	resp, err := cli.Get(ctx, root+"/marker")
	require.NoError(t, err)
	markerRevision := resp.Kvs[0].ModRevision
	for i := 0; i < 5; i++ {
		require.Less(t, results[i].Version, markerRevision, "the commit marker lands after every other op")
	}

	atomic := persist.Txn(ctx)
	for i := 10; i < 13; i++ {
		atomic.PutIfVersion(fmt.Sprintf("%s/s/%d", root, i), &datapb.SegmentInfo{ID: int64(i)}, 0, false)
	}
	atomic.AtomicOnly()
	_, err = atomic.Commit()
	require.Error(t, err)
}

// A write built from a cached value that another writer changes before the
// write commits must not overwrite that change: it conflicts and runs again on
// the refreshed value.
func TestSegmentWriteRebuildsAfterAConcurrentWriteOfTheSegment(t *testing.T) {
	ctx := context.Background()
	m, persist := newCASTestMeta(t)
	require.NoError(t, m.AddSegment(ctx, newFlushedTestSegment(100)))

	runs := 0
	require.NoError(t, m.UpdateSegmentsInfo(ctx, func(pack *updateSegmentPack) bool {
		runs++
		segment := pack.Get(100)
		if runs == 1 {
			// Another writer changes the segment after this write read it.
			require.NoError(t, m.UpdateSegment(100, SetMaxRowCount(42)))
		}
		segment.IsSorted = true
		return true
	}))
	require.Equal(t, 2, runs, "the write runs again on the refreshed value")
	for _, segment := range []*datapb.SegmentInfo{m.GetSegment(ctx, 100).SegmentInfo, persist.data[storedSegmentKey(100)].value} {
		require.EqualValues(t, 42, segment.GetMaxRowNum())
		require.True(t, segment.GetIsSorted())
	}
}

// The first read of a segment pins it for the rest of a write: the value an
// operator checked and the value the write is built on are the same, so a
// change in between fails the write instead of publishing a value whose check
// saw an older segment.
func TestSegmentWritePinsTheFirstReadOfASegment(t *testing.T) {
	ctx := context.Background()
	m, persist := newCASTestMeta(t)
	require.NoError(t, m.AddSegment(ctx, newFlushedTestSegment(100)))

	var built []int64
	require.NoError(t, m.UpdateSegmentsInfo(ctx, func(pack *updateSegmentPack) bool {
		checked := pack.Peek(100).GetMaxRowNum()
		if len(built) == 0 {
			require.NoError(t, m.UpdateSegment(100, SetMaxRowCount(42)))
		}
		segment := pack.Get(100)
		require.Equal(t, checked, segment.GetMaxRowNum(), "the write builds on the checked value")
		built = append(built, segment.GetMaxRowNum())
		segment.MaxRowNum++
		return true
	}))
	require.Equal(t, []int64{0, 42}, built)
	require.EqualValues(t, 43, persist.data[storedSegmentKey(100)].value.GetMaxRowNum())
}

// Concurrent writers of one segment run without a lock; every update lands.
func TestSegmentWritesOfOneSegmentDoNotLoseUpdates(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	m, persist := newCASTestMeta(t)
	require.NoError(t, m.AddSegment(ctx, newFlushedTestSegment(100)))
	const writers, rounds = 8, 5
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := 0; round < rounds; round++ {
				assert.NoError(t, m.UpdateSegmentsInfo(ctx, MutationsOperator(map[int64][]MutateFunc{
					100: {func(seg *datapb.SegmentInfo) bool {
						seg.MaxRowNum++
						return true
					}},
				})))
			}
		}()
	}
	wg.Wait()
	require.EqualValues(t, writers*rounds, m.GetSegment(ctx, 100).GetMaxRowNum())
	require.EqualValues(t, writers*rounds, persist.data[storedSegmentKey(100)].value.GetMaxRowNum())
}

// hookPersist runs hook once, before the first commit after it is set, and
// afterCommit once, after the first commit that succeeds after it is set.
type hookPersist struct {
	OptimisticTxnPersist[string, *datapb.SegmentInfo]
	hook        func()
	afterCommit func()
}

func (p *hookPersist) Txn(ctx context.Context) Txn[string, *datapb.SegmentInfo] {
	return &hookTxn{Txn: p.OptimisticTxnPersist.Txn(ctx), persist: p}
}

type hookTxn struct {
	Txn[string, *datapb.SegmentInfo]
	persist *hookPersist
}

func (t *hookTxn) Commit() ([]TxnResult[*datapb.SegmentInfo], error) {
	if hook := t.persist.hook; hook != nil {
		t.persist.hook = nil
		hook()
	}
	results, err := t.Txn.Commit()
	if hook := t.persist.afterCommit; hook != nil && err == nil {
		t.persist.afterCommit = nil
		hook()
	}
	return results, err
}

// orderPersist records the keys of the versioned records each transaction writes.
type orderPersist struct {
	OptimisticTxnPersist[string, *datapb.SegmentInfo]
	keys []string
}

func (p *orderPersist) Txn(ctx context.Context) Txn[string, *datapb.SegmentInfo] {
	return &orderTxn{Txn: p.OptimisticTxnPersist.Txn(ctx), persist: p}
}

type orderTxn struct {
	Txn[string, *datapb.SegmentInfo]
	persist *orderPersist
}

func (t *orderTxn) PutIfVersion(key string, value *datapb.SegmentInfo, version int64, commit bool) {
	t.persist.keys = append(t.persist.keys, key)
	t.Txn.PutIfVersion(key, value, version, commit)
}

// A sort compaction stores its output before it retires its input: a write
// split over the op limit that lands only its first batches must not leave the
// input retired without the output, which a retry would take for a commit
// that landed.
func TestCASSegmentStoreStagesRetiredSegmentsLast(t *testing.T) {
	ctx := context.Background()
	m, persist := newCASTestMeta(t)
	require.NoError(t, m.AddSegment(ctx, newFlushedTestSegment(100)))
	ordered := &orderPersist{OptimisticTxnPersist: persist}
	m.segmentStore = newCASSegmentStore(ordered, m.catalog, testSegmentStoreRoot)

	input, version := m.segments.GetSegmentWithVersion(100)
	retired := input.Clone()
	retired.State = commonpb.SegmentState_Dropped
	output := newFlushedTestSegment(200)
	_, err := m.store().AlterSegments(ctx, map[int64]int64{100: version}, []*datapb.SegmentInfo{retired.SegmentInfo, output.SegmentInfo})
	require.NoError(t, err)
	require.Equal(t, []string{storedSegmentKey(200), storedSegmentKey(100)}, ordered.keys)
}

// publishOrderStore records the segments each publication installs, in order.
type publishOrderStore struct {
	metacache.MetaStore
	published []int64
}

func (s *publishOrderStore) PublishSegments(segments []*datapb.SegmentInfo, versions map[int64]int64, merge metacache.SegmentMerge) []metacache.SegmentPublication {
	for _, segment := range segments {
		s.published = append(s.published, segment.GetID())
	}
	return s.MetaStore.PublishSegments(segments, versions, merge)
}

// A reader that does not wait for a whole publication sees a compaction's
// output before its input is retired (see retrieveSegment).
func TestPublishSegmentsInstallsRetiredSegmentsLast(t *testing.T) {
	store := &publishOrderStore{MetaStore: metacache.NewMetaStore(nil)}
	segments := NewSegmentsInfo(store)
	segments.SetSegment(100, newFlushedTestSegment(100), 1)
	retired := segments.GetSegment(100).Clone()
	retired.State = commonpb.SegmentState_Dropped

	segments.PublishSegments([]*SegmentInfo{retired, newFlushedTestSegment(200)}, map[int64]int64{100: 2, 200: 2})
	require.Equal(t, []int64{200, 100}, store.published)
	require.Equal(t, commonpb.SegmentState_Dropped, segments.GetSegment(100).GetState())
	require.NotNil(t, segments.GetSegment(200))
}

// An external collection refresh checks that the segments it keeps are alive
// and rewrites them unchanged. A drop of the collection committed after the
// check makes its write conflict and run again, so it fails instead of adding
// segments to the dropped collection.
func TestExternalRefreshConflictsWithADropOfAKeptSegment(t *testing.T) {
	ctx := context.Background()
	m, persist := newCASTestMeta(t)
	hooked := &hookPersist{OptimisticTxnPersist: persist}
	m.segmentStore = newCASSegmentStore(hooked, m.catalog, testSegmentStoreRoot)
	m.metaStore = m.segments.store
	m.metaStore.PutCollection(&collectionInfo{ID: 1, VChannelNames: []string{"ch-1"}, Partitions: []int64{2}})
	require.NoError(t, m.AddSegment(ctx, newFlushedTestSegment(100)))

	hooked.hook = func() {
		require.NoError(t, m.UpdateSegmentsInfo(ctx, UpdateStatusOperator(100, commonpb.SegmentState_Dropped)))
	}
	err := applyExternalCollectionSegmentUpdateForBaseline(ctx, m, 1, []int64{100}, []int64{100},
		[]*datapb.SegmentInfo{newTestExternalRefreshSegment(200, 1, 50)})
	require.ErrorContains(t, err, "cannot keep dropped segment 100")
	require.Nil(t, m.GetSegment(ctx, 200))
	require.NotContains(t, persist.data, storedSegmentKey(200))
	require.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(ctx, 100).GetState())
}

// A truncate selects the segments of the channel before it writes them. A
// clustering compaction that commits its outputs in between rewrites its
// inputs, so the truncate conflicts and selects again, the outputs included.
func TestTruncateConflictsWithAClusteringCompactionOfItsSegments(t *testing.T) {
	ctx := context.Background()
	m, persist := newCASTestMeta(t)
	hooked := &hookPersist{OptimisticTxnPersist: persist}
	m.segmentStore = newCASSegmentStore(hooked, m.catalog, testSegmentStoreRoot)
	input := newFlushedTestSegment(100)
	input.DmlPosition = &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 10}
	require.NoError(t, m.AddSegment(ctx, input))

	hooked.hook = func() {
		_, err := m.CompleteCompactionMutation(ctx, &datapb.CompactionTask{
			Type:          datapb.CompactionType_ClusteringCompaction,
			Channel:       "ch-1",
			InputSegments: []int64{100},
		}, &datapb.CompactionPlanResult{Segments: []*datapb.CompactionSegment{{SegmentID: 200, NumOfRows: 10}}})
		require.NoError(t, err)
	}
	require.NoError(t, m.TruncateChannelByTime(ctx, "ch-1", 20))
	require.Nil(t, hooked.hook, "the compaction committed while the truncate was writing")
	for _, id := range []int64{100, 200} {
		require.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(ctx, id).GetState(), "segment %d", id)
	}
}

// A mix compaction commit split over the transaction limit may land in part.
// Its rerun retires the remaining inputs and rewrites the outputs that landed.
func TestMixCompactionCompletesACommitThatLandedInPart(t *testing.T) {
	ctx := context.Background()
	m, persist := newCASTestMeta(t)
	retired := newDroppedTestSegment(1)
	retired.Compacted = true
	require.NoError(t, m.AddSegment(ctx, retired))
	require.NoError(t, m.AddSegment(ctx, newFlushedTestSegment(2)))
	landed := newFlushedTestSegment(10)
	landed.CompactionFrom = []int64{1, 2}
	landed.CreatedByCompaction = true
	require.NoError(t, m.AddSegment(ctx, landed))

	outputs, err := m.CompleteCompactionMutation(ctx, &datapb.CompactionTask{
		Type:          datapb.CompactionType_MixCompaction,
		Channel:       "ch-1",
		InputSegments: []int64{1, 2},
		Schema:        &schemapb.CollectionSchema{},
	}, &datapb.CompactionPlanResult{Segments: []*datapb.CompactionSegment{{SegmentID: 10, NumOfRows: 20}}})
	require.NoError(t, err)
	require.Len(t, outputs, 1)
	input := persist.data[storedSegmentKey(2)].value
	require.Equal(t, commonpb.SegmentState_Dropped, input.GetState())
	require.True(t, input.GetCompacted())
	output := m.GetSegment(ctx, 10)
	require.Equal(t, []int64{1, 2}, output.GetCompactionFrom())
	require.EqualValues(t, 20, output.GetNumOfRows())
	require.EqualValues(t, 20, persist.data[storedSegmentKey(10)].value.GetNumOfRows())
}

// An over-limit transaction keeps the ops of a group in one batch: the group
// of a conflicting record lands none of its ops, while the groups before it
// land whole.
func TestOptimisticTxnEtcdKeepsAGroupInOneBatch(t *testing.T) {
	ctx := context.Background()
	persist, cli, root := newTestEtcdPersist(t)
	key := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.Key
	paramtable.Get().Save(key, "3")
	defer paramtable.Get().Reset(key)

	existing := persist.Txn(ctx)
	existing.PutIfVersion(root+"/s/2", &datapb.SegmentInfo{ID: 2}, 0, false)
	_, err := existing.Commit()
	require.NoError(t, err)

	// [b1 s1] [b2 b2' s2]: split by op count, b2 would land with s1.
	txn := persist.Txn(ctx)
	txn.Group()
	txn.Raw(RawOp{Key: root + "/binlog/1/a", Value: "1"})
	txn.PutIfVersion(root+"/s/1", &datapb.SegmentInfo{ID: 1}, 0, false)
	txn.Group()
	txn.Raw(RawOp{Key: root + "/binlog/2/a", Value: "2"})
	txn.Raw(RawOp{Key: root + "/binlog/2/b", Value: "2"})
	txn.PutIfVersion(root+"/s/2", &datapb.SegmentInfo{ID: 2, NumOfRows: 1}, 0, false)
	results, err := txn.Commit()
	require.ErrorIs(t, err, ErrVersionConflict)
	require.Positive(t, results[1].Version)
	require.EqualValues(t, 1, countKeys(t, cli, root+"/binlog/1/"))
	require.Zero(t, countKeys(t, cli, root+"/binlog/2/"), "the group of the conflicting record lands nothing")
}

// A truncate that conflicts with a compaction of its segments, stored but not
// published yet, selects again from the store: it drops the compaction outputs
// too, without waiting for the compaction to publish.
func TestTruncateDropsTheOutputsOfAnUnpublishedCompaction(t *testing.T) {
	ctx := context.Background()
	m, persist := newCASTestMeta(t)
	hooked := &hookPersist{OptimisticTxnPersist: persist}
	m.segmentStore = newCASSegmentStore(hooked, m.catalog, testSegmentStoreRoot)
	for _, id := range []int64{100, 101} {
		input := newFlushedTestSegment(id)
		input.DmlPosition = &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 10}
		require.NoError(t, m.AddSegment(ctx, input))
	}

	hooked.afterCommit = func() {
		// The compaction is stored and not published yet.
		require.NoError(t, m.TruncateChannelByTime(ctx, "ch-1", 20))
	}
	_, err := m.CompleteCompactionMutation(ctx, &datapb.CompactionTask{
		Type:          datapb.CompactionType_MixCompaction,
		Channel:       "ch-1",
		InputSegments: []int64{100, 101},
		Schema:        &schemapb.CollectionSchema{},
	}, &datapb.CompactionPlanResult{Segments: []*datapb.CompactionSegment{{SegmentID: 200, NumOfRows: 20}}})
	require.NoError(t, err)
	require.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(ctx, 200).GetState())
	require.Equal(t, commonpb.SegmentState_Dropped, persist.data[storedSegmentKey(200)].value.GetState())
}

// A mix compaction commit that another DataCoord stored in part, retiring one
// input, completes on the stored segments, and installs the retired input
// with the rest of the commit: the cache, which never saw it retired, does not
// keep it alive next to the output.
func TestMixCompactionCompletesACommitItFindsStoredInPart(t *testing.T) {
	ctx := context.Background()
	m, persist := newCASTestMeta(t)
	for _, id := range []int64{100, 101} {
		require.NoError(t, m.AddSegment(ctx, newFlushedTestSegment(id)))
	}
	other := newCASTestMetaOn(t, m, persist)
	retired := other.GetSegment(ctx, 100).Clone()
	retired.State = commonpb.SegmentState_Dropped
	retired.Compacted = true
	output := newFlushedTestSegment(200)
	output.CompactionFrom = []int64{100, 101}
	output.CreatedByCompaction = true
	version, _ := cachedVersion(other, 100)
	_, err := other.store().AlterSegments(ctx, map[int64]int64{100: version}, []*datapb.SegmentInfo{retired.SegmentInfo, output.SegmentInfo})
	require.NoError(t, err)

	_, err = m.CompleteCompactionMutation(ctx, &datapb.CompactionTask{
		Type:          datapb.CompactionType_MixCompaction,
		Channel:       "ch-1",
		InputSegments: []int64{100, 101},
		Schema:        &schemapb.CollectionSchema{},
	}, &datapb.CompactionPlanResult{Segments: []*datapb.CompactionSegment{{SegmentID: 200, NumOfRows: 20}}})
	require.NoError(t, err)
	for _, id := range []int64{100, 101} {
		input := m.GetSegment(ctx, id)
		require.Equal(t, commonpb.SegmentState_Dropped, input.GetState(), "input %d", id)
		require.True(t, input.GetCompacted(), "input %d", id)
		version, _ := cachedVersion(m, id)
		require.Equal(t, persist.data[storedSegmentKey(id)].version, version, "input %d", id)
	}
	require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(ctx, 200).GetState())
	require.EqualValues(t, 20, persist.data[storedSegmentKey(200)].value.GetNumOfRows())
}

// A compaction commit stored by another DataCoord, and never published to
// this cache, is found complete in the store: it is installed, inputs and
// outputs together, rather than written again or rolled back.
func TestCompactionCommitInstallsTheCommitItFindsStored(t *testing.T) {
	ctx := context.Background()
	for _, compactionType := range []datapb.CompactionType{datapb.CompactionType_MixCompaction, datapb.CompactionType_SortCompaction} {
		t.Run(compactionType.String(), func(t *testing.T) {
			m, persist := newCASTestMeta(t)
			inputs := []int64{100, 101}
			if compactionType == datapb.CompactionType_SortCompaction {
				inputs = []int64{100}
			}
			for _, id := range inputs {
				require.NoError(t, m.AddSegment(ctx, newFlushedTestSegment(id)))
			}
			task := &datapb.CompactionTask{
				Type:          compactionType,
				Channel:       "ch-1",
				InputSegments: inputs,
				Schema:        &schemapb.CollectionSchema{},
			}
			result := &datapb.CompactionPlanResult{Segments: []*datapb.CompactionSegment{{SegmentID: 200, NumOfRows: 20}}}
			other := newCASTestMetaOn(t, m, persist)
			_, err := other.CompleteCompactionMutation(ctx, task, result)
			require.NoError(t, err)
			stored := persist.data[storedSegmentKey(200)].version

			outputs, err := m.CompleteCompactionMutation(ctx, task, result)
			require.NoError(t, err)
			require.Len(t, outputs, 1)
			require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(ctx, 200).GetState())
			version, _ := cachedVersion(m, 200)
			require.Equal(t, stored, version, "the stored output is installed, not written again")
			for _, id := range inputs {
				input := m.GetSegment(ctx, id)
				require.Equal(t, commonpb.SegmentState_Dropped, input.GetState())
				require.True(t, input.GetCompacted())
			}
		})
	}
}

// A compaction commit that stored its outputs before it failed, and finds an
// input dropped by another writer when it runs again, drops those outputs: the
// rows they copy were dropped with the input.
func TestCompactionDropsItsStoredOutputsWhenAnInputWasDropped(t *testing.T) {
	ctx := context.Background()
	for _, compactionType := range []datapb.CompactionType{datapb.CompactionType_MixCompaction, datapb.CompactionType_ClusteringCompaction} {
		t.Run(compactionType.String(), func(t *testing.T) {
			m, persist := newCASTestMeta(t)
			// A truncate dropped input 100; it is not compacted.
			require.NoError(t, m.AddSegment(ctx, newDroppedTestSegment(100)))
			require.NoError(t, m.AddSegment(ctx, newFlushedTestSegment(101)))
			stored := newFlushedTestSegment(200)
			stored.CompactionFrom = []int64{100, 101}
			stored.CreatedByCompaction = true
			require.NoError(t, m.AddSegment(ctx, stored))

			_, err := m.CompleteCompactionMutation(ctx, &datapb.CompactionTask{
				Type:          compactionType,
				Channel:       "ch-1",
				InputSegments: []int64{100, 101},
				Schema:        &schemapb.CollectionSchema{},
			}, &datapb.CompactionPlanResult{Segments: []*datapb.CompactionSegment{{SegmentID: 200, NumOfRows: 20}}})
			require.ErrorIs(t, err, merr.ErrSegmentNotFound)
			require.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(ctx, 200).GetState())
			require.Equal(t, commonpb.SegmentState_Dropped, persist.data[storedSegmentKey(200)].value.GetState())
			require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(ctx, 101).GetState())
		})
	}
}

// A transaction split into ordered batches that fails after its first batch
// reports the results of the batches that landed with the error.
func TestOptimisticTxnEtcdReportsTheBatchesThatLanded(t *testing.T) {
	ctx := context.Background()
	persist, _, root := newTestEtcdPersist(t)
	key := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.Key
	paramtable.Get().Save(key, "1")
	defer paramtable.Get().Reset(key)

	existing := persist.Txn(ctx)
	existing.PutIfVersion(root+"/s/2", &datapb.SegmentInfo{ID: 2}, 0, false)
	_, err := existing.Commit()
	require.NoError(t, err)

	// [s/1] lands, [s/2] expects a record that does not exist.
	txn := persist.Txn(ctx)
	txn.PutIfVersion(root+"/s/1", &datapb.SegmentInfo{ID: 1}, 0, false)
	txn.PutIfVersion(root+"/s/2", &datapb.SegmentInfo{ID: 2, NumOfRows: 1}, 0, false)
	results, err := txn.Commit()
	require.ErrorIs(t, err, ErrVersionConflict)
	require.Len(t, results, 2)
	require.Positive(t, results[0].Version)
	require.Zero(t, results[1].Version)
}

// MutateFunc modifies a *datapb.SegmentInfo in place. Returns true to proceed
// with the write, false to skip this segment's update.
type MutateFunc func(seg *datapb.SegmentInfo) bool

// MutationsOperator adapts per-segment MutateFuncs to an UpdateOperator. The
// funcs of a segment run in order against the value the write reads; when any of
// them returns false the segment is left untouched by this operator.
func MutationsOperator(mutations map[int64][]MutateFunc) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segmentIDs := make([]int64, 0, len(mutations))
		for segmentID := range mutations {
			segmentIDs = append(segmentIDs, segmentID)
		}
		slices.Sort(segmentIDs)
		updated := false
		for _, segmentID := range segmentIDs {
			_, inPack := modPack.segments[segmentID]
			segment := modPack.Get(segmentID)
			if segment == nil {
				continue
			}
			mutated := proto.Clone(segment.SegmentInfo).(*datapb.SegmentInfo)
			apply := true
			for _, fn := range mutations[segmentID] {
				if !fn(mutated) {
					apply = false
					break
				}
			}
			if !apply {
				if !inPack {
					delete(modPack.segments, segmentID)
				}
				continue
			}
			segment.SegmentInfo = mutated
			updated = true
		}
		return updated
	}
}

package datacoord

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	tikverr "github.com/tikv/client-go/v2/error"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

func compatTestSegment(id int64) *datapb.SegmentInfo {
	return &datapb.SegmentInfo{
		ID:            id,
		CollectionID:  1,
		PartitionID:   2,
		InsertChannel: "ch-1",
		State:         commonpb.SegmentState_Flushed,
		NumOfRows:     10,
		Binlogs:       []*datapb.FieldBinlog{{FieldID: 101, Binlogs: []*datapb.Binlog{{LogID: id*10 + 1, EntriesNum: 10, LogSize: 8}}}},
		Deltalogs:     []*datapb.FieldBinlog{{FieldID: 0, Binlogs: []*datapb.Binlog{{LogID: id*10 + 2, EntriesNum: 1}}}},
		Statslogs:     []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: id*10 + 3}}}},
		Bm25Statslogs: []*datapb.FieldBinlog{{FieldID: 102, Binlogs: []*datapb.Binlog{{LogID: id*10 + 4}}}},
	}
}

func logIDs(fieldBinlogs []*datapb.FieldBinlog) []int64 {
	var ids []int64
	for _, fieldBinlog := range fieldBinlogs {
		for _, b := range fieldBinlog.GetBinlogs() {
			ids = append(ids, b.GetLogID())
		}
	}
	return ids
}

func countKeys(t *testing.T, cli *clientv3.Client, prefix string) int64 {
	resp, err := cli.Get(context.Background(), prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	require.NoError(t, err)
	return resp.Count
}

func recoverIntoCache(t *testing.T, store *casSegmentStore, cache *SegmentsInfo) map[int64]*datapb.SegmentInfo {
	records, versions, err := store.ListSegments(context.Background(), 1)
	require.NoError(t, err)
	byID := make(map[int64]*datapb.SegmentInfo, len(records))
	for i, record := range records {
		cache.SetSegment(record.GetID(), NewSegmentInfo(record), versions[i])
		byID[record.GetID()] = record
	}
	return byID
}

// cachedVersions returns the cached version of each of segmentIDs: the base of
// a write built from the cached values.
func cachedVersions(cache *SegmentsInfo, segmentIDs ...int64) map[int64]int64 {
	versions := make(map[int64]int64, len(segmentIDs))
	for _, id := range segmentIDs {
		versions[id], _ = cache.store.SegmentVersion(id)
	}
	return versions
}

// The store and the catalog read each other's writes, so a cluster can move
// to this store and back: segments written by the catalog recover with their
// binlogs, and what the store writes, the catalog lists.
func TestCASSegmentStoreSharesTheCatalogEncoding(t *testing.T) {
	ctx := context.Background()
	persist, cli, root := newTestEtcdPersist(t)
	catalog := datacoord.NewCatalog(etcdkv.NewEtcdKV(cli, root), "files", root)

	// Segment 1000 shares the key prefix of segment 100 without the separator.
	written := compatTestSegment(100)
	written.TextStatsLogs = map[int64]*datapb.TextIndexStats{
		103: {FieldID: 103, Version: 1, BuildID: 9, Files: []string{"files/text_log/9/1/1/2/100/103/a"}},
	}
	require.NoError(t, catalog.AddSegment(ctx, proto.Clone(written).(*datapb.SegmentInfo)))
	require.NoError(t, catalog.AddSegment(ctx, compatTestSegment(1000)))

	cache := NewSegmentsInfo(metacache.NewMetaStore(nil))
	store := newCASSegmentStore(persist, catalog, root)
	recovered := recoverIntoCache(t, store, cache)
	require.Len(t, recovered, 2)
	segment := recovered[100]
	require.Equal(t, []int64{1001}, logIDs(segment.GetBinlogs()))
	require.EqualValues(t, 8, segment.GetBinlogs()[0].GetBinlogs()[0].GetMemorySize(), "old binlogs get their memory size")
	require.Equal(t, []int64{1002}, logIDs(segment.GetDeltalogs()))
	require.Equal(t, []int64{1003}, logIDs(segment.GetStatslogs()))
	require.Equal(t, []int64{1004}, logIDs(segment.GetBm25Statslogs()))
	require.Equal(t, []string{"files/text_log/9/1/1/2/100/103/a"}, segment.GetTextStatsLogs()[103].GetFiles())
	require.Equal(t, []int64{10001}, logIDs(recovered[1000].GetBinlogs()))
	// GC keeps a stats or delta file only while the segment lists it.
	info := NewSegmentInfo(segment)
	require.True(t, info.IsStatsLogExists(1003))
	require.True(t, info.IsDeltaLogExists(1002))
	require.False(t, store.inline.Contain(100))

	// A delete flush through the store writes only the deltalog family.
	updated := proto.Clone(segment).(*datapb.SegmentInfo)
	updated.Deltalogs[0].Binlogs = append(updated.Deltalogs[0].Binlogs, &datapb.Binlog{LogID: 1005, EntriesNum: 1})
	versions, err := store.AlterSegments(ctx, cachedVersions(cache, 100), []*datapb.SegmentInfo{updated}, metastore.BinlogsIncrement{
		Segment: updated,
		UpdateMask: metastore.BinlogsUpdateMask{
			WithoutBinlogs:       true,
			WithoutStatslogs:     true,
			WithoutBm25Statslogs: true,
		},
	})
	require.NoError(t, err)
	cache.PublishSegments([]*SegmentInfo{NewSegmentInfo(updated)}, versions)

	// The record keeps the catalog shape.
	resp, err := cli.Get(ctx, root+"/datacoord-meta/s/1/2/100")
	require.NoError(t, err)
	record := &datapb.SegmentInfo{}
	require.NoError(t, proto.Unmarshal(resp.Kvs[0].Value, record))
	require.False(t, datacoord.HasInlineBinlogs(record))
	require.Equal(t, []string{"a"}, record.GetTextStatsLogs()[103].GetFiles())

	// The catalog lists what the store wrote.
	listed, err := catalog.ListSegments(ctx, 1)
	require.NoError(t, err)
	require.Len(t, listed, 2)
	for _, s := range listed {
		if s.GetID() != 100 {
			continue
		}
		require.Equal(t, []int64{1001}, logIDs(s.GetBinlogs()))
		require.ElementsMatch(t, []int64{1002, 1005}, logIDs(s.GetDeltalogs()))
		require.Equal(t, []int64{1003}, logIDs(s.GetStatslogs()))
		require.Equal(t, []int64{1004}, logIDs(s.GetBm25Statslogs()))
	}

	// A drop removes the record and every binlog KV of the segment, and
	// nothing of segment 1000.
	require.NoError(t, store.DropSegments(ctx, cachedVersions(cache, 100), []*datapb.SegmentInfo{cache.GetSegment(100).SegmentInfo}))
	for _, family := range []string{"binlog", "deltalog", "statslog", "bm25log"} {
		require.Zero(t, countKeys(t, cli, root+"/datacoord-meta/"+family+"/1/2/100/"), family)
		require.Equal(t, int64(1), countKeys(t, cli, root+"/datacoord-meta/"+family+"/1/2/1000/"), family)
	}
	listed, err = catalog.ListSegments(ctx, 1)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.EqualValues(t, 1000, listed[0].GetID())
}

// Records written before the store kept the catalog encoding carry their
// binlogs. They recover as they are, and their first write moves every
// binlog family out, even the families the write does not touch.
func TestCASSegmentStoreMovesInlineBinlogsOut(t *testing.T) {
	ctx := context.Background()
	persist, cli, root := newTestEtcdPersist(t)
	catalog := datacoord.NewCatalog(etcdkv.NewEtcdKV(cli, root), "", root)

	txn := persist.Txn(ctx)
	txn.PutIfVersion(root+"/"+segmentRecordKey(1, 2, 7), compatTestSegment(7), 0, false)
	_, err := txn.Commit()
	require.NoError(t, err)

	cache := NewSegmentsInfo(metacache.NewMetaStore(nil))
	store := newCASSegmentStore(persist, catalog, root)
	recovered := recoverIntoCache(t, store, cache)
	require.Equal(t, []int64{71}, logIDs(recovered[7].GetBinlogs()))
	require.True(t, store.inline.Contain(7))

	updated := proto.Clone(recovered[7]).(*datapb.SegmentInfo)
	updated.Deltalogs[0].Binlogs = append(updated.Deltalogs[0].Binlogs, &datapb.Binlog{LogID: 75})
	_, err = store.AlterSegments(ctx, cachedVersions(cache, 7), []*datapb.SegmentInfo{updated}, metastore.BinlogsIncrement{
		Segment:    updated,
		UpdateMask: metastore.BinlogsUpdateMask{WithoutBinlogs: true, WithoutStatslogs: true, WithoutBm25Statslogs: true},
	})
	require.NoError(t, err)
	require.False(t, store.inline.Contain(7))

	listed, err := catalog.ListSegments(ctx, 1)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, []int64{71}, logIDs(listed[0].GetBinlogs()))
	require.ElementsMatch(t, []int64{72, 75}, logIDs(listed[0].GetDeltalogs()))
	require.Equal(t, []int64{73}, logIDs(listed[0].GetStatslogs()))
	require.Equal(t, []int64{74}, logIDs(listed[0].GetBm25Statslogs()))
	current, err := persist.Get(ctx, []string{root + "/" + segmentRecordKey(1, 2, 7)})
	require.NoError(t, err)
	require.False(t, datacoord.HasInlineBinlogs(current[root+"/"+segmentRecordKey(1, 2, 7)].Value))
}

// lostResponseKV lets the first transaction land and then reports a transport
// failure, or reports it without sending the transaction when drop is set. Its
// first getFailures reads fail too.
type lostResponseKV struct {
	clientv3.KV
	drop        bool
	failed      bool
	getFailures int
}

func (kv *lostResponseKV) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	if kv.getFailures > 0 {
		kv.getFailures--
		return nil, status.Error(codes.Unavailable, "mock read failure")
	}
	return kv.KV.Get(ctx, key, opts...)
}

func (kv *lostResponseKV) Txn(ctx context.Context) clientv3.Txn {
	return &lostResponseTxn{Txn: kv.KV.Txn(ctx), kv: kv}
}

type lostResponseTxn struct {
	clientv3.Txn
	kv *lostResponseKV
}

func (t *lostResponseTxn) If(cs ...clientv3.Cmp) clientv3.Txn {
	t.Txn = t.Txn.If(cs...)
	return t
}

func (t *lostResponseTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	t.Txn = t.Txn.Then(ops...)
	return t
}

func (t *lostResponseTxn) Commit() (*clientv3.TxnResponse, error) {
	if t.kv.failed {
		return t.Txn.Commit()
	}
	t.kv.failed = true
	if !t.kv.drop {
		if _, err := t.Txn.Commit(); err != nil {
			return nil, err
		}
	}
	return nil, status.Error(codes.Unavailable, "mock lost response")
}

func newLostResponsePersist(t *testing.T, drop bool) (OptimisticTxnPersist[string, *datapb.SegmentInfo], *clientv3.Client, string) {
	_, cli, root := newTestEtcdPersist(t)
	faulty := clientv3.NewCtxClient(context.Background())
	faulty.KV = &lostResponseKV{KV: cli.KV, drop: drop}
	return NewOptimisticTxnEtcdPersist[string, *datapb.SegmentInfo](faulty, &SegmentInfoMarshaler{}), cli, root
}

// A versioned batch whose commit failed in transport is read back: one that
// landed succeeds, one that did not keeps failing with the transport error,
// so the caller's resend still compares against the versions it was built
// from.
func TestOptimisticTxnEtcdResolvesUnknownCommitOutcome(t *testing.T) {
	ctx := context.Background()

	t.Run("landed", func(t *testing.T) {
		persist, cli, root := newLostResponsePersist(t, false)
		txn := persist.Txn(ctx)
		txn.PutIfVersion(root+"/s/1", &datapb.SegmentInfo{ID: 1, NumOfRows: 3}, 0, false)
		txn.Raw(RawOp{Key: root + "/marker", Value: "m"})
		results, err := txn.Commit()
		require.NoError(t, err)
		resp, err := cli.Get(ctx, root+"/s/1")
		require.NoError(t, err)
		require.Equal(t, resp.Kvs[0].ModRevision, results[0].Version)
	})

	t.Run("landed, read back after failed reads", func(t *testing.T) {
		_, cli, root := newTestEtcdPersist(t)
		faulty := clientv3.NewCtxClient(context.Background())
		faulty.KV = &lostResponseKV{KV: cli.KV, getFailures: 2}
		persist := NewOptimisticTxnEtcdPersist[string, *datapb.SegmentInfo](faulty, &SegmentInfoMarshaler{})
		txn := persist.Txn(ctx)
		txn.PutIfVersion(root+"/s/1", &datapb.SegmentInfo{ID: 1, NumOfRows: 3}, 0, false)
		results, err := txn.Commit()
		require.NoError(t, err, "the outcome is read back until the store answers")
		resp, err := cli.Get(ctx, root+"/s/1")
		require.NoError(t, err)
		require.Equal(t, resp.Kvs[0].ModRevision, results[0].Version)
	})

	t.Run("not landed", func(t *testing.T) {
		persist, cli, root := newLostResponsePersist(t, true)
		txn := persist.Txn(ctx)
		txn.PutIfVersion(root+"/s/1", &datapb.SegmentInfo{ID: 1}, 0, false)
		_, err := txn.Commit()
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrVersionConflict)
		require.True(t, retry.IsRecoverable(err), "a transport failure stays retriable")
		require.Zero(t, countKeys(t, cli, root+"/s/1"))

		// The resend with the same expected version lands.
		_, err = txn.Commit()
		require.NoError(t, err)
		require.Equal(t, int64(1), countKeys(t, cli, root+"/s/1"))
	})
}

// An oversized request fails at once and stays failed for the caller's retry
// loops instead of being resent forever.
func TestOptimisticTxnEtcdPermanentErrorIsNotRetried(t *testing.T) {
	persist, _, root := newTestEtcdPersist(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	txn := persist.Txn(ctx)
	txn.PutIfVersion(root+"/s/1", &datapb.SegmentInfo{ID: 1}, 0, false)
	txn.Raw(RawOp{Key: root + "/big", Value: strings.Repeat("x", 3<<20)})
	start := time.Now()
	_, err := txn.Commit()
	require.Error(t, err)
	require.False(t, retry.IsRecoverable(err), "%v", err)
	require.Less(t, time.Since(start), 10*time.Second)
}

func TestIsPermanentStoreError(t *testing.T) {
	require.True(t, isPermanentStoreError(rpctypes.ErrRequestTooLarge))
	require.True(t, isPermanentStoreError(rpctypes.ErrTooManyOps))
	require.True(t, isPermanentStoreError(errors.Wrap(rpctypes.ErrGRPCRequestTooLarge, "wrapped")))
	require.True(t, isPermanentStoreError(status.Error(codes.ResourceExhausted, "trying to send message larger than max")))
	require.True(t, isPermanentStoreError(&tikverr.ErrTxnTooLarge{Size: 1}))
	require.True(t, isPermanentStoreError(&tikverr.ErrEntryTooLarge{Limit: 1, Size: 2}))
	require.False(t, isPermanentStoreError(context.DeadlineExceeded))
	require.False(t, isPermanentStoreError(status.Error(codes.Unavailable, "leader changed")))
	require.False(t, isPermanentStoreError(rpctypes.ErrLeaderChanged))
	// Throttling and a full backend recover.
	require.False(t, isPermanentStoreError(rpctypes.ErrTooManyRequests))
	require.False(t, isPermanentStoreError(rpctypes.ErrGRPCNoSpace))
}

func TestSegmentsInfoPublishKeepsLastExpireTime(t *testing.T) {
	cache := NewSegmentsInfo(metacache.NewMetaStore(nil))
	cache.SetSegment(1, NewSegmentInfo(&datapb.SegmentInfo{ID: 1, LastExpireTime: 10}), 1)
	writerCopy := cache.GetSegment(1).Clone()
	cache.AddAllocation(1, &Allocation{SegmentID: 1, ExpireTime: 20})

	writerCopy.NumOfRows = 5
	cache.PublishSegments([]*SegmentInfo{writerCopy}, map[int64]int64{1: 2})
	published := cache.GetSegment(1)
	require.EqualValues(t, 20, published.GetLastExpireTime(), "an allocation made meanwhile is not rolled back")
	require.EqualValues(t, 5, published.GetNumOfRows())
	require.Len(t, published.allocations, 1)
}

// countingPersist counts the transactions committed through it.
type countingPersist struct {
	OptimisticTxnPersist[string, *datapb.SegmentInfo]
	txns int
}

func (p *countingPersist) Txn(ctx context.Context) Txn[string, *datapb.SegmentInfo] {
	p.txns++
	return p.OptimisticTxnPersist.Txn(ctx)
}

// A drop takes the record and four binlog prefixes of each segment, so a
// transaction holds as many whole segments as the op limit allows.
func TestCASSegmentStoreDropsWholeSegmentsPerTransaction(t *testing.T) {
	ctx := context.Background()
	inner, cli, root := newTestEtcdPersist(t)
	key := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.Key
	paramtable.Get().Save(key, "10")
	defer paramtable.Get().Reset(key)

	persist := &countingPersist{OptimisticTxnPersist: inner}
	cache := NewSegmentsInfo(metacache.NewMetaStore(nil))
	store := newCASSegmentStore(persist, datacoord.NewCatalog(etcdkv.NewEtcdKV(cli, root), "", root), root)
	var segments []*datapb.SegmentInfo
	for id := int64(1); id <= 5; id++ {
		segment := compatTestSegment(id)
		segment.State = commonpb.SegmentState_Dropped
		version, err := store.AddSegment(ctx, segment)
		require.NoError(t, err)
		cache.SetSegment(id, NewSegmentInfo(segment), version)
		segments = append(segments, segment)
	}

	persist.txns = 0
	require.NoError(t, store.DropSegments(ctx, cachedVersions(cache, 1, 2, 3, 4, 5), segments))
	require.Equal(t, 3, persist.txns, "two segments of five ops fit a limit of ten")
	require.Zero(t, countKeys(t, cli, root+"/datacoord-meta/"))
}

// A compaction commit split into ordered batches whose last batch conflicts
// publishes the batches that landed, so its rerun builds on them: nothing
// else would bring the cache up to date with the store.
func TestSegmentWritePublishesTheBatchesThatLanded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	persist, cli, root := newTestEtcdPersist(t)
	hooked := &hookPersist{OptimisticTxnPersist: persist}
	m := &meta{
		ctx:      context.Background(),
		catalog:  datacoord.NewCatalog(etcdkv.NewEtcdKV(cli, root), "", root),
		segments: NewSegmentsInfo(metacache.NewMetaStore(nil)),
	}
	m.segmentStore = newCASSegmentStore(hooked, m.catalog, root)
	for _, id := range []int64{100, 101} {
		require.NoError(t, m.AddSegment(ctx, newFlushedTestSegment(id)))
	}

	// One op per batch: [output 200] [input 100] [input 101]. Another writer
	// changes input 101 first, so the last batch conflicts.
	key := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.Key
	paramtable.Get().Save(key, "1")
	defer paramtable.Get().Reset(key)
	hooked.hook = func() {
		require.NoError(t, m.UpdateSegment(101, SetMaxRowCount(7)))
	}
	_, err := m.CompleteCompactionMutation(ctx, &datapb.CompactionTask{
		Type:          datapb.CompactionType_MixCompaction,
		Channel:       "ch-1",
		InputSegments: []int64{100, 101},
		Schema:        &schemapb.CollectionSchema{},
	}, &datapb.CompactionPlanResult{Segments: []*datapb.CompactionSegment{{SegmentID: 200, NumOfRows: 20}}})
	require.NoError(t, err)

	require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(ctx, 200).GetState())
	for _, id := range []int64{100, 101} {
		input := m.GetSegment(ctx, id)
		require.Equal(t, commonpb.SegmentState_Dropped, input.GetState())
		require.True(t, input.GetCompacted())
	}
	require.EqualValues(t, 7, m.GetSegment(ctx, 101).GetMaxRowNum())
	listed, err := m.catalog.ListSegments(ctx, 1)
	require.NoError(t, err)
	states := make(map[int64]commonpb.SegmentState)
	for _, segment := range listed {
		states[segment.GetID()] = segment.GetState()
	}
	require.Equal(t, map[int64]commonpb.SegmentState{
		100: commonpb.SegmentState_Dropped,
		101: commonpb.SegmentState_Dropped,
		200: commonpb.SegmentState_Flushed,
	}, states)
}

// Two DataCoords add binlogs to one segment, each write split over the op
// limit, built on the same version. The loser's binlog KVs never overwrite the
// winner's, which its cache has not seen: its rebuild keeps them.
func TestSplitSegmentWritesKeepTheBinlogsOfEachOther(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	persist, cli, root := newTestEtcdPersist(t)
	hooked := &hookPersist{OptimisticTxnPersist: persist}
	m := &meta{
		ctx:      context.Background(),
		catalog:  datacoord.NewCatalog(etcdkv.NewEtcdKV(cli, root), "", root),
		segments: NewSegmentsInfo(metacache.NewMetaStore(nil)),
	}
	m.segmentStore = newCASSegmentStore(hooked, m.catalog, root)
	require.NoError(t, m.AddSegment(ctx, newFlushedTestSegment(100)))
	other := newCASTestMetaOn(t, m, persist)

	// Four binlog KVs and the record: batches of three ops split the write.
	key := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.Key
	paramtable.Get().Save(key, "3")
	defer paramtable.Get().Reset(key)
	logs := func(logID int64) []*datapb.FieldBinlog {
		var binlogs []*datapb.FieldBinlog
		for field := int64(101); field <= 104; field++ {
			binlogs = append(binlogs, &datapb.FieldBinlog{FieldID: field, Binlogs: []*datapb.Binlog{{LogID: logID, EntriesNum: 1}}})
		}
		return binlogs
	}
	hooked.hook = func() {
		require.NoError(t, other.UpdateSegmentsInfo(ctx, AddBinlogsOperator(100, logs(200), nil, nil, nil)))
	}
	require.NoError(t, m.UpdateSegmentsInfo(ctx, AddBinlogsOperator(100, logs(100), nil, nil, nil)))

	listed, err := m.catalog.ListSegments(ctx, 1)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	for _, segment := range []*datapb.SegmentInfo{listed[0], m.GetSegment(ctx, 100).SegmentInfo} {
		require.Len(t, segment.GetBinlogs(), 4)
		for _, field := range segment.GetBinlogs() {
			require.ElementsMatch(t, []int64{100, 200}, logIDs([]*datapb.FieldBinlog{field}), "field %d", field.GetFieldID())
		}
	}
}

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

package proxy

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/proxy/channelmgr"
	"github.com/milvus-io/milvus/internal/proxy/scheduler"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	streamingmessage "github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	streamingtypes "github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// These tests drive the real write paths against the split's contract: the
// fence on the source is final, the routing commit that lists the targets
// becomes visible some time after it, and a row the source refused has to
// reach the target owning its residue -- never the source again.

// splitFenceTestCollectionID matches createTestUpdateTask, so the upsert
// builders can be reused as they are.
const splitFenceTestCollectionID = int64(1001)

const (
	splitSource  = "by-dev-rootcoord-dml_0_1001v0"
	splitSibling = "by-dev-rootcoord-dml_1_1001v1"
	splitTarget0 = "by-dev-rootcoord-dml_2_1001v2"
	splitTarget1 = "by-dev-rootcoord-dml_3_1001v3"
)

// oneShardSplit is a single never-split shard before, and after the routing
// commit its two targets owning residues {0} and {1} of modulus 2; the fenced
// source stays listed but owns nothing.
func oneShardSplit() (pre, post *collectionInfo) {
	pre = splitCollectionInfo(splitFenceTestCollectionID, 0, []string{splitSource})
	post = splitCollectionInfo(splitFenceTestCollectionID, 2, []string{splitSource, splitTarget0, splitTarget1},
		splitShardInfo(schemapb.ShardState_ShardSplitting, splitSource),
		splitShardInfo(schemapb.ShardState_ShardCreating, splitTarget0, 0),
		splitShardInfo(schemapb.ShardState_ShardCreating, splitTarget1, 1),
	)
	return pre, post
}

// twoShardSplit is two never-split shards, of which only the source (residue 0
// of 2) is split: the targets own {0} and {2} of modulus 4 and the untouched
// sibling is re-expressed as {1, 3}.
func twoShardSplit() (pre, post *collectionInfo) {
	pre = splitCollectionInfo(splitFenceTestCollectionID, 0, []string{splitSource, splitSibling})
	post = splitCollectionInfo(splitFenceTestCollectionID, 4, []string{splitSource, splitSibling, splitTarget0, splitTarget1},
		splitShardInfo(schemapb.ShardState_ShardSplitting, splitSource),
		splitShardInfo(schemapb.ShardState_ShardNormal, splitSibling, 1, 3),
		splitShardInfo(schemapb.ShardState_ShardCreating, splitTarget0, 0),
		splitShardInfo(schemapb.ShardState_ShardCreating, splitTarget1, 2),
	)
	return pre, post
}

// splitFenceFixture is the proxy's view of the collection while a split lands:
// the cache serves the pre-split routing until an eviction re-describes it and
// the routing commit is visible.
type splitFenceFixture struct {
	pre, post *collectionInfo
	committed bool
	// staleRefreshes is how many evictions still re-describe the pre-split
	// routing: the fence lands before the routing commit.
	staleRefreshes int
	// channelErr fails every channel-list read from failChannelReadAt on.
	channelErr        error
	failChannelReadAt int
	channelReads      int
	// infoErr fails every describe of the collection, partitionErr every
	// partition lookup.
	infoErr      error
	partitionErr error
	evictions    int
	// onEvict runs on every eviction, after the eviction is counted: it is where
	// a test injects what happens between two attempts.
	onEvict func(eviction int)
	cache   *MockCache
}

func newSplitFenceFixture(t *testing.T, pre, post *collectionInfo) *splitFenceFixture {
	f := &splitFenceFixture{pre: pre, post: post}
	f.cache = NewMockCache(t)
	f.cache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).
		Return(splitFenceTestCollectionID, nil).Maybe()
	f.cache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(context.Context, string, string, int64) (*collectionInfo, error) {
			if f.infoErr != nil {
				return nil, f.infoErr
			}
			return f.routing(), nil
		}).Maybe()
	f.cache.EXPECT().GetPartitionID(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(context.Context, string, string, string) (int64, error) {
			return 200, f.partitionErr
		}).Maybe()
	f.cache.EXPECT().RemoveCollectionsByID(mock.Anything, splitFenceTestCollectionID).
		RunAndReturn(func(context.Context, int64) []string {
			f.evictions++
			if f.evictions > f.staleRefreshes {
				f.committed = true
			}
			if f.onEvict != nil {
				f.onEvict(f.evictions)
			}
			return nil
		}).Maybe()
	return f
}

func (f *splitFenceFixture) routing() *collectionInfo {
	if f.committed {
		return f.post
	}
	return f.pre
}

func (f *splitFenceFixture) chMgr() channelmgr.ChannelsMgr {
	return channelmgr.NewChannelsMgr(func(typeutil.UniqueID) (channelmgr.ChannelInfo, error) {
		f.channelReads++
		if f.failChannelReadAt > 0 && f.channelReads >= f.failChannelReadAt {
			return channelmgr.ChannelInfo{}, f.channelErr
		}
		info := f.routing()
		return channelmgr.ChannelInfo{VChans: info.VChannels, PChans: info.PChannels}, nil
	})
}

func (f *splitFenceFixture) insertTask(pks []int64) *insertTask {
	rows := make([]string, len(pks))
	for i := range rows {
		rows[i] = fmt.Sprintf("row-%d", i)
	}
	return &insertTask{
		baseTask:     baseTask{MetaCache: f.cache},
		ctx:          context.Background(),
		insertMsg:    newInt64VarCharInsertMsgForRepackTest(splitFenceTestCollectionID, rows...),
		result:       &milvuspb.MutationResult{Status: merr.Success(), IDs: int64IDs(pks...)},
		chMgr:        f.chMgr(),
		schema:       &schemapb.CollectionSchema{},
		collectionID: splitFenceTestCollectionID,
	}
}

func (f *splitFenceFixture) deleteTask(pks []int64) *deleteTask {
	return &deleteTask{
		baseTask:     baseTask{MetaCache: f.cache},
		chMgr:        f.chMgr(),
		collectionID: splitFenceTestCollectionID,
		partitionID:  200,
		vChannels:    f.routing().VChannels,
		idAllocator:  allocator.NewLocalAllocator(1, 1<<30),
		ts:           100,
		req: &milvuspb.DeleteRequest{
			DbName:         "db",
			CollectionName: "collection",
			Expr:           "pk > 0",
		},
		primaryKeys: int64IDs(pks...),
	}
}

func (f *splitFenceFixture) upsertTask(t *testing.T, insertPKs, deletePKs []int64) *upsertTask {
	allocPatch := mockey.Mock((*allocator.IDAllocator).Alloc).Return(int64(1), int64(1<<30), nil).Build()
	t.Cleanup(func() { allocPatch.UnPatch() })
	task := partialUpdateCASRealPackTestTask(t, insertPKs, insertPKs, deletePKs)
	task.req.PartialUpdate = false
	task.req.FieldOps = nil
	task.MetaCache = f.cache
	task.chMgr = f.chMgr()
	return task
}

// splitFenceTestWAL refuses every append to a fenced vchannel with the error
// the shard interceptor raises, fails the ones listed in failing, and lands the
// rest.
//
// Like the real client, one append commits the messages of one vchannel in one
// transaction, and every message of it takes the commit's time tick. rows
// applies them the way segcore does: a tombstone removes a row only when its
// tick is greater than the row's insert tick, so an upsert's insert and delete
// of the same key in one transaction leave the new row in place.
//
// windows models each vchannel's idempotency window, in the streamingnode's
// interceptor order (idempotency before shard): a single keyed insert is
// answered from the window before the fence is consulted, while a keyed
// transaction reaches the window only at its commit, after the fence has
// already refused its bodies.
type splitFenceTestWAL struct {
	streaming.WALAccesser
	fenced  map[string]struct{}
	failing map[string]error
	batches [][]streamingmessage.MutableMessage
	tick    uint64
	// rows is vchannel -> primary key -> insert tick of the live row.
	rows map[string]map[int64]uint64
	// windows is vchannel -> idempotency key -> the write unit it recorded.
	windows map[string]map[string]*splitFenceTestWindowEntry
	// insertedRowIDs and deletedPKs record, per vchannel, every row id and
	// tombstone an append actually wrote there.
	insertedRowIDs map[string][]int64
	deletedPKs     map[string][]int64
}

type splitFenceTestWindowEntry struct {
	tick   uint64
	result *messagespb.IdempotentInsertResult
}

func installSplitFenceTestWAL(t *testing.T, fenced ...string) *splitFenceTestWAL {
	w := &splitFenceTestWAL{
		fenced:  lo.SliceToMap(fenced, func(v string) (string, struct{}) { return v, struct{}{} }),
		failing: map[string]error{},
		rows:    map[string]map[int64]uint64{},
		windows: map[string]map[string]*splitFenceTestWindowEntry{},

		insertedRowIDs: map[string][]int64{},
		deletedPKs:     map[string][]int64{},
	}
	old := streaming.WAL()
	streaming.SetWALForTest(w)
	t.Cleanup(func() { streaming.SetWALForTest(old) })
	return w
}

func (w *splitFenceTestWAL) AppendMessages(ctx context.Context, msgs ...streamingmessage.MutableMessage) streaming.AppendResponses {
	return w.AppendMessagesWithOptions(ctx, msgs)
}

func (w *splitFenceTestWAL) AppendMessagesWithOptions(_ context.Context, msgs []streamingmessage.MutableMessage, opts ...streaming.AppendOption) streaming.AppendResponses {
	txnKey := ""
	if len(opts) > 0 {
		txnKey = opts[0].IdempotencyKey
	}
	w.batches = append(w.batches, append([]streamingmessage.MutableMessage(nil), msgs...))
	resp := streaming.AppendResponses{Responses: make([]streaming.AppendResponse, len(msgs))}
	var order []string
	txns := make(map[string][]int)
	for i, msg := range msgs {
		if _, ok := txns[msg.VChannel()]; !ok {
			order = append(order, msg.VChannel())
		}
		txns[msg.VChannel()] = append(txns[msg.VChannel()], i)
	}
	for _, vchannel := range order {
		indexes := txns[vchannel]
		key := txnKey
		if len(indexes) == 1 {
			key = string(streamingmessage.IdempotencyKeyOf(msgs[indexes[0]]))
		}
		_, fenced := w.fenced[vchannel]
		checkWindow := key != "" && (len(indexes) == 1 || !fenced)
		if entry, ok := w.windows[vchannel][key]; checkWindow && ok {
			extra, err := anypb.New(entry.result)
			if err != nil {
				panic(err)
			}
			for _, i := range indexes {
				resp.Responses[i].AppendResult = &streamingtypes.AppendResult{TimeTick: entry.tick, Extra: extra}
			}
			continue
		}
		var err error
		if fenced {
			err = status.NewShardFenced(vchannel, 0, 0)
		} else if failure, ok := w.failing[vchannel]; ok {
			err = failure
		}
		if err != nil {
			for _, i := range indexes {
				resp.Responses[i].Error = err
			}
			continue
		}
		w.tick++
		for _, i := range indexes {
			resp.Responses[i].AppendResult = &streamingtypes.AppendResult{TimeTick: w.tick}
			w.apply(msgs[i], w.tick)
		}
		if key != "" {
			w.record(vchannel, key, w.tick, msgs, indexes)
		}
	}
	return resp
}

// record stores the write unit a keyed append wrote on a vchannel: the row
// offsets and ids its insert headers carry.
func (w *splitFenceTestWAL) record(vchannel, key string, tick uint64, msgs []streamingmessage.MutableMessage, indexes []int) {
	result := &messagespb.IdempotentInsertResult{Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{}}}}
	for _, i := range indexes {
		if msgs[i].MessageType() != streamingmessage.MessageTypeInsert {
			continue
		}
		header, ok := streamingmessage.IdempotentInsertResultFromInsertHeader(streamingmessage.MustAsMutableInsertMessageV1(msgs[i]).Header())
		if !ok {
			continue
		}
		result.RowOffsets = append(result.RowOffsets, header.GetRowOffsets()...)
		result.Ids.GetIntId().Data = append(result.Ids.GetIntId().Data, header.GetIds().GetIntId().GetData()...)
	}
	if w.windows[vchannel] == nil {
		w.windows[vchannel] = map[string]*splitFenceTestWindowEntry{}
	}
	w.windows[vchannel][key] = &splitFenceTestWindowEntry{tick: tick, result: result}
}

func (w *splitFenceTestWAL) apply(msg streamingmessage.MutableMessage, tick uint64) {
	switch msg.MessageType() {
	case streamingmessage.MessageTypeInsert:
		body := streamingmessage.MustAsMutableInsertMessageV1(msg).MustBody()
		w.insertedRowIDs[msg.VChannel()] = append(w.insertedRowIDs[msg.VChannel()], body.GetRowIDs()...)
		fields := body.GetFieldsData()
		if len(fields) == 0 {
			return
		}
		for _, pk := range fields[0].GetScalars().GetLongData().GetData() {
			w.insertAt(msg.VChannel(), pk, tick)
		}
	case streamingmessage.MessageTypeDelete:
		pks := streamingmessage.MustAsMutableDeleteMessageV1(msg).MustBody().GetPrimaryKeys().GetIntId().GetData()
		w.deletedPKs[msg.VChannel()] = append(w.deletedPKs[msg.VChannel()], pks...)
		for _, pk := range pks {
			if insertedAt, ok := w.rows[msg.VChannel()][pk]; ok && tick > insertedAt {
				delete(w.rows[msg.VChannel()], pk)
			}
		}
	}
}

// insertAt records a live row, as a committed insert -- the upsert's own, or
// one a concurrent request wrote -- would.
func (w *splitFenceTestWAL) insertAt(vchannel string, pk int64, tick uint64) {
	if w.rows[vchannel] == nil {
		w.rows[vchannel] = make(map[int64]uint64)
	}
	w.rows[vchannel][pk] = tick
}

// assertLiveOnTheirOwner checks that every key has a live row, on exactly the
// shard that owns it.
func (w *splitFenceTestWAL) assertLiveOnTheirOwner(t *testing.T, post *collectionInfo, pks []int64) {
	t.Helper()
	for _, pk := range pks {
		var live []string
		for vchannel, rows := range w.rows {
			if _, ok := rows[pk]; ok {
				live = append(live, vchannel)
			}
		}
		assert.Equal(t, []string{splitOwner(t, post, pk)}, live, "live row of pk %d", pk)
	}
}

// vchannelsOf returns the distinct vchannels one append batch addressed, sorted.
func (w *splitFenceTestWAL) vchannelsOf(batch int) []string {
	set := make(map[string]struct{})
	for _, msg := range w.batches[batch] {
		set[msg.VChannel()] = struct{}{}
	}
	out := lo.Keys(set)
	sort.Strings(out)
	return out
}

// assertNoAppendToTheSourceAfterItRefused checks that the source is only ever
// addressed by the attempt it refused.
func (w *splitFenceTestWAL) assertNoAppendToTheSourceAfterItRefused(t *testing.T) {
	t.Helper()
	for batch := 1; batch < len(w.batches); batch++ {
		assert.NotContains(t, w.vchannelsOf(batch), splitSource, "attempt %d went back to the fenced source", batch)
	}
}

func seqPKs(n int) []int64 {
	pks := make([]int64, n)
	for i := range pks {
		pks[i] = int64(i + 1)
	}
	return pks
}

// splitOwner is the shard owning pk: by the legacy modulo for a collection that
// has never been split, by its residue for a split one.
func splitOwner(t *testing.T, info *collectionInfo, pk int64) string {
	t.Helper()
	if info.SplitRouting == nil {
		idx, err := typeutil.HashPK2Channels(int64IDs(pk), info.VChannels)
		require.NoError(t, err)
		return info.VChannels[idx[0]]
	}
	owner, err := info.SplitRouting.Table.VChannelOfPK(pk)
	require.NoError(t, err)
	return owner
}

// assertRowsLandedOnceOnTheirOwner checks that the row with id firstRowID+i
// (primary key pks[i]) landed exactly once, on the shard owning its key.
func assertRowsLandedOnceOnTheirOwner(t *testing.T, w *splitFenceTestWAL, post *collectionInfo, pks []int64, firstRowID int64) {
	t.Helper()
	landed := make(map[int64][]string)
	for vchannel, rowIDs := range w.insertedRowIDs {
		for _, rowID := range rowIDs {
			landed[rowID] = append(landed[rowID], vchannel)
		}
	}
	require.Len(t, landed, len(pks), "every row lands, and nothing else does")
	for i, pk := range pks {
		assert.Equal(t, []string{splitOwner(t, post, pk)}, landed[firstRowID+int64(i)], "row of pk %d", pk)
	}
}

// assertTombstonesLandedOnTheirOwner checks that every key's tombstone landed
// on the shard owning the key, and nowhere else.
func assertTombstonesLandedOnTheirOwner(t *testing.T, w *splitFenceTestWAL, post *collectionInfo, pks []int64) {
	t.Helper()
	landed := make(map[int64][]string)
	for vchannel, deleted := range w.deletedPKs {
		for _, pk := range deleted {
			landed[pk] = append(landed[pk], vchannel)
		}
	}
	require.Len(t, landed, len(pks))
	for _, pk := range pks {
		owner := splitOwner(t, post, pk)
		require.NotEmpty(t, landed[pk], "pk %d", pk)
		for _, vchannel := range landed[pk] {
			assert.Equal(t, owner, vchannel, "tombstone of pk %d", pk)
		}
	}
}

func useSingleMessageRepack(t *testing.T) {
	old := Params.ProxyCfg.SplitChunkProxy.SwapTempValue("false")
	t.Cleanup(func() { Params.ProxyCfg.SplitChunkProxy.SwapTempValue(old) })
}

func TestInsertExecuteResendsFencedRowsToTheTargetOwningTheirResidue(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	wal := installSplitFenceTestWAL(t, splitSource)
	pks := seqPKs(64)
	task := f.insertTask(pks)

	require.NoError(t, task.Execute(context.Background()))
	require.True(t, merr.Ok(task.result.GetStatus()), task.result.GetStatus().GetReason())

	require.Len(t, wal.batches, 2)
	assert.Equal(t, []string{splitSource}, wal.vchannelsOf(0))
	assert.Equal(t, []string{splitTarget0, splitTarget1}, wal.vchannelsOf(1))
	wal.assertNoAppendToTheSourceAfterItRefused(t)
	assertRowsLandedOnceOnTheirOwner(t, wal, post, pks, 1)
	assert.Equal(t, 1, f.evictions, "one refresh re-describes the collection")
	assert.Equal(t, wal.tick, task.result.GetTimestamp())
}

// When a split fences ONE shard of a multi-shard batch, the rows on the other
// shard commit in the same attempt; only the fenced shard's rows are re-sent,
// and nothing lands twice.
func TestInsertExecuteResendsOnlyTheRowsTheFenceRefused(t *testing.T) {
	for _, splitChunk := range []string{"false", "true"} {
		t.Run("splitChunkProxy="+splitChunk, func(t *testing.T) {
			old := Params.ProxyCfg.SplitChunkProxy.SwapTempValue(splitChunk)
			t.Cleanup(func() { Params.ProxyCfg.SplitChunkProxy.SwapTempValue(old) })
			pre, post := twoShardSplit()
			f := newSplitFenceFixture(t, pre, post)
			wal := installSplitFenceTestWAL(t, splitSource)
			pks := seqPKs(64)
			task := f.insertTask(pks)

			require.NoError(t, task.Execute(context.Background()))
			require.True(t, merr.Ok(task.result.GetStatus()), task.result.GetStatus().GetReason())

			require.Len(t, wal.batches, 2)
			assert.Equal(t, []string{splitSource, splitSibling}, wal.vchannelsOf(0))
			assert.Equal(t, []string{splitTarget0, splitTarget1}, wal.vchannelsOf(1), "the sibling's rows landed already")
			for _, msg := range wal.batches[1] {
				for _, rowID := range streamingmessage.MustAsMutableInsertMessageV1(msg).MustBody().GetRowIDs() {
					assert.Equal(t, splitSource, splitOwner(t, pre, pks[rowID-1]), "only the source's rows are re-sent")
				}
			}
			assertRowsLandedOnceOnTheirOwner(t, wal, post, pks, 1)
		})
	}
}

// The fence lands before the routing commit; a refresh in between still routes
// the refused rows to the source. They are held back instead of being sent
// there to be refused again.
func TestInsertExecuteHoldsRowsBackUntilTheRoutingCommitIsVisible(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	f.staleRefreshes = 1
	wal := installSplitFenceTestWAL(t, splitSource)
	pks := seqPKs(32)
	task := f.insertTask(pks)

	require.NoError(t, task.Execute(context.Background()))
	require.True(t, merr.Ok(task.result.GetStatus()), task.result.GetStatus().GetReason())

	require.Len(t, wal.batches, 3)
	assert.Equal(t, []string{splitSource}, wal.vchannelsOf(0))
	assert.Empty(t, wal.batches[1], "the stale routing names only the fenced source")
	wal.assertNoAppendToTheSourceAfterItRefused(t)
	assertRowsLandedOnceOnTheirOwner(t, wal, post, pks, 1)
	assert.Equal(t, 2, f.evictions)
}

// A fence is final and the routing commit that follows it is driven forward
// until it lands, so a write keeps refreshing for as long as its deadline
// allows rather than for a fixed number of attempts.
func TestInsertExecuteRetriesUntilTheRoutingCommitIsVisible(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	f.staleRefreshes = 4
	wal := installSplitFenceTestWAL(t, splitSource)
	pks := seqPKs(16)
	task := f.insertTask(pks)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, task.Execute(ctx))
	require.True(t, merr.Ok(task.result.GetStatus()), task.result.GetStatus().GetReason())
	assert.Equal(t, 5, f.evictions)
	wal.assertNoAppendToTheSourceAfterItRefused(t)
	assertRowsLandedOnceOnTheirOwner(t, wal, post, pks, 1)
}

func TestInsertExecuteFailsRetriablyOnceTheDeadlineIsSpent(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	f.staleRefreshes = 1 << 30
	wal := installSplitFenceTestWAL(t, splitSource)
	task := f.insertTask(seqPKs(8))

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	require.NoError(t, task.Execute(ctx))
	st := task.result.GetStatus()
	assert.Equal(t, merr.Code(merr.ErrServiceUnavailable), st.GetCode(), st.GetReason())
	assert.True(t, st.GetRetriable())

	require.GreaterOrEqual(t, len(wal.batches), 2, "the write retried until its deadline")
	assert.Equal(t, []string{splitSource}, wal.vchannelsOf(0))
	wal.assertNoAppendToTheSourceAfterItRefused(t)
}

// A request with no deadline stops after proxy.shardSplit.maxFenceRetryWait.
func TestInsertExecuteWithoutADeadlineGivesUpAfterTheMaxFenceRetryWait(t *testing.T) {
	useSingleMessageRepack(t)
	old := Params.ProxyCfg.ShardSplitMaxFenceRetryWait.SwapTempValue("500ms")
	t.Cleanup(func() { Params.ProxyCfg.ShardSplitMaxFenceRetryWait.SwapTempValue(old) })
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	f.staleRefreshes = 1 << 30
	installSplitFenceTestWAL(t, splitSource)
	task := f.insertTask(seqPKs(8))

	start := time.Now()
	require.NoError(t, task.Execute(context.Background()))
	assert.Less(t, time.Since(start), 10*time.Second)
	st := task.result.GetStatus()
	assert.Equal(t, merr.Code(merr.ErrServiceUnavailable), st.GetCode(), st.GetReason())
	assert.True(t, st.GetRetriable())
	assert.GreaterOrEqual(t, f.evictions, 1)
}

func TestInsertExecuteDoesNotRetryAnErrorThatIsNotAFence(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := twoShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	wal := installSplitFenceTestWAL(t, splitSource)
	wal.failing[splitSibling] = errors.New("boom")
	task := f.insertTask(seqPKs(16))

	require.NoError(t, task.Execute(context.Background()))
	assert.False(t, merr.Ok(task.result.GetStatus()))
	assert.Contains(t, task.result.GetStatus().GetReason(), "boom")
	assert.Len(t, wal.batches, 1)
	assert.Zero(t, f.evictions)
}

// A split collection routes by the channel list its table was derived from,
// whatever list the task resolved earlier.
func TestInsertExecuteRoutesASplitCollectionByTheListOfItsTable(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	wal := installSplitFenceTestWAL(t, splitSource)
	pks := seqPKs(16)
	task := f.insertTask(pks)
	task.vChannels = pre.VChannels
	f.committed = true

	require.NoError(t, task.Execute(context.Background()))
	require.True(t, merr.Ok(task.result.GetStatus()), task.result.GetStatus().GetReason())
	require.Len(t, wal.batches, 1)
	assert.Equal(t, []string{splitTarget0, splitTarget1}, wal.vchannelsOf(0))
	assertRowsLandedOnceOnTheirOwner(t, wal, post, pks, 1)
	assert.Zero(t, f.evictions)
}

func TestInsertExecuteReportsARoutingReadFailureWithoutAppending(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	f.infoErr = errors.New("describe failed")
	wal := installSplitFenceTestWAL(t, splitSource)
	task := f.insertTask(seqPKs(4))

	assert.ErrorContains(t, task.Execute(context.Background()), "describe failed")
	assert.Contains(t, task.result.GetStatus().GetReason(), "describe failed")
	assert.Empty(t, wal.batches)
	assert.Zero(t, f.evictions)
}

func TestInsertExecuteReportsAChannelReadFailureOnRetry(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	f.staleRefreshes = 1
	f.channelErr = errors.New("channels unavailable")
	f.failChannelReadAt = 2 // the first attempt's read succeeds, the retry's fails
	wal := installSplitFenceTestWAL(t, splitSource)
	task := f.insertTask(seqPKs(4))

	assert.ErrorContains(t, task.Execute(context.Background()), "channels unavailable")
	assert.Contains(t, task.result.GetStatus().GetReason(), "channels unavailable")
	assert.Len(t, wal.batches, 1)
}

// A collection with a routing modulus has been split; the legacy modulo over
// its grown channel list places rows on shards that do not own them. Without a
// routing table -- malformed meta the cache refused to derive -- the write is
// rejected rather than routed by position.
func TestInsertExecuteRejectsASplitCollectionWithoutARoutingTable(t *testing.T) {
	useSingleMessageRepack(t)
	pre, _ := oneShardSplit()
	malformed := splitCollectionInfo(splitFenceTestCollectionID, 4, []string{splitSource},
		splitShardInfo(schemapb.ShardState_ShardNormal, splitSource, 0))
	f := newSplitFenceFixture(t, pre, malformed)
	f.committed = true
	wal := installSplitFenceTestWAL(t, splitSource)
	task := f.insertTask(seqPKs(8))

	assert.ErrorIs(t, task.Execute(context.Background()), merr.ErrServiceInternal)
	st := task.result.GetStatus()
	assert.Equal(t, merr.Code(merr.ErrServiceInternal), st.GetCode(), st.GetReason())
	assert.False(t, st.GetRetriable())
	assert.Empty(t, wal.batches, "nothing is routed by the legacy modulo")
}

func TestUpsertAppendResendsFencedRowsAndTombstonesToTheirOwners(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := twoShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	wal := installSplitFenceTestWAL(t, splitSource)
	pks := seqPKs(32)
	task := f.upsertTask(t, pks, pks)

	require.NoError(t, task.appendUpsertAttempt(context.Background(), nil))

	require.Len(t, wal.batches, 2)
	assert.Equal(t, []string{splitSource, splitSibling}, wal.vchannelsOf(0))
	wal.assertNoAppendToTheSourceAfterItRefused(t)
	assertRowsLandedOnceOnTheirOwner(t, wal, post, pks, 101)
	assertTombstonesLandedOnTheirOwner(t, wal, post, pks)
	assert.Equal(t, wal.tick, task.result.GetTimestamp())
}

// An upsert whose insert half lands whole but part of whose delete half is
// refused re-sends only the refused tombstones: a tombstone re-sent to the
// sibling that already committed would take a later tick and delete the row
// this upsert just wrote there.
func TestUpsertAppendRetriesOnlyTheRefusedTombstones(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := twoShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	wal := installSplitFenceTestWAL(t, splitSource)

	// every inserted key hashes to the sibling, the deleted keys span both shards.
	var insertPKs []int64
	for pk := int64(1); len(insertPKs) < 8; pk++ {
		if splitOwner(t, pre, pk) == splitSibling {
			insertPKs = append(insertPKs, pk)
		}
	}
	deletePKs := seqPKs(16)
	task := f.upsertTask(t, insertPKs, deletePKs)

	require.NoError(t, task.appendUpsertAttempt(context.Background(), nil))

	require.Len(t, wal.batches, 2)
	for _, msg := range wal.batches[1] {
		assert.Equal(t, streamingmessage.MessageTypeDelete, msg.MessageType(), "the insert half landed already")
	}
	assert.NotContains(t, wal.vchannelsOf(1), splitSibling, "the sibling committed its tombstones already")
	wal.assertNoAppendToTheSourceAfterItRefused(t)
	assertRowsLandedOnceOnTheirOwner(t, wal, post, insertPKs, 101)
	assertTombstonesLandedOnTheirOwner(t, wal, post, deletePKs)
	wal.assertLiveOnTheirOwner(t, post, insertPKs)
}

// The upsert's own rows survive a fence: its insert and delete of one key share
// a transaction and a tick, and a retry never re-sends a tombstone to a
// vchannel whose transaction already committed.
func TestUpsertAppendDoesNotDeleteTheRowsItWroteAcrossARetry(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := twoShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	wal := installSplitFenceTestWAL(t, splitSource)
	pks := seqPKs(32)
	task := f.upsertTask(t, pks, pks)

	require.NoError(t, task.appendUpsertAttempt(context.Background(), nil))

	require.Len(t, wal.batches, 2)
	assert.Contains(t, wal.vchannelsOf(0), splitSibling, "the first attempt committed on the sibling")
	wal.assertLiveOnTheirOwner(t, post, pks)
}

func TestUpsertAppendFailsRetriablyOnceTheDeadlineIsSpent(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	f.staleRefreshes = 1 << 30
	wal := installSplitFenceTestWAL(t, splitSource)
	pks := seqPKs(4)
	task := f.upsertTask(t, pks, pks)

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	err := task.appendUpsertAttempt(ctx, nil)
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
	assert.True(t, merr.IsRetryableErr(err))
	assert.GreaterOrEqual(t, len(wal.batches), 2, "the write retried until its deadline")
	wal.assertNoAppendToTheSourceAfterItRefused(t)
}

func TestUpsertAppendReportsARoutingReadFailureWithoutAppending(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	f.infoErr = errors.New("describe failed")
	wal := installSplitFenceTestWAL(t, splitSource)
	task := f.upsertTask(t, seqPKs(4), seqPKs(4))

	err := task.appendUpsertAttempt(context.Background(), nil)
	assert.ErrorContains(t, err, "describe failed")
	assert.Empty(t, wal.batches)
	assert.Zero(t, f.evictions)
}

func TestUpsertAppendDoesNotRetryAnErrorThatIsNotAFence(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := twoShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	wal := installSplitFenceTestWAL(t, splitSource)
	wal.failing[splitSibling] = errors.New("boom")
	task := f.upsertTask(t, seqPKs(8), seqPKs(8))

	err := task.appendUpsertAttempt(context.Background(), nil)
	assert.ErrorContains(t, err, "boom")
	assert.Len(t, wal.batches, 1)
	assert.Zero(t, f.evictions)
}

// A partial update binds CAS proofs to the vchannels it read, so it does not
// re-route what a fence refused: it evicts the collection and fails retriably,
// and the client's retry reads and writes against the targets.
func TestPartialUpdateRefusedByAFenceEvictsAndFailsRetriably(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	wal := installSplitFenceTestWAL(t, splitSource)
	pks := seqPKs(4)
	task := f.upsertTask(t, pks, pks)
	task.req.PartialUpdate = true
	task.partialUpdateCASGroups = map[string]*messagespb.PartialUpdateCAS{splitSource: {ReadTs: 10, ObservedPchannelTerm: 1}}

	err := task.appendUpsertAttempt(context.Background(), nil)
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
	assert.True(t, merr.IsRetryableErr(err))
	assert.Len(t, wal.batches, 1, "a partial update is not re-routed")
	assert.Equal(t, 1, f.evictions)

	assert.NoError(t, task.partialUpdateFenceRefusal(context.Background(), appendResponses(nil)))
	cas := status.NewPartialUpdateRetryable("cas")
	assert.Equal(t, cas, task.partialUpdateFenceRefusal(context.Background(), appendResponses(nil, cas)))
}

func TestDeleteExecuteResendsTombstonesToTheTargetOwningTheirKey(t *testing.T) {
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	wal := installSplitFenceTestWAL(t, splitSource)
	pks := seqPKs(32)
	task := f.deleteTask(pks)

	require.NoError(t, task.Execute(context.Background()))

	require.Len(t, wal.batches, 2)
	assert.Equal(t, []string{splitSource}, wal.vchannelsOf(0))
	wal.assertNoAppendToTheSourceAfterItRefused(t)
	assertTombstonesLandedOnTheirOwner(t, wal, post, pks)
	assert.EqualValues(t, len(pks), task.count, "a retried delete counts its rows once")
	assert.Equal(t, wal.tick, task.sessionTS)
	assert.Equal(t, post.VChannels, task.vChannels)
}

// A delete settles per message: it re-sends only the tombstones a fence
// refused. One re-sent to the sibling that already committed would take a
// later tick and delete a row that another request inserted there in between.
func TestDeleteExecuteNeverResendsATombstoneToACommittedVChannel(t *testing.T) {
	pre, post := twoShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	wal := installSplitFenceTestWAL(t, splitSource)
	pks := seqPKs(16)
	var reinserted int64
	for _, pk := range pks {
		if splitOwner(t, pre, pk) == splitSibling {
			reinserted = pk
			break
		}
	}
	require.NotZero(t, reinserted)
	f.onEvict = func(eviction int) {
		if eviction == 1 {
			// a concurrent insert of the key commits on the sibling after the
			// delete's tombstone did.
			wal.tick++
			wal.insertAt(splitSibling, reinserted, wal.tick)
		}
	}
	task := f.deleteTask(pks)

	require.NoError(t, task.Execute(context.Background()))

	require.Len(t, wal.batches, 2)
	assert.Contains(t, wal.vchannelsOf(0), splitSibling)
	assert.NotContains(t, wal.vchannelsOf(1), splitSibling, "the sibling committed its tombstones already")
	wal.assertNoAppendToTheSourceAfterItRefused(t)
	assertTombstonesLandedOnTheirOwner(t, wal, post, pks)
	wal.assertLiveOnTheirOwner(t, post, []int64{reinserted})
	assert.EqualValues(t, len(pks), task.count, "a retried delete counts its rows once")
}

func TestDeleteExecuteFailsRetriablyOnceTheDeadlineIsSpent(t *testing.T) {
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	f.staleRefreshes = 1 << 30
	wal := installSplitFenceTestWAL(t, splitSource)
	task := f.deleteTask(seqPKs(4))

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	err := task.Execute(ctx)
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
	assert.True(t, merr.IsRetryableErr(err))
	assert.Zero(t, task.count)
	assert.GreaterOrEqual(t, len(wal.batches), 2, "the write retried until its deadline")
	wal.assertNoAppendToTheSourceAfterItRefused(t)
}

func TestDeleteExecuteDoesNotRetryAnErrorThatIsNotAFence(t *testing.T) {
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	wal := installSplitFenceTestWAL(t)
	wal.failing[splitSource] = errors.New("boom")
	task := f.deleteTask(seqPKs(4))

	err := task.Execute(context.Background())
	assert.ErrorContains(t, err, "boom")
	assert.Len(t, wal.batches, 1)
	assert.Zero(t, f.evictions)
}

func TestDeleteExecuteReportsARoutingReadFailure(t *testing.T) {
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	f.infoErr = errors.New("describe failed")
	wal := installSplitFenceTestWAL(t, splitSource)

	err := f.deleteTask(seqPKs(4)).Execute(context.Background())
	assert.ErrorContains(t, err, "describe failed")
	assert.Empty(t, wal.batches)
}

func TestDeleteExecuteReportsAChannelReadFailureOnRetry(t *testing.T) {
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	f.staleRefreshes = 1
	f.channelErr = errors.New("channels unavailable")
	f.failChannelReadAt = 1 // a delete reads its channels again only on a retry
	wal := installSplitFenceTestWAL(t, splitSource)

	err := f.deleteTask(seqPKs(4)).Execute(context.Background())
	assert.ErrorContains(t, err, "channels unavailable")
	assert.Len(t, wal.batches, 1)
}

// A delete task that resolved its channels before the routing commit still
// routes a split collection by the list its table was derived from.
func TestDeleteExecuteRoutesASplitCollectionByTheListOfItsTable(t *testing.T) {
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	wal := installSplitFenceTestWAL(t, splitSource)
	pks := seqPKs(16)
	task := f.deleteTask(pks) // resolved its channels before the commit
	f.committed = true

	require.NoError(t, task.Execute(context.Background()))
	require.Len(t, wal.batches, 1)
	assert.Equal(t, []string{splitTarget0, splitTarget1}, wal.vchannelsOf(0))
	assertTombstonesLandedOnTheirOwner(t, wal, post, pks)
	assert.Zero(t, f.evictions)
}

func TestDeleteExecuteRejectsASplitCollectionWithoutARoutingTable(t *testing.T) {
	pre, _ := oneShardSplit()
	malformed := splitCollectionInfo(splitFenceTestCollectionID, 4, []string{splitSource},
		splitShardInfo(schemapb.ShardState_ShardNormal, splitSource, 0))
	f := newSplitFenceFixture(t, pre, malformed)
	f.committed = true
	wal := installSplitFenceTestWAL(t, splitSource)

	err := f.deleteTask(seqPKs(8)).Execute(context.Background())
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
	assert.Empty(t, wal.batches)
}

// A delete by expression queries the keys it matches and deletes them batch by
// batch; each batch is a delete task that settles per message exactly like a
// delete by primary key: only the tombstones a fence refused are re-sent, and
// none to a vchannel whose transaction committed.
func TestDeleteByExpressionSettlesEachBatchPerMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pre, post := twoShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	wal := installSplitFenceTestWAL(t, splitSource)

	queue, err := scheduler.NewTaskScheduler(ctx, &mockTsoAllocator{})
	require.NoError(t, err)
	queue.Start()
	defer queue.Close()

	schema := mustNewSchemaInfo(&schemapb.CollectionSchema{
		Name: "collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.StartOfUserFieldID, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: common.StartOfUserFieldID + 1, Name: "non_pk", DataType: schemapb.DataType_Int64},
		},
	})
	expr := "non_pk > 0"
	plan, err := planparserv2.CreateRetrievePlan(schema.SchemaHelper, expr, nil)
	require.NoError(t, err)

	batches := [][]int64{seqPKs(8), {9, 10, 11, 12, 13, 14, 15, 16}}
	var pks []int64
	for _, batch := range batches {
		pks = append(pks, batch...)
	}
	qn := mocks.NewMockQueryNodeClient(t)
	qn.EXPECT().QueryStream(mock.Anything, mock.Anything).Call.Return(
		func(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) querypb.QueryNode_QueryStreamClient {
			client := streamrpc.NewLocalQueryClient(ctx)
			server := client.CreateServer()
			for _, batch := range batches {
				server.Send(&internalpb.RetrieveResults{Status: merr.Success(), Ids: int64IDs(batch...)})
			}
			server.FinishSend(nil)
			return client
		}, nil)
	lb := shardclient.NewMockLBPolicy(t)
	lb.EXPECT().Execute(mock.Anything, mock.Anything).Call.Return(func(ctx context.Context, workload shardclient.CollectionWorkLoad) error {
		return workload.Exec(ctx, 1, qn, "")
	})

	dr := deleteRunner{
		queue:           queue.DmQueue,
		metaCache:       f.cache,
		chMgr:           f.chMgr(),
		schema:          schema,
		collectionID:    splitFenceTestCollectionID,
		partitionIDs:    []int64{200},
		vChannels:       pre.VChannels,
		idAllocator:     allocator.NewLocalAllocator(1, 1<<30),
		tsoAllocatorIns: &mockTsoAllocator{},
		lb:              lb,
		result:          &milvuspb.MutationResult{Status: merr.Success(), IDs: &schemapb.IDs{}},
		req: &milvuspb.DeleteRequest{
			DbName:         "db",
			CollectionName: "collection",
			Expr:           expr,
		},
		plan: plan,
	}
	require.NoError(t, dr.Run(ctx))

	assert.EqualValues(t, len(pks), dr.result.DeleteCnt, "each tombstone is counted once")
	assertTombstonesLandedOnTheirOwner(t, wal, post, pks)
	// Every batch committed its sibling tombstones in its first append; none is
	// ever re-sent there, and nothing goes back to the fenced source.
	siblingAppends := 0
	for batch := range wal.batches {
		if lo.Contains(wal.vchannelsOf(batch), splitSibling) {
			siblingAppends++
		}
	}
	assert.Equal(t, len(batches), siblingAppends)
	for batch := range wal.batches {
		if lo.Contains(wal.vchannelsOf(batch), splitSource) {
			assert.NotContains(t, wal.vchannelsOf(batch), splitTarget0, "a batch reaches the source only before the refresh")
		}
	}
}

// keyedInsertTask is an idempotent insert of pks under key, whose ids are the
// primary keys themselves.
func (f *splitFenceFixture) keyedInsertTask(pks []int64, key string) *insertTask {
	task := f.insertTask(pks)
	task.idempotencyEnabled = true
	task.idempotencyKey = key
	return task
}

func idempotentResult(offsets []uint32, ids ...int64) *messagespb.IdempotentInsertResult {
	return streamingmessage.NewIdempotentInsertResult(offsets, int64IDs(ids...))
}

// A keyed retry whose earlier attempt landed on one shard is answered there
// from the idempotency window: its rows are not written again, and the result
// carries the ids of the first attempt.
func TestInsertExecuteMergesTheIdempotentDuplicateOfAnEarlierAttempt(t *testing.T) {
	useSingleMessageRepack(t)
	pre, _ := twoShardSplit()
	f := newSplitFenceFixture(t, pre, pre)
	wal := installSplitFenceTestWAL(t)
	pks := seqPKs(16)
	var siblingOffsets []uint32
	var siblingIDs []int64
	for i, pk := range pks {
		if splitOwner(t, pre, pk) == splitSibling {
			siblingOffsets = append(siblingOffsets, uint32(i))
			siblingIDs = append(siblingIDs, 1000+pk)
		}
	}
	require.NotEmpty(t, siblingOffsets)
	wal.windows[splitSibling] = map[string]*splitFenceTestWindowEntry{
		"key": {tick: 7, result: idempotentResult(siblingOffsets, siblingIDs...)},
	}
	task := f.keyedInsertTask(pks, "key")

	require.NoError(t, task.Execute(context.Background()))
	require.True(t, merr.Ok(task.result.GetStatus()), task.result.GetStatus().GetReason())
	assert.Empty(t, wal.insertedRowIDs[splitSibling], "the sibling answered from its window")
	assert.NotEmpty(t, wal.insertedRowIDs[splitSource])
	ids := task.result.GetIDs().GetIntId().GetData()
	for i, offset := range siblingOffsets {
		assert.Equal(t, siblingIDs[i], ids[offset])
	}
}

// A key reused with a payload of another shape cannot be merged; the data the
// key wrote exists, so the request fails as an input error, not as a write
// failure.
func TestInsertExecuteReportsAKeyReusedWithADifferentPayload(t *testing.T) {
	useSingleMessageRepack(t)
	pre, _ := oneShardSplit()
	f := newSplitFenceFixture(t, pre, pre)
	wal := installSplitFenceTestWAL(t)
	wal.windows[splitSource] = map[string]*splitFenceTestWindowEntry{
		"key": {tick: 7, result: idempotentResult([]uint32{100}, 1)},
	}
	task := f.keyedInsertTask(seqPKs(4), "key")

	require.NoError(t, task.Execute(context.Background()))
	st := task.result.GetStatus()
	assert.Equal(t, merr.Code(merr.ErrParameterInvalid), st.GetCode(), st.GetReason())
	assert.Empty(t, wal.insertedRowIDs[splitSource])
}

// A never-split collection's first attempt keeps the channel list PreExecute
// resolved.
func TestInsertExecuteKeepsTheChannelsPreExecuteResolved(t *testing.T) {
	useSingleMessageRepack(t)
	pre, _ := twoShardSplit()
	f := newSplitFenceFixture(t, pre, pre)
	wal := installSplitFenceTestWAL(t)
	pks := seqPKs(8)
	task := f.insertTask(pks)
	task.vChannels = pre.VChannels

	require.NoError(t, task.Execute(context.Background()))
	require.True(t, merr.Ok(task.result.GetStatus()), task.result.GetStatus().GetReason())
	assert.Zero(t, f.channelReads)
	assertRowsLandedOnceOnTheirOwner(t, wal, pre, pks, 1)
}

func TestInsertExecuteReportsARepackFailureWithoutAppending(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	f.partitionErr = errors.New("partition lookup failed")
	wal := installSplitFenceTestWAL(t)
	task := f.insertTask(seqPKs(4))

	assert.ErrorContains(t, task.Execute(context.Background()), "partition lookup failed")
	assert.Contains(t, task.result.GetStatus().GetReason(), "partition lookup failed")
	assert.Empty(t, wal.batches)
}

func TestUpsertAppendReportsAPackFailureWithoutAppending(t *testing.T) {
	useSingleMessageRepack(t)
	t.Run("insert half", func(t *testing.T) {
		pre, post := oneShardSplit()
		f := newSplitFenceFixture(t, pre, post)
		f.partitionErr = errors.New("partition lookup failed")
		wal := installSplitFenceTestWAL(t)
		task := f.upsertTask(t, seqPKs(4), seqPKs(4))

		assert.ErrorContains(t, task.appendUpsertAttempt(context.Background(), nil), "partition lookup failed")
		assert.Empty(t, wal.batches)
	})
	t.Run("delete half", func(t *testing.T) {
		old := Params.QuotaConfig.MaxDeleteSize.SwapTempValue("1")
		t.Cleanup(func() { Params.QuotaConfig.MaxDeleteSize.SwapTempValue(old) })
		pre, post := oneShardSplit()
		f := newSplitFenceFixture(t, pre, post)
		wal := installSplitFenceTestWAL(t)
		task := f.upsertTask(t, seqPKs(4), seqPKs(4))

		assert.ErrorIs(t, task.appendUpsertAttempt(context.Background(), nil), merr.ErrParameterTooLarge)
		assert.Empty(t, wal.batches)
	})
}

func TestDeleteExecuteReportsARepackFailureWithoutAppending(t *testing.T) {
	old := Params.QuotaConfig.MaxDeleteSize.SwapTempValue("1")
	t.Cleanup(func() { Params.QuotaConfig.MaxDeleteSize.SwapTempValue(old) })
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	wal := installSplitFenceTestWAL(t)

	err := f.deleteTask(seqPKs(4)).Execute(context.Background())
	assert.ErrorIs(t, err, merr.ErrParameterTooLarge)
	assert.Empty(t, wal.batches)
}

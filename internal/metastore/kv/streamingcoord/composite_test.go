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

package streamingcoord

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// Frozen copies of today's key layout, so a key-scheme change trips the
// byte-parity tests below instead of silently moving data.
const (
	frozenReplicateConfigKey   = "streamingcoord-meta/replicate-configuration"
	frozenReplicatePChannelPfx = "streamingcoord-meta/replicating-pchannel/"
	frozenPChannelPfx          = "streamingcoord-meta/pchannel/"
	frozenBroadcastTaskPfx     = "streamingcoord-meta/broadcast-task/"
)

// streamingMetaKV adapts memkv.MemoryKV (a TxnKV) to the kv.MetaKv surface the
// streamingcoord catalog holds; the extra MetaKv methods are never exercised
// by the write paths under test.
type streamingMetaKV struct {
	*memkv.MemoryKV
}

func (streamingMetaKV) GetPath(key string) string { return key }

func (streamingMetaKV) CompareVersionAndSwap(ctx context.Context, key string, version int64, target string) (bool, error) {
	panic("not used by streamingcoord composite tests")
}

func (streamingMetaKV) WalkWithPrefix(ctx context.Context, prefix string, paginationSize int, fn func([]byte, []byte) error) error {
	panic("not used by streamingcoord composite tests")
}

// scWriteRecordingKV records every write-path call (and its payload) so a test
// can assert call shapes and cross-batch ordering.
type scWriteRecordingKV struct {
	streamingMetaKV
	maxTxnOps        int // 0 = underlying (unlimited)
	calls            []string
	multiSaveBatches [][]string          // sorted key set of each MultiSave call, in call order
	txnSaves         []map[string]string // saves of each MultiSaveAndRemove call, in call order
	txnRemovals      [][]string          // removals of each MultiSaveAndRemove call, in call order
}

func (w *scWriteRecordingKV) MaxTxnOps() int {
	if w.maxTxnOps > 0 {
		return w.maxTxnOps
	}
	return w.streamingMetaKV.MaxTxnOps()
}

func (w *scWriteRecordingKV) Save(ctx context.Context, key, value string) error {
	w.calls = append(w.calls, "Save")
	return w.MemoryKV.Save(ctx, key, value)
}

func (w *scWriteRecordingKV) Remove(ctx context.Context, key string) error {
	w.calls = append(w.calls, "Remove")
	return w.MemoryKV.Remove(ctx, key)
}

func (w *scWriteRecordingKV) MultiSave(ctx context.Context, kvs map[string]string) error {
	w.calls = append(w.calls, "MultiSave")
	keys := make([]string, 0, len(kvs))
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	w.multiSaveBatches = append(w.multiSaveBatches, keys)
	return w.MemoryKV.MultiSave(ctx, kvs)
}

func (w *scWriteRecordingKV) MultiSaveAndRemove(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	w.calls = append(w.calls, "MultiSaveAndRemove")
	w.txnSaves = append(w.txnSaves, saves)
	w.txnRemovals = append(w.txnRemovals, removals)
	return w.MemoryKV.MultiSaveAndRemove(ctx, saves, removals, preds...)
}

// scConfigMarkerCrashKV shrinks MaxTxnOps so SaveReplicateConfiguration takes
// the chunked fallback and fails the final guarded commit txn - the only
// MultiSaveAndRemove carrying the config key - simulating a crash right before
// the visibility marker lands.
type scConfigMarkerCrashKV struct {
	streamingMetaKV
	failures int
}

func (f *scConfigMarkerCrashKV) MaxTxnOps() int { return 2 }

func (f *scConfigMarkerCrashKV) MultiSaveAndRemove(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	if f.failures > 0 {
		if _, ok := saves[frozenReplicateConfigKey]; ok {
			f.failures--
			return errors.New("injected crash")
		}
	}
	return f.MemoryKV.MultiSaveAndRemove(ctx, saves, removals, preds...)
}

// scSecondChunkCrashKV shrinks MaxTxnOps so a put run splits into chunks and
// fails only the given MultiSave call, simulating a crash between chunks.
type scSecondChunkCrashKV struct {
	streamingMetaKV
	saveCalls  int
	failAtCall int
}

func (f *scSecondChunkCrashKV) MaxTxnOps() int { return 2 }

func (f *scSecondChunkCrashKV) MultiSave(ctx context.Context, kvs map[string]string) error {
	f.saveCalls++
	if f.saveCalls == f.failAtCall {
		return errors.New("injected crash")
	}
	return f.MemoryKV.MultiSave(ctx, kvs)
}

// scFlakyTxnKV fails the first MultiSaveAndRemove with a transient error; used
// UNDER the production ReliableWriteMetaKv wrapper to prove the wrapper still
// retries the engine's txn calls to success.
type scFlakyTxnKV struct {
	streamingMetaKV
	failures int
}

func (f *scFlakyTxnKV) MultiSaveAndRemove(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	if f.failures > 0 {
		f.failures--
		return errors.New("transient error")
	}
	return f.MemoryKV.MultiSaveAndRemove(ctx, saves, removals, preds...)
}

func dumpStore(t *testing.T, store *memkv.MemoryKV) map[string]string {
	keys, vals, err := store.LoadWithPrefix(context.TODO(), "")
	require.NoError(t, err)
	got := make(map[string]string, len(keys))
	for i, key := range keys {
		got[key] = vals[i]
	}
	return got
}

// legacySaveReplicateConfiguration is a frozen copy of today's (pre-engine)
// write encoding: one flat MultiSave of the config record plus every
// replicating-pchannel record. The engine-backed path must reproduce these
// keys and bytes exactly.
func legacySaveReplicateConfiguration(t *testing.T, store *memkv.MemoryKV, config *streamingpb.ReplicateConfigurationMeta, tasks []*streamingpb.ReplicatePChannelMeta) {
	v, err := proto.Marshal(config)
	require.NoError(t, err)
	kvs := map[string]string{frozenReplicateConfigKey: string(v)}
	for _, task := range tasks {
		tv, err := proto.Marshal(task)
		require.NoError(t, err)
		kvs[frozenReplicatePChannelPfx+task.GetTargetCluster().GetClusterId()+"-"+task.GetSourceChannelName()] = string(tv)
	}
	require.NoError(t, store.MultiSave(context.TODO(), kvs))
}

// legacySavePChannels is a frozen copy of today's SavePChannels encoding.
func legacySavePChannels(t *testing.T, store *memkv.MemoryKV, infos []*streamingpb.PChannelMeta) {
	kvs := make(map[string]string, len(infos))
	for _, info := range infos {
		v, err := proto.Marshal(info)
		require.NoError(t, err)
		kvs[frozenPChannelPfx+info.GetChannel().GetName()] = string(v)
	}
	require.NoError(t, store.MultiSave(context.TODO(), kvs))
}

func replicateFixture(n int) (*streamingpb.ReplicateConfigurationMeta, []*streamingpb.ReplicatePChannelMeta) {
	config := &streamingpb.ReplicateConfigurationMeta{
		ReplicateConfiguration: &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "source-cluster", Pchannels: []string{"source-channel-1"}},
				{ClusterId: "target-cluster", Pchannels: []string{"target-channel-1"}},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{
				{SourceClusterId: "source-cluster", TargetClusterId: "target-cluster"},
			},
		},
	}
	tasks := make([]*streamingpb.ReplicatePChannelMeta, 0, n)
	for i := 0; i < n; i++ {
		tasks = append(tasks, &streamingpb.ReplicatePChannelMeta{
			SourceChannelName: fmt.Sprintf("source-channel-%d", i),
			TargetChannelName: fmt.Sprintf("target-channel-%d", i),
			TargetCluster:     &commonpb.MilvusCluster{ClusterId: "target-cluster"},
		})
	}
	return config, tasks
}

func pchannelFixture(n int) []*streamingpb.PChannelMeta {
	infos := make([]*streamingpb.PChannelMeta, 0, n)
	for i := 0; i < n; i++ {
		infos = append(infos, &streamingpb.PChannelMeta{
			Channel: &streamingpb.PChannelInfo{Name: fmt.Sprintf("pchannel-%d", i), Term: int64(i + 1)},
			Node:    &streamingpb.StreamingNodeInfo{ServerId: 1},
		})
	}
	return infos
}

// TestCatalog_SaveReplicateConfiguration_SingleTxnMatchesLegacyBytes proves
// the config record and every replicating-pchannel record land in ONE
// transaction (today: raw MultiSave batches with no atomicity across the
// batch boundary) and byte-for-byte identical to today's encoding.
func TestCatalog_SaveReplicateConfiguration_SingleTxnMatchesLegacyBytes(t *testing.T) {
	ctx := context.TODO()
	config, tasks := replicateFixture(2)

	legacyStore := memkv.NewMemoryKV()
	legacySaveReplicateConfiguration(t, legacyStore, config, tasks)

	rec := &scWriteRecordingKV{streamingMetaKV: streamingMetaKV{memkv.NewMemoryKV()}}
	c := NewCataLog(rec)
	require.NoError(t, c.SaveReplicateConfiguration(ctx, config, tasks))

	assert.Equal(t, []string{"MultiSaveAndRemove"}, rec.calls)
	require.Len(t, rec.txnRemovals, 1)
	assert.Empty(t, rec.txnRemovals[0])
	assert.Equal(t, dumpStore(t, legacyStore), dumpStore(t, rec.MemoryKV))
}

// TestCatalog_SaveReplicateConfiguration_FallbackOrderConfigLast proves that
// over the txn limit the replicating-pchannel records flush first, chunked in
// input order, and the config record - the composite's visibility marker -
// lands alone in the final guarded txn.
func TestCatalog_SaveReplicateConfiguration_FallbackOrderConfigLast(t *testing.T) {
	ctx := context.TODO()
	config, tasks := replicateFixture(4)

	legacyStore := memkv.NewMemoryKV()
	legacySaveReplicateConfiguration(t, legacyStore, config, tasks)

	rec := &scWriteRecordingKV{streamingMetaKV: streamingMetaKV{memkv.NewMemoryKV()}, maxTxnOps: 2}
	c := NewCataLog(rec)
	require.NoError(t, c.SaveReplicateConfiguration(ctx, config, tasks))

	assert.Equal(t, []string{"MultiSave", "MultiSave", "MultiSaveAndRemove"}, rec.calls)
	taskKey := func(i int) string {
		return frozenReplicatePChannelPfx + tasks[i].GetTargetCluster().GetClusterId() + "-" + tasks[i].GetSourceChannelName()
	}
	// task chunks preserve the input order across batches.
	assert.Equal(t, [][]string{
		{taskKey(0), taskKey(1)},
		{taskKey(2), taskKey(3)},
	}, rec.multiSaveBatches)
	// the final guarded txn carries only the config marker.
	require.Len(t, rec.txnSaves, 1)
	assert.Len(t, rec.txnSaves[0], 1)
	assert.Contains(t, rec.txnSaves[0], frozenReplicateConfigKey)
	require.Len(t, rec.txnRemovals, 1)
	assert.Empty(t, rec.txnRemovals[0])

	assert.Equal(t, dumpStore(t, legacyStore), dumpStore(t, rec.MemoryKV))
}

// TestCatalog_SaveReplicateConfiguration_FallbackMarkerCrashRetry: on the
// chunked fallback path the config record is the commit marker of the whole
// configuration update. A crash before the final commit txn must leave the OLD
// config bytes visible (new task records sit inert), and the retry must
// converge to the legacy end state.
//
// The catalog is built WITHOUT the ReliableWriteMetaKv wrapper: the injected
// failure models a process crash, which kills the wrapper's in-process retry
// loop as well; the wrapper would otherwise absorb the injection.
func TestCatalog_SaveReplicateConfiguration_FallbackMarkerCrashRetry(t *testing.T) {
	ctx := context.TODO()
	oldConfig, oldTasks := replicateFixture(1)
	newConfig, newTasks := replicateFixture(4)
	newConfig.ForcePromoted = true // make the new config bytes differ from the old

	legacyStore := memkv.NewMemoryKV()
	legacySaveReplicateConfiguration(t, legacyStore, oldConfig, oldTasks)
	legacySaveReplicateConfiguration(t, legacyStore, newConfig, newTasks)

	fk := &scConfigMarkerCrashKV{streamingMetaKV: streamingMetaKV{memkv.NewMemoryKV()}, failures: 1}
	legacySaveReplicateConfiguration(t, fk.MemoryKV, oldConfig, oldTasks)
	before := dumpStore(t, fk.MemoryKV)

	c := &catalog{metaKV: fk}
	save := func() error { return c.SaveReplicateConfiguration(ctx, newConfig, newTasks) }
	assert.Error(t, save())
	// crash-safety: the marker has not flipped - config bytes are still old.
	got := dumpStore(t, fk.MemoryKV)
	assert.Equal(t, before[frozenReplicateConfigKey], got[frozenReplicateConfigKey])

	assert.NoError(t, save())
	assert.Equal(t, dumpStore(t, legacyStore), dumpStore(t, fk.MemoryKV))
}

// TestCatalog_SavePChannels_SingleTxnMatchesLegacyBytes proves a pchannel
// batch within the txn limit lands in ONE transaction, byte-for-byte identical
// to today's encoding.
func TestCatalog_SavePChannels_SingleTxnMatchesLegacyBytes(t *testing.T) {
	ctx := context.TODO()
	infos := pchannelFixture(3)

	legacyStore := memkv.NewMemoryKV()
	legacySavePChannels(t, legacyStore, infos)

	rec := &scWriteRecordingKV{streamingMetaKV: streamingMetaKV{memkv.NewMemoryKV()}}
	c := NewCataLog(rec)
	require.NoError(t, c.SavePChannels(ctx, infos))

	assert.Equal(t, []string{"MultiSaveAndRemove"}, rec.calls)
	require.Len(t, rec.txnRemovals, 1)
	assert.Empty(t, rec.txnRemovals[0])
	assert.Equal(t, dumpStore(t, legacyStore), dumpStore(t, rec.MemoryKV))
}

// TestCatalog_SavePChannels_ChunkedKeepsInputOrder proves an over-limit batch
// is flushed against the store's own txn limit in input-slice order, not the
// map-iteration order of the legacy paramtable-sized batching.
func TestCatalog_SavePChannels_ChunkedKeepsInputOrder(t *testing.T) {
	ctx := context.TODO()
	infos := pchannelFixture(5)

	rec := &scWriteRecordingKV{streamingMetaKV: streamingMetaKV{memkv.NewMemoryKV()}, maxTxnOps: 2}
	c := NewCataLog(rec)
	require.NoError(t, c.SavePChannels(ctx, infos))

	assert.Equal(t, []string{"MultiSave", "MultiSave", "MultiSave"}, rec.calls)
	assert.Equal(t, [][]string{
		{frozenPChannelPfx + "pchannel-0", frozenPChannelPfx + "pchannel-1"},
		{frozenPChannelPfx + "pchannel-2", frozenPChannelPfx + "pchannel-3"},
		{frozenPChannelPfx + "pchannel-4"},
	}, rec.multiSaveBatches)
}

// TestCatalog_SavePChannels_ChunkCrashRetry: a crash between chunks must leave
// earlier chunks persisted and later ones absent, and the retry (the caller
// re-issues the same batch) must converge to the legacy end state. Built
// without the ReliableWriteMetaKv wrapper - see the marker-crash test above.
func TestCatalog_SavePChannels_ChunkCrashRetry(t *testing.T) {
	ctx := context.TODO()
	infos := pchannelFixture(5)

	legacyStore := memkv.NewMemoryKV()
	legacySavePChannels(t, legacyStore, infos)

	fk := &scSecondChunkCrashKV{streamingMetaKV: streamingMetaKV{memkv.NewMemoryKV()}, failAtCall: 2}
	c := &catalog{metaKV: fk}
	save := func() error { return c.SavePChannels(ctx, infos) }
	assert.Error(t, save())
	got := dumpStore(t, fk.MemoryKV)
	// first chunk persisted, everything after the crash absent.
	assert.Contains(t, got, frozenPChannelPfx+"pchannel-0")
	assert.Contains(t, got, frozenPChannelPfx+"pchannel-1")
	assert.NotContains(t, got, frozenPChannelPfx+"pchannel-2")
	assert.NotContains(t, got, frozenPChannelPfx+"pchannel-3")
	assert.NotContains(t, got, frozenPChannelPfx+"pchannel-4")

	assert.NoError(t, save())
	assert.Equal(t, dumpStore(t, legacyStore), dumpStore(t, fk.MemoryKV))
}

// TestCatalog_SaveCChannelAndVersion_MatchesLegacyBytes pins the
// canonical-write-plus-legacy-trailing-slash-removal encoding: one
// MultiSaveAndRemove whose saves and removals are unchanged by the engine
// move.
func TestCatalog_SaveCChannelAndVersion_MatchesLegacyBytes(t *testing.T) {
	ctx := context.TODO()

	t.Run("cchannel", func(t *testing.T) {
		info := &streamingpb.CChannelMeta{Pchannel: "test-channel"}
		v, err := proto.Marshal(info)
		require.NoError(t, err)

		rec := &scWriteRecordingKV{streamingMetaKV: streamingMetaKV{memkv.NewMemoryKV()}}
		require.NoError(t, rec.MemoryKV.Save(ctx, canonicalCChannelMetaKeyForTest+"/", "legacy"))
		c := NewCataLog(rec)
		require.NoError(t, c.SaveCChannel(ctx, info))

		assert.Equal(t, []string{"MultiSaveAndRemove"}, rec.calls)
		assert.Equal(t, map[string]string{canonicalCChannelMetaKeyForTest: string(v)}, dumpStore(t, rec.MemoryKV))
	})

	t.Run("version", func(t *testing.T) {
		version := &streamingpb.StreamingVersion{Version: 7}
		v, err := proto.Marshal(version)
		require.NoError(t, err)

		rec := &scWriteRecordingKV{streamingMetaKV: streamingMetaKV{memkv.NewMemoryKV()}}
		require.NoError(t, rec.MemoryKV.Save(ctx, canonicalVersionKeyForTest+"/", "legacy"))
		c := NewCataLog(rec)
		require.NoError(t, c.SaveVersion(ctx, version))

		assert.Equal(t, []string{"MultiSaveAndRemove"}, rec.calls)
		assert.Equal(t, map[string]string{canonicalVersionKeyForTest: string(v)}, dumpStore(t, rec.MemoryKV))
	})
}

// TestCatalog_SaveBroadcastTask_EngineWriteMatchesLegacyBytes proves the save
// branch goes through the engine's guarded txn while keeping today's key and
// bytes, and the DONE branch stays a plain Remove (legacy compat logic).
func TestCatalog_SaveBroadcastTask_EngineWriteMatchesLegacyBytes(t *testing.T) {
	ctx := context.TODO()
	task := &streamingpb.BroadcastTask{State: streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING}
	v, err := proto.Marshal(task)
	require.NoError(t, err)

	rec := &scWriteRecordingKV{streamingMetaKV: streamingMetaKV{memkv.NewMemoryKV()}}
	c := NewCataLog(rec)
	require.NoError(t, c.SaveBroadcastTask(ctx, 1, task))
	assert.Equal(t, []string{"MultiSaveAndRemove"}, rec.calls)
	assert.Equal(t, map[string]string{frozenBroadcastTaskPfx + "1": string(v)}, dumpStore(t, rec.MemoryKV))

	// DONE branch: unchanged legacy Remove.
	require.NoError(t, c.SaveBroadcastTask(ctx, 1, &streamingpb.BroadcastTask{
		State: streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE,
	}))
	assert.Equal(t, []string{"MultiSaveAndRemove", "Remove"}, rec.calls)
	assert.Empty(t, dumpStore(t, rec.MemoryKV))
}

// TestCatalog_SaveReplicateConfiguration_ReliableRetryPreserved proves the
// ReliableWriteMetaKv wrapper still guards the engine's txn calls after the
// move: a transient MultiSaveAndRemove failure is retried inside the call and
// the composite write completes with today's bytes.
func TestCatalog_SaveReplicateConfiguration_ReliableRetryPreserved(t *testing.T) {
	ctx := context.TODO()
	config, tasks := replicateFixture(2)

	legacyStore := memkv.NewMemoryKV()
	legacySaveReplicateConfiguration(t, legacyStore, config, tasks)

	fk := &scFlakyTxnKV{streamingMetaKV: streamingMetaKV{memkv.NewMemoryKV()}, failures: 1}
	c := NewCataLog(fk)
	require.NoError(t, c.SaveReplicateConfiguration(ctx, config, tasks))
	assert.Equal(t, dumpStore(t, legacyStore), dumpStore(t, fk.MemoryKV))
}

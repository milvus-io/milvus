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

package manifest

import (
	"context"
	"fmt"
	"maps"
	"math/rand"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// The tests use vchannel vch of collection 1; its pchannel is pch.
const (
	pch = "p"
	vch = "p_1v0"
)

const testRoot = "files"

func newTestManager() (*Manager, *FakeMetaStore, *FakeObjectStore) {
	p, s := NewFakeMetaStore(), NewFakeObjectStore()
	return New(p, s, testRoot), p, s
}

// flush returns a flush of vch of collection 1 with an Int64 primary key.
func flush(term int64, tick uint64, ids ...int64) *pkindexpb.CommitFlushRequest {
	return flushOn(vch, term, tick, ids...)
}

// flushOn returns a flush of vchannel of collection 1 with an Int64 primary key.
func flushOn(vchannel string, term int64, tick uint64, ids ...int64) *pkindexpb.CommitFlushRequest {
	req := &pkindexpb.CommitFlushRequest{
		CollectionId: 1, Vchannel: vchannel, PchannelTerm: term,
		CheckpointTimeTick: tick,
	}
	for _, id := range ids {
		req.AddSsts = append(req.AddSsts, &pkindexpb.SSTRef{Id: id})
	}
	return req
}

// get returns the current manifest of a vchannel as m sees it, and false when
// the vchannel has no manifest.
func get(ctx context.Context, m *Manager, collectionID int64, vchannel string) (*pkindexpb.Manifest, bool, error) {
	cur, err := m.load(ctx, PointerKey(collectionID, vchannel))
	if err != nil || cur == nil {
		return nil, false, err
	}
	return cur.pb, true, nil
}

func sstIDs(m *pkindexpb.Manifest) []int64 {
	out := []int64{}
	for _, s := range m.GetSsts() {
		out = append(out, s.GetId())
	}
	return out
}

// bodyKeys returns the manifest body keys of vch in the store.
func bodyKeys(t *testing.T, s *FakeObjectStore) []string {
	keys := []string{}
	err := s.Walk(context.Background(), pkindex.ManifestDir(testRoot, 1, vch)+"/", func(k string, _ time.Time) bool {
		keys = append(keys, k)
		return true
	})
	require.NoError(t, err)
	return keys
}

// assigned returns a manager on new stores whose pch is assigned at term in
// the pchannel meta.
func assigned(t *testing.T, term int64) (*Manager, *FakeMetaStore, *FakeObjectStore) {
	m, p, s := newTestManager()
	p.AssignPChannel(pch, term)
	return m, p, s
}

// A flush checks the pchannel meta but does not write it.
func TestFlushDoesNotWritePChannelMeta(t *testing.T) {
	m, p, _ := assigned(t, 3)
	_, err := m.CommitFlush(context.Background(), flush(3, 50, 9))
	require.NoError(t, err)
	assert.Len(t, p.LastSave, 1)
	assert.Contains(t, p.LastSave, PointerKey(1, vch))
}

// The first flush of a vchannel creates version 1 with the SST base path of
// the vchannel.
func TestFirstFlushCreatesVersion1(t *testing.T) {
	m, p, s := assigned(t, 3)
	ctx := context.Background()
	got, err := m.CommitFlush(ctx, flush(3, 50, 9))
	require.NoError(t, err)
	assert.EqualValues(t, 1, got.GetVersion())
	assert.Equal(t, "files/pk_index/1/p_1v0/sst", got.GetSstBasePath())
	assert.Equal(t, pkindex.SSTBasePath("files", 1, vch), got.GetSstBasePath())
	assert.EqualValues(t, 3, got.GetLastFlushTerm())
	assert.Equal(t, []int64{9}, sstIDs(got))

	// a new manager reads the same state back from the stores
	read, found, err := get(ctx, New(p, s, testRoot), 1, vch)
	require.NoError(t, err)
	require.True(t, found)
	assert.True(t, proto.Equal(got, read))
}

func TestFlushTermRules(t *testing.T) {
	ctx := context.Background()
	m, _, _ := newTestManager()
	_, err := m.CommitFlush(ctx, flush(1, 10))
	assert.True(t, errors.Is(err, ErrTermNotAssigned), "no pchannel meta")

	m, _, _ = assigned(t, 3)
	_, err = m.CommitFlush(ctx, flush(2, 10, 1))
	assert.True(t, errors.Is(err, ErrFenced), "a lower term is fenced")
	_, err = m.CommitFlush(ctx, flush(4, 10, 1))
	assert.True(t, errors.Is(err, ErrTermNotAssigned), "a higher term is not in the pchannel meta")
	_, found, err := get(ctx, m, 1, vch)
	require.NoError(t, err)
	assert.False(t, found)
}

// A flush term above the term of the first read of the pchannel meta reads
// the meta again.
func TestFlushHigherTermReadsAgain(t *testing.T) {
	m, p, _ := assigned(t, 3)
	p.AfterLoad = func() { p.AssignPChannel(pch, 4) }
	got, err := m.CommitFlush(context.Background(), flush(4, 10, 1))
	require.NoError(t, err)
	assert.EqualValues(t, 4, got.GetLastFlushTerm())
}

// The predicate of a flush holds only for the stored bytes of the pchannel
// meta that the term was checked against.
func TestCheckPChannelTermPredicate(t *testing.T) {
	ctx := context.Background()
	p := NewFakeMetaStore()
	p.AssignPChannel(pch, 3)
	key := streamingcoord.BuildPChannelInfoPath(pch)
	raw, _, err := p.Load(ctx, key)
	require.NoError(t, err)

	pred, err := checkPChannelTerm(ctx, p, pch, 3)
	require.NoError(t, err)
	assert.Equal(t, key, pred.Key())
	assert.True(t, pred.IsTrue(raw))

	p.PutPChannelMeta(pch, 3, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNAVAILABLE)
	changed, _, err := p.Load(ctx, key)
	require.NoError(t, err)
	assert.False(t, pred.IsTrue(changed))
}

// The pchannel meta does not decode, has term 0, or cannot be read.
func TestPChannelMetaLoadErrors(t *testing.T) {
	ctx := context.Background()
	m, p, _ := assigned(t, 3)

	p.LoadErr = errors.New("etcd down")
	_, err := m.CommitFlush(ctx, flush(3, 10))
	assert.True(t, errors.Is(err, pkindex.ErrUnavailable))

	zeroTerm, err := proto.Marshal(&streamingpb.PChannelMeta{Channel: &streamingpb.PChannelInfo{Name: pch}})
	require.NoError(t, err)
	for _, v := range []string{"\xff", "", string(zeroTerm)} {
		p.Put(streamingcoord.BuildPChannelInfoPath(pch), v)
		_, err = m.CommitFlush(ctx, flush(3, 10))
		assert.True(t, errors.Is(err, pkindex.ErrCorrupted), "meta %q", v)
	}
}

// A flush whose checkpoint does not move forward is a retry when its term is
// the last flush term, and a regression otherwise.
func TestFlushRetryVersusRegression(t *testing.T) {
	m, p, _ := assigned(t, 3)
	ctx := context.Background()
	first, err := m.CommitFlush(ctx, flush(3, 50, 9))
	require.NoError(t, err)

	again, err := m.CommitFlush(ctx, flush(3, 50, 9))
	require.NoError(t, err)
	assert.True(t, proto.Equal(first, again))

	p.AssignPChannel(pch, 4)
	_, err = m.CommitFlush(ctx, flush(4, 50, 10))
	assert.True(t, errors.Is(err, ErrCheckpointRegressed))
	_, err = m.CommitFlush(ctx, flush(3, 60, 10))
	assert.True(t, errors.Is(err, ErrFenced))

	up, err := m.CommitFlush(ctx, flush(4, 60, 10))
	require.NoError(t, err)
	assert.EqualValues(t, 2, up.GetVersion())
	assert.EqualValues(t, 4, up.GetLastFlushTerm())
}

// Every flush reads the pchannel meta: a flush sees a new term at once.
func TestFlushReadsPChannelMetaEveryTime(t *testing.T) {
	m, p, _ := assigned(t, 3)
	ctx := context.Background()
	_, err := m.CommitFlush(ctx, flush(3, 10, 1))
	require.NoError(t, err)
	p.AssignPChannel(pch, 4)

	_, err = m.CommitFlush(ctx, flush(3, 20, 2))
	assert.True(t, errors.Is(err, ErrFenced))
	got, err := m.CommitFlush(ctx, flush(4, 20, 2))
	require.NoError(t, err)
	assert.Equal(t, []int64{2, 1}, sstIDs(got))
}

// The pchannel meta changes between the term check of a flush and its write:
// the predicate fails, the error is retryable, and the retry checks the term
// again.
func TestFlushRetriesWhenPChannelMetaChangesBeforeWrite(t *testing.T) {
	t.Run("new term", func(t *testing.T) {
		m, p, s := assigned(t, 3)
		ctx := context.Background()
		_, err := m.CommitFlush(ctx, flush(3, 10, 1))
		require.NoError(t, err)

		p.BeforeWrite = func() { p.AssignPChannel(pch, 4) }
		_, err = m.CommitFlush(ctx, flush(3, 20, 2))
		assert.True(t, errors.Is(err, pkindex.ErrUnavailable), "%v", err)
		assert.True(t, errors.Is(err, ErrFakeValueMismatch), "%v", err)

		_, err = m.CommitFlush(ctx, flush(3, 20, 2))
		assert.True(t, errors.Is(err, ErrFenced))
		got, _, err := get(ctx, New(p, s, testRoot), 1, vch)
		require.NoError(t, err)
		assert.Equal(t, []int64{1}, sstIDs(got))
	})
	t.Run("same term", func(t *testing.T) {
		m, p, s := assigned(t, 3)
		ctx := context.Background()
		_, err := m.CommitFlush(ctx, flush(3, 10, 1))
		require.NoError(t, err)

		p.BeforeWrite = func() {
			p.PutPChannelMeta(pch, 3, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNAVAILABLE)
		}
		_, err = m.CommitFlush(ctx, flush(3, 20, 2))
		assert.True(t, errors.Is(err, pkindex.ErrUnavailable), "%v", err)
		assert.True(t, errors.Is(err, ErrFakeValueMismatch), "%v", err)

		got, err := m.CommitFlush(ctx, flush(3, 20, 2))
		require.NoError(t, err)
		assert.EqualValues(t, 2, got.GetVersion())
		assert.Equal(t, []int64{2, 1}, sstIDs(got))
		read, _, err := get(ctx, New(p, s, testRoot), 1, vch)
		require.NoError(t, err)
		assert.True(t, proto.Equal(got, read))
	})
}

func TestCommitCompaction(t *testing.T) {
	m, p, _ := assigned(t, 1)
	ctx := context.Background()
	_, err := m.CommitCompaction(ctx, 1, vch, Compaction{Inputs: []int64{1}, Outputs: runOut(5, 2)})
	assert.True(t, errors.Is(err, ErrNoManifest), "no manifest before the first flush")

	_, err = m.CommitFlush(ctx, flush(1, 10, 3, 2, 1))
	require.NoError(t, err)
	got, err := m.CommitCompaction(ctx, 1, vch, Compaction{Inputs: []int64{2, 1}, Outputs: runOut(6, 10)})
	require.NoError(t, err)
	assert.EqualValues(t, 2, got.GetVersion())
	assert.Equal(t, []int64{3, 10}, sstIDs(got))

	_, err = m.CommitCompaction(ctx, 1, vch, Compaction{Inputs: []int64{2, 1}, Outputs: runOut(7, 11)})
	assert.True(t, errors.Is(err, ErrStaleEdit))

	// a compaction does not read or check the pchannel meta
	p.AssignPChannel(pch, 9)
	p.LoadErr, p.LoadErrKey = errors.New("must not be read"), streamingcoord.BuildPChannelInfoPath(pch)
	got, err = m.CommitCompaction(ctx, 1, vch, Compaction{Inputs: []int64{10}, Outputs: runOut(8, 11)})
	require.NoError(t, err)
	assert.Equal(t, []int64{3, 11}, sstIDs(got))
	assert.Equal(t, []string{PointerKey(1, vch)}, slices.Collect(maps.Keys(p.LastSave)))
	p.LoadErr, p.LoadErrKey = nil, ""
}

// The pointer swap reports an error after it took effect: the manager reads
// the pointer back and reports success.
func TestSwapUncertainButApplied(t *testing.T) {
	m, p, s := assigned(t, 1)
	ctx := context.Background()
	_, err := m.CommitFlush(ctx, flush(1, 1))
	require.NoError(t, err)
	p.WriteErr, p.WriteApplies = errors.New("timeout"), true
	got, err := m.CommitFlush(ctx, flush(1, 5, 1))
	require.NoError(t, err)
	assert.EqualValues(t, 2, got.GetVersion())

	read, _, err := get(ctx, New(p, s, testRoot), 1, vch)
	require.NoError(t, err)
	assert.True(t, proto.Equal(got, read))
}

// The caller's ctx ends during the pointer swap and the swap took effect: the
// read back does not use the ended ctx, and the commit reports success.
func TestSwapUncertainAfterCallerCtxEnds(t *testing.T) {
	m, p, s := assigned(t, 1)
	_, err := m.CommitFlush(context.Background(), flush(1, 1))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.BeforeWrite = cancel
	p.WriteErr, p.WriteApplies = context.Canceled, true
	got, err := m.CommitFlush(ctx, flush(1, 5, 1))
	require.NoError(t, err)
	assert.EqualValues(t, 2, got.GetVersion())

	read, _, err := get(context.Background(), New(p, s, testRoot), 1, vch)
	require.NoError(t, err)
	assert.True(t, proto.Equal(got, read))
}

// The first pointer write reports an error after it took effect: the manager
// reads the pointer back and reports success.
func TestCreateUncertainButApplied(t *testing.T) {
	m, p, s := assigned(t, 1)
	ctx := context.Background()
	p.WriteErr, p.WriteApplies = errors.New("timeout"), true
	got, err := m.CommitFlush(ctx, flush(1, 5, 1))
	require.NoError(t, err)
	assert.EqualValues(t, 1, got.GetVersion())

	read, found, err := get(ctx, New(p, s, testRoot), 1, vch)
	require.NoError(t, err)
	require.True(t, found)
	assert.True(t, proto.Equal(got, read))
}

// The first pointer write fails and did not take effect: the error is
// retryable and the next flush succeeds.
func TestCreateFailedIsRetryable(t *testing.T) {
	m, p, _ := assigned(t, 1)
	ctx := context.Background()
	cause := errors.New("timeout")
	p.WriteErr = cause
	_, err := m.CommitFlush(ctx, flush(1, 5, 1))
	assert.True(t, errors.Is(err, pkindex.ErrUnavailable))
	assert.True(t, errors.Is(err, cause), "the cause stays in the chain")

	got, err := m.CommitFlush(ctx, flush(1, 5, 1))
	require.NoError(t, err)
	assert.EqualValues(t, 1, got.GetVersion())
}

// The pointer swap fails and did not take effect: the error is retryable and
// the next commit succeeds.
func TestSwapFailedIsRetryable(t *testing.T) {
	m, p, _ := assigned(t, 1)
	ctx := context.Background()
	_, err := m.CommitFlush(ctx, flush(1, 1))
	require.NoError(t, err)
	cause := errors.New("timeout")
	p.WriteErr = cause
	_, err = m.CommitFlush(ctx, flush(1, 5, 1))
	assert.True(t, errors.Is(err, pkindex.ErrUnavailable))
	assert.True(t, errors.Is(err, cause), "the cause stays in the chain")
	got, err := m.CommitFlush(ctx, flush(1, 5, 1))
	require.NoError(t, err)
	assert.EqualValues(t, 2, got.GetVersion())
	assert.Equal(t, []int64{1}, sstIDs(got))
}

// The pointer swap took effect, but the read back fails: the commit reports a
// retryable error and drops its cache, so the retry loads the applied version.
func TestReadBackFails(t *testing.T) {
	m, p, _ := assigned(t, 1)
	ctx := context.Background()
	_, err := m.CommitFlush(ctx, flush(1, 1))
	require.NoError(t, err)
	p.WriteErr, p.WriteApplies = errors.New("timeout"), true
	p.LoadErr = errors.New("etcd down")
	_, err = m.CommitFlush(ctx, flush(1, 5, 1))
	assert.True(t, errors.Is(err, pkindex.ErrUnavailable))
	assert.Contains(t, fmt.Sprintf("%+v", err), "etcd down", "the read-back error is kept")

	got, err := m.CommitFlush(ctx, flush(1, 5, 1))
	require.NoError(t, err)
	assert.EqualValues(t, 2, got.GetVersion())
	assert.Equal(t, []int64{1}, sstIDs(got))
}

// A crash between the body write and the pointer swap leaves the old version
// current and an orphan body in the store.
func TestCrashBeforeSwap(t *testing.T) {
	m, p, s := assigned(t, 1)
	ctx := context.Background()
	old, err := m.CommitFlush(ctx, flush(1, 1))
	require.NoError(t, err)
	require.Len(t, bodyKeys(t, s), 1)

	p.WriteErr = errors.New("crash")
	_, err = m.CommitFlush(ctx, flush(1, 5, 1))
	require.Error(t, err)

	got, found, err := get(ctx, New(p, s, testRoot), 1, vch)
	require.NoError(t, err)
	require.True(t, found)
	assert.True(t, proto.Equal(old, got))

	keys := bodyKeys(t, s)
	require.Len(t, keys, 2)
	var orphans int
	for _, k := range keys {
		v, ok := pkindex.ParseManifestBodyKey(pkindex.ManifestDir(testRoot, 1, vch), k)
		require.True(t, ok)
		if v == 2 {
			orphans++
		}
	}
	assert.Equal(t, 1, orphans)
}

// Two managers on shared stores both create the first version: exactly one
// create succeeds, the other gets a retryable error, and its retry applies
// its flush on top of the winner's version.
func TestTwoManagersCreateFirstVersion(t *testing.T) {
	a, p, s := assigned(t, 1)
	b := New(p, s, testRoot)
	ctx := context.Background()

	var aGot *pkindexpb.Manifest
	var aErr error
	// b has loaded "not found" and written its body; a creates the first
	// version before b writes the pointer
	p.BeforeWrite = func() { aGot, aErr = a.CommitFlush(ctx, flush(1, 5, 1)) }
	_, err := b.CommitFlush(ctx, flush(1, 6, 2))
	assert.True(t, errors.Is(err, pkindex.ErrUnavailable))
	require.NoError(t, aErr)
	assert.EqualValues(t, 1, aGot.GetVersion())

	got, err := b.CommitFlush(ctx, flush(1, 6, 2))
	require.NoError(t, err)
	assert.EqualValues(t, 2, got.GetVersion())
	assert.Equal(t, []int64{2, 1}, sstIDs(got))

	read, found, err := get(ctx, New(p, s, testRoot), 1, vch)
	require.NoError(t, err)
	require.True(t, found)
	assert.True(t, proto.Equal(got, read))
}

// A pointer whose body has another version is corrupted.
func TestLoadVersionMismatch(t *testing.T) {
	m, p, s := assigned(t, 1)
	ctx := context.Background()
	body, err := proto.Marshal(&pkindexpb.Manifest{CollectionId: 1, Vchannel: vch, Version: 5})
	require.NoError(t, err)
	bodyKey := pkindex.ManifestBodyKey(pkindex.ManifestDir(testRoot, 1, vch), 5, "x")
	require.NoError(t, s.Write(ctx, bodyKey, body))
	ptr, err := proto.Marshal(&pkindexpb.ManifestPointer{Version: 6, BodyKey: bodyKey})
	require.NoError(t, err)
	p.Put(PointerKey(1, vch), string(ptr))

	_, _, err = get(ctx, m, 1, vch)
	assert.True(t, errors.Is(err, pkindex.ErrCorrupted))
	_, err = m.CommitFlush(ctx, flush(1, 5, 1))
	assert.True(t, errors.Is(err, pkindex.ErrCorrupted))
}

func TestLoadErrors(t *testing.T) {
	ctx := context.Background()
	m, p, s := assigned(t, 1)
	_, err := m.CommitFlush(ctx, flush(1, 1))
	require.NoError(t, err)

	// a fresh manager has no cache, so every read goes to the stores
	t.Run("pointer read fails", func(t *testing.T) {
		p.LoadErr = errors.New("etcd down")
		_, _, err := get(ctx, New(p, s, testRoot), 1, vch)
		assert.True(t, errors.Is(err, pkindex.ErrUnavailable))
	})
	t.Run("body read fails", func(t *testing.T) {
		s.ReadErr = errors.New("s3 down")
		_, _, err := get(ctx, New(p, s, testRoot), 1, vch)
		assert.True(t, errors.Is(err, pkindex.ErrUnavailable))
		assert.False(t, errors.Is(err, pkindex.ErrCorrupted))
	})
	t.Run("body missing", func(t *testing.T) {
		require.NoError(t, s.MultiRemove(ctx, bodyKeys(t, s)))
		_, _, err := get(ctx, New(p, s, testRoot), 1, vch)
		assert.True(t, errors.Is(err, pkindex.ErrCorrupted))
		assert.Equal(t, merr.Code(merr.ErrDataIntegrity), merr.Code(pkindex.ToMerr(err)), "a missing body is not retryable")
	})
	t.Run("pointer undecodable", func(t *testing.T) {
		p.Put(PointerKey(1, vch), "\xff")
		_, _, err := get(ctx, New(p, s, testRoot), 1, vch)
		assert.True(t, errors.Is(err, pkindex.ErrCorrupted))
	})
	t.Run("pointer names no body", func(t *testing.T) {
		zeroVersion, err := proto.Marshal(&pkindexpb.ManifestPointer{BodyKey: pkindex.ManifestBodyKey(pkindex.ManifestDir(testRoot, 1, vch), 1, "x")})
		require.NoError(t, err)
		for _, ptr := range []string{"", string(zeroVersion)} {
			p.Put(PointerKey(1, vch), ptr)
			// an object store rejects an empty key as a request error, not as a missing object
			s.ReadErr = errors.New("object name cannot be empty")
			_, _, err := get(ctx, New(p, s, testRoot), 1, vch)
			s.ReadErr = nil
			assert.True(t, errors.Is(err, pkindex.ErrCorrupted), "pointer %q", ptr)
			assert.False(t, errors.Is(err, pkindex.ErrUnavailable), "pointer %q", ptr)
		}
	})
}

func TestBodyWriteFails(t *testing.T) {
	m, p, s := assigned(t, 1)
	ctx := context.Background()
	s.WriteErr = errors.New("s3 down")
	_, err := m.CommitFlush(ctx, flush(1, 5, 1))
	assert.True(t, errors.Is(err, pkindex.ErrUnavailable))
	_, found, err := get(ctx, New(p, s, testRoot), 1, vch)
	require.NoError(t, err)
	assert.False(t, found)
}

// Edits on one manager from many goroutines apply one at a time: all succeed
// and none is lost.
func TestConcurrentEdits(t *testing.T) {
	m, p, s := assigned(t, 1)
	ctx := context.Background()
	const compacts, flushes = 16, 16
	base := make([]int64, 0, compacts)
	for i := compacts; i >= 1; i-- {
		base = append(base, int64(i))
	}
	_, err := m.CommitFlush(ctx, flush(1, 10, base...))
	require.NoError(t, err)
	// the L0 tables are compacted oldest first into one run each; the
	// concurrent compactions then replace these runs
	for i := 1; i <= compacts; i++ {
		_, err := m.CommitCompaction(ctx, 1, vch, Compaction{Inputs: []int64{int64(i)}, Outputs: runOut(int64(100+i), int64(500+i))})
		require.NoError(t, err)
	}

	var wg sync.WaitGroup
	for i := 1; i <= compacts; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			_, err := m.CommitCompaction(ctx, 1, vch, Compaction{Inputs: []int64{500 + id}, Outputs: runOut(300+id, 1000+id)})
			assert.NoError(t, err)
		}(int64(i))
	}
	// flushes need an increasing checkpoint, so one goroutine sends them in order
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 1; j <= flushes; j++ {
			_, err := m.CommitFlush(ctx, flush(1, uint64(10+j), int64(2000+j)))
			assert.NoError(t, err)
		}
	}()
	wg.Wait()

	got, found, err := get(ctx, New(p, s, testRoot), 1, vch)
	require.NoError(t, err)
	require.True(t, found)
	// the first flush, then every concurrent edit
	assert.EqualValues(t, 1+2*compacts+flushes, got.GetVersion())
	assert.EqualValues(t, 10+flushes, got.GetCheckpointTimeTick())
	want := []int64{}
	for j := flushes; j >= 1; j-- {
		want = append(want, int64(2000+j))
	}
	for i := compacts; i >= 1; i-- {
		want = append(want, int64(1000+i))
	}
	assert.Equal(t, want, sstIDs(got))
}

// Flushes of the vchannels of one pchannel run in parallel without errors.
func TestParallelVChannelsOfOnePChannel(t *testing.T) {
	m, _, _ := assigned(t, 1)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(v string) {
			defer wg.Done()
			for j := 1; j <= 10; j++ {
				_, err := m.CommitFlush(ctx, flushOn(v, 1, uint64(j), int64(j)))
				assert.NoError(t, err)
			}
		}(fmt.Sprintf("%s_1v%d", pch, i))
	}
	wg.Wait()
}

// Two managers on the same stores model two active coordinators. Each commit
// either succeeds or returns a retryable error. The final pointer names a body
// with the pointer's version that holds exactly the SSTs of the applied
// commits.
func TestTwoWriters(t *testing.T) {
	a, p, s := assigned(t, 1)
	b := New(p, s, testRoot)
	ctx := context.Background()
	_, err := a.CommitFlush(ctx, flush(1, 1))
	require.NoError(t, err)

	type result struct {
		id      int64
		tick    uint64
		applied bool
	}
	results := make([][]result, 2)
	var wg sync.WaitGroup
	for i, mgr := range []*Manager{a, b} {
		wg.Add(1)
		go func(i int, mgr *Manager) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				id := int64(i*1000 + j)
				tick := uint64(id + 10)
				got, err := mgr.CommitFlush(ctx, flush(1, tick, id))
				if err != nil {
					assert.True(t, errors.Is(err, pkindex.ErrUnavailable), "%v", err)
					continue
				}
				// a flush whose checkpoint is not after the current one is an
				// applied retry and returns the current version without its SST
				applied := false
				for _, sid := range sstIDs(got) {
					applied = applied || sid == id
				}
				results[i] = append(results[i], result{id: id, tick: tick, applied: applied})
			}
		}(i, mgr)
	}
	wg.Wait()

	got, found, err := get(ctx, New(p, s, testRoot), 1, vch)
	require.NoError(t, err)
	require.True(t, found)

	want := map[int64]struct{}{}
	var maxTick uint64
	for _, rs := range results {
		for _, r := range rs {
			if r.applied {
				want[r.id] = struct{}{}
				maxTick = max(maxTick, r.tick)
			}
		}
	}
	require.NotEmpty(t, want)
	have := map[int64]struct{}{}
	for _, id := range sstIDs(got) {
		_, dup := have[id]
		assert.False(t, dup, "sst %d is listed twice", id)
		have[id] = struct{}{}
	}
	assert.Equal(t, want, have)
	assert.EqualValues(t, 1+len(want), got.GetVersion())
	assert.Equal(t, maxTick, got.GetCheckpointTimeTick())
}

// LoadPointers reads the store in batches and returns one result per target,
// in order: found, not found, and corrupted targets each get their own result.
func TestLoadPointers(t *testing.T) {
	m, p, _ := assigned(t, 1)
	ctx := context.Background()
	const n = 2*pointerBatchSize + 5
	targets := make([]Target, 0, n)
	for i := 0; i < n; i++ {
		v := fmt.Sprintf("%s_1v%d", pch, i)
		targets = append(targets, Target{CollectionID: 1, VChannel: v})
		if i%3 == 0 {
			_, err := m.CommitFlush(ctx, flushOn(v, 1, 10, int64(i+1)))
			require.NoError(t, err)
		}
	}
	p.Put(PointerKey(1, targets[1].VChannel), "\xff")

	p.MultiLoads = 0
	results := m.LoadPointers(ctx, targets)
	require.Len(t, results, n)
	assert.Equal(t, 3, p.MultiLoads, "%d targets are read in batches of %d", n, pointerBatchSize)
	for i, r := range results {
		switch {
		case i == 1:
			assert.True(t, errors.Is(r.Err, pkindex.ErrCorrupted), "target %d", i)
			assert.False(t, r.Found)
		case i%3 == 0:
			require.NoError(t, r.Err, "target %d", i)
			assert.True(t, r.Found)
			assert.EqualValues(t, 1, r.Pointer.GetVersion())
			assert.Contains(t, r.Pointer.GetBodyKey(), pkindex.ManifestDir(testRoot, 1, targets[i].VChannel)+"/")
		default:
			require.NoError(t, r.Err, "target %d", i)
			assert.False(t, r.Found)
			assert.Nil(t, r.Pointer)
		}
	}
}

// The failure of one batch read fails the targets of that batch only.
func TestLoadPointersBatchFailure(t *testing.T) {
	m, p, _ := assigned(t, 1)
	ctx := context.Background()
	targets := make([]Target, 0, pointerBatchSize+1)
	for i := 0; i <= pointerBatchSize; i++ {
		targets = append(targets, Target{CollectionID: 1, VChannel: fmt.Sprintf("%s_1v%d", pch, i)})
	}
	last := targets[pointerBatchSize].VChannel
	_, err := m.CommitFlush(ctx, flushOn(last, 1, 10, 1))
	require.NoError(t, err)

	p.MultiLoadErr = errors.New("etcd down")
	results := m.LoadPointers(ctx, targets)
	for _, r := range results[:pointerBatchSize] {
		assert.True(t, errors.Is(r.Err, pkindex.ErrUnavailable))
	}
	require.NoError(t, results[pointerBatchSize].Err)
	assert.True(t, results[pointerBatchSize].Found)
}

// LoadPointers reads the store, so it sees a commit of another manager.
func TestLoadPointersIgnoresCache(t *testing.T) {
	a, p, s := assigned(t, 1)
	ctx := context.Background()
	_, err := a.CommitFlush(ctx, flush(1, 10, 1))
	require.NoError(t, err)
	got, err := New(p, s, testRoot).CommitFlush(ctx, flush(1, 20, 2))
	require.NoError(t, err)

	results := a.LoadPointers(ctx, []Target{{CollectionID: 1, VChannel: vch}})
	require.NoError(t, results[0].Err)
	assert.Equal(t, got.GetVersion(), results[0].Pointer.GetVersion())
}

// editFlush returns a flush of vchannel "v" of collection 1 with an Int64
// primary key.
func editFlush(term int64, tick uint64, ids ...int64) *pkindexpb.CommitFlushRequest {
	return &pkindexpb.CommitFlushRequest{
		CollectionId: 1, Vchannel: "v",
		PchannelTerm: term, AddSsts: ssts(ids...), CheckpointTimeTick: tick,
	}
}

// applyEditFlush applies a flush with SST base path "b".
func applyEditFlush(cur *pkindexpb.Manifest, req *pkindexpb.CommitFlushRequest) (*pkindexpb.Manifest, error) {
	return applyFlush(cur, req, "b")
}

// runOut returns the outputs of one compaction: tables with the given IDs,
// all in run runID, with ascending disjoint key ranges.
func runOut(runID int64, ids ...int64) []*pkindexpb.SSTRef {
	out := make([]*pkindexpb.SSTRef, 0, len(ids))
	for i, id := range ids {
		k := fmt.Sprintf("k%03d", i)
		out = append(out, sstRef(id, runID, k, k))
	}
	return out
}

func ssts(ids ...int64) []*pkindexpb.SSTRef {
	out := make([]*pkindexpb.SSTRef, 0, len(ids))
	for _, id := range ids {
		out = append(out, &pkindexpb.SSTRef{Id: id})
	}
	return out
}

// The first flush of a vchannel creates version 1 from the fields of the flush.
func TestFlushCreatesFirstVersion(t *testing.T) {
	m, err := applyEditFlush(nil, editFlush(5, 100, 2, 1))
	require.NoError(t, err)
	require.NotNil(t, m)
	assert.EqualValues(t, 1, m.GetVersion())
	assert.EqualValues(t, 1, m.GetCollectionId())
	assert.Equal(t, "v", m.GetVchannel())
	assert.Equal(t, "b", m.GetSstBasePath())
	assert.EqualValues(t, 5, m.GetLastFlushTerm())
	assert.EqualValues(t, 100, m.GetCheckpointTimeTick())
	assert.Equal(t, []int64{2, 1}, sstIDs(m))
}

func TestFlush(t *testing.T) {
	m1, err := applyEditFlush(nil, editFlush(5, 100, 2, 1))
	require.NoError(t, err)
	require.NotNil(t, m1)

	m2, err := applyEditFlush(m1, editFlush(5, 200, 3))
	require.NoError(t, err)
	assert.Equal(t, []int64{3, 2, 1}, sstIDs(m2), "new SSTs go first")
	assert.EqualValues(t, 2, m2.GetVersion())

	// a retry of an applied flush changes nothing
	again, err := applyEditFlush(m2, editFlush(5, 200, 3))
	require.NoError(t, err)
	assert.Nil(t, again)

	// the last flush term with an old checkpoint is an applied retry, whatever its SSTs are
	old, err := applyEditFlush(m2, editFlush(5, 150, 9))
	require.NoError(t, err)
	assert.Nil(t, old)
	_, err = applyEditFlush(m2, editFlush(5, 300, 1))
	assert.True(t, errors.Is(err, ErrDuplicateSST))

	up, err := applyEditFlush(m2, editFlush(6, 300, 9))
	require.NoError(t, err)
	assert.EqualValues(t, 6, up.GetLastFlushTerm(), "the flush term becomes the last flush term")
	assert.EqualValues(t, 5, m2.GetLastFlushTerm(), "input must not change")
	assert.Equal(t, []int64{3, 2, 1}, sstIDs(m2), "input must not change")
}

func TestFlushRetryAfterCompaction(t *testing.T) {
	m, err := applyEditFlush(nil, editFlush(5, 100, 2, 1))
	require.NoError(t, err)
	c, err := applyCompaction(m, Compaction{Inputs: []int64{2, 1}, Outputs: runOut(5, 10)})
	require.NoError(t, err)

	again, err := applyEditFlush(c, editFlush(5, 100, 2, 1))
	require.NoError(t, err)
	assert.Nil(t, again)
}

// A flush whose checkpoint does not move forward and whose term is not the
// last flush term is a regression, not a retry.
func TestFlushOtherTermOldCheckpoint(t *testing.T) {
	m, err := applyEditFlush(nil, editFlush(5, 100, 1))
	require.NoError(t, err)
	for _, term := range []int64{4, 6} {
		for _, tick := range []uint64{100, 50} {
			_, err = applyEditFlush(m, editFlush(term, tick, 1))
			assert.True(t, errors.Is(err, ErrCheckpointRegressed), "term %d tick %d", term, tick)
		}
	}
}

func TestFlushCheckpointOnly(t *testing.T) {
	m, err := applyEditFlush(nil, editFlush(5, 100, 1))
	require.NoError(t, err)

	adv, err := applyEditFlush(m, editFlush(5, 200))
	require.NoError(t, err)
	assert.NotNil(t, adv)
	assert.EqualValues(t, m.GetVersion()+1, adv.GetVersion())
	assert.EqualValues(t, 200, adv.GetCheckpointTimeTick())
	assert.Equal(t, []int64{1}, sstIDs(adv))

	// an empty flush that does not move the checkpoint is an applied retry
	same, err := applyEditFlush(adv, editFlush(5, 200))
	require.NoError(t, err)
	assert.Nil(t, same)
}

func TestFlushDuplicateInsideAddSSTs(t *testing.T) {
	_, err := applyEditFlush(nil, editFlush(5, 100, 7, 7))
	assert.True(t, errors.Is(err, ErrDuplicateSST))
}

func TestCompaction(t *testing.T) {
	m, err := applyEditFlush(nil, editFlush(1, 10, 4, 3, 2, 1))
	require.NoError(t, err)

	c, err := applyCompaction(m, Compaction{Inputs: []int64{2, 1}, Outputs: runOut(5, 10)})
	require.NoError(t, err)
	assert.NotNil(t, c)
	assert.Equal(t, []int64{4, 3, 10}, sstIDs(c))
	assert.EqualValues(t, m.GetVersion()+1, c.GetVersion())
	assert.EqualValues(t, 10, c.GetCheckpointTimeTick())
	assert.Equal(t, []int64{4, 3, 2, 1}, sstIDs(m), "input must not change")

	for _, in := range [][]int64{{3, 1}, {1, 2}, {5}, {}} {
		_, err := applyCompaction(m, Compaction{Inputs: in, Outputs: runOut(5, 10)})
		assert.True(t, errors.Is(err, ErrStaleEdit), "%v", in)
	}
	_, err = applyCompaction(m, Compaction{Inputs: []int64{2, 1}, Outputs: runOut(5, 4)})
	assert.True(t, errors.Is(err, ErrDuplicateSST))
	_, err = applyCompaction(nil, Compaction{Inputs: []int64{1}, Outputs: runOut(5, 10)})
	assert.True(t, errors.Is(err, ErrNoManifest))
}

// A retry of an applied compaction finds none of its inputs and all of its
// outputs in the list: it changes nothing.
func TestCompactionRetryOfApplied(t *testing.T) {
	m, err := applyEditFlush(nil, editFlush(1, 10, 4, 3, 2, 1))
	require.NoError(t, err)
	edit := Compaction{Inputs: []int64{2, 1}, Outputs: runOut(5, 11, 10)}
	c, err := applyCompaction(m, edit)
	require.NoError(t, err)
	require.NotNil(t, c)

	again, err := applyCompaction(c, edit)
	require.NoError(t, err)
	assert.Nil(t, again)

	// a flush after the compaction does not change the result
	f, err := applyEditFlush(c, editFlush(1, 20, 20))
	require.NoError(t, err)
	again, err = applyCompaction(f, edit)
	require.NoError(t, err)
	assert.Nil(t, again)
}

// Every other mismatch of the inputs and the outputs is a stale edit.
func TestCompactionMixedIsStale(t *testing.T) {
	m, err := applyEditFlush(nil, editFlush(1, 10, 4, 3, 2, 1))
	require.NoError(t, err)
	// list after this compaction: 4 3 11 10
	c, err := applyCompaction(m, Compaction{Inputs: []int64{2, 1}, Outputs: runOut(5, 11, 10)})
	require.NoError(t, err)

	cases := map[string]Compaction{
		"some inputs present, outputs present":  {Inputs: []int64{3, 2}, Outputs: ssts(11, 10)},
		"some inputs present, outputs absent":   {Inputs: []int64{3, 2}, Outputs: ssts(30)},
		"inputs present not contiguous":         {Inputs: []int64{4, 11}, Outputs: ssts(30)},
		"inputs absent, outputs absent":         {Inputs: []int64{2, 1}, Outputs: ssts(30)},
		"inputs absent, outputs partly present": {Inputs: []int64{2, 1}, Outputs: ssts(11, 30)},
		"no inputs, outputs present":            {Inputs: []int64{}, Outputs: ssts(11, 10)},
	}
	for name, edit := range cases {
		_, err := applyCompaction(c, edit)
		assert.True(t, errors.Is(err, ErrStaleEdit), name)
	}
}

func TestCompactionDuplicateInsideOutputs(t *testing.T) {
	m, err := applyEditFlush(nil, editFlush(1, 10, 2, 1))
	require.NoError(t, err)
	_, err = applyCompaction(m, Compaction{Inputs: []int64{2, 1}, Outputs: runOut(5, 10, 10)})
	assert.True(t, errors.Is(err, ErrDuplicateSST))
}

// Flush and Compaction commute: applying all compactions before all flushes, or
// interleaving them in any way that keeps each kind's own order, gives the same
// manifest.
func TestFlushAndCompactCommute(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for round := 0; round < 500; round++ {
		n := 2 + r.Intn(8)
		base := make([]int64, n)
		for i := range base {
			base[i] = int64(n - i)
		}
		start, err := applyEditFlush(nil, editFlush(1, 10, base...))
		require.NoError(t, err)
		// the oldest tables are single-table runs, the others are L0 tables
		l0 := r.Intn(n + 1)
		start = proto.Clone(start).(*pkindexpb.Manifest)
		for i := l0; i < n; i++ {
			start.Ssts[i].RunId = 500 + start.Ssts[i].GetId()
		}

		nextID, nextRun := int64(1000), int64(1000)
		fresh := func(count int) []*pkindexpb.SSTRef {
			ids := make([]int64, 0, count)
			for i := 0; i < count; i++ {
				ids = append(ids, nextID)
				nextID++
			}
			nextRun++
			return runOut(nextRun, ids...)
		}

		flushes := make([]*pkindexpb.CommitFlushRequest, 1+r.Intn(4))
		for i := range flushes {
			flushes[i] = editFlush(1, uint64(20+10*i))
			flushes[i].AddSsts = fresh(r.Intn(3))
		}

		// compactions take disjoint contiguous ranges of the base list: the
		// range of the L0 tables ends at the last L0 table, the others are
		// among the runs
		var compacts []Compaction
		if l0 > 0 && r.Intn(2) == 0 {
			compacts = append(compacts, Compaction{Inputs: base[r.Intn(l0):l0], Outputs: fresh(1 + r.Intn(2))})
		}
		for pos, want := l0, 1+r.Intn(3); pos < n && len(compacts) < want; {
			from := pos + r.Intn(n-pos)
			to := from + 1 + r.Intn(n-from)
			compacts = append(compacts, Compaction{Inputs: base[from:to], Outputs: fresh(1 + r.Intn(2))})
			pos = to + 1
		}

		compactFirst := start
		for _, c := range compacts {
			compactFirst, err = applyCompaction(compactFirst, c)
			require.NoError(t, err)
		}
		for _, f := range flushes {
			compactFirst, err = applyEditFlush(compactFirst, f)
			require.NoError(t, err)
		}

		mixed := start
		for fi, ci := 0, 0; fi < len(flushes) || ci < len(compacts); {
			if ci == len(compacts) || (fi < len(flushes) && r.Intn(2) == 0) {
				mixed, err = applyEditFlush(mixed, flushes[fi])
				fi++
			} else {
				mixed, err = applyCompaction(mixed, compacts[ci])
				ci++
			}
			require.NoError(t, err)
		}

		assert.True(t, proto.Equal(compactFirst, mixed), "round %d: %v vs %v", round, sstIDs(compactFirst), sstIDs(mixed))
	}
}

func TestKeys(t *testing.T) {
	assert.Equal(t, "pkindex-meta/manifest/7/by-dev-dml_0_7v0", PointerKey(7, "by-dev-dml_0_7v0"))
	assert.Equal(t, "files/pk_index/7/by-dev-dml_0_7v0/sst", pkindex.SSTBasePath("files", 7, "by-dev-dml_0_7v0"))
	assert.Equal(t, "files/pk_index/7/by-dev-dml_0_7v0/sst/42.sst", pkindex.SSTKey("files/pk_index/7/by-dev-dml_0_7v0/sst", 42))
	assert.Equal(t, "files/pk_index/7/by-dev-dml_0_7v0/manifest/3-abc.pb", pkindex.ManifestBodyKey(pkindex.ManifestDir("files", 7, "by-dev-dml_0_7v0"), 3, "abc"))
}

func TestParsePointerKey(t *testing.T) {
	coll, vch, ok := ParsePointerKey("pkindex-meta/manifest/7/by-dev-dml_0_7v0")
	assert.True(t, ok)
	assert.EqualValues(t, 7, coll)
	assert.Equal(t, "by-dev-dml_0_7v0", vch)
	for _, bad := range []string{
		"pkindex-meta/manifest/x/v",
		"pkindex-meta/manifest/7/a/b",
		"pkindex-meta/manifest/7/",
		"pkindex-meta/manifest/7",
	} {
		_, _, ok = ParsePointerKey(bad)
		assert.False(t, ok, bad)
	}
}

func TestParseSSTKey(t *testing.T) {
	base := "files/pk_index/7/v/sst"
	id, ok := pkindex.ParseSSTKey(base, base+"/42.sst")
	assert.True(t, ok)
	assert.EqualValues(t, 42, id)
	for _, bad := range []string{base + "/42.tmp", base + "/x.sst", "other/42.sst", base + "/sub/42.sst"} {
		_, ok := pkindex.ParseSSTKey(base, bad)
		assert.False(t, ok, bad)
	}
}

func TestParseManifestBodyKey(t *testing.T) {
	dir := pkindex.ManifestDir("files", 7, "v")
	v, ok := pkindex.ParseManifestBodyKey(dir, dir+"/12-abc.pb")
	assert.True(t, ok)
	assert.EqualValues(t, 12, v)
	v, ok = pkindex.ParseManifestBodyKey(dir, dir+"/13-0b2f6c1e-8d4a-4f7e-9c3b-5a1d2e3f4a5b.pb")
	assert.True(t, ok)
	assert.EqualValues(t, 13, v)
	for _, bad := range []string{dir + "/12.pb", dir + "/x-abc.pb", dir + "/12-abc.txt"} {
		_, ok := pkindex.ParseManifestBodyKey(dir, bad)
		assert.False(t, ok, bad)
	}
}

// A store failure that a retry does not fix keeps its own merr code. Other
// store failures, such as a throttle or a generic IO failure, are marked as
// pkindex.ErrUnavailable.
func TestStoreFailureClassification(t *testing.T) {
	cases := []struct {
		name        string
		err         error
		unavailable bool
		code        int32
	}{
		{"throttle", merr.WrapErrIoTooManyRequests("k", errors.New("slow down")), true, merr.Code(merr.ErrServiceUnavailable)},
		{"io failed", merr.WrapErrIoFailed("k", errors.New("connection refused")), true, merr.Code(merr.ErrServiceUnavailable)},
		{"untyped", errors.New("x"), true, merr.Code(merr.ErrServiceUnavailable)},
		{"permission denied", merr.WrapErrIoPermissionDenied("k", errors.New("access denied")), false, merr.Code(merr.ErrIoPermissionDenied)},
		{"bucket not found", merr.WrapErrIoBucketNotFound("k", errors.New("no such bucket")), false, merr.Code(merr.ErrIoBucketNotFound)},
		{"invalid credentials", merr.WrapErrIoInvalidCredentials("k", errors.New("expired token")), false, merr.Code(merr.ErrIoInvalidCredentials)},
		{"entity too large", merr.WrapErrIoEntityTooLarge("k", errors.New("too large")), false, merr.Code(merr.ErrIoEntityTooLarge)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m, _, s := assigned(t, 3)
			s.WriteErr = c.err
			_, err := m.CommitFlush(context.Background(), flush(3, 10, 1))
			require.Error(t, err)
			assert.Equal(t, c.unavailable, errors.Is(err, pkindex.ErrUnavailable), "%v", err)
			got := pkindex.ToMerr(err)
			assert.Equal(t, c.code, merr.Code(got), "%v", got)
			assert.Equal(t, c.unavailable, merr.IsRetryableErr(got), "%v", got)
		})
	}
}

func sstRef(id, runID int64, minKey, maxKey string) *pkindexpb.SSTRef {
	return &pkindexpb.SSTRef{Id: id, RunId: runID, Size: 10, MinKey: []byte(minKey), MaxKey: []byte(maxKey)}
}

func manifestWith(ssts ...*pkindexpb.SSTRef) *pkindexpb.Manifest {
	return &pkindexpb.Manifest{CollectionId: 1, Vchannel: vch, Version: 3, Ssts: ssts}
}

func TestApplyCompactionRunRules(t *testing.T) {
	// list (newest first): L0 9, L0 8, run 5 {7, 6}, run 4 {3}
	cur := manifestWith(sstRef(9, 0, "a", "z"), sstRef(8, 0, "a", "z"), sstRef(7, 5, "a", "m"), sstRef(6, 5, "n", "z"), sstRef(3, 4, "a", "z"))
	out := func(runID int64) []*pkindexpb.SSTRef {
		return []*pkindexpb.SSTRef{sstRef(20, runID, "a", "k"), sstRef(21, runID, "l", "z")}
	}

	t.Run("l0 to run keeps the l0 prefix", func(t *testing.T) {
		next, err := applyCompaction(cur, Compaction{Inputs: []int64{9, 8}, Outputs: out(6)})
		require.NoError(t, err)
		require.Equal(t, []int64{20, 21, 7, 6, 3}, sstIDs(next))
	})
	t.Run("inputs are exactly one run", func(t *testing.T) {
		next, err := applyCompaction(cur, Compaction{Inputs: []int64{7, 6}, Outputs: out(6)})
		require.NoError(t, err)
		require.Equal(t, []int64{9, 8, 20, 21, 3}, sstIDs(next))
	})
	t.Run("l0 tables plus a whole run", func(t *testing.T) {
		next, err := applyCompaction(cur, Compaction{Inputs: []int64{8, 7, 6}, Outputs: out(6)})
		require.NoError(t, err)
		require.Equal(t, []int64{9, 20, 21, 3}, sstIDs(next))
	})
	t.Run("inputs must not split a run at the start edge", func(t *testing.T) {
		_, err := applyCompaction(cur, Compaction{Inputs: []int64{6, 3}, Outputs: out(6)})
		require.ErrorIs(t, err, ErrStaleEdit)
	})
	t.Run("all inputs may vanish", func(t *testing.T) {
		next, err := applyCompaction(cur, Compaction{Inputs: []int64{7, 6}, Outputs: nil})
		require.NoError(t, err)
		require.Equal(t, []int64{9, 8, 3}, sstIDs(next))
		next, err = applyCompaction(cur, Compaction{Inputs: []int64{3}, Outputs: nil})
		require.NoError(t, err)
		require.Equal(t, []int64{9, 8, 7, 6}, sstIDs(next))
	})
	t.Run("inputs must not split a run", func(t *testing.T) {
		_, err := applyCompaction(cur, Compaction{Inputs: []int64{8, 7}, Outputs: out(6)})
		require.ErrorIs(t, err, ErrStaleEdit)
	})
	t.Run("outputs need one new run id", func(t *testing.T) {
		_, err := applyCompaction(cur, Compaction{Inputs: []int64{9, 8}, Outputs: out(0)})
		require.ErrorIs(t, err, ErrStaleEdit)
		_, err = applyCompaction(cur, Compaction{Inputs: []int64{9, 8}, Outputs: out(5)})
		require.ErrorIs(t, err, ErrStaleEdit)
		mixed := out(6)
		mixed[1].RunId = 7
		_, err = applyCompaction(cur, Compaction{Inputs: []int64{9, 8}, Outputs: mixed})
		require.ErrorIs(t, err, ErrStaleEdit)
	})
	t.Run("outputs must be ascending and disjoint", func(t *testing.T) {
		overlap := []*pkindexpb.SSTRef{sstRef(20, 6, "a", "m"), sstRef(21, 6, "m", "z")}
		_, err := applyCompaction(cur, Compaction{Inputs: []int64{9, 8}, Outputs: overlap})
		require.ErrorIs(t, err, ErrStaleEdit)
	})
	t.Run("an output needs min key not above max key", func(t *testing.T) {
		inverted := []*pkindexpb.SSTRef{sstRef(20, 6, "k", "a")}
		_, err := applyCompaction(cur, Compaction{Inputs: []int64{9, 8}, Outputs: inverted})
		require.ErrorIs(t, err, ErrStaleEdit)
	})
	t.Run("an l0 table must not end up behind a run", func(t *testing.T) {
		_, err := applyCompaction(cur, Compaction{Inputs: []int64{9}, Outputs: []*pkindexpb.SSTRef{sstRef(20, 6, "a", "z")}})
		require.ErrorIs(t, err, ErrStaleEdit)
		single := manifestWith(sstRef(9, 0, "a", "z"), sstRef(8, 0, "a", "z"), sstRef(7, 0, "a", "z"))
		_, err = applyCompaction(single, Compaction{Inputs: []int64{8}, Outputs: []*pkindexpb.SSTRef{sstRef(20, 6, "a", "z")}})
		require.ErrorIs(t, err, ErrStaleEdit)
	})
}

func TestApplyFlushClearsRunID(t *testing.T) {
	req := &pkindexpb.CommitFlushRequest{CollectionId: 1, Vchannel: vch, PchannelTerm: 1, CheckpointTimeTick: 10,
		AddSsts: []*pkindexpb.SSTRef{sstRef(1, 99, "a", "z")}}
	next, err := applyFlush(nil, req, "base")
	require.NoError(t, err)
	require.Zero(t, next.GetSsts()[0].GetRunId())
}

type recordingObserver struct {
	mu       sync.Mutex
	versions []int64
}

func (o *recordingObserver) ManifestChanged(_ context.Context, m *pkindexpb.Manifest) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.versions = append(o.versions, m.GetVersion())
}

func TestManagerObserverAndCurrent(t *testing.T) {
	ctx := context.Background()
	m, _, _ := assigned(t, 1)
	obs := &recordingObserver{}
	m.SetObserver(obs)
	_, err := m.CommitFlush(ctx, flush(1, 10, 1))
	require.NoError(t, err)
	_, err = m.CommitFlush(ctx, flush(1, 20, 2))
	require.NoError(t, err)
	// an applied retry changes nothing and is not reported
	_, err = m.CommitFlush(ctx, flush(1, 20, 2))
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, obs.versions)

	cur, err := m.Current(ctx, Target{CollectionID: 1, VChannel: vch})
	require.NoError(t, err)
	require.EqualValues(t, 2, cur.GetVersion())
	none, err := m.Current(ctx, Target{CollectionID: 1, VChannel: vch + "x"})
	require.NoError(t, err)
	require.Nil(t, none)
}

// A zero-output compaction that is applied a second time is an applied retry.
func TestZeroOutputCompactionRetry(t *testing.T) {
	m, err := applyEditFlush(nil, editFlush(1, 10, 2, 1))
	require.NoError(t, err)
	edit := Compaction{Inputs: []int64{2, 1}}
	c, err := applyCompaction(m, edit)
	require.NoError(t, err)
	require.NotNil(t, c)
	again, err := applyCompaction(c, edit)
	require.NoError(t, err)
	require.Nil(t, again)
}

// A commit whose pointer write fails and is not confirmed by the read-back is
// not reported.
func TestObserverNotCalledOnFailedCommit(t *testing.T) {
	ctx := context.Background()
	m, p, _ := assigned(t, 1)
	obs := &recordingObserver{}
	m.SetObserver(obs)
	_, err := m.CommitFlush(ctx, flush(1, 10, 1))
	require.NoError(t, err)
	p.WriteErr = errors.New("crash")
	_, err = m.CommitFlush(ctx, flush(1, 20, 2))
	require.Error(t, err)
	require.Equal(t, []int64{1}, obs.versions)
}

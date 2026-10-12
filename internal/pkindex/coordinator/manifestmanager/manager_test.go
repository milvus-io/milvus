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

package manifestmanager

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/pkindex/coordinator/internal/fakes"
	"github.com/milvus-io/milvus/internal/pkindex/core/manifest"
	"github.com/milvus-io/milvus/internal/pkindex/pkerr"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

var testLayout = manifest.Layout{RootPath: "files"}

func newTestManager() (*Manager, *fakes.Pointers, *fakes.Store) {
	p, s := fakes.NewPointers(), fakes.NewStore()
	return New(p, s, testLayout), p, s
}

func claim(term int64) manifest.Claim {
	return manifest.Claim{CollectionID: 1, VChannel: "v", PChannelTerm: term, PKDataType: schemapb.DataType_Int64}
}

func flush(term int64, tick uint64, ids ...int64) manifest.Flush {
	f := manifest.Flush{PChannelTerm: term, CheckpointTimeTick: tick}
	for _, id := range ids {
		f.AddSSTs = append(f.AddSSTs, &pkindexpb.SSTRef{Id: id})
	}
	return f
}

func sstIDs(m *pkindexpb.Manifest) []int64 {
	out := []int64{}
	for _, s := range m.GetSsts() {
		out = append(out, s.GetId())
	}
	return out
}

// bodyKeys returns the manifest body keys of vchannel "v" in the store.
func bodyKeys(t *testing.T, s *fakes.Store) []string {
	keys := []string{}
	err := s.Walk(context.Background(), testLayout.ManifestDir(1, "v")+"/", func(k string, _ time.Time) bool {
		keys = append(keys, k)
		return true
	})
	require.NoError(t, err)
	return keys
}

func TestClaimCreatesFirstVersion(t *testing.T) {
	m, p, s := newTestManager()
	ctx := context.Background()
	got, err := m.Claim(ctx, claim(3))
	require.NoError(t, err)
	assert.EqualValues(t, 1, got.GetVersion())
	assert.Equal(t, "files/pk_index/1/v/sst", got.GetSstBasePath())

	// a new manager reads the same state back from the stores
	read, found, err := New(p, s, testLayout).Get(ctx, 1, "v")
	require.NoError(t, err)
	require.True(t, found)
	assert.True(t, proto.Equal(got, read))
}

func TestGetWithoutManifest(t *testing.T) {
	m, _, _ := newTestManager()
	got, found, err := m.Get(context.Background(), 1, "v")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, got)
}

func TestClaimTermRules(t *testing.T) {
	m, _, _ := newTestManager()
	ctx := context.Background()

	first, err := m.Claim(ctx, claim(3))
	require.NoError(t, err)
	assert.EqualValues(t, 1, first.GetVersion())

	same, err := m.Claim(ctx, claim(3))
	require.NoError(t, err)
	assert.EqualValues(t, 1, same.GetVersion(), "the same term is idempotent")

	up, err := m.Claim(ctx, claim(4))
	require.NoError(t, err)
	assert.EqualValues(t, 2, up.GetVersion())
	assert.EqualValues(t, 4, up.GetPchannelTerm())

	_, err = m.Claim(ctx, claim(3))
	assert.True(t, errors.Is(err, manifest.ErrFenced))
	got, _, err := m.Get(ctx, 1, "v")
	require.NoError(t, err)
	assert.EqualValues(t, 2, got.GetVersion())
}

func TestCommitFlushAndFence(t *testing.T) {
	m, _, _ := newTestManager()
	ctx := context.Background()
	_, err := m.Claim(ctx, claim(3))
	require.NoError(t, err)
	got, err := m.CommitFlush(ctx, 1, "v", flush(3, 50, 9))
	require.NoError(t, err)
	assert.EqualValues(t, 2, got.GetVersion())
	assert.Equal(t, []int64{9}, sstIDs(got))

	// a retry of the applied flush returns the current version
	again, err := m.CommitFlush(ctx, 1, "v", flush(3, 50, 9))
	require.NoError(t, err)
	assert.EqualValues(t, 2, again.GetVersion())

	_, err = m.Claim(ctx, claim(4))
	require.NoError(t, err)
	_, err = m.CommitFlush(ctx, 1, "v", flush(3, 60, 10))
	assert.True(t, errors.Is(err, manifest.ErrFenced))
}

func TestCommitCompact(t *testing.T) {
	m, _, _ := newTestManager()
	ctx := context.Background()
	_, err := m.Claim(ctx, claim(1))
	require.NoError(t, err)
	_, err = m.CommitFlush(ctx, 1, "v", flush(1, 10, 3, 2, 1))
	require.NoError(t, err)
	got, err := m.CommitCompact(ctx, 1, "v", manifest.Compact{Inputs: []int64{2, 1}, Outputs: []*pkindexpb.SSTRef{{Id: 10}}})
	require.NoError(t, err)
	assert.EqualValues(t, 3, got.GetVersion())
	assert.Equal(t, []int64{3, 10}, sstIDs(got))

	_, err = m.CommitCompact(ctx, 1, "v", manifest.Compact{Inputs: []int64{2, 1}, Outputs: []*pkindexpb.SSTRef{{Id: 11}}})
	assert.True(t, errors.Is(err, manifest.ErrStaleEdit))
}

func TestEditBeforeClaim(t *testing.T) {
	m, _, _ := newTestManager()
	ctx := context.Background()
	_, err := m.CommitFlush(ctx, 1, "v", flush(1, 1))
	assert.True(t, errors.Is(err, manifest.ErrNotClaimed))
	_, err = m.CommitCompact(ctx, 1, "v", manifest.Compact{Inputs: []int64{1}, Outputs: []*pkindexpb.SSTRef{{Id: 2}}})
	assert.True(t, errors.Is(err, manifest.ErrNotClaimed))
}

// The pointer swap reports an error after it took effect: the manager reads
// the pointer back and reports success.
func TestSwapUncertainButApplied(t *testing.T) {
	m, p, s := newTestManager()
	ctx := context.Background()
	_, err := m.Claim(ctx, claim(1))
	require.NoError(t, err)
	p.WriteErr, p.WriteApplies = errors.New("timeout"), true
	got, err := m.CommitFlush(ctx, 1, "v", flush(1, 5, 1))
	require.NoError(t, err)
	assert.EqualValues(t, 2, got.GetVersion())

	read, _, err := New(p, s, testLayout).Get(ctx, 1, "v")
	require.NoError(t, err)
	assert.True(t, proto.Equal(got, read))
}

// The caller's ctx ends during the pointer swap and the swap took effect: the
// read back does not use the ended ctx, and the commit reports success.
func TestSwapUncertainAfterCallerCtxEnds(t *testing.T) {
	m, p, s := newTestManager()
	_, err := m.Claim(context.Background(), claim(1))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.BeforeWrite = cancel
	p.WriteErr, p.WriteApplies = context.Canceled, true
	got, err := m.CommitFlush(ctx, 1, "v", flush(1, 5, 1))
	require.NoError(t, err)
	assert.EqualValues(t, 2, got.GetVersion())

	read, _, err := New(p, s, testLayout).Get(context.Background(), 1, "v")
	require.NoError(t, err)
	assert.True(t, proto.Equal(got, read))
}

// The first pointer write reports an error after it took effect: the manager
// reads the pointer back and reports success.
func TestCreateUncertainButApplied(t *testing.T) {
	m, p, s := newTestManager()
	ctx := context.Background()
	p.WriteErr, p.WriteApplies = errors.New("timeout"), true
	got, err := m.Claim(ctx, claim(1))
	require.NoError(t, err)
	assert.EqualValues(t, 1, got.GetVersion())

	read, found, err := New(p, s, testLayout).Get(ctx, 1, "v")
	require.NoError(t, err)
	require.True(t, found)
	assert.True(t, proto.Equal(got, read))
}

// The first pointer write fails and did not take effect: the error is
// retryable and the next claim succeeds.
func TestCreateFailedIsRetryable(t *testing.T) {
	m, p, _ := newTestManager()
	ctx := context.Background()
	cause := errors.New("timeout")
	p.WriteErr = cause
	_, err := m.Claim(ctx, claim(1))
	assert.True(t, errors.Is(err, pkerr.ErrUnavailable))
	assert.True(t, errors.Is(err, cause), "the cause stays in the chain")

	got, err := m.Claim(ctx, claim(1))
	require.NoError(t, err)
	assert.EqualValues(t, 1, got.GetVersion())
}

// The pointer swap fails and did not take effect: the error is retryable and
// the next commit succeeds.
func TestSwapFailedIsRetryable(t *testing.T) {
	m, p, _ := newTestManager()
	ctx := context.Background()
	_, err := m.Claim(ctx, claim(1))
	require.NoError(t, err)
	cause := errors.New("timeout")
	p.WriteErr = cause
	_, err = m.CommitFlush(ctx, 1, "v", flush(1, 5, 1))
	assert.True(t, errors.Is(err, pkerr.ErrUnavailable))
	assert.True(t, errors.Is(err, cause), "the cause stays in the chain")
	got, err := m.CommitFlush(ctx, 1, "v", flush(1, 5, 1))
	require.NoError(t, err)
	assert.EqualValues(t, 2, got.GetVersion())
	assert.Equal(t, []int64{1}, sstIDs(got))
}

// The pointer swap took effect, but the read back fails: the commit reports a
// retryable error and drops its cache, so the retry loads the applied version.
func TestReadBackFails(t *testing.T) {
	m, p, _ := newTestManager()
	ctx := context.Background()
	_, err := m.Claim(ctx, claim(1))
	require.NoError(t, err)
	p.WriteErr, p.WriteApplies = errors.New("timeout"), true
	p.LoadErr = errors.New("etcd down")
	_, err = m.CommitFlush(ctx, 1, "v", flush(1, 5, 1))
	assert.True(t, errors.Is(err, pkerr.ErrUnavailable))
	assert.Contains(t, fmt.Sprintf("%+v", err), "etcd down", "the read-back error is kept")

	got, err := m.CommitFlush(ctx, 1, "v", flush(1, 5, 1))
	require.NoError(t, err)
	assert.EqualValues(t, 2, got.GetVersion())
	assert.Equal(t, []int64{1}, sstIDs(got))
}

// A crash between the body write and the pointer swap leaves the old version
// current and an orphan body in the store.
func TestCrashBeforeSwap(t *testing.T) {
	m, p, s := newTestManager()
	ctx := context.Background()
	old, err := m.Claim(ctx, claim(1))
	require.NoError(t, err)
	require.Len(t, bodyKeys(t, s), 1)

	p.WriteErr = errors.New("crash")
	_, err = m.CommitFlush(ctx, 1, "v", flush(1, 5, 1))
	require.Error(t, err)

	got, found, err := New(p, s, testLayout).Get(ctx, 1, "v")
	require.NoError(t, err)
	require.True(t, found)
	assert.True(t, proto.Equal(old, got))

	keys := bodyKeys(t, s)
	require.Len(t, keys, 2)
	var orphans int
	for _, k := range keys {
		v, ok := manifest.ParseManifestBodyKey(testLayout.ManifestDir(1, "v"), k)
		require.True(t, ok)
		if v == 2 {
			orphans++
		}
	}
	assert.Equal(t, 1, orphans)
}

// Two managers on shared stores both create the first version: exactly one
// create succeeds, the other gets a retryable error, and its retry applies
// its claim on top of the winner's version.
func TestTwoManagersCreateFirstVersion(t *testing.T) {
	p, s := fakes.NewPointers(), fakes.NewStore()
	a, b := New(p, s, testLayout), New(p, s, testLayout)
	ctx := context.Background()

	var aGot *pkindexpb.Manifest
	var aErr error
	// b has loaded "not found" and written its body; a creates the first
	// version before b writes the pointer
	p.BeforeWrite = func() { aGot, aErr = a.Claim(ctx, claim(1)) }
	_, err := b.Claim(ctx, claim(2))
	assert.True(t, errors.Is(err, pkerr.ErrUnavailable))
	require.NoError(t, aErr)
	assert.EqualValues(t, 1, aGot.GetVersion())
	assert.EqualValues(t, 1, aGot.GetPchannelTerm())

	got, err := b.Claim(ctx, claim(2))
	require.NoError(t, err)
	assert.EqualValues(t, 2, got.GetVersion())
	assert.EqualValues(t, 2, got.GetPchannelTerm())

	// the winner's pointer names the body that b wrote
	read, found, err := New(p, s, testLayout).Get(ctx, 1, "v")
	require.NoError(t, err)
	require.True(t, found)
	assert.True(t, proto.Equal(got, read))

	// a has a stale cache: its commit fails, and its retry is fenced
	_, err = a.CommitFlush(ctx, 1, "v", flush(1, 5, 1))
	assert.True(t, errors.Is(err, pkerr.ErrUnavailable))
	_, err = a.CommitFlush(ctx, 1, "v", flush(1, 5, 1))
	assert.True(t, errors.Is(err, manifest.ErrFenced))
}

// A pointer whose body has another version is corrupted.
func TestLoadVersionMismatch(t *testing.T) {
	m, p, s := newTestManager()
	ctx := context.Background()
	body, err := proto.Marshal(&pkindexpb.Manifest{CollectionId: 1, Vchannel: "v", Version: 5})
	require.NoError(t, err)
	bodyKey := testLayout.ManifestBodyKey(1, "v", 5, "x")
	require.NoError(t, s.Write(ctx, bodyKey, body))
	ptr, err := proto.Marshal(&pkindexpb.ManifestPointer{Version: 6, BodyKey: bodyKey})
	require.NoError(t, err)
	p.Put(manifest.PointerKey(1, "v"), string(ptr))

	_, _, err = m.Get(ctx, 1, "v")
	assert.True(t, errors.Is(err, pkerr.ErrCorrupted))
	_, err = m.CommitFlush(ctx, 1, "v", flush(1, 5, 1))
	assert.True(t, errors.Is(err, pkerr.ErrCorrupted))
}

func TestLoadErrors(t *testing.T) {
	ctx := context.Background()
	m, p, s := newTestManager()
	_, err := m.Claim(ctx, claim(1))
	require.NoError(t, err)

	// a fresh manager has no cache, so every Get reads the stores
	t.Run("pointer read fails", func(t *testing.T) {
		p.LoadErr = errors.New("etcd down")
		_, _, err := New(p, s, testLayout).Get(ctx, 1, "v")
		assert.True(t, errors.Is(err, pkerr.ErrUnavailable))
	})
	t.Run("body read fails", func(t *testing.T) {
		s.ReadErr = errors.New("s3 down")
		_, _, err := New(p, s, testLayout).Get(ctx, 1, "v")
		assert.True(t, errors.Is(err, pkerr.ErrUnavailable))
		assert.False(t, errors.Is(err, pkerr.ErrCorrupted))
	})
	t.Run("body missing", func(t *testing.T) {
		require.NoError(t, s.MultiRemove(ctx, bodyKeys(t, s)))
		_, _, err := New(p, s, testLayout).Get(ctx, 1, "v")
		assert.True(t, errors.Is(err, pkerr.ErrCorrupted))
		assert.Equal(t, merr.Code(merr.ErrDataIntegrity), merr.Code(pkerr.ToMerr(err)), "a missing body is not retryable")
	})
	t.Run("pointer undecodable", func(t *testing.T) {
		p.Put(manifest.PointerKey(1, "v"), "\xff")
		_, _, err := New(p, s, testLayout).Get(ctx, 1, "v")
		assert.True(t, errors.Is(err, pkerr.ErrCorrupted))
	})
}

func TestBodyWriteFails(t *testing.T) {
	m, p, s := newTestManager()
	ctx := context.Background()
	s.WriteErr = errors.New("s3 down")
	_, err := m.Claim(ctx, claim(1))
	assert.True(t, errors.Is(err, pkerr.ErrUnavailable))
	_, found, err := New(p, s, testLayout).Get(ctx, 1, "v")
	require.NoError(t, err)
	assert.False(t, found)
}

// Edits on one manager from many goroutines apply one at a time: all succeed
// and none is lost.
func TestConcurrentEdits(t *testing.T) {
	m, p, s := newTestManager()
	ctx := context.Background()
	const compacts, flushes = 16, 16
	_, err := m.Claim(ctx, claim(1))
	require.NoError(t, err)
	base := make([]int64, 0, compacts)
	for i := compacts; i >= 1; i-- {
		base = append(base, int64(i))
	}
	_, err = m.CommitFlush(ctx, 1, "v", flush(1, 10, base...))
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 1; i <= compacts; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			_, err := m.CommitCompact(ctx, 1, "v", manifest.Compact{Inputs: []int64{id}, Outputs: []*pkindexpb.SSTRef{{Id: 1000 + id}}})
			assert.NoError(t, err)
		}(int64(i))
	}
	// flushes need an increasing checkpoint, so one goroutine sends them in order
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 1; j <= flushes; j++ {
			_, err := m.CommitFlush(ctx, 1, "v", flush(1, uint64(10+j), int64(2000+j)))
			assert.NoError(t, err)
		}
	}()
	wg.Wait()

	got, found, err := New(p, s, testLayout).Get(ctx, 1, "v")
	require.NoError(t, err)
	require.True(t, found)
	// claim, the first flush, then every concurrent edit
	assert.EqualValues(t, 2+compacts+flushes, got.GetVersion())
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

// Two managers on the same stores model two active coordinators. Each commit
// either succeeds or returns a retryable error. The final pointer names a body
// with the pointer's version that holds exactly the SSTs of the applied
// commits.
func TestTwoWriters(t *testing.T) {
	p, s := fakes.NewPointers(), fakes.NewStore()
	a, b := New(p, s, testLayout), New(p, s, testLayout)
	ctx := context.Background()
	_, err := a.Claim(ctx, claim(1))
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
				got, err := mgr.CommitFlush(ctx, 1, "v", flush(1, tick, id))
				if err != nil {
					assert.True(t, errors.Is(err, pkerr.ErrUnavailable), "%v", err)
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

	fresh := New(p, s, testLayout)
	got, found, err := fresh.Get(ctx, 1, "v")
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

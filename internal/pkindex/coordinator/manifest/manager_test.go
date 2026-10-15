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
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/pkindex/core/objpath"
	"github.com/milvus-io/milvus/internal/pkindex/pkerr"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
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
		PkDataType: schemapb.DataType_Int64, CheckpointTimeTick: tick,
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
	err := s.Walk(context.Background(), objpath.ManifestDir(testRoot, 1, vch)+"/", func(k string, _ time.Time) bool {
		keys = append(keys, k)
		return true
	})
	require.NoError(t, err)
	return keys
}

// storedClaim returns the term of the stored claim record of pch.
func storedClaim(t *testing.T, p *FakeMetaStore) int64 {
	v, found, err := p.Load(context.Background(), ClaimKey(pch))
	require.NoError(t, err)
	require.True(t, found)
	var c pkindexpb.PChannelClaim
	require.NoError(t, proto.Unmarshal([]byte(v), &c))
	return c.GetPchannelTerm()
}

// claimed returns a manager on new stores whose pch is claimed for term.
func claimed(t *testing.T, term int64) (*Manager, *FakeMetaStore, *FakeObjectStore) {
	m, p, s := newTestManager()
	require.NoError(t, m.ClaimPChannel(context.Background(), pch, term))
	return m, p, s
}

// A flush checks the claim record but does not write it.
func TestFlushDoesNotWriteClaimRecord(t *testing.T) {
	m, p, _ := claimed(t, 3)
	_, err := m.CommitFlush(context.Background(), flush(3, 50, 9))
	require.NoError(t, err)
	assert.Len(t, p.LastSave, 1)
	assert.Contains(t, p.LastSave, PointerKey(1, vch))
}

func TestClaimPChannelRules(t *testing.T) {
	m, p, s := newTestManager()
	ctx := context.Background()

	require.NoError(t, m.ClaimPChannel(ctx, pch, 3))
	assert.EqualValues(t, 3, storedClaim(t, p))
	require.NoError(t, m.ClaimPChannel(ctx, pch, 3), "the same term is idempotent")
	require.NoError(t, m.ClaimPChannel(ctx, pch, 4))
	assert.EqualValues(t, 4, storedClaim(t, p))
	err := m.ClaimPChannel(ctx, pch, 3)
	assert.True(t, errors.Is(err, ErrFenced))
	assert.EqualValues(t, 4, storedClaim(t, p))

	// a claim does not create a manifest
	_, found, err := get(ctx, m, 1, vch)
	require.NoError(t, err)
	assert.False(t, found)

	// a new manager reads the claim record from the store
	err = New(p, s, testRoot).ClaimPChannel(ctx, pch, 3)
	assert.True(t, errors.Is(err, ErrFenced))
}

// The first flush of a vchannel creates version 1 with the SST base path of
// the vchannel and the primary key data type of the flush.
func TestFirstFlushCreatesVersion1(t *testing.T) {
	m, p, s := claimed(t, 3)
	ctx := context.Background()
	got, err := m.CommitFlush(ctx, flush(3, 50, 9))
	require.NoError(t, err)
	assert.EqualValues(t, 1, got.GetVersion())
	assert.Equal(t, "files/pk_index/1/p_1v0/sst", got.GetSstBasePath())
	assert.Equal(t, objpath.SSTBasePath("files", 1, vch), got.GetSstBasePath())
	assert.Equal(t, schemapb.DataType_Int64, got.GetPkDataType())
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
	assert.True(t, errors.Is(err, ErrNotClaimed), "no claim record")

	m, _, _ = claimed(t, 3)
	_, err = m.CommitFlush(ctx, flush(2, 10, 1))
	assert.True(t, errors.Is(err, ErrFenced), "a lower term is fenced")
	_, err = m.CommitFlush(ctx, flush(4, 10, 1))
	assert.True(t, errors.Is(err, ErrNotClaimed), "a higher term must claim first")
	_, found, err := get(ctx, m, 1, vch)
	require.NoError(t, err)
	assert.False(t, found)
}

// A flush whose checkpoint does not move forward is a retry when its term is
// the last flush term, and a regression otherwise.
func TestFlushRetryVersusRegression(t *testing.T) {
	m, _, _ := claimed(t, 3)
	ctx := context.Background()
	first, err := m.CommitFlush(ctx, flush(3, 50, 9))
	require.NoError(t, err)

	again, err := m.CommitFlush(ctx, flush(3, 50, 9))
	require.NoError(t, err)
	assert.True(t, proto.Equal(first, again))

	require.NoError(t, m.ClaimPChannel(ctx, pch, 4))
	_, err = m.CommitFlush(ctx, flush(4, 50, 10))
	assert.True(t, errors.Is(err, ErrCheckpointRegressed))
	_, err = m.CommitFlush(ctx, flush(3, 60, 10))
	assert.True(t, errors.Is(err, ErrFenced))

	up, err := m.CommitFlush(ctx, flush(4, 60, 10))
	require.NoError(t, err)
	assert.EqualValues(t, 2, up.GetVersion())
	assert.EqualValues(t, 4, up.GetLastFlushTerm())
}

// Another coordinator raises the claim record after this manager cached it:
// a flush with the new term reloads the record and succeeds.
func TestFlushReloadsRaisedClaim(t *testing.T) {
	a, p, s := claimed(t, 3)
	ctx := context.Background()
	_, err := a.CommitFlush(ctx, flush(3, 10, 1))
	require.NoError(t, err)
	require.NoError(t, New(p, s, testRoot).ClaimPChannel(ctx, pch, 4))

	got, err := a.CommitFlush(ctx, flush(4, 20, 2))
	require.NoError(t, err)
	assert.Equal(t, []int64{2, 1}, sstIDs(got))
}

// The claim record changes between the cache read of a flush and its write:
// the write fails, the error is retryable, and the retry is fenced.
func TestFlushFailsWhenClaimChangesBeforeWrite(t *testing.T) {
	a, p, s := claimed(t, 3)
	b := New(p, s, testRoot)
	ctx := context.Background()
	_, err := a.CommitFlush(ctx, flush(3, 10, 1))
	require.NoError(t, err)

	var claimErr error
	p.BeforeWrite = func() { claimErr = b.ClaimPChannel(ctx, pch, 4) }
	_, err = a.CommitFlush(ctx, flush(3, 20, 2))
	require.NoError(t, claimErr)
	assert.True(t, errors.Is(err, pkerr.ErrUnavailable), "%v", err)

	_, err = a.CommitFlush(ctx, flush(3, 20, 2))
	assert.True(t, errors.Is(err, ErrFenced))
	got, _, err := get(ctx, New(p, s, testRoot), 1, vch)
	require.NoError(t, err)
	assert.Equal(t, []int64{1}, sstIDs(got))
	assert.EqualValues(t, 4, storedClaim(t, p))
}

// A flush with a stale cached claim record does not pass the claim predicate
// of the write, even when the pointer predicate holds.
func TestFlushWithStaleCachedClaim(t *testing.T) {
	a, p, s := claimed(t, 3)
	ctx := context.Background()
	require.NoError(t, New(p, s, testRoot).ClaimPChannel(ctx, pch, 4))

	_, err := a.CommitFlush(ctx, flush(3, 10, 1))
	assert.True(t, errors.Is(err, pkerr.ErrUnavailable), "%v", err)
	_, found, err := get(ctx, New(p, s, testRoot), 1, vch)
	require.NoError(t, err)
	assert.False(t, found)
	_, err = a.CommitFlush(ctx, flush(3, 10, 1))
	assert.True(t, errors.Is(err, ErrFenced))
}

func TestCommitCompaction(t *testing.T) {
	m, _, _ := claimed(t, 1)
	ctx := context.Background()
	_, err := m.CommitCompaction(ctx, 1, vch, Compaction{Inputs: []int64{1}, Outputs: []*pkindexpb.SSTRef{{Id: 2}}})
	assert.True(t, errors.Is(err, ErrNoManifest), "no manifest before the first flush")

	_, err = m.CommitFlush(ctx, flush(1, 10, 3, 2, 1))
	require.NoError(t, err)
	got, err := m.CommitCompaction(ctx, 1, vch, Compaction{Inputs: []int64{2, 1}, Outputs: []*pkindexpb.SSTRef{{Id: 10}}})
	require.NoError(t, err)
	assert.EqualValues(t, 2, got.GetVersion())
	assert.Equal(t, []int64{3, 10}, sstIDs(got))

	_, err = m.CommitCompaction(ctx, 1, vch, Compaction{Inputs: []int64{2, 1}, Outputs: []*pkindexpb.SSTRef{{Id: 11}}})
	assert.True(t, errors.Is(err, ErrStaleEdit))
}

// The claim write reports an error after it took effect: the manager reads
// the record back and reports success.
func TestClaimUncertainButApplied(t *testing.T) {
	m, p, _ := newTestManager()
	p.WriteErr, p.WriteApplies = errors.New("timeout"), true
	require.NoError(t, m.ClaimPChannel(context.Background(), pch, 3))
	assert.EqualValues(t, 3, storedClaim(t, p))
}

// The claim write fails and did not take effect: the error is retryable and
// the next claim succeeds.
func TestClaimFailedIsRetryable(t *testing.T) {
	m, p, _ := newTestManager()
	ctx := context.Background()
	cause := errors.New("timeout")
	p.WriteErr = cause
	err := m.ClaimPChannel(ctx, pch, 3)
	assert.True(t, errors.Is(err, pkerr.ErrUnavailable))
	assert.True(t, errors.Is(err, cause), "the cause stays in the chain")
	require.NoError(t, m.ClaimPChannel(ctx, pch, 3))
}

func TestClaimRecordLoadErrors(t *testing.T) {
	ctx := context.Background()
	m, p, s := claimed(t, 3)

	p.LoadErr = errors.New("etcd down")
	err := m.ClaimPChannel(ctx, pch, 3)
	assert.True(t, errors.Is(err, pkerr.ErrUnavailable))
	p.LoadErr = errors.New("etcd down")
	_, err = New(p, s, testRoot).CommitFlush(ctx, flush(3, 10))
	assert.True(t, errors.Is(err, pkerr.ErrUnavailable))

	for _, v := range []string{"\xff", ""} {
		p.Put(ClaimKey(pch), v)
		err = New(p, s, testRoot).ClaimPChannel(ctx, pch, 3)
		assert.True(t, errors.Is(err, pkerr.ErrCorrupted), "record %q", v)
		_, err = New(p, s, testRoot).CommitFlush(ctx, flush(3, 10))
		assert.True(t, errors.Is(err, pkerr.ErrCorrupted), "record %q", v)
	}
}

// The pointer swap reports an error after it took effect: the manager reads
// the pointer back and reports success.
func TestSwapUncertainButApplied(t *testing.T) {
	m, p, s := claimed(t, 1)
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
	m, p, s := claimed(t, 1)
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
	m, p, s := claimed(t, 1)
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
	m, p, _ := claimed(t, 1)
	ctx := context.Background()
	cause := errors.New("timeout")
	p.WriteErr = cause
	_, err := m.CommitFlush(ctx, flush(1, 5, 1))
	assert.True(t, errors.Is(err, pkerr.ErrUnavailable))
	assert.True(t, errors.Is(err, cause), "the cause stays in the chain")

	got, err := m.CommitFlush(ctx, flush(1, 5, 1))
	require.NoError(t, err)
	assert.EqualValues(t, 1, got.GetVersion())
}

// The pointer swap fails and did not take effect: the error is retryable and
// the next commit succeeds.
func TestSwapFailedIsRetryable(t *testing.T) {
	m, p, _ := claimed(t, 1)
	ctx := context.Background()
	_, err := m.CommitFlush(ctx, flush(1, 1))
	require.NoError(t, err)
	cause := errors.New("timeout")
	p.WriteErr = cause
	_, err = m.CommitFlush(ctx, flush(1, 5, 1))
	assert.True(t, errors.Is(err, pkerr.ErrUnavailable))
	assert.True(t, errors.Is(err, cause), "the cause stays in the chain")
	got, err := m.CommitFlush(ctx, flush(1, 5, 1))
	require.NoError(t, err)
	assert.EqualValues(t, 2, got.GetVersion())
	assert.Equal(t, []int64{1}, sstIDs(got))
}

// The pointer swap took effect, but the read back fails: the commit reports a
// retryable error and drops its cache, so the retry loads the applied version.
func TestReadBackFails(t *testing.T) {
	m, p, _ := claimed(t, 1)
	ctx := context.Background()
	_, err := m.CommitFlush(ctx, flush(1, 1))
	require.NoError(t, err)
	p.WriteErr, p.WriteApplies = errors.New("timeout"), true
	p.LoadErr = errors.New("etcd down")
	_, err = m.CommitFlush(ctx, flush(1, 5, 1))
	assert.True(t, errors.Is(err, pkerr.ErrUnavailable))
	assert.Contains(t, fmt.Sprintf("%+v", err), "etcd down", "the read-back error is kept")

	got, err := m.CommitFlush(ctx, flush(1, 5, 1))
	require.NoError(t, err)
	assert.EqualValues(t, 2, got.GetVersion())
	assert.Equal(t, []int64{1}, sstIDs(got))
}

// A crash between the body write and the pointer swap leaves the old version
// current and an orphan body in the store.
func TestCrashBeforeSwap(t *testing.T) {
	m, p, s := claimed(t, 1)
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
		v, ok := objpath.ParseManifestBodyKey(objpath.ManifestDir(testRoot, 1, vch), k)
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
	a, p, s := claimed(t, 1)
	b := New(p, s, testRoot)
	ctx := context.Background()

	var aGot *pkindexpb.Manifest
	var aErr error
	// b has loaded "not found" and written its body; a creates the first
	// version before b writes the pointer
	p.BeforeWrite = func() { aGot, aErr = a.CommitFlush(ctx, flush(1, 5, 1)) }
	_, err := b.CommitFlush(ctx, flush(1, 6, 2))
	assert.True(t, errors.Is(err, pkerr.ErrUnavailable))
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
	m, p, s := claimed(t, 1)
	ctx := context.Background()
	body, err := proto.Marshal(&pkindexpb.Manifest{CollectionId: 1, Vchannel: vch, Version: 5})
	require.NoError(t, err)
	bodyKey := objpath.ManifestBodyKey(objpath.ManifestDir(testRoot, 1, vch), 5, "x")
	require.NoError(t, s.Write(ctx, bodyKey, body))
	ptr, err := proto.Marshal(&pkindexpb.ManifestPointer{Version: 6, BodyKey: bodyKey})
	require.NoError(t, err)
	p.Put(PointerKey(1, vch), string(ptr))

	_, _, err = get(ctx, m, 1, vch)
	assert.True(t, errors.Is(err, pkerr.ErrCorrupted))
	_, err = m.CommitFlush(ctx, flush(1, 5, 1))
	assert.True(t, errors.Is(err, pkerr.ErrCorrupted))
}

func TestLoadErrors(t *testing.T) {
	ctx := context.Background()
	m, p, s := claimed(t, 1)
	_, err := m.CommitFlush(ctx, flush(1, 1))
	require.NoError(t, err)

	// a fresh manager has no cache, so every read goes to the stores
	t.Run("pointer read fails", func(t *testing.T) {
		p.LoadErr = errors.New("etcd down")
		_, _, err := get(ctx, New(p, s, testRoot), 1, vch)
		assert.True(t, errors.Is(err, pkerr.ErrUnavailable))
	})
	t.Run("body read fails", func(t *testing.T) {
		s.ReadErr = errors.New("s3 down")
		_, _, err := get(ctx, New(p, s, testRoot), 1, vch)
		assert.True(t, errors.Is(err, pkerr.ErrUnavailable))
		assert.False(t, errors.Is(err, pkerr.ErrCorrupted))
	})
	t.Run("body missing", func(t *testing.T) {
		require.NoError(t, s.MultiRemove(ctx, bodyKeys(t, s)))
		_, _, err := get(ctx, New(p, s, testRoot), 1, vch)
		assert.True(t, errors.Is(err, pkerr.ErrCorrupted))
		assert.Equal(t, merr.Code(merr.ErrDataIntegrity), merr.Code(pkerr.ToMerr(err)), "a missing body is not retryable")
	})
	t.Run("pointer undecodable", func(t *testing.T) {
		p.Put(PointerKey(1, vch), "\xff")
		_, _, err := get(ctx, New(p, s, testRoot), 1, vch)
		assert.True(t, errors.Is(err, pkerr.ErrCorrupted))
	})
	t.Run("pointer names no body", func(t *testing.T) {
		zeroVersion, err := proto.Marshal(&pkindexpb.ManifestPointer{BodyKey: objpath.ManifestBodyKey(objpath.ManifestDir(testRoot, 1, vch), 1, "x")})
		require.NoError(t, err)
		for _, ptr := range []string{"", string(zeroVersion)} {
			p.Put(PointerKey(1, vch), ptr)
			// an object store rejects an empty key as a request error, not as a missing object
			s.ReadErr = errors.New("object name cannot be empty")
			_, _, err := get(ctx, New(p, s, testRoot), 1, vch)
			s.ReadErr = nil
			assert.True(t, errors.Is(err, pkerr.ErrCorrupted), "pointer %q", ptr)
			assert.False(t, errors.Is(err, pkerr.ErrUnavailable), "pointer %q", ptr)
		}
	})
}

func TestBodyWriteFails(t *testing.T) {
	m, p, s := claimed(t, 1)
	ctx := context.Background()
	s.WriteErr = errors.New("s3 down")
	_, err := m.CommitFlush(ctx, flush(1, 5, 1))
	assert.True(t, errors.Is(err, pkerr.ErrUnavailable))
	_, found, err := get(ctx, New(p, s, testRoot), 1, vch)
	require.NoError(t, err)
	assert.False(t, found)
}

// Edits on one manager from many goroutines apply one at a time: all succeed
// and none is lost.
func TestConcurrentEdits(t *testing.T) {
	m, p, s := claimed(t, 1)
	ctx := context.Background()
	const compacts, flushes = 16, 16
	base := make([]int64, 0, compacts)
	for i := compacts; i >= 1; i-- {
		base = append(base, int64(i))
	}
	_, err := m.CommitFlush(ctx, flush(1, 10, base...))
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 1; i <= compacts; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			_, err := m.CommitCompaction(ctx, 1, vch, Compaction{Inputs: []int64{id}, Outputs: []*pkindexpb.SSTRef{{Id: 1000 + id}}})
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
	assert.EqualValues(t, 1+compacts+flushes, got.GetVersion())
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

// Flushes of the vchannels of one pchannel, and claims of that pchannel with
// the same term, run in parallel without errors.
func TestParallelVChannelsOfOnePChannel(t *testing.T) {
	m, _, _ := claimed(t, 1)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(v string) {
			defer wg.Done()
			for j := 1; j <= 10; j++ {
				_, err := m.CommitFlush(ctx, flushOn(v, 1, uint64(j), int64(j)))
				assert.NoError(t, err)
				assert.NoError(t, m.ClaimPChannel(ctx, pch, 1))
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
	a, p, s := claimed(t, 1)
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
	m, p, _ := claimed(t, 1)
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
			assert.True(t, errors.Is(r.Err, pkerr.ErrCorrupted), "target %d", i)
			assert.False(t, r.Found)
		case i%3 == 0:
			require.NoError(t, r.Err, "target %d", i)
			assert.True(t, r.Found)
			assert.EqualValues(t, 1, r.Pointer.GetVersion())
			assert.Contains(t, r.Pointer.GetBodyKey(), objpath.ManifestDir(testRoot, 1, targets[i].VChannel)+"/")
		default:
			require.NoError(t, r.Err, "target %d", i)
			assert.False(t, r.Found)
			assert.Nil(t, r.Pointer)
		}
	}
}

// The failure of one batch read fails the targets of that batch only.
func TestLoadPointersBatchFailure(t *testing.T) {
	m, p, _ := claimed(t, 1)
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
		assert.True(t, errors.Is(r.Err, pkerr.ErrUnavailable))
	}
	require.NoError(t, results[pointerBatchSize].Err)
	assert.True(t, results[pointerBatchSize].Found)
}

// LoadPointers reads the store, so it sees a commit of another manager.
func TestLoadPointersIgnoresCache(t *testing.T) {
	a, p, s := claimed(t, 1)
	ctx := context.Background()
	_, err := a.CommitFlush(ctx, flush(1, 10, 1))
	require.NoError(t, err)
	got, err := New(p, s, testRoot).CommitFlush(ctx, flush(1, 20, 2))
	require.NoError(t, err)

	results := a.LoadPointers(ctx, []Target{{CollectionID: 1, VChannel: vch}})
	require.NoError(t, results[0].Err)
	assert.Equal(t, got.GetVersion(), results[0].Pointer.GetVersion())
}

// The cached claim term only goes up.
func TestClaimCacheOnlyRaises(t *testing.T) {
	m, _, _ := newTestManager()
	claims := m.claims
	claim := func(term int64) *pchannelClaim {
		return &pchannelClaim{pb: &pkindexpb.PChannelClaim{PchannelTerm: term}, value: fmt.Sprint(term)}
	}
	claims.remember(pch, claim(4))
	claims.remember(pch, claim(3))
	assert.EqualValues(t, 4, claims.cached(pch).pb.GetPchannelTerm())
	claims.remember(pch, claim(5))
	assert.EqualValues(t, 5, claims.cached(pch).pb.GetPchannelTerm())
	claims.forget(pch)
	claims.remember(pch, claim(3))
	assert.EqualValues(t, 3, claims.cached(pch).pb.GetPchannelTerm())
}

// editFlush returns a flush of vchannel "v" of collection 1 with an Int64
// primary key.
func editFlush(term int64, tick uint64, ids ...int64) *pkindexpb.CommitFlushRequest {
	return &pkindexpb.CommitFlushRequest{
		CollectionId: 1, Vchannel: "v", PkDataType: schemapb.DataType_Int64,
		PchannelTerm: term, AddSsts: ssts(ids...), CheckpointTimeTick: tick,
	}
}

// applyEditFlush applies a flush with SST base path "b".
func applyEditFlush(cur *pkindexpb.Manifest, req *pkindexpb.CommitFlushRequest) (*pkindexpb.Manifest, error) {
	return applyFlush(cur, req, "b")
}

func ssts(ids ...int64) []*pkindexpb.SSTRef {
	out := make([]*pkindexpb.SSTRef, 0, len(ids))
	for _, id := range ids {
		out = append(out, &pkindexpb.SSTRef{Id: id})
	}
	return out
}

func TestPChannelClaimRules(t *testing.T) {
	ctx := context.Background()
	c := newClaimTable(NewFakeMetaStore())

	require.NoError(t, c.claim(ctx, "p", 5))
	assert.EqualValues(t, 5, c.cached("p").pb.GetPchannelTerm())

	require.NoError(t, c.claim(ctx, "p", 5), "the same term is idempotent")
	assert.EqualValues(t, 5, c.cached("p").pb.GetPchannelTerm())

	require.NoError(t, c.claim(ctx, "p", 6))
	assert.EqualValues(t, 6, c.cached("p").pb.GetPchannelTerm())

	assert.True(t, errors.Is(c.claim(ctx, "p", 5), ErrFenced))
}

func TestCheckClaimedTermRules(t *testing.T) {
	ctx := context.Background()
	c := newClaimTable(NewFakeMetaStore())
	_, err := c.checkClaimedTerm(ctx, "p", 5)
	assert.True(t, errors.Is(err, ErrNotClaimed))

	require.NoError(t, c.claim(ctx, "p", 5))
	_, err = c.checkClaimedTerm(ctx, "p", 5)
	assert.NoError(t, err)
	_, err = c.checkClaimedTerm(ctx, "p", 4)
	assert.True(t, errors.Is(err, ErrFenced))
	_, err = c.checkClaimedTerm(ctx, "p", 6)
	assert.True(t, errors.Is(err, ErrNotClaimed))
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
	assert.Equal(t, schemapb.DataType_Int64, m.GetPkDataType())
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

	other := editFlush(6, 300, 9)
	other.PkDataType = schemapb.DataType_VarChar
	_, err = applyEditFlush(m2, other)
	assert.True(t, errors.Is(err, ErrSchemaMismatch))

	up, err := applyEditFlush(m2, editFlush(6, 300, 9))
	require.NoError(t, err)
	assert.EqualValues(t, 6, up.GetLastFlushTerm(), "the flush term becomes the last flush term")
	assert.EqualValues(t, 5, m2.GetLastFlushTerm(), "input must not change")
	assert.Equal(t, []int64{3, 2, 1}, sstIDs(m2), "input must not change")
}

func TestFlushRetryAfterCompaction(t *testing.T) {
	m, err := applyEditFlush(nil, editFlush(5, 100, 2, 1))
	require.NoError(t, err)
	c, err := applyCompaction(m, Compaction{Inputs: []int64{2, 1}, Outputs: ssts(10)})
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

	c, err := applyCompaction(m, Compaction{Inputs: []int64{2, 1}, Outputs: ssts(10)})
	require.NoError(t, err)
	assert.NotNil(t, c)
	assert.Equal(t, []int64{4, 3, 10}, sstIDs(c))
	assert.EqualValues(t, m.GetVersion()+1, c.GetVersion())
	assert.EqualValues(t, 10, c.GetCheckpointTimeTick())
	assert.Equal(t, []int64{4, 3, 2, 1}, sstIDs(m), "input must not change")

	for _, in := range [][]int64{{3, 1}, {1, 2}, {5}, {}} {
		_, err := applyCompaction(m, Compaction{Inputs: in, Outputs: ssts(10)})
		assert.True(t, errors.Is(err, ErrStaleEdit), "%v", in)
	}
	_, err = applyCompaction(m, Compaction{Inputs: []int64{2, 1}, Outputs: ssts(4)})
	assert.True(t, errors.Is(err, ErrDuplicateSST))
	_, err = applyCompaction(nil, Compaction{Inputs: []int64{1}, Outputs: ssts(10)})
	assert.True(t, errors.Is(err, ErrNoManifest))
}

// A retry of an applied compaction finds none of its inputs and all of its
// outputs in the list: it changes nothing.
func TestCompactionRetryOfApplied(t *testing.T) {
	m, err := applyEditFlush(nil, editFlush(1, 10, 4, 3, 2, 1))
	require.NoError(t, err)
	edit := Compaction{Inputs: []int64{2, 1}, Outputs: ssts(11, 10)}
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
	c, err := applyCompaction(m, Compaction{Inputs: []int64{2, 1}, Outputs: ssts(11, 10)})
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
	_, err = applyCompaction(m, Compaction{Inputs: []int64{2, 1}, Outputs: ssts(10, 10)})
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

		nextID := int64(1000)
		fresh := func(count int) []*pkindexpb.SSTRef {
			out := make([]*pkindexpb.SSTRef, 0, count)
			for i := 0; i < count; i++ {
				out = append(out, &pkindexpb.SSTRef{Id: nextID})
				nextID++
			}
			return out
		}

		flushes := make([]*pkindexpb.CommitFlushRequest, 1+r.Intn(4))
		for i := range flushes {
			flushes[i] = editFlush(1, uint64(20+10*i))
			flushes[i].AddSsts = fresh(r.Intn(3))
		}

		// compactions take disjoint contiguous runs of the base list
		var compacts []Compaction
		for pos, want := 0, 1+r.Intn(3); pos < n && len(compacts) < want; {
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
	assert.Equal(t, "pkindex-meta/claim/by-dev-dml_0", ClaimKey("by-dev-dml_0"))
	assert.Equal(t, "files/pk_index/7/by-dev-dml_0_7v0/sst", objpath.SSTBasePath("files", 7, "by-dev-dml_0_7v0"))
	assert.Equal(t, "files/pk_index/7/by-dev-dml_0_7v0/sst/42.sst", objpath.SSTKey("files/pk_index/7/by-dev-dml_0_7v0/sst", 42))
	assert.Equal(t, "files/pk_index/7/by-dev-dml_0_7v0/manifest/3-abc.pb", objpath.ManifestBodyKey(objpath.ManifestDir("files", 7, "by-dev-dml_0_7v0"), 3, "abc"))
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
	id, ok := objpath.ParseSSTKey(base, base+"/42.sst")
	assert.True(t, ok)
	assert.EqualValues(t, 42, id)
	for _, bad := range []string{base + "/42.tmp", base + "/x.sst", "other/42.sst", base + "/sub/42.sst"} {
		_, ok := objpath.ParseSSTKey(base, bad)
		assert.False(t, ok, bad)
	}
}

func TestParseManifestBodyKey(t *testing.T) {
	dir := objpath.ManifestDir("files", 7, "v")
	v, ok := objpath.ParseManifestBodyKey(dir, dir+"/12-abc.pb")
	assert.True(t, ok)
	assert.EqualValues(t, 12, v)
	v, ok = objpath.ParseManifestBodyKey(dir, dir+"/13-0b2f6c1e-8d4a-4f7e-9c3b-5a1d2e3f4a5b.pb")
	assert.True(t, ok)
	assert.EqualValues(t, 13, v)
	for _, bad := range []string{dir + "/12.pb", dir + "/x-abc.pb", dir + "/12-abc.txt"} {
		_, ok := objpath.ParseManifestBodyKey(dir, bad)
		assert.False(t, ok, bad)
	}
}

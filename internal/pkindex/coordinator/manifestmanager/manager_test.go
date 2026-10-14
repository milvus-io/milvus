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
	"github.com/milvus-io/milvus/internal/pkindex/core/manifest"
	"github.com/milvus-io/milvus/internal/pkindex/pkerr"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

// The tests use vchannel vch of collection 1; its pchannel is pch.
const (
	pch = "p"
	vch = "p_1v0"
)

var testLayout = manifest.Layout{RootPath: "files"}

func newTestManager() (*Manager, *FakeMetaStore, *manifest.FakeObjectStore) {
	p, s := NewFakeMetaStore(), manifest.NewFakeObjectStore()
	return New(p, s, testLayout), p, s
}

func flush(term int64, tick uint64, ids ...int64) manifest.Flush {
	f := manifest.Flush{PChannelTerm: term, PKDataType: schemapb.DataType_Int64, CheckpointTimeTick: tick}
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

// bodyKeys returns the manifest body keys of vch in the store.
func bodyKeys(t *testing.T, s *manifest.FakeObjectStore) []string {
	keys := []string{}
	err := s.Walk(context.Background(), testLayout.ManifestDir(1, vch)+"/", func(k string, _ time.Time) bool {
		keys = append(keys, k)
		return true
	})
	require.NoError(t, err)
	return keys
}

// storedClaim returns the term of the stored claim record of pch.
func storedClaim(t *testing.T, p *FakeMetaStore) int64 {
	v, found, err := p.Load(context.Background(), manifest.ClaimKey(pch))
	require.NoError(t, err)
	require.True(t, found)
	var c pkindexpb.PChannelClaim
	require.NoError(t, proto.Unmarshal([]byte(v), &c))
	return c.GetPchannelTerm()
}

// claimed returns a manager on new stores whose pch is claimed for term.
func claimed(t *testing.T, term int64) (*Manager, *FakeMetaStore, *manifest.FakeObjectStore) {
	m, p, s := newTestManager()
	require.NoError(t, m.ClaimPChannel(context.Background(), pch, term))
	return m, p, s
}

// A flush checks the claim record but does not write it.
func TestFlushDoesNotWriteClaimRecord(t *testing.T) {
	m, p, _ := claimed(t, 3)
	_, err := m.CommitFlush(context.Background(), 1, vch, flush(3, 50, 9))
	require.NoError(t, err)
	assert.Len(t, p.LastSave, 1)
	assert.Contains(t, p.LastSave, manifest.PointerKey(1, vch))
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
	assert.True(t, errors.Is(err, manifest.ErrFenced))
	assert.EqualValues(t, 4, storedClaim(t, p))

	// a claim does not create a manifest
	_, found, err := m.Get(ctx, 1, vch)
	require.NoError(t, err)
	assert.False(t, found)

	// a new manager reads the claim record from the store
	err = New(p, s, testLayout).ClaimPChannel(ctx, pch, 3)
	assert.True(t, errors.Is(err, manifest.ErrFenced))
}

// The first flush of a vchannel creates version 1 with the SST base path of
// the layout and the primary key data type of the flush.
func TestFirstFlushCreatesVersion1(t *testing.T) {
	m, p, s := claimed(t, 3)
	ctx := context.Background()
	got, err := m.CommitFlush(ctx, 1, vch, flush(3, 50, 9))
	require.NoError(t, err)
	assert.EqualValues(t, 1, got.GetVersion())
	assert.Equal(t, "files/pk_index/1/p_1v0/sst", got.GetSstBasePath())
	assert.Equal(t, metautil.BuildPKIndexSSTBasePath("files", 1, vch), got.GetSstBasePath())
	assert.Equal(t, schemapb.DataType_Int64, got.GetPkDataType())
	assert.EqualValues(t, 3, got.GetLastFlushTerm())
	assert.Equal(t, []int64{9}, sstIDs(got))

	// a new manager reads the same state back from the stores
	read, found, err := New(p, s, testLayout).Get(ctx, 1, vch)
	require.NoError(t, err)
	require.True(t, found)
	assert.True(t, proto.Equal(got, read))
}

func TestGetWithoutManifest(t *testing.T) {
	m, _, _ := newTestManager()
	got, found, err := m.Get(context.Background(), 1, vch)
	require.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, got)
}

func TestFlushTermRules(t *testing.T) {
	ctx := context.Background()
	m, _, _ := newTestManager()
	_, err := m.CommitFlush(ctx, 1, vch, flush(1, 10))
	assert.True(t, errors.Is(err, manifest.ErrNotClaimed), "no claim record")

	m, _, _ = claimed(t, 3)
	_, err = m.CommitFlush(ctx, 1, vch, flush(2, 10, 1))
	assert.True(t, errors.Is(err, manifest.ErrFenced), "a lower term is fenced")
	_, err = m.CommitFlush(ctx, 1, vch, flush(4, 10, 1))
	assert.True(t, errors.Is(err, manifest.ErrNotClaimed), "a higher term must claim first")
	_, found, err := m.Get(ctx, 1, vch)
	require.NoError(t, err)
	assert.False(t, found)
}

// A flush whose checkpoint does not move forward is a retry when its term is
// the last flush term, and a regression otherwise.
func TestFlushRetryVersusRegression(t *testing.T) {
	m, _, _ := claimed(t, 3)
	ctx := context.Background()
	first, err := m.CommitFlush(ctx, 1, vch, flush(3, 50, 9))
	require.NoError(t, err)

	again, err := m.CommitFlush(ctx, 1, vch, flush(3, 50, 9))
	require.NoError(t, err)
	assert.True(t, proto.Equal(first, again))

	require.NoError(t, m.ClaimPChannel(ctx, pch, 4))
	_, err = m.CommitFlush(ctx, 1, vch, flush(4, 50, 10))
	assert.True(t, errors.Is(err, manifest.ErrCheckpointRegressed))
	_, err = m.CommitFlush(ctx, 1, vch, flush(3, 60, 10))
	assert.True(t, errors.Is(err, manifest.ErrFenced))

	up, err := m.CommitFlush(ctx, 1, vch, flush(4, 60, 10))
	require.NoError(t, err)
	assert.EqualValues(t, 2, up.GetVersion())
	assert.EqualValues(t, 4, up.GetLastFlushTerm())
}

// Another coordinator raises the claim record after this manager cached it:
// a flush with the new term reloads the record and succeeds.
func TestFlushReloadsRaisedClaim(t *testing.T) {
	a, p, s := claimed(t, 3)
	ctx := context.Background()
	_, err := a.CommitFlush(ctx, 1, vch, flush(3, 10, 1))
	require.NoError(t, err)
	require.NoError(t, New(p, s, testLayout).ClaimPChannel(ctx, pch, 4))

	got, err := a.CommitFlush(ctx, 1, vch, flush(4, 20, 2))
	require.NoError(t, err)
	assert.Equal(t, []int64{2, 1}, sstIDs(got))
}

// The claim record changes between the cache read of a flush and its write:
// the write fails, the error is retryable, and the retry is fenced.
func TestFlushFailsWhenClaimChangesBeforeWrite(t *testing.T) {
	a, p, s := claimed(t, 3)
	b := New(p, s, testLayout)
	ctx := context.Background()
	_, err := a.CommitFlush(ctx, 1, vch, flush(3, 10, 1))
	require.NoError(t, err)

	var claimErr error
	p.BeforeWrite = func() { claimErr = b.ClaimPChannel(ctx, pch, 4) }
	_, err = a.CommitFlush(ctx, 1, vch, flush(3, 20, 2))
	require.NoError(t, claimErr)
	assert.True(t, errors.Is(err, pkerr.ErrUnavailable), "%v", err)

	_, err = a.CommitFlush(ctx, 1, vch, flush(3, 20, 2))
	assert.True(t, errors.Is(err, manifest.ErrFenced))
	got, _, err := New(p, s, testLayout).Get(ctx, 1, vch)
	require.NoError(t, err)
	assert.Equal(t, []int64{1}, sstIDs(got))
	assert.EqualValues(t, 4, storedClaim(t, p))
}

// A flush with a stale cached claim record does not pass the claim predicate
// of the write, even when the pointer predicate holds.
func TestFlushWithStaleCachedClaim(t *testing.T) {
	a, p, s := claimed(t, 3)
	ctx := context.Background()
	require.NoError(t, New(p, s, testLayout).ClaimPChannel(ctx, pch, 4))

	_, err := a.CommitFlush(ctx, 1, vch, flush(3, 10, 1))
	assert.True(t, errors.Is(err, pkerr.ErrUnavailable), "%v", err)
	_, found, err := New(p, s, testLayout).Get(ctx, 1, vch)
	require.NoError(t, err)
	assert.False(t, found)
	_, err = a.CommitFlush(ctx, 1, vch, flush(3, 10, 1))
	assert.True(t, errors.Is(err, manifest.ErrFenced))
}

func TestCommitCompaction(t *testing.T) {
	m, _, _ := claimed(t, 1)
	ctx := context.Background()
	_, err := m.CommitCompaction(ctx, 1, vch, manifest.Compaction{Inputs: []int64{1}, Outputs: []*pkindexpb.SSTRef{{Id: 2}}})
	assert.True(t, errors.Is(err, manifest.ErrNotClaimed), "no manifest before the first flush")

	_, err = m.CommitFlush(ctx, 1, vch, flush(1, 10, 3, 2, 1))
	require.NoError(t, err)
	got, err := m.CommitCompaction(ctx, 1, vch, manifest.Compaction{Inputs: []int64{2, 1}, Outputs: []*pkindexpb.SSTRef{{Id: 10}}})
	require.NoError(t, err)
	assert.EqualValues(t, 2, got.GetVersion())
	assert.Equal(t, []int64{3, 10}, sstIDs(got))

	_, err = m.CommitCompaction(ctx, 1, vch, manifest.Compaction{Inputs: []int64{2, 1}, Outputs: []*pkindexpb.SSTRef{{Id: 11}}})
	assert.True(t, errors.Is(err, manifest.ErrStaleEdit))
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
	_, err = New(p, s, testLayout).CommitFlush(ctx, 1, vch, flush(3, 10))
	assert.True(t, errors.Is(err, pkerr.ErrUnavailable))

	for _, v := range []string{"\xff", ""} {
		p.Put(manifest.ClaimKey(pch), v)
		err = New(p, s, testLayout).ClaimPChannel(ctx, pch, 3)
		assert.True(t, errors.Is(err, pkerr.ErrCorrupted), "record %q", v)
		_, err = New(p, s, testLayout).CommitFlush(ctx, 1, vch, flush(3, 10))
		assert.True(t, errors.Is(err, pkerr.ErrCorrupted), "record %q", v)
	}
}

// The pointer swap reports an error after it took effect: the manager reads
// the pointer back and reports success.
func TestSwapUncertainButApplied(t *testing.T) {
	m, p, s := claimed(t, 1)
	ctx := context.Background()
	_, err := m.CommitFlush(ctx, 1, vch, flush(1, 1))
	require.NoError(t, err)
	p.WriteErr, p.WriteApplies = errors.New("timeout"), true
	got, err := m.CommitFlush(ctx, 1, vch, flush(1, 5, 1))
	require.NoError(t, err)
	assert.EqualValues(t, 2, got.GetVersion())

	read, _, err := New(p, s, testLayout).Get(ctx, 1, vch)
	require.NoError(t, err)
	assert.True(t, proto.Equal(got, read))
}

// The caller's ctx ends during the pointer swap and the swap took effect: the
// read back does not use the ended ctx, and the commit reports success.
func TestSwapUncertainAfterCallerCtxEnds(t *testing.T) {
	m, p, s := claimed(t, 1)
	_, err := m.CommitFlush(context.Background(), 1, vch, flush(1, 1))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.BeforeWrite = cancel
	p.WriteErr, p.WriteApplies = context.Canceled, true
	got, err := m.CommitFlush(ctx, 1, vch, flush(1, 5, 1))
	require.NoError(t, err)
	assert.EqualValues(t, 2, got.GetVersion())

	read, _, err := New(p, s, testLayout).Get(context.Background(), 1, vch)
	require.NoError(t, err)
	assert.True(t, proto.Equal(got, read))
}

// The first pointer write reports an error after it took effect: the manager
// reads the pointer back and reports success.
func TestCreateUncertainButApplied(t *testing.T) {
	m, p, s := claimed(t, 1)
	ctx := context.Background()
	p.WriteErr, p.WriteApplies = errors.New("timeout"), true
	got, err := m.CommitFlush(ctx, 1, vch, flush(1, 5, 1))
	require.NoError(t, err)
	assert.EqualValues(t, 1, got.GetVersion())

	read, found, err := New(p, s, testLayout).Get(ctx, 1, vch)
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
	_, err := m.CommitFlush(ctx, 1, vch, flush(1, 5, 1))
	assert.True(t, errors.Is(err, pkerr.ErrUnavailable))
	assert.True(t, errors.Is(err, cause), "the cause stays in the chain")

	got, err := m.CommitFlush(ctx, 1, vch, flush(1, 5, 1))
	require.NoError(t, err)
	assert.EqualValues(t, 1, got.GetVersion())
}

// The pointer swap fails and did not take effect: the error is retryable and
// the next commit succeeds.
func TestSwapFailedIsRetryable(t *testing.T) {
	m, p, _ := claimed(t, 1)
	ctx := context.Background()
	_, err := m.CommitFlush(ctx, 1, vch, flush(1, 1))
	require.NoError(t, err)
	cause := errors.New("timeout")
	p.WriteErr = cause
	_, err = m.CommitFlush(ctx, 1, vch, flush(1, 5, 1))
	assert.True(t, errors.Is(err, pkerr.ErrUnavailable))
	assert.True(t, errors.Is(err, cause), "the cause stays in the chain")
	got, err := m.CommitFlush(ctx, 1, vch, flush(1, 5, 1))
	require.NoError(t, err)
	assert.EqualValues(t, 2, got.GetVersion())
	assert.Equal(t, []int64{1}, sstIDs(got))
}

// The pointer swap took effect, but the read back fails: the commit reports a
// retryable error and drops its cache, so the retry loads the applied version.
func TestReadBackFails(t *testing.T) {
	m, p, _ := claimed(t, 1)
	ctx := context.Background()
	_, err := m.CommitFlush(ctx, 1, vch, flush(1, 1))
	require.NoError(t, err)
	p.WriteErr, p.WriteApplies = errors.New("timeout"), true
	p.LoadErr = errors.New("etcd down")
	_, err = m.CommitFlush(ctx, 1, vch, flush(1, 5, 1))
	assert.True(t, errors.Is(err, pkerr.ErrUnavailable))
	assert.Contains(t, fmt.Sprintf("%+v", err), "etcd down", "the read-back error is kept")

	got, err := m.CommitFlush(ctx, 1, vch, flush(1, 5, 1))
	require.NoError(t, err)
	assert.EqualValues(t, 2, got.GetVersion())
	assert.Equal(t, []int64{1}, sstIDs(got))
}

// A crash between the body write and the pointer swap leaves the old version
// current and an orphan body in the store.
func TestCrashBeforeSwap(t *testing.T) {
	m, p, s := claimed(t, 1)
	ctx := context.Background()
	old, err := m.CommitFlush(ctx, 1, vch, flush(1, 1))
	require.NoError(t, err)
	require.Len(t, bodyKeys(t, s), 1)

	p.WriteErr = errors.New("crash")
	_, err = m.CommitFlush(ctx, 1, vch, flush(1, 5, 1))
	require.Error(t, err)

	got, found, err := New(p, s, testLayout).Get(ctx, 1, vch)
	require.NoError(t, err)
	require.True(t, found)
	assert.True(t, proto.Equal(old, got))

	keys := bodyKeys(t, s)
	require.Len(t, keys, 2)
	var orphans int
	for _, k := range keys {
		v, ok := manifest.ParseManifestBodyKey(testLayout.ManifestDir(1, vch), k)
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
	b := New(p, s, testLayout)
	ctx := context.Background()

	var aGot *pkindexpb.Manifest
	var aErr error
	// b has loaded "not found" and written its body; a creates the first
	// version before b writes the pointer
	p.BeforeWrite = func() { aGot, aErr = a.CommitFlush(ctx, 1, vch, flush(1, 5, 1)) }
	_, err := b.CommitFlush(ctx, 1, vch, flush(1, 6, 2))
	assert.True(t, errors.Is(err, pkerr.ErrUnavailable))
	require.NoError(t, aErr)
	assert.EqualValues(t, 1, aGot.GetVersion())

	got, err := b.CommitFlush(ctx, 1, vch, flush(1, 6, 2))
	require.NoError(t, err)
	assert.EqualValues(t, 2, got.GetVersion())
	assert.Equal(t, []int64{2, 1}, sstIDs(got))

	read, found, err := New(p, s, testLayout).Get(ctx, 1, vch)
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
	bodyKey := testLayout.ManifestBodyKey(1, vch, 5, "x")
	require.NoError(t, s.Write(ctx, bodyKey, body))
	ptr, err := proto.Marshal(&pkindexpb.ManifestPointer{Version: 6, BodyKey: bodyKey})
	require.NoError(t, err)
	p.Put(manifest.PointerKey(1, vch), string(ptr))

	_, _, err = m.Get(ctx, 1, vch)
	assert.True(t, errors.Is(err, pkerr.ErrCorrupted))
	_, err = m.CommitFlush(ctx, 1, vch, flush(1, 5, 1))
	assert.True(t, errors.Is(err, pkerr.ErrCorrupted))
}

func TestLoadErrors(t *testing.T) {
	ctx := context.Background()
	m, p, s := claimed(t, 1)
	_, err := m.CommitFlush(ctx, 1, vch, flush(1, 1))
	require.NoError(t, err)

	// a fresh manager has no cache, so every Get reads the stores
	t.Run("pointer read fails", func(t *testing.T) {
		p.LoadErr = errors.New("etcd down")
		_, _, err := New(p, s, testLayout).Get(ctx, 1, vch)
		assert.True(t, errors.Is(err, pkerr.ErrUnavailable))
	})
	t.Run("body read fails", func(t *testing.T) {
		s.ReadErr = errors.New("s3 down")
		_, _, err := New(p, s, testLayout).Get(ctx, 1, vch)
		assert.True(t, errors.Is(err, pkerr.ErrUnavailable))
		assert.False(t, errors.Is(err, pkerr.ErrCorrupted))
	})
	t.Run("body missing", func(t *testing.T) {
		require.NoError(t, s.MultiRemove(ctx, bodyKeys(t, s)))
		_, _, err := New(p, s, testLayout).Get(ctx, 1, vch)
		assert.True(t, errors.Is(err, pkerr.ErrCorrupted))
		assert.Equal(t, merr.Code(merr.ErrDataIntegrity), merr.Code(pkerr.ToMerr(err)), "a missing body is not retryable")
	})
	t.Run("pointer undecodable", func(t *testing.T) {
		p.Put(manifest.PointerKey(1, vch), "\xff")
		_, _, err := New(p, s, testLayout).Get(ctx, 1, vch)
		assert.True(t, errors.Is(err, pkerr.ErrCorrupted))
	})
	t.Run("pointer names no body", func(t *testing.T) {
		zeroVersion, err := proto.Marshal(&pkindexpb.ManifestPointer{BodyKey: testLayout.ManifestBodyKey(1, vch, 1, "x")})
		require.NoError(t, err)
		for _, ptr := range []string{"", string(zeroVersion)} {
			p.Put(manifest.PointerKey(1, vch), ptr)
			// an object store rejects an empty key as a request error, not as a missing object
			s.ReadErr = errors.New("object name cannot be empty")
			_, _, err := New(p, s, testLayout).Get(ctx, 1, vch)
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
	_, err := m.CommitFlush(ctx, 1, vch, flush(1, 5, 1))
	assert.True(t, errors.Is(err, pkerr.ErrUnavailable))
	_, found, err := New(p, s, testLayout).Get(ctx, 1, vch)
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
	_, err := m.CommitFlush(ctx, 1, vch, flush(1, 10, base...))
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 1; i <= compacts; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			_, err := m.CommitCompaction(ctx, 1, vch, manifest.Compaction{Inputs: []int64{id}, Outputs: []*pkindexpb.SSTRef{{Id: 1000 + id}}})
			assert.NoError(t, err)
		}(int64(i))
	}
	// flushes need an increasing checkpoint, so one goroutine sends them in order
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 1; j <= flushes; j++ {
			_, err := m.CommitFlush(ctx, 1, vch, flush(1, uint64(10+j), int64(2000+j)))
			assert.NoError(t, err)
		}
	}()
	wg.Wait()

	got, found, err := New(p, s, testLayout).Get(ctx, 1, vch)
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
				_, err := m.CommitFlush(ctx, 1, v, flush(1, uint64(j), int64(j)))
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
	b := New(p, s, testLayout)
	ctx := context.Background()
	_, err := a.CommitFlush(ctx, 1, vch, flush(1, 1))
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
				got, err := mgr.CommitFlush(ctx, 1, vch, flush(1, tick, id))
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

	got, found, err := New(p, s, testLayout).Get(ctx, 1, vch)
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
			_, err := m.CommitFlush(ctx, 1, v, flush(1, 10, int64(i+1)))
			require.NoError(t, err)
		}
	}
	p.Put(manifest.PointerKey(1, targets[1].VChannel), "\xff")

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
			assert.Contains(t, r.Pointer.GetBodyKey(), testLayout.ManifestDir(1, targets[i].VChannel)+"/")
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
	_, err := m.CommitFlush(ctx, 1, last, flush(1, 10, 1))
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
	_, err := a.CommitFlush(ctx, 1, vch, flush(1, 10, 1))
	require.NoError(t, err)
	got, err := New(p, s, testLayout).CommitFlush(ctx, 1, vch, flush(1, 20, 2))
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

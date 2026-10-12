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

package service

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/pkindex/coordinator/internal/fakes"
	"github.com/milvus-io/milvus/internal/pkindex/coordinator/manifestmanager"
	"github.com/milvus-io/milvus/internal/pkindex/core/manifest"
	"github.com/milvus-io/milvus/internal/pkindex/pkerr"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func newService() (*Service, *fakes.Pointers, *fakes.Store) {
	p, st := fakes.NewPointers(), fakes.NewStore()
	s := New()
	s.SetManager(manifestmanager.New(p, st, manifest.Layout{RootPath: "files"}))
	return s, p, st
}

func claimReq(vchannel string, term int64) *pkindexpb.ClaimManifestRequest {
	return &pkindexpb.ClaimManifestRequest{CollectionId: 1, Vchannel: vchannel, PchannelTerm: term, PkDataType: schemapb.DataType_Int64}
}

func flushReq(vchannel string, term int64, sst int64, tick uint64) *pkindexpb.CommitFlushRequest {
	return &pkindexpb.CommitFlushRequest{
		CollectionId: 1, Vchannel: vchannel, PchannelTerm: term,
		AddSsts: []*pkindexpb.SSTRef{{Id: sst}}, CheckpointTimeTick: tick,
	}
}

// statusErr turns the status of a response back into the error a client sees.
func statusErr(st *commonpb.Status) error { return merr.Error(st) }

func assertCode(t *testing.T, st *commonpb.Status, code int32, retryable bool) {
	t.Helper()
	err := statusErr(st)
	require.Error(t, err)
	assert.Equal(t, code, merr.Code(err))
	assert.Equal(t, retryable, merr.IsRetryableErr(err))
	assert.Equal(t, retryable, st.GetRetriable())
}

func TestNotReady(t *testing.T) {
	s := New()
	ctx := context.Background()
	get, err := s.GetManifest(ctx, &pkindexpb.GetManifestRequest{CollectionId: 1, Vchannel: "v"})
	require.NoError(t, err)
	assertCode(t, get.GetStatus(), merr.Code(merr.ErrServiceNotReady), true)
	claim, err := s.ClaimManifest(ctx, claimReq("v", 1))
	require.NoError(t, err)
	assertCode(t, claim.GetStatus(), merr.Code(merr.ErrServiceNotReady), true)
	flush, err := s.CommitFlush(ctx, flushReq("v", 1, 1, 1))
	require.NoError(t, err)
	assertCode(t, flush.GetStatus(), merr.Code(merr.ErrServiceNotReady), true)
}

func TestFlow(t *testing.T) {
	s, _, _ := newService()
	ctx := context.Background()

	get, err := s.GetManifest(ctx, &pkindexpb.GetManifestRequest{CollectionId: 1, Vchannel: "v"})
	require.NoError(t, merr.CheckRPCCall(get, err))
	assert.False(t, get.GetFound())
	assert.Nil(t, get.GetManifest())

	claim, err := s.ClaimManifest(ctx, claimReq("v", 2))
	require.NoError(t, merr.CheckRPCCall(claim, err))
	assert.EqualValues(t, 1, claim.GetManifest().GetVersion())

	flush, err := s.CommitFlush(ctx, flushReq("v", 2, 7, 9))
	require.NoError(t, merr.CheckRPCCall(flush, err))
	assert.EqualValues(t, 2, flush.GetManifest().GetVersion())
	require.Len(t, flush.GetManifest().GetSsts(), 1)
	assert.EqualValues(t, 7, flush.GetManifest().GetSsts()[0].GetId())

	get, err = s.GetManifest(ctx, &pkindexpb.GetManifestRequest{CollectionId: 1, Vchannel: "v"})
	require.NoError(t, merr.CheckRPCCall(get, err))
	assert.True(t, get.GetFound())
	assert.EqualValues(t, 2, get.GetManifest().GetVersion())
}

func TestFenced(t *testing.T) {
	s, _, _ := newService()
	ctx := context.Background()
	_, err := s.ClaimManifest(ctx, claimReq("v", 2))
	require.NoError(t, err)

	fenced, err := s.CommitFlush(ctx, flushReq("v", 1, 8, 10))
	require.NoError(t, err)
	assertCode(t, fenced.GetStatus(), merr.Code(merr.ErrChannelNotAvailable), false)
	assert.True(t, errors.Is(statusErr(fenced.GetStatus()), merr.ErrChannelNotAvailable))

	oldClaim, err := s.ClaimManifest(ctx, claimReq("v", 1))
	require.NoError(t, err)
	assertCode(t, oldClaim.GetStatus(), merr.Code(merr.ErrChannelNotAvailable), false)
}

func TestNotClaimed(t *testing.T) {
	s, _, _ := newService()
	resp, err := s.CommitFlush(context.Background(), flushReq("w", 1, 1, 1))
	require.NoError(t, err)
	assertCode(t, resp.GetStatus(), merr.Code(merr.ErrServiceInternal), false)
}

func TestCheckpointRegressed(t *testing.T) {
	s, _, _ := newService()
	ctx := context.Background()
	_, err := s.ClaimManifest(ctx, claimReq("v", 1))
	require.NoError(t, err)
	_, err = s.CommitFlush(ctx, flushReq("v", 1, 1, 9))
	require.NoError(t, err)
	resp, err := s.CommitFlush(ctx, flushReq("v", 2, 2, 5))
	require.NoError(t, err)
	assertCode(t, resp.GetStatus(), merr.Code(merr.ErrServiceInternal), false)
}

func TestDuplicateSST(t *testing.T) {
	s, _, _ := newService()
	ctx := context.Background()
	_, err := s.ClaimManifest(ctx, claimReq("v", 1))
	require.NoError(t, err)
	_, err = s.CommitFlush(ctx, flushReq("v", 1, 1, 9))
	require.NoError(t, err)
	resp, err := s.CommitFlush(ctx, flushReq("v", 1, 1, 10))
	require.NoError(t, err)
	assertCode(t, resp.GetStatus(), merr.Code(merr.ErrServiceInternal), false)
}

func TestSchemaMismatch(t *testing.T) {
	s, _, _ := newService()
	ctx := context.Background()
	_, err := s.ClaimManifest(ctx, claimReq("v", 1))
	require.NoError(t, err)
	req := claimReq("v", 1)
	req.PkDataType = schemapb.DataType_VarChar
	resp, err := s.ClaimManifest(ctx, req)
	require.NoError(t, err)
	assertCode(t, resp.GetStatus(), merr.Code(merr.ErrServiceInternal), false)
}

func TestPointerStoreUnavailable(t *testing.T) {
	s, p, _ := newService()
	ctx := context.Background()

	p.LoadErr = errors.New("etcd down")
	get, err := s.GetManifest(ctx, &pkindexpb.GetManifestRequest{CollectionId: 1, Vchannel: "v"})
	require.NoError(t, err)
	assertCode(t, get.GetStatus(), merr.Code(merr.ErrServiceUnavailable), true)

	p.WriteErr = errors.New("etcd down")
	claim, err := s.ClaimManifest(ctx, claimReq("v", 1))
	require.NoError(t, err)
	assertCode(t, claim.GetStatus(), merr.Code(merr.ErrServiceUnavailable), true)
}

func TestObjectStoreUnavailable(t *testing.T) {
	s, _, st := newService()
	st.WriteErr = errors.New("object storage down")
	resp, err := s.ClaimManifest(context.Background(), claimReq("v", 1))
	require.NoError(t, err)
	assertCode(t, resp.GetStatus(), merr.Code(merr.ErrServiceUnavailable), true)
}

func TestMissingBody(t *testing.T) {
	s, p, _ := newService()
	ctx := context.Background()
	_, err := s.ClaimManifest(ctx, claimReq("v", 1))
	require.NoError(t, err)

	// a new service over the same pointers and an empty object store has no cache
	other := New()
	other.SetManager(manifestmanager.New(p, fakes.NewStore(), manifest.Layout{RootPath: "files"}))
	get, err := other.GetManifest(ctx, &pkindexpb.GetManifestRequest{CollectionId: 1, Vchannel: "v"})
	require.NoError(t, err)
	assertCode(t, get.GetStatus(), merr.Code(merr.ErrDataIntegrity), false)
}

func TestToMerr(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		code      int32
		retryable bool
	}{
		{"fenced", manifest.ErrFenced, merr.Code(merr.ErrChannelNotAvailable), false},
		{"wrapped fenced", errors.Wrap(manifest.ErrFenced, "context"), merr.Code(merr.ErrChannelNotAvailable), false},
		{"not claimed", manifest.ErrNotClaimed, merr.Code(merr.ErrServiceInternal), false},
		{"checkpoint regressed", manifest.ErrCheckpointRegressed, merr.Code(merr.ErrServiceInternal), false},
		{"duplicate sst", manifest.ErrDuplicateSST, merr.Code(merr.ErrServiceInternal), false},
		{"stale edit", manifest.ErrStaleEdit, merr.Code(merr.ErrServiceInternal), false},
		{"schema mismatch", manifest.ErrSchemaMismatch, merr.Code(merr.ErrServiceInternal), false},
		{"unavailable", errors.Mark(errors.New("x"), pkerr.ErrUnavailable), merr.Code(merr.ErrServiceUnavailable), true},
		{"corrupted", errors.Mark(errors.New("x"), pkerr.ErrCorrupted), merr.Code(merr.ErrDataIntegrity), false},
		{"unmarked", errors.New("x"), merr.Code(merr.ErrServiceInternal), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := toMerr(c.err, "v")
			assert.Equal(t, c.code, merr.Code(got))
			assert.Equal(t, c.retryable, merr.IsRetryableErr(got))
			assert.Equal(t, c.code, merr.Status(got).GetCode())
		})
	}
}

func TestToMerrFencedKeepsCause(t *testing.T) {
	cause := errors.Wrap(manifest.ErrFenced, "recorded term 5 is above request term 3")
	got := toMerr(cause, "v")
	assert.Contains(t, fmt.Sprintf("%+v", got), "recorded term 5 is above request term 3")
	assert.NotContains(t, got.Error(), "recorded term 5")
}

func TestInvalidRequest(t *testing.T) {
	s, _, _ := newService()
	ctx := context.Background()
	cases := []struct {
		name         string
		collectionID int64
		vchannel     string
	}{
		{"zero collection", 0, "v"},
		{"negative collection", -1, "v"},
		{"empty vchannel", 1, ""},
		{"slash in vchannel", 1, "a/b"},
		{"dot vchannel", 1, "."},
		{"dot dot vchannel", 1, ".."},
		{"leading slash in vchannel", 1, "/v"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			claim, err := s.ClaimManifest(ctx, &pkindexpb.ClaimManifestRequest{
				CollectionId: c.collectionID, Vchannel: c.vchannel, PchannelTerm: 1, PkDataType: schemapb.DataType_Int64,
			})
			require.NoError(t, err)
			assertCode(t, claim.GetStatus(), merr.Code(merr.ErrParameterInvalid), false)

			flush, err := s.CommitFlush(ctx, &pkindexpb.CommitFlushRequest{
				CollectionId: c.collectionID, Vchannel: c.vchannel, PchannelTerm: 1,
				AddSsts: []*pkindexpb.SSTRef{{Id: 1}}, CheckpointTimeTick: 1,
			})
			require.NoError(t, err)
			assertCode(t, flush.GetStatus(), merr.Code(merr.ErrParameterInvalid), false)

			get, err := s.GetManifest(ctx, &pkindexpb.GetManifestRequest{CollectionId: c.collectionID, Vchannel: c.vchannel})
			require.NoError(t, err)
			assertCode(t, get.GetStatus(), merr.Code(merr.ErrParameterInvalid), false)
		})
	}
}

// A vchannel name with ".." inside it, but no "/", is a valid name.
func TestVChannelWithDotsInside(t *testing.T) {
	s, _, _ := newService()
	ctx := context.Background()
	claim, err := s.ClaimManifest(ctx, claimReq("a..b", 1))
	require.NoError(t, err)
	require.NoError(t, statusErr(claim.GetStatus()))
	flush, err := s.CommitFlush(ctx, flushReq("a..b", 1, 1, 5))
	require.NoError(t, err)
	require.NoError(t, statusErr(flush.GetStatus()))
	get, err := s.GetManifest(ctx, &pkindexpb.GetManifestRequest{CollectionId: 1, Vchannel: "a..b"})
	require.NoError(t, err)
	require.NoError(t, statusErr(get.GetStatus()))
	assert.True(t, get.GetFound())
}

func TestClaimPKDataType(t *testing.T) {
	cases := []struct {
		pkType schemapb.DataType
		valid  bool
	}{
		{schemapb.DataType_Int64, true},
		{schemapb.DataType_VarChar, true},
		{schemapb.DataType_None, false},
		{schemapb.DataType_Int32, false},
		{schemapb.DataType_String, false},
		{schemapb.DataType_FloatVector, false},
	}
	for _, c := range cases {
		t.Run(c.pkType.String(), func(t *testing.T) {
			s, _, _ := newService()
			claim, err := s.ClaimManifest(context.Background(), &pkindexpb.ClaimManifestRequest{
				CollectionId: 1, Vchannel: "v", PchannelTerm: 1, PkDataType: c.pkType,
			})
			require.NoError(t, err)
			if c.valid {
				assert.NoError(t, statusErr(claim.GetStatus()))
				return
			}
			assertCode(t, claim.GetStatus(), merr.Code(merr.ErrParameterInvalid), false)
		})
	}
}

func TestCommitFlushCheckpointTimeTick(t *testing.T) {
	cases := []struct {
		name  string
		tick  uint64
		valid bool
	}{
		{"zero", 0, false},
		{"one", 1, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, _, _ := newService()
			ctx := context.Background()
			claim, err := s.ClaimManifest(ctx, claimReq("v", 1))
			require.NoError(t, err)
			require.NoError(t, statusErr(claim.GetStatus()))
			flush, err := s.CommitFlush(ctx, flushReq("v", 1, 1, c.tick))
			require.NoError(t, err)
			if c.valid {
				assert.NoError(t, statusErr(flush.GetStatus()))
				return
			}
			assertCode(t, flush.GetStatus(), merr.Code(merr.ErrParameterInvalid), false)
		})
	}
}

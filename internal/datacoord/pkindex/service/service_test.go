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
	"github.com/milvus-io/milvus/internal/datacoord/pkindex/manifest"
	"github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// The tests use vchannels of pchannel pch.
const (
	pch = "p"
	vch = "p_1v0"
)

func newService() (*Service, *manifest.FakeMetaStore, *manifest.FakeObjectStore) {
	p, st := manifest.NewFakeMetaStore(), manifest.NewFakeObjectStore()
	s := New()
	s.SetManager(manifest.New(p, st, "files"))
	return s, p, st
}

func flushReq(vchannel string, term int64, sst int64, tick uint64) *pkindexpb.CommitFlushRequest {
	return &pkindexpb.CommitFlushRequest{
		CollectionId: 1, IndexId: 2, Vchannel: vchannel, PchannelTerm: term,
		AddSsts: []*pkindexpb.SSTRef{{Id: sst}}, CheckpointTimeTick: tick,
	}
}

func getReq(vchannels ...string) *pkindexpb.GetRecoveryManifestsRequest {
	req := &pkindexpb.GetRecoveryManifestsRequest{}
	for _, v := range vchannels {
		req.Targets = append(req.Targets, &pkindexpb.ManifestTarget{CollectionId: 1, IndexId: 2, Vchannel: v})
	}
	return req
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
	get, err := s.GetRecoveryManifests(ctx, getReq(vch))
	require.NoError(t, err)
	assertCode(t, get.GetStatus(), merr.Code(merr.ErrServiceNotReady), true)
	flush, err := s.CommitFlush(ctx, flushReq(vch, 1, 1, 1))
	require.NoError(t, err)
	assertCode(t, flush.GetStatus(), merr.Code(merr.ErrServiceNotReady), true)
}

func TestFlow(t *testing.T) {
	s, p, _ := newService()
	ctx := context.Background()
	p.AssignPChannel(pch, 2)

	flush, err := s.CommitFlush(ctx, flushReq(vch, 2, 7, 9))
	require.NoError(t, merr.CheckRPCCall(flush, err))
	assert.EqualValues(t, 1, flush.GetManifest().GetVersion())
	assert.EqualValues(t, 2, flush.GetManifest().GetLastFlushTerm())
	require.Len(t, flush.GetManifest().GetSsts(), 1)
	assert.EqualValues(t, 7, flush.GetManifest().GetSsts()[0].GetId())

	other := "p_1v1"
	get, err := s.GetRecoveryManifests(ctx, getReq(vch, other))
	require.NoError(t, merr.CheckRPCCall(get, err))
	require.Len(t, get.GetEntries(), 2)
	found, missing := get.GetEntries()[0], get.GetEntries()[1]
	require.NoError(t, merr.Error(found.GetStatus()))
	assert.True(t, found.GetFound())
	assert.EqualValues(t, 1, found.GetPointer().GetVersion())
	assert.Contains(t, found.GetPointer().GetBodyKey(), "files/pk_index/1/2/p_1v0/manifest/1-")
	require.NoError(t, merr.Error(missing.GetStatus()))
	assert.False(t, missing.GetFound())
	assert.Nil(t, missing.GetPointer())
}

// One target with a corrupted pointer gets its own status; the other targets
// still return their pointers.
func TestRecoveryManifestsItemStatus(t *testing.T) {
	s, p, _ := newService()
	ctx := context.Background()
	p.AssignPChannel(pch, 1)
	_, err := s.CommitFlush(ctx, flushReq(vch, 1, 1, 1))
	require.NoError(t, err)
	p.Put(manifest.PointerKey(1, 2, "p_1v1"), "\xff")

	get, err := s.GetRecoveryManifests(ctx, getReq("p_1v1", vch, "p_1v2"))
	require.NoError(t, merr.CheckRPCCall(get, err))
	require.Len(t, get.GetEntries(), 3)
	bad, good, missing := get.GetEntries()[0], get.GetEntries()[1], get.GetEntries()[2]
	assertCode(t, bad.GetStatus(), merr.Code(merr.ErrDataIntegrity), false)
	assert.False(t, bad.GetFound())
	require.NoError(t, merr.Error(good.GetStatus()))
	assert.True(t, good.GetFound())
	require.NoError(t, merr.Error(missing.GetStatus()))
	assert.False(t, missing.GetFound())
}

func TestRecoveryManifestsInvalidTargets(t *testing.T) {
	s, _, _ := newService()
	ctx := context.Background()
	many := make([]string, maxRecoveryTargets+1)
	for i := range many {
		many[i] = fmt.Sprintf("p_1v%d", i)
	}
	cases := map[string]*pkindexpb.GetRecoveryManifestsRequest{
		"no targets":         {},
		"too many targets":   getReq(many...),
		"repeated target":    getReq(vch, "p_1v1", vch),
		"invalid collection": {Targets: []*pkindexpb.ManifestTarget{{CollectionId: 0, Vchannel: vch}}},
	}
	for name, req := range cases {
		t.Run(name, func(t *testing.T) {
			resp, err := s.GetRecoveryManifests(ctx, req)
			require.NoError(t, err)
			assertCode(t, resp.GetStatus(), merr.Code(merr.ErrServiceInternal), false)
			assert.Empty(t, resp.GetEntries())
		})
	}

	ok, err := s.GetRecoveryManifests(ctx, getReq(many[:maxRecoveryTargets]...))
	require.NoError(t, merr.CheckRPCCall(ok, err))
	assert.Len(t, ok.GetEntries(), maxRecoveryTargets)
}

func TestFenced(t *testing.T) {
	s, p, _ := newService()
	ctx := context.Background()
	p.AssignPChannel(pch, 2)

	fenced, err := s.CommitFlush(ctx, flushReq(vch, 1, 8, 10))
	require.NoError(t, err)
	assertCode(t, fenced.GetStatus(), merr.Code(merr.ErrChannelNotAvailable), false)
	assert.True(t, errors.Is(statusErr(fenced.GetStatus()), merr.ErrChannelNotAvailable))
}

func TestTermNotAssigned(t *testing.T) {
	s, p, _ := newService()
	ctx := context.Background()
	resp, err := s.CommitFlush(ctx, flushReq(vch, 1, 1, 1))
	require.NoError(t, err)
	assertCode(t, resp.GetStatus(), merr.Code(merr.ErrServiceInternal), false)

	p.AssignPChannel(pch, 1)
	resp, err = s.CommitFlush(ctx, flushReq(vch, 2, 1, 1))
	require.NoError(t, err)
	assertCode(t, resp.GetStatus(), merr.Code(merr.ErrServiceInternal), false)
}

func TestCheckpointRegressed(t *testing.T) {
	s, p, _ := newService()
	ctx := context.Background()
	p.AssignPChannel(pch, 1)
	_, err := s.CommitFlush(ctx, flushReq(vch, 1, 1, 9))
	require.NoError(t, err)
	p.AssignPChannel(pch, 2)
	resp, err := s.CommitFlush(ctx, flushReq(vch, 2, 2, 5))
	require.NoError(t, err)
	assertCode(t, resp.GetStatus(), merr.Code(merr.ErrServiceInternal), false)
}

func TestDuplicateSST(t *testing.T) {
	s, p, _ := newService()
	ctx := context.Background()
	p.AssignPChannel(pch, 1)
	_, err := s.CommitFlush(ctx, flushReq(vch, 1, 1, 9))
	require.NoError(t, err)
	resp, err := s.CommitFlush(ctx, flushReq(vch, 1, 1, 10))
	require.NoError(t, err)
	assertCode(t, resp.GetStatus(), merr.Code(merr.ErrServiceInternal), false)
}

func TestMetaStoreUnavailable(t *testing.T) {
	s, p, _ := newService()
	ctx := context.Background()

	p.MultiLoadErr = errors.New("etcd down")
	get, err := s.GetRecoveryManifests(ctx, getReq(vch))
	require.NoError(t, merr.CheckRPCCall(get, err))
	require.Len(t, get.GetEntries(), 1)
	assertCode(t, get.GetEntries()[0].GetStatus(), merr.Code(merr.ErrServiceUnavailable), true)

	p.AssignPChannel(pch, 1)
	p.LoadErr = errors.New("etcd down")
	flush, err := s.CommitFlush(ctx, flushReq(vch, 1, 1, 1))
	require.NoError(t, err)
	assertCode(t, flush.GetStatus(), merr.Code(merr.ErrServiceUnavailable), true)

	p.WriteErr = errors.New("etcd down")
	flush, err = s.CommitFlush(ctx, flushReq(vch, 1, 1, 1))
	require.NoError(t, err)
	assertCode(t, flush.GetStatus(), merr.Code(merr.ErrServiceUnavailable), true)
}

func TestObjectStoreUnavailable(t *testing.T) {
	s, p, st := newService()
	p.AssignPChannel(pch, 1)
	st.WriteErr = errors.New("object storage down")
	resp, err := s.CommitFlush(context.Background(), flushReq(vch, 1, 1, 1))
	require.NoError(t, err)
	assertCode(t, resp.GetStatus(), merr.Code(merr.ErrServiceUnavailable), true)
}

func TestMissingBody(t *testing.T) {
	s, p, _ := newService()
	ctx := context.Background()
	p.AssignPChannel(pch, 1)
	_, err := s.CommitFlush(ctx, flushReq(vch, 1, 1, 1))
	require.NoError(t, err)

	// a new service over the same pointers and an empty object store has no cache
	other := New()
	other.SetManager(manifest.New(p, manifest.NewFakeObjectStore(), "files"))
	flush, err := other.CommitFlush(ctx, flushReq(vch, 1, 2, 2))
	require.NoError(t, err)
	assertCode(t, flush.GetStatus(), merr.Code(merr.ErrDataIntegrity), false)
}

func TestEmptyPointer(t *testing.T) {
	s, p, st := newService()
	p.AssignPChannel(pch, 1)
	p.Put(manifest.PointerKey(1, 2, vch), "")
	st.ReadErr = errors.New("object name cannot be empty")
	flush, err := s.CommitFlush(context.Background(), flushReq(vch, 1, 1, 1))
	require.NoError(t, err)
	assertCode(t, flush.GetStatus(), merr.Code(merr.ErrDataIntegrity), false)
}

func TestCorruptPChannelMeta(t *testing.T) {
	s, p, _ := newService()
	p.Put(streamingcoord.BuildPChannelInfoPath(pch), "\xff")
	flush, err := s.CommitFlush(context.Background(), flushReq(vch, 1, 1, 1))
	require.NoError(t, err)
	assertCode(t, flush.GetStatus(), merr.Code(merr.ErrDataIntegrity), false)
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
		{"term not assigned", manifest.ErrTermNotAssigned, merr.Code(merr.ErrServiceInternal), false},
		{"no manifest", manifest.ErrNoManifest, merr.Code(merr.ErrServiceInternal), false},
		{"checkpoint regressed", manifest.ErrCheckpointRegressed, merr.Code(merr.ErrServiceInternal), false},
		{"duplicate sst", manifest.ErrDuplicateSST, merr.Code(merr.ErrServiceInternal), false},
		{"stale edit", manifest.ErrStaleEdit, merr.Code(merr.ErrServiceInternal), false},
		{"unavailable", errors.Mark(errors.New("x"), pkindex.ErrUnavailable), merr.Code(merr.ErrServiceUnavailable), true},
		{"corrupted", errors.Mark(errors.New("x"), pkindex.ErrCorrupted), merr.Code(merr.ErrDataIntegrity), false},
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

func TestInvalidTarget(t *testing.T) {
	s, p, _ := newService()
	ctx := context.Background()
	p.AssignPChannel(pch, 1)
	cases := []struct {
		name         string
		collectionID int64
		indexID      int64
		vchannel     string
	}{
		{"zero collection", 0, 2, vch},
		{"negative collection", -1, 2, vch},
		{"zero index", 1, 0, vch},
		{"negative index", 1, -1, vch},
		{"empty vchannel", 1, 2, ""},
		{"slash in vchannel", 1, 2, "p_1v0/b"},
		{"dot vchannel", 1, 2, "."},
		{"dot dot vchannel", 1, 2, ".."},
		{"leading slash in vchannel", 1, 2, "/p_1v0"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := flushReq(c.vchannel, 1, 1, 1)
			req.CollectionId = c.collectionID
			req.IndexId = c.indexID
			flush, err := s.CommitFlush(ctx, req)
			require.NoError(t, err)
			assertCode(t, flush.GetStatus(), merr.Code(merr.ErrServiceInternal), false)

			get, err := s.GetRecoveryManifests(ctx, &pkindexpb.GetRecoveryManifestsRequest{Targets: []*pkindexpb.ManifestTarget{
				{CollectionId: 1, IndexId: 2, Vchannel: vch},
				{CollectionId: c.collectionID, IndexId: c.indexID, Vchannel: c.vchannel},
			}})
			require.NoError(t, err)
			assertCode(t, get.GetStatus(), merr.Code(merr.ErrServiceInternal), false)
		})
	}
}

func TestInvalidFlush(t *testing.T) {
	s, p, _ := newService()
	p.AssignPChannel(pch, 1)
	cases := map[string]func(*pkindexpb.CommitFlushRequest){
		"vchannel names no pchannel": func(r *pkindexpb.CommitFlushRequest) { r.Vchannel = "p" },
		"pchannel name as vchannel":  func(r *pkindexpb.CommitFlushRequest) { r.Vchannel = "by-dev-dml_0" },
		"zero term":                  func(r *pkindexpb.CommitFlushRequest) { r.PchannelTerm = 0 },
		"zero checkpoint":            func(r *pkindexpb.CommitFlushRequest) { r.CheckpointTimeTick = 0 },
		"reset after the checkpoint": func(r *pkindexpb.CommitFlushRequest) { r.ResetTimeTick = r.GetCheckpointTimeTick() + 1 },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			req := flushReq(vch, 1, 1, 1)
			change(req)
			resp, err := s.CommitFlush(context.Background(), req)
			require.NoError(t, err)
			assertCode(t, resp.GetStatus(), merr.Code(merr.ErrServiceInternal), false)
		})
	}
}

// A real vchannel name maps to its pchannel.
func TestValidFlush(t *testing.T) {
	s, p, _ := newService()
	ctx := context.Background()
	p.AssignPChannel("by-dev-dml_0", 1)
	resp, err := s.CommitFlush(ctx, flushReq("by-dev-dml_0_449v0", 1, 1, 1))
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assert.Equal(t, "by-dev-dml_0_449v0", resp.GetManifest().GetVchannel())
}

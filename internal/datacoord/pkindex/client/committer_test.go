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

package client

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/pkindex/manifest"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func sampleManifest() *pkindexpb.Manifest {
	return &pkindexpb.Manifest{
		CollectionId:       7,
		Vchannel:           "ch_0_7v0",
		Version:            3,
		LastFlushTerm:      2,
		CheckpointTimeTick: 900,
		SstBasePath:        "root/pk_index/7/11/ch_0_7v0/sst",
		IndexId:            11,
		Ssts: []*pkindexpb.SSTRef{
			{Id: 102, TableFormat: uint32(sst.TableFormat), Size: 20, MinKey: []byte("c"), MaxKey: []byte("d"), NumEntries: 2},
			{Id: 101, TableFormat: uint32(sst.TableFormat), Size: 10, MinKey: []byte("a"), MaxKey: []byte("b"), NumEntries: 1},
			{Id: 100, TableFormat: uint32(sst.TableFormat), Size: 5, NumEntries: 0},
		},
	}
}

func sampleView() manifest.View {
	return manifest.View{
		Version:            3,
		SSTBasePath:        "root/pk_index/7/11/ch_0_7v0/sst",
		CheckpointTimeTick: 900,
		Tables: []manifest.TableRef{
			{ID: 102, TableFormat: sst.TableFormat, Size: 20, MinKey: []byte("c"), MaxKey: []byte("d"), NumEntries: 2},
			{ID: 101, TableFormat: sst.TableFormat, Size: 10, MinKey: []byte("a"), MaxKey: []byte("b"), NumEntries: 1},
			{ID: 100, TableFormat: sst.TableFormat, Size: 5},
		},
	}
}

func TestClientCommitFlush(t *testing.T) {
	svc := &fakeCoordClient{flushResp: sampleManifest()}
	c := newTestClient(svc)

	got, err := c.CommitFlush(context.Background(), manifest.FlushArgs{
		CollectionID:       7,
		IndexID:            11,
		VChannel:           "ch_0_7v0",
		PChannelTerm:       2,
		AddTables:          sampleView().Tables,
		CheckpointTimeTick: 900,
		ResetTimeTick:      800,
	})
	require.NoError(t, err)
	assert.Equal(t, sampleView(), got)

	want := &pkindexpb.CommitFlushRequest{
		CollectionId:       7,
		Vchannel:           "ch_0_7v0",
		PchannelTerm:       2,
		AddSsts:            sampleManifest().Ssts,
		CheckpointTimeTick: 900,
		IndexId:            11,
		ResetTimeTick:      800,
	}
	assert.True(t, proto.Equal(want, svc.flushReq), svc.flushReq.String())
}

func TestClientCommitFlushWithoutManifest(t *testing.T) {
	c := newTestClient(&fakeCoordClient{})
	_, err := c.CommitFlush(context.Background(), manifest.FlushArgs{VChannel: "v"})
	require.Error(t, err)
	assert.True(t, errors.Is(err, manifest.ErrRejected), err)
}

func TestClientErrorClassification(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		fenced   bool
		rejected bool
	}{
		{"fenced", merr.WrapErrChannelNotAvailable("v", "pchannel term is fenced"), true, false},
		{"service internal", merr.WrapErrServiceInternalMsg("x"), false, true},
		{"parameter invalid", merr.WrapErrParameterInvalidMsg("x"), false, true},
		{"data integrity", merr.WrapErrDataIntegrity(errors.New("x"), "y"), false, true},
		// permanent by merr's classification; the manifest service does not send it today
		{"io failed", merr.WrapErrIoFailed("k", errors.New("x")), false, true},
		{"unavailable", merr.WrapErrServiceUnavailableErr(errors.New("x"), "y"), false, false},
		{"not ready", merr.WrapErrServiceNotReadyMsg("x"), false, false},
		{"deadline", context.DeadlineExceeded, false, false},
		{"canceled", context.Canceled, false, false},
		{"plain", errors.New("x"), false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := &fakeCoordClient{flushErr: tc.err, lookupErr: tc.err}
			c := newTestClient(svc)

			_, err := c.CommitFlush(context.Background(), manifest.FlushArgs{VChannel: "v"})
			require.Error(t, err)
			assert.Equal(t, tc.fenced, errors.Is(err, manifest.ErrFenced), err)
			assert.Equal(t, tc.rejected, errors.Is(err, manifest.ErrRejected), err)
			assert.Equal(t, tc.fenced || tc.rejected, manifest.IsFatal(err), err)
			// the cause stays in the chain
			assert.True(t, errors.Is(err, tc.err), err)

			_, err = c.GetRecoveryManifests(context.Background(), []manifest.VChannelRef{{CollectionID: 7, IndexID: 11, VChannel: "v"}})
			require.Error(t, err)
			assert.Equal(t, tc.fenced, errors.Is(err, manifest.ErrFenced), err)
			assert.Equal(t, tc.rejected, errors.Is(err, manifest.ErrRejected), err)
			assert.Equal(t, tc.fenced || tc.rejected, manifest.IsFatal(err), err)
			assert.True(t, errors.Is(err, tc.err), err)
		})
	}
}

func TestClientGetRecoveryManifests(t *testing.T) {
	svc := &fakeCoordClient{lookupResp: []*pkindexpb.ManifestPointerEntry{
		{Status: merr.Success(), Found: true, Pointer: &pkindexpb.ManifestPointer{Version: 3, BodyKey: "root/pk_index/7/11/v0/manifest/3"}},
		{Status: merr.Success(), Found: false},
		{Status: merr.Status(merr.WrapErrServiceInternalMsg("x"))},
		{Status: merr.Status(merr.WrapErrServiceUnavailableMsg("x"))},
		{Status: merr.Success(), Found: true, Pointer: &pkindexpb.ManifestPointer{}},
		{Status: merr.Success(), Found: true},
		{Status: nil, Found: false},
	}}
	c := newTestClient(svc)
	refs := []manifest.VChannelRef{
		{CollectionID: 7, IndexID: 11, VChannel: "v0"},
		{CollectionID: 7, IndexID: 11, VChannel: "v1"},
		{CollectionID: 7, IndexID: 11, VChannel: "v2"},
		{CollectionID: 7, IndexID: 11, VChannel: "v3"},
		{CollectionID: 7, IndexID: 11, VChannel: "v4"},
		{CollectionID: 7, IndexID: 11, VChannel: "v5"},
		{CollectionID: 7, IndexID: 11, VChannel: "v6"},
	}

	got, err := c.GetRecoveryManifests(context.Background(), refs)
	require.NoError(t, err)
	require.Len(t, got, 7)
	assert.Equal(t, manifest.RecoveryManifest{Found: true, Version: 3, BodyKey: "root/pk_index/7/11/v0/manifest/3"}, got[0])
	assert.Equal(t, manifest.RecoveryManifest{}, got[1])
	assert.True(t, errors.Is(got[2].Err, manifest.ErrRejected), got[2].Err)
	require.Error(t, got[3].Err)
	assert.False(t, manifest.IsFatal(got[3].Err), got[3].Err)
	assert.True(t, errors.Is(got[4].Err, manifest.ErrRejected), got[4].Err)
	assert.True(t, errors.Is(got[5].Err, manifest.ErrRejected), got[5].Err)
	assert.Equal(t, manifest.RecoveryManifest{}, got[6])

	wantTargets := []*pkindexpb.ManifestTarget{
		{CollectionId: 7, IndexId: 11, Vchannel: "v0"},
		{CollectionId: 7, IndexId: 11, Vchannel: "v1"},
		{CollectionId: 7, IndexId: 11, Vchannel: "v2"},
		{CollectionId: 7, IndexId: 11, Vchannel: "v3"},
		{CollectionId: 7, IndexId: 11, Vchannel: "v4"},
		{CollectionId: 7, IndexId: 11, Vchannel: "v5"},
		{CollectionId: 7, IndexId: 11, Vchannel: "v6"},
	}
	require.Len(t, svc.lookupReq.GetTargets(), 7)
	for i := range wantTargets {
		assert.True(t, proto.Equal(wantTargets[i], svc.lookupReq.GetTargets()[i]))
	}
}

func TestClientGetRecoveryManifestsCountMismatch(t *testing.T) {
	svc := &fakeCoordClient{lookupResp: []*pkindexpb.ManifestPointerEntry{{Status: merr.Success()}}}
	c := newTestClient(svc)
	_, err := c.GetRecoveryManifests(context.Background(), []manifest.VChannelRef{{VChannel: "a"}, {VChannel: "b"}})
	require.Error(t, err)
	assert.True(t, errors.Is(err, manifest.ErrRejected), err)
}

func TestClientReclaimedSSTRequiresNewIDs(t *testing.T) {
	cause := merr.WrapErrChannelPKIndexSSTReclaimed("v")
	c := newTestClient(&fakeCoordClient{flushErr: cause})
	_, err := c.CommitFlush(context.Background(), manifest.FlushArgs{VChannel: "v"})
	require.True(t, errors.Is(err, manifest.ErrSSTReclaimed), "%v", err)
	require.ErrorIs(t, err, cause)
	require.False(t, manifest.IsFatal(err))
	require.False(t, errors.Is(err, manifest.ErrRejected))
}

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

package pkindex

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// fakeManifestService records the last request and answers what the test sets.
type fakeManifestService struct {
	flushReq   *pkindexpb.CommitFlushRequest
	flushResp  *pkindexpb.Manifest
	flushErr   error
	lookupReq  *pkindexpb.GetRecoveryManifestsRequest
	lookupResp []*pkindexpb.ManifestPointerEntry
	lookupErr  error
}

func (f *fakeManifestService) CommitFlush(ctx context.Context, req *pkindexpb.CommitFlushRequest) (*pkindexpb.Manifest, error) {
	f.flushReq = req
	return f.flushResp, f.flushErr
}

func (f *fakeManifestService) GetRecoveryManifests(ctx context.Context, req *pkindexpb.GetRecoveryManifestsRequest) ([]*pkindexpb.ManifestPointerEntry, error) {
	f.lookupReq = req
	return f.lookupResp, f.lookupErr
}

// fakeObjectReader serves objects from a map and marks a missing object as
// pkroot.ErrCorrupted, like objstore.Store.
type fakeObjectReader struct {
	objects map[string][]byte
	err     error
}

func (f *fakeObjectReader) ReadObject(ctx context.Context, key string) ([]byte, error) {
	if f.err != nil {
		return nil, f.err
	}
	b, ok := f.objects[key]
	if !ok {
		return nil, errors.Mark(errors.Newf("object %s does not exist", key), pkroot.ErrCorrupted)
	}
	return b, nil
}

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

func sampleView() index.ManifestView {
	return index.ManifestView{
		Version:            3,
		SSTBasePath:        "root/pk_index/7/11/ch_0_7v0/sst",
		CheckpointTimeTick: 900,
		Tables: []index.TableRef{
			{ID: 102, TableFormat: sst.TableFormat, Size: 20, MinKey: []byte("c"), MaxKey: []byte("d"), NumEntries: 2},
			{ID: 101, TableFormat: sst.TableFormat, Size: 10, MinKey: []byte("a"), MaxKey: []byte("b"), NumEntries: 1},
			{ID: 100, TableFormat: sst.TableFormat, Size: 5},
		},
	}
}

func TestManifestServiceCommitterCommitFlush(t *testing.T) {
	svc := &fakeManifestService{flushResp: sampleManifest()}
	c := NewManifestServiceCommitter(svc, &fakeObjectReader{})

	got, err := c.CommitFlush(context.Background(), index.FlushArgs{
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

func TestManifestServiceCommitterCommitFlushWithoutManifest(t *testing.T) {
	c := NewManifestServiceCommitter(&fakeManifestService{}, &fakeObjectReader{})
	_, err := c.CommitFlush(context.Background(), index.FlushArgs{VChannel: "v"})
	require.Error(t, err)
	assert.True(t, errors.Is(err, index.ErrRejected), err)
}

func TestManifestServiceCommitterErrors(t *testing.T) {
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
			svc := &fakeManifestService{flushErr: tc.err, lookupErr: tc.err}
			c := NewManifestServiceCommitter(svc, &fakeObjectReader{})

			_, err := c.CommitFlush(context.Background(), index.FlushArgs{VChannel: "v"})
			require.Error(t, err)
			assert.Equal(t, tc.fenced, errors.Is(err, index.ErrFenced), err)
			assert.Equal(t, tc.rejected, errors.Is(err, index.ErrRejected), err)
			assert.Equal(t, tc.fenced || tc.rejected, index.IsFatal(err), err)
			// the cause stays in the chain
			assert.True(t, errors.Is(err, tc.err), err)

			_, err = c.GetRecoveryManifests(context.Background(), []VChannelRef{{CollectionID: 7, IndexID: 11, VChannel: "v"}})
			require.Error(t, err)
			assert.Equal(t, tc.fenced, errors.Is(err, index.ErrFenced), err)
			assert.Equal(t, tc.rejected, errors.Is(err, index.ErrRejected), err)
			assert.Equal(t, tc.fenced || tc.rejected, index.IsFatal(err), err)
			assert.True(t, errors.Is(err, tc.err), err)
		})
	}
}

func TestManifestServiceCommitterGetRecoveryManifests(t *testing.T) {
	svc := &fakeManifestService{lookupResp: []*pkindexpb.ManifestPointerEntry{
		{Status: merr.Success(), Found: true, Pointer: &pkindexpb.ManifestPointer{Version: 3, BodyKey: "root/pk_index/7/11/v0/manifest/3"}},
		{Status: merr.Success(), Found: false},
		{Status: merr.Status(merr.WrapErrServiceInternalMsg("x"))},
		{Status: merr.Status(merr.WrapErrServiceUnavailableMsg("x"))},
		{Status: merr.Success(), Found: true, Pointer: &pkindexpb.ManifestPointer{}},
		{Status: merr.Success(), Found: true},
		{Status: nil, Found: false},
	}}
	c := NewManifestServiceCommitter(svc, &fakeObjectReader{})
	refs := []VChannelRef{
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
	assert.Equal(t, RecoveryManifest{Found: true, Version: 3, BodyKey: "root/pk_index/7/11/v0/manifest/3"}, got[0])
	assert.Equal(t, RecoveryManifest{}, got[1])
	assert.True(t, errors.Is(got[2].Err, index.ErrRejected), got[2].Err)
	require.Error(t, got[3].Err)
	assert.False(t, index.IsFatal(got[3].Err), got[3].Err)
	assert.True(t, errors.Is(got[4].Err, index.ErrRejected), got[4].Err)
	assert.True(t, errors.Is(got[5].Err, index.ErrRejected), got[5].Err)
	assert.Equal(t, RecoveryManifest{}, got[6])

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

func TestManifestServiceCommitterGetRecoveryManifestsCountMismatch(t *testing.T) {
	svc := &fakeManifestService{lookupResp: []*pkindexpb.ManifestPointerEntry{{Status: merr.Success()}}}
	c := NewManifestServiceCommitter(svc, &fakeObjectReader{})
	_, err := c.GetRecoveryManifests(context.Background(), []VChannelRef{{VChannel: "a"}, {VChannel: "b"}})
	require.Error(t, err)
	assert.True(t, errors.Is(err, index.ErrRejected), err)
}

func TestManifestServiceCommitterReadManifest(t *testing.T) {
	body, err := proto.Marshal(sampleManifest())
	require.NoError(t, err)
	reader := &fakeObjectReader{objects: map[string][]byte{
		"good": body,
		"bad":  {0xff, 0xff, 0xff},
	}}
	c := NewManifestServiceCommitter(&fakeManifestService{}, reader)

	got, err := c.ReadManifest(context.Background(), "good")
	require.NoError(t, err)
	assert.Equal(t, sampleView(), got)

	_, err = c.ReadManifest(context.Background(), "bad")
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkroot.ErrCorrupted), err)
	assert.True(t, index.IsFatal(err))

	_, err = c.ReadManifest(context.Background(), "missing")
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkroot.ErrCorrupted), err)

	reader.err = errors.Mark(errors.New("s3 down"), pkroot.ErrUnavailable)
	_, err = c.ReadManifest(context.Background(), "good")
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkroot.ErrUnavailable), err)
	assert.False(t, index.IsFatal(err))
}

func TestManifestServiceCommitterPanicsWithoutParts(t *testing.T) {
	assert.Panics(t, func() { NewManifestServiceCommitter(nil, &fakeObjectReader{}) })
	assert.Panics(t, func() { NewManifestServiceCommitter(&fakeManifestService{}, nil) })
}

func TestManifestServiceCommitterReadEmptyManifest(t *testing.T) {
	body, err := proto.Marshal(&pkindexpb.Manifest{Version: 1, SstBasePath: "p", CheckpointTimeTick: 5})
	require.NoError(t, err)
	c := NewManifestServiceCommitter(&fakeManifestService{}, &fakeObjectReader{objects: map[string][]byte{"empty": body}})

	got, err := c.ReadManifest(context.Background(), "empty")
	require.NoError(t, err)
	assert.Equal(t, int64(1), got.Version)
	assert.Equal(t, "p", got.SSTBasePath)
	assert.Equal(t, uint64(5), got.CheckpointTimeTick)
	assert.Empty(t, got.Tables)
}

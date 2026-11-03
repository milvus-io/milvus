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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
)

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

func sampleView() View {
	return View{
		Version:            3,
		SSTBasePath:        "root/pk_index/7/11/ch_0_7v0/sst",
		CheckpointTimeTick: 900,
		Tables: []TableRef{
			{ID: 102, TableFormat: sst.TableFormat, Size: 20, MinKey: []byte("c"), MaxKey: []byte("d"), NumEntries: 2},
			{ID: 101, TableFormat: sst.TableFormat, Size: 10, MinKey: []byte("a"), MaxKey: []byte("b"), NumEntries: 1},
			{ID: 100, TableFormat: sst.TableFormat, Size: 5},
		},
	}
}

func TestReadBody(t *testing.T) {
	body, err := proto.Marshal(sampleManifest())
	require.NoError(t, err)
	reader := &fakeObjectReader{objects: map[string][]byte{
		"good": body,
		"bad":  {0xff, 0xff, 0xff},
	}}
	ctx := context.Background()

	got, err := ReadBody(ctx, reader, "good")
	require.NoError(t, err)
	assert.Equal(t, sampleView(), got)

	_, err = ReadBody(ctx, reader, "bad")
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkroot.ErrCorrupted), err)
	assert.True(t, IsFatal(err))

	_, err = ReadBody(ctx, reader, "missing")
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkroot.ErrCorrupted), err)

	reader.err = errors.Mark(errors.New("s3 down"), pkroot.ErrUnavailable)
	_, err = ReadBody(ctx, reader, "good")
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkroot.ErrUnavailable), err)
	assert.False(t, IsFatal(err))
}

func TestReadBodyEmpty(t *testing.T) {
	body, err := proto.Marshal(&pkindexpb.Manifest{Version: 1, SstBasePath: "p", CheckpointTimeTick: 5})
	require.NoError(t, err)
	got, err := ReadBody(context.Background(), &fakeObjectReader{objects: map[string][]byte{"empty": body}}, "empty")
	require.NoError(t, err)
	assert.Equal(t, int64(1), got.Version)
	assert.Equal(t, "p", got.SSTBasePath)
	assert.Equal(t, uint64(5), got.CheckpointTimeTick)
	assert.Empty(t, got.Tables)
}

func TestFakeBodyRoundTrip(t *testing.T) {
	v := sampleView()
	got, err := ReadBody(context.Background(), &fakeObjectReader{objects: map[string][]byte{"k": FakeBody(v)}}, "k")
	require.NoError(t, err)
	assert.Equal(t, v, got)
}

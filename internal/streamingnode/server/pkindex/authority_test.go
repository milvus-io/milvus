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

	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/codec"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
)

func newTestAuthority(t *testing.T) Authority {
	t.Helper()
	eng := NewMemoryEngine()
	a, err := NewAuthority("v1", eng)
	require.NoError(t, err)
	t.Cleanup(func() { _ = eng.Close() })
	return a
}

func TestAuthorityIsReadyAfterNew(t *testing.T) {
	a := newTestAuthority(t)
	select {
	case <-a.Ready():
	default:
		t.Fatal("authority should be ready once the engine is created")
	}
}

func TestAuthorityGetAndWrite(t *testing.T) {
	ctx := context.Background()
	a := newTestAuthority(t)

	entry, err := a.Get(ctx, Int64PK(1))
	require.NoError(t, err)
	require.Nil(t, entry)

	b := a.NewBatch()
	b.Put(Int64PK(1), Entry{SegmentID: 100})
	b.Put(Int64PK(2), Entry{SegmentID: 200})
	require.Equal(t, 2, b.Len())

	// nothing is visible before Write.
	entry, err = a.Get(ctx, Int64PK(1))
	require.NoError(t, err)
	require.Nil(t, entry)

	require.NoError(t, a.Write(ctx, b))
	entries, err := a.MultiGet(ctx, []PK{Int64PK(2), Int64PK(3), Int64PK(1)})
	require.NoError(t, err)
	require.Equal(t, []*Entry{{SegmentID: 200}, nil, {SegmentID: 100}}, entries)
}

func TestAuthorityOverwriteAndDelete(t *testing.T) {
	ctx := context.Background()
	a := newTestAuthority(t)

	b := a.NewBatch()
	b.Put(VarCharPK("a"), Entry{SegmentID: 1})
	require.NoError(t, a.Write(ctx, b))

	b = a.NewBatch()
	b.Put(VarCharPK("a"), Entry{SegmentID: 2})
	b.Delete(VarCharPK("b")) // deleting an absent key is fine
	require.NoError(t, a.Write(ctx, b))
	entry, err := a.Get(ctx, VarCharPK("a"))
	require.NoError(t, err)
	require.Equal(t, &Entry{SegmentID: 2}, entry)

	b = a.NewBatch()
	b.Delete(VarCharPK("a"))
	require.NoError(t, a.Write(ctx, b))
	entry, err = a.Get(ctx, VarCharPK("a"))
	require.NoError(t, err)
	require.Nil(t, entry)
}

func TestAuthorityEmptyBatchAndEmptyMultiGet(t *testing.T) {
	ctx := context.Background()
	a := newTestAuthority(t)
	require.NoError(t, a.Write(ctx, a.NewBatch()))
	entries, err := a.MultiGet(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestAuthorityRejectsCorruptValue(t *testing.T) {
	ctx := context.Background()
	eng := NewMemoryEngine()
	require.NoError(t, eng.Write(ctx, []engine.Mutation{{Key: Int64PK(1).Encode(), Value: []byte{1, 2, 3}}}))
	a := newAuthority("v1", eng)

	_, err := a.Get(ctx, Int64PK(1))
	require.Error(t, err)
	require.True(t, errors.Is(err, pkindex.ErrCorrupted))
}

func TestAuthorityAfterEngineClose(t *testing.T) {
	ctx := context.Background()
	eng := NewMemoryEngine()
	a, err := NewAuthority("v1", eng)
	require.NoError(t, err)
	require.NoError(t, eng.Close())
	_, err = a.Get(ctx, Int64PK(1))
	require.Error(t, err)
}

// fakeLengthEngine returns a MultiGet result of a fixed length, regardless of
// the number of keys asked for.
type fakeLengthEngine struct {
	engine.RW
	returnLen int
}

func (e *fakeLengthEngine) MultiGet(_ context.Context, _ [][]byte) ([][]byte, error) {
	return make([][]byte, e.returnLen), nil
}

func TestAuthorityMultiGetRejectsWrongLength(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name      string
		returnLen int
	}{
		{"too few values", 1},
		{"too many values", 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := newAuthority("v1", &fakeLengthEngine{returnLen: tc.returnLen})
			_, err := a.MultiGet(ctx, []PK{Int64PK(1), Int64PK(2)})
			require.Error(t, err)
		})
	}
}

func TestNewTakesEngine(t *testing.T) {
	a, err := NewAuthority("v", NewMemoryEngine())
	require.NoError(t, err)
	b := a.NewBatch()
	b.Put(Int64PK(7), Entry{SegmentID: 3})
	require.NoError(t, a.Write(context.Background(), b))
	got, err := a.MultiGet(context.Background(), []PK{Int64PK(7)})
	require.NoError(t, err)
	assert.EqualValues(t, 3, got[0].SegmentID)
}

func TestNewRejectsNilEngine(t *testing.T) {
	_, err := NewAuthority("v", nil)
	assert.Error(t, err)
}

func TestKeyAndValueUseCodec(t *testing.T) {
	assert.Equal(t, codec.EncodeInt64PK(-5), Int64PK(-5).Encode())
	assert.Equal(t, codec.EncodeVarcharPK("a"), VarCharPK("a").Encode())
	assert.Equal(t, codec.EncodePKEntry(codec.PKEntry{SegmentID: 9}), Entry{SegmentID: 9}.encode())
}

func TestCorruptEntryIsMarked(t *testing.T) {
	_, err := decodeEntry([]byte{0x7f})
	assert.True(t, errors.Is(err, pkindex.ErrCorrupted))
}

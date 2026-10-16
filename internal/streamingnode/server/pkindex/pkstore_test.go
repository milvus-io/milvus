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

	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/codec"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
)

func newTestPKStore(t *testing.T) pkStore {
	t.Helper()
	eng := NewMemoryEngine()
	t.Cleanup(func() { _ = eng.Close() })
	return pkStore{eng: eng}
}

func TestPKStoreGetAndWrite(t *testing.T) {
	ctx := context.Background()
	s := newTestPKStore(t)

	entry, err := s.get(ctx, Int64PK(1))
	require.NoError(t, err)
	require.Nil(t, entry)

	b := &entryBatch{}
	b.put(Int64PK(1), Entry{SegmentID: 100})
	b.put(Int64PK(2), Entry{SegmentID: 200})
	require.Equal(t, 2, b.len())

	// nothing is visible before Write.
	entry, err = s.get(ctx, Int64PK(1))
	require.NoError(t, err)
	require.Nil(t, entry)

	require.NoError(t, s.write(ctx, b))
	entries, err := s.multiGet(ctx, []PK{Int64PK(2), Int64PK(3), Int64PK(1)})
	require.NoError(t, err)
	require.Equal(t, []*Entry{{SegmentID: 200}, nil, {SegmentID: 100}}, entries)
}

func TestPKStoreOverwriteAndDelete(t *testing.T) {
	ctx := context.Background()
	s := newTestPKStore(t)

	b := &entryBatch{}
	b.put(VarCharPK("a"), Entry{SegmentID: 1})
	require.NoError(t, s.write(ctx, b))

	b = &entryBatch{}
	b.put(VarCharPK("a"), Entry{SegmentID: 2})
	b.delete(VarCharPK("b")) // deleting an absent key is fine
	require.NoError(t, s.write(ctx, b))
	entry, err := s.get(ctx, VarCharPK("a"))
	require.NoError(t, err)
	require.Equal(t, &Entry{SegmentID: 2}, entry)

	b = &entryBatch{}
	b.delete(VarCharPK("a"))
	require.NoError(t, s.write(ctx, b))
	entry, err = s.get(ctx, VarCharPK("a"))
	require.NoError(t, err)
	require.Nil(t, entry)
}

func TestPKStoreEmptyBatchAndEmptyMultiGet(t *testing.T) {
	ctx := context.Background()
	s := newTestPKStore(t)
	require.NoError(t, s.write(ctx, &entryBatch{}))
	entries, err := s.multiGet(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestPKStoreRejectsCorruptValue(t *testing.T) {
	ctx := context.Background()
	eng := NewMemoryEngine()
	require.NoError(t, eng.Write(ctx, []engine.Mutation{{Key: Int64PK(1).Encode(), Value: []byte{1, 2, 3}}}))
	s := pkStore{eng: eng}

	_, err := s.get(ctx, Int64PK(1))
	require.Error(t, err)
	require.True(t, errors.Is(err, pkroot.ErrCorrupted))
}

func TestPKStoreAfterEngineClose(t *testing.T) {
	ctx := context.Background()
	eng := NewMemoryEngine()
	s := pkStore{eng: eng}
	require.NoError(t, eng.Close())
	_, err := s.get(ctx, Int64PK(1))
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

func TestPKStoreMultiGetRejectsWrongLength(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name      string
		returnLen int
	}{
		{"too few values", 1},
		{"too many values", 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := pkStore{eng: &fakeLengthEngine{returnLen: tc.returnLen}}
			_, err := s.multiGet(ctx, []PK{Int64PK(1), Int64PK(2)})
			require.Error(t, err)
		})
	}
}

func TestKeyAndValueUseCodec(t *testing.T) {
	assert.Equal(t, codec.EncodeInt64PK(-5), Int64PK(-5).Encode())
	assert.Equal(t, codec.EncodeVarcharPK("a"), VarCharPK("a").Encode())
	assert.Equal(t, codec.EncodePKEntry(codec.PKEntry{SegmentID: 9}), Entry{SegmentID: 9}.encode())
}

func TestCorruptEntryIsMarked(t *testing.T) {
	_, err := decodeEntry([]byte{0x7f})
	assert.True(t, errors.Is(err, pkroot.ErrCorrupted))
}

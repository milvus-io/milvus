// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package authority

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

func lookupOne(t *testing.T, eng engine.RW, pk PK) *codec.PKEntry {
	t.Helper()
	entries, err := lookup(context.Background(), eng, []PK{pk})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	return entries[0]
}

func TestLookupAlignsWithTheKeys(t *testing.T) {
	ctx := context.Background()
	eng := NewMemoryEngine()
	require.Nil(t, lookupOne(t, eng, Int64PK(1)))

	require.NoError(t, writeRecords(ctx, eng, 100, int64PKs(1), nil))
	require.NoError(t, writeRecords(ctx, eng, 200, int64PKs(2), nil))
	entries, err := lookup(ctx, eng, []PK{Int64PK(2), Int64PK(3), Int64PK(1)})
	require.NoError(t, err)
	require.Equal(t, []*codec.PKEntry{{SegmentID: 200}, nil, {SegmentID: 100}}, entries)

	// a tombstone makes the key absent
	require.NoError(t, writeRecords(ctx, eng, 0, nil, int64PKs(1)))
	require.Nil(t, lookupOne(t, eng, Int64PK(1)))

	entries, err = lookup(ctx, eng, nil)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestLookupRejectsACorruptValue(t *testing.T) {
	ctx := context.Background()
	eng := NewMemoryEngine()
	require.NoError(t, eng.Write(ctx, []engine.Mutation{{Key: Int64PK(1).Encode(), Value: []byte{1, 2, 3}}}))

	_, err := lookup(ctx, eng, []PK{Int64PK(1)})
	require.Error(t, err)
	require.True(t, errors.Is(err, pkroot.ErrCorrupted))
}

func TestLookupAfterEngineClose(t *testing.T) {
	eng := NewMemoryEngine()
	require.NoError(t, eng.Close())
	_, err := lookup(context.Background(), eng, []PK{Int64PK(1)})
	require.True(t, errors.Is(err, engine.ErrClosed))
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

func TestLookupRejectsAWrongLength(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name      string
		returnLen int
	}{
		{"too few values", 1},
		{"too many values", 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := lookup(ctx, &fakeLengthEngine{returnLen: tc.returnLen}, []PK{Int64PK(1), Int64PK(2)})
			require.Error(t, err)
			require.True(t, errors.Is(err, errValueCountMismatch))
		})
	}
}

func TestKeyUsesTheCodec(t *testing.T) {
	assert.Equal(t, codec.EncodeInt64PK(-5), Int64PK(-5).Encode())
	assert.Equal(t, codec.EncodeVarcharPK("a"), VarCharPK("a").Encode())
}

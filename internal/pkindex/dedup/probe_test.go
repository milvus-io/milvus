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

package dedup

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/pkindex/engine"
)

func TestProbeKeepsTheOrderOfTheKeys(t *testing.T) {
	ctx := context.Background()
	d := New("v1", NewMemoryEngine(), Config{}).(*decider)
	defer d.Close()

	present, err := d.probe(ctx, []PK{Int64PK(1)})
	require.NoError(t, err)
	require.Empty(t, present)

	require.NoError(t, writeRecords(ctx, d.eng, 100, int64PKs(1), nil))
	require.NoError(t, writeRecords(ctx, d.eng, 200, int64PKs(2), nil))
	present, err = d.probe(ctx, []PK{Int64PK(2), Int64PK(3), Int64PK(1)})
	require.NoError(t, err)
	require.Equal(t, []PK{Int64PK(2), Int64PK(1)}, present)

	// a tombstone makes the key absent
	require.NoError(t, writeRecords(ctx, d.eng, 0, nil, int64PKs(1)))
	present, err = d.probe(ctx, []PK{Int64PK(1)})
	require.NoError(t, err)
	require.Empty(t, present)

	present, err = d.probe(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, present)
}

func TestProbeDoesNotDecodeTheValue(t *testing.T) {
	ctx := context.Background()
	d := New("v1", NewMemoryEngine(), Config{}).(*decider)
	defer d.Close()
	require.NoError(t, d.eng.Write(ctx, []engine.Mutation{{Key: Int64PK(1).Encode(), Value: []byte{1, 2, 3}}}))

	present, err := d.probe(ctx, []PK{Int64PK(1)})
	require.NoError(t, err)
	require.Equal(t, []PK{Int64PK(1)}, present)
}

func TestProbeAfterEngineClose(t *testing.T) {
	eng := NewMemoryEngine()
	d := New("v1", eng, Config{}).(*decider)
	defer d.Close()
	require.NoError(t, eng.Close())
	_, err := d.probe(context.Background(), []PK{Int64PK(1)})
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

func TestProbeRejectsAWrongLength(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name      string
		returnLen int
	}{
		{"too few values", 1},
		{"too many values", 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := New("v1", &fakeLengthEngine{returnLen: tc.returnLen}, Config{}).(*decider)
			defer d.Close()
			_, err := d.probe(ctx, []PK{Int64PK(1), Int64PK(2)})
			require.Error(t, err)
			require.True(t, errors.Is(err, errValueCountMismatch))
		})
	}
}

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

package engine

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/pkindex/core/codec"
	"github.com/milvus-io/milvus/internal/pkindex/core/sst"
)

func newTestEngine(t *testing.T, opts ...func(*Config)) *Engine {
	shared := NewSharedResources(SharedConfig{CacheBytes: 32 << 20})
	t.Cleanup(shared.Release)
	var nextID atomic.Int64
	nextID.Store(1000)
	cfg := Config{
		Dir:      t.TempDir(),
		VChannel: "test-vchannel-v0",
		Shared:   shared,
		AllocID:  func(context.Context) (int64, error) { return nextID.Add(1), nil },
	}
	for _, o := range opts {
		o(&cfg)
	}
	e, err := Open(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { e.Close() })
	return e
}

func pk(i int64) []byte { return codec.EncodeInt64PK(i) }

func entry(seg int64) []byte { return codec.EncodePKEntry(codec.PKEntry{SegmentID: seg}) }

func put(key []byte, seg int64) Mutation { return Mutation{Key: key, Value: entry(seg)} }

func del(key []byte) Mutation { return Mutation{Key: key, Delete: true} }

func getOne(t *testing.T, e *Engine, key []byte) []byte {
	vs, err := e.MultiGet(context.Background(), [][]byte{key})
	require.NoError(t, err)
	require.Len(t, vs, 1)
	return vs[0]
}

func requireSegment(t *testing.T, e *Engine, key []byte, want int64) {
	t.Helper()
	v := getOne(t, e, key)
	require.NotNil(t, v)
	got, err := codec.DecodePKEntry(v)
	require.NoError(t, err)
	require.Equal(t, want, got.SegmentID)
}

// buildCommitted writes one SST from (pk, value) pairs into dir and returns it
// as a table ready to install.
func buildCommitted(t *testing.T, dir string, id sst.ID, pairs map[int64][]byte) CommittedTable {
	w, err := sst.NewWriter(dir, id)
	require.NoError(t, err)
	keys := make([]int64, 0, len(pairs))
	for k := range pairs {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] }) // int64 order == encoded order
	for _, k := range keys {
		require.NoError(t, w.Add(pk(k), pairs[k]))
	}
	info, err := w.Close()
	require.NoError(t, err)
	return CommittedTable{Info: info, Path: filepath.Join(dir, sst.FileName(id))}
}

// publishFlushed stands in for the upload plus manifest commit: it copies each
// flushed table somewhere the engine does not own, because the engine deletes
// its staging when the generation retires.
func publishFlushed(t *testing.T, flushed []FlushedTable, dstDir string) []CommittedTable {
	t.Helper()
	out := make([]CommittedTable, 0, len(flushed))
	for _, f := range flushed {
		// a link, not a copy: the engine drops its staging when the generation
		// retires, so a publisher has to own the copy it serves from
		dst := filepath.Join(dstDir, sst.FileName(f.Info.ID))
		if _, err := os.Stat(dst); os.IsNotExist(err) {
			require.NoError(t, os.Link(f.Path, dst))
		}
		out = append(out, CommittedTable{Info: f.Info, Path: dst})
	}
	return out
}

// commitTables runs the caller-side half of one handover: publish the flushed
// tables and install them ahead of whatever was committed before.
func commitTables(t *testing.T, e *Engine, dstDir string, flushed []FlushedTable, existing []CommittedTable, retire ...Generation) []CommittedTable {
	t.Helper()
	tables := append(publishFlushed(t, flushed, dstDir), existing...) // newest first
	require.NoError(t, e.InstallCommitted(context.Background(), tables, retire...))
	return tables
}

// requireReadWriteUnblocked asserts Apply and Probe finish while some other
// operation is parked inside its disk IO.
func requireReadWriteUnblocked(t *testing.T, e *Engine) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx := context.Background()
		assert.NoError(t, e.Write(ctx, []Mutation{{Key: pk(7), Value: entry(70)}}))
		_, err := e.MultiGet(ctx, [][]byte{pk(7)})
		assert.NoError(t, err)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("MultiGet/Write blocked behind disk IO of a structural operation")
	}
}

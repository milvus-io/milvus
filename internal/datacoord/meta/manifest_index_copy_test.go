// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package meta

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type migrationCopyManager struct {
	storage.ChunkManager
	copy func(context.Context, string, string) error
}

func (m migrationCopyManager) Copy(ctx context.Context, src, dst string) error {
	return m.copy(ctx, src, dst)
}

func TestManifestIndexMigrationCopyConcurrencyAndCancellation(t *testing.T) {
	paramtable.Init()
	concurrency := Params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.SwapTempValue("2")
	connections := Params.MinioCfg.MaxConnections.SwapTempValue("2")
	t.Cleanup(func() {
		Params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.SwapTempValue(concurrency)
		Params.MinioCfg.MaxConnections.SwapTempValue(connections)
	})
	m := &Meta{}
	entered := make(chan struct{}, 8)
	release := make(chan struct{})
	var running, peak atomic.Int32
	m.ChunkManager = migrationCopyManager{ChunkManager: storage.NewLocalChunkManager(objectstorage.RootPath("files")), copy: func(context.Context, string, string) error {
		n := running.Add(1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		entered <- struct{}{}
		<-release // accepted operations must be drained even after cancellation
		running.Add(-1)
		return nil
	}}
	source := &model.SegmentIndex{BuildID: 1, IndexVersion: 1, IndexFileKeys: []string{"a", "b", "c"}}
	dest := model.CloneSegmentIndex(source)
	dest.IndexStorePathVersion = indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	waiting := make(chan error, 1)
	go func() {
		done <- m.copyMigratedIndexFiles(ctx, &SegmentInfo{}, []*StagedSegmentIndexMutation{{Source: source, Record: dest}})
	}()
	for range 2 {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			close(release)
			t.Fatal("file copies did not overlap")
		}
	}
	go func() {
		waiting <- m.copyMigratedIndexFiles(ctx, &SegmentInfo{}, []*StagedSegmentIndexMutation{{Source: source, Record: dest}})
	}()
	select {
	case <-entered:
		close(release)
		t.Fatal("second segment exceeded the global copy budget")
	case <-time.After(30 * time.Millisecond):
	}
	cancel()
	require.ErrorIs(t, <-waiting, context.Canceled)
	select {
	case <-done:
		close(release)
		t.Fatal("returned before accepted copies drained")
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	require.ErrorIs(t, <-done, context.Canceled)
	assert.Equal(t, int32(2), peak.Load(), "the connection budget is shared across segments")
	assert.Zero(t, running.Load())
}

func TestManifestIndexMigrationRejectsLocalCopy(t *testing.T) {
	m := &Meta{ChunkManager: storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))}
	source := &model.SegmentIndex{IndexFileKeys: []string{"a"}}
	dest := model.CloneSegmentIndex(source)
	dest.IndexStorePathVersion = indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED
	require.ErrorIs(t, m.copyMigratedIndexFiles(context.Background(), &SegmentInfo{}, []*StagedSegmentIndexMutation{{Source: source, Record: dest}}), merr.ErrServiceUnimplemented)
}

func TestManifestIndexMigrationRejectsInvalidSource(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version indexpb.IndexStorePathVersion
		key     string
	}{
		{"unknown layout", 99, "a"}, {"escaping file", 0, "../a"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &Meta{ChunkManager: migrationCopyManager{ChunkManager: storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir())), copy: func(context.Context, string, string) error { t.Fatal("invalid source reached copy"); return nil }}}
			source := &model.SegmentIndex{IndexStorePathVersion: tc.version, IndexFileKeys: []string{tc.key}}
			dest := model.CloneSegmentIndex(source)
			dest.IndexStorePathVersion = indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED
			require.ErrorIs(t, m.copyMigratedIndexFiles(context.Background(), &SegmentInfo{}, []*StagedSegmentIndexMutation{{Source: source, Record: dest}}), merr.ErrDataIntegrity)
		})
	}
}

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

package datacoord

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	metastorekv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type migrationCopyManager struct {
	storage.ChunkManager
	copy func(context.Context, string, string) error
}

func (m migrationCopyManager) Copy(ctx context.Context, src, dst string) error {
	return m.copy(ctx, src, dst)
}

// Use the actual RemoteChunkManager and MinIO SDK against a CopyObject endpoint.
// The test server alone accesses fixture bytes. GET/download and ordinary
// PUT/upload requests are rejected. Manifests still exercise the local real FFI.
func migrationServerCopyFixture(t *testing.T, root string) storage.ChunkManager {
	t.Helper()
	local := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	fixture, err := os.OpenRoot(root)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, fixture.Close()) })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dst := strings.TrimPrefix(r.URL.Path, "/fixture/")
		if !strings.HasPrefix(dst, root+"/") {
			http.Error(w, "outside fixture", 400)
			return
		}
		dst = strings.TrimPrefix(dst, root+"/")
		if r.Method == http.MethodHead {
			info, err := fixture.Stat(dst)
			if err != nil {
				http.Error(w, "missing", 404)
				return
			}
			w.Header().Set("Content-Length", fmt.Sprint(info.Size()))
			w.Header().Set("ETag", `"fixture"`)
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			return
		}
		if r.Method != http.MethodPut || r.Header.Get("X-Amz-Copy-Source") == "" {
			t.Errorf("artifact migration used non-copy request: %s %s", r.Method, r.URL.Path)
			http.Error(w, "only CopyObject is supported", 400)
			return
		}
		src, err := url.PathUnescape(strings.TrimPrefix(strings.TrimPrefix(r.Header.Get("X-Amz-Copy-Source"), "/"), "fixture/"))
		if err != nil || !strings.HasPrefix(src, root+"/") {
			http.Error(w, "invalid source", 400)
			return
		}
		data, err := fixture.ReadFile(strings.TrimPrefix(src, root+"/"))
		if err != nil {
			http.Error(w, "missing source", 404)
			return
		}
		if err := fixture.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
			t.Error(err)
			http.Error(w, "mkdir", 500)
			return
		}
		if err := fixture.WriteFile(dst, data, 0o600); err != nil {
			t.Error(err)
			http.Error(w, "write", 500)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = fmt.Fprint(w, `<CopyObjectResult><LastModified>2026-10-19T00:00:00.000Z</LastModified><ETag>"fixture"</ETag></CopyObjectResult>`)
	}))
	t.Cleanup(server.Close)
	client, err := minio.New(strings.TrimPrefix(server.URL, "http://"), &minio.Options{Region: "us-east-1", BucketLookup: minio.BucketLookupPath})
	require.NoError(t, err)
	remote := storage.NewRemoteChunkManagerForTesting(client, "fixture", root)
	return migrationCopyManager{ChunkManager: local, copy: remote.Copy}
}

func TestManifestIndexMigrationCopyFailureAndRetry(t *testing.T) {
	for _, rollback := range []bool{false, true} {
		t.Run(fmt.Sprint(rollback), func(t *testing.T) {
			withSegmentIndexManifestWrites(t, false)
			newFakeManifestStore(t)
			catalog := metastorekv.NewCatalog(NewMetaMemoryKV(), "", "")
			m := bootMetaForRestart(t, catalog, restartCollID)
			seedLegacyBackfillRecord(t, m, restartSegID, restartBuildID)
			ctx := context.Background()
			forward := newManifestIndexBackfillInspector(ctx, m)
			if rollback {
				require.NoError(t, forward.backfillIndexes(ctx, restartSegID, restartBuildID))
			}
			before := m.GetSegment(ctx, restartSegID).GetManifestPath()
			source, _ := m.indexMeta.GetIndexJob(restartBuildID)
			sourceVersion := source.IndexStorePathVersion
			var fail atomic.Bool
			fail.Store(true)
			var copied atomic.Int32
			firstCopied := make(chan struct{})
			m.chunkManager = migrationCopyManager{ChunkManager: m.chunkManager, copy: func(ctx context.Context, src, dst string) error {
				if fail.Load() {
					if strings.HasSuffix(src, "/0") {
						copied.Add(1)
						close(firstCopied)
						return nil
					}
					<-firstCopied
					return merr.ErrIoTooManyRequests
				}
				if rollback {
					assert.Contains(t, src, "/_index/")
					assert.Contains(t, dst, "/index_v1/")
				} else {
					assert.Contains(t, src, "/index_v1/")
					assert.Contains(t, dst, "/_index/")
				}
				copied.Add(1)
				return nil
			}}
			migrate := func() error {
				if rollback {
					_, err := m.rollbackSegmentIndexes(ctx, newGCManifestReadIO(t), restartSegID, restartBuildID)
					return err
				}
				return forward.backfillIndexes(ctx, restartSegID, restartBuildID)
			}
			require.ErrorIs(t, migrate(), merr.ErrIoTooManyRequests)
			assert.Equal(t, before, m.GetSegment(ctx, restartSegID).GetManifestPath())
			current, _ := m.indexMeta.GetIndexJob(restartBuildID)
			assert.Equal(t, source, current)
			rows, err := catalog.ListSegmentIndexes(ctx, restartCollID)
			require.NoError(t, err)
			if rollback {
				require.Empty(t, rows)
			} else {
				require.Len(t, rows, 1)
			}
			assert.Equal(t, int32(1), copied.Load(), "one destination was copied before the failure")
			copied.Store(0)
			fail.Store(false)
			require.NoError(t, migrate())
			assert.Equal(t, int32(len(source.IndexFileKeys)), copied.Load())
			current, _ = m.indexMeta.GetIndexJob(restartBuildID)
			targetVersion := indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED
			if rollback {
				targetVersion = indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED
			}
			assert.Equal(t, targetVersion, current.IndexStorePathVersion)
			assert.Equal(t, sourceVersion, source.IndexStorePathVersion, "existing reader snapshot is immutable")
		})
	}
}

func TestManifestIndexMigrationCopyConcurrencyAndCancellation(t *testing.T) {
	setRollbackTestParam(t, &Params.DataCoordCfg.SegmentIndexManifestLoadConcurrency, "2")
	setRollbackTestParam(t, &Params.MinioCfg.MaxConnections, "2")
	m := &meta{}
	entered := make(chan struct{}, 8)
	release := make(chan struct{})
	var running, peak atomic.Int32
	m.chunkManager = migrationCopyManager{ChunkManager: storage.NewLocalChunkManager(objectstorage.RootPath("files")), copy: func(context.Context, string, string) error {
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
		done <- m.copyMigratedIndexFiles(ctx, &SegmentInfo{}, []*stagedSegmentIndexMutation{{source: source, record: dest}})
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
		waiting <- m.copyMigratedIndexFiles(ctx, &SegmentInfo{}, []*stagedSegmentIndexMutation{{source: source, record: dest}})
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
	m := &meta{chunkManager: storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))}
	source := &model.SegmentIndex{IndexFileKeys: []string{"a"}}
	dest := model.CloneSegmentIndex(source)
	dest.IndexStorePathVersion = indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED
	require.ErrorIs(t, m.copyMigratedIndexFiles(context.Background(), &SegmentInfo{}, []*stagedSegmentIndexMutation{{source: source, record: dest}}), merr.ErrServiceUnimplemented)
}

func TestManifestIndexMigrationRetainsLayoutAliases(t *testing.T) {
	m, _, _, _ := rollbackFixture(t)
	ctx := context.Background()
	segment := m.GetSegment(ctx, restartSegID)
	record, _ := m.indexMeta.GetIndexJob(restartBuildID)
	gc := newGarbageCollector(m, nil, GcOption{cli: m.chunkManager})
	defer gc.close()
	canReclaim := gc.newIndexFileReclaimChecker(ctx)
	var files []string
	for _, version := range []indexpb.IndexStorePathVersion{0, 1, 2} {
		candidate := model.CloneSegmentIndex(record)
		candidate.IndexStorePathVersion = version
		files = append(files, migrationIndexPaths(m.chunkManager.RootPath(), segment.GetManifestPath(), candidate)...)
	}
	for _, file := range files {
		require.False(t, canReclaim(file, restartCollID, restartSegID, restartBuildID), file)
	}
	_, err := m.rollbackSegmentIndexes(ctx, newGCManifestReadIO(t), restartSegID, restartBuildID)
	require.NoError(t, err)
	// Also exercise the decision cached before the placement change.
	for _, check := range []func(string, int64, int64, int64) bool{canReclaim, gc.newIndexFileReclaimChecker(ctx)} {
		for _, file := range files {
			require.False(t, check(file, restartCollID, restartSegID, restartBuildID), file)
		}
	}
	require.True(t, gc.newIndexFileReclaimChecker(ctx)(files[0]+"-unrecorded", restartCollID, restartSegID, restartBuildID))
	require.NoError(t, m.indexMeta.RemoveSegmentIndex(ctx, restartBuildID))
	for _, file := range files {
		require.True(t, gc.newIndexFileReclaimChecker(ctx)(file, restartCollID, restartSegID, restartBuildID), file)
	}
}

func TestManifestIndexMigrationWaitsForGCRetirement(t *testing.T) {
	m, _, _, _ := rollbackFixture(t)
	ctx := context.Background()
	record, _ := m.indexMeta.GetIndexJob(restartBuildID)
	gc := newGarbageCollector(m, nil, GcOption{cli: m.chunkManager})
	defer gc.close()
	entered, release := make(chan struct{}), make(chan struct{})
	patch := mockey.Mock((*garbageCollector).removeObjectFiles).To(func(*garbageCollector, context.Context, map[string]struct{}) error {
		close(entered)
		<-release
		return nil
	}).Build()
	defer patch.UnPatch()
	retired := make(chan struct{})
	go func() {
		defer close(retired)
		gc.recycleManifestSegmentIndexes(ctx, []segmentIndexGCItem{{segIdx: record, files: gc.getRetainedIndexFilesOfIndex(record)}})
	}()
	<-entered
	restored := make(chan int, 1)
	restoreErr := make(chan error, 1)
	io := newGCManifestReadIO(t)
	go func() {
		n, err := m.rollbackSegmentIndexes(ctx, io, restartSegID, restartBuildID)
		restored <- n
		restoreErr <- err
	}()
	select {
	case <-restored:
		close(release)
		<-retired
		t.Fatal("rollback overtook source-file retirement")
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	<-retired
	assert.Zero(t, <-restored)
	require.NoError(t, <-restoreErr)
	_, present := m.indexMeta.GetIndexJob(restartBuildID)
	assert.False(t, present, "rollback must not resurrect a retired build")
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
			m := &meta{chunkManager: migrationCopyManager{ChunkManager: storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir())), copy: func(context.Context, string, string) error { t.Fatal("invalid source reached copy"); return nil }}}
			source := &model.SegmentIndex{IndexStorePathVersion: tc.version, IndexFileKeys: []string{tc.key}}
			dest := model.CloneSegmentIndex(source)
			dest.IndexStorePathVersion = indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED
			require.ErrorIs(t, m.copyMigratedIndexFiles(context.Background(), &SegmentInfo{}, []*stagedSegmentIndexMutation{{source: source, record: dest}}), merr.ErrDataIntegrity)
		})
	}
}

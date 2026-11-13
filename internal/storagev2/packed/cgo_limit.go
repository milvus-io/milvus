// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package packed

import (
	"context"
	"sync"

	"golang.org/x/sync/semaphore"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// cgo call entry points in this package
//
// A goroutine that enters a cgo call is pinned to an OS thread for the whole
// call, so the number of OS threads the process holds tracks the number of
// goroutines simultaneously blocked inside C. Storage v3 routes every manifest
// add-file / read / commit through milvus-storage FFI; the per-context
// ManifestIOContext pools bound only their own callers, and the synchronous
// entry points had no process-wide ceiling at all. Under load (segment change
// commit bursts, index/stats publication, L0 compaction, startup manifest
// reload, GC, copy-segment and backfill reads) hundreds of callers across many
// contexts blocked in cgo at once, growing process threads to 1500-2000 and
// inflating RSS by gigabytes. Every *blocking* cgo call below therefore
// acquires a shared process-wide slot (manifestFFI) before entering C.
//
// Blocking manifest transaction I/O (wrapped):
//   - openTransaction          (C.loon_transaction_open)   ffi_common.go
//   - commitTransaction        (C.loon_transaction_commit) ffi_common.go
//   - createMilvusTableManifest
//     (C.loon_transaction_open +
//      C.loon_milvus_table_append_source_manifests)         manifest_ffi.go
//
//   Every synchronous manifest mutator funnels through the first two:
//   CommitManifestUpdates, CreateManifestForSegment, AppendSegmentManifestColumns,
//   AddLobFilesToTransaction, AddDeltaLogsToManifest(Overwrite), AddStatsToManifest.
//   Every synchronous manifest reader funnels through openTransaction:
//   GetManifestHandle(WithExtfs), GetManifestFieldIDs, ReadFragmentsFromManifest,
//   ManifestHasColumns, ResolveManifestSingleWriterFormat, ReadManifestColumnGroups,
//   GetManifestIndexInfos, GetManifestLobFiles, GetManifestStats,
//   StatsBinlogSizeFromManifest, GetDeltaLogPathsFromManifest,
//   GetDeltaLogsFromManifestWithExtfs, ReadMilvusTableSnapshotMetadata.
//
// Blocking manifest filesystem I/O (wrapped):
//   - readFileWithExternalSpec (C.loon_filesystem_read_file_all)  ffi_filesystem.go
//   - WriteFile               (C.loon_filesystem_create_dir /
//                              C.loon_filesystem_write_file)      ffi_filesystem.go
//
// Blocking external-source explore I/O (wrapped):
//   - GetFileInfoFromExternalSource
//     (C.loon_exttable_get_file_info)                             explore_ffi.go
//   - ExploreFilesFromExternalSource
//     (C.loon_exttable_explore)                                   explore_ffi.go
//   - ReadFileInfosFromManifestPath
//     (C.loon_exttable_read_manifest)                             explore_ffi.go
//
// Async manifest executor worker (wrapped):
//   - runWorker               (C.milvusRunManifestTask)           manifest_async.go
//
//   The caller's submitOpen/submitCommit only enqueue to the native async
//   engine; the actual blocking manifest I/O runs on these workers, so bounding
//   worker execution bounds the async path's threads process-wide. The worker
//   never re-enters acquire for native continuations (rejected enqueues run
//   inline on the same worker), so this cannot deadlock with the budget.
//
// Quick staging / parse cgo (not wrapped): these build in-memory column-group /
// properties / stat descriptors or copy loaded manifest bytes and do not block
// on storage: loon_properties_create/free, loon_properties_inject_external_spec,
// loon_column_groups_create, loon_transaction_append_files/add_lob_file/
// add_delta_log/drop_column/update_stat, loon_transaction_get_manifest,
// loon_manifest_destroy, CString/free, and the result/error helpers.
//
// Data-plane writer/reader FFI (not wrapped): NewPackedWriter/NewFFIPackedWriter
// Close (parquet file write) and NewPackedReader/NewFFISegmentReader run on
// DataNode / QueryNode write/read paths, not on coordinators; the coordinator
// manifest-add operations they feed are the wrapped ones above.

// manifestFFILimit bounds the number of goroutines that may simultaneously run
// blocking milvus-storage FFI calls in this process. The budget is read once
// from CommonCfg.ManifestFFIConcurrency at first use (the param is
// refreshable=false) and is shared by every component in the process, so the
// per-context pools keep their own admission while the process-wide ceiling
// caps cgo thread growth.
type manifestFFILimit struct {
	once  sync.Once
	slots *semaphore.Weighted
}

func (l *manifestFFILimit) acquire(ctx context.Context) error {
	l.once.Do(func() {
		if l.slots == nil {
			limit := max(1, paramtable.Get().CommonCfg.ManifestFFIConcurrency.GetAsInt())
			l.slots = semaphore.NewWeighted(int64(limit))
		}
	})
	return l.slots.Acquire(ctx, 1)
}

func (l *manifestFFILimit) release() {
	l.slots.Release(1)
}

// manifestFFI is the process-wide budget shared by every blocking manifest FFI
// entry point. Acquire is cancelable so callers that carry a context can abort
// instead of parking on a saturated budget.
var manifestFFI = &manifestFFILimit{}

func acquireManifestFFI(ctx context.Context) error {
	return manifestFFI.acquire(ctx)
}

func releaseManifestFFI() {
	manifestFFI.release()
}

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

//go:build test

package datacoord

import (
	"context"
	"github.com/bytedance/mockey"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/metastore"
	kv_datacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/stretchr/testify/require"
	"testing"
)

func newCopyCleanupLifecycle(t *testing.T) (*copySegmentTask, CopySegmentMeta, *meta, *snapshotTestMetas) {
	t.Helper()
	ctx := context.Background()
	catalog := kvdatacoord.NewCatalog(NewMetaMemoryKV(), "", "")
	m, dm := bootMetaForRestartWithSnapshot(t, catalog, 100)
	root := t.TempDir()
	m.chunkManager = storage.NewLocalChunkManager(objectstorage.RootPath(root))
	copies, err := NewCopySegmentMeta(ctx, catalog, m, dm.indexMeta, nil, nil)
	require.NoError(t, err)
	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	require.NoError(t, copies.AddTask(ctx, task))
	require.NoError(t, copies.AddJob(ctx, newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting)))
	segment := newTestCopySegment(2001)
	segment.StorageVersion = storage.StorageV3
	segment.IsImporting = true
	require.NoError(t, m.AddSegment(ctx, segment))
	require.NoError(t, task.prepareCopyCleanup(ctx, &datapb.CopySegmentRequest{
		StorageConfig: &indexpb.StorageConfig{RootPath: root},
		Sources: []*datapb.CopySegmentSource{{StorageVersion: storage.StorageV3, IndexFiles: []*indexpb.IndexFilePathInfo{{
			BuildID: 5, IndexVersion: 1, IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
		}}}},
		Targets: []*datapb.CopySegmentTarget{{CollectionId: 100, PartitionId: 10, SegmentId: 2001, NewBuildIds: map[int64]int64{5: 6}}},
	}))
	require.NoError(t, copies.UpdateTask(ctx, task.GetTaskId(), UpdateCopyTaskNodeID(42), UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskInProgress)))
	return task, copies, m, dm
}

// snapshotTestMetas holds the domain metas that snapshot-layer and higher tests build together with a meta.
type snapshotTestMetas struct {
	indexMeta    *indexMeta
	analyzeMeta  *analyzeMeta
	snapshotMeta *snapshotMeta
}

// loadSnapshotTestMetas returns a DomainMetaLoader that builds the index, analyze and snapshot metas into out.
// It returns the index meta as the index catalog and the snapshot meta as the compaction protection.
func loadSnapshotTestMetas(catalog metastore.DataCoordCatalog, cm storage.ChunkManager, out *snapshotTestMetas) DomainMetaLoader {
	return func(ctx context.Context, collectionIDs []int64) (SegmentIndexCatalog, CompactionProtection, error) {
		var err error
		if out.indexMeta, err = newIndexMeta(ctx, catalog, collectionIDs); err != nil {
			return nil, nil, err
		}
		if out.analyzeMeta, err = newAnalyzeMeta(ctx, catalog); err != nil {
			return nil, nil, err
		}
		if out.snapshotMeta, err = newSnapshotMeta(ctx, catalog, cm); err != nil {
			return nil, nil, err
		}
		return out.indexMeta, out.snapshotMeta, nil
	}
}

// withSnapshotTestMetas returns a factory of loaders that build the snapshot-layer domain metas into out.
func withSnapshotTestMetas(out *snapshotTestMetas) func(metastore.DataCoordCatalog, storage.ChunkManager) DomainMetaLoader {
	return func(catalog metastore.DataCoordCatalog, cm storage.ChunkManager) DomainMetaLoader {
		return loadSnapshotTestMetas(catalog, cm, out)
	}
}

// bootMetaForRestartWithSnapshot is bootMetaForRestart with the index, analyze and snapshot metas.
func bootMetaForRestartWithSnapshot(t *testing.T, catalog metastore.DataCoordCatalog, collectionID int64) (*meta, *snapshotTestMetas) {
	t.Helper()
	dm := &snapshotTestMetas{}
	m := bootMetaWithLoader(t, catalog, collectionID, withSnapshotTestMetas(dm))
	return m, dm
}

type restoreBrokerTarget struct {
	broker.Broker
}

// newRestoreAbsentTargetBroker supplies admission metadata for tests focused on
// later restore phases. Concurrency tests exercise the changing target state.
func newRestoreAbsentTargetBroker(t *testing.T) broker.Broker {
	t.Helper()
	m := mockey.Mock((*restoreBrokerTarget).DescribeCollectionByName).
		Return(nil, merr.WrapErrCollectionNotFound("target")).Build()
	t.Cleanup(func() { m.UnPatch() })
	return &restoreBrokerTarget{}
}

func createTestSnapshotMeta(t *testing.T) *snapshotMeta {
	// Create empty Catalog for mockey to mock
	catalog := &kv_datacoord.Catalog{}

	// Use temporary directory to avoid polluting project directory
	tempDir := t.TempDir()
	tempChunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))

	loaderCtx, loaderCancel := context.WithCancel(context.Background())
	return &snapshotMeta{
		catalog:                      catalog,
		snapshotID2Info:              typeutil.NewConcurrentMap[typeutil.UniqueID, *datapb.SnapshotInfo](),
		snapshotID2RefIndex:          typeutil.NewConcurrentMap[typeutil.UniqueID, *SnapshotRefIndex](),
		snapshotName2ID:              typeutil.NewConcurrentMap[typeutil.UniqueID, *typeutil.ConcurrentMap[string, typeutil.UniqueID]](),
		collectionID2Snapshots:       typeutil.NewConcurrentMap[typeutil.UniqueID, typeutil.UniqueSet](),
		segmentProtectionUntil:       make(map[int64]uint64),
		compactionBlockedCollections: typeutil.NewUniqueSet(),
		gcBlockedCollections:         typeutil.NewUniqueSet(),
		segmentReferencedByGC:        typeutil.NewUniqueSet(),
		buildIDReferencedByGC:        typeutil.NewUniqueSet(),
		snapshotPendingCollections:   typeutil.NewUniqueSet(),
		loaderCtx:                    loaderCtx,
		loaderCancel:                 loaderCancel,
		reader:                       snapshotstorage.NewSnapshotReader(tempChunkManager),
		writer:                       snapshotstorage.NewSnapshotWriter(tempChunkManager),
	}
}

// createTestSnapshotMetaLoaded creates a snapshotMeta for tests that don't call reload().
// Same as createTestSnapshotMeta since RefIndex state is now per-snapshot.
func createTestSnapshotMetaLoaded(t *testing.T) *snapshotMeta {
	return createTestSnapshotMeta(t)
}

// insertTestSnapshot is a LIGHTWEIGHT helper that only primes the three lookup maps
// (snapshotID2Info / snapshotID2RefIndex / secondary indexes). It is meant for tests
// that exercise pure lookup paths (ListSnapshots, name→ID resolution, etc.) where the
// snapshot protection state is irrelevant.
//
// It does NOT maintain any of the protection sets — segmentProtectionUntil,
// segmentReferencedByGC, buildIDReferencedByGC, compactionBlockedCollections, or
// gcBlockedCollections are left untouched. If your test needs protection state to
// reflect the inserted snapshot, one of the following is required:
//   - call sm.rebuildAllSegmentProtection() after insertion to derive all 5 sets, OR
//   - call sm.registerSnapshotProtection(info, segmentIDs, buildIDs) for an incremental
//     update, OR
//   - use saveTestSnapshots() which goes through the real SaveSnapshot code path.
//
// Parameters:
//   - sm: the snapshotMeta to populate
//   - info: the snapshot metadata (with Id, Name, CollectionId, etc.)
//   - segmentIDs: segment IDs the snapshot references (passed to RefIndex)
//   - buildIDs: index build IDs the snapshot references (passed to RefIndex)
func insertTestSnapshot(sm *snapshotMeta, info *datapb.SnapshotInfo, segmentIDs, buildIDs []int64) {
	sm.snapshotID2Info.Insert(info.GetId(), info)
	sm.snapshotID2RefIndex.Insert(info.GetId(), NewLoadedSnapshotRefIndex(segmentIDs, buildIDs))
	sm.addToSecondaryIndexes(info)
}

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
	"sort"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func ManifestIndexRollbackEnabled() bool {
	return Params.DataCoordCfg.ManifestIndexRollbackEnabled.GetAsBool()
}

func IsSegmentIndexRollbackRetained(segment *SegmentInfo) bool {
	return segment != nil && segment.GetStorageVersion() == storage.StorageV3 &&
		segment.GetLevel() != datapb.SegmentLevel_L0 &&
		(IsSegmentHealthy(segment) || segment.GetState() == commonpb.SegmentState_Dropped)
}

// rollbackSegmentIndexes restores selected in-memory records to etcd. The
// segment lock protects the source manifest through publication; BuildID locks
// in the commit revalidate the records before changing their durable placement.
func (m *Meta) RollbackSegmentIndexes(ctx context.Context, io *packed.ManifestIOContext, segmentID int64, selectedBuilds ...int64) (int, error) {
	var manifestUpdates []*SegmentInfo
	defer func() { m.NotifyQueryViewSegmentUpdates(ctx, manifestUpdates) }()
	locks := m.GetSegmentManifestLocks()
	lockStart := time.Now()
	locks.Lock(segmentID)
	defer locks.Unlock(segmentID)
	lockWait := time.Since(lockStart)
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	sources := make(map[int64]*packed.ManifestIndexInfo, len(selectedBuilds))
	for _, buildID := range selectedBuilds {
		if record, ok := m.IndexCatalog.GetIndexJob(buildID); ok && record.ManifestPublished && record.SegmentID == segmentID {
			sources[buildID] = nil
		}
	}
	if len(sources) == 0 {
		return 0, nil
	}
	segment := m.GetSegment(ctx, segmentID)
	if segment == nil {
		return 0, nil
	}
	if !IsSegmentIndexRollbackRetained(segment) || segment.GetManifestPath() == "" {
		return 0, merr.Wrapf(merr.ErrDataIntegrity, "segment %d has an unsupported manifest index placement", segmentID)
	}
	cfg := CreateStorageConfig()
	entries, err := m.ReadManifestIndexesWithIO(ctx, io, segment.GetManifestPath(), cfg)
	if err != nil {
		return 0, merr.Wrap(err, "read manifest indexes for rollback")
	}
	// Resolve only selected records. Superseded builds may no longer have an
	// entry in the current manifest. Entries without selected records belong
	// to their existing lifecycle (including GC and copy installation).
	indexIDs := make(map[int64]struct{}, len(sources))
	for idx := range entries {
		entry := &entries[idx]
		previous, selected := sources[entry.BuildID]
		if !selected {
			continue
		}
		_, duplicateIndex := indexIDs[entry.IndexID]
		if _, valid := ManifestIndexFilePathInfoForSegment(m.ChunkManager.RootPath(), segment.SegmentInfo, *entry); !valid || duplicateIndex || previous != nil {
			return 0, merr.Wrapf(merr.ErrDataIntegrity, "segment %d has an invalid or duplicate selected manifest index %d build %d", segmentID, entry.IndexID, entry.BuildID)
		}
		indexIDs[entry.IndexID] = struct{}{}
		sources[entry.BuildID] = entry
	}
	limit := Params.MetaStoreCfg.MaxEtcdTxnNum.GetAsInt() - 1
	if limit < 1 {
		return 0, merr.WrapErrServiceInternalMsg("index rollback requires at least two etcd transaction operations")
	}
	selected := make([]int64, 0, len(sources))
	for buildID := range sources {
		selected = append(selected, buildID)
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i] < selected[j] })
	selected = selected[:min(len(selected), limit)]
	drops := make([]packed.DropIndexEntry, 0, len(selected))
	mutations := make([]SegmentIndexMutation, 0, len(selected))
	for _, buildID := range selected {
		entry := sources[buildID]
		if entry != nil {
			drops = append(drops, packed.DropIndexEntry{IndexID: entry.IndexID, ExpectedBuildID: buildID})
		}
		mutations = append(mutations, SegmentIndexMutation{Type: SegmentIndexRollback, BuildID: buildID, rollbackEntry: entry})
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	mutation := ManifestMutation{Type: ManifestMutationCommitUpdates, Updates: &packed.ManifestUpdates{DropIndexes: drops}}
	if len(drops) == 0 {
		// The source read proved these records are already absent from the
		// manifest. Publish their PUTs against the unchanged, locked pointer.
		mutation = ManifestMutation{Type: ManifestMutationNoop, ManifestPath: segment.GetManifestPath()}
	}
	manifestUpdates, err = m.commitSegmentManifestLocked(ctx, SegmentManifestCommit{
		SegmentID: segmentID, StorageConfig: cfg,
		Mutation:        mutation,
		CatalogMutation: SegmentCatalogMutation{SegmentIndexes: mutations, manifestHasIndex: proto.Bool(len(entries) > 0)},
	}, segment.GetManifestPath(), lockWait)
	if err != nil {
		return 0, err
	}
	return len(selected), nil
}

// validateRollbackIndex runs with the segment and BuildID locks held. The
// record must still describe the artifact whose metadata leaves the manifest.
func (m *Meta) validateRollbackIndex(segment *SegmentInfo, mutation SegmentIndexMutation, record *model.SegmentIndex) error {
	entry := mutation.rollbackEntry
	if record == nil || !record.ManifestPublished {
		return ErrSegmentIndexRollbackSkipped
	}
	if entry == nil {
		// Only the locked rollback preparer can supply this proof of absence.
		return nil
	}
	if entry.BuildID != record.BuildID || entry.IndexID != record.IndexID {
		return merr.Wrapf(merr.ErrDataIntegrity, "rollback record disagrees with manifest index identity, buildID=%d", mutation.BuildID)
	}
	if record.IndexState != commonpb.IndexState_Finished || len(record.IndexFileKeys) == 0 {
		return merr.Wrapf(merr.ErrDataIntegrity, "manifest-only rollback record is not a finished artifact, buildID=%d", record.BuildID)
	}
	if _, valid := ManifestIndexFilePathInfoForSegment(m.ChunkManager.RootPath(), segment.SegmentInfo, *entry); !valid {
		return merr.Wrapf(merr.ErrDataIntegrity, "rollback source path disagrees with record, buildID=%d", record.BuildID)
	}
	return validateManifestIndexRecordProjection(*entry, record)
}

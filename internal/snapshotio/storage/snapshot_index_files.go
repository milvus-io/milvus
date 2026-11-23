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

package storage

import (
	"context"

	"golang.org/x/sync/errgroup"

	milvusstorage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ResolveSnapshotIndexFiles fills the missing file records of a snapshot read from
// storage. The explicit records and the pinned BuildIDs together define the
// inventory. The caller owns the snapshot and supplies the source storage config.
func ResolveSnapshotIndexFiles(ctx context.Context, snapshot *SnapshotData, config *indexpb.StorageConfig) error {
	if snapshot == nil || !snapshot.IndexFilesFromManifest {
		return nil
	}
	if config == nil {
		config = packed.CreateStorageConfig()
	}

	pending := typeutil.NewUniqueSet(snapshot.BuildIDs...)
	for _, segment := range snapshot.Segments {
		for _, index := range segment.GetIndexFiles() {
			pending.Remove(index.GetBuildID())
		}
		for _, index := range segment.GetTextIndexFiles() {
			pending.Remove(index.GetBuildID())
		}
		for _, index := range segment.GetJsonKeyIndexFiles() {
			pending.Remove(index.GetBuildID())
		}
	}
	if len(pending) == 0 {
		return nil
	}

	type source struct {
		segment *datapb.SegmentDescription
		path    string
		files   []*indexpb.IndexFilePathInfo
	}
	var sources []source
	var refs []SnapshotFileRef
	checkSourceRoot := hasURITransportScheme(snapshot.MetadataPath)
	for _, segment := range snapshot.Segments {
		if !segment.GetManifestHasIndex() || segment.GetStorageVersion() != milvusstorage.StorageV3 {
			continue
		}
		revision, err := packed.ParseManifestPath(segment.GetManifestPath())
		if err != nil || revision.ManifestVersion <= packed.ManifestEarliest || revision.BasePath == "" {
			return merr.WrapErrDataIntegrityMsg("snapshot segment %d has no valid pinned manifest revision", segment.GetSegmentId())
		}
		base, err := normalizeSnapshotPathReference(revision.BasePath)
		if err != nil {
			return merr.Wrapf(err, "resolve manifest path for snapshot segment %d", segment.GetSegmentId())
		}
		if checkSourceRoot {
			refs = append(refs, SnapshotFileRef{Path: revision.BasePath, NormalizedPath: base})
		}
		sources = append(sources, source{
			segment: segment,
			path:    packed.CommittedManifestPath(base, revision.ManifestVersion, revision.Size, config.GetStorageType()),
		})
	}
	if checkSourceRoot {
		if err := ValidateExternalSnapshotPaths(snapshot.MetadataPath, snapshot, refs); err != nil {
			return err
		}
	}

	definitions := make(map[int64]*indexpb.IndexInfo, len(snapshot.Indexes))
	for _, index := range snapshot.Indexes {
		definitions[index.GetIndexID()] = index
	}
	if len(sources) > 0 {
		limit := max(1, min(len(sources), paramtable.Get().DataCoordCfg.SegmentIndexManifestLoadConcurrency.GetAsInt()))
		if connections := config.GetMaxConnections(); connections > 0 {
			limit = min(limit, int(connections))
		}
		io := packed.NewManifestIOContext(limit)
		defer io.Close()
		group, readCtx := errgroup.WithContext(ctx)
		group.SetLimit(limit)
		for i := range sources {
			group.Go(func() error {
				source := &sources[i]
				files, err := readSnapshotManifestIndexes(readCtx, io, config, snapshot.SnapshotInfo.GetCollectionId(),
					source.segment, source.path, pending, definitions)
				source.files = files
				return err
			})
		}
		if err := group.Wait(); err != nil {
			return err
		}
	}

	for _, source := range sources {
		for _, file := range source.files {
			if !pending.Contain(file.GetBuildID()) {
				return merr.WrapErrDataIntegrityMsg("duplicate build %d in pinned snapshot manifests", file.GetBuildID())
			}
			pending.Remove(file.GetBuildID())
		}
	}
	for id := range pending {
		return merr.WrapErrDataIntegrityMsg("snapshot build %d is missing from the pinned manifests", id)
	}
	for _, source := range sources {
		source.segment.IndexFiles = append(source.segment.IndexFiles, source.files...)
	}
	return nil
}

func readSnapshotManifestIndexes(ctx context.Context, io *packed.ManifestIOContext, config *indexpb.StorageConfig, collectionID int64,
	segment *datapb.SegmentDescription, manifestPath string, pending typeutil.UniqueSet, definitions map[int64]*indexpb.IndexInfo,
) ([]*indexpb.IndexFilePathInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := packed.GetManifestIndexInfosAsync(ctx, io, manifestPath, config)
	if err != nil {
		return nil, merr.Wrapf(err, "read indexes for snapshot segment %d", segment.GetSegmentId())
	}
	seen := typeutil.NewUniqueSet()
	for _, index := range segment.GetIndexFiles() {
		seen.Insert(index.GetIndexID())
	}
	var files []*indexpb.IndexFilePathInfo
	for _, entry := range entries {
		if !pending.Contain(entry.BuildID) {
			continue
		}
		definition := definitions[entry.IndexID]
		if definition == nil || entry.FieldID != definition.GetFieldID() || seen.Contain(entry.IndexID) ||
			!metautil.IsSegmentRooted(entry.IndexStorePathVersion) {
			return nil, merr.WrapErrDataIntegrityMsg("invalid index identity for snapshot segment %d build %d", segment.GetSegmentId(), entry.BuildID)
		}
		info, valid := packed.ManifestIndexFilePathInfoForSegment(config.GetRootPath(), manifestPath,
			collectionID, segment.GetPartitionId(), segment.GetSegmentId(), entry)
		if !valid {
			return nil, merr.WrapErrDataIntegrityMsg("invalid index paths for snapshot segment %d build %d", segment.GetSegmentId(), entry.BuildID)
		}
		info.IndexName = definition.GetIndexName()
		info.IndexParams = common.CloneKeyValuePairs(definition.GetIndexParams())
		info.IndexParams = append(info.IndexParams, common.CloneKeyValuePairs(definition.GetTypeParams())...)
		for _, param := range info.IndexParams {
			if param.GetKey() == common.IndexTypeKey {
				param.Value = info.GetIndexType()
			}
		}
		seen.Insert(entry.IndexID)
		files = append(files, info)
	}
	return files, nil
}

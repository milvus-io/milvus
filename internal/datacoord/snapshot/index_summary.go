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

package snapshot

import (
	"context"

	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	snapshotstorage "github.com/milvus-io/milvus/pkg/v3/snapshotio"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func validateSummarySnapshot(ctx context.Context, mt *meta.Meta, snapshot *snapshotstorage.SnapshotData, foreignConfig *indexpb.StorageConfig) error {
	liveIndexes := make(map[int64]struct{}, len(snapshot.Indexes))
	for _, definition := range snapshot.Indexes {
		if definition != nil {
			liveIndexes[definition.GetIndexID()] = struct{}{}
		}
	}
	type manifestCheck struct {
		segmentID int64
		path      string
		captured  map[int64]struct{}
	}
	var checks []manifestCheck
	for _, segment := range snapshot.Segments {
		if segment.GetSegmentLevel() == datapb.SegmentLevel_L0 && len(segment.GetIndexFiles()) == 0 &&
			!segment.GetManifestHasIndex() {
			continue
		}
		if segment.GetStorageVersion() != storage.StorageV3 || segment.GetManifestPath() == "" || segment.ManifestHasIndex == nil {
			return merr.Wrap(merr.ErrServiceUnimplemented, "snapshot is incompatible with segment index summary mode: StorageV3 and explicit manifest index inventory are required")
		}
		captured := make(map[int64]struct{}, len(segment.GetIndexFiles()))
		for _, index := range segment.GetIndexFiles() {
			if len(index.GetIndexFilePaths()) == 0 {
				continue
			}
			if index.GetIndexStorePathVersion() != indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED {
				return merr.Wrap(merr.ErrServiceUnimplemented, "snapshot is incompatible with segment index summary mode: indexes must use segment-rooted storage")
			}
			if _, live := liveIndexes[index.GetIndexID()]; live {
				captured[index.GetIndexID()] = struct{}{}
			}
		}
		if segment.GetManifestHasIndex() && len(captured) < len(liveIndexes) {
			path := segment.GetManifestPath()
			if foreignConfig != nil && foreignConfig.GetStorageType() != "local" {
				revision, err := packed.ParseManifestPath(path)
				if err != nil {
					return merr.WrapErrDataIntegrityMsg("invalid pinned manifest path for snapshot segment %d", segment.GetSegmentId())
				}
				base := snapshotstorage.NormalizeSnapshotObjectPath(revision.BasePath)
				path = packed.CommittedManifestPath(base, revision.ManifestVersion, revision.Size, foreignConfig.GetStorageType())
			}
			checks = append(checks, manifestCheck{segment.GetSegmentId(), path, captured})
		}
	}
	if len(checks) == 0 {
		return nil
	}
	cfg := foreignConfig
	if cfg == nil {
		cfg = meta.CreateStorageConfig()
	}
	io := packed.NewManifestIOContext(min(len(checks), meta.SegmentIndexManifestReadConcurrency()))
	defer io.Close()
	return meta.RunManifestBatch(ctx, len(checks), func(ctx context.Context, i int, complete func(error)) error {
		check := checks[i]
		return mt.SubmitManifestIndexRead(ctx, io, check.path, cfg, func(entries []packed.ManifestIndexInfo, err error) {
			if err != nil {
				complete(merr.Wrapf(err, "verify explicit index files for snapshot segment %d", check.segmentID))
				return
			}
			for _, entry := range entries {
				_, live := liveIndexes[entry.IndexID]
				_, captured := check.captured[entry.IndexID]
				if live && !captured {
					complete(merr.Wrapf(merr.ErrServiceUnimplemented, "snapshot is incompatible with segment index summary mode: explicit index files are missing for segment %d index %d", check.segmentID, entry.IndexID))
					return
				}
			}
			complete(nil)
		})
	})
}

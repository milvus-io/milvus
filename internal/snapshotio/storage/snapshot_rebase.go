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
	"strings"

	milvusstorage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type snapshotRootRebaser struct {
	oldRoot string
	newRoot string
}

func newSnapshotRootRebaser(oldRoot string, newRoot string) snapshotRootRebaser {
	return snapshotRootRebaser{
		oldRoot: strings.TrimRight(NormalizeSnapshotObjectPath(oldRoot), "/"),
		newRoot: strings.TrimRight(NormalizeSnapshotObjectPath(newRoot), "/"),
	}
}

func (r snapshotRootRebaser) rebasePath(src string) string {
	if src == "" || r.oldRoot == r.newRoot {
		return src
	}
	// Root relocation is prefix-only: it supports moving the whole exported
	// bundle while preserving its internal layout, not arbitrary path rewrites.
	normalized := NormalizeSnapshotObjectPath(src)
	if r.oldRoot == "" {
		if normalized == "" {
			return r.newRoot
		}
		return r.newRoot + "/" + strings.TrimLeft(normalized, "/")
	}
	if normalized == r.oldRoot {
		return r.newRoot
	}
	if strings.HasPrefix(normalized, r.oldRoot+"/") {
		relativePath := strings.TrimPrefix(normalized, r.oldRoot+"/")
		if r.newRoot == "" {
			return relativePath
		}
		return r.newRoot + "/" + relativePath
	}
	return src
}

func (r snapshotRootRebaser) rebaseManifestPath(src string) (string, error) {
	if src == "" {
		return "", nil
	}
	if !strings.HasPrefix(strings.TrimSpace(src), "{") {
		return r.rebasePath(src), nil
	}
	// StorageV3 manifest_path is a packed value whose base_path must be rebased
	// and then marshaled back without changing the packed version.
	basePath, version, err := packed.UnmarshalManifestPath(src)
	if err != nil {
		return "", merr.WrapErrDataIntegrity(err, "failed to parse manifest path")
	}
	rebasedBasePath := r.rebasePath(basePath)
	if rebasedBasePath == basePath {
		return src, nil
	}
	return packed.MarshalManifestPath(rebasedBasePath, version), nil
}

func RebaseSelfContainedSnapshotMetadata(
	metadata *datapb.SnapshotMetadata,
	oldRoot string,
	newRoot string,
) error {
	if metadata == nil {
		return merr.WrapErrDataIntegrityMsg("snapshot metadata cannot be nil")
	}

	rebaser := newSnapshotRootRebaser(oldRoot, newRoot)
	for i, manifestPath := range metadata.GetManifestList() {
		metadata.ManifestList[i] = rebaser.rebasePath(manifestPath)
	}
	for i, manifest := range metadata.GetStoragev2ManifestList() {
		if manifest == nil {
			return merr.WrapErrDataIntegrityMsg("storage v2 manifest at index %d cannot be nil", i)
		}
		rewritten, err := rebaser.rebaseManifestPath(manifest.GetManifest())
		if err != nil {
			return merr.Wrapf(err, "failed to rebase storage v2 manifest for segment %d", manifest.GetSegmentId())
		}
		manifest.Manifest = rewritten
	}
	return nil
}

func RebaseSelfContainedSnapshotData(
	snapshot *SnapshotData,
	oldRoot string,
	newRoot string,
) error {
	if snapshot == nil {
		return merr.WrapErrDataIntegrityMsg("snapshot cannot be nil")
	}

	rebaser := newSnapshotRootRebaser(oldRoot, newRoot)
	for i, segment := range snapshot.Segments {
		if segment == nil {
			return merr.WrapErrDataIntegrityMsg("snapshot segment at index %d cannot be nil", i)
		}
		if err := rebaser.rebaseSegment(segment); err != nil {
			return err
		}
	}
	return nil
}

func (r snapshotRootRebaser) rebaseSegment(segment *datapb.SegmentDescription) error {
	// StorageV3 insert and text/JSON files are addressed through the manifest;
	// their PB paths are metadata placeholders rather than relocation inputs.
	includeManifestOwnedPaths := segment.GetStorageVersion() < milvusstorage.StorageV3
	if err := rewriteSegmentFilePaths(segment, includeManifestOwnedPaths, includeManifestOwnedPaths, func(src, _ string) (string, error) {
		return r.rebasePath(src), nil
	}); err != nil {
		return err
	}
	if segment.GetManifestPath() != "" {
		rewritten, err := r.rebaseManifestPath(segment.GetManifestPath())
		if err != nil {
			return merr.Wrapf(err, "failed to rebase manifest path for segment %d", segment.GetSegmentId())
		}
		segment.ManifestPath = rewritten
	}
	return nil
}

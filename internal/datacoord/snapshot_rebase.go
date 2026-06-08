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

package datacoord

import (
	"fmt"
	"strings"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

type snapshotRootRebaser struct {
	cm      storage.ChunkManager
	oldRoot string
	newRoot string
}

func newSnapshotRootRebaser(cm storage.ChunkManager, oldRoot string, newRoot string) snapshotRootRebaser {
	return snapshotRootRebaser{
		cm:      cm,
		oldRoot: strings.TrimRight(normalizeSnapshotObjectPath(cm, oldRoot), "/"),
		newRoot: strings.TrimRight(normalizeSnapshotObjectPath(cm, newRoot), "/"),
	}
}

func (r snapshotRootRebaser) rebasePath(src string) string {
	if src == "" || r.oldRoot == "" || r.newRoot == "" {
		return src
	}
	normalized := normalizeSnapshotObjectPath(r.cm, src)
	if normalized == r.oldRoot {
		return r.newRoot
	}
	if strings.HasPrefix(normalized, r.oldRoot+"/") {
		return r.newRoot + strings.TrimPrefix(normalized, r.oldRoot)
	}
	return src
}

func (r snapshotRootRebaser) rebaseManifestPath(src string) (string, error) {
	if src == "" {
		return "", nil
	}
	if r.oldRoot == "" || r.newRoot == "" {
		return src, nil
	}
	basePath, version, err := packed.UnmarshalManifestPath(src)
	if err != nil {
		return "", err
	}
	rebasedBasePath := r.rebasePath(basePath)
	if rebasedBasePath == basePath {
		return src, nil
	}
	return packed.MarshalManifestPath(rebasedBasePath, version), nil
}

func RebaseSelfContainedSnapshotMetadata(
	cm storage.ChunkManager,
	metadata *datapb.SnapshotMetadata,
	oldRoot string,
	newRoot string,
) error {
	if cm == nil {
		return fmt.Errorf("chunk manager cannot be nil")
	}
	if metadata == nil {
		return fmt.Errorf("snapshot metadata cannot be nil")
	}

	rebaser := newSnapshotRootRebaser(cm, oldRoot, newRoot)
	for i, manifestPath := range metadata.GetManifestList() {
		metadata.ManifestList[i] = rebaser.rebasePath(manifestPath)
	}
	for i, manifest := range metadata.GetStoragev2ManifestList() {
		if manifest == nil {
			return fmt.Errorf("storage v2 manifest at index %d cannot be nil", i)
		}
		rewritten, err := rebaser.rebaseManifestPath(manifest.GetManifest())
		if err != nil {
			return fmt.Errorf("failed to rebase storage v2 manifest for segment %d: %w", manifest.GetSegmentId(), err)
		}
		manifest.Manifest = rewritten
	}
	return nil
}

func RebaseSelfContainedSnapshotData(
	cm storage.ChunkManager,
	snapshot *SnapshotData,
	oldRoot string,
	newRoot string,
) error {
	if cm == nil {
		return fmt.Errorf("chunk manager cannot be nil")
	}
	if snapshot == nil {
		return fmt.Errorf("snapshot cannot be nil")
	}

	rebaser := newSnapshotRootRebaser(cm, oldRoot, newRoot)
	for i, segment := range snapshot.Segments {
		if segment == nil {
			return fmt.Errorf("snapshot segment at index %d cannot be nil", i)
		}
		if err := rebaser.rebaseSegment(segment); err != nil {
			return err
		}
	}
	return nil
}

func (r snapshotRootRebaser) rebaseSegment(segment *datapb.SegmentDescription) error {
	if err := r.rebaseFieldBinlogs(segment.GetBinlogs(), "insert binlog", segment.GetSegmentId()); err != nil {
		return err
	}
	if err := r.rebaseFieldBinlogs(segment.GetStatslogs(), "stats binlog", segment.GetSegmentId()); err != nil {
		return err
	}
	if err := r.rebaseFieldBinlogs(segment.GetDeltalogs(), "delta binlog", segment.GetSegmentId()); err != nil {
		return err
	}
	if err := r.rebaseFieldBinlogs(segment.GetBm25Statslogs(), "bm25 stats binlog", segment.GetSegmentId()); err != nil {
		return err
	}
	if err := r.rebaseIndexFiles(segment.GetIndexFiles(), segment.GetSegmentId()); err != nil {
		return err
	}
	if err := r.rebaseTextIndexes(segment.GetTextIndexFiles(), segment.GetSegmentId()); err != nil {
		return err
	}
	if err := r.rebaseJSONKeyIndexes(segment.GetJsonKeyIndexFiles(), segment.GetSegmentId()); err != nil {
		return err
	}
	if segment.GetManifestPath() != "" {
		rewritten, err := r.rebaseManifestPath(segment.GetManifestPath())
		if err != nil {
			return fmt.Errorf("failed to rebase manifest path for segment %d: %w", segment.GetSegmentId(), err)
		}
		segment.ManifestPath = rewritten
	}
	return nil
}

func (r snapshotRootRebaser) rebaseFieldBinlogs(fieldBinlogs []*datapb.FieldBinlog, fileType string, segmentID int64) error {
	for fieldIdx, fieldBinlog := range fieldBinlogs {
		if fieldBinlog == nil {
			return fmt.Errorf("%s segment %d field binlog at index %d cannot be nil", fileType, segmentID, fieldIdx)
		}
		for binlogIdx, binlog := range fieldBinlog.GetBinlogs() {
			if binlog == nil {
				return fmt.Errorf("%s segment %d field %d binlog at index %d cannot be nil", fileType, segmentID, fieldBinlog.GetFieldID(), binlogIdx)
			}
			binlog.LogPath = r.rebasePath(binlog.GetLogPath())
		}
	}
	return nil
}

func (r snapshotRootRebaser) rebaseIndexFiles(indexFiles []*indexpb.IndexFilePathInfo, segmentID int64) error {
	for i, indexFile := range indexFiles {
		if indexFile == nil {
			return fmt.Errorf("index file segment %d entry at index %d cannot be nil", segmentID, i)
		}
		for j, filePath := range indexFile.GetIndexFilePaths() {
			indexFile.IndexFilePaths[j] = r.rebasePath(filePath)
		}
	}
	return nil
}

func (r snapshotRootRebaser) rebaseTextIndexes(indexes map[int64]*datapb.TextIndexStats, segmentID int64) error {
	for fieldID, index := range indexes {
		if index == nil {
			return fmt.Errorf("text index segment %d field %d cannot be nil", segmentID, fieldID)
		}
		for i, filePath := range index.GetFiles() {
			index.Files[i] = r.rebasePath(filePath)
		}
	}
	return nil
}

func (r snapshotRootRebaser) rebaseJSONKeyIndexes(indexes map[int64]*datapb.JsonKeyStats, segmentID int64) error {
	for fieldID, index := range indexes {
		if index == nil {
			return fmt.Errorf("json key index segment %d field %d cannot be nil", segmentID, fieldID)
		}
		for i, filePath := range index.GetFiles() {
			index.Files[i] = r.rebasePath(filePath)
		}
	}
	return nil
}

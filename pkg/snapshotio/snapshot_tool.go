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

package snapshotio

import (
	"context"
	"fmt"
	"path"
	"sort"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	snapshotio "github.com/milvus-io/milvus/pkg/v3/snapshotio/format"
	packed "github.com/milvus-io/milvus/pkg/v3/snapshotio/internal/native"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const ExportedSnapshotFilesPath = "files"

type SnapshotFileType string

const (
	SnapshotFileTypeInsertBinlog            SnapshotFileType = "insert_binlog"
	SnapshotFileTypeStatsBinlog             SnapshotFileType = "stats_binlog"
	SnapshotFileTypeDeltaBinlog             SnapshotFileType = "delta_binlog"
	SnapshotFileTypeBM25StatsBinlog         SnapshotFileType = "bm25_stats_binlog"
	SnapshotFileTypeIndexFile               SnapshotFileType = "index_file"
	SnapshotFileTypeTextIndexFile           SnapshotFileType = "text_index_file"
	SnapshotFileTypeJSONKeyIndexFile        SnapshotFileType = "json_key_index_file"
	SnapshotFileTypeJSONStatsFile           SnapshotFileType = "json_stats_file"
	SnapshotFileTypeStorageV2Manifest       SnapshotFileType = "storage_v2_manifest"
	SnapshotFileTypeStorageV3ManifestRoot   SnapshotFileType = "storage_v3_manifest_root"
	SnapshotFileTypeStorageV3ManifestFile   SnapshotFileType = "storage_v3_manifest_file"
	SnapshotFileTypeStorageV3ManifestObject SnapshotFileType = "storage_v3_manifest_object"
	SnapshotFileTypeStorageV3LOBFile        SnapshotFileType = "storage_v3_lob_file"
)

type SnapshotFileRef struct {
	Path           string
	NormalizedPath string
	Type           SnapshotFileType
	SegmentID      int64
}

// ValidateExternalSnapshotDataFiles checks metadata/Avro references only. Native
// references are checked by DataNode's pinned inventory before any task writes;
// missing objects fail copying instead of requiring per-object existence probes.
func ValidateExternalSnapshotDataFiles(
	ctx context.Context,
	metadataFilePath string,
	snapshot *SnapshotData,
) error {
	if snapshot == nil {
		return merr.WrapErrServiceInternalMsg("snapshot cannot be nil")
	}
	collector := &snapshotFileRefCollector{byPath: make(map[string]SnapshotFileRef)}
	for _, segment := range snapshot.Segments {
		if err := ctx.Err(); err != nil {
			return err
		}
		if segment.GetStorageVersion() >= StorageV3 {
			if _, err := collector.addStorageV3ManifestRoot(segment); err != nil {
				return err
			}
			collector.addSegmentSupplements(segment)
		} else {
			collector.addLegacySegment(segment)
		}
	}
	return ValidateExternalSnapshotPaths(metadataFilePath, snapshot, collector.refs())
}

type snapshotFileRefCollector struct {
	byPath map[string]SnapshotFileRef
	// raw keeps provenance and URI spelling until the caller selects copy inputs.
	raw   bool
	files []SnapshotFileRef
}

// addLegacySegment collects V1/V2 references; V3 uses the pinned native inventory.
func (c *snapshotFileRefCollector) addLegacySegment(segment *datapb.SegmentDescription) {
	c.addFieldBinlogRefs(segment.GetBinlogs(), segment, SnapshotFileTypeInsertBinlog)
	c.addTextIndexRefs(segment.GetTextIndexFiles(), segment)
	c.addJSONIndexRefs(segment.GetJsonKeyIndexFiles(), segment)
	if segment.GetStorageVersion() == StorageV2 && segment.GetManifestPath() != "" {
		c.add(SnapshotFileRef{Path: segment.GetManifestPath(), Type: SnapshotFileTypeStorageV2Manifest, SegmentID: segment.GetSegmentId()})
	}
	c.addSegmentSupplements(segment)
}

func (c *snapshotFileRefCollector) addSegmentSupplements(segment *datapb.SegmentDescription) {
	c.addFieldBinlogRefs(segment.GetStatslogs(), segment, SnapshotFileTypeStatsBinlog)
	c.addFieldBinlogRefs(segment.GetDeltalogs(), segment, SnapshotFileTypeDeltaBinlog)
	c.addFieldBinlogRefs(segment.GetBm25Statslogs(), segment, SnapshotFileTypeBM25StatsBinlog)
	c.addIndexRefs(segment.GetIndexFiles(), segment)
}

func (c *snapshotFileRefCollector) addStorageV3ManifestRoot(segment *datapb.SegmentDescription) (string, error) {
	basePath, _, err := packed.UnmarshalManifestPath(segment.GetManifestPath())
	if err != nil {
		return "", merr.WrapErrDataIntegrity(err, "failed to parse manifest path for segment %d", segment.GetSegmentId())
	}
	if basePath == "" {
		return "", merr.WrapErrDataIntegrityMsg("storage v3 segment %d requires manifest base path", segment.GetSegmentId())
	}
	normalizedBasePath := NormalizeSnapshotObjectPath(basePath)
	if normalizedBasePath == "" {
		return "", merr.WrapErrDataIntegrityMsg("storage v3 segment %d requires manifest object prefix", segment.GetSegmentId())
	}
	c.add(SnapshotFileRef{
		Path:           basePath,
		NormalizedPath: normalizedBasePath,
		Type:           SnapshotFileTypeStorageV3ManifestRoot,
		SegmentID:      segment.GetSegmentId(),
	})

	return normalizedBasePath, nil
}

func (c *snapshotFileRefCollector) addFieldBinlogRefs(fieldBinlogs []*datapb.FieldBinlog, segment *datapb.SegmentDescription, fileType SnapshotFileType) {
	for _, fieldBinlog := range fieldBinlogs {
		for _, binlog := range fieldBinlog.GetBinlogs() {
			c.add(SnapshotFileRef{
				Path:      binlog.GetLogPath(),
				Type:      fileType,
				SegmentID: segment.GetSegmentId(),
			})
		}
	}
}

func (c *snapshotFileRefCollector) addIndexRefs(indexFiles []*indexpb.IndexFilePathInfo, segment *datapb.SegmentDescription) {
	for _, indexFile := range indexFiles {
		for _, filePath := range indexFile.GetIndexFilePaths() {
			c.add(SnapshotFileRef{
				Path:      filePath,
				Type:      SnapshotFileTypeIndexFile,
				SegmentID: segment.GetSegmentId(),
			})
		}
	}
}

func (c *snapshotFileRefCollector) addTextIndexRefs(indexes map[int64]*datapb.TextIndexStats, segment *datapb.SegmentDescription) {
	for _, index := range indexes {
		for _, filePath := range index.GetFiles() {
			c.add(SnapshotFileRef{
				Path:      filePath,
				Type:      SnapshotFileTypeTextIndexFile,
				SegmentID: segment.GetSegmentId(),
			})
		}
	}
}

func (c *snapshotFileRefCollector) addJSONIndexRefs(indexes map[int64]*datapb.JsonKeyStats, segment *datapb.SegmentDescription) {
	for _, index := range indexes {
		kind := SnapshotFileTypeJSONKeyIndexFile
		if index.GetJsonKeyStatsDataFormat() >= 2 {
			kind = SnapshotFileTypeJSONStatsFile
		}
		for _, filePath := range index.GetFiles() {
			c.add(SnapshotFileRef{
				Path:      filePath,
				Type:      kind,
				SegmentID: segment.GetSegmentId(),
			})
		}
	}
}

func (c *snapshotFileRefCollector) add(ref SnapshotFileRef) {
	if c.raw {
		if ref.Path != "" {
			c.files = append(c.files, ref)
		}
		return
	}
	if ref.NormalizedPath == "" {
		ref.NormalizedPath = NormalizeSnapshotObjectPath(ref.Path)
	}
	if ref.NormalizedPath == "" {
		return
	}
	if _, ok := c.byPath[ref.NormalizedPath]; ok {
		return
	}
	c.byPath[ref.NormalizedPath] = ref
}

func (c *snapshotFileRefCollector) refs() []SnapshotFileRef {
	if c.raw {
		return c.files
	}
	refs := make([]SnapshotFileRef, 0, len(c.byPath))
	for _, ref := range c.byPath {
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool {
		return refs[i].NormalizedPath < refs[j].NormalizedPath
	})
	return refs
}

func validateStorageV3LOBFiles(segment *datapb.SegmentDescription, sourceBasePath, rewrittenBasePath, sourceRoot, targetRoot string, lobFileInfos []packed.LobFileInfo) error {
	for _, info := range lobFileInfos {
		// LOB files are referenced through manifest metadata rather than normal
		// binlog lists. Validate both source and rewritten paths stay under the
		// expected LOB root so export cannot smuggle files across bundle roots.
		context := fmt.Sprintf("storage v3 lob file segment %d field %d", segment.GetSegmentId(), info.FieldID)
		normalizedSource := NormalizeSnapshotObjectPath(info.Path)
		sourceLOBRoot := NormalizeSnapshotObjectPath(storageV3LOBBasePath(sourceBasePath, info.FieldID))
		if !IsSnapshotPathUnderRoot(normalizedSource, sourceLOBRoot) {
			return merr.WrapErrDataIntegrityMsg("%s %q is outside manifest LOB root %q", context, info.Path, sourceLOBRoot)
		}
		rewritten := exportedSnapshotPath(sourceRoot, normalizedSource, targetRoot)
		rewrittenLOBRoot := NormalizeSnapshotObjectPath(storageV3LOBBasePath(rewrittenBasePath, info.FieldID))
		if !IsSnapshotPathUnderRoot(NormalizeSnapshotObjectPath(rewritten), rewrittenLOBRoot) {
			return merr.WrapErrDataIntegrityMsg("%s rewritten path %q is outside rewritten manifest LOB root %q", context, rewritten, rewrittenLOBRoot)
		}
	}
	return nil
}

func storageV3LOBBasePath(manifestBasePath string, fieldID int64) string {
	return path.Join(path.Dir(manifestBasePath), "lobs", fmt.Sprintf("%d", fieldID))
}

type segmentPathRewriteFunc func(src string, context string) (string, error)

func rewriteSegmentFilePaths(
	segment *datapb.SegmentDescription,
	includeInsert bool,
	includeManifestOwnedIndexes bool,
	rewrite segmentPathRewriteFunc,
) error {
	if includeInsert {
		if err := rewriteFieldBinlogPaths(segment.GetBinlogs(), "insert binlog", segment.GetSegmentId(), rewrite); err != nil {
			return err
		}
	}
	if err := rewriteFieldBinlogPaths(segment.GetStatslogs(), "stats binlog", segment.GetSegmentId(), rewrite); err != nil {
		return err
	}
	if err := rewriteFieldBinlogPaths(segment.GetDeltalogs(), "delta binlog", segment.GetSegmentId(), rewrite); err != nil {
		return err
	}
	if err := rewriteFieldBinlogPaths(segment.GetBm25Statslogs(), "bm25 stats binlog", segment.GetSegmentId(), rewrite); err != nil {
		return err
	}
	if err := rewriteIndexFilePaths(segment.GetIndexFiles(), segment.GetSegmentId(), rewrite); err != nil {
		return err
	}
	if includeManifestOwnedIndexes {
		if err := rewriteTextIndexPaths(segment.GetTextIndexFiles(), segment.GetSegmentId(), rewrite); err != nil {
			return err
		}
		return rewriteJSONKeyIndexPaths(segment.GetJsonKeyIndexFiles(), segment.GetSegmentId(), rewrite)
	}
	return nil
}

func rewriteFieldBinlogPaths(fieldBinlogs []*datapb.FieldBinlog, fileType string, segmentID int64, rewrite segmentPathRewriteFunc) error {
	for fieldIdx, fieldBinlog := range fieldBinlogs {
		if fieldBinlog == nil {
			return merr.WrapErrDataIntegrityMsg("%s segment %d field binlog at index %d cannot be nil", fileType, segmentID, fieldIdx)
		}
		for binlogIdx, binlog := range fieldBinlog.GetBinlogs() {
			if binlog == nil {
				return merr.WrapErrDataIntegrityMsg("%s segment %d field %d binlog at index %d cannot be nil", fileType, segmentID, fieldBinlog.GetFieldID(), binlogIdx)
			}
			rewritten, err := rewrite(binlog.GetLogPath(), fmt.Sprintf("%s segment %d field %d", fileType, segmentID, fieldBinlog.GetFieldID()))
			if err != nil {
				return err
			}
			binlog.LogPath = rewritten
		}
	}
	return nil
}

func rewriteIndexFilePaths(indexFiles []*indexpb.IndexFilePathInfo, segmentID int64, rewrite segmentPathRewriteFunc) error {
	for i, indexFile := range indexFiles {
		if indexFile == nil {
			return merr.WrapErrDataIntegrityMsg("index file segment %d entry at index %d cannot be nil", segmentID, i)
		}
		for i, filePath := range indexFile.GetIndexFilePaths() {
			rewritten, err := rewrite(filePath, fmt.Sprintf("index file segment %d field %d build %d", segmentID, indexFile.GetFieldID(), indexFile.GetBuildID()))
			if err != nil {
				return err
			}
			indexFile.IndexFilePaths[i] = rewritten
		}
	}
	return nil
}

func rewriteTextIndexPaths(indexes map[int64]*datapb.TextIndexStats, segmentID int64, rewrite segmentPathRewriteFunc) error {
	for fieldID, index := range indexes {
		if index == nil {
			return merr.WrapErrDataIntegrityMsg("text index segment %d field %d cannot be nil", segmentID, fieldID)
		}
		if index.GetFieldID() != 0 {
			fieldID = index.GetFieldID()
		}
		for i, filePath := range index.GetFiles() {
			rewritten, err := rewrite(filePath, fmt.Sprintf("text index segment %d field %d build %d", segmentID, fieldID, index.GetBuildID()))
			if err != nil {
				return err
			}
			index.Files[i] = rewritten
		}
	}
	return nil
}

func rewriteJSONKeyIndexPaths(indexes map[int64]*datapb.JsonKeyStats, segmentID int64, rewrite segmentPathRewriteFunc) error {
	for fieldID, index := range indexes {
		if index == nil {
			return merr.WrapErrDataIntegrityMsg("json key index segment %d field %d cannot be nil", segmentID, fieldID)
		}
		if index.GetFieldID() != 0 {
			fieldID = index.GetFieldID()
		}
		for i, filePath := range index.GetFiles() {
			rewritten, err := rewrite(filePath, fmt.Sprintf("json key index segment %d field %d build %d", segmentID, fieldID, index.GetBuildID()))
			if err != nil {
				return err
			}
			index.Files[i] = rewritten
		}
	}
	return nil
}

func exportedSnapshotPath(sourceRoot, src, targetRoot string) string {
	return snapshotio.ExportedSnapshotObjectPath(NormalizeSnapshotObjectPath(sourceRoot), src, targetRoot)
}

package storage

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/compaction"
	milvusstorage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
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
	SnapshotFileTypeStorageV2Manifest       SnapshotFileType = "storage_v2_manifest"
	SnapshotFileTypeStorageV3ManifestRoot   SnapshotFileType = "storage_v3_manifest_root"
	SnapshotFileTypeStorageV3ManifestObject SnapshotFileType = "storage_v3_manifest_object"
	SnapshotFileTypeStorageV3LOBFile        SnapshotFileType = "storage_v3_lob_file"
)

type SnapshotFileRef struct {
	Path           string
	NormalizedPath string
	Type           SnapshotFileType
	SegmentID      int64
}

// ListSnapshotDataFiles collects concrete objects referenced by a snapshot.
func ListSnapshotDataFiles(
	ctx context.Context,
	cm milvusstorage.ChunkManager,
	snapshot *SnapshotData,
	storageConfig *indexpb.StorageConfig,
) ([]SnapshotFileRef, error) {
	if snapshot == nil {
		return nil, merr.WrapErrServiceInternalMsg("snapshot cannot be nil")
	}
	if cm == nil {
		return nil, merr.WrapErrServiceInternalMsg("chunk manager cannot be nil")
	}

	if storageConfig == nil {
		storageConfig = compaction.CreateStorageConfig()
	}
	collector := &snapshotFileRefCollector{
		cm:            cm,
		storageConfig: storageConfig,
		byPath:        make(map[string]SnapshotFileRef),
	}
	for _, segment := range snapshot.Segments {
		if err := collector.addSegment(ctx, segment); err != nil {
			return nil, err
		}
	}
	return collector.refs(), nil
}

// ValidateExternalSnapshotDataFiles also enforces the root derived from metadata URI.
func ValidateExternalSnapshotDataFiles(
	ctx context.Context,
	cm milvusstorage.ChunkManager,
	metadataFilePath string,
	snapshot *SnapshotData,
	storageConfig *indexpb.StorageConfig,
) error {
	refs, err := ListSnapshotDataFiles(ctx, cm, snapshot, storageConfig)
	if err != nil {
		return err
	}
	if err := ValidateExternalSnapshotPaths(metadataFilePath, snapshot, refs); err != nil {
		return err
	}
	return validateSnapshotFileRefs(ctx, cm, refs)
}

func validateSnapshotFileRefs(ctx context.Context, cm milvusstorage.ChunkManager, refs []SnapshotFileRef) error {
	for _, ref := range refs {
		if ref.Type == SnapshotFileTypeStorageV3ManifestRoot {
			continue
		}

		if ref.Type == SnapshotFileTypeStorageV3ManifestObject {
			// Manifest objects are discovered and row-count validated while
			// listing the prefix, so only concrete references reach this check.
			continue
		}

		exists, err := cm.Exist(ctx, ref.NormalizedPath)
		if err != nil {
			return merr.Wrapf(err, "failed to check snapshot file %q", ref.NormalizedPath)
		}
		if !exists {
			return merr.WrapErrDataIntegrityMsg("snapshot file does not exist: %s (%s segment %d)", ref.NormalizedPath, ref.Type, ref.SegmentID)
		}
	}

	return nil
}

type snapshotFileRefCollector struct {
	cm            milvusstorage.ChunkManager
	storageConfig *indexpb.StorageConfig
	byPath        map[string]SnapshotFileRef
}

func (c *snapshotFileRefCollector) addSegment(ctx context.Context, segment *datapb.SegmentDescription) error {
	if segment.GetStorageVersion() >= milvusstorage.StorageV3 {
		if err := c.addStorageV3Segment(ctx, segment); err != nil {
			return err
		}
		// Manifest listing already includes text/JSON physical files. PB paths
		// are metadata placeholders and may be stale after format migration.
	} else {
		c.addFieldBinlogRefs(segment.GetBinlogs(), segment, SnapshotFileTypeInsertBinlog)
		c.addTextIndexRefs(segment.GetTextIndexFiles(), segment)
		c.addJSONIndexRefs(segment.GetJsonKeyIndexFiles(), segment)
		if segment.GetStorageVersion() == milvusstorage.StorageV2 && segment.GetManifestPath() != "" {
			c.add(SnapshotFileRef{
				Path:      segment.GetManifestPath(),
				Type:      SnapshotFileTypeStorageV2Manifest,
				SegmentID: segment.GetSegmentId(),
			})
		}
	}

	c.addFieldBinlogRefs(segment.GetStatslogs(), segment, SnapshotFileTypeStatsBinlog)
	c.addFieldBinlogRefs(segment.GetDeltalogs(), segment, SnapshotFileTypeDeltaBinlog)
	c.addFieldBinlogRefs(segment.GetBm25Statslogs(), segment, SnapshotFileTypeBM25StatsBinlog)
	c.addIndexRefs(segment.GetIndexFiles(), segment)
	return nil
}

func (c *snapshotFileRefCollector) addStorageV3Segment(ctx context.Context, segment *datapb.SegmentDescription) error {
	basePath, _, err := packed.UnmarshalManifestPath(segment.GetManifestPath())
	if err != nil {
		return merr.WrapErrDataIntegrity(err, "failed to parse manifest path for segment %d", segment.GetSegmentId())
	}
	if basePath == "" {
		return merr.WrapErrDataIntegrityMsg("storage v3 segment %d requires manifest base path", segment.GetSegmentId())
	}
	normalizedBasePath := NormalizeSnapshotObjectPath(basePath)
	if normalizedBasePath == "" {
		return merr.WrapErrDataIntegrityMsg("storage v3 segment %d requires manifest object prefix", segment.GetSegmentId())
	}
	c.add(SnapshotFileRef{
		Path:           basePath,
		NormalizedPath: normalizedBasePath,
		Type:           SnapshotFileTypeStorageV3ManifestRoot,
		SegmentID:      segment.GetSegmentId(),
	})

	if normalizedBasePath != "" {
		// Keep the manifest root as a prefix reference for path rewriting, then
		// list concrete objects separately so export copies physical files only.
		walkPrefix := normalizedBasePath
		if walkPrefix[len(walkPrefix)-1] != '/' {
			walkPrefix += "/"
		}
		manifestObjectCount := 0
		if err := c.cm.WalkWithPrefix(ctx, walkPrefix, true, func(info *milvusstorage.ChunkObjectInfo) bool {
			manifestObjectCount++
			c.add(SnapshotFileRef{
				Path:      info.FilePath,
				Type:      SnapshotFileTypeStorageV3ManifestObject,
				SegmentID: segment.GetSegmentId(),
			})
			return true
		}); err != nil {
			return merr.Wrapf(err, "failed to list manifest files for segment %d", segment.GetSegmentId())
		}
		if segment.GetNumOfRows() > 0 && manifestObjectCount == 0 {
			return merr.WrapErrDataIntegrityMsg(
				"storage v3 segment %d has %d rows but no manifest objects",
				segment.GetSegmentId(),
				segment.GetNumOfRows(),
			)
		}
	}

	lobFileInfos, err := packed.GetManifestLobFiles(segment.GetManifestPath(), c.storageConfig)
	if err != nil {
		return merr.Wrap(err, fmt.Sprintf("failed to list LOB files for segment %d", segment.GetSegmentId()))
	}
	for _, info := range lobFileInfos {
		c.add(SnapshotFileRef{
			Path:      info.Path,
			Type:      SnapshotFileTypeStorageV3LOBFile,
			SegmentID: segment.GetSegmentId(),
		})
	}
	return nil
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
		for _, filePath := range index.GetFiles() {
			c.add(SnapshotFileRef{
				Path:      filePath,
				Type:      SnapshotFileTypeJSONKeyIndexFile,
				SegmentID: segment.GetSegmentId(),
			})
		}
	}
}

func (c *snapshotFileRefCollector) add(ref SnapshotFileRef) {
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
	refs := make([]SnapshotFileRef, 0, len(c.byPath))
	for _, ref := range c.byPath {
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool {
		return refs[i].NormalizedPath < refs[j].NormalizedPath
	})
	return refs
}

func RewriteSnapshotWithMapping(
	snapshot *SnapshotData,
	mappings map[string]string,
	targetRoot string,
	metadataURI string,
) (*SnapshotData, error) {
	if snapshot == nil {
		return nil, merr.WrapErrServiceInternalMsg("snapshot cannot be nil")
	}
	if snapshot.SnapshotInfo == nil {
		return nil, merr.WrapErrDataIntegrityMsg("snapshot info cannot be nil")
	}
	if snapshot.Collection == nil {
		return nil, merr.WrapErrDataIntegrityMsg("collection description cannot be nil")
	}
	if targetRoot == "" {
		return nil, merr.WrapErrServiceInternalMsg("target root cannot be empty")
	}
	if metadataURI == "" {
		return nil, merr.WrapErrServiceInternalMsg("metadata URI cannot be empty")
	}
	rewriter := snapshotPathRewriter{mappings: mappings}
	// Export writes a self-contained snapshot. Clone the source metadata before
	// rewriting paths so the in-memory referenced snapshot remains unchanged.
	exported := &SnapshotData{
		SnapshotInfo: proto.Clone(snapshot.SnapshotInfo).(*datapb.SnapshotInfo),
		Collection:   proto.Clone(snapshot.Collection).(*datapb.CollectionDescription),
		SegmentIDs:   append([]int64(nil), snapshot.SegmentIDs...),
		BuildIDs:     append([]int64(nil), snapshot.BuildIDs...),
		Layout:       datapb.SnapshotLayout_SnapshotLayoutSelfContained,
	}
	exported.SnapshotInfo.S3Location = metadataURI
	exported.Indexes = make([]*indexpb.IndexInfo, 0, len(snapshot.Indexes))
	for i, index := range snapshot.Indexes {
		if index == nil {
			return nil, merr.WrapErrDataIntegrityMsg("snapshot index at index %d cannot be nil", i)
		}
		exported.Indexes = append(exported.Indexes, proto.Clone(index).(*indexpb.IndexInfo))
	}
	exported.Segments = make([]*datapb.SegmentDescription, 0, len(snapshot.Segments))
	for i, segment := range snapshot.Segments {
		if segment == nil {
			return nil, merr.WrapErrDataIntegrityMsg("snapshot segment at index %d cannot be nil", i)
		}
		cloned := proto.Clone(segment).(*datapb.SegmentDescription)
		if err := rewriter.rewriteSegment(cloned); err != nil {
			return nil, err
		}
		exported.Segments = append(exported.Segments, cloned)
	}
	return exported, nil
}

type snapshotPathRewriter struct {
	mappings map[string]string
}

func (r snapshotPathRewriter) rewriteSegment(segment *datapb.SegmentDescription) error {
	includeInsert := true
	includeManifestOwnedIndexes := true
	if segment.GetStorageVersion() >= milvusstorage.StorageV3 {
		// StorageV3 insert files are owned by the packed manifest. Drop legacy
		// protobuf insert binlogs from exported metadata to avoid copying the
		// same physical data through two path representations.
		segment.Binlogs = nil
		if err := r.rewriteStorageV3Manifest(segment); err != nil {
			return err
		}
		includeInsert = false
		includeManifestOwnedIndexes = false
	}
	if err := rewriteSegmentFilePaths(segment, includeInsert, includeManifestOwnedIndexes, r.rewritePath); err != nil {
		return err
	}
	if segment.GetStorageVersion() == milvusstorage.StorageV2 && segment.GetManifestPath() != "" {
		rewritten, err := r.rewritePath(
			segment.GetManifestPath(),
			fmt.Sprintf("storage v2 manifest segment %d", segment.GetSegmentId()),
		)
		if err != nil {
			return err
		}
		segment.ManifestPath = rewritten
	}
	return nil
}

func (r snapshotPathRewriter) rewriteStorageV3Manifest(segment *datapb.SegmentDescription) error {
	if segment.GetManifestPath() == "" {
		return merr.WrapErrDataIntegrityMsg("storage v3 segment %d requires manifest path", segment.GetSegmentId())
	}
	sourceBasePath, version, rewrittenBasePath, err := r.rewriteManifestPath(segment, "storage v3 manifest root")
	if err != nil {
		return err
	}
	return r.validateStorageV3LOBMappings(segment, sourceBasePath, rewrittenBasePath, version)
}

func (r snapshotPathRewriter) rewriteManifestPath(segment *datapb.SegmentDescription, context string) (string, int64, string, error) {
	basePath, version, err := packed.UnmarshalManifestPath(segment.GetManifestPath())
	if err != nil {
		return "", 0, "", merr.WrapErrDataIntegrity(err, "failed to parse manifest path for segment %d", segment.GetSegmentId())
	}
	rewrittenBasePath, err := r.rewritePath(basePath, fmt.Sprintf("%s segment %d", context, segment.GetSegmentId()))
	if err != nil {
		return "", 0, "", err
	}
	segment.ManifestPath = packed.MarshalManifestPath(rewrittenBasePath, version)
	return basePath, version, rewrittenBasePath, nil
}

func (r snapshotPathRewriter) validateStorageV3LOBMappings(
	segment *datapb.SegmentDescription,
	sourceBasePath string,
	rewrittenBasePath string,
	version int64,
) error {
	lobFileInfos, err := packed.GetManifestLobFiles(packed.MarshalManifestPath(sourceBasePath, version), compaction.CreateStorageConfig())
	if err != nil {
		return merr.Wrap(err, fmt.Sprintf("failed to list LOB files for segment %d", segment.GetSegmentId()))
	}
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
		rewritten, err := r.rewritePath(info.Path, context)
		if err != nil {
			return err
		}
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

func (r snapshotPathRewriter) rewritePath(src string, context string) (string, error) {
	if src == "" {
		return "", nil
	}
	if dst, ok := r.mappings[src]; ok {
		return dst, nil
	}
	normalized := NormalizeSnapshotObjectPath(src)
	if dst, ok := r.mappings[normalized]; ok {
		return dst, nil
	}
	return "", merr.WrapErrDataIntegrityMsg("missing snapshot file mapping for %s: %s", context, src)
}

func ExportedSnapshotPath(cm milvusstorage.ChunkManager, src string, targetRoot string) string {
	root := strings.TrimSuffix(NormalizeSnapshotObjectPath(cm.RootPath()), "/")
	relative := src
	if root != "" {
		if src == root {
			relative = ""
		} else if strings.HasPrefix(src, root+"/") {
			relative = strings.TrimPrefix(src, root+"/")
		}
	}
	// Data files are placed under targetRoot/files while metadata stays under
	// targetRoot/snapshots/..., giving restore a stable bundle anchor plus a
	// relocatable data subtree.
	return path.Join(targetRoot, ExportedSnapshotFilesPath, relative)
}

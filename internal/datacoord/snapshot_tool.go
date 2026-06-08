package datacoord

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

type SnapshotFileRefKind string

const (
	SnapshotFileRefKindObject SnapshotFileRefKind = "object"
	SnapshotFileRefKindPrefix SnapshotFileRefKind = "prefix"
)

type SnapshotFileType string

const (
	SnapshotFileTypeInsertBinlog            SnapshotFileType = "insert_binlog"
	SnapshotFileTypeStatsBinlog             SnapshotFileType = "stats_binlog"
	SnapshotFileTypeDeltaBinlog             SnapshotFileType = "delta_binlog"
	SnapshotFileTypeBM25StatsBinlog         SnapshotFileType = "bm25_stats_binlog"
	SnapshotFileTypeIndexFile               SnapshotFileType = "index_file"
	SnapshotFileTypeTextIndexFile           SnapshotFileType = "text_index_file"
	SnapshotFileTypeJSONKeyIndexFile        SnapshotFileType = "json_key_index_file"
	SnapshotFileTypeStorageV3ManifestRoot   SnapshotFileType = "storage_v3_manifest_root"
	SnapshotFileTypeStorageV3ManifestObject SnapshotFileType = "storage_v3_manifest_object"
	SnapshotFileTypeStorageV3LOBFile        SnapshotFileType = "storage_v3_lob_file"
)

type SnapshotFileRef struct {
	Path           string
	NormalizedPath string
	Kind           SnapshotFileRefKind
	Type           SnapshotFileType
	SegmentID      int64
	FieldID        int64
	BuildID        int64
	StorageVersion int64
}

func ListSnapshotDataFiles(ctx context.Context, cm storage.ChunkManager, snapshot *SnapshotData, storageConfigs ...*indexpb.StorageConfig) ([]SnapshotFileRef, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("snapshot cannot be nil")
	}
	if cm == nil {
		return nil, fmt.Errorf("chunk manager cannot be nil")
	}

	collector := newSnapshotFileRefCollector(cm, firstSnapshotStorageConfig(storageConfigs))
	for _, segment := range snapshot.Segments {
		if err := collector.addSegment(ctx, segment); err != nil {
			return nil, err
		}
	}
	return collector.refs(), nil
}

func ValidateSnapshotDataFiles(ctx context.Context, cm storage.ChunkManager, snapshot *SnapshotData, storageConfigs ...*indexpb.StorageConfig) error {
	refs, err := ListSnapshotDataFiles(ctx, cm, snapshot, storageConfigs...)
	if err != nil {
		return err
	}

	prefixHasObject := make(map[string]bool)
	manifestRootBySegment := make(map[int64]string)
	for _, ref := range refs {
		if ref.Kind == SnapshotFileRefKindPrefix {
			prefixHasObject[ref.NormalizedPath] = false
			if ref.Type == SnapshotFileTypeStorageV3ManifestRoot {
				manifestRootBySegment[ref.SegmentID] = ref.NormalizedPath
			}
		}
	}

	for _, ref := range refs {
		if ref.Kind != SnapshotFileRefKindObject {
			continue
		}

		if ref.Type == SnapshotFileTypeStorageV3ManifestObject {
			if prefix, ok := manifestRootBySegment[ref.SegmentID]; ok && ref.NormalizedPath != prefix {
				prefixHasObject[prefix] = true
			}
			continue
		}

		exists, err := cm.Exist(ctx, ref.NormalizedPath)
		if err != nil {
			return fmt.Errorf("failed to check snapshot file %q: %w", ref.NormalizedPath, err)
		}
		if !exists {
			return fmt.Errorf("snapshot file does not exist: %s (%s segment %d)", ref.NormalizedPath, ref.Type, ref.SegmentID)
		}
	}

	for prefix, hasObject := range prefixHasObject {
		if !hasObject {
			return fmt.Errorf("snapshot file prefix does not contain any objects: %s", prefix)
		}
	}
	return nil
}

func firstSnapshotStorageConfig(storageConfigs []*indexpb.StorageConfig) *indexpb.StorageConfig {
	for _, storageConfig := range storageConfigs {
		if storageConfig != nil {
			return storageConfig
		}
	}
	return nil
}

type snapshotFileRefCollector struct {
	cm            storage.ChunkManager
	storageConfig *indexpb.StorageConfig
	byPath        map[string]SnapshotFileRef
}

func newSnapshotFileRefCollector(cm storage.ChunkManager, storageConfig *indexpb.StorageConfig) *snapshotFileRefCollector {
	if storageConfig == nil {
		storageConfig = compaction.CreateStorageConfig()
	}
	return &snapshotFileRefCollector{
		cm:            cm,
		storageConfig: storageConfig,
		byPath:        make(map[string]SnapshotFileRef),
	}
}

func (c *snapshotFileRefCollector) addSegment(ctx context.Context, segment *datapb.SegmentDescription) error {
	if segment.GetStorageVersion() >= storage.StorageV3 {
		if err := c.addStorageV3Segment(ctx, segment); err != nil {
			return err
		}
	} else {
		c.addFieldBinlogRefs(segment.GetBinlogs(), segment, SnapshotFileTypeInsertBinlog)
	}

	c.addTextIndexRefs(segment.GetTextIndexFiles(), segment)
	c.addJSONIndexRefs(segment.GetJsonKeyIndexFiles(), segment)
	c.addFieldBinlogRefs(segment.GetStatslogs(), segment, SnapshotFileTypeStatsBinlog)
	c.addFieldBinlogRefs(segment.GetDeltalogs(), segment, SnapshotFileTypeDeltaBinlog)
	c.addFieldBinlogRefs(segment.GetBm25Statslogs(), segment, SnapshotFileTypeBM25StatsBinlog)
	c.addIndexRefs(segment.GetIndexFiles(), segment)
	return nil
}

func (c *snapshotFileRefCollector) addStorageV3Segment(ctx context.Context, segment *datapb.SegmentDescription) error {
	basePath, _, err := packed.UnmarshalManifestPath(segment.GetManifestPath())
	if err != nil {
		return fmt.Errorf("failed to parse manifest path for segment %d: %w", segment.GetSegmentId(), err)
	}
	if basePath == "" {
		return fmt.Errorf("storage v3 segment %d requires manifest base path", segment.GetSegmentId())
	}
	normalizedBasePath := normalizeSnapshotObjectPath(c.cm, basePath)
	if normalizedBasePath == "" {
		return fmt.Errorf("storage v3 segment %d requires manifest object prefix", segment.GetSegmentId())
	}
	c.add(SnapshotFileRef{
		Path:           basePath,
		NormalizedPath: normalizedBasePath,
		Kind:           SnapshotFileRefKindPrefix,
		Type:           SnapshotFileTypeStorageV3ManifestRoot,
		SegmentID:      segment.GetSegmentId(),
		StorageVersion: segment.GetStorageVersion(),
	})

	if normalizedBasePath != "" {
		walkPrefix := normalizedBasePath
		if walkPrefix[len(walkPrefix)-1] != '/' {
			walkPrefix += "/"
		}
		if err := c.cm.WalkWithPrefix(ctx, walkPrefix, true, func(info *storage.ChunkObjectInfo) bool {
			c.add(SnapshotFileRef{
				Path:           info.FilePath,
				Kind:           SnapshotFileRefKindObject,
				Type:           SnapshotFileTypeStorageV3ManifestObject,
				SegmentID:      segment.GetSegmentId(),
				StorageVersion: segment.GetStorageVersion(),
			})
			return true
		}); err != nil {
			return fmt.Errorf("failed to list manifest files for segment %d: %w", segment.GetSegmentId(), err)
		}
	}

	lobFileInfos, err := packed.GetManifestLobFiles(segment.GetManifestPath(), c.storageConfig)
	if err != nil {
		return fmt.Errorf("failed to list LOB files for segment %d: %w", segment.GetSegmentId(), err)
	}
	for _, info := range lobFileInfos {
		c.add(SnapshotFileRef{
			Path:           info.Path,
			Kind:           SnapshotFileRefKindObject,
			Type:           SnapshotFileTypeStorageV3LOBFile,
			SegmentID:      segment.GetSegmentId(),
			FieldID:        info.FieldID,
			StorageVersion: segment.GetStorageVersion(),
		})
	}
	return nil
}

func (c *snapshotFileRefCollector) addFieldBinlogRefs(fieldBinlogs []*datapb.FieldBinlog, segment *datapb.SegmentDescription, fileType SnapshotFileType) {
	for _, fieldBinlog := range fieldBinlogs {
		for _, binlog := range fieldBinlog.GetBinlogs() {
			c.add(SnapshotFileRef{
				Path:           binlog.GetLogPath(),
				Kind:           SnapshotFileRefKindObject,
				Type:           fileType,
				SegmentID:      segment.GetSegmentId(),
				FieldID:        fieldBinlog.GetFieldID(),
				StorageVersion: segment.GetStorageVersion(),
			})
		}
	}
}

func (c *snapshotFileRefCollector) addIndexRefs(indexFiles []*indexpb.IndexFilePathInfo, segment *datapb.SegmentDescription) {
	for _, indexFile := range indexFiles {
		for _, filePath := range indexFile.GetIndexFilePaths() {
			c.add(SnapshotFileRef{
				Path:           filePath,
				Kind:           SnapshotFileRefKindObject,
				Type:           SnapshotFileTypeIndexFile,
				SegmentID:      segment.GetSegmentId(),
				FieldID:        indexFile.GetFieldID(),
				BuildID:        indexFile.GetBuildID(),
				StorageVersion: segment.GetStorageVersion(),
			})
		}
	}
}

func (c *snapshotFileRefCollector) addTextIndexRefs(indexes map[int64]*datapb.TextIndexStats, segment *datapb.SegmentDescription) {
	for fieldID, index := range indexes {
		if index.GetFieldID() != 0 {
			fieldID = index.GetFieldID()
		}
		for _, filePath := range index.GetFiles() {
			c.add(SnapshotFileRef{
				Path:           filePath,
				Kind:           SnapshotFileRefKindObject,
				Type:           SnapshotFileTypeTextIndexFile,
				SegmentID:      segment.GetSegmentId(),
				FieldID:        fieldID,
				BuildID:        index.GetBuildID(),
				StorageVersion: segment.GetStorageVersion(),
			})
		}
	}
}

func (c *snapshotFileRefCollector) addJSONIndexRefs(indexes map[int64]*datapb.JsonKeyStats, segment *datapb.SegmentDescription) {
	for fieldID, index := range indexes {
		if index.GetFieldID() != 0 {
			fieldID = index.GetFieldID()
		}
		for _, filePath := range index.GetFiles() {
			c.add(SnapshotFileRef{
				Path:           filePath,
				Kind:           SnapshotFileRefKindObject,
				Type:           SnapshotFileTypeJSONKeyIndexFile,
				SegmentID:      segment.GetSegmentId(),
				FieldID:        fieldID,
				BuildID:        index.GetBuildID(),
				StorageVersion: segment.GetStorageVersion(),
			})
		}
	}
}

func (c *snapshotFileRefCollector) add(ref SnapshotFileRef) {
	if ref.NormalizedPath == "" {
		ref.NormalizedPath = normalizeSnapshotObjectPath(c.cm, ref.Path)
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
		if refs[i].NormalizedPath == refs[j].NormalizedPath {
			return refs[i].Type < refs[j].Type
		}
		return refs[i].NormalizedPath < refs[j].NormalizedPath
	})
	return refs
}

type SnapshotRewriteOptions struct {
	TargetRoot    string
	MetadataURI   string
	StrictMapping bool
}

func RewriteSnapshotWithMapping(cm storage.ChunkManager, snapshot *SnapshotData, mappings map[string]string, opts SnapshotRewriteOptions) (*SnapshotData, error) {
	if cm == nil {
		return nil, fmt.Errorf("chunk manager cannot be nil")
	}
	if snapshot == nil {
		return nil, fmt.Errorf("snapshot cannot be nil")
	}
	if snapshot.SnapshotInfo == nil {
		return nil, fmt.Errorf("snapshot info cannot be nil")
	}
	if snapshot.Collection == nil {
		return nil, fmt.Errorf("collection description cannot be nil")
	}
	if opts.TargetRoot == "" {
		return nil, fmt.Errorf("target root cannot be empty")
	}
	if err := validateSnapshotIndexInfos(snapshot.Indexes); err != nil {
		return nil, err
	}

	rewriter := snapshotPathRewriter{
		cm:       cm,
		mappings: mappings,
		opts:     opts,
	}
	exported := &SnapshotData{
		SnapshotInfo: proto.Clone(snapshot.SnapshotInfo).(*datapb.SnapshotInfo),
		Collection:   proto.Clone(snapshot.Collection).(*datapb.CollectionDescription),
		Indexes:      cloneIndexInfos(snapshot.Indexes),
		SegmentIDs:   append([]int64(nil), snapshot.SegmentIDs...),
		BuildIDs:     append([]int64(nil), snapshot.BuildIDs...),
		Layout:       datapb.SnapshotLayout_SnapshotLayoutSelfContained,
	}
	exported.SnapshotInfo.S3Location = opts.MetadataURI
	exported.Segments = make([]*datapb.SegmentDescription, 0, len(snapshot.Segments))
	for i, segment := range snapshot.Segments {
		if segment == nil {
			return nil, fmt.Errorf("snapshot segment at index %d cannot be nil", i)
		}
		if err := validateSnapshotSegmentRewriteInput(segment); err != nil {
			return nil, err
		}
		cloned := proto.Clone(segment).(*datapb.SegmentDescription)
		if segment.GetStorageVersion() >= storage.StorageV3 {
			cloned.Binlogs = nil
		}
		if err := rewriter.rewriteSegment(cloned); err != nil {
			return nil, err
		}
		exported.Segments = append(exported.Segments, cloned)
	}
	return exported, nil
}

func cloneIndexInfos(indexes []*indexpb.IndexInfo) []*indexpb.IndexInfo {
	cloned := make([]*indexpb.IndexInfo, 0, len(indexes))
	for _, index := range indexes {
		cloned = append(cloned, proto.Clone(index).(*indexpb.IndexInfo))
	}
	return cloned
}

func validateSnapshotIndexInfos(indexes []*indexpb.IndexInfo) error {
	for i, index := range indexes {
		if index == nil {
			return fmt.Errorf("snapshot index at index %d cannot be nil", i)
		}
	}
	return nil
}

func validateSnapshotSegmentRewriteInput(segment *datapb.SegmentDescription) error {
	if segment.GetStorageVersion() < storage.StorageV3 {
		if err := validateSnapshotFieldBinlogs(segment.GetBinlogs(), "insert binlog", segment.GetSegmentId()); err != nil {
			return err
		}
	}
	if err := validateSnapshotFieldBinlogs(segment.GetStatslogs(), "stats binlog", segment.GetSegmentId()); err != nil {
		return err
	}
	if err := validateSnapshotFieldBinlogs(segment.GetDeltalogs(), "delta binlog", segment.GetSegmentId()); err != nil {
		return err
	}
	if err := validateSnapshotFieldBinlogs(segment.GetBm25Statslogs(), "bm25 stats binlog", segment.GetSegmentId()); err != nil {
		return err
	}
	if err := validateSnapshotIndexFiles(segment.GetIndexFiles(), segment.GetSegmentId()); err != nil {
		return err
	}
	if err := validateSnapshotTextIndexes(segment.GetTextIndexFiles(), segment.GetSegmentId()); err != nil {
		return err
	}
	return validateSnapshotJSONKeyIndexes(segment.GetJsonKeyIndexFiles(), segment.GetSegmentId())
}

func validateSnapshotFieldBinlogs(fieldBinlogs []*datapb.FieldBinlog, fileType string, segmentID int64) error {
	for fieldIdx, fieldBinlog := range fieldBinlogs {
		if fieldBinlog == nil {
			return fmt.Errorf("%s segment %d field binlog at index %d cannot be nil", fileType, segmentID, fieldIdx)
		}
		for binlogIdx, binlog := range fieldBinlog.Binlogs {
			if binlog == nil {
				return fmt.Errorf("%s segment %d field %d binlog at index %d cannot be nil", fileType, segmentID, fieldBinlog.GetFieldID(), binlogIdx)
			}
		}
	}
	return nil
}

func validateSnapshotIndexFiles(indexFiles []*indexpb.IndexFilePathInfo, segmentID int64) error {
	for i, indexFile := range indexFiles {
		if indexFile == nil {
			return fmt.Errorf("index file segment %d entry at index %d cannot be nil", segmentID, i)
		}
	}
	return nil
}

func validateSnapshotTextIndexes(indexes map[int64]*datapb.TextIndexStats, segmentID int64) error {
	for fieldID, index := range indexes {
		if index == nil {
			return fmt.Errorf("text index segment %d field %d cannot be nil", segmentID, fieldID)
		}
	}
	return nil
}

func validateSnapshotJSONKeyIndexes(indexes map[int64]*datapb.JsonKeyStats, segmentID int64) error {
	for fieldID, index := range indexes {
		if index == nil {
			return fmt.Errorf("json key index segment %d field %d cannot be nil", segmentID, fieldID)
		}
	}
	return nil
}

func WriteSnapshotWithMapping(ctx context.Context, cm storage.ChunkManager, snapshot *SnapshotData, mappings map[string]string, opts SnapshotRewriteOptions) (string, error) {
	rewritten, err := RewriteSnapshotWithMapping(cm, snapshot, mappings, opts)
	if err != nil {
		return "", err
	}
	metadataURI := opts.MetadataURI
	if metadataURI == "" {
		metadataURI = joinSnapshotURI(opts.TargetRoot,
			SnapshotRootPath,
			fmt.Sprintf("%d", rewritten.SnapshotInfo.GetCollectionId()),
			SnapshotMetadataSubPath,
			fmt.Sprintf("%d.json", rewritten.SnapshotInfo.GetId()))
		rewritten.SnapshotInfo.S3Location = metadataURI
	}
	if _, err := NewSnapshotWriter(cm).SaveToRoot(ctx, rewritten, opts.TargetRoot, datapb.SnapshotLayout_SnapshotLayoutSelfContained); err != nil {
		return "", err
	}
	return metadataURI, nil
}

type snapshotPathRewriter struct {
	cm       storage.ChunkManager
	mappings map[string]string
	opts     SnapshotRewriteOptions
}

func (r snapshotPathRewriter) rewriteSegment(segment *datapb.SegmentDescription) error {
	if segment.GetStorageVersion() >= storage.StorageV3 {
		segment.Binlogs = nil
		if err := r.rewriteStorageV3Manifest(segment); err != nil {
			return err
		}
	} else {
		if err := r.rewriteFieldBinlogs(segment.GetBinlogs(), "insert binlog", segment.GetSegmentId()); err != nil {
			return err
		}
	}
	if err := r.rewriteFieldBinlogs(segment.GetStatslogs(), "stats binlog", segment.GetSegmentId()); err != nil {
		return err
	}
	if err := r.rewriteFieldBinlogs(segment.GetDeltalogs(), "delta binlog", segment.GetSegmentId()); err != nil {
		return err
	}
	if err := r.rewriteFieldBinlogs(segment.GetBm25Statslogs(), "bm25 stats binlog", segment.GetSegmentId()); err != nil {
		return err
	}
	if err := r.rewriteIndexFiles(segment.GetIndexFiles(), segment.GetSegmentId()); err != nil {
		return err
	}
	if err := r.rewriteTextIndexes(segment.GetTextIndexFiles(), segment.GetSegmentId()); err != nil {
		return err
	}
	if err := r.rewriteJSONKeyIndexes(segment.GetJsonKeyIndexFiles(), segment.GetSegmentId()); err != nil {
		return err
	}
	if segment.GetStorageVersion() < storage.StorageV3 && segment.GetManifestPath() != "" {
		if _, _, _, err := r.rewriteManifestPath(segment, "manifest root"); err != nil {
			return err
		}
	}
	return nil
}

func (r snapshotPathRewriter) rewriteStorageV3Manifest(segment *datapb.SegmentDescription) error {
	if segment.GetManifestPath() == "" {
		return fmt.Errorf("storage v3 segment %d requires manifest path", segment.GetSegmentId())
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
		return "", 0, "", fmt.Errorf("failed to parse manifest path for segment %d: %w", segment.GetSegmentId(), err)
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
		return fmt.Errorf("failed to list LOB files for segment %d: %w", segment.GetSegmentId(), err)
	}
	for _, info := range lobFileInfos {
		context := fmt.Sprintf("storage v3 lob file segment %d field %d", segment.GetSegmentId(), info.FieldID)
		normalizedSource := normalizeSnapshotObjectPath(r.cm, info.Path)
		sourceLOBRoot := normalizeSnapshotObjectPath(r.cm, storageV3LOBBasePath(sourceBasePath, info.FieldID))
		if !isSnapshotPathUnderRoot(normalizedSource, sourceLOBRoot) {
			return fmt.Errorf("%s %q is outside manifest LOB root %q", context, info.Path, sourceLOBRoot)
		}
		rewritten, err := r.rewritePath(info.Path, context)
		if err != nil {
			return err
		}
		rewrittenLOBRoot := normalizeSnapshotObjectPath(r.cm, storageV3LOBBasePath(rewrittenBasePath, info.FieldID))
		if !isSnapshotPathUnderRoot(normalizeSnapshotObjectPath(r.cm, rewritten), rewrittenLOBRoot) {
			return fmt.Errorf("%s rewritten path %q is outside rewritten manifest LOB root %q", context, rewritten, rewrittenLOBRoot)
		}
	}
	return nil
}

func storageV3LOBBasePath(manifestBasePath string, fieldID int64) string {
	return path.Join(path.Dir(manifestBasePath), "lobs", fmt.Sprintf("%d", fieldID))
}

func (r snapshotPathRewriter) rewriteFieldBinlogs(fieldBinlogs []*datapb.FieldBinlog, fileType string, segmentID int64) error {
	for fieldIdx, fieldBinlog := range fieldBinlogs {
		if fieldBinlog == nil {
			return fmt.Errorf("%s segment %d field binlog at index %d cannot be nil", fileType, segmentID, fieldIdx)
		}
		for binlogIdx, binlog := range fieldBinlog.GetBinlogs() {
			if binlog == nil {
				return fmt.Errorf("%s segment %d field %d binlog at index %d cannot be nil", fileType, segmentID, fieldBinlog.GetFieldID(), binlogIdx)
			}
			rewritten, err := r.rewritePath(binlog.GetLogPath(), fmt.Sprintf("%s segment %d field %d", fileType, segmentID, fieldBinlog.GetFieldID()))
			if err != nil {
				return err
			}
			binlog.LogPath = rewritten
		}
	}
	return nil
}

func (r snapshotPathRewriter) rewriteIndexFiles(indexFiles []*indexpb.IndexFilePathInfo, segmentID int64) error {
	for i, indexFile := range indexFiles {
		if indexFile == nil {
			return fmt.Errorf("index file segment %d entry at index %d cannot be nil", segmentID, i)
		}
		for i, filePath := range indexFile.GetIndexFilePaths() {
			rewritten, err := r.rewritePath(filePath, fmt.Sprintf("index file segment %d field %d build %d", segmentID, indexFile.GetFieldID(), indexFile.GetBuildID()))
			if err != nil {
				return err
			}
			indexFile.IndexFilePaths[i] = rewritten
		}
	}
	return nil
}

func (r snapshotPathRewriter) rewriteTextIndexes(indexes map[int64]*datapb.TextIndexStats, segmentID int64) error {
	for fieldID, index := range indexes {
		if index == nil {
			return fmt.Errorf("text index segment %d field %d cannot be nil", segmentID, fieldID)
		}
		if index.GetFieldID() != 0 {
			fieldID = index.GetFieldID()
		}
		for i, filePath := range index.GetFiles() {
			rewritten, err := r.rewritePath(filePath, fmt.Sprintf("text index segment %d field %d build %d", segmentID, fieldID, index.GetBuildID()))
			if err != nil {
				return err
			}
			index.Files[i] = rewritten
		}
	}
	return nil
}

func (r snapshotPathRewriter) rewriteJSONKeyIndexes(indexes map[int64]*datapb.JsonKeyStats, segmentID int64) error {
	for fieldID, index := range indexes {
		if index == nil {
			return fmt.Errorf("json key index segment %d field %d cannot be nil", segmentID, fieldID)
		}
		if index.GetFieldID() != 0 {
			fieldID = index.GetFieldID()
		}
		for i, filePath := range index.GetFiles() {
			rewritten, err := r.rewritePath(filePath, fmt.Sprintf("json key index segment %d field %d build %d", segmentID, fieldID, index.GetBuildID()))
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
	normalized := normalizeSnapshotObjectPath(r.cm, src)
	if dst, ok := r.mappings[normalized]; ok {
		return dst, nil
	}
	if r.opts.StrictMapping {
		return "", fmt.Errorf("missing snapshot file mapping for %s: %s", context, src)
	}
	return exportedSnapshotPath(r.cm, normalized, r.opts.TargetRoot), nil
}

func exportedSnapshotPath(cm storage.ChunkManager, src string, targetRoot string) string {
	root := strings.TrimSuffix(normalizeSnapshotObjectPath(cm, cm.RootPath()), "/")
	relative := src
	if root != "" {
		if src == root {
			relative = ""
		} else if strings.HasPrefix(src, root+"/") {
			relative = strings.TrimPrefix(src, root+"/")
		}
	}
	return path.Join(targetRoot, exportedSnapshotFilesPath, relative)
}

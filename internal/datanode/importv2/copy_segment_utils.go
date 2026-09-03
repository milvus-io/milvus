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

package importv2

import (
	"context"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// SegmentFiles organizes source files by type for copy operations.
// InsertBinlogs come from manifest (when storage_version >= StorageV3) or PB
// (otherwise). V3 text/JSON physical files are also manifest-owned; the PB
// fields remain metadata only.
type SegmentFiles struct {
	// From manifest (when storage_version >= StorageV3) or pb (when < StorageV3)
	InsertBinlogs []string

	// LOB files at partition level (only for StorageV3+ with TEXT fields)
	LobFiles []string

	// Always from PB
	DeltaBinlogs      []string
	StatsBinlogs      []string
	Bm25Binlogs       []string
	VectorScalarIndex []string

	// From PB before StorageV3; manifest-owned from StorageV3 onward.
	TextIndex    []string
	JSONKeyIndex []string
	JSONStats    []string
}

// copyObjectWithTimeout bounds one provider-managed copy operation. Retrying
// the whole call here is unsafe for asynchronous providers such as Azure.
func copyObjectWithTimeout(
	ctx context.Context,
	copier storage.CrossBucketCopier,
	sourceBucket string,
	sourceObject string,
	targetBucket string,
	targetObject string,
) error {
	timeout := paramtable.Get().DataNodeCfg.ImportCopyObjectTimeout.GetAsDuration(time.Second)
	copyCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	err := copier.CopyCrossBucket(copyCtx, sourceBucket, sourceObject, targetBucket, targetObject)
	if ctxErr := copyCtx.Err(); ctxErr != nil {
		return ctxErr
	}
	return err
}

// transformManifestPath replaces source IDs in manifest path with target IDs.
//
// Manifest path is a JSON string: {"ver": 2, "base_path": "files/insert_log/coll/part/seg"}
//
// Process:
// 1. Unmarshal JSON to get base_path and version
// 2. Replace collection/partition/segment IDs in base_path
// 3. Marshal back to JSON
func transformManifestPath(
	manifestPath string,
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
) (string, error) {
	basePath, version, err := packed.UnmarshalManifestPath(manifestPath)
	if err != nil {
		return "", merr.Wrap(err, "failed to unmarshal manifest path")
	}

	targetBasePath, err := generateTargetPath(basePath, source, target)
	if err != nil {
		return "", merr.Wrap(err, "failed to generate target base path")
	}

	targetManifestPath := packed.MarshalManifestPath(targetBasePath, version)
	return targetManifestPath, nil
}

// excludeUnpinnedManifestRevisions drops every manifest revision of the source
// segment except the one the snapshot pinned.
//
// A snapshot pins one manifest version, but the source segment keeps evolving
// afterwards (compaction, index build, stats), so by copy time its manifest
// directory can hold revisions newer than the pinned one. Those newer revisions
// describe state the snapshot never included and whose files this copy does not
// carry over.
//
// Carrying them to the target is not merely wasteful, it is unsound: loon
// discovers the current version by listing the manifest directory and taking
// the highest revision number, and a commit whose read version is behind that
// resolves against the highest revision and writes one past it. The target's
// next manifest commit would therefore merge onto the SOURCE's post-snapshot
// state and publish it as the target's own. Keeping only the pinned revision
// makes the target's history start exactly where the snapshot ended.
func excludeUnpinnedManifestRevisions(files []string, pinnedManifest string) []string {
	pinnedName := path.Base(pinnedManifest)
	kept := make([]string, 0, len(files))
	for _, file := range files {
		if packed.IsManifestRevisionObject(file) && path.Base(file) != pinnedName {
			continue
		}
		kept = append(kept, file)
	}
	return kept
}

// listAllFiles recursively lists all files under the given path using WalkWithPrefix.
// Returns (nil, error) if the walk fails.
func listAllFiles(ctx context.Context, cm storage.ChunkManager, basePath string) ([]string, error) {
	var files []string
	walkPrefix := basePath
	if !strings.HasSuffix(walkPrefix, "/") {
		walkPrefix += "/"
	}
	err := cm.WalkWithPrefix(ctx, walkPrefix, true, func(info *storage.ChunkObjectInfo) bool {
		files = append(files, info.FilePath)
		return true
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}

// extractFromPb extracts file paths from FieldBinlog list (insert/delta/stats/bm25).
func extractFromPb(fieldBinlogs []*datapb.FieldBinlog) []string {
	var paths []string
	for _, fieldBinlog := range fieldBinlogs {
		for _, binlog := range fieldBinlog.GetBinlogs() {
			if path := binlog.GetLogPath(); path != "" {
				paths = append(paths, path)
			}
		}
	}
	return paths
}

// extractIndexFiles extracts vector/scalar index file paths.
func extractIndexFiles(indexInfos []*indexpb.IndexFilePathInfo) []string {
	var paths []string
	for _, info := range indexInfos {
		paths = append(paths, info.GetIndexFilePaths()...)
	}
	return paths
}

func buildIndexPathVersionByFile(source *datapb.CopySegmentSource) map[string]indexpb.IndexStorePathVersion {
	versions := make(map[string]indexpb.IndexStorePathVersion)
	for _, indexInfo := range source.GetIndexFiles() {
		for _, filePath := range indexInfo.GetIndexFilePaths() {
			versions[filePath] = indexInfo.GetIndexStorePathVersion()
		}
	}
	return versions
}

// extractTextIndexFiles extracts text index file paths.
func extractTextIndexFiles(textIndexInfos map[int64]*datapb.TextIndexStats) []string {
	var paths []string
	for _, info := range textIndexInfos {
		paths = append(paths, info.GetFiles()...)
	}
	return paths
}

// extractJSONFiles extracts JSON index files, separated by data format version.
// Returns (jsonKeyFiles, jsonStatsFiles).
func extractJSONFiles(jsonIndexInfos map[int64]*datapb.JsonKeyStats) ([]string, []string) {
	var jsonKeyFiles []string
	var jsonStatsFiles []string

	for _, info := range jsonIndexInfos {
		dataFormat := info.GetJsonKeyStatsDataFormat()
		files := info.GetFiles()

		if dataFormat < 2 {
			// Legacy format (< v2) -> JSON Key Index
			jsonKeyFiles = append(jsonKeyFiles, files...)
		} else {
			// New format (>= v2) -> JSON Stats
			jsonStatsFiles = append(jsonStatsFiles, files...)
		}
	}

	return jsonKeyFiles, jsonStatsFiles
}

// collectSegmentFiles collects all files to copy, organized by type.
//
// For InsertBinlogs, the decision is based on storage_version:
//   - storage_version >= StorageV3 (3): MUST resolve from manifest_path.
//     manifest_path missing → error. Listing fails → error. Empty file list → OK (no binlogs).
//   - storage_version < StorageV3: use pb paths (traditional non-packed format).
//
// Delta/stats/BM25 and vector/scalar indexes still come from PB. Text/JSON
// physical files come from PB only before StorageV3.
func collectSegmentFiles(
	ctx context.Context,
	sourceCM storage.ChunkManager,
	sourceStorageConfig *indexpb.StorageConfig,
	source *datapb.CopySegmentSource,
) (*SegmentFiles, error) {
	files := &SegmentFiles{}

	if source.GetStorageVersion() >= storage.StorageV3 {
		// StorageV3+: binlog paths MUST come from manifest
		manifestPath := source.GetManifestPath()
		if manifestPath == "" {
			return nil, merr.WrapErrParameterInvalidMsg("storage_version=%d requires manifest_path but it is empty (segmentID=%d)",
				source.GetStorageVersion(), source.GetSegmentId())
		}

		basePath, _, err := packed.UnmarshalManifestPath(manifestPath)
		if err != nil {
			return nil, merr.Wrapf(err, "failed to unmarshal manifest path %q for segment %d", manifestPath, source.GetSegmentId())
		}
		basePath = snapshotstorage.NormalizeSnapshotObjectPath(basePath)

		allFiles, listErr := listAllFiles(ctx, sourceCM, basePath)
		if listErr != nil {
			return nil, merr.Wrapf(listErr, "failed to list files from manifest base path %q for segment %d", basePath, source.GetSegmentId())
		}
		pinnedManifest, pinErr := packed.ManifestFilePath(manifestPath)
		if pinErr != nil {
			return nil, merr.Wrapf(pinErr, "failed to resolve pinned manifest object for segment %d", source.GetSegmentId())
		}
		allFiles = excludeUnpinnedManifestRevisions(allFiles, pinnedManifest)

		// Empty file list is OK for V3 — segment may have only deltas and no insert binlogs
		files.InsertBinlogs = allFiles
		mlog.Info(context.TODO(), "collected InsertBinlogs from manifest",
			mlog.String("basePath", basePath),
			mlog.Int("fileCount", len(allFiles)),
			mlog.Int64("storageVersion", source.GetStorageVersion()))

		// Collect LOB files owned by THIS segment from the manifest.
		// LOB files live at partition level ({root}/insert_log/{coll}/{part}/lobs/),
		// but multiple segments share that directory. We must only copy the files
		// referenced by this segment's manifest to preserve the invariant that
		// each LOB file belongs to exactly one segment.
		lobFileInfos, lobErr := packed.GetManifestLobFiles(manifestPath, sourceStorageConfig)
		if lobErr != nil {
			return nil, merr.Wrapf(lobErr, "failed to collect LOB files from manifest for segment %d", source.GetSegmentId())
		} else if len(lobFileInfos) > 0 {
			// GetManifestLobFiles returns absolute paths (the manifest
			// deserializer calls ToAbsolute internally), so use them directly.
			files.LobFiles = lobFileInfosToPaths(lobFileInfos)
			mlog.Info(context.TODO(), "collected LOB files from segment manifest",
				mlog.String("manifestPath", manifestPath),
				mlog.Int("lobFileCount", len(files.LobFiles)))
		}
	} else {
		// StorageV1/V2: use pb paths (traditional non-packed format)
		files.InsertBinlogs = extractFromPb(source.GetInsertBinlogs())
		mlog.Info(context.TODO(), "using InsertBinlogs from pb",
			mlog.Int("fileCount", len(files.InsertBinlogs)),
			mlog.Int64("storageVersion", source.GetStorageVersion()))
	}

	// Other types from pb
	files.DeltaBinlogs = extractFromPb(source.GetDeltaBinlogs())
	files.StatsBinlogs = extractFromPb(source.GetStatsBinlogs())
	files.Bm25Binlogs = extractFromPb(source.GetBm25Binlogs())
	files.VectorScalarIndex = extractIndexFiles(source.GetIndexFiles())

	// For V3, text/json stats files live under basePath/_stats/ and are already
	// included in InsertBinlogs via listAllFiles(). Skip pb extraction to avoid
	// using potentially stale or wrong-format paths from etcd metadata.
	if source.GetStorageVersion() < storage.StorageV3 {
		files.TextIndex = extractTextIndexFiles(source.GetTextIndexFiles())
		files.JSONKeyIndex, files.JSONStats = extractJSONFiles(source.GetJsonKeyIndexFiles())
	}

	return files, nil
}

// generateMappingsFromFiles generates file copy mappings from SegmentFiles.
// Each source file path is transformed to target path by replacing collection/partition/segment IDs.
func generateMappingsFromFiles(
	files *SegmentFiles,
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
) (map[string]string, error) {
	mappings := make(map[string]string)
	indexPathVersions := buildIndexPathVersionByFile(source)

	// Helper to add mappings with error handling
	addMappings := func(srcPaths []string, fileType string) error {
		for _, srcPath := range srcPaths {
			var dstPath string
			var err error

			// Determine path generation logic based on file type
			switch fileType {
			case IndexTypeVectorScalarV0, IndexTypeText, IndexTypeJSONKey, IndexTypeJSONStats:
				dstPath, err = generateTargetIndexPath(srcPath, source, target, fileType, indexPathVersions[srcPath])
			case FileTypeLOB:
				dstPath, err = generateTargetLOBPath(srcPath, source, target)
			default:
				dstPath, err = generateTargetPath(srcPath, source, target)
			}

			if err != nil {
				return merr.Wrapf(err, "failed to generate target path for %s file %s", fileType, srcPath)
			}
			mappings[srcPath] = dstPath
		}
		return nil
	}

	// Generate mappings for all file types
	if err := addMappings(files.InsertBinlogs, BinlogTypeInsert); err != nil {
		return nil, err
	}
	if err := addMappings(files.DeltaBinlogs, BinlogTypeDelta); err != nil {
		return nil, err
	}
	if err := addMappings(files.StatsBinlogs, BinlogTypeStats); err != nil {
		return nil, err
	}
	if err := addMappings(files.Bm25Binlogs, BinlogTypeBM25); err != nil {
		return nil, err
	}
	// Vector/scalar index copy uses the v0 type as the logical input; the
	// per-file IndexStorePathVersion switches storage matching to index_v1 when needed.
	if err := addMappings(files.VectorScalarIndex, IndexTypeVectorScalarV0); err != nil {
		return nil, err
	}
	if err := addMappings(files.TextIndex, IndexTypeText); err != nil {
		return nil, err
	}
	if err := addMappings(files.JSONKeyIndex, IndexTypeJSONKey); err != nil {
		return nil, err
	}
	if err := addMappings(files.JSONStats, IndexTypeJSONStats); err != nil {
		return nil, err
	}
	if err := addMappings(files.LobFiles, FileTypeLOB); err != nil {
		return nil, err
	}
	if source.GetStorageVersion() == storage.StorageV2 && source.GetManifestPath() != "" {
		if err := addMappings([]string{source.GetManifestPath()}, BinlogTypeInsert); err != nil {
			return nil, err
		}
	}

	return mappings, nil
}

// CopySegmentAndIndexFiles copies all segment files and index files sequentially.
func CopySegmentAndIndexFiles(
	ctx context.Context,
	sourceCM storage.ChunkManager,
	sourceStorageConfig *indexpb.StorageConfig,
	targetStorageConfig *indexpb.StorageConfig,
	copier storage.CrossBucketCopier,
	sourceBucket string,
	targetBucket string,
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
	logFields []mlog.Field,
) (*datapb.CopySegmentResult, []string, error) {
	if copier == nil {
		return nil, nil, merr.WrapErrServiceInternalMsg("cross-bucket copier is nil")
	}

	segmentID := source.GetSegmentId()
	useManifest := source.GetStorageVersion() >= storage.StorageV3

	mlog.Info(context.TODO(), "start copying segment and index files",
		mlog.Int64("sourceSegmentID", segmentID),
		mlog.Int64("storageVersion", source.GetStorageVersion()),
		mlog.Bool("useManifest", useManifest),
		mlog.Bool("isExternalCollection", source.GetIsExternalCollection()))

	// Step 1: Collect all files to copy
	files, err := collectSegmentFiles(ctx, sourceCM, sourceStorageConfig, source)
	if err != nil {
		return nil, nil, merr.Wrap(err, "failed to collect segment files")
	}

	// Step 2: Generate src->dst mappings for file copying
	mappings, err := generateMappingsFromFiles(files, source, target)
	if err != nil {
		return nil, nil, merr.Wrap(err, "failed to generate file mappings")
	}
	// Step 3: Execute all copy operations
	copiedFiles := make([]string, 0, len(mappings))
	for src, dst := range mappings {
		copySource := snapshotstorage.NormalizeSnapshotObjectPath(src)
		mlog.Debug(context.TODO(), "copying file",
			mlog.String("src", snapshotstorage.RedactSnapshotObjectPath(src)),
			mlog.String("dst", dst))

		if err := copyObjectWithTimeout(ctx, copier, sourceBucket, copySource, targetBucket, dst); err != nil {
			fields := make([]mlog.Field, 0, len(logFields)+3)
			fields = append(fields, logFields...)
			fields = append(fields, mlog.String("src", snapshotstorage.RedactSnapshotObjectPath(src)), mlog.String("dst", dst), mlog.Err(err))
			mlog.Warn(context.TODO(), "failed to copy file", fields...)
			return nil, copiedFiles, merr.Wrapf(err, "failed to copy file from %s to %s", snapshotstorage.RedactSnapshotObjectPath(src), dst)
		}
		copiedFiles = append(copiedFiles, dst)
	}

	mlog.Info(context.TODO(), "all files copied successfully",
		mlog.Int("fileCount", len(mappings)))

	// Step 3.5: When manifest is used (StorageV3+), InsertBinlogs were collected from manifest
	// (actual file paths under base_path including _data/ and _metadata/), but
	// generateSegmentInfoFromSource needs mappings for the protobuf logical paths too.
	// Add these "logical-only" mappings AFTER file copying so they don't trigger actual copy operations.
	if useManifest {
		pbInsertPaths := extractFromPb(source.GetInsertBinlogs())
		for _, srcPath := range pbInsertPaths {
			if _, exists := mappings[srcPath]; !exists {
				dstPath, pathErr := generateTargetPath(srcPath, source, target)
				if pathErr != nil {
					return nil, copiedFiles, merr.Wrapf(pathErr, "failed to generate target path for pb insert binlog %s", srcPath)
				}
				mappings[srcPath] = dstPath
			}
		}
		mlog.Info(context.TODO(), "added logical insert binlog mappings for manifest segment",
			mlog.Int("pbPathCount", len(pbInsertPaths)))
	}

	// Step 4: Build index metadata from source
	indexInfos, textIndexInfos, jsonKeyIndexInfos, err := buildIndexInfoFromSource(source, target, mappings)
	if err != nil {
		return nil, copiedFiles, merr.Wrap(err, "failed to build index info")
	}

	// Step 5: Generate segment metadata with path mappings
	segmentInfo, err := generateSegmentInfoFromSource(source, target, mappings)
	if err != nil {
		return nil, copiedFiles, merr.Wrap(err, "failed to generate segment info")
	}

	// Step 6: Compress paths
	err = binlog.CompressBinLogs(segmentInfo.GetBinlogs(), segmentInfo.GetStatslogs(),
		segmentInfo.GetDeltalogs(), segmentInfo.GetBm25Logs())
	if err != nil {
		return nil, copiedFiles, merr.Wrap(err, "failed to compress binlog paths")
	}

	// Step 7: Publish the target manifest (StorageV3+).
	//
	// This must run before step 8, which rewrites indexInfos[].IndexFilePaths
	// IN PLACE - the same structs read here. A manifest index entry stores an
	// artifact as a directory plus the file names within it, so it is derived
	// while those paths are still the full ones this copy wrote; after step 8
	// each is a bare file name and no entry could name a directory.
	var (
		targetManifestPath    string
		manifestIndexBuildIDs []int64
	)
	if useManifest {
		targetManifestPath, err = transformManifestPath(source.GetManifestPath(), source, target)
		if err != nil {
			return nil, copiedFiles, merr.Wrap(err, "failed to transform manifest path")
		}
		sourceManifestKnownEmpty := source.ManifestHasIndex != nil && !source.GetManifestHasIndex()
		targetManifestPath, manifestIndexBuildIDs, err = republishCopiedManifestIndexes(
			targetManifestPath, target, source.GetNumOfRows(), targetStorageConfig, indexInfos,
			sourceManifestKnownEmpty)
		if err != nil {
			return nil, copiedFiles, merr.Wrap(err, "failed to republish copied manifest indexes")
		}
	}

	// Step 8: Shorten index and JSON stats paths for the DataCoord-facing
	// result. Both rewrite their inputs in place - see step 7.
	for _, indexInfo := range indexInfos {
		indexInfo.IndexFilePaths = shortenIndexFilePaths(indexInfo.IndexFilePaths)
	}

	jsonKeyIndexInfos = shortenJSONStatsPath(jsonKeyIndexInfos)

	mlog.Info(context.TODO(), "path compression completed",
		mlog.Int("binlogFields", len(segmentInfo.GetBinlogs())),
		mlog.Int("indexCount", len(indexInfos)),
		mlog.Int("jsonStatsCount", len(jsonKeyIndexInfos)))

	// Step 9: Build result
	result := &datapb.CopySegmentResult{
		SegmentId:         segmentInfo.GetSegmentID(),
		ImportedRows:      segmentInfo.GetImportedRows(),
		Binlogs:           segmentInfo.GetBinlogs(),
		Statslogs:         segmentInfo.GetStatslogs(),
		Deltalogs:         segmentInfo.GetDeltalogs(),
		Bm25Logs:          segmentInfo.GetBm25Logs(),
		IndexInfos:        indexInfos,
		TextIndexInfos:    textIndexInfos,
		JsonKeyIndexInfos: jsonKeyIndexInfos,
	}

	// Step 10: Propagate manifest_path. The StorageV3 pointer was already
	// produced by step 7, before index paths were shortened.
	if useManifest {
		result.ManifestPath = targetManifestPath
		result.ManifestIndexRewritten = proto.Bool(true)
		result.ManifestIndexBuildIds = manifestIndexBuildIDs
	} else if source.GetStorageVersion() == storage.StorageV2 && source.GetManifestPath() != "" {
		targetManifestPath, ok := mappings[source.GetManifestPath()]
		if !ok {
			return nil, copiedFiles, merr.WrapErrDataIntegrityMsg(
				"missing copied StorageV2 manifest mapping for segment %d",
				source.GetSegmentId(),
			)
		}
		result.ManifestPath = targetManifestPath
	}

	mlog.Info(context.TODO(), "copy segment and index files completed successfully",
		mlog.Int64("importedRows", result.ImportedRows))

	return result, copiedFiles, nil
}

// republishCopiedManifestIndexes rewrites the index entries of the manifest this
// copy produced, and returns the pointer to publish.
//
// The manifest object is copied byte-for-byte from the source and its pointer is
// merely re-based onto the target path. That is faithful for everything stored
// relative to the segment's base path - column groups and their per-file
// properties, stats, LOB - which is why the copy preserves them exactly. It is
// NOT faithful for index entries: an index artifact lives outside the segment
// directory, so its stored relative path walks back out of the base and thereby
// hardcodes the SOURCE collection/partition/segment/build IDs. Re-basing the
// pointer moves where that walk starts but not the IDs it encodes, so an
// inherited entry keeps pointing at the source's artifacts.
//
// Every inherited entry is therefore dropped and re-derived from the artifacts
// this copy actually wrote, in one transaction on top of the copied manifest, so
// the pointer DataCoord publishes is already correct and needs no second commit.
//
// The entries to drop are enumerated from the copied manifest itself unless
// the snapshot's sticky marker proves the index section empty. The manifest
// object is already in the target bucket, so enumeration also works for an
// external restore where DataCoord cannot read the source bucket.
func republishCopiedManifestIndexes(
	targetManifestPath string,
	target *datapb.CopySegmentTarget,
	numRows int64,
	targetStorageConfig *indexpb.StorageConfig,
	indexInfos map[int64]*datapb.VectorScalarIndexInfo,
	sourceManifestKnownEmpty bool,
) (string, []int64, error) {
	var drops []packed.DropIndexEntry
	if !sourceManifestKnownEmpty {
		existing, err := packed.GetManifestIndexInfos(targetManifestPath, targetStorageConfig)
		if err != nil {
			return "", nil, merr.Wrap(err, "failed to enumerate copied manifest index entries")
		}
		existingIDs := make(map[int64]struct{}, len(existing))
		drops = make([]packed.DropIndexEntry, 0, len(existing))
		for _, entry := range existing {
			if _, seen := existingIDs[entry.IndexID]; seen {
				// milvus-storage drops every entry whose index_id matches, so one
				// drop per unique ID covers duplicates.
				continue
			}
			existingIDs[entry.IndexID] = struct{}{}
			// No ExpectedBuildID: the target segment is freshly created and
			// exclusively owned by this task, so no rebuild can race this drop.
			drops = append(drops, packed.DropIndexEntry{IndexID: entry.IndexID})
		}
	}
	adds, err := buildTargetManifestIndexes(targetManifestPath, target, numRows, indexInfos)
	if err != nil {
		return "", nil, err
	}
	publishedBuildIDs := make([]int64, 0, len(adds))
	for _, entry := range adds {
		publishedBuildIDs = append(publishedBuildIDs, entry.BuildID)
	}
	if len(drops) == 0 && len(adds) == 0 {
		return targetManifestPath, publishedBuildIDs, nil
	}

	basePath, version, err := packed.UnmarshalManifestPath(targetManifestPath)
	if err != nil {
		return "", nil, merr.Wrap(err, "failed to parse copied manifest path")
	}
	republished, err := packed.CommitManifestUpdates(basePath, version, targetStorageConfig,
		&packed.ManifestUpdates{DropIndexes: drops, Indexes: adds})
	if err != nil {
		return "", nil, merr.Wrap(err, "failed to commit copied manifest indexes")
	}
	mlog.Info(context.TODO(), "republished copied segment manifest indexes",
		mlog.Int64("targetSegmentID", target.GetSegmentId()),
		mlog.Int("droppedInheritedEntries", len(drops)),
		mlog.Int("addedEntries", len(adds)),
		mlog.String("manifestPath", republished))
	return republished, publishedBuildIDs, nil
}

// buildTargetManifestIndexes projects the index artifacts this copy wrote into
// manifest entries for the target segment.
//
// The worker owns the physical facts (where the artifact landed, its build ID,
// sizes, engine versions); DataCoord owns the identity and the index definition
// and ships them in CopySegmentTarget.target_indexes. Index identity cannot be
// inherited from the source: RestoreIndexes() allocates fresh index IDs for the
// restored collection, so index name is the only key that survives the snapshot
// boundary.
func buildTargetManifestIndexes(
	targetManifestPath string,
	target *datapb.CopySegmentTarget,
	numRows int64,
	indexInfos map[int64]*datapb.VectorScalarIndexInfo,
) ([]packed.ManifestIndexInfo, error) {
	if len(indexInfos) == 0 {
		return nil, nil
	}
	basePath, _, err := packed.UnmarshalManifestPath(targetManifestPath)
	if err != nil {
		return nil, merr.Wrap(err, "failed to parse copied manifest path")
	}
	targetIndexes := make(map[string]*datapb.CopySegmentTargetIndex, len(target.GetTargetIndexes()))
	for _, definition := range target.GetTargetIndexes() {
		if definition != nil {
			targetIndexes[definition.GetIndexName()] = definition
		}
	}

	entries := make([]packed.ManifestIndexInfo, 0, len(indexInfos))
	for _, info := range indexInfos {
		definition, ok := targetIndexes[info.GetIndexName()]
		if !ok {
			// An index definition that did not survive the restore has no target
			// to be recorded under. DataCoord's syncVectorScalarIndexes logs and
			// skips the same case.
			mlog.Warn(context.TODO(), "copied index has no target definition, skipping manifest entry",
				mlog.Int64("targetSegmentID", target.GetSegmentId()),
				mlog.String("indexName", info.GetIndexName()))
			continue
		}
		if len(info.GetIndexFilePaths()) == 0 {
			// An index record that names no artifact - a build too small to
			// produce files, or an index whose files a snapshot never captured -
			// has nothing to record in a manifest. It reaches here because
			// uncompressIndexFiles emits a record per finished SegmentIndex
			// regardless of its file keys, and the rest of the copy path has
			// always carried it through harmlessly: syncVectorScalarIndexes
			// installs it with empty IndexFileKeys. Failing the whole copy on it
			// here would turn a benign empty record into a failed restore.
			mlog.Warn(context.TODO(), "copied index carries no artifact path, skipping manifest entry",
				mlog.Int64("targetSegmentID", target.GetSegmentId()),
				mlog.String("indexName", info.GetIndexName()))
			continue
		}
		// Derive the entry from the paths the copy actually wrote rather than
		// from a rebuilt prefix, so it cannot drift from the real layout.
		indexPrefix, fileKeys, err := splitIndexArtifactPaths(info.GetIndexFilePaths())
		if err != nil {
			return nil, merr.Wrapf(err, "index %s of copied segment %d", info.GetIndexName(), target.GetSegmentId())
		}
		relativePath, err := packed.ManifestIndexRelativePath(basePath, indexPrefix)
		if err != nil {
			return nil, err
		}
		properties := make(map[string]string, len(definition.GetProperties())+1)
		for key, value := range definition.GetProperties() {
			properties[key] = value
		}
		properties[common.IndexTypeKey] = definition.GetIndexType()
		entries = append(entries, packed.ManifestIndexInfo{
			ColumnName:                definition.GetColumnName(),
			IndexName:                 info.GetIndexName(),
			IndexType:                 definition.GetIndexType(),
			Path:                      relativePath,
			FieldID:                   definition.GetFieldId(),
			IndexID:                   definition.GetIndexId(),
			BuildID:                   info.GetBuildId(),
			IndexVersion:              info.GetVersion(),
			NumRows:                   numRows,
			SerializedSize:            info.GetIndexSize(),
			MemSize:                   info.GetIndexSize(),
			CurrentIndexVersion:       info.GetCurrentIndexVersion(),
			CurrentScalarIndexVersion: info.GetCurrentScalarIndexVersion(),
			IndexStorePathVersion:     info.GetIndexStorePathVersion(),
			IndexFileKeys:             fileKeys,
			Properties:                properties,
		})
	}
	return entries, nil
}

// splitIndexArtifactPaths separates a copied index's object-storage paths into
// the single directory holding them and the plain file names within it. A
// manifest index entry stores exactly that shape, and every reader rebuilds a
// full path by joining the two, so paths spread across directories cannot be
// represented and are rejected instead of silently truncated.
// The caller rejects an empty list before calling; the guard here keeps that a
// precondition rather than a silent empty prefix.
func splitIndexArtifactPaths(filePaths []string) (string, []string, error) {
	if len(filePaths) == 0 {
		return "", nil, merr.WrapErrServiceInternalMsg("copied index carries no artifact path")
	}
	var dir string
	fileKeys := make([]string, 0, len(filePaths))
	for i, filePath := range filePaths {
		fileDir, fileName := path.Split(path.Clean(filePath))
		fileDir = path.Clean(fileDir)
		if fileName == "" || fileDir == "." || fileDir == "/" {
			return "", nil, merr.WrapErrServiceInternalMsg("copied index artifact path %q has no directory", filePath)
		}
		if i == 0 {
			dir = fileDir
		} else if fileDir != dir {
			return "", nil, merr.WrapErrServiceInternalMsg(
				"copied index artifacts span directories %q and %q", dir, fileDir)
		}
		fileKeys = append(fileKeys, fileName)
	}
	return dir, fileKeys, nil
}

// transformFieldBinlogs transforms source FieldBinlog list to destination by replacing paths
// using the pre-calculated mappings, while preserving all other metadata.
//
// This function is used to build the segment metadata that DataCoord needs for tracking
// the imported segment. All source binlog metadata is preserved except for the file paths,
// which are replaced using the mappings generated during the copy operation.
//
// Parameters:
//   - srcFieldBinlogs: Source field binlogs with original paths
//   - mappings: Pre-calculated map of source path -> target path
//   - countRows: If true, accumulate total row count from EntriesNum (for insert logs only)
//   - isExternalTable: If true, skip path mapping because external table insert
//     binlogs carry row metadata without physical log paths
//
// Returns:
//   - []*datapb.FieldBinlog: Transformed binlog list with target paths
//   - int64: Total row count (sum of EntriesNum from all binlogs if countRows=true, 0 otherwise)
//   - error: Non-nil if any source path has no mapping (fail-fast on missing mappings)
func transformFieldBinlogs(
	srcFieldBinlogs []*datapb.FieldBinlog,
	mappings map[string]string,
	countRows bool,
	isExternalTable bool,
) ([]*datapb.FieldBinlog, int64, error) {
	result := make([]*datapb.FieldBinlog, 0, len(srcFieldBinlogs))
	var totalRows int64

	for _, srcFieldBinlog := range srcFieldBinlogs {
		dstFieldBinlog := proto.Clone(srcFieldBinlog).(*datapb.FieldBinlog)
		dstFieldBinlog.Binlogs = make([]*datapb.Binlog, 0, len(srcFieldBinlog.GetBinlogs()))

		for _, srcBinlog := range srcFieldBinlog.GetBinlogs() {
			dstBinlog := proto.Clone(srcBinlog).(*datapb.Binlog)

			if !isExternalTable {
				srcPath := srcBinlog.GetLogPath()
				if srcPath == "" {
					continue
				}
				dstPath, ok := mappings[srcPath]
				if !ok {
					return nil, 0, merr.WrapErrServiceInternalMsg("no mapping found for source path: %s", srcPath)
				}
				dstBinlog.LogPath = dstPath
			}

			dstFieldBinlog.Binlogs = append(dstFieldBinlog.Binlogs, dstBinlog)
			if countRows {
				totalRows += srcBinlog.GetEntriesNum()
			}
		}

		if len(dstFieldBinlog.Binlogs) > 0 {
			result = append(result, dstFieldBinlog)
		}
	}

	return result, totalRows, nil
}

// generateSegmentInfoFromSource generates ImportSegmentInfo from CopySegmentSource
// by transforming all binlog paths and preserving metadata.
//
// This function constructs the complete segment metadata that DataCoord uses to track
// the imported segment. It processes all four types of binlogs:
//   - Insert binlogs (required): Contains row data, row count is summed for ImportedRows
//   - Stats binlogs (optional): Contains statistics like min/max values
//   - Delta binlogs (optional): Contains delete operations
//   - BM25 binlogs (optional): Contains BM25 index data
//
// All source binlog metadata (EntriesNum, TimestampFrom, TimestampTo, LogSize) is preserved
// to maintain data integrity and enable proper query/compaction operations.
//
// Parameters:
//   - source: Source segment with original binlog paths and metadata
//   - target: Target IDs (collection/partition/segment) for segment identification
//   - mappings: Pre-calculated path mappings (source -> target)
//
// Returns:
//   - *datapb.ImportSegmentInfo: Complete segment metadata with target paths and row counts
//   - error: Error if any binlog transformation fails
func generateSegmentInfoFromSource(
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
	mappings map[string]string,
) (*datapb.ImportSegmentInfo, error) {
	segmentInfo := &datapb.ImportSegmentInfo{
		SegmentID:    target.GetSegmentId(),
		ImportedRows: 0,
		Binlogs:      []*datapb.FieldBinlog{},
		Statslogs:    []*datapb.FieldBinlog{},
		Deltalogs:    []*datapb.FieldBinlog{},
		Bm25Logs:     []*datapb.FieldBinlog{},
	}

	// Process insert binlogs (count rows)
	binlogs, totalRows, err := transformFieldBinlogs(source.GetInsertBinlogs(), mappings, true, source.GetIsExternalCollection())
	if err != nil {
		return nil, merr.Wrap(err, "failed to transform insert binlogs")
	}
	segmentInfo.Binlogs = binlogs
	segmentInfo.ImportedRows = totalRows
	if source.GetStorageVersion() >= storage.StorageV3 && source.GetNumOfRows() > 0 {
		segmentInfo.ImportedRows = source.GetNumOfRows()
	}

	// Process stats binlogs (no row counting)
	statslogs, _, err := transformFieldBinlogs(source.GetStatsBinlogs(), mappings, false, false)
	if err != nil {
		return nil, merr.Wrap(err, "failed to transform stats binlogs")
	}
	segmentInfo.Statslogs = statslogs

	// Process delta binlogs (no row counting)
	deltalogs, _, err := transformFieldBinlogs(source.GetDeltaBinlogs(), mappings, false, false)
	if err != nil {
		return nil, merr.Wrap(err, "failed to transform delta binlogs")
	}
	segmentInfo.Deltalogs = deltalogs

	// Process BM25 binlogs (no row counting)
	bm25logs, _, err := transformFieldBinlogs(source.GetBm25Binlogs(), mappings, false, false)
	if err != nil {
		return nil, merr.Wrap(err, "failed to transform BM25 binlogs")
	}
	segmentInfo.Bm25Logs = bm25logs

	return segmentInfo, nil
}

func remapSourceRootPath(sourcePath string, source *datapb.CopySegmentSource, target *datapb.CopySegmentTarget) (string, error) {
	rawSourceRoot := strings.TrimSpace(source.GetSourceRootPath())
	if rawSourceRoot == "" {
		return sourcePath, nil
	}

	rootBucket, rootObject, rootEndpoint, err := snapshotstorage.ParseForeignRootURI(rawSourceRoot)
	if err != nil {
		return "", merr.Wrap(err, "invalid copy segment source root")
	}
	pathBucket, pathObject, pathEndpoint, err := snapshotstorage.ParseForeignURI(sourcePath)
	if err != nil {
		return "", merr.Wrap(err, "invalid snapshot file path")
	}

	rootURI, err := url.Parse(rawSourceRoot)
	if err != nil {
		return "", merr.WrapErrDataIntegrity(err, "invalid copy segment source root")
	}
	pathURI, err := url.Parse(sourcePath)
	if err != nil {
		return "", merr.WrapErrDataIntegrity(err, "invalid snapshot file path")
	}
	rootIsCompleteURI := rootURI.Scheme != "" && rootURI.Host != ""
	pathIsCompleteURI := pathURI.Scheme != "" && pathURI.Host != ""
	if rootIsCompleteURI && pathIsCompleteURI &&
		(snapshotstorage.CanonicalForeignScheme(rootURI.Scheme) != snapshotstorage.CanonicalForeignScheme(pathURI.Scheme) ||
			rootBucket != pathBucket ||
			!strings.EqualFold(rootEndpoint, pathEndpoint)) {
		return "", merr.WrapErrDataIntegrityMsg(
			"snapshot file URI %q does not match source root %q",
			snapshotstorage.RedactSnapshotObjectPath(sourcePath),
			snapshotstorage.RedactSnapshotObjectPath(rawSourceRoot),
		)
	}

	rootObject = strings.Trim(rootObject, "/")
	pathObject = strings.Trim(pathObject, "/")
	relativePath := pathObject
	if rootObject != "" {
		switch {
		case pathObject == rootObject:
			relativePath = ""
		case strings.HasPrefix(pathObject, rootObject+"/"):
			relativePath = strings.TrimPrefix(pathObject, rootObject+"/")
		default:
			return "", merr.WrapErrDataIntegrityMsg(
				"snapshot file path %q is outside source root %q",
				snapshotstorage.RedactSnapshotObjectPath(sourcePath),
				snapshotstorage.RedactSnapshotObjectPath(rawSourceRoot),
			)
		}
	}

	targetRootPath := strings.Trim(target.GetTargetRootPath(), "/")
	if targetRootPath == "" {
		return relativePath, nil
	}
	if relativePath == "" {
		return targetRootPath, nil
	}
	return path.Join(targetRootPath, relativePath), nil
}

// generateTargetPath converts source file path to target path by replacing collection/partition/segment IDs
// Binlog path format: {rootPath}/{log_type}/{collectionID}/{partitionID}/{segmentID}/{fieldID}/{logID}
// Example: files/insert_log/111/222/333/444/555.log -> files/insert_log/aaa/bbb/ccc/444/555.log
func generateTargetPath(sourcePath string, source *datapb.CopySegmentSource, target *datapb.CopySegmentTarget) (string, error) {
	var err error
	sourcePath, err = remapSourceRootPath(sourcePath, source, target)
	if err != nil {
		return "", err
	}

	// Convert IDs to strings for replacement
	targetCollectionIDStr := strconv.FormatInt(target.GetCollectionId(), 10)
	targetPartitionIDStr := strconv.FormatInt(target.GetPartitionId(), 10)
	targetSegmentIDStr := strconv.FormatInt(target.GetSegmentId(), 10)

	// Split path into parts
	parts := strings.Split(sourcePath, "/")

	// Find the log type index (insert_log, delta_log, stats_log, bm25_stats)
	// Path structure: .../log_type/collectionID/partitionID/segmentID/...
	logTypeIndex := -1
	for i, part := range parts {
		if part == BinlogTypeInsert || part == BinlogTypeDelta || part == BinlogTypeStats || part == BinlogTypeBM25 {
			logTypeIndex = i
			break
		}
	}

	if logTypeIndex == -1 || logTypeIndex+3 >= len(parts) {
		return "", merr.WrapErrParameterInvalidMsg("invalid binlog path structure: %s (expected log_type at a valid position)", sourcePath)
	}

	// Replace IDs in order: collectionID, partitionID, segmentID
	// log_type is at index logTypeIndex
	// collectionID is at index logTypeIndex + 1
	// partitionID is at index logTypeIndex + 2
	// segmentID is at index logTypeIndex + 3
	parts[logTypeIndex+1] = targetCollectionIDStr
	parts[logTypeIndex+2] = targetPartitionIDStr
	parts[logTypeIndex+3] = targetSegmentIDStr

	return path.Join(parts...), nil
}

// generateTargetLOBPath replaces collection and partition IDs in a LOB file path.
// LOB path structure: {root}/insert_log/{coll}/{part}/lobs/{field}/_data/{file}.vx
// Unlike segment paths, LOB paths have no segment ID component.
func generateTargetLOBPath(sourcePath string, source *datapb.CopySegmentSource, target *datapb.CopySegmentTarget) (string, error) {
	var err error
	sourcePath, err = remapSourceRootPath(sourcePath, source, target)
	if err != nil {
		return "", err
	}

	parts := strings.Split(sourcePath, "/")

	logTypeIndex := -1
	for i, part := range parts {
		if part == BinlogTypeInsert {
			logTypeIndex = i
			break
		}
	}

	// Path: .../{insert_log}/{coll}/{part}/lobs/...
	// Need at least logTypeIndex + 2 (coll and part) after insert_log
	if logTypeIndex == -1 || logTypeIndex+2 >= len(parts) {
		return "", merr.WrapErrParameterInvalidMsg("invalid LOB path structure: %s", sourcePath)
	}

	parts[logTypeIndex+1] = strconv.FormatInt(target.GetCollectionId(), 10)
	parts[logTypeIndex+2] = strconv.FormatInt(target.GetPartitionId(), 10)

	return path.Join(parts...), nil
}

// buildIndexInfoFromSource builds complete index metadata from source information.
//
// This function extracts and transforms all index metadata (vector/scalar, text, JSON)
// from the source segment, converting file paths to target paths using the provided mappings.
//
// Parameters:
//   - source: Source segment with index file information
//   - target: Target IDs for the segment
//   - mappings: Pre-calculated source->target path mappings
//
// Returns:
//   - Vector/Scalar index metadata (buildID -> VectorScalarIndexInfo)
//   - Text index metadata (fieldID -> TextIndexStats)
//   - JSON Key index metadata (fieldID -> JsonKeyStats)
//   - error: Non-nil if any index file path has no mapping (fail-fast on missing mappings)
func buildIndexInfoFromSource(
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
	mappings map[string]string,
) (
	map[int64]*datapb.VectorScalarIndexInfo,
	map[int64]*datapb.TextIndexStats,
	map[int64]*datapb.JsonKeyStats,
	error,
) {
	// Process vector/scalar indexes
	indexInfos := make(map[int64]*datapb.VectorScalarIndexInfo)
	for _, srcIndex := range source.GetIndexFiles() {
		// Transform index file paths using mappings
		targetPaths := make([]string, 0, len(srcIndex.GetIndexFilePaths()))
		for _, srcPath := range srcIndex.GetIndexFilePaths() {
			targetPath, ok := mappings[srcPath]
			if !ok {
				return nil, nil, nil, merr.WrapErrServiceInternalMsg("no mapping found for index file: %s", srcPath)
			}
			targetPaths = append(targetPaths, targetPath)
		}

		// Use new buildID if available, otherwise fall back to source buildID
		buildID := srcIndex.GetBuildID()
		if newID, ok := target.GetNewBuildIds()[buildID]; ok {
			buildID = newID
		}

		indexInfos[buildID] = &datapb.VectorScalarIndexInfo{
			FieldId:                   srcIndex.GetFieldID(),
			IndexId:                   srcIndex.GetIndexID(),
			BuildId:                   buildID,
			Version:                   srcIndex.GetIndexVersion(),
			IndexFilePaths:            targetPaths,
			IndexSize:                 int64(srcIndex.GetSerializedSize()),
			CurrentIndexVersion:       srcIndex.GetCurrentIndexVersion(),
			CurrentScalarIndexVersion: srcIndex.GetCurrentScalarIndexVersion(),
			IndexName:                 srcIndex.GetIndexName(),
			IndexStorePathVersion:     srcIndex.GetIndexStorePathVersion(),
		}
	}

	// Process text indexes
	// For V3, text files are already copied via manifest basePath/_stats/;
	// pass metadata as placeholders (etcd paths may be stale or wrong format).
	// For V2, transform file paths using mappings.
	textIndexInfos := make(map[int64]*datapb.TextIndexStats)
	if source.GetStorageVersion() >= storage.StorageV3 {
		for fieldID, srcText := range source.GetTextIndexFiles() {
			dstText := proto.Clone(srcText).(*datapb.TextIndexStats)
			if newID, ok := target.GetNewBuildIds()[dstText.GetBuildID()]; ok {
				dstText.BuildID = newID
			}
			textIndexInfos[fieldID] = dstText
		}
	} else {
		for fieldID, srcText := range source.GetTextIndexFiles() {
			targetFiles := make([]string, 0, len(srcText.GetFiles()))
			for _, srcFile := range srcText.GetFiles() {
				targetFile, ok := mappings[srcFile]
				if !ok {
					return nil, nil, nil, merr.WrapErrServiceInternalMsg("no mapping found for text index file: %s", srcFile)
				}
				targetFiles = append(targetFiles, targetFile)
			}

			dstText := proto.Clone(srcText).(*datapb.TextIndexStats)
			dstText.Files = targetFiles
			if newID, ok := target.GetNewBuildIds()[dstText.GetBuildID()]; ok {
				dstText.BuildID = newID
			}
			textIndexInfos[fieldID] = dstText
		}
	}

	// Process JSON Key indexes
	// For V3, json files are already copied via manifest basePath/_stats/;
	// pass metadata as placeholders. For V2, transform file paths using mappings.
	jsonKeyIndexInfos := make(map[int64]*datapb.JsonKeyStats)
	if source.GetStorageVersion() >= storage.StorageV3 {
		for fieldID, srcJSON := range source.GetJsonKeyIndexFiles() {
			dstJSON := proto.Clone(srcJSON).(*datapb.JsonKeyStats)
			if newID, ok := target.GetNewBuildIds()[dstJSON.GetBuildID()]; ok {
				dstJSON.BuildID = newID
			}
			jsonKeyIndexInfos[fieldID] = dstJSON
		}
	} else {
		for fieldID, srcJSON := range source.GetJsonKeyIndexFiles() {
			targetFiles := make([]string, 0, len(srcJSON.GetFiles()))
			for _, srcFile := range srcJSON.GetFiles() {
				targetFile, ok := mappings[srcFile]
				if !ok {
					return nil, nil, nil, merr.WrapErrServiceInternalMsg("no mapping found for JSON index file: %s", srcFile)
				}
				targetFiles = append(targetFiles, targetFile)
			}

			dstJSON := proto.Clone(srcJSON).(*datapb.JsonKeyStats)
			dstJSON.Files = targetFiles
			if newID, ok := target.GetNewBuildIds()[dstJSON.GetBuildID()]; ok {
				dstJSON.BuildID = newID
			}
			jsonKeyIndexInfos[fieldID] = dstJSON
		}
	}

	return indexInfos, textIndexInfos, jsonKeyIndexInfos, nil
}

// ============================================================================
// File Type Constants
// ============================================================================

// lobFileInfosToPaths extracts absolute file paths from LobFileInfo structs.
// GetManifestLobFiles returns paths that have already been resolved to absolute
// form by the C++ manifest deserializer (Manifest::ToAbsolutePaths), so we use
// them directly without any path concatenation.
func lobFileInfosToPaths(infos []packed.LobFileInfo) []string {
	paths := make([]string, 0, len(infos))
	for _, info := range infos {
		paths = append(paths, info.Path)
	}
	return paths
}

// File type constants used for path identification and generation.
// These constants match the directory names in Milvus storage paths.
const (
	BinlogTypeInsert        = "insert_log"
	BinlogTypeStats         = "stats_log"
	BinlogTypeDelta         = "delta_log"
	BinlogTypeBM25          = "bm25_stats"
	IndexTypeVectorScalarV0 = "index_files"
	IndexTypeVectorScalarV1 = "index_v1"
	IndexTypeText           = "text_log"
	IndexTypeJSONKey        = "json_key_index_log" // Legacy: JSON Key Inverted Index
	IndexTypeJSONStats      = "json_stats"         // New: JSON Stats with Shredding Design
	FileTypeLOB             = "lob"                // LOB files at partition level for TEXT fields
)

// generateTargetIndexPath is the unified function for generating target paths for all index types
// The indexType parameter specifies which type of index path to generate
//
// Supported index types (use constants):
//   - IndexTypeVectorScalarV0: Vector/Scalar v0 path format (legacy index_files prefix)
//     {rootPath}/index_files/{build_id}/{index_version}/{partition_id}/{segment_id}/file
//     Note: collectionID is NOT in the path, only partitionID and segmentID are replaced
//   - IndexTypeVectorScalarV1: Vector/Scalar v1 path format (index_v1 prefix)
//     {rootPath}/index_v1/{collection_id}/{partition_id}/{segment_id}/{build_id}/{index_version}/file
//   - IndexTypeText: Text Index path format
//     {rootPath}/text_log/{build_id}/{version}/{collection_id}/{partition_id}/{segment_id}/{field_id}/file
//   - IndexTypeJSONKey: JSON Key Index path format (legacy)
//     {rootPath}/json_key_index_log/{build_id}/{version}/{collection_id}/{partition_id}/{segment_id}/{field_id}/file
//   - IndexTypeJSONStats: JSON Stats path format (new, data_format >= 2)
//     {rootPath}/json_stats/{data_format_version}/{build_id}/{version}/{collection_id}/{partition_id}/{segment_id}/{field_id}/(shared_key_index|shredding_data)/...
//
// Examples:
// generateTargetIndexPath(..., IndexTypeVectorScalarV0):
//
//	files/index_files/1001/1/222/333/scalar_index -> files/index_files/1001/1/bbb/ccc/scalar_index
//
// generateTargetIndexPath(..., IndexTypeText):
//
//	files/text_log/123/1/111/222/333/444/index_file -> files/text_log/123/1/aaa/bbb/ccc/444/index_file
//
// generateTargetIndexPath(..., IndexTypeJSONKey):
//
//	files/json_key_index_log/123/1/111/222/333/444/index_file -> files/json_key_index_log/123/1/aaa/bbb/ccc/444/index_file
//
// generateTargetIndexPath(..., IndexTypeJSONStats):
//
//	files/json_stats/2/123/1/111/222/333/444/shared_key_index/file -> files/json_stats/2/123/1/aaa/bbb/ccc/444/shared_key_index/file
func generateTargetIndexPath(
	sourcePath string,
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
	indexType string,
	pathVersion indexpb.IndexStorePathVersion,
) (string, error) {
	var err error
	sourcePath, err = remapSourceRootPath(sourcePath, source, target)
	if err != nil {
		return "", err
	}

	// Split path into parts
	parts := strings.Split(sourcePath, "/")

	// Determine keyword and offsets based on index type
	var keywordIdx int
	var collectionOffset, partitionOffset, segmentOffset int

	keyword := indexType
	if indexType == IndexTypeVectorScalarV0 && metautil.IsCollectionRooted(pathVersion) {
		// The caller still passes the vector/scalar logical type, but v1 files
		// live under a different object-storage prefix.
		keyword = IndexTypeVectorScalarV1
	}

	// Find the keyword position in the path
	keywordIdx = -1
	for i, part := range parts {
		if part == keyword {
			keywordIdx = i
			break
		}
	}

	if keywordIdx == -1 {
		return "", merr.WrapErrServiceInternalMsg("keyword '%s' not found in path: %s", keyword, sourcePath)
	}

	// Set offsets based on index type
	// collectionOffset = -1 means collectionID is not present in the path
	var buildIDOffset int
	switch indexType {
	case IndexTypeVectorScalarV0:
		if metautil.IsCollectionRooted(pathVersion) {
			collectionOffset = 1
			partitionOffset = 2
			segmentOffset = 3
			buildIDOffset = 4
		} else {
			collectionOffset = -1
			partitionOffset = 3
			segmentOffset = 4
			buildIDOffset = 1
		}
	case IndexTypeText, IndexTypeJSONKey:
		// Text/JSON index: text_log|json_key_index_log/build/ver/coll/part/seg/field
		collectionOffset = 3
		partitionOffset = 4
		segmentOffset = 5
		buildIDOffset = 1
	case IndexTypeJSONStats:
		// JSON Stats: json_stats/data_format_ver/build/ver/coll/part/seg/field/(shared_key_index|shredding_data)/...
		collectionOffset = 4 // One more level than legacy (data_format_version)
		partitionOffset = 5
		segmentOffset = 6
		buildIDOffset = 2
	default:
		return "", merr.WrapErrParameterInvalidMsg("unsupported index type: %s (expected '%s', '%s', '%s', or '%s')",
			indexType, IndexTypeVectorScalarV0, IndexTypeText, IndexTypeJSONKey, IndexTypeJSONStats)
	}

	// Validate path structure has enough components
	if keywordIdx+segmentOffset >= len(parts) {
		return "", merr.WrapErrParameterInvalidMsg("invalid %s path structure: %s (expected '%s' with at least %d components after it)",
			indexType, sourcePath, indexType, segmentOffset+1)
	}

	// Replace buildID if a mapping exists in target.NewBuildIds
	if keywordIdx+buildIDOffset < len(parts) {
		oldBuildIDStr := parts[keywordIdx+buildIDOffset]
		oldBuildID, parseErr := strconv.ParseInt(oldBuildIDStr, 10, 64)
		if parseErr == nil {
			if newBuildID, ok := target.GetNewBuildIds()[oldBuildID]; ok {
				parts[keywordIdx+buildIDOffset] = strconv.FormatInt(newBuildID, 10)
			}
		}
	}

	// Replace IDs at specified offsets
	// collectionOffset = -1 means collectionID is not present in the path (e.g., vector/scalar index)
	if collectionOffset >= 0 {
		parts[keywordIdx+collectionOffset] = strconv.FormatInt(target.GetCollectionId(), 10)
	}
	parts[keywordIdx+partitionOffset] = strconv.FormatInt(target.GetPartitionId(), 10)
	parts[keywordIdx+segmentOffset] = strconv.FormatInt(target.GetSegmentId(), 10)

	return path.Join(parts...), nil
}

// ============================================================================
// Path Compression Utilities
// ============================================================================
// These functions compress file paths before returning to DataCoord to reduce
// RPC response size and network transmission overhead.
// The implementations are copied from internal/datacoord/copy_segment_task.go
// to maintain consistency with DataCoord's compression logic.

const (
	jsonStatsSharedIndexPath   = "shared_key_index"
	jsonStatsShreddingDataPath = "shredding_data"
)

// shortenIndexFilePaths shortens vector/scalar index file paths to only keep the base filename.
//
// In normal index building flow, only the base filename (last path segment) is stored in IndexFileKeys.
// In copy segment flow, DataNode returns full paths after file copying.
// This function extracts the base filename to match the format expected by QueryNode loading.
//
// Path transformation:
//   - Input:  "files/index_files/444/555/666/100/1001/1002/scalar_index"
//   - Output: "scalar_index"
//
// Why only base filename:
// - DataCoord rebuilds full paths using BuildSegmentIndexFilePaths when needed
// - Storing full paths would cause duplicate path concatenation
// - Matches the convention from normal index building
//
// Parameters:
//   - fullPaths: List of full index file paths
//
// Returns:
//   - List of base filenames (last segment of each path)
func shortenIndexFilePaths(fullPaths []string) []string {
	result := make([]string, 0, len(fullPaths))
	for _, fullPath := range fullPaths {
		result = append(result, path.Base(fullPath))
	}
	return result
}

// shortenJSONStatsPath shortens JSON stats file paths in place to only keep the last 2+ segments.
//
// In normal import flow, the C++ core returns already-shortened paths (e.g., "shared_key_index/file").
// In copy segment flow, DataNode returns full paths after file copying.
// This function normalizes the paths to match the format expected by query nodes.
//
// Path transformation:
//   - Input:  "files/json_stats/2/123/1/444/555/666/100/shared_key_index/inverted_index_0"
//   - Output: "shared_key_index/inverted_index_0"
//
// Parameters:
//   - jsonStats: Map of field ID to JsonKeyStats with full paths
//
// Returns:
//   - Map of field ID to JsonKeyStats with shortened paths
func shortenJSONStatsPath(jsonStats map[int64]*datapb.JsonKeyStats) map[int64]*datapb.JsonKeyStats {
	for _, stats := range jsonStats {
		for i, file := range stats.GetFiles() {
			stats.Files[i] = shortenSingleJSONStatsPath(file)
		}
	}
	return jsonStats
}

// shortenSingleJSONStatsPath shortens a single JSON stats file path.
//
// This function extracts the relative path from a full JSON stats file path by:
//  1. Finding "shared_key_index" or "shredding_data" keywords and extracting from that position
//  2. For files directly under fieldID directory (e.g., meta.json), extracting everything after
//     the 7 path components following "json_stats"
//
// Path format: {root}/json_stats/{dataFormat}/{buildID}/{version}/{collID}/{partID}/{segID}/{fieldID}/...
//
// Path examples:
//   - Input:  "files/json_stats/2/123/1/444/555/666/100/shared_key_index/inverted_index_0"
//     Output: "shared_key_index/inverted_index_0"
//   - Input:  "files/json_stats/2/123/1/444/555/666/100/shredding_data/parquet_data_0"
//     Output: "shredding_data/parquet_data_0"
//   - Input:  "files/json_stats/2/123/1/444/555/666/100/meta.json"
//     Output: "meta.json"
//   - Input:  "shared_key_index/inverted_index_0" (already shortened)
//     Output: "shared_key_index/inverted_index_0" (idempotent)
//   - Input:  "meta.json" (already shortened)
//     Output: "meta.json" (idempotent)
//
// Parameters:
//   - fullPath: Full or partial JSON stats file path
//
// Returns:
//   - Shortened path relative to fieldID directory
func shortenSingleJSONStatsPath(fullPath string) string {
	// Find "shared_key_index" in path
	if idx := strings.Index(fullPath, jsonStatsSharedIndexPath); idx != -1 {
		return fullPath[idx:]
	}
	// Find "shredding_data" in path
	if idx := strings.Index(fullPath, jsonStatsShreddingDataPath); idx != -1 {
		return fullPath[idx:]
	}

	// Handle files directly under fieldID directory (e.g., meta.json)
	// Path format: .../json_stats/{dataFormat}/{build}/{ver}/{coll}/{part}/{seg}/{field}/filename
	// json_stats is followed by 7 components, the 8th onwards is the file path
	parts := strings.Split(fullPath, "/")
	for i, part := range parts {
		if part == common.JSONStatsPath && i+8 < len(parts) {
			return path.Join(parts[i+8:]...)
		}
	}

	// If already shortened or no json_stats found, return as-is
	return fullPath
}

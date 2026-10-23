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
	"errors"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	objectstore "github.com/milvus-io/milvus/pkg/v3/objectstorage/store"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	snapshotstorage "github.com/milvus-io/milvus/pkg/v3/snapshotio"
	snapshotformat "github.com/milvus-io/milvus/pkg/v3/snapshotio/format"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// SegmentFiles organizes source files by type for copy operations.
// InsertBinlogs holds manifest-owned physical files for ordinary StorageV3,
// including External Collections after excluding external references, or PB paths for V1/V2.
// V3 TEXT/JSON PB fields remain metadata only.
type SegmentFiles struct {
	// Nil means no native inventory was read (legacy path).
	// A non-nil empty slice proves that the pinned manifest has no indexes.
	ManifestIndexIDs []int64

	// Includes the pinned manifest and its column/delta/stats files for V3.
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

// segmentCopyPlan retains only native index IDs, not the full file inventory,
// until completion. The copied pinned manifest has exactly these inherited IDs.
type segmentCopyPlan struct {
	mappings         map[string]string
	manifestIndexIDs []int64
}

// copyObjectWithTimeout bounds one provider-managed copy operation. Retrying
// the whole call here is unsafe for asynchronous providers such as Azure.
func copyObjectWithTimeout(
	ctx context.Context,
	targetStore objectstore.ObjectStore,
	sourceBucket string,
	sourceObject string,
	targetBucket string,
	targetObject string,
	completed func(int64),
) error {
	timeout := paramtable.Get().DataNodeCfg.ImportCopyObjectTimeout.GetAsDuration(time.Second)
	copyCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	size, err := targetStore.Copy(copyCtx, objectstore.CopyRequest{Source: objectstore.Object{Bucket: sourceBucket, Key: sourceObject}, Destination: objectstore.Object{Bucket: targetBucket, Key: targetObject}})
	// Record confirmed writes before reporting size or cancellation errors.
	// A failed size lookup must not hide a destination from failure cleanup.
	if completed != nil {
		var sizeErr *objectstore.CopySizeError
		if err == nil {
			completed(size)
		} else if errors.As(err, &sizeErr) {
			completed(-1)
		}
	}
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

	// The manifest stores the complete ChunkManager key on every backend (an
	// absolute path under localStorage.path for local, a bucket-relative key
	// under minio.rootPath for remote), so the transformed physical path is
	// the new manifest base as-is.
	targetBasePath, err := generateTargetPath(basePath, source, target)
	if err != nil {
		return "", merr.Wrap(err, "failed to generate target base path")
	}

	targetManifestPath := packed.MarshalManifestPath(targetBasePath, version)
	return targetManifestPath, nil
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

func buildIndexPathVersionByFile(source *datapb.CopySegmentSource) map[string]indexpb.IndexStorePathVersion {
	versions := make(map[string]indexpb.IndexStorePathVersion)
	for _, indexInfo := range source.GetIndexFiles() {
		for _, filePath := range indexInfo.GetIndexFilePaths() {
			versions[filePath] = indexInfo.GetIndexStorePathVersion()
		}
	}
	return versions
}

// collectSegmentFiles selects Restore copy inputs from the shared raw inventory.
// External native references retain their URI in the copied manifest, not a copy task.
func collectSegmentFiles(ctx context.Context, sourceCM snapshotstorage.ChunkManager,
	sourceStorageConfig *indexpb.StorageConfig, source *datapb.CopySegmentSource,
) (*SegmentFiles, error) {
	inventory, err := snapshotstorage.EnumerateSegmentFiles(ctx, &datapb.SegmentDescription{
		SegmentId: source.GetSegmentId(), NumOfRows: source.GetNumOfRows(),
		StorageVersion: source.GetStorageVersion(), ManifestPath: source.GetManifestPath(),
		Binlogs: source.GetInsertBinlogs(), Deltalogs: source.GetDeltaBinlogs(),
		Statslogs: source.GetStatsBinlogs(), Bm25Statslogs: source.GetBm25Binlogs(),
		IndexFiles: source.GetIndexFiles(), TextIndexFiles: source.GetTextIndexFiles(),
		JsonKeyIndexFiles: source.GetJsonKeyIndexFiles(),
	}, func(_ context.Context, descriptor string) (*snapshotformat.NativeManifestFiles, error) {
		return snapshotstorage.ReadNativeManifestFiles(descriptor, sourceStorageConfig)
	})
	if err != nil {
		return nil, merr.Wrap(err, "failed to enumerate segment files")
	}
	files := &SegmentFiles{}
	if inventory.Manifest != nil {
		files.ManifestIndexIDs = make([]int64, 0, len(inventory.Manifest.Indexes))
		for _, index := range inventory.Manifest.Indexes {
			files.ManifestIndexIDs = append(files.ManifestIndexIDs, index.IndexID)
		}
	}
	for _, ref := range inventory.Files {
		nativeReference := ref.Type == snapshotstorage.SnapshotFileTypeStorageV3ManifestObject || ref.Type == snapshotstorage.SnapshotFileTypeStorageV3LOBFile
		if source.GetIsExternalCollection() && nativeReference && strings.Contains(ref.Path, "://") {
			uri, err := url.Parse(ref.Path)
			if err != nil {
				return nil, merr.WrapErrDataIntegrityMsg("invalid external manifest reference")
			}
			if uri.Scheme != "" {
				// Only native references are external. The manifest object and
				// snapshot-selected indexes must still be copied, even as URIs.
				if uri.Host == "" || uri.User != nil {
					return nil, merr.WrapErrDataIntegrityMsg("invalid external manifest URI")
				}
				continue
			}
		}
		// Check original paths before normalization/deduplication can hide aliases.
		if source.GetSourceRootPath() != "" {
			if _, err := remapSourceRootPath(ref.Path, source, &datapb.CopySegmentTarget{}); err != nil {
				return nil, err
			}
		}
		if err := snapshotstorage.ValidateSnapshotObjectPathForBucket(sourceCM, "restore source", ref.Path, sourceStorageConfig.GetBucketName()); err != nil {
			return nil, err
		}
		switch ref.Type {
		case snapshotstorage.SnapshotFileTypeStorageV3LOBFile:
			files.LobFiles = append(files.LobFiles, ref.Path)
		case snapshotstorage.SnapshotFileTypeDeltaBinlog:
			files.DeltaBinlogs = append(files.DeltaBinlogs, ref.Path)
		case snapshotstorage.SnapshotFileTypeStatsBinlog:
			files.StatsBinlogs = append(files.StatsBinlogs, ref.Path)
		case snapshotstorage.SnapshotFileTypeBM25StatsBinlog:
			files.Bm25Binlogs = append(files.Bm25Binlogs, ref.Path)
		case snapshotstorage.SnapshotFileTypeIndexFile:
			files.VectorScalarIndex = append(files.VectorScalarIndex, ref.Path)
		case snapshotstorage.SnapshotFileTypeTextIndexFile:
			files.TextIndex = append(files.TextIndex, ref.Path)
		case snapshotstorage.SnapshotFileTypeJSONKeyIndexFile:
			files.JSONKeyIndex = append(files.JSONKeyIndex, ref.Path)
		case snapshotstorage.SnapshotFileTypeJSONStatsFile:
			files.JSONStats = append(files.JSONStats, ref.Path)
		default:
			files.InsertBinlogs = append(files.InsertBinlogs, ref.Path)
		}
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

	return mappings, nil
}

// prepareSegmentCopy enumerates physical files and resolves target IDs without writing.
// Logical PB summary paths are added only during completion, never to the copy plan.
func prepareSegmentCopy(ctx context.Context, sourceCM snapshotstorage.ChunkManager, config *indexpb.StorageConfig,
	source *datapb.CopySegmentSource, target *datapb.CopySegmentTarget,
) (*segmentCopyPlan, error) {
	// Validate the native manifest entry before opening it. Physical references
	// are checked by collection/mapping below before this segment writes files or
	// commits metadata. Other segment workers may already have written outputs;
	// on failure, the task joins all workers and retains confirmed writes for cleanup.
	if source.GetStorageVersion() >= storage.StorageV3 && source.GetSourceRootPath() != "" {
		base, _, err := packed.UnmarshalManifestPath(source.GetManifestPath())
		if err != nil {
			return nil, merr.Wrap(err, "invalid source manifest path")
		}
		if _, err := remapSourceRootPath(base, source, target); err != nil {
			return nil, err
		}
	}
	files, err := collectSegmentFiles(ctx, sourceCM, config, source)
	if err != nil {
		return nil, merr.Wrap(err, "failed to collect segment files")
	}
	mappings, err := generateMappingsFromFiles(files, source, target)
	if err != nil {
		return nil, merr.Wrap(err, "failed to generate file mappings")
	}
	return &segmentCopyPlan{mappings: mappings, manifestIndexIDs: files.ManifestIndexIDs}, nil
}

// segmentCopyFiles validates and deduplicates physical copies within a segment.
// Different segments can copy shared immutable LOBs independently.
func segmentCopyFiles(plan *segmentCopyPlan) ([]snapshotstorage.FileCopy, error) {
	sources := make([]string, 0, len(plan.mappings))
	for source := range plan.mappings {
		sources = append(sources, source)
	}
	return snapshotstorage.PlanFileCopies(sources, func(source string) (snapshotstorage.FileCopy, error) {
		return snapshotstorage.FileCopy{SourcePath: source, DestinationPath: plan.mappings[source]}, nil
	})
}

// copySegmentFiles records confirmed writes before reporting cancellation. CopyFiles
// drains every active callback before this cleanup list can be consumed.
func copySegmentFiles(ctx context.Context, permits chan struct{}, items []snapshotstorage.FileCopy,
	targetStore objectstore.ObjectStore, sourceBucket, targetBucket string, logFields []mlog.Field,
) ([]string, error) {
	if targetStore == nil {
		return nil, merr.WrapErrServiceInternalMsg("snapshot target store is nil")
	}
	report, err := snapshotstorage.ExecuteFileCopies(ctx, items, permits, func(copyCtx context.Context, file snapshotstorage.FileCopy) (snapshotstorage.FileCopyOutcome, error) {
		var outcome snapshotstorage.FileCopyOutcome
		src, dst := file.SourcePath, file.DestinationPath
		copySource := snapshotstorage.NormalizeSnapshotObjectPath(src)
		mlog.Debug(copyCtx, "copying file",
			mlog.String("src", snapshotstorage.RedactSnapshotObjectPath(src)), mlog.String("dst", dst))
		if err := copyObjectWithTimeout(copyCtx, targetStore, sourceBucket, copySource, targetBucket, dst, func(size int64) {
			outcome.Confirmed = true
			outcome.Bytes = size
		}); err != nil {
			fields := make([]mlog.Field, 0, len(logFields)+3)
			fields = append(fields, logFields...)
			fields = append(fields, mlog.String("src", snapshotstorage.RedactSnapshotObjectPath(src)), mlog.String("dst", dst), mlog.Err(err))
			mlog.Warn(copyCtx, "failed to copy file", fields...)
			return outcome, merr.Wrapf(err, "failed to copy file from %s to %s", snapshotstorage.RedactSnapshotObjectPath(src), dst)
		}
		return outcome, nil
	}, nil)
	return report.CopiedPaths, err
}

// completeSegmentCopy builds Restore metadata only after all physical copies finish.
// Newly committed manifest revisions are returned separately for failure cleanup.
func completeSegmentCopy(ctx context.Context, targetStorageConfig *indexpb.StorageConfig,
	source *datapb.CopySegmentSource, target *datapb.CopySegmentTarget, plan *segmentCopyPlan,
) (*datapb.CopySegmentResult, []string, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	mappings := plan.mappings
	useManifest := source.GetStorageVersion() >= storage.StorageV3
	var createdFiles []string
	// Step 1: When manifest is used (StorageV3+), physical paths were
	// collected from the manifest, but generateSegmentInfoFromSource still
	// needs mappings for the protobuf paths (which for V3 are summaries whose
	// LogPath may be empty or a manifest-internal key). Add those "logical-only"
	// mappings after physical copying so they never trigger a copy.
	if useManifest {
		logicalPathGroups := [][]string{
			extractFromPb(source.GetInsertBinlogs()),
			extractFromPb(source.GetDeltaBinlogs()),
			extractFromPb(source.GetStatsBinlogs()),
			extractFromPb(source.GetBm25Binlogs()),
		}
		logicalPathCount := 0
		for _, logicalPaths := range logicalPathGroups {
			logicalPathCount += len(logicalPaths)
			for _, srcPath := range logicalPaths {
				dstPath, pathErr := generateTargetPath(srcPath, source, target)
				if pathErr != nil {
					return nil, createdFiles, merr.Wrapf(pathErr, "failed to generate target path for protobuf binlog %s", srcPath)
				}
				mappings[srcPath] = dstPath
			}
		}
		mlog.Info(ctx, "added logical insert binlog mappings for manifest segment",
			mlog.Int("pbPathCount", logicalPathCount))
	}

	// Step 2: Build index metadata from source
	indexInfos, textIndexInfos, jsonKeyIndexInfos, err := buildIndexInfoFromSource(source, target, mappings)
	if err != nil {
		return nil, createdFiles, merr.Wrap(err, "failed to build index info")
	}

	// Step 3: Generate segment metadata with path mappings
	segmentInfo, err := generateSegmentInfoFromSource(source, target, mappings)
	if err != nil {
		return nil, createdFiles, merr.Wrap(err, "failed to generate segment info")
	}

	// Step 4: Compress paths
	err = binlog.CompressBinLogs(segmentInfo.GetBinlogs(), segmentInfo.GetStatslogs(),
		segmentInfo.GetDeltalogs(), segmentInfo.GetBm25Logs())
	if err != nil {
		return nil, createdFiles, merr.Wrap(err, "failed to compress binlog paths")
	}

	// Step 5: Publish the target manifest (StorageV3+).
	//
	// This must run before step 6, which rewrites indexInfos[].IndexFilePaths
	// IN PLACE - the same structs read here. A manifest index entry stores an
	// artifact as a directory plus the file names within it, so it is derived
	// while those paths are still the full ones this copy wrote; after step 6
	// each is a bare file name and no entry could name a directory.
	var (
		targetManifestPath    string
		manifestIndexBuildIDs []int64
	)
	if useManifest {
		targetManifestPath, err = transformManifestPath(source.GetManifestPath(), source, target)
		if err != nil {
			return nil, createdFiles, merr.Wrap(err, "failed to transform manifest path")
		}
		sourceManifestKnownEmpty := source.ManifestHasIndex != nil && !source.GetManifestHasIndex()
		copiedManifestPath := targetManifestPath
		targetManifestPath, manifestIndexBuildIDs, err = republishCopiedManifestIndexes(
			ctx, targetManifestPath, target, source.GetNumOfRows(), targetStorageConfig, indexInfos,
			sourceManifestKnownEmpty, plan.manifestIndexIDs)
		if err != nil {
			return nil, createdFiles, merr.Wrap(err, "failed to republish copied manifest indexes")
		}
		if targetManifestPath != copiedManifestPath {
			revisionFile, err := packed.ManifestFilePath(targetManifestPath)
			if err != nil {
				return nil, createdFiles, err
			}
			createdFiles = append(createdFiles, revisionFile)
		}
	}

	// Step 6: Shorten index and JSON stats paths for the DataCoord-facing
	// result. Both rewrite their inputs in place - see step 5.
	for _, indexInfo := range indexInfos {
		indexInfo.IndexFilePaths = shortenIndexFilePaths(indexInfo.IndexFilePaths)
	}

	jsonKeyIndexInfos = shortenJSONStatsPath(jsonKeyIndexInfos)

	mlog.Info(ctx, "path compression completed",
		mlog.Int("binlogFields", len(segmentInfo.GetBinlogs())),
		mlog.Int("indexCount", len(indexInfos)),
		mlog.Int("jsonStatsCount", len(jsonKeyIndexInfos)))

	// Step 7: Build result
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

	// Step 8: Propagate manifest_path. The StorageV3 pointer was already
	// produced by step 5, before index paths were shortened.
	if useManifest {
		result.ManifestPath = targetManifestPath
		result.ManifestIndexRewritten = proto.Bool(true)
		result.ManifestIndexBuildIds = manifestIndexBuildIDs
	} else if source.GetStorageVersion() == storage.StorageV2 && source.GetManifestPath() != "" {
		targetManifestPath, ok := mappings[source.GetManifestPath()]
		if !ok {
			return nil, createdFiles, merr.WrapErrDataIntegrityMsg(
				"missing copied StorageV2 manifest mapping for segment %d",
				source.GetSegmentId(),
			)
		}
		result.ManifestPath = targetManifestPath
	}

	mlog.Info(ctx, "copy segment and index files completed successfully",
		mlog.Int64("importedRows", result.ImportedRows))

	return result, createdFiles, nil
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
// V3 preparation supplies every inherited index ID from its pinned
// source read, including an explicitly empty inventory. Only paths without that
// inventory enumerate the copied target manifest.
func republishCopiedManifestIndexes(
	ctx context.Context,
	targetManifestPath string,
	target *datapb.CopySegmentTarget,
	numRows int64,
	targetStorageConfig *indexpb.StorageConfig,
	indexInfos map[int64]*datapb.VectorScalarIndexInfo,
	sourceManifestKnownEmpty bool,
	inheritedIndexIDs []int64,
) (string, []int64, error) {
	if err := ctx.Err(); err != nil {
		return "", nil, err
	}
	if inheritedIndexIDs == nil && !sourceManifestKnownEmpty {
		existing, err := packed.GetManifestIndexInfos(targetManifestPath, targetStorageConfig)
		if err != nil {
			return "", nil, merr.Wrap(err, "failed to enumerate copied manifest index entries")
		}
		for _, entry := range existing {
			inheritedIndexIDs = append(inheritedIndexIDs, entry.IndexID)
		}
	}
	adds, err := buildTargetManifestIndexes(ctx, targetManifestPath, target, numRows, indexInfos)
	if err != nil {
		return "", nil, err
	}
	publishedBuildIDs := make([]int64, 0, len(adds))
	for _, entry := range adds {
		publishedBuildIDs = append(publishedBuildIDs, entry.BuildID)
	}
	if len(inheritedIndexIDs) == 0 && len(adds) == 0 {
		return targetManifestPath, publishedBuildIDs, nil
	}
	if err := ctx.Err(); err != nil {
		return "", nil, err
	}

	basePath, version, err := packed.UnmarshalManifestPath(targetManifestPath)
	if err != nil {
		return "", nil, merr.Wrap(err, "failed to parse copied manifest path")
	}
	republished, err := packed.ReplaceCopiedManifestIndexes(basePath, version, targetStorageConfig, inheritedIndexIDs, adds)
	if err != nil {
		return "", nil, merr.Wrap(err, "failed to commit copied manifest indexes")
	}
	mlog.Info(ctx, "republished copied segment manifest indexes",
		mlog.Int64("targetSegmentID", target.GetSegmentId()),
		mlog.Int("inheritedIndexEntries", len(inheritedIndexIDs)),
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
	ctx context.Context,
	targetManifestPath string,
	target *datapb.CopySegmentTarget,
	numRows int64,
	indexInfos map[int64]*datapb.VectorScalarIndexInfo,
) ([]packed.ManifestIndexInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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
			mlog.Warn(ctx, "copied index has no target definition, skipping manifest entry",
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
			mlog.Warn(ctx, "copied index carries no artifact path, skipping manifest entry",
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
//   - opts: see transformFieldBinlogOptions
//
// Returns:
//   - []*datapb.FieldBinlog: Transformed binlog list with target paths
//   - int64: Total row count (sum of EntriesNum from all binlogs if countRows=true, 0 otherwise)
//   - error: Non-nil if any source path has no mapping (fail-fast on missing mappings)
//
// transformFieldBinlogOptions controls how one binlog family is transformed.
type transformFieldBinlogOptions struct {
	// countRows accumulates total row count from EntriesNum (insert logs only).
	countRows bool
	// isExternalTable skips path mapping: external table insert binlogs carry
	// row metadata without physical log paths.
	isExternalTable bool
	// preservePathless keeps metadata-only summaries that have no physical
	// LogPath (StorageV3 insert and delta summaries).
	preservePathless bool
}

func transformFieldBinlogs(
	srcFieldBinlogs []*datapb.FieldBinlog,
	mappings map[string]string,
	opts transformFieldBinlogOptions,
) ([]*datapb.FieldBinlog, int64, error) {
	result := make([]*datapb.FieldBinlog, 0, len(srcFieldBinlogs))
	var totalRows int64

	for _, srcFieldBinlog := range srcFieldBinlogs {
		dstFieldBinlog := proto.Clone(srcFieldBinlog).(*datapb.FieldBinlog)
		dstFieldBinlog.Binlogs = make([]*datapb.Binlog, 0, len(srcFieldBinlog.GetBinlogs()))

		for _, srcBinlog := range srcFieldBinlog.GetBinlogs() {
			dstBinlog := proto.Clone(srcBinlog).(*datapb.Binlog)

			if !opts.isExternalTable {
				srcPath := srcBinlog.GetLogPath()
				if srcPath == "" {
					if !opts.preservePathless {
						continue
					}
				} else {
					dstPath, ok := mappings[srcPath]
					if !ok {
						return nil, 0, merr.WrapErrServiceInternalMsg("no mapping found for source path: %s", srcPath)
					}
					dstBinlog.LogPath = dstPath
				}
			}

			dstFieldBinlog.Binlogs = append(dstFieldBinlog.Binlogs, dstBinlog)
			if opts.countRows {
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
	isStorageV3 := source.GetStorageVersion() >= storage.StorageV3
	binlogs, totalRows, err := transformFieldBinlogs(source.GetInsertBinlogs(), mappings,
		transformFieldBinlogOptions{
			countRows:        true,
			isExternalTable:  source.GetIsExternalCollection(),
			preservePathless: isStorageV3,
		})
	if err != nil {
		return nil, merr.Wrap(err, "failed to transform insert binlogs")
	}
	segmentInfo.Binlogs = binlogs
	segmentInfo.ImportedRows = totalRows
	if source.GetStorageVersion() >= storage.StorageV3 && source.GetNumOfRows() > 0 {
		segmentInfo.ImportedRows = source.GetNumOfRows()
	}

	// Process stats binlogs (no row counting)
	statslogs, _, err := transformFieldBinlogs(source.GetStatsBinlogs(), mappings, transformFieldBinlogOptions{})
	if err != nil {
		return nil, merr.Wrap(err, "failed to transform stats binlogs")
	}
	segmentInfo.Statslogs = statslogs

	// Process delta binlogs (no row counting)
	deltalogs, _, err := transformFieldBinlogs(source.GetDeltaBinlogs(), mappings, transformFieldBinlogOptions{preservePathless: isStorageV3})
	if err != nil {
		return nil, merr.Wrap(err, "failed to transform delta binlogs")
	}
	segmentInfo.Deltalogs = deltalogs

	// Process BM25 binlogs (no row counting)
	bm25logs, _, err := transformFieldBinlogs(source.GetBm25Binlogs(), mappings, transformFieldBinlogOptions{})
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

	rawTargetRootPath := strings.TrimSpace(target.GetTargetRootPath())
	targetRootIsAbs := path.IsAbs(rawTargetRootPath)
	targetRootPath := strings.Trim(rawTargetRootPath, "/")
	var targetPath string
	switch {
	case targetRootPath == "":
		targetPath = relativePath
	case relativePath == "":
		targetPath = targetRootPath
	default:
		targetPath = path.Join(targetRootPath, relativePath)
	}
	if targetRootIsAbs {
		return "/" + targetPath, nil
	}
	return targetPath, nil
}

// buildCanonicalTargetPath rebuilds the layout under the target root without
// carrying over the source storage prefix. Joining the root separately preserves
// the leading slash for local paths; empty/dot roots address the remote bucket.
func buildCanonicalTargetPath(parts []string, layoutIndex int, target *datapb.CopySegmentTarget) string {
	return path.Join(strings.TrimSpace(target.GetTargetRootPath()), path.Join(parts[layoutIndex:]...))
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

	// Convert IDs to strings for replacement.
	targetCollectionIDStr := strconv.FormatInt(target.GetCollectionId(), 10)
	targetPartitionIDStr := strconv.FormatInt(target.GetPartitionId(), 10)
	targetSegmentIDStr := strconv.FormatInt(target.GetSegmentId(), 10)

	// Split path into parts
	parts := strings.Split(sourcePath, "/")

	// Find the log layout keyword.
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

	return buildCanonicalTargetPath(parts, logTypeIndex, target), nil
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

	logTypeIndex := slices.Index(parts, BinlogTypeInsert)

	// Path: .../{insert_log}/{coll}/{part}/lobs/...
	// Need at least logTypeIndex + 2 (coll and part) after insert_log
	if logTypeIndex == -1 || logTypeIndex+2 >= len(parts) {
		return "", merr.WrapErrParameterInvalidMsg("invalid LOB path structure: %s", sourcePath)
	}

	parts[logTypeIndex+1] = strconv.FormatInt(target.GetCollectionId(), 10)
	parts[logTypeIndex+2] = strconv.FormatInt(target.GetPartitionId(), 10)

	return buildCanonicalTargetPath(parts, logTypeIndex, target), nil
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

	// Determine keyword and offsets based on index type.
	var collectionOffset, partitionOffset, segmentOffset int

	keyword := indexType
	if indexType == IndexTypeVectorScalarV0 && metautil.IsCollectionRooted(pathVersion) {
		// The caller still passes the vector/scalar logical type, but v1 files
		// live under a different object-storage prefix.
		keyword = IndexTypeVectorScalarV1
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

	parts := strings.Split(sourcePath, "/")
	keywordIdx := slices.Index(parts, keyword)
	if keywordIdx == -1 || keywordIdx+segmentOffset >= len(parts) {
		return "", merr.WrapErrParameterInvalidMsg("invalid %s path structure: %s (expected '%s' layout)",
			indexType, sourcePath, keyword)
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

	return buildCanonicalTargetPath(parts, keywordIdx, target), nil
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
			stats.Files[i] = shortenJSONStatsFilePath(file, stats)
		}
	}
	return jsonStats
}

// shortenJSONStatsFilePath extracts the path after the fieldID component of a
// JSON-stats layout matching stats. The caller only passes non-nil stats that
// contain files; nil map entries are skipped by shortenJSONStatsPath.
//
// Path format: {root}/json_stats/{dataFormat}/{buildID}/{version}/{collID}/{partID}/{segID}/{fieldID}/...
func shortenJSONStatsFilePath(fullPath string, stats *datapb.JsonKeyStats) string {
	parts := strings.Split(fullPath, "/")
	if parts[0] == jsonStatsSharedIndexPath || parts[0] == jsonStatsShreddingDataPath {
		return path.Join(parts...)
	}

	// Find the last structurally valid JSON-stats layout. Root prefixes may
	// themselves contain json_stats/shared_key_index, so substring or first-key
	// matching is not safe. Also match the layout's data format, build, version,
	// and field components against the stats metadata.
	anchor := -1
	for i, part := range parts {
		if part != common.JSONStatsPath || i+8 >= len(parts) {
			continue
		}
		valid := true
		for offset := 1; offset <= 7; offset++ {
			if _, err := strconv.ParseInt(parts[i+offset], 10, 64); err != nil {
				valid = false
				break
			}
		}
		if !valid {
			continue
		}
		if parts[i+1] != strconv.FormatInt(stats.GetJsonKeyStatsDataFormat(), 10) ||
			parts[i+2] != strconv.FormatInt(stats.GetBuildID(), 10) ||
			parts[i+3] != strconv.FormatInt(stats.GetVersion(), 10) ||
			parts[i+7] != strconv.FormatInt(stats.GetFieldID(), 10) {
			continue
		}
		anchor = i
	}
	if anchor >= 0 {
		return path.Join(parts[anchor+8:]...)
	}

	// If already shortened or no json_stats found, return as-is
	return fullPath
}

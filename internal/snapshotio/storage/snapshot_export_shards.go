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

package storage

import (
	"context"
	"math"
	"path"
	"slices"
	"strconv"
	"strings"

	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	milvusstorage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ValidateSnapshotExportResult checks scalar segment-range completion statistics.
// Results persist in the coordinator job record; final paths come from metadata.
func ValidateSnapshotExportResult(req *datapb.SnapshotExportCopyRequest, result *datapb.SnapshotExportCopyResponse) error {
	if req.GetGenerationId() <= 0 || req.GetSegmentStart() < 0 || req.GetSegmentEnd() <= req.GetSegmentStart() ||
		result.GetTaskId() != req.GetTaskId() || result.GetState() != datapb.ImportTaskStateV2_Completed ||
		result.GetTotalFiles() < req.GetSegmentEnd()-req.GetSegmentStart() || result.GetCopiedFiles() != result.GetTotalFiles() ||
		result.GetCopiedBytes() < 0 {
		return merr.WrapErrDataIntegrityMsg("incomplete snapshot export shard result")
	}
	return nil
}

func snapshotExportManifestSegmentID(manifest string) (int64, error) {
	name := path.Base(NormalizeSnapshotObjectPath(manifest))
	id, err := strconv.ParseInt(strings.TrimSuffix(name, ".avro"), 10, 64)
	if err != nil || id <= 0 || name != strconv.FormatInt(id, 10)+".avro" {
		return 0, merr.WrapErrDataIntegrityMsg("snapshot manifest has no canonical segment identity: %s", manifest)
	}
	return id, nil
}

// visitSnapshotSegments keeps only active segments decoded. The metadata is
// loaded once; canonical Avro identities and native mappings are checked before
// workers start. Each worker releases its read permit before invoking visit,
// which may acquire the same task-local permits for native reads and copies.
func (r *SnapshotReader) visitSnapshotSegments(ctx context.Context, snapshot *SnapshotData,
	oldRoot, newRoot string, rebase bool, visit func(context.Context, *SnapshotData) error,
) error {
	if cap(r.manifestReadPermits) == 0 {
		return merr.WrapErrServiceInternalMsg("segment processing requires buffered permits")
	}
	allowed := make(map[int64]bool, len(snapshot.SegmentIDs))
	for _, id := range snapshot.SegmentIDs {
		allowed[id] = true
	}
	selected := make(map[int64]bool, len(snapshot.ManifestPaths))
	placeholders := make([]*datapb.SegmentDescription, len(snapshot.ManifestPaths))
	for i, manifest := range snapshot.ManifestPaths {
		id, err := snapshotExportManifestSegmentID(manifest)
		if err != nil {
			return err
		}
		if selected[id] || len(allowed) > 0 && !allowed[id] {
			return merr.WrapErrDataIntegrityMsg("duplicate or unexpected shard segment %d", id)
		}
		selected[id] = true
		placeholders[i] = &datapb.SegmentDescription{SegmentId: id}
	}
	var mappings []*datapb.StorageV2SegmentManifest
	for _, mapping := range snapshot.StorageManifests {
		if selected[mapping.GetSegmentId()] {
			mappings = append(mappings, mapping)
		}
	}
	if err := applyStorageManifestPaths(mappings, placeholders); err != nil {
		return err
	}
	group, workCtx := errgroup.WithContext(ctx)
	group.SetLimit(cap(r.manifestReadPermits))
	for i, manifest := range snapshot.ManifestPaths {
		if workCtx.Err() != nil {
			break
		}
		group.Go(func() error {
			select {
			case r.manifestReadPermits <- struct{}{}:
			case <-workCtx.Done():
				return workCtx.Err()
			}
			segment, err := r.readManifestFile(workCtx, manifest, int(snapshot.ManifestFormatVersion))
			<-r.manifestReadPermits
			if err != nil {
				return err
			}
			if segment.GetSegmentId() != placeholders[i].GetSegmentId() {
				return merr.WrapErrDataIntegrityMsg("snapshot manifest identity does not match segment %d", placeholders[i].GetSegmentId())
			}
			one := *snapshot
			one.ManifestPaths = []string{manifest}
			one.Segments = []*datapb.SegmentDescription{segment}
			if rebase {
				if err := RebaseSelfContainedSnapshotData(&one, oldRoot, newRoot); err != nil {
					return err
				}
			}
			// Top-level native paths were already rebased by the metadata reader.
			if native := placeholders[i].GetManifestPath(); native != "" {
				segment.ManifestPath = native
			}
			if err := checkSegmentSnapshotPaths(segment, validateSnapshotPathReference, validateSnapshotPathReference); err != nil {
				return err
			}
			if snapshot.Layout == datapb.SnapshotLayout_SnapshotLayoutSelfContained {
				// Top-level references were checked once before admission.
				if err := ValidateSelfContainedSnapshotMetadata(snapshot.MetadataPath, &datapb.SnapshotMetadata{}, one.Segments); err != nil {
					return err
				}
			}
			if err := workCtx.Err(); err != nil {
				return err
			}
			return visit(workCtx, &one)
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}
	return ctx.Err()
}

// copySnapshotExportSegment consumes one visitor segment and its validated plan.
// Progress reports deltas; the caller owns synchronization and overflow checks.
func copySnapshotExportSegment(ctx context.Context, req *datapb.SnapshotExportCopyRequest, items []FileCopy, snapshot *SnapshotData, sourceRoot, targetRoot string,
	resolved *ResolvedForeignStorage, permits chan struct{}, progress func(int64, int64, int64),
) error {
	if len(snapshot.Segments) != 1 || snapshot.Segments[0] == nil {
		return merr.WrapErrServiceInternalMsg("export copy requires one segment")
	}
	if progress != nil {
		progress(0, 0, int64(len(items)))
	}
	// The planner includes the generated Avro destination in conflict checks.
	// Copy physical files first, then write that Avro from the decoded segment.
	segment := snapshot.Segments[0]
	manifestDir, _ := GetSnapshotPaths(targetRoot, snapshot.SnapshotInfo.GetCollectionId(), snapshot.SnapshotInfo.GetId())
	manifest := FileCopy{DestinationPath: GetSegmentManifestPath(manifestDir, segment.GetSegmentId())}
	files := slices.DeleteFunc(items, func(file FileCopy) bool { return file.DestinationPath == manifest.DestinationPath })
	completed := func(size int64) {
		if progress != nil {
			progress(1, size, 0)
		}
	}
	if err := CopySnapshotExportPlan(ctx, files, req.GetSourceStorageConfig().GetBucketName(), resolved.ForeignBucket, resolved.Copier, permits, completed); err != nil {
		return err
	}
	// Encode under the same task permit; do not reread source Avro or mutate it.
	if err := CopyFiles(ctx, []FileCopy{manifest}, permits, func(writeCtx context.Context, file FileCopy) error {
		data, err := marshalSnapshotExportManifest(segment, sourceRoot, targetRoot)
		if err != nil {
			return err
		}
		if err := resolved.ForeignCM.Write(writeCtx, file.DestinationPath, data); err != nil {
			return merr.Wrap(err, "failed to write exported snapshot manifest")
		}
		completed(0) // Count Avro as a file, excluding it from data/index bytes.
		return nil
	}); err != nil {
		return err
	}
	return ctx.Err()
}

// SnapshotExportFinalizationResult contains DataCoord-local publication metadata.
type SnapshotExportFinalizationResult struct {
	MetadataURI string
	TotalFiles  int64
	TotalBytes  int64
}

// FinalizeSnapshotExport prepares staging on DataCoord after all persisted
// range completions have been acknowledged and every copy worker has exited.
// It reads top-level metadata only, never Avro/native manifests or file lists.
func FinalizeSnapshotExport(ctx context.Context, req *datapb.SnapshotExportCopyRequest, tasks []*datapb.SnapshotExportCopyTask,
	sourceCM milvusstorage.ChunkManager, resolved *ResolvedForeignStorage,
) (*SnapshotExportFinalizationResult, error) {
	if req.GetGenerationId() <= 0 {
		return nil, merr.WrapErrServiceInternalMsg("snapshot export finalization requires a generation")
	}
	snapshot, err := NewSnapshotReader(sourceCM).ReadSnapshot(ctx, req.GetSnapshotMetadataPath(), false)
	if err != nil {
		return nil, err
	}
	if snapshot.SnapshotInfo.GetId() != req.GetSnapshotId() || snapshot.SnapshotInfo.GetCollectionId() != req.GetCollectionId() {
		return nil, merr.WrapErrDataIntegrityMsg("snapshot export source identity changed")
	}
	snapshot.SnapshotInfo.S3Location = req.GetSnapshotMetadataPath()
	plan, err := buildSnapshotExportMetadataPlan(snapshot, sourceCM.RootPath(), SnapshotExportAttemptURI(req), resolved.ForeignStorageConfig)
	if err != nil {
		return nil, err
	}
	var next, totalBytes, totalFiles int64
	shardReq := proto.Clone(req).(*datapb.SnapshotExportCopyRequest)
	seenTasks := make(map[int64]bool, len(tasks))
	for _, task := range tasks {
		if task.GetTaskId() <= 0 || seenTasks[task.GetTaskId()] || task.GetNodeId() != 0 || task.GetSegmentStart() != next || task.GetSegmentEnd() <= next || task.GetSegmentEnd() > int64(len(snapshot.ManifestPaths)) {
			return nil, merr.WrapErrDataIntegrityMsg("export shards do not cover snapshot or still have active writers")
		}
		seenTasks[task.GetTaskId()] = true
		shardReq.TaskId = task.GetTaskId()
		shardReq.SegmentStart, shardReq.SegmentEnd = task.GetSegmentStart(), task.GetSegmentEnd()
		if err := ValidateSnapshotExportResult(shardReq, task.GetResult()); err != nil {
			return nil, err
		}
		result := task.GetResult()
		if result.GetCopiedBytes() > math.MaxInt64-totalBytes || result.GetCopiedFiles() > math.MaxInt64-totalFiles {
			return nil, merr.WrapErrDataIntegrityMsg("export statistics overflow")
		}
		totalBytes += result.GetCopiedBytes()
		totalFiles += result.GetCopiedFiles()
		next = task.GetSegmentEnd()
	}
	if next != int64(len(snapshot.ManifestPaths)) {
		return nil, merr.WrapErrDataIntegrityMsg("incomplete export shard coverage")
	}
	if err = prepareSnapshotExportMetadata(ctx, resolved.ForeignCM, snapshot, plan); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return &SnapshotExportFinalizationResult{MetadataURI: plan.MetadataURI, TotalFiles: totalFiles, TotalBytes: totalBytes}, nil
}

func buildSnapshotExportMetadataPlan(snapshot *SnapshotData, sourceRoot, target string, targetConfig *indexpb.StorageConfig) (*SnapshotExportPlan, error) {
	targetRoot := strings.TrimSuffix(NormalizeSnapshotObjectPath(target), "/")
	manifestDir, metadata := GetSnapshotPaths(targetRoot, snapshot.SnapshotInfo.GetCollectionId(), snapshot.SnapshotInfo.GetId())
	uri, err := BuildStorageConfigSnapshotURI(targetConfig, metadata)
	if err != nil {
		return nil, err
	}
	sourceRoot, err = snapshotExportSourceRoot(snapshot, sourceRoot)
	if err != nil {
		return nil, err
	}
	plan := &SnapshotExportPlan{TargetRoot: targetRoot, MetadataURI: uri}
	segments := make([]*datapb.SegmentDescription, 0, len(snapshot.ManifestPaths))
	for _, manifest := range snapshot.ManifestPaths {
		id, err := snapshotExportManifestSegmentID(manifest)
		if err != nil {
			return nil, err
		}
		segments = append(segments, &datapb.SegmentDescription{SegmentId: id})
		plan.ManifestPaths = append(plan.ManifestPaths, GetSegmentManifestPath(manifestDir, id))
	}
	if err := validateSnapshotSegmentIDs(snapshot.SegmentIDs, segments); err != nil {
		return nil, err
	}
	if err := applyStorageManifestPaths(snapshot.StorageManifests, segments); err != nil {
		return nil, err
	}
	for _, entry := range snapshot.StorageManifests {
		manifest := entry.GetManifest()
		if strings.HasPrefix(strings.TrimSpace(manifest), "{") {
			base, version, err := packed.UnmarshalManifestPath(manifest)
			if err != nil {
				return nil, merr.WrapErrDataIntegrity(err, "invalid storage manifest descriptor")
			}
			manifest = packed.MarshalManifestPath(exportedSnapshotPath(sourceRoot, NormalizeSnapshotObjectPath(base), targetRoot), version)
		} else {
			manifest = exportedSnapshotPath(sourceRoot, NormalizeSnapshotObjectPath(manifest), targetRoot)
		}
		plan.StorageManifests = append(plan.StorageManifests, &datapb.StorageV2SegmentManifest{SegmentId: entry.GetSegmentId(), Manifest: manifest})
	}
	return plan, nil
}

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
	"math"
	"sort"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	objectstore "github.com/milvus-io/milvus/pkg/v3/objectstorage/store"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	snapshotformat "github.com/milvus-io/milvus/pkg/v3/snapshotio/format"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type Options struct{ Concurrency int }
type segmentRange struct{ Start, End int64 }
type CopyRequest struct {
	SourceMetadataURI string
	TargetRoot        string
}

type rangeResult struct {
	SourceMetadataURI                    string
	SnapshotID, CollectionID             int64
	TargetRoot                           string
	Range                                segmentRange
	CopiedFiles, CopiedBytes, TotalFiles int64
}

// publication describes the staged metadata and final destination.
type publication struct {
	StagingMetadataURI     string
	MetadataURI            string
	TotalFiles, TotalBytes int64
}
type CopyResult struct {
	MetadataURI            string
	TotalFiles, TotalBytes int64
}

// CopySnapshot copies and publishes a complete snapshot. Each call owns its
// concurrency budget; source and target must support concurrent operations.
func CopySnapshot(ctx context.Context, source objectstore.ObjectReader, target objectstore.ObjectStore, req CopyRequest, options Options) (*CopyResult, error) {
	if options.Concurrency <= 0 {
		return nil, merr.WrapErrParameterInvalidMsg("copy concurrency must be positive")
	}
	if source == nil {
		return nil, merr.WrapErrParameterMissingMsg("source reader is required")
	}
	bucket, _, _, err := ParseForeignURI(req.SourceMetadataURI)
	if err != nil {
		return nil, err
	}
	root, ok := DeriveSnapshotRootPath(req.SourceMetadataURI)
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg("snapshot metadata URI requires a snapshot root")
	}
	sourceCM := &chunkManagerAdapter{reader: source, bucket: bucket, root: root}
	snapshot, err := NewSnapshotReader(sourceCM).ReadSnapshot(ctx, req.SourceMetadataURI, false)
	if err != nil {
		return nil, err
	}
	if typeutil.IsExternalCollection(snapshot.Collection.GetSchema()) {
		return nil, merr.WrapErrParameterInvalidMsg("exporting external collections is not supported")
	}
	resolved, err := resolveSnapshotTarget(target, req.TargetRoot)
	if err != nil {
		return nil, err
	}
	var ranges []rangeResult
	if count := int64(len(snapshot.ManifestPaths)); count > 0 {
		result, err := copyRange(ctx, snapshot, sourceCM, nil, resolved, req.TargetRoot,
			segmentRange{End: count}, make(chan struct{}, options.Concurrency),
			func(ctx context.Context, descriptor string) (*snapshotformat.NativeManifestFiles, error) {
				return readManifestBytes(ctx, sourceCM, bucket, descriptor)
			}, nil)
		if err != nil {
			return nil, err
		}
		ranges = append(ranges, *result)
	}
	prepared, err := prepareRanges(ctx, snapshot, req.TargetRoot, resolved, ranges)
	if err != nil {
		return nil, err
	}
	return publishSnapshot(ctx, resolved, *prepared)
}

func resolveSnapshotTarget(target objectstore.ObjectStore, targetRoot string) (*ResolvedForeignStorage, error) {
	if target == nil {
		return nil, merr.WrapErrParameterMissingMsg("target store is required")
	}
	bucket, key, _, err := ParseForeignRootURI(targetRoot)
	if err != nil {
		return nil, err
	}
	if _, err := snapshotTargetObjectURI(targetRoot, GetSnapshotStagingMetadataPath(key)); err != nil {
		return nil, err
	}
	cm := &chunkManagerAdapter{reader: target, store: target, bucket: bucket, root: key}
	return &ResolvedForeignStorage{ForeignBucket: bucket, ForeignCM: cm, TargetStore: target}, nil
}

func publishSnapshot(ctx context.Context, resolved *ResolvedForeignStorage, p publication) (*CopyResult, error) {
	if p.TotalFiles < 0 || p.TotalBytes < 0 {
		return nil, merr.WrapErrDataIntegrityMsg("negative publication statistics")
	}
	if err := ValidateSnapshotObjectPathForBucket(resolved.ForeignCM, "staging metadata", p.StagingMetadataURI, resolved.ForeignBucket); err != nil {
		return nil, err
	}
	stagingKey := NormalizeSnapshotObjectPath(p.StagingMetadataURI)
	staging, err := snapshotTargetObjectURI(p.StagingMetadataURI, stagingKey)
	if err != nil {
		return nil, err
	}
	expected, err := snapshotTargetObjectURI(p.MetadataURI, stagingKey)
	if err != nil {
		return nil, err
	}
	if staging != expected {
		return nil, merr.WrapErrParameterInvalidMsg("publication URIs must use the same storage location")
	}
	_, err = NewSnapshotWriter(resolved.ForeignCM).CommitStagedMetadata(ctx, p.StagingMetadataURI, p.MetadataURI, p.MetadataURI)
	if err != nil {
		return nil, err
	}
	return &CopyResult{MetadataURI: p.MetadataURI, TotalFiles: p.TotalFiles, TotalBytes: p.TotalBytes}, nil
}

type chunkManagerAdapter struct {
	reader       objectstore.ObjectReader
	store        objectstore.ObjectStore
	bucket, root string
}

func (s *chunkManagerAdapter) RootPath() string   { return s.root }
func (s *chunkManagerAdapter) BucketName() string { return s.bucket }
func (s *chunkManagerAdapter) Read(ctx context.Context, key string) ([]byte, error) {
	data, err := s.reader.Read(ctx, objectstore.Object{Bucket: s.bucket, Key: key})
	return data, objectstorage.ToMilvusIoError(key, err)
}
func (s *chunkManagerAdapter) Write(ctx context.Context, key string, data []byte) error {
	if s.store == nil {
		return merr.Wrap(merr.ErrServiceUnimplemented, "source store is read-only")
	}
	return objectstorage.ToMilvusIoError(key, s.store.Write(ctx, objectstore.Object{Bucket: s.bucket, Key: key}, data))
}
func (s *chunkManagerAdapter) Remove(context.Context, string) error {
	return merr.Wrap(merr.ErrServiceUnimplemented, "caller owns snapshot cleanup")
}
func (s *chunkManagerAdapter) MultiRemove(context.Context, []string) error {
	return merr.Wrap(merr.ErrServiceUnimplemented, "caller owns snapshot cleanup")
}

func copyRange(ctx context.Context, snapshot *SnapshotData, sourceCM ChunkManager, sourceConfig *indexpb.StorageConfig, resolved *ResolvedForeignStorage, targetRoot string, r segmentRange, permits chan struct{}, readManifest ManifestReader, progress func(int64, int64, int64)) (*rangeResult, error) {
	if r.Start < 0 || r.End <= r.Start || r.End > int64(len(snapshot.ManifestPaths)) {
		return nil, merr.WrapErrDataIntegrityMsg("invalid snapshot manifest range")
	}
	if typeutil.IsExternalCollection(snapshot.Collection.GetSchema()) {
		return nil, merr.WrapErrParameterInvalidMsg("exporting external collections is not supported")
	}
	if readManifest == nil {
		readManifest = nativeManifestReader(sourceConfig)
	}
	data := *snapshot
	data.ManifestPaths = snapshot.ManifestPaths[r.Start:r.End]
	result := &rangeResult{SourceMetadataURI: snapshot.MetadataPath, SnapshotID: snapshot.SnapshotInfo.GetId(), CollectionID: snapshot.SnapshotInfo.GetCollectionId(), TargetRoot: targetRoot, Range: r}
	var mu sync.Mutex
	var statsErr error
	update := func(dc, db, dt int64) {
		mu.Lock()
		defer mu.Unlock()
		if db < 0 || dc < 0 || dt < 0 || db > math.MaxInt64-result.CopiedBytes || dc > math.MaxInt64-result.CopiedFiles || dt > math.MaxInt64-result.TotalFiles {
			statsErr = merr.WrapErrDataIntegrityMsg("export statistics overflow")
			return
		}
		result.CopiedFiles += dc
		result.CopiedBytes += db
		result.TotalFiles += dt
		if progress != nil {
			progress(result.CopiedFiles, result.CopiedBytes, result.TotalFiles)
		}
	}
	err := NewSnapshotReader(sourceCM, WithManifestReadPermits(permits)).visitSnapshotSegments(ctx, &data, data.oldRoot, data.newRoot, data.shouldRebase, func(ctx context.Context, one *SnapshotData) error {
		segment := one.Segments[0]
		// The visitor already binds the Avro identity; require its native entry point.
		if segment.GetStorageVersion() == StorageV3 && segment.GetNumOfRows() > 0 && segment.GetManifestPath() == "" {
			return merr.WrapErrDataIntegrityMsg("snapshot metadata is missing storage manifest for segment %d", segment.GetSegmentId())
		}
		bucket := sourceBucket(sourceCM, sourceConfig)
		items, err := buildSnapshotExportFiles(ctx, sourceCM, resolved.ForeignCM, bucket, resolved.ForeignBucket, one, targetRoot, permits, readManifest)
		if err != nil {
			return err
		}
		sourceRoot, err := snapshotExportSourceRoot(one, sourceCM.RootPath())
		if err != nil {
			return err
		}
		if err := copySnapshotExportSegment(ctx, bucket, items, one, sourceRoot, NormalizeSnapshotObjectPath(targetRoot), resolved, permits, update); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if statsErr != nil {
		return nil, statsErr
	}
	return result, nil
}

func prepareRanges(ctx context.Context, snapshot *SnapshotData, targetRoot string, resolved *ResolvedForeignStorage, results []rangeResult) (*publication, error) {
	sourceRoot, _ := DeriveSnapshotRootPath(snapshot.MetadataPath)
	plan, err := buildSnapshotExportMetadataPlan(snapshot, sourceRoot, targetRoot)
	if err != nil {
		return nil, err
	}
	// Empty snapshots also need overlap protection; they have no range copy.
	sourceBucket, _, _, err := ParseForeignURI(snapshot.MetadataPath)
	if err != nil {
		return nil, err
	}
	if sourceBucket == resolved.ForeignBucket && NormalizeSnapshotObjectPath(plan.MetadataURI) == NormalizeSnapshotObjectPath(snapshot.MetadataPath) {
		return nil, merr.WrapErrParameterInvalidMsg("export metadata would overwrite its source")
	}
	ranges := append([]rangeResult(nil), results...)
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].Range.Start < ranges[j].Range.Start })
	var next, totalFiles, totalBytes int64
	for _, r := range ranges {
		if r.SourceMetadataURI != snapshot.MetadataPath || r.SnapshotID != snapshot.SnapshotInfo.GetId() || r.CollectionID != snapshot.SnapshotInfo.GetCollectionId() || r.TargetRoot != targetRoot || r.Range.Start != next || r.Range.End <= next || r.Range.End > int64(len(snapshot.ManifestPaths)) || r.TotalFiles < r.Range.End-r.Range.Start || r.CopiedFiles != r.TotalFiles || r.CopiedBytes < 0 {
			return nil, merr.WrapErrDataIntegrityMsg("incomplete or conflicting snapshot range result")
		}
		if r.CopiedFiles > math.MaxInt64-totalFiles || r.CopiedBytes > math.MaxInt64-totalBytes {
			return nil, merr.WrapErrDataIntegrityMsg("export statistics overflow")
		}
		next = r.Range.End
		totalFiles += r.CopiedFiles
		totalBytes += r.CopiedBytes
	}
	if next != int64(len(snapshot.ManifestPaths)) {
		return nil, merr.WrapErrDataIntegrityMsg("incomplete export range coverage")
	}
	if err := prepareSnapshotExportMetadata(ctx, resolved.ForeignCM, snapshot, plan); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	staging, err := snapshotTargetObjectURI(targetRoot, GetSnapshotStagingMetadataPath(plan.TargetRoot))
	if err != nil {
		return nil, err
	}
	return &publication{StagingMetadataURI: staging, MetadataURI: plan.MetadataURI, TotalFiles: totalFiles, TotalBytes: totalBytes}, nil
}

func sourceBucket(cm ChunkManager, config *indexpb.StorageConfig) string {
	if config != nil {
		return config.GetBucketName()
	}
	if source, ok := cm.(bucketChunkManager); ok {
		return source.BucketName()
	}
	return ""
}

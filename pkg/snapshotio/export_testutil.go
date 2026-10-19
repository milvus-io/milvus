//go:build test

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

	objectstore "github.com/milvus-io/milvus/pkg/v3/objectstorage/store"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

// These bridges let Milvus integration fixtures exercise the shared planners
// without making internal server dependencies part of this module's tests.
// SnapshotExportTestPlan composes worker copies and coordinator metadata for fixtures.
type SnapshotExportTestPlan struct {
	*SnapshotExportPlan
	Items      []FileCopy
	snapshot   *SnapshotData
	sourceRoot string
}

// BuildSnapshotExportPlanForTest exposes real planners only in this package's tests.
func BuildSnapshotExportPlanForTest(ctx context.Context, sourceCM, targetCM ChunkManager,
	sourceBucket, targetBucket string, snapshot *SnapshotData, targetPath string,
	targetConfig, sourceConfig *indexpb.StorageConfig,
) (*SnapshotExportTestPlan, error) {
	targetPath, err := BuildStorageConfigSnapshotURI(targetConfig, NormalizeSnapshotObjectPath(targetPath))
	if err != nil {
		return nil, err
	}
	items, err := buildSnapshotExportFiles(ctx, sourceCM, targetCM, sourceBucket, targetBucket, snapshot, targetPath, nil, nativeManifestReader(sourceConfig))
	if err != nil {
		return nil, err
	}
	plan, err := buildSnapshotExportMetadataPlan(snapshot, sourceCM.RootPath(), targetPath)
	if err != nil {
		return nil, err
	}
	root, err := snapshotExportSourceRoot(snapshot, sourceCM.RootPath())
	if err != nil {
		return nil, err
	}
	return &SnapshotExportTestPlan{SnapshotExportPlan: plan, Items: items, snapshot: snapshot, sourceRoot: root}, nil
}

// CopySnapshotExportPlanForTest executes the real shard stages for fixtures.
func CopySnapshotExportPlanForTest(ctx context.Context, plan *SnapshotExportTestPlan, sourceBucket, targetBucket string, targetStore objectstore.ObjectStore, targetCM ChunkManager, permits chan struct{}, completed func(int64)) error {
	return copySnapshotExportSegment(ctx, sourceBucket, plan.Items, plan.snapshot, plan.sourceRoot, plan.TargetRoot,
		&ResolvedForeignStorage{TargetStore: targetStore, ForeignCM: targetCM, ForeignBucket: targetBucket}, permits, func(files, bytes, _ int64) {
			if completed != nil && files > 0 {
				completed(bytes)
			}
		})
}

// BuildSnapshotExportFilesForTest exposes the worker inventory without finalization.
func BuildSnapshotExportFilesForTest(ctx context.Context, sourceCM, targetCM ChunkManager,
	sourceBucket, targetBucket string, snapshot *SnapshotData, targetPath string,
	targetConfig, sourceConfig *indexpb.StorageConfig,
) ([]FileCopy, error) {
	targetPath, err := BuildStorageConfigSnapshotURI(targetConfig, NormalizeSnapshotObjectPath(targetPath))
	if err != nil {
		return nil, err
	}
	return buildSnapshotExportFiles(ctx, sourceCM, targetCM, sourceBucket, targetBucket, snapshot, targetPath, nil, nativeManifestReader(sourceConfig))
}

func PrepareSnapshotExportMetadataForTest(ctx context.Context, cm ChunkManager, snapshot *SnapshotData, plan *SnapshotExportPlan) error {
	return prepareSnapshotExportMetadata(ctx, cm, snapshot, plan)
}

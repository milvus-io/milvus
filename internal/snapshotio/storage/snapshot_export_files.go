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
	"path"
	"path/filepath"

	milvusstorage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// listSnapshotExportFiles inventories V3 data from the pinned manifest and
// takes vector/scalar indexes exclusively from Avro, as Restore does.
func listSnapshotExportFiles(ctx context.Context, cm milvusstorage.ChunkManager, snapshot *SnapshotData,
	config *indexpb.StorageConfig, sourceRoot, targetRoot, sourceBucket string, permits chan struct{},
) ([]SnapshotFileRef, error) {
	if permits != nil && cap(permits) == 0 {
		return nil, merr.WrapErrServiceInternalMsg("export manifest permits must be buffered")
	}
	collector := &snapshotFileRefCollector{byPath: make(map[string]SnapshotFileRef)}
	inventory := func(segment *datapb.SegmentDescription) error {
		if permits != nil {
			select {
			case permits <- struct{}{}:
				defer func() { <-permits }()
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if segment.GetStorageVersion() >= milvusstorage.StorageV3 {
			if err := collector.addExportManifestFiles(ctx, segment, config, sourceRoot, targetRoot); err != nil {
				return merr.Wrapf(err, "inventory segment %d", segment.GetSegmentId())
			}
		} else {
			collector.addLegacySegment(segment)
		}
		// The outer segment visitor waits for this native read to return.
		// Never publish inventory produced after cancellation.
		return ctx.Err()
	}
	for _, segment := range snapshot.Segments {
		if err := inventory(segment); err != nil {
			return nil, err
		}
	}
	for _, ref := range collector.byPath {
		if err := ValidateSnapshotObjectPathForBucket(cm, "export source", ref.Path, sourceBucket); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return collector.refs(), nil
}

func (c *snapshotFileRefCollector) addExportManifestFiles(ctx context.Context, segment *datapb.SegmentDescription,
	config *indexpb.StorageConfig, sourceRoot, targetRoot string,
) error {
	base, _, err := packed.UnmarshalManifestPath(segment.GetManifestPath())
	if err != nil {
		return merr.Wrap(err, "parse export manifest descriptor")
	}
	inventory, err := CollectStorageV3DataFiles(ctx, segment, config)
	if err != nil {
		return err
	}
	files := inventory.Manifest
	base = NormalizeSnapshotObjectPath(base)
	destinationBase := exportedSnapshotPath(sourceRoot, base, targetRoot)
	c.add(SnapshotFileRef{Path: base, Type: SnapshotFileTypeStorageV3ManifestRoot, SegmentID: segment.GetSegmentId()})
	add := func(file string, kind SnapshotFileType) error {
		if err := validateSnapshotPathReference(file); err != nil {
			return err
		}
		normalized := NormalizeSnapshotObjectPath(file)
		destination := exportedSnapshotPath(sourceRoot, normalized, targetRoot)
		// Copying the manifest unchanged must preserve each resolved relative
		// reference, even when it walks out to partition LOBs or legacy indexes.
		relative, err := filepath.Rel(base, normalized)
		if err != nil || path.Join(destinationBase, filepath.ToSlash(relative)) != destination {
			return merr.WrapErrDataIntegrityMsg("manifest reference cannot retain its relative layout: %q", file)
		}
		c.add(SnapshotFileRef{Path: file, Type: kind, SegmentID: segment.GetSegmentId()})
		return nil
	}
	for _, ref := range inventory.Files {
		switch ref.Type {
		case SnapshotFileTypeStorageV3ManifestFile, SnapshotFileTypeStorageV3ManifestObject, SnapshotFileTypeStorageV3LOBFile:
			if err := add(ref.Path, ref.Type); err != nil {
				return err
			}
		default:
			// PB supplements are independent physical logs, not relative references
			// in the native manifest. Their destination is handled by the export plan.
			c.add(ref)
		}
	}
	// Native indexes may include historical artifacts absent from the snapshot.
	// An empty Avro list is authoritative too; never add inherited entries.
	for _, index := range segment.GetIndexFiles() {
		for _, file := range index.GetIndexFilePaths() {
			if err := add(file, SnapshotFileTypeIndexFile); err != nil {
				return err
			}
		}
	}
	return validateStorageV3LOBFiles(segment, base, destinationBase, sourceRoot, targetRoot, files.LobFiles)
}

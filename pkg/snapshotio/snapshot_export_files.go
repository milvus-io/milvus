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
	"path"
	"path/filepath"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	packed "github.com/milvus-io/milvus/pkg/v3/snapshotio/internal/native"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// listSnapshotExportFiles inventories V3 data from the pinned manifest and
// takes vector/scalar indexes exclusively from Avro, as Restore does.
func listSnapshotExportFiles(ctx context.Context, cm ChunkManager, snapshot *SnapshotData,
	sourceRoot, targetRoot, sourceBucket string, permits chan struct{}, readManifest ManifestReader,
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
		inventory, err := EnumerateSegmentFiles(ctx, segment, readManifest)
		if err != nil {
			return err
		}
		// Validate original URI ownership before aliases can be deduplicated.
		for _, ref := range inventory.Files {
			if err := ValidateSnapshotObjectPathForBucket(cm, "export source", ref.Path, sourceBucket); err != nil {
				return err
			}
		}
		if segment.GetStorageVersion() >= StorageV3 {
			if err := collector.addExportManifestFiles(segment, sourceRoot, targetRoot, inventory); err != nil {
				return merr.Wrapf(err, "inventory segment %d", segment.GetSegmentId())
			}
		} else {
			for _, ref := range inventory.Files {
				collector.add(ref)
			}
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

func (c *snapshotFileRefCollector) addExportManifestFiles(segment *datapb.SegmentDescription,
	sourceRoot, targetRoot string, inventory *SegmentInventory,
) error {
	base, _, err := packed.UnmarshalManifestPath(segment.GetManifestPath())
	if err != nil {
		return merr.Wrap(err, "parse export manifest descriptor")
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
		case SnapshotFileTypeStorageV3ManifestFile, SnapshotFileTypeStorageV3ManifestObject, SnapshotFileTypeStorageV3LOBFile, SnapshotFileTypeIndexFile:
			if err := add(ref.Path, ref.Type); err != nil {
				return err
			}
		default:
			// PB supplements are independent physical logs, not relative references
			// in the native manifest. Their destination is handled by the export plan.
			c.add(ref)
		}
	}
	return validateStorageV3LOBFiles(segment, base, destinationBase, sourceRoot, targetRoot, files.LobFiles)
}

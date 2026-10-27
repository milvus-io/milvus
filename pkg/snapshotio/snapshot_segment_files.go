// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package snapshotio

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/snapshotio/format"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// SegmentInventory retains raw references until callers select and validate copy inputs.
// Manifest is nil for legacy segments; otherwise it comes from the same pinned read.
type SegmentInventory struct {
	Files    []SnapshotFileRef
	Manifest *format.NativeManifestFiles
}

// EnumerateSegmentFiles inventories source references without normalization, deduplication,
// destination mapping, or I/O beyond one pinned native manifest read. Callers must select
// and validate references before planning copies. Native index entries are metadata only:
// the snapshot's IndexFiles list is authoritative, including an empty list.
func EnumerateSegmentFiles(ctx context.Context, segment *datapb.SegmentDescription,
	readManifest ManifestReader,
) (*SegmentInventory, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if segment == nil {
		return nil, merr.WrapErrServiceInternalMsg("segment is nil")
	}
	collector := &snapshotFileRefCollector{raw: true}
	if segment.GetStorageVersion() < StorageV3 {
		collector.addLegacySegment(segment)
		return &SegmentInventory{Files: collector.refs()}, nil
	}
	if readManifest == nil {
		return nil, merr.WrapErrServiceInternalMsg("manifest reader is nil")
	}
	files, err := readManifest(ctx, segment.GetManifestPath())
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if files == nil {
		return nil, merr.WrapErrServiceInternalMsg("manifest inventory is nil")
	}
	if files.ManifestFile == "" && segment.GetNumOfRows() > 0 {
		return nil, merr.WrapErrDataIntegrityMsg("nonempty segment has no physical manifest")
	}
	for _, paths := range [][]string{files.ColumnFiles, files.DeltaFiles, files.StatsFiles} {
		for _, file := range paths {
			collector.add(SnapshotFileRef{Path: file, Type: SnapshotFileTypeStorageV3ManifestObject, SegmentID: segment.GetSegmentId()})
		}
	}
	collector.add(SnapshotFileRef{Path: files.ManifestFile, Type: SnapshotFileTypeStorageV3ManifestFile, SegmentID: segment.GetSegmentId()})
	for _, lob := range files.LobFiles {
		collector.add(SnapshotFileRef{Path: lob.Path, Type: SnapshotFileTypeStorageV3LOBFile, SegmentID: segment.GetSegmentId()})
	}
	collector.addSegmentSupplements(segment)
	return &SegmentInventory{Files: collector.refs(), Manifest: files}, nil
}

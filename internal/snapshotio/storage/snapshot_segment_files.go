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

package storage

import (
	"context"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// StorageV3DataFiles is the shared physical data inventory for ordinary V3
// snapshot segments. Manifest retains index and LOB metadata from the same read;
// index selection and destination layout remain the caller's responsibility.
type StorageV3DataFiles struct {
	Files    []SnapshotFileRef
	Manifest *packed.ManifestFiles
}

// CollectStorageV3DataFiles reads the pinned manifest once, then supplements it
// with physical delta/stats/BM25 paths captured in snapshot metadata. Empty PB
// paths are logical summaries, not copy tasks. V3 insert and TEXT/JSON PB paths
// are not authoritative physical references. No LIST or object-size query is used.
func CollectStorageV3DataFiles(ctx context.Context, segment *datapb.SegmentDescription,
	config *indexpb.StorageConfig,
) (*StorageV3DataFiles, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	files, err := packed.ReadManifestFiles(segment.GetManifestPath(), config)
	if err != nil {
		return nil, err
	}
	// Native reads cannot be canceled mid-call. Do not return a usable inventory
	// if cancellation arrived while the native reader was running.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if files.ManifestFile == "" && segment.GetNumOfRows() > 0 {
		return nil, merr.WrapErrDataIntegrityMsg("nonempty segment has no physical manifest")
	}
	collector := &snapshotFileRefCollector{byPath: make(map[string]SnapshotFileRef)}
	for _, paths := range [][]string{files.ColumnFiles, files.DeltaFiles, files.StatsFiles} {
		for _, file := range paths {
			collector.add(SnapshotFileRef{Path: file, Type: SnapshotFileTypeStorageV3ManifestObject, SegmentID: segment.GetSegmentId()})
		}
	}
	collector.add(SnapshotFileRef{Path: files.ManifestFile, Type: SnapshotFileTypeStorageV3ManifestFile, SegmentID: segment.GetSegmentId()})
	for _, lob := range files.LobFiles {
		collector.add(SnapshotFileRef{Path: lob.Path, Type: SnapshotFileTypeStorageV3LOBFile, SegmentID: segment.GetSegmentId()})
	}
	collector.addFieldBinlogRefs(segment.GetDeltalogs(), segment, SnapshotFileTypeDeltaBinlog)
	collector.addFieldBinlogRefs(segment.GetStatslogs(), segment, SnapshotFileTypeStatsBinlog)
	collector.addFieldBinlogRefs(segment.GetBm25Statslogs(), segment, SnapshotFileTypeBM25StatsBinlog)
	refs := collector.refs()
	for _, ref := range refs {
		if err := validateSnapshotPathReference(ref.Path); err != nil {
			return nil, err
		}
	}
	return &StorageV3DataFiles{Files: refs, Manifest: files}, nil
}

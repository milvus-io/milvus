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

package inspector

import (
	"strconv"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// LocateRawDataV3 enumerates raw objects from the committed manifest, not the
// legacy binlog metadata. These campaigns explicitly write parquet so the
// independent encrypted-footer inspector can verify every physical object.
func LocateRawDataV3(config *indexpb.StorageConfig, segments []*datapb.SegmentInfo) ([]RawDataObject, error) {
	if len(segments) == 0 {
		return nil, merr.WrapErrDataIntegrityMsg("no sealed segments were reported")
	}
	collectionID := segments[0].GetCollectionID()
	var objects []RawDataObject
	paths := make(map[string]struct{})
	for _, segment := range segments {
		if segment.GetCollectionID() != collectionID || segment.GetStorageVersion() != storage.StorageV3 {
			return nil, merr.WrapErrDataIntegrityMsg("segment %d must belong to collection %d and use StorageV3", segment.GetID(), collectionID)
		}
		_, version, err := packed.UnmarshalManifestPath(segment.GetManifestPath())
		if err != nil {
			return nil, merr.Wrap(err, "parse raw-data manifest")
		}
		if version <= 0 {
			return nil, merr.WrapErrDataIntegrityMsg("segment %d has no committed manifest", segment.GetID())
		}
		groups, err := packed.ReadManifestColumnGroups(segment.GetManifestPath(), config)
		if err != nil {
			return nil, merr.Wrapf(err, "read raw-data manifest for segment %d", segment.GetID())
		}
		if len(groups) == 0 {
			return nil, merr.WrapErrDataIntegrityMsg("segment %d has no manifest column groups", segment.GetID())
		}
		for _, group := range groups {
			if group.Format != "parquet" || len(group.Columns) == 0 || len(group.Files) == 0 {
				return nil, merr.WrapErrDataIntegrityMsg("segment %d requires nonempty parquet column groups", segment.GetID())
			}
			// FieldID identifies the group for diagnostics; every file is inspected,
			// including groups holding several scalar fields.
			fieldID, err := strconv.ParseInt(group.Columns[0], 10, 64)
			if err != nil {
				return nil, merr.Wrap(err, "parse manifest column field ID")
			}
			for _, file := range group.Files {
				if file.Path == "" {
					return nil, merr.WrapErrDataIntegrityMsg("segment %d has an empty raw-data object path", segment.GetID())
				}
				if _, duplicate := paths[file.Path]; duplicate {
					return nil, merr.WrapErrDataIntegrityMsg("raw-data object path %q is reported more than once", file.Path)
				}
				paths[file.Path] = struct{}{}
				objects = append(objects, RawDataObject{
					CollectionID: collectionID, PartitionID: segment.GetPartitionID(), SegmentID: segment.GetID(),
					FieldID: fieldID, Path: file.Path, StorageVersion: storage.StorageV3,
				})
			}
		}
	}
	return objects, nil
}

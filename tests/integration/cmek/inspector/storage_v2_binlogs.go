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
	"fmt"

	binlogutil "github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

type RawDataObject struct {
	CollectionID   int64
	PartitionID    int64
	SegmentID      int64
	FieldID        int64
	Path           string
	StorageVersion int64
}

// LocateRawDataV2 enumerates the complete authoritative FieldBinlog/Binlog set
// for every current sealed segment. Current metadata may compress a path to
// its log ID; in that case the canonical metadata codec expands it using the
// storage root instead of guessing an object prefix.
func LocateRawDataV2(rootPath string, segments []*datapb.SegmentInfo) ([]RawDataObject, error) {
	if len(segments) == 0 {
		return nil, fmt.Errorf("no sealed segments were reported")
	}
	objects := make([]RawDataObject, 0)
	paths := make(map[string]struct{})
	collectionID := segments[0].GetCollectionID()
	for _, segment := range segments {
		if segment.GetCollectionID() != collectionID {
			return nil, fmt.Errorf("segment %d belongs to collection %d, want %d",
				segment.GetID(), segment.GetCollectionID(), collectionID)
		}
		if segment.GetStorageVersion() != storage.StorageV2 {
			return nil, fmt.Errorf("segment %d reported storage version %d, want %d",
				segment.GetID(), segment.GetStorageVersion(), storage.StorageV2)
		}
		if len(segment.GetBinlogs()) == 0 {
			return nil, fmt.Errorf("segment %d has no raw-data FieldBinlog metadata", segment.GetID())
		}
		for _, fieldBinlog := range segment.GetBinlogs() {
			if len(fieldBinlog.GetBinlogs()) == 0 {
				return nil, fmt.Errorf("segment %d field %d has no raw-data Binlog metadata",
					segment.GetID(), fieldBinlog.GetFieldID())
			}
			for _, binlog := range fieldBinlog.GetBinlogs() {
				objectPath := binlog.GetLogPath()
				if objectPath == "" {
					if binlog.GetLogID() <= 0 {
						return nil, fmt.Errorf("segment %d field %d has neither a raw-data object path nor a valid log ID",
							segment.GetID(), fieldBinlog.GetFieldID())
					}
					var err error
					objectPath, err = binlogutil.BuildLogPathWithRootPath(rootPath, storage.InsertBinlog,
						segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID(), fieldBinlog.GetFieldID(), binlog.GetLogID())
					if err != nil {
						return nil, fmt.Errorf("expand raw-data log ID for segment %d field %d: %w",
							segment.GetID(), fieldBinlog.GetFieldID(), err)
					}
				}
				if _, duplicate := paths[objectPath]; duplicate {
					return nil, fmt.Errorf("raw-data object path %q is reported more than once", objectPath)
				}
				paths[objectPath] = struct{}{}
				objects = append(objects, RawDataObject{
					CollectionID: segment.GetCollectionID(), PartitionID: segment.GetPartitionID(),
					SegmentID: segment.GetID(), FieldID: fieldBinlog.GetFieldID(), Path: objectPath,
					StorageVersion: segment.GetStorageVersion(),
				})
			}
		}
	}
	return objects, nil
}

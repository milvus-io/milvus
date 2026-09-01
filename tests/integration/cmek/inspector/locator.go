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
	"context"
	"fmt"
	"path"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
)

type Object struct {
	SegmentID     int64
	FieldID       int64
	Path          string
	EngineVersion int32
}

// LocateScalarIndex resolves final scalar-index objects through the public
// segment view and DataCoord's GetIndexInfos response. DataCoord currently
// ignores GetIndexInfoRequest.IndexName and rewrites each finished segment
// index's response IndexName to its scalar index type, so indexType is
// intentionally not the user-facing CreateIndex name. The returned paths are
// deliberately not reconstructed or prefixed by the test.
func LocateScalarIndex(ctx context.Context,
	client types.MixCoordClient,
	segments []*datapb.SegmentInfo,
	indexType string,
	fieldID int64,
	expectedEngineVersion int32,
) ([]Object, error) {
	if len(segments) == 0 {
		return nil, fmt.Errorf("no sealed segments were reported")
	}
	collectionID := segments[0].GetCollectionID()
	segmentIDs := make([]int64, 0, len(segments))
	for _, segment := range segments {
		segmentIDs = append(segmentIDs, segment.GetID())
	}

	response, err := client.GetIndexInfos(ctx, &indexpb.GetIndexInfoRequest{
		CollectionID: collectionID,
		SegmentIDs:   segmentIDs,
	})
	if err = merr.CheckRPCCall(response, err); err != nil {
		return nil, err
	}

	objects := make([]Object, 0)
	for _, segment := range segments {
		segmentInfo, ok := response.GetSegmentInfo()[segment.GetID()]
		if !ok || segmentInfo == nil {
			return nil, fmt.Errorf("missing index metadata for segment %d", segment.GetID())
		}
		found := false
		for _, indexInfo := range segmentInfo.GetIndexInfos() {
			if indexInfo.GetFieldID() != fieldID || indexInfo.GetIndexName() != indexType {
				continue
			}
			found = true
			if indexInfo.GetCurrentScalarIndexVersion() != expectedEngineVersion {
				return nil, fmt.Errorf("segment %d field %d reported scalar engine %d, want %d",
					segment.GetID(), fieldID, indexInfo.GetCurrentScalarIndexVersion(), expectedEngineVersion)
			}
			if len(indexInfo.GetIndexFilePaths()) == 0 {
				return nil, fmt.Errorf("segment %d field %d has no index objects", segment.GetID(), fieldID)
			}
			for _, path := range indexInfo.GetIndexFilePaths() {
				if path == "" {
					return nil, fmt.Errorf("segment %d field %d has an empty index object path", segment.GetID(), fieldID)
				}
				objects = append(objects, Object{
					SegmentID:     segment.GetID(),
					FieldID:       fieldID,
					Path:          path,
					EngineVersion: indexInfo.GetCurrentScalarIndexVersion(),
				})
			}
		}
		if !found {
			return nil, fmt.Errorf("missing index metadata for segment %d field %d index %q",
				segment.GetID(), fieldID, indexType)
		}
	}
	return objects, nil
}

// LocateTextLog resolves TextMatch output from SegmentInfo.TextStatsLogs.
// TextStatsLogs identifies the text_log objects. Legacy metadata stores only
// the object filenames, so the authoritative segment/text-stat IDs are used
// with Milvus's path helper to obtain the exact object keys.
func LocateTextLog(rootPath string, segments []*datapb.SegmentInfo,
	fieldID int64,
	expectedEngineVersion int32,
) ([]Object, error) {
	if len(segments) == 0 {
		return nil, fmt.Errorf("no sealed segments were reported")
	}
	objects := make([]Object, 0)
	for _, segment := range segments {
		stats, ok := segment.GetTextStatsLogs()[fieldID]
		if !ok || stats == nil {
			return nil, fmt.Errorf("missing text stats metadata for segment %d field %d", segment.GetID(), fieldID)
		}
		if stats.GetCurrentScalarIndexVersion() != expectedEngineVersion {
			return nil, fmt.Errorf("segment %d field %d reported scalar engine %d, want %d",
				segment.GetID(), fieldID, stats.GetCurrentScalarIndexVersion(), expectedEngineVersion)
		}
		basePath := metautil.BuildTextIndexPrefix(rootPath,
			stats.GetBuildID(), stats.GetVersion(),
			segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID(), fieldID)
		files := stats.GetFiles()
		if len(files) == 0 {
			return nil, fmt.Errorf("segment %d field %d has no text_log objects", segment.GetID(), fieldID)
		}
		for _, file := range files {
			if file == "" {
				return nil, fmt.Errorf("segment %d field %d has an empty text_log object path", segment.GetID(), fieldID)
			}
			objectPath := file
			if basePath != "" {
				objectPath = path.Join(basePath, file)
			}
			objects = append(objects, Object{
				SegmentID:     segment.GetID(),
				FieldID:       fieldID,
				Path:          objectPath,
				EngineVersion: stats.GetCurrentScalarIndexVersion(),
			})
		}
	}
	return objects, nil
}

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

package index

import (
	"context"
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func FilterInIndexedSegments(ctx context.Context, handler meta.Handler, indexMeta *IndexMeta, skipNoIndexCollection bool, segments ...*meta.SegmentInfo) []*meta.SegmentInfo {
	if len(segments) == 0 {
		return nil
	}

	if ctx.Err() != nil {
		return nil
	}

	collectionSegments := lo.GroupBy(segments, func(segment *meta.SegmentInfo) int64 {
		return segment.GetCollectionID()
	})

	ret := make([]*meta.SegmentInfo, 0)
	for collection, segmentList := range collectionSegments {
		// No segments will be filtered if there are no indices in the collection.
		if skipNoIndexCollection && !indexMeta.HasIndex(collection) {
			ret = append(ret, segmentList...)
			continue
		}

		timeoutCtx, cancel := context.WithTimeout(ctx, time.Second*2)

		coll, err := handler.GetCollection(timeoutCtx, collection)
		cancel()
		if err != nil {
			mlog.Warn(ctx, "failed to get collection schema", mlog.Err(err))
			continue
		}

		// get vector field id
		var targetFieldIds []int64
		// wait all vector datatype fields only
		for _, field := range coll.Schema.GetFields() {
			if typeutil.IsVectorType(field.GetDataType()) {
				targetFieldIds = append(targetFieldIds, field.GetFieldID())
			}
		}

		// include all scalar fields with index
		if paramtable.Get().DataCoordCfg.DVForceAllIndexReady.GetAsBool() {
			indices := indexMeta.GetIndexesForCollection(collection, "")
			for _, index := range indices {
				targetFieldIds = append(targetFieldIds, index.FieldID)
			}
		}
		segmentIDs := lo.Map(segmentList, func(seg *meta.SegmentInfo, _ int) UniqueID {
			return seg.GetID()
		})

		// get indexed segments which finish build index on all vector field
		indexed := indexMeta.GetIndexedSegments(collection, segmentIDs, targetFieldIds)
		if len(indexed) > 0 {
			indexedSet := typeutil.NewUniqueSet(indexed...)
			for _, segment := range segmentList {
				if !meta.IsFlushState(segment.GetState()) && segment.GetState() != commonpb.SegmentState_Dropped {
					continue
				}

				if indexedSet.Contain(segment.GetID()) {
					ret = append(ret, segment)
				}
			}
		}
	}

	return ret
}

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
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// FilterInHandoffReadySegments returns the flushed or dropped segments that may
// replace their parents in the query view. The indexes they wait for are
// meta.HandoffIndexTargetFields: every vector index; for a collection without a
// vector field every index defined on it; with forceAllIndexReady every index
// for any collection. A collection with no index defined has nothing to wait
// for and all its segments are returned. This is the read-time counterpart of
// the write-time predicate meta.PrepareReadiness, and the readiness rule for
// compaction outputs, for the serving frontier behind DescribeIndex, and for
// retiring compacted parents in garbage collection.
func FilterInHandoffReadySegments(ctx context.Context, handler meta.Handler, indexMeta *IndexMeta, segments ...*meta.SegmentInfo) []*meta.SegmentInfo {
	return filterIndexedSegments(ctx, handler, indexMeta, true, func(coll *meta.CollectionInfo) []int64 {
		return meta.HandoffIndexTargetFields(indexMeta, coll)
	}, segments...)
}

// FilterInCompactionInputReadySegments returns the flushed or dropped segments
// that may be fed into another compaction: every vector field of their
// collection, struct vector sub-fields included, has a built index on them.
// Scalar indexes are not part of the rule, because each compaction output
// rebuilds them and waiting for them on the input side only delays the
// compaction. A collection with no vector field has nothing to wait for and
// all its segments are returned. With skipNoIndexCollection, a collection with
// no index defined at all is not filtered either (force compaction and target
// reconciliation use this).
func FilterInCompactionInputReadySegments(ctx context.Context, handler meta.Handler, indexMeta *IndexMeta, skipNoIndexCollection bool, segments ...*meta.SegmentInfo) []*meta.SegmentInfo {
	return filterIndexedSegments(ctx, handler, indexMeta, skipNoIndexCollection, func(coll *meta.CollectionInfo) []int64 {
		var fieldIDs []int64
		for _, field := range typeutil.GetVectorFieldSchemas(coll.Schema) {
			fieldIDs = append(fieldIDs, field.GetFieldID())
		}
		return fieldIDs
	}, segments...)
}

// filterIndexedSegments keeps the flushed or dropped segments of each
// collection on which every field returned by targetFields has a finished
// index. An empty target set means nothing to wait for. With
// skipNoIndexCollection, a collection with no index defined is not filtered
// and its schema is not fetched.
func filterIndexedSegments(ctx context.Context, handler meta.Handler, indexMeta *IndexMeta, skipNoIndexCollection bool,
	targetFields func(coll *meta.CollectionInfo) []int64, segments ...*meta.SegmentInfo,
) []*meta.SegmentInfo {
	if len(segments) == 0 {
		return nil
	}

	if ctx.Err() != nil {
		return nil
	}

	collectionSegments := lo.GroupBy(segments, func(segment *meta.SegmentInfo) int64 {
		return segment.GetCollectionID()
	})

	flushedOrDropped := func(segmentList []*meta.SegmentInfo) []*meta.SegmentInfo {
		return lo.Filter(segmentList, func(segment *meta.SegmentInfo, _ int) bool {
			return meta.IsFlushState(segment.GetState()) || segment.GetState() == commonpb.SegmentState_Dropped
		})
	}

	ret := make([]*meta.SegmentInfo, 0)
	for collection, segmentList := range collectionSegments {
		// nothing to wait for when the collection has no index at all
		if skipNoIndexCollection && !indexMeta.HasIndex(collection) {
			ret = append(ret, flushedOrDropped(segmentList)...)
			continue
		}

		timeoutCtx, cancel := context.WithTimeout(ctx, time.Second*2)

		coll, err := handler.GetCollection(timeoutCtx, collection)
		cancel()
		if err != nil {
			mlog.Warn(ctx, "failed to get collection schema", mlog.Err(err))
			continue
		}

		targetFieldIds := targetFields(coll)
		if len(targetFieldIds) == 0 {
			// nothing to wait for
			ret = append(ret, flushedOrDropped(segmentList)...)
			continue
		}

		segmentIDs := lo.Map(segmentList, func(seg *meta.SegmentInfo, _ int) UniqueID {
			return seg.GetID()
		})

		// get indexed segments which finish build index on all target fields
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

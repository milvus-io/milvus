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

package datacoord

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func (s *DDLCallbacks) dropIndexV2Callback(ctx context.Context, result message.BroadcastResultDropIndexMessageV2) error {
	header := result.Message.Header()
	if err := s.meta.indexMeta.MarkIndexAsDeleted(ctx, header.GetCollectionId(), header.GetIndexIds()); err != nil {
		return err
	}

	// Deleted index definitions are excluded by GetSegmentIndexes. Reconcile
	// every segment in the collection so their aggregate footprint stops
	// counting the dropped vector indexes before asynchronous file GC runs.
	for _, segment := range s.meta.SelectSegments(ctx, WithCollection(header.GetCollectionId())) {
		if err := s.meta.syncVectorIndexSize(ctx, header.GetCollectionId(), segment.ID); err != nil {
			mlog.Warn(ctx, "failed to update vector index size after dropping index",
				mlog.FieldSegmentID(segment.ID), mlog.Err(err))
		}
	}
	return nil
}

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

	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type (
	DataViewManager                  = dataview.Manager
	FlushDataViewEvent               = dataview.FlushDataViewEvent
	ImportDataViewEvent              = dataview.ImportDataViewEvent
	CopySegmentCompleteDataViewEvent = dataview.CopySegmentCompleteDataViewEvent
	CompactDataViewEvent             = dataview.CompactDataViewEvent
	L0CompactDataViewEvent           = dataview.L0CompactDataViewEvent
	ExternalRefreshDataViewEvent     = dataview.ExternalRefreshDataViewEvent
	DropPartitionDataViewEvent       = dataview.DropPartitionDataViewEvent
	TruncateDataViewEvent            = dataview.TruncateDataViewEvent
)

type dataViewSegmentStore struct {
	meta *meta
}

func newDataViewManager(catalog metastore.DataCoordCatalog, meta *meta) DataViewManager {
	return dataview.NewManager(catalog, &dataViewSegmentStore{meta: meta})
}

func (s *Server) Snapshot(ctx context.Context, collectionIDs []int64) ([]*viewpb.DataViewOfCollection, error) {
	if s.dataViewManager == nil {
		return nil, nil
	}
	return s.dataViewManager.Snapshot(ctx, collectionIDs)
}

func (s *dataViewSegmentStore) GetSegment(ctx context.Context, segmentID int64) *dataview.Segment {
	return newDataViewSegment(s.meta.GetSegment(ctx, segmentID))
}

func (s *dataViewSegmentStore) SelectSegments(ctx context.Context, collectionID int64) []*dataview.Segment {
	segments := s.meta.SelectSegments(ctx, WithCollection(collectionID))
	result := make([]*dataview.Segment, 0, len(segments))
	validPartitions := s.validPartitions(collectionID)
	for _, segment := range segments {
		if validPartitions != nil {
			if _, ok := validPartitions[segment.GetPartitionID()]; !ok {
				continue
			}
		}
		result = append(result, newDataViewSegment(segment))
	}
	return result
}

func (s *dataViewSegmentStore) validPartitions(collectionID int64) map[int64]struct{} {
	collection := s.meta.GetCollection(collectionID)
	if collection == nil || len(collection.Partitions) == 0 {
		return nil
	}
	partitions := make(map[int64]struct{}, len(collection.Partitions))
	for _, partitionID := range collection.Partitions {
		partitions[partitionID] = struct{}{}
	}
	return partitions
}

func newDataViewSegment(segment *SegmentInfo) *dataview.Segment {
	if segment == nil {
		return nil
	}
	return &dataview.Segment{
		ID:                  segment.GetID(),
		CollectionID:        segment.GetCollectionID(),
		PartitionID:         segment.GetPartitionID(),
		InsertChannel:       segment.GetInsertChannel(),
		State:               segment.GetState(),
		Level:               segment.GetLevel(),
		IsImporting:         segment.GetIsImporting(),
		IsInvisible:         segment.GetIsInvisible(),
		DmlPosition:         segment.GetDmlPosition(),
		CommitTimestamp:     segment.GetCommitTimestamp(),
		CreatedByCompaction: segment.GetCreatedByCompaction(),
		CompactionFrom:      append([]int64(nil), segment.GetCompactionFrom()...),
	}
}

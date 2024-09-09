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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	invalidIndex = "invalid"
)

type IndexMeta interface {
	CanCreateIndex(req *indexpb.CreateIndexRequest) (typeutil.UniqueID, error)
	HasSameReq(req *indexpb.CreateIndexRequest) (bool, typeutil.UniqueID)
	CreateIndex(index *model.Index) error
	AlterIndex(ctx context.Context, indexes ...*model.Index) error
	AddSegmentIndex(segIndex *model.SegmentIndex) error
	GetIndexIDByName(collID int64, indexName string) map[int64]uint64
	GetSegmentIndexState(collID, segmentID typeutil.UniqueID, indexID typeutil.UniqueID) *indexpb.SegmentIndexState
	GetIndexedSegments(collectionID int64, segmentIDs, fieldIDs []typeutil.UniqueID) []int64
	GetIndexesForCollection(collID typeutil.UniqueID, indexName string) []*model.Index
	GetFieldIndexes(collID, fieldID typeutil.UniqueID, indexName string) []*model.Index
	MarkIndexAsDeleted(collID typeutil.UniqueID, indexIDs []typeutil.UniqueID) error
	IsUnIndexedSegment(collectionID typeutil.UniqueID, segID typeutil.UniqueID) bool
	GetSegmentIndexes(collectionID typeutil.UniqueID, segID typeutil.UniqueID) map[typeutil.UniqueID]*model.SegmentIndex
	GetFieldIDByIndexID(collID, indexID typeutil.UniqueID) typeutil.UniqueID
	GetIndexNameByID(collID, indexID typeutil.UniqueID) string
	GetIndexParams(collID, indexID typeutil.UniqueID) []*commonpb.KeyValuePair
	GetTypeParams(collID, indexID typeutil.UniqueID) []*commonpb.KeyValuePair
	GetIndexJob(buildID typeutil.UniqueID) (*model.SegmentIndex, bool)
	IsIndexExist(collID, indexID typeutil.UniqueID) bool
	UpdateVersion(buildID typeutil.UniqueID) error
	FinishTask(taskInfo *workerpb.IndexTaskInfo) error
	DeleteTask(buildID int64) error
	BuildIndex(buildID, nodeID typeutil.UniqueID) error
	GetAllSegIndexes() map[int64]*model.SegmentIndex
	RemoveSegmentIndex(collID, partID, segID, indexID, buildID typeutil.UniqueID) error
	GetDeletedIndexes() []*model.Index
	RemoveIndex(collID, indexID typeutil.UniqueID) error
	CheckCleanSegmentIndex(buildID typeutil.UniqueID) (bool, *model.SegmentIndex)
	GetMetasByNodeID(nodeID typeutil.UniqueID) []*model.SegmentIndex
	GetSegmentsIndexStates(collectionID typeutil.UniqueID, segmentIDs []typeutil.UniqueID) map[int64]map[int64]*indexpb.SegmentIndexState
	GetUnindexedSegments(collectionID int64, segmentIDs []int64) []int64
	AreAllDiskIndex(collectionID int64, schema *schemapb.CollectionSchema) bool
}

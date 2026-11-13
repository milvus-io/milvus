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

package meta

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
)

// SegmentIndexCatalog is what the manifest commit path and the segment
// readiness predicate need from the index meta. indexMeta implements it.
type SegmentIndexCatalog interface {
	// SetCollectionSchemaGetter binds the local schema cache before recovery.
	SetCollectionSchemaGetter(func(UniqueID) *schemapb.CollectionSchema)

	// GetIndexParams returns the index params of the index.
	GetIndexParams(collID, indexID UniqueID) []*commonpb.KeyValuePair
	// GetTypeParams returns the type params of the index.
	GetTypeParams(collID, indexID UniqueID) []*commonpb.KeyValuePair
	// GetFieldIDByIndexID returns the ID of the field the index is built on.
	GetFieldIDByIndexID(collID, indexID UniqueID) UniqueID
	// GetIndexNameByID returns the name of the index.
	GetIndexNameByID(collID, indexID UniqueID) string

	// HasIndex reports whether the collection has an index.
	HasIndex(collectionID int64) bool
	// GetPhysicalIndexesForCollection excludes logical-only index definitions.
	GetPhysicalIndexesForCollection(collID UniqueID) []*model.Index
	// GetIndexedSegments returns the segments in segmentIDs whose indexes on
	// fieldIDs are all finished.
	GetIndexedSegments(collectionID int64, segmentIDs, fieldIDs []UniqueID) []int64
}

// CompactionProtection answers whether a snapshot pins a segment or a
// collection against compaction. snapshotMeta implements it.
type CompactionProtection interface {
	// IsSegmentCompactionProtected reports whether an active snapshot protects the segment from compaction.
	IsSegmentCompactionProtected(segmentID int64) bool
	// IsCollectionCompactionBlocked reports whether compaction of the collection is blocked:
	// a snapshot's RefIndex is not loaded yet, or a snapshot of the collection is being created.
	IsCollectionCompactionBlocked(collectionID int64) bool
}

// DomainMetaLoader registers domain metadata loads in the segment loader's error group.
// Its result is read only after all registered loads finish.
type DomainMetaLoader func(ctx context.Context, collectionIDs []int64, group *errgroup.Group) func() (SegmentIndexCatalog, CompactionProtection)

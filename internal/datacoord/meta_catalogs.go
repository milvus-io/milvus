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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
)

// SegmentIndexCatalog is what the manifest commit path and the segment
// readiness predicate need from the index meta. indexMeta implements it.
type SegmentIndexCatalog interface {
	// GetIndexParams returns the index params of the index.
	GetIndexParams(collID, indexID UniqueID) []*commonpb.KeyValuePair
	// GetTypeParams returns the type params of the index.
	GetTypeParams(collID, indexID UniqueID) []*commonpb.KeyValuePair
	// GetFieldIDByIndexID returns the ID of the field the index is built on.
	GetFieldIDByIndexID(collID, indexID UniqueID) UniqueID
	// GetIndexNameByID returns the name of the index.
	GetIndexNameByID(collID, indexID UniqueID) string

	// HasBuildInfo reports whether a SegmentIndex record for the build ID is in memory.
	HasBuildInfo(buildID int64) bool
	// LockBuildIDs takes the write lock of each build ID, in ascending order
	// after removing duplicates, so overlapping callers cannot deadlock.
	// Callers must not hold segMu. Lock order: segment manifest lock, then
	// build-ID locks, then segMu.
	LockBuildIDs(buildIDs []int64)
	// UnlockBuildIDs releases the locks taken by LockBuildIDs. buildIDs must
	// not contain duplicates.
	UnlockBuildIDs(buildIDs []int64)
	// UpdateSegmentIndex stores the SegmentIndex record in memory. It does not
	// take the build-ID lock; the only caller today is the startup reload from
	// manifests, which does not hold it.
	UpdateSegmentIndex(segIdx *model.SegmentIndex)
	// AddStoredIndexSizeMetric adds delta to the stored index size gauge of the
	// index. It reads the index definition without taking fieldIndexLock or the
	// build-ID lock, so it is safe only when no index DDL can run at the same
	// time. The only caller today is the startup reload from manifests.
	AddStoredIndexSizeMetric(collID, indexID UniqueID, delta float64)
	// StageSegmentIndexMutation projects mut against the SegmentIndex record
	// for mut.BuildID, and returns the catalog action to compose into the
	// caller's transaction. The caller must hold LockBuildIDs for mut.BuildID
	// from this call until the metric closure has run. The returned install
	// updates memory only and takes no fieldIndexLock, so the caller runs it
	// after the catalog write succeeds, inside its segMu critical section.
	// install returns a metric closure that takes fieldIndexLock; the caller
	// runs it after releasing segMu and before releasing the build-ID lock.
	StageSegmentIndexMutation(mut SegmentIndexMutation) (*stagedSegmentIndexMutation, error)

	// HasIndex reports whether the collection has an index.
	HasIndex(collectionID int64) bool
	// GetIndexesForCollection returns the indexes of the collection, filtered
	// by indexName when it is not empty.
	GetIndexesForCollection(collID UniqueID, indexName string) []*model.Index
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

// DomainMetaLoader builds the per-domain metas. newMeta runs it in parallel
// with segment loading and installs the returned catalog and protection
// before reloading segment indexes from manifests.
type DomainMetaLoader func(ctx context.Context, collectionIDs []int64) (SegmentIndexCatalog, CompactionProtection, error)

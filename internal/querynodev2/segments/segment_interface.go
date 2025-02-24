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

package segments

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// ResourceUsage is used to estimate the resource usage of a sealed segment.
type ResourceUsage struct {
	MemorySize         uint64
	DiskSize           uint64
	MmapFieldCount     int
	FieldGpuMemorySize []uint64
}

// Segment is the interface of a segment implementation.
// Some methods can not apply to all segment types，such as LoadInfo, ResourceUsageEstimate.
// Add more interface to represent different segment types is a better implementation.
type Segment interface {
	// ResourceUsageEstimate() ResourceUsage

	// Properties
	ID() int64
	DatabaseName() string
	ResourceGroup() string
	Collection() int64
	Partition() int64
	Shard() metautil.Channel
	Version() int64
	CASVersion(int64, int64) bool
	StartPosition() *msgpb.MsgPosition
	Type() SegmentType
	Level() datapb.SegmentLevel
	IsSorted() bool
	LoadInfo() *querypb.SegmentLoadInfo
	// PinIfNotReleased the segment to prevent it from being released
	PinIfNotReleased() error
	// Unpin the segment to allow it to be released
	Unpin()

	// Stats related
	// InsertCount returns the number of inserted rows, not effected by deletion
	InsertCount() int64
	// RowNum returns the number of rows, it's slow, so DO NOT call it in a loop
	RowNum() int64
	MemSize() int64
	// ResourceUsageEstimate returns the estimated resource usage of the segment
	ResourceUsageEstimate() ResourceUsage

	// Index related
	GetIndexByID(indexID int64) *IndexedFieldInfo
	GetIndex(fieldID int64) []*IndexedFieldInfo
	ExistIndex(fieldID int64) bool
	Indexes() []*IndexedFieldInfo
	HasRawData(fieldID int64) bool

	// Modification related
	Insert(ctx context.Context, rowIDs []int64, timestamps []typeutil.Timestamp, record *segcorepb.InsertRecord) error
	Delete(ctx context.Context, primaryKeys storage.PrimaryKeys, timestamps []typeutil.Timestamp) error
	LoadDeltaData(ctx context.Context, deltaData *storage.DeltaData) error
	LastDeltaTimestamp() uint64
	Release(ctx context.Context, opts ...releaseOption)

	// Bloom filter related
	UpdateBloomFilter(pks []storage.PrimaryKey)
	MayPkExist(lc *storage.LocationsCache) bool
	BatchPkExist(lc *storage.BatchLocationsCache) []bool

	// BM25 stats
	UpdateBM25Stats(stats map[int64]*storage.BM25Stats)
	GetBM25Stats() map[int64]*storage.BM25Stats

	// Read operations
	Search(ctx context.Context, searchReq *segcore.SearchRequest) (*segcore.SearchResult, error)
	Retrieve(ctx context.Context, plan *segcore.RetrievePlan) (*segcorepb.RetrieveResults, error)
	RetrieveByOffsets(ctx context.Context, plan *segcore.RetrievePlanWithOffsets) (*segcorepb.RetrieveResults, error)
	IsLazyLoad() bool
	ResetIndexesLazyLoad(lazyState bool)

	// lazy load related
	NeedUpdatedVersion() int64
	RemoveUnusedFieldFiles() error
}

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
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// SegmentResourceUsage is used to estimate the resource usage of a sealed segment.
type SegmentResourceUsage struct {
	Predict         ResourceUsage // by predict with segment loader strategy.
	InUsed          ResourceUsage // only returned if segment is loaded otherwise empty returned.
	BinLogSizeAtOSS uint64        // bin log size at oss, it is calculated from load meta of segment.
	// only sealed segment has this value, and it may be not equal to the real disk/mem usage of binary logs.
	// include stat_log, insert_log and delta_log, not include any index files.
}

// GetInuseOrPredictDiskUsage return the inused disk usage.
// if the inused disk usage is 0,return the predicted disk usage.
func (sru SegmentResourceUsage) GetInuseOrPredictDiskUsage() uint64 {
	if sru.InUsed.DiskSize != 0 {
		return sru.InUsed.DiskSize
	}
	return sru.Predict.DiskSize
}

// String returns a string representation of the SegmentResourceUsage.
func (sru SegmentResourceUsage) String() string {
	return fmt.Sprintf("Predict: %s, InUsed: %s, BinLogSizeAtOSS: %d", sru.Predict.String(), sru.InUsed.String(), sru.BinLogSizeAtOSS)
}

// ResourceUsage is used to estimate the resource usage of a sealed segment.
type ResourceUsage struct {
	MemorySize uint64
	DiskSize   uint64
}

// String returns a string representation of the ResourceUsage.
func (ru *ResourceUsage) String() string {
	return fmt.Sprintf("MemorySize: %d, DiskSize: %d", ru.MemorySize, ru.DiskSize)
}

// Segment is the interface of a segment implementation.
// Some methods can not apply to all segment types，such as LoadInfo, ResourceUsageEstimate.
// Add more interface to represent different segment types is a better implementation.
type Segment interface {
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

	// ResourceUsageEstimate returns the estimated resource usage of the segment.
	ResourceUsageEstimate() SegmentResourceUsage

	// Index related
	GetIndex(fieldID int64) *IndexedFieldInfo
	ExistIndex(fieldID int64) bool
	Indexes() []*IndexedFieldInfo
	HasRawData(fieldID int64) bool

	// Modification related
	Insert(ctx context.Context, rowIDs []int64, timestamps []typeutil.Timestamp, record *segcorepb.InsertRecord) error
	Delete(ctx context.Context, primaryKeys []storage.PrimaryKey, timestamps []typeutil.Timestamp) error
	LoadDeltaData(ctx context.Context, deltaData *storage.DeleteData) error
	LastDeltaTimestamp() uint64
	Release(ctx context.Context, opts ...releaseOption)

	// Bloom filter related
	UpdateBloomFilter(pks []storage.PrimaryKey)
	MayPkExist(pk storage.PrimaryKey) bool
	TestLocations(pk storage.PrimaryKey, loc []uint64) bool
	GetHashFuncNum() uint

	// Read operations
	Search(ctx context.Context, searchReq *SearchRequest) (*SearchResult, error)
	Retrieve(ctx context.Context, plan *RetrievePlan) (*segcorepb.RetrieveResults, error)
	RetrieveByOffsets(ctx context.Context, plan *RetrievePlan, offsets []int64) (*segcorepb.RetrieveResults, error)
	IsLazyLoad() bool
	ResetIndexesLazyLoad(lazyState bool)

	// lazy load related
	NeedUpdatedVersion() int64
	RemoveUnusedFieldFiles() error
}

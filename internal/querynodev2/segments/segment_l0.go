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
	"sync"

	"github.com/samber/lo"
	"go.uber.org/zap"

	storage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ Segment = (*L0Segment)(nil)

type L0Segment struct {
	baseSegment

	dataGuard sync.RWMutex
	pks       []storage.PrimaryKey
	tss       []uint64
}

func NewL0Segment(collection *Collection,
	segmentType SegmentType,
	version int64,
	loadInfo *querypb.SegmentLoadInfo,
) (Segment, error) {
	/*
		CSegmentInterface
		NewSegment(CCollection collection, uint64_t segment_id, SegmentType seg_type);
	*/

	log.Info("create L0 segment",
		zap.Int64("collectionID", loadInfo.GetCollectionID()),
		zap.Int64("partitionID", loadInfo.GetPartitionID()),
		zap.Int64("segmentID", loadInfo.GetSegmentID()),
		zap.String("segmentType", segmentType.String()))

	base, err := newBaseSegment(collection, segmentType, version, loadInfo)
	if err != nil {
		return nil, err
	}

	segment := &L0Segment{
		baseSegment: base,
	}

	// level 0 segments are always in memory
	return segment, nil
}

func (s *L0Segment) PinIfNotReleased() error {
	return nil
}

func (s *L0Segment) Unpin() {}

func (s *L0Segment) InsertCount() int64 {
	return 0
}

func (s *L0Segment) RowNum() int64 {
	return 0
}

func (s *L0Segment) MemSize() int64 {
	s.dataGuard.RLock()
	defer s.dataGuard.RUnlock()
	return lo.SumBy(s.pks, func(pk storage.PrimaryKey) int64 {
		return pk.Size() + 8
	})
}

func (s *L0Segment) LastDeltaTimestamp() uint64 {
	s.dataGuard.RLock()
	defer s.dataGuard.RUnlock()

	last, err := lo.Last(s.tss)
	if err != nil {
		return 0
	}
	return last
}

func (s *L0Segment) GetIndex(fieldID int64) *IndexedFieldInfo {
	return nil
}

func (s *L0Segment) GetIndexByID(indexID int64) *IndexedFieldInfo {
	return nil
}

func (s *L0Segment) ExistIndex(fieldID int64) bool {
	return false
}

func (s *L0Segment) HasRawData(fieldID int64) bool {
	return false
}

func (s *L0Segment) Indexes() []*IndexedFieldInfo {
	return nil
}

func (s *L0Segment) ResetIndexesLazyLoad(lazyState bool) {
}

func (s *L0Segment) Type() SegmentType {
	return s.segmentType
}

func (s *L0Segment) Level() datapb.SegmentLevel {
	return datapb.SegmentLevel_L0
}

func (s *L0Segment) Search(ctx context.Context, searchReq *segcore.SearchRequest) (*segcore.SearchResult, error) {
	return nil, nil
}

func (s *L0Segment) Retrieve(ctx context.Context, plan *segcore.RetrievePlan) (*segcorepb.RetrieveResults, error) {
	return nil, nil
}

func (s *L0Segment) RetrieveByOffsets(ctx context.Context, plan *segcore.RetrievePlanWithOffsets) (*segcorepb.RetrieveResults, error) {
	return nil, nil
}

func (s *L0Segment) Insert(ctx context.Context, rowIDs []int64, timestamps []typeutil.Timestamp, record *segcorepb.InsertRecord) error {
	return merr.WrapErrIoFailedReason("insert not supported for L0 segment")
}

func (s *L0Segment) Delete(ctx context.Context, primaryKeys storage.PrimaryKeys, timestamps []typeutil.Timestamp) error {
	return merr.WrapErrIoFailedReason("delete not supported for L0 segment")
}

func (s *L0Segment) LoadDeltaData(ctx context.Context, deltaData *storage.DeltaData) error {
	s.dataGuard.Lock()
	defer s.dataGuard.Unlock()

	for i := 0; i < int(deltaData.DeleteRowCount()); i++ {
		s.pks = append(s.pks, deltaData.DeletePks().Get(i))
	}
	s.tss = append(s.tss, deltaData.DeleteTimestamps()...)
	return nil
}

func (s *L0Segment) DeleteRecords() ([]storage.PrimaryKey, []uint64) {
	s.dataGuard.RLock()
	defer s.dataGuard.RUnlock()

	return s.pks, s.tss
}

func (s *L0Segment) Release(ctx context.Context, opts ...releaseOption) {
	s.dataGuard.Lock()
	defer s.dataGuard.Unlock()

	s.pks = nil
	s.tss = nil

	log.Ctx(ctx).Info("release L0 segment from memory",
		zap.Int64("collectionID", s.Collection()),
		zap.Int64("partitionID", s.Partition()),
		zap.Int64("segmentID", s.ID()),
		zap.String("segmentType", s.segmentType.String()),
	)
}

func (s *L0Segment) RemoveUnusedFieldFiles() error {
	panic("not implemented")
}

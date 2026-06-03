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
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestCatalogCompactionReasonRecordSuite(t *testing.T) {
	suite.Run(t, new(CatalogCompactionReasonRecordSuite))
}

type CatalogCompactionReasonRecordSuite struct {
	suite.Suite

	ctx     context.Context
	metakv  *catalogCompactionReasonRecordMemoryKV
	catalog *Catalog
}

func (s *CatalogCompactionReasonRecordSuite) SetupTest() {
	s.ctx = context.Background()
	s.metakv = newCatalogCompactionReasonRecordMemoryKV()
	s.catalog = NewCatalog(s.metakv, rootPath, "")
}

func (s *CatalogCompactionReasonRecordSuite) TestSaveListUpdateDrop() {
	record := &datapb.CompactionReasonRecord{
		ReasonID: 10,
		Scope: &datapb.CompactionReasonScope{
			CollectionID: 100,
			PartitionID:  200,
			Channel:      "by-dev-rootcoord-dml_0_100v0",
			SegmentIDs:   []int64{1, 2},
		},
		ReasonType:  datapb.CompactionReasonType_REASON_INTENT_REWRITE,
		ExpectedTS:  300,
		TailLimit:   0,
		State:       datapb.CompactionReasonState_REASON_STATE_ACTIVE,
		CreatedAtTS: 301,
	}

	s.Require().NoError(s.catalog.SaveCompactionReasonRecord(s.ctx, record))
	s.True(proto.Equal(record, s.loadRecord(10)))

	unknownReason := proto.Clone(record).(*datapb.CompactionReasonRecord)
	unknownReason.ReasonID = 11
	unknownReason.ReasonType = datapb.CompactionReasonType(999)
	s.Require().NoError(s.catalog.SaveCompactionReasonRecord(s.ctx, unknownReason))

	records, err := s.catalog.ListCompactionReasonRecords(s.ctx)
	s.Require().NoError(err)
	s.Require().Len(records, 2)
	s.Equal(datapb.CompactionReasonType_REASON_INTENT_REWRITE, records[0].GetReasonType())
	s.Equal(datapb.CompactionReasonType(999), records[1].GetReasonType())

	s.Require().NoError(s.catalog.UpdateCompactionReasonRecordState(s.ctx, 10, datapb.CompactionReasonState_REASON_STATE_DROPPED, 500))
	updated := s.loadRecord(10)
	s.Equal(int64(10), updated.GetReasonID())
	s.Equal(datapb.CompactionReasonState_REASON_STATE_DROPPED, updated.GetState())
	s.Equal(uint64(500), updated.GetDroppedAtTS())

	s.Require().NoError(s.catalog.UpdateCompactionReasonRecordState(s.ctx, 10, datapb.CompactionReasonState_REASON_STATE_DONE, 500))
	updated = s.loadRecord(10)
	s.Equal(int64(10), updated.GetReasonID())
	s.Equal(datapb.CompactionReasonState_REASON_STATE_DONE, updated.GetState())
	s.Zero(updated.GetDroppedAtTS())

	s.Require().NoError(s.catalog.UpdateCompactionReasonRecordState(s.ctx, 10, datapb.CompactionReasonState_REASON_STATE_INACTIVE, 500))
	updated = s.loadRecord(10)
	s.Equal(int64(10), updated.GetReasonID())
	s.Equal(datapb.CompactionReasonState_REASON_STATE_INACTIVE, updated.GetState())
	s.Zero(updated.GetDroppedAtTS())

	s.Require().NoError(s.catalog.DropCompactionReasonRecord(s.ctx, record))
	_, err = s.metakv.Load(s.ctx, CompactionReasonRecordPrefix+"/10")
	s.Error(err)
}

func (s *CatalogCompactionReasonRecordSuite) TestListReturnsUnmarshalError() {
	s.Require().NoError(s.metakv.Save(s.ctx, CompactionReasonRecordPrefix+"/10", "not-protobuf"))

	records, err := s.catalog.ListCompactionReasonRecords(s.ctx)
	s.Error(err)
	s.Nil(records)
}

func (s *CatalogCompactionReasonRecordSuite) TestUpdateStateReturnsLoadError() {
	err := s.catalog.UpdateCompactionReasonRecordState(s.ctx, 10, datapb.CompactionReasonState_REASON_STATE_INACTIVE, 0)
	s.Error(err)
}

func (s *CatalogCompactionReasonRecordSuite) loadRecord(reasonID int64) *datapb.CompactionReasonRecord {
	value, err := s.metakv.Load(s.ctx, buildCompactionReasonRecordPath(reasonID))
	s.Require().NoError(err)
	record := &datapb.CompactionReasonRecord{}
	s.Require().NoError(proto.Unmarshal([]byte(value), record))
	return record
}

type catalogCompactionReasonRecordMemoryKV struct {
	memkv.MemoryKV
}

var _ kv.MetaKv = (*catalogCompactionReasonRecordMemoryKV)(nil)

func newCatalogCompactionReasonRecordMemoryKV() *catalogCompactionReasonRecordMemoryKV {
	return &catalogCompactionReasonRecordMemoryKV{MemoryKV: *memkv.NewMemoryKV()}
}

func (m *catalogCompactionReasonRecordMemoryKV) WalkWithPrefix(ctx context.Context, prefix string, paginationSize int, fn func([]byte, []byte) error) error {
	keys, values, err := m.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return err
	}
	for i, key := range keys {
		if err := fn([]byte(key), []byte(values[i])); err != nil {
			return err
		}
	}
	return nil
}

func (m *catalogCompactionReasonRecordMemoryKV) GetPath(key string) string {
	return key
}

func (m *catalogCompactionReasonRecordMemoryKV) CompareVersionAndSwap(context.Context, string, int64, string) (bool, error) {
	panic("unexpected CompareVersionAndSwap")
}

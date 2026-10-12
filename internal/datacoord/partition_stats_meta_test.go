// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

type PartitionStatsMetaSuite struct {
	suite.Suite

	catalog *mocks.DataCoordCatalog
	meta    *partitionStatsMeta
}

func TestPartitionStatsMetaSuite(t *testing.T) {
	suite.Run(t, new(PartitionStatsMetaSuite))
}

// seedPartitionStatsInfo inserts info into psm's in-memory state (lazily
// creating the channel/partition buckets), mirroring what a persisted load
// leaves behind - a memory-only test fixture with no catalog write.
func seedPartitionStatsInfo(psm *partitionStatsMeta, info *datapb.PartitionStatsInfo) {
	vch, part := info.GetVChannel(), info.GetPartitionID()
	if _, ok := psm.partitionStatsInfos[vch]; !ok {
		psm.partitionStatsInfos[vch] = make(map[int64]*partitionStatsInfo)
	}
	if _, ok := psm.partitionStatsInfos[vch][part]; !ok {
		psm.partitionStatsInfos[vch][part] = &partitionStatsInfo{
			infos: make(map[int64]*datapb.PartitionStatsInfo),
		}
	}
	psm.partitionStatsInfos[vch][part].infos[info.GetVersion()] = info
}

func (s *PartitionStatsMetaSuite) SetupTest() {
	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil).Maybe()
	s.catalog = catalog
}

func (s *PartitionStatsMetaSuite) TestGetPartitionStats() {
	ctx := context.Background()
	psm, err := newPartitionStatsMeta(ctx, s.catalog)
	s.NoError(err)

	info := &datapb.PartitionStatsInfo{
		CollectionID: 1,
		PartitionID:  2,
		VChannel:     "ch-1",
		SegmentIDs:   []int64{100000},
		Version:      100,
	}
	seedPartitionStatsInfo(psm, info)

	s.Nil(psm.GetPartitionStats(1, 2, "ch-2", 100))
	s.Nil(psm.GetPartitionStats(1, 3, "ch-1", 100))
	s.Nil(psm.GetPartitionStats(1, 2, "ch-1", 101))
	s.NotNil(psm.GetPartitionStats(1, 2, "ch-1", 100))

	s.Equal(emptyPartitionStatsVersion, psm.GetCurrentPartitionStatsVersion(1, 2, "ch-1"))
	s.Equal(emptyPartitionStatsVersion, psm.GetCurrentPartitionStatsVersion(1, 2, "ch-2"))
	s.Equal(emptyPartitionStatsVersion, psm.GetCurrentPartitionStatsVersion(1, 3, "ch-1"))

	psm.partitionStatsInfos["ch-1"][2].currentVersion = 100
	s.Equal(int64(100), psm.GetCurrentPartitionStatsVersion(1, 2, "ch-1"))
}

// TestDropPartitionStats exercises the live drop bookkeeping - the rollback
// target computed by getRollbackVersionLocked and applied by applyDropLocked -
// which the meta-level composite drop path uses (see meta.go).
func (s *PartitionStatsMetaSuite) TestDropPartitionStats() {
	ctx := context.Background()
	psm, err := newPartitionStatsMeta(ctx, s.catalog)
	s.NoError(err)

	collectionID := int64(1)
	partitionID := int64(2)
	channel := "ch-1"
	mk := func(v int64) *datapb.PartitionStatsInfo {
		return &datapb.PartitionStatsInfo{
			CollectionID: collectionID,
			PartitionID:  partitionID,
			VChannel:     channel,
			SegmentIDs:   []int64{100000},
			Version:      v,
		}
	}
	seedPartitionStatsInfo(psm, mk(100))
	seedPartitionStatsInfo(psm, mk(101))
	seedPartitionStatsInfo(psm, mk(102))
	psm.partitionStatsInfos[channel][partitionID].currentVersion = 102
	s.Equal(int64(102), psm.GetCurrentPartitionStatsVersion(collectionID, partitionID, channel))

	// Drop the current version (102): rollback to the max remaining below it (101).
	rb := psm.getRollbackVersionLocked(mk(102))
	s.NotNil(rb)
	s.Equal(int64(101), *rb)
	psm.applyDropLocked(mk(102), rb)
	s.Equal(2, len(psm.partitionStatsInfos[channel][partitionID].infos))
	s.Equal(int64(101), psm.GetCurrentPartitionStatsVersion(collectionID, partitionID, channel))

	// Drop the new current version (101): rollback to 100.
	rb = psm.getRollbackVersionLocked(mk(101))
	s.NotNil(rb)
	s.Equal(int64(100), *rb)
	psm.applyDropLocked(mk(101), rb)
	s.Equal(1, len(psm.partitionStatsInfos[channel][partitionID].infos))
	s.Equal(int64(100), psm.GetCurrentPartitionStatsVersion(collectionID, partitionID, channel))

	// Drop the last version (100): no smaller version remains, so the rollback
	// target is the empty version and the whole entry is removed.
	rb = psm.getRollbackVersionLocked(mk(100))
	s.NotNil(rb)
	s.Equal(emptyPartitionStatsVersion, *rb)
	psm.applyDropLocked(mk(100), rb)
	s.Nil(psm.partitionStatsInfos[channel][partitionID])
	s.Equal(emptyPartitionStatsVersion, psm.GetCurrentPartitionStatsVersion(collectionID, partitionID, channel))
}

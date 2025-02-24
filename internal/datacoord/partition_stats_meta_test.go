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
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type PartitionStatsMetaSuite struct {
	suite.Suite

	catalog *mocks.DataCoordCatalog
	meta    *partitionStatsMeta
}

func TestPartitionStatsMetaSuite(t *testing.T) {
	suite.Run(t, new(PartitionStatsMetaSuite))
}

func (s *PartitionStatsMetaSuite) SetupTest() {
	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().SavePartitionStatsInfo(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().SaveCurrentPartitionStatsVersion(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	s.catalog = catalog
}

func (s *PartitionStatsMetaSuite) TestGetPartitionStats() {
	ctx := context.Background()
	partitionStatsMeta, err := newPartitionStatsMeta(ctx, s.catalog)
	s.NoError(err)
	partitionStats := []*datapb.PartitionStatsInfo{
		{
			CollectionID: 1,
			PartitionID:  2,
			VChannel:     "ch-1",
			SegmentIDs:   []int64{100000},
			Version:      100,
		},
	}
	for _, partitionStats := range partitionStats {
		partitionStatsMeta.SavePartitionStatsInfo(partitionStats)
	}

	ps1 := partitionStatsMeta.GetPartitionStats(1, 2, "ch-2", 100)
	s.Nil(ps1)

	ps2 := partitionStatsMeta.GetPartitionStats(1, 3, "ch-1", 100)
	s.Nil(ps2)

	ps3 := partitionStatsMeta.GetPartitionStats(1, 2, "ch-1", 101)
	s.Nil(ps3)

	ps := partitionStatsMeta.GetPartitionStats(1, 2, "ch-1", 100)
	s.NotNil(ps)

	err = partitionStatsMeta.SaveCurrentPartitionStatsVersion(1, 2, "ch-1", 100)
	s.NoError(err)

	currentVersion := partitionStatsMeta.GetCurrentPartitionStatsVersion(1, 2, "ch-1")
	s.Equal(int64(100), currentVersion)

	currentVersion2 := partitionStatsMeta.GetCurrentPartitionStatsVersion(1, 2, "ch-2")
	s.Equal(emptyPartitionStatsVersion, currentVersion2)

	currentVersion3 := partitionStatsMeta.GetCurrentPartitionStatsVersion(1, 3, "ch-1")
	s.Equal(emptyPartitionStatsVersion, currentVersion3)

	partitionStatsMeta.partitionStatsInfos["ch-1"][2].currentVersion = 100
	currentVersion4 := partitionStatsMeta.GetCurrentPartitionStatsVersion(1, 2, "ch-1")
	s.Equal(int64(100), currentVersion4)
}

func (s *PartitionStatsMetaSuite) TestDropPartitionStats() {
	ctx := context.Background()
	partitionStatsMeta, err := newPartitionStatsMeta(ctx, s.catalog)
	s.NoError(err)
	collectionID := int64(1)
	partitionID := int64(2)
	channel := "ch-1"
	s.catalog.EXPECT().DropPartitionStatsInfo(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCurrentPartitionStatsVersion(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	partitionStats := []*datapb.PartitionStatsInfo{
		{
			CollectionID: collectionID,
			PartitionID:  partitionID,
			VChannel:     channel,
			SegmentIDs:   []int64{100000},
			Version:      100,
		},
		{
			CollectionID: collectionID,
			PartitionID:  partitionID,
			VChannel:     channel,
			SegmentIDs:   []int64{100000},
			Version:      101,
		},
		{
			CollectionID: collectionID,
			PartitionID:  partitionID,
			VChannel:     channel,
			SegmentIDs:   []int64{100000},
			Version:      102,
		},
	}
	for _, partitionStats := range partitionStats {
		partitionStatsMeta.SavePartitionStatsInfo(partitionStats)
	}
	partitionStatsMeta.SaveCurrentPartitionStatsVersion(collectionID, partitionID, channel, 102)
	version := partitionStatsMeta.GetCurrentPartitionStatsVersion(collectionID, partitionID, channel)
	s.Equal(int64(102), version)

	err = partitionStatsMeta.DropPartitionStatsInfo(context.Background(), partitionStats[2])
	s.NoError(err)
	s.Equal(2, len(partitionStatsMeta.partitionStatsInfos[channel][partitionID].infos))
	version2 := partitionStatsMeta.GetCurrentPartitionStatsVersion(collectionID, partitionID, channel)
	s.Equal(int64(101), version2)

	err = partitionStatsMeta.DropPartitionStatsInfo(context.Background(), partitionStats[1])
	s.Equal(1, len(partitionStatsMeta.partitionStatsInfos[channel][partitionID].infos))
	version3 := partitionStatsMeta.GetCurrentPartitionStatsVersion(collectionID, partitionID, channel)
	s.Equal(int64(100), version3)

	err = partitionStatsMeta.DropPartitionStatsInfo(context.Background(), partitionStats[0])
	s.NoError(err)
	s.Nil(partitionStatsMeta.partitionStatsInfos[channel][partitionID])
	version4 := partitionStatsMeta.GetCurrentPartitionStatsVersion(collectionID, partitionID, channel)
	s.Equal(emptyPartitionStatsVersion, version4)
}

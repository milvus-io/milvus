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

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type VshardManagerSuite struct {
	suite.Suite

	catalog *mocks.DataCoordCatalog
	meta    VshardMeta

	mockMeta   *MockCompactionMeta
	mockAlloc  allocator.Allocator
	vshardMeta VshardMeta
}

func TestVshardManagerSuite(t *testing.T) {
	suite.Run(t, new(VshardManagerSuite))
}

func (s *VshardManagerSuite) SetupTest() {
	paramtable.Init()
	s.mockMeta = NewMockCompactionMeta(s.T())
	s.mockAlloc = allocator.NewMockAllocator(s.T())
	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().ListVShardInfos(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListVShardTasks(mock.Anything).Return(nil, nil).Maybe()
	s.catalog = catalog

	ctx := context.Background()
	vshardMeta, err := newVshardMetaImpl(ctx, s.catalog)
	s.NoError(err)
	s.vshardMeta = vshardMeta
}

func (s *VshardManagerSuite) TestVshardManagerStart() {
	manager := NewVshardManagerImpl(s.mockMeta, s.mockAlloc)
	s.NotNil(manager)
}

func (s *VshardManagerSuite) TestVshardManagerEnable() {
	manager := NewVshardManagerImpl(s.mockMeta, s.mockAlloc)
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.VShardEnable.Key, "false")
	err := manager.check()
	s.NoError(err)
}

func (s *VshardManagerSuite) TestGetActiveVShardInfos() {
	s.catalog.EXPECT().SaveVShardInfos(mock.Anything, mock.Anything).Return(nil).Maybe()
	manager := NewVshardManagerImpl(s.mockMeta, s.mockAlloc)
	s.mockMeta.EXPECT().GetVshardMeta().Return(s.vshardMeta)
	vshardInfos := manager.GetNormalVShardInfos(100, "ch-1")
	s.Equal(0, len(vshardInfos))

	vshard1 := &datapb.VShardInfo{
		CollectionId: 10,
		PartitionId:  100,
		Vchannel:     "ch-1",
		VshardDesc: &datapb.VShardDesc{
			VshardModulus: 2,
			VshardResidue: 0,
		},
		State: datapb.VShardInfoState_VShard_dropped,
	}
	vshard2 := &datapb.VShardInfo{
		CollectionId: 10,
		PartitionId:  100,
		Vchannel:     "ch-1",
		VshardDesc: &datapb.VShardDesc{
			VshardModulus: 4,
			VshardResidue: 0,
		},
		State: datapb.VShardInfoState_VShard_resharding,
	}
	vshard3 := &datapb.VShardInfo{
		CollectionId: 10,
		PartitionId:  100,
		Vchannel:     "ch-1",
		VshardDesc: &datapb.VShardDesc{
			VshardModulus: 8,
			VshardResidue: 0,
		},
		State: datapb.VShardInfoState_VShard_normal,
	}
	s.vshardMeta.SaveVShardInfos([]*datapb.VShardInfo{vshard1, vshard2, vshard3})

	vshardInfos2 := manager.GetNormalVShardInfos(100, "ch-1")
	s.Equal(1, len(vshardInfos2))
}

func (s *VshardManagerSuite) TestGetReVShardTasks() {
	s.catalog.EXPECT().SaveVShardInfos(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.catalog.EXPECT().SaveVShardTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	manager := NewVshardManagerImpl(s.mockMeta, s.mockAlloc)
	s.mockMeta.EXPECT().GetVshardMeta().Return(s.vshardMeta)
	tasks := manager.GetVShardTasks(100)
	s.Equal(0, len(tasks))

	vshardTask := &datapb.VShardTask{
		Id:           1,
		CollectionId: 10,
		PartitionId:  100,
		Vchannel:     "ch-1",
		To: []*datapb.VShardDesc{
			{
				VshardModulus: 2,
				VshardResidue: 0,
			},
			{
				VshardModulus: 2,
				VshardResidue: 1,
			},
		},
		State: datapb.VShardTaskState_VShardTask_created,
	}
	err := s.vshardMeta.SaveVShardTask(vshardTask)
	s.NoError(err)

	tasks2 := manager.GetVShardTasks(100)
	s.Equal(1, len(tasks2))
}

func (s *VshardManagerSuite) TestClean() {
	s.catalog.EXPECT().SaveVShardInfos(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.catalog.EXPECT().SaveVShardTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.catalog.EXPECT().DropVShardInfo(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.catalog.EXPECT().DropVShardTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	manager := NewVshardManagerImpl(s.mockMeta, s.mockAlloc)
	s.mockMeta.EXPECT().GetVshardMeta().Return(s.vshardMeta)

	vshard1 := &datapb.VShardInfo{
		CollectionId: 10,
		PartitionId:  100,
		Vchannel:     "ch-1",
		VshardDesc: &datapb.VShardDesc{
			VshardModulus: 2,
			VshardResidue: 0,
		},
		State: datapb.VShardInfoState_VShard_dropped,
	}
	vshard2 := &datapb.VShardInfo{
		CollectionId: 10,
		PartitionId:  100,
		Vchannel:     "ch-1",
		VshardDesc: &datapb.VShardDesc{
			VshardModulus: 4,
			VshardResidue: 0,
		},
		State: datapb.VShardInfoState_VShard_resharding,
	}
	vshard3 := &datapb.VShardInfo{
		CollectionId: 10,
		PartitionId:  100,
		Vchannel:     "ch-1",
		VshardDesc: &datapb.VShardDesc{
			VshardModulus: 8,
			VshardResidue: 0,
		},
		State: datapb.VShardInfoState_VShard_normal,
	}
	s.vshardMeta.SaveVShardInfos([]*datapb.VShardInfo{vshard1, vshard2, vshard3})

	vshardTask1 := &datapb.VShardTask{
		Id:           1,
		CollectionId: 10,
		PartitionId:  100,
		Vchannel:     "ch-1",
		To: []*datapb.VShardDesc{
			{
				VshardModulus: 2,
				VshardResidue: 0,
			},
			{
				VshardModulus: 2,
				VshardResidue: 1,
			},
		},
		State: datapb.VShardTaskState_VShardTask_created,
	}

	vshardTask2 := &datapb.VShardTask{
		Id:           2,
		CollectionId: 10,
		PartitionId:  100,
		Vchannel:     "ch-1",
		To: []*datapb.VShardDesc{
			{
				VshardModulus: 2,
				VshardResidue: 0,
			},
			{
				VshardModulus: 2,
				VshardResidue: 1,
			},
		},
		State: datapb.VShardTaskState_VShardTask_failed,
	}

	vshardTask3 := &datapb.VShardTask{
		Id:           3,
		CollectionId: 10,
		PartitionId:  100,
		Vchannel:     "ch-1",
		To: []*datapb.VShardDesc{
			{
				VshardModulus: 2,
				VshardResidue: 0,
			},
			{
				VshardModulus: 2,
				VshardResidue: 1,
			},
		},
		State: datapb.VShardTaskState_VShardTask_finished,
	}
	err := s.vshardMeta.SaveVShardTask(vshardTask1)
	s.NoError(err)
	err = s.vshardMeta.SaveVShardTask(vshardTask2)
	s.NoError(err)
	err = s.vshardMeta.SaveVShardTask(vshardTask3)
	s.NoError(err)

	vshards := manager.GetNormalVShardInfos(100, "ch-1")
	s.Equal(1, len(vshards))
	tasks := manager.GetVShardTasks(100)
	s.Equal(3, len(tasks))

	manager.clean()
	tasks2 := manager.GetVShardTasks(100)
	s.Equal(1, len(tasks2))
}

func (s *VshardManagerSuite) TestNextPowerOfTwo() {
	s.Equal(int32(1), nextPowerOfTwo(1))
	s.Equal(int32(2), nextPowerOfTwo(2))
	s.Equal(int32(4), nextPowerOfTwo(3))
	s.Equal(int32(4), nextPowerOfTwo(4))
	s.Equal(int32(8), nextPowerOfTwo(5))
	s.Equal(int32(8), nextPowerOfTwo(6))
	s.Equal(int32(8), nextPowerOfTwo(7))
	s.Equal(int32(8), nextPowerOfTwo(8))
	s.Equal(int32(256), nextPowerOfTwo(255))
	s.Equal(int32(256), nextPowerOfTwo(256))
	s.Equal(int32(512), nextPowerOfTwo(257))
}

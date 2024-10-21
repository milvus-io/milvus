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

	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
)

type VshardMetaSuite struct {
	suite.Suite

	catalog *mocks.DataCoordCatalog
	meta    VshardMeta
}

func TestVshardMetaSuite(t *testing.T) {
	suite.Run(t, new(VshardMetaSuite))
}

func (s *VshardMetaSuite) SetupTest() {
	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().ListVShardTasks(mock.Anything).Return(nil, nil).Maybe()
	s.catalog = catalog
}

func (s *VshardMetaSuite) TestGetVShardInfo_empty() {
	s.catalog.EXPECT().ListVShardInfos(mock.Anything).Return(nil, nil).Maybe()
	ctx := context.Background()
	meta, err := newVshardMetaImpl(ctx, s.catalog)
	s.NoError(err)
	s.meta = meta
	// null
	vshardInfos := s.meta.GetVShardInfo(100, "ch-1")
	s.Equal(0, len(vshardInfos))
}

func (s *VshardMetaSuite) TestGetVShardInfo_exist() {
	s.catalog.EXPECT().ListVShardInfos(mock.Anything).Return([]*datapb.VShardInfo{
		{
			CollectionId: 10,
			PartitionId:  100,
			Vchannel:     "ch-1",
			VshardDesc: &datapb.VShardDesc{
				VshardModulus: 1,
				VshardResidue: 0,
			},
			State: datapb.VShardInfoState_VShard_normal,
		},
	}, nil)

	ctx := context.Background()
	meta, err := newVshardMetaImpl(ctx, s.catalog)
	s.NoError(err)
	s.meta = meta
	vshardInfos := s.meta.GetVShardInfo(100, "ch-1")
	s.Equal(1, len(vshardInfos))

	listVshardInfos := s.meta.ListVShardInfos()
	s.Equal(1, len(listVshardInfos))
}

func (s *VshardMetaSuite) TestSaveVShardInfos() {
	s.catalog.EXPECT().ListVShardInfos(mock.Anything).Return(nil, nil).Maybe()
	s.catalog.EXPECT().SaveVShardInfos(mock.Anything, mock.Anything).Return(nil).Maybe()
	ctx := context.Background()
	meta, err := newVshardMetaImpl(ctx, s.catalog)
	s.NoError(err)
	s.meta = meta
	err = s.meta.SaveVShardInfos([]*datapb.VShardInfo{
		{
			CollectionId: 10,
			PartitionId:  100,
			Vchannel:     "ch-1",
			VshardDesc: &datapb.VShardDesc{
				VshardModulus: 1,
				VshardResidue: 0,
			},
			State: datapb.VShardInfoState_VShard_normal,
		},
	})
	s.NoError(err)
	vshardInfos := s.meta.GetVShardInfo(100, "ch-1")
	s.Equal(1, len(vshardInfos))
}

func (s *VshardMetaSuite) TestDropVShardInfos() {
	s.catalog.EXPECT().ListVShardInfos(mock.Anything).Return(nil, nil).Maybe()
	s.catalog.EXPECT().SaveVShardInfos(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.catalog.EXPECT().DropVShardInfo(mock.Anything, mock.Anything).Return(nil).Maybe()
	ctx := context.Background()
	meta, err := newVshardMetaImpl(ctx, s.catalog)
	s.NoError(err)
	s.meta = meta
	vshard := &datapb.VShardInfo{
		CollectionId: 10,
		PartitionId:  100,
		Vchannel:     "ch-1",
		VshardDesc: &datapb.VShardDesc{
			VshardModulus: 1,
			VshardResidue: 0,
		},
		State: datapb.VShardInfoState_VShard_normal,
	}
	err = s.meta.SaveVShardInfos([]*datapb.VShardInfo{
		vshard,
	})
	s.NoError(err)
	vshardInfos := s.meta.GetVShardInfo(100, "ch-1")
	s.Equal(1, len(vshardInfos))
	err = s.meta.DropVShardInfo(vshard)
	s.NoError(err)
}

func (s *VshardMetaSuite) TestReVShardTaskOperations() {
	s.catalog.EXPECT().ListVShardInfos(mock.Anything).Return(nil, nil).Maybe()
	s.catalog.EXPECT().SaveVShardInfos(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.catalog.EXPECT().DropVShardInfo(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.catalog.EXPECT().SaveVShardTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.catalog.EXPECT().DropVShardTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.catalog.EXPECT().ListVShardTasks(mock.Anything).Return(nil, nil).Maybe()
	ctx := context.Background()
	meta, err := newVshardMetaImpl(ctx, s.catalog)
	s.NoError(err)
	s.meta = meta
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
	err = s.meta.SaveVShardTask(vshardTask)
	s.NoError(err)
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
		State: datapb.VShardTaskState_VShardTask_created,
	}
	err = s.meta.SaveVShardTask(vshardTask2)
	s.NoError(err)

	getVshardTask := s.meta.GetVShardTaskByID(100, 1)
	s.Equal(getVshardTask, vshardTask)

	listVshardTasks := s.meta.ListVShardTasks()
	s.Equal(2, len(listVshardTasks))

	vshardTasks := s.meta.GetVShardTasksByPartition(100)
	s.Equal(2, len(vshardTasks))

	err = s.meta.DropVShardTask(vshardTask2)
	s.NoError(err)

	vshardTasks2 := s.meta.GetVShardTasksByPartition(100)
	s.Equal(1, len(vshardTasks2))
}

func (s *VshardMetaSuite) TestSaveVShardInfosAndReVshardTask() {
	s.catalog.EXPECT().ListVShardInfos(mock.Anything).Return(nil, nil).Maybe()
	s.catalog.EXPECT().SaveVShardInfos(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.catalog.EXPECT().DropVShardInfo(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.catalog.EXPECT().SaveVShardTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.catalog.EXPECT().DropVShardTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.catalog.EXPECT().ListVShardTasks(mock.Anything).Return(nil, nil).Maybe()
	s.catalog.EXPECT().SaveVShardInfosAndVShardTasks(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	ctx := context.Background()
	meta, err := newVshardMetaImpl(ctx, s.catalog)
	s.NoError(err)
	s.meta = meta

	vshard1 := &datapb.VShardInfo{
		CollectionId: 10,
		PartitionId:  100,
		Vchannel:     "ch-1",
		VshardDesc: &datapb.VShardDesc{
			VshardModulus: 2,
			VshardResidue: 0,
		},
		State: datapb.VShardInfoState_VShard_normal,
	}
	vshard2 := &datapb.VShardInfo{
		CollectionId: 10,
		PartitionId:  100,
		Vchannel:     "ch-1",
		VshardDesc: &datapb.VShardDesc{
			VshardModulus: 2,
			VshardResidue: 1,
		},
		State: datapb.VShardInfoState_VShard_normal,
	}

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
	err = s.meta.SaveVShardTask(vshardTask)
	s.NoError(err)

	err = s.meta.SaveVShardInfosAndVshardTask([]*datapb.VShardInfo{vshard1, vshard2}, vshardTask)
	s.NoError(err)
}

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

package session

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/testutils"
)

func TestDataNodeManagerSuite(t *testing.T) {
	suite.Run(t, new(DataNodeManagerSuite))
}

type DataNodeManagerSuite struct {
	testutils.PromMetricsSuite

	dn *mocks.MockDataNodeClient

	m *DataNodeManagerImpl
}

func (s *DataNodeManagerSuite) SetupSuite() {
	paramtable.Init()
}

func (s *DataNodeManagerSuite) SetupTest() {
	s.dn = mocks.NewMockDataNodeClient(s.T())

	s.m = NewDataNodeManagerImpl(WithDataNodeCreator(func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
		return s.dn, nil
	}))

	s.m.AddSession(&NodeInfo{1000, "addr-1", true})
	s.MetricsEqual(metrics.DataCoordNumDataNodes, 1)
}

func (s *DataNodeManagerSuite) SetupSubTest() {
	s.SetupTest()
}

func (s *DataNodeManagerSuite) TestExecFlush() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := &datapb.FlushSegmentsRequest{
		CollectionID: 1,
		SegmentIDs:   []int64{100, 200},
		ChannelName:  "ch-1",
	}

	s.Run("no node", func() {
		s.m.execFlush(ctx, 100, req)
	})

	s.Run("fail", func() {
		s.dn.EXPECT().FlushSegments(mock.Anything, mock.Anything).Return(nil, errors.New("mock")).Once()
		s.m.execFlush(ctx, 1000, req)
	})

	s.Run("normal", func() {
		s.dn.EXPECT().FlushSegments(mock.Anything, mock.Anything).Return(merr.Status(nil), nil).Once()
		s.m.execFlush(ctx, 1000, req)
	})
}

func (s *DataNodeManagerSuite) TestNotifyChannelOperation() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	info := &datapb.ChannelWatchInfo{
		Vchan: &datapb.VchannelInfo{},
		State: datapb.ChannelWatchState_ToWatch,
		OpID:  1,
	}

	req := &datapb.ChannelOperationsRequest{
		Infos: []*datapb.ChannelWatchInfo{info},
	}
	s.Run("no node", func() {
		err := s.m.NotifyChannelOperation(ctx, 100, req)
		s.Error(err)
	})

	s.Run("fail", func() {
		s.dn.EXPECT().NotifyChannelOperation(mock.Anything, mock.Anything).Return(nil, errors.New("mock")).Once()

		err := s.m.NotifyChannelOperation(ctx, 1000, req)
		s.Error(err)
	})

	s.Run("normal", func() {
		s.dn.EXPECT().NotifyChannelOperation(mock.Anything, mock.Anything).Return(merr.Status(nil), nil).Once()

		err := s.m.NotifyChannelOperation(ctx, 1000, req)
		s.NoError(err)
	})
}

func (s *DataNodeManagerSuite) TestCheckCHannelOperationProgress() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	info := &datapb.ChannelWatchInfo{
		Vchan: &datapb.VchannelInfo{},
		State: datapb.ChannelWatchState_ToWatch,
		OpID:  1,
	}

	s.Run("no node", func() {
		resp, err := s.m.CheckChannelOperationProgress(ctx, 100, info)
		s.Error(err)
		s.Nil(resp)
	})

	s.Run("fail", func() {
		s.dn.EXPECT().CheckChannelOperationProgress(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("mock")).Once()

		resp, err := s.m.CheckChannelOperationProgress(ctx, 1000, info)
		s.Error(err)
		s.Nil(resp)
	})

	s.Run("normal", func() {
		s.dn.EXPECT().CheckChannelOperationProgress(mock.Anything, mock.Anything, mock.Anything).
			Return(&datapb.ChannelOperationProgressResponse{
				Status:   merr.Status(nil),
				OpID:     info.OpID,
				State:    info.State,
				Progress: 100,
			}, nil).Once()

		resp, err := s.m.CheckChannelOperationProgress(ctx, 1000, info)
		s.NoError(err)
		s.Equal(resp.GetState(), info.State)
		s.Equal(resp.OpID, info.OpID)
		s.EqualValues(100, resp.Progress)
	})
}

func (s *DataNodeManagerSuite) TestImportV2() {
	mockErr := errors.New("mock error")

	s.Run("PreImport", func() {
		err := s.m.PreImport(0, &datapb.PreImportRequest{})
		s.Error(err)

		s.SetupTest()
		s.dn.EXPECT().PreImport(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		err = s.m.PreImport(1000, &datapb.PreImportRequest{})
		s.NoError(err)
	})

	s.Run("ImportV2", func() {
		err := s.m.ImportV2(0, &datapb.ImportRequest{})
		s.Error(err)

		s.SetupTest()
		s.dn.EXPECT().ImportV2(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		err = s.m.ImportV2(1000, &datapb.ImportRequest{})
		s.NoError(err)
	})

	s.Run("QueryPreImport", func() {
		_, err := s.m.QueryPreImport(0, &datapb.QueryPreImportRequest{})
		s.Error(err)

		s.SetupTest()
		s.dn.EXPECT().QueryPreImport(mock.Anything, mock.Anything).Return(&datapb.QueryPreImportResponse{
			Status: merr.Status(mockErr),
		}, nil)
		_, err = s.m.QueryPreImport(1000, &datapb.QueryPreImportRequest{})
		s.Error(err)
	})

	s.Run("QueryImport", func() {
		_, err := s.m.QueryImport(0, &datapb.QueryImportRequest{})
		s.Error(err)

		s.SetupTest()
		s.dn.EXPECT().QueryImport(mock.Anything, mock.Anything).Return(&datapb.QueryImportResponse{
			Status: merr.Status(mockErr),
		}, nil)
		_, err = s.m.QueryImport(1000, &datapb.QueryImportRequest{})
		s.Error(err)
	})

	s.Run("DropImport", func() {
		err := s.m.DropImport(0, &datapb.DropImportRequest{})
		s.Error(err)

		s.SetupTest()
		s.dn.EXPECT().DropImport(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		err = s.m.DropImport(1000, &datapb.DropImportRequest{})
		s.NoError(err)
	})
}

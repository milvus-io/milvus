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

package flusherimpl

import (
	"context"
	broker2 "github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/flushcommon/util"
	"github.com/milvus-io/milvus/internal/streamingnode/server/flusher"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

type FlusherSuite struct {
	integration.MiniClusterSuite

	pchannel  string
	vchannels []string
	flusher   flusher.Flusher
}

func (s *FlusherSuite) SetupSuite() {
	paramtable.Init()
}

func (s *FlusherSuite) TearDownSuite() {}

func (s *FlusherSuite) SetupTest() {
	s.pchannel = "by-dev-rootcoord-dml_0"
	s.vchannels = []string{
		"by-dev-rootcoord-dml_0_123456v0",
		"by-dev-rootcoord-dml_0_123456v1",
		"by-dev-rootcoord-dml_0_123456v2",
	}

	chunkManager := mocks.NewChunkManager(s.T())

	rootcoord := mocks.NewMockRootCoordClient(s.T())
	rootcoord.EXPECT().GetVChannels(mock.Anything, mock.Anything).
		Return(&rootcoordpb.GetVChannelsResponse{Vchannels: s.vchannels}, nil)

	datacoord := mocks.NewMockDataCoordClient(s.T())
	datacoord.EXPECT().GetChannelRecoveryInfo(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, request *datapb.GetChannelRecoveryInfoRequest, option ...grpc.CallOption,
		) (*datapb.GetChannelRecoveryInfoResponse, error) {
			return &datapb.GetChannelRecoveryInfoResponse{
				Info: &datapb.VchannelInfo{
					ChannelName: request.GetVchannel(),
				},
				Schema: nil,
			}, nil
		})

	resource.Init(
		resource.OptRootCoordClient(rootcoord),
		resource.OptDataCoordClient(datacoord),
	)

	broker := broker2.NewMockBroker(s.T())

	syncMgr := syncmgr.NewMockSyncManager(s.T())

	wbMgr := writebuffer.NewMockBufferManager(s.T())
	wbMgr.EXPECT().Start().Return()
	wbMgr.EXPECT().Stop().Return()

	pp := &util.PipelineParams{
		Broker:             broker,
		SyncMgr:            syncMgr,
		ChunkManager:       chunkManager,
		WriteBufferManager: wbMgr,
		CheckpointUpdater:  util.NewChannelCheckpointUpdater(broker),
		Allocator:          resource.Resource().IDAllocator(),
	}
	s.flusher = NewFlusher(pp)
}

func (s *FlusherSuite) TestFlusher() {
	s.flusher.Start()
	defer s.flusher.Stop()

	scanner := mock_wal.NewMockScanner(s.T())
	scanner.EXPECT().Chan().Return(make(chan message.ImmutableMessage, 1024))

	wal := mock_wal.NewMockWAL(s.T())
	wal.EXPECT().WALName().Return(s.pchannel)
	wal.EXPECT().Read(mock.Anything, mock.Anything).Return(scanner, nil)

	err := s.flusher.RegisterPChannel(wal)
	s.NoError(err)

	s.Eventually(func() bool {
		return s.flusher.(*flusherImpl).fgMgr.GetFlowgraphCount() == len(s.vchannels)
	}, 10*time.Second, 100*time.Millisecond)
}

func TestFlusherSuite(t *testing.T) {
	suite.Run(t, new(FlusherSuite))
}

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
	"sync"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/flusher"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type FlusherSuite struct {
	suite.Suite

	pchannel  string
	vchannels []string

	wbMgr     *writebuffer.MockBufferManager
	rootcoord *mocks.MockRootCoordClient

	wal     wal.WAL
	flusher flusher.Flusher
}

func (s *FlusherSuite) SetupSuite() {
	paramtable.Init()
	tickDuration = 10 * time.Millisecond

	s.pchannel = "by-dev-rootcoord-dml_0"
	s.vchannels = []string{
		"by-dev-rootcoord-dml_0_123456v0",
		"by-dev-rootcoord-dml_0_123456v1",
		"by-dev-rootcoord-dml_0_123456v2",
	}

	rootcoord := mocks.NewMockRootCoordClient(s.T())

	datacoord := mocks.NewMockDataCoordClient(s.T())
	datacoord.EXPECT().GetChannelRecoveryInfo(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, request *datapb.GetChannelRecoveryInfoRequest, option ...grpc.CallOption,
		) (*datapb.GetChannelRecoveryInfoResponse, error) {
			messageID := 1
			b := make([]byte, 8)
			common.Endian.PutUint64(b, uint64(messageID))
			return &datapb.GetChannelRecoveryInfoResponse{
				Info: &datapb.VchannelInfo{
					ChannelName:  request.GetVchannel(),
					SeekPosition: &msgpb.MsgPosition{MsgID: b},
				},
				Schema: &schemapb.CollectionSchema{
					Fields: []*schemapb.FieldSchema{
						{FieldID: 100, Name: "ID", IsPrimaryKey: true},
						{FieldID: 101, Name: "Vector"},
					},
				},
			}, nil
		})

	syncMgr := syncmgr.NewMockSyncManager(s.T())
	wbMgr := writebuffer.NewMockBufferManager(s.T())
	wbMgr.EXPECT().Register(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	wbMgr.EXPECT().RemoveChannel(mock.Anything).Return()
	wbMgr.EXPECT().Start().Return()
	wbMgr.EXPECT().Stop().Return()

	resource.Init(
		resource.OptSyncManager(syncMgr),
		resource.OptBufferManager(wbMgr),
		resource.OptRootCoordClient(rootcoord),
		resource.OptDataCoordClient(datacoord),
	)

	s.wbMgr = wbMgr
	s.rootcoord = rootcoord
}

func (s *FlusherSuite) SetupTest() {
	handlers := make([]wal.MessageHandler, 0, len(s.vchannels))
	scanner := mock_wal.NewMockScanner(s.T())

	w := mock_wal.NewMockWAL(s.T())
	w.EXPECT().WALName().Return("rocksmq")
	w.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, option wal.ReadOption) (wal.Scanner, error) {
			handlers = append(handlers, option.MesasgeHandler)
			return scanner, nil
		})

	once := sync.Once{}
	scanner.EXPECT().Close().RunAndReturn(func() error {
		once.Do(func() {
			for _, handler := range handlers {
				handler.Close()
			}
		})
		return nil
	})

	s.wal = w
	s.flusher = NewFlusher()
	s.flusher.Start()
}

func (s *FlusherSuite) TearDownTest() {
	s.flusher.Stop()
}

func (s *FlusherSuite) TestFlusher_RegisterPChannel() {
	collectionsInfo := lo.Map(s.vchannels, func(vchannel string, i int) *rootcoordpb.CollectionInfoOnPChannel {
		return &rootcoordpb.CollectionInfoOnPChannel{
			CollectionId: int64(i),
			Partitions:   []*rootcoordpb.PartitionInfoOnPChannel{{PartitionId: int64(i)}},
			Vchannel:     vchannel,
		}
	})
	s.rootcoord.EXPECT().GetPChannelInfo(mock.Anything, mock.Anything).
		Return(&rootcoordpb.GetPChannelInfoResponse{Collections: collectionsInfo}, nil)

	err := s.flusher.RegisterPChannel(s.pchannel, s.wal)
	s.NoError(err)

	s.Eventually(func() bool {
		return lo.EveryBy(s.vchannels, func(vchannel string) bool {
			return s.flusher.(*flusherImpl).fgMgr.HasFlowgraph(vchannel)
		})
	}, 10*time.Second, 10*time.Millisecond)

	s.flusher.UnregisterPChannel(s.pchannel)
	s.Equal(0, s.flusher.(*flusherImpl).fgMgr.GetFlowgraphCount())
	s.Equal(0, s.flusher.(*flusherImpl).scanners.Len())
}

func (s *FlusherSuite) TestFlusher_RegisterVChannel() {
	for _, vchannel := range s.vchannels {
		s.flusher.RegisterVChannel(vchannel, s.wal)
	}
	s.Eventually(func() bool {
		return lo.EveryBy(s.vchannels, func(vchannel string) bool {
			return s.flusher.(*flusherImpl).fgMgr.HasFlowgraph(vchannel)
		})
	}, 10*time.Second, 10*time.Millisecond)

	for _, vchannel := range s.vchannels {
		s.flusher.UnregisterVChannel(vchannel)
	}
	s.Equal(0, s.flusher.(*flusherImpl).fgMgr.GetFlowgraphCount())
	s.Equal(0, s.flusher.(*flusherImpl).scanners.Len())
}

func TestFlusherSuite(t *testing.T) {
	suite.Run(t, new(FlusherSuite))
}

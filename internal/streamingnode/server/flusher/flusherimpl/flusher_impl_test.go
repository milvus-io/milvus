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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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

func init() {
	paramtable.Init()
}

func newMockDatacoord(t *testing.T, maybe bool) *mocks.MockDataCoordClient {
	datacoord := mocks.NewMockDataCoordClient(t)
	expect := datacoord.EXPECT().GetChannelRecoveryInfo(mock.Anything, mock.Anything).RunAndReturn(
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
	if maybe {
		expect.Maybe()
	}
	return datacoord
}

func newMockWAL(t *testing.T, vchannels []string, maybe bool) *mock_wal.MockWAL {
	w := mock_wal.NewMockWAL(t)
	walName := w.EXPECT().WALName().Return("rocksmq")
	if maybe {
		walName.Maybe()
	}
	for range vchannels {
		read := w.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, option wal.ReadOption) (wal.Scanner, error) {
				handler := option.MesasgeHandler
				scanner := mock_wal.NewMockScanner(t)
				scanner.EXPECT().Close().RunAndReturn(func() error {
					handler.Close()
					return nil
				})
				return scanner, nil
			})
		if maybe {
			read.Maybe()
		}
	}
	return w
}

func newTestFlusher(t *testing.T, maybe bool) flusher.Flusher {
	wbMgr := writebuffer.NewMockBufferManager(t)
	register := wbMgr.EXPECT().Register(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	removeChannel := wbMgr.EXPECT().RemoveChannel(mock.Anything).Return()
	start := wbMgr.EXPECT().Start().Return()
	stop := wbMgr.EXPECT().Stop().Return()
	if maybe {
		register.Maybe()
		removeChannel.Maybe()
		start.Maybe()
		stop.Maybe()
	}
	m := mocks.NewChunkManager(t)
	params := getPipelineParams(m)
	params.SyncMgr = syncmgr.NewMockSyncManager(t)
	params.WriteBufferManager = wbMgr
	return newFlusherWithParam(params)
}

func TestFlusher_RegisterPChannel(t *testing.T) {
	const (
		pchannel = "by-dev-rootcoord-dml_0"
		maybe    = false
	)
	vchannels := []string{
		"by-dev-rootcoord-dml_0_123456v0",
		"by-dev-rootcoord-dml_0_123456v1",
		"by-dev-rootcoord-dml_0_123456v2",
	}

	collectionsInfo := lo.Map(vchannels, func(vchannel string, i int) *rootcoordpb.CollectionInfoOnPChannel {
		return &rootcoordpb.CollectionInfoOnPChannel{
			CollectionId: int64(i),
			Partitions:   []*rootcoordpb.PartitionInfoOnPChannel{{PartitionId: int64(i)}},
			Vchannel:     vchannel,
		}
	})
	rootcoord := mocks.NewMockRootCoordClient(t)
	rootcoord.EXPECT().GetPChannelInfo(mock.Anything, mock.Anything).
		Return(&rootcoordpb.GetPChannelInfoResponse{Collections: collectionsInfo}, nil)
	datacoord := newMockDatacoord(t, maybe)
	resource.InitForTest(
		t,
		resource.OptRootCoordClient(rootcoord),
		resource.OptDataCoordClient(datacoord),
	)

	f := newTestFlusher(t, maybe)
	f.Start()
	defer f.Stop()

	w := newMockWAL(t, vchannels, maybe)
	err := f.RegisterPChannel(pchannel, w)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		return lo.EveryBy(vchannels, func(vchannel string) bool {
			return f.(*flusherImpl).fgMgr.HasFlowgraph(vchannel)
		})
	}, 10*time.Second, 10*time.Millisecond)

	f.UnregisterPChannel(pchannel)
	assert.Equal(t, 0, f.(*flusherImpl).fgMgr.GetFlowgraphCount())
	assert.Equal(t, 0, f.(*flusherImpl).channelLifetimes.Len())
}

func TestFlusher_RegisterVChannel(t *testing.T) {
	const (
		maybe = false
	)
	vchannels := []string{
		"by-dev-rootcoord-dml_0_123456v0",
		"by-dev-rootcoord-dml_0_123456v1",
		"by-dev-rootcoord-dml_0_123456v2",
	}

	datacoord := newMockDatacoord(t, maybe)
	resource.InitForTest(
		t,
		resource.OptDataCoordClient(datacoord),
	)

	f := newTestFlusher(t, maybe)
	f.Start()
	defer f.Stop()

	w := newMockWAL(t, vchannels, maybe)
	for _, vchannel := range vchannels {
		f.RegisterVChannel(vchannel, w)
	}

	assert.Eventually(t, func() bool {
		return lo.EveryBy(vchannels, func(vchannel string) bool {
			return f.(*flusherImpl).fgMgr.HasFlowgraph(vchannel)
		})
	}, 10*time.Second, 10*time.Millisecond)

	for _, vchannel := range vchannels {
		f.UnregisterVChannel(vchannel)
	}
	assert.Equal(t, 0, f.(*flusherImpl).fgMgr.GetFlowgraphCount())
	assert.Equal(t, 0, f.(*flusherImpl).channelLifetimes.Len())
}

func TestFlusher_Concurrency(t *testing.T) {
	const (
		maybe = true
	)
	vchannels := []string{
		"by-dev-rootcoord-dml_0_123456v0",
		"by-dev-rootcoord-dml_0_123456v1",
		"by-dev-rootcoord-dml_0_123456v2",
	}

	datacoord := newMockDatacoord(t, maybe)
	resource.InitForTest(
		t,
		resource.OptDataCoordClient(datacoord),
	)

	f := newTestFlusher(t, maybe)
	f.Start()
	defer f.Stop()

	w := newMockWAL(t, vchannels, maybe)
	wg := &sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		for _, vchannel := range vchannels {
			wg.Add(1)
			go func(vchannel string) {
				f.RegisterVChannel(vchannel, w)
				wg.Done()
			}(vchannel)
		}
		for _, vchannel := range vchannels {
			wg.Add(1)
			go func(vchannel string) {
				f.UnregisterVChannel(vchannel)
				wg.Done()
			}(vchannel)
		}
	}
	wg.Wait()

	for _, vchannel := range vchannels {
		f.UnregisterVChannel(vchannel)
	}

	assert.Equal(t, 0, f.(*flusherImpl).fgMgr.GetFlowgraphCount())
	assert.Equal(t, 0, f.(*flusherImpl).channelLifetimes.Len())
}

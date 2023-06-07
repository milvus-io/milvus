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

package mocks

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type MockQueryNode struct {
	*MockQueryNodeServer

	ID      int64
	addr    string
	ctx     context.Context
	cancel  context.CancelFunc
	session *sessionutil.Session
	server  *grpc.Server

	rwmutex        sync.RWMutex
	channels       map[int64][]string
	channelVersion map[string]int64
	segments       map[int64]map[string][]int64
	segmentVersion map[int64]int64
}

func NewMockQueryNode(t *testing.T, etcdCli *clientv3.Client, nodeID int64) *MockQueryNode {
	ctx, cancel := context.WithCancel(context.Background())
	node := &MockQueryNode{
		MockQueryNodeServer: NewMockQueryNodeServer(t),
		ctx:                 ctx,
		cancel:              cancel,
		session:             sessionutil.NewSession(ctx, Params.EtcdCfg.MetaRootPath.GetValue(), etcdCli),
		channels:            make(map[int64][]string),
		segments:            make(map[int64]map[string][]int64),
		ID:                  nodeID,
	}

	return node
}

func (node *MockQueryNode) Start() error {
	// Start gRPC server
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return err
	}
	node.addr = lis.Addr().String()
	node.server = grpc.NewServer()
	querypb.RegisterQueryNodeServer(node.server, node)
	go func() {
		err = node.server.Serve(lis)
	}()

	successStatus := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	node.EXPECT().GetDataDistribution(mock.Anything, mock.Anything).Return(&querypb.GetDataDistributionResponse{
		Status:   successStatus,
		NodeID:   node.ID,
		Channels: node.getAllChannels(),
		Segments: node.getAllSegments(),
	}, nil).Maybe()

	node.EXPECT().WatchDmChannels(mock.Anything, mock.Anything).Run(func(ctx context.Context, req *querypb.WatchDmChannelsRequest) {
		node.rwmutex.Lock()
		defer node.rwmutex.Unlock()

		node.channels[req.GetCollectionID()] = append(node.channels[req.GetCollectionID()],
			req.GetInfos()[0].GetChannelName())
	}).Return(successStatus, nil).Maybe()
	node.EXPECT().LoadSegments(mock.Anything, mock.Anything).Run(func(ctx context.Context, req *querypb.LoadSegmentsRequest) {
		node.rwmutex.Lock()
		defer node.rwmutex.Unlock()

		shardSegments, ok := node.segments[req.GetCollectionID()]
		if !ok {
			shardSegments = make(map[string][]int64)
			node.segments[req.GetCollectionID()] = shardSegments
		}
		segment := req.GetInfos()[0]
		shardSegments[segment.GetInsertChannel()] = append(shardSegments[segment.GetInsertChannel()],
			segment.GetSegmentID())
		node.segmentVersion[segment.GetSegmentID()] = req.GetVersion()
	}).Return(successStatus, nil).Maybe()
	node.EXPECT().GetComponentStates(mock.Anything, mock.AnythingOfType("*milvuspb.GetComponentStatesRequest")).
		Call.Return(func(context.Context, *milvuspb.GetComponentStatesRequest) *milvuspb.ComponentStates {
		select {
		case <-node.ctx.Done():
			return nil
		default:
			return &milvuspb.ComponentStates{}
		}
	}, func(context.Context, *milvuspb.GetComponentStatesRequest) error {
		select {
		case <-node.ctx.Done():
			return grpc.ErrServerStopped
		default:
			return nil
		}
	}).Maybe()

	// Register
	node.session.Init(typeutil.QueryNodeRole, node.addr, false, true)
	node.session.ServerID = node.ID
	node.session.Register()
	log.Debug("mock QueryNode started",
		zap.Int64("nodeID", node.ID),
		zap.String("nodeAddr", node.addr))

	return err
}

func (node *MockQueryNode) Stopping() {
	node.session.GoingStop()
}

func (node *MockQueryNode) Stop() {
	node.cancel()
	node.server.GracefulStop()
	node.session.Revoke(time.Second)
}

func (node *MockQueryNode) getAllChannels() []*querypb.ChannelVersionInfo {
	node.rwmutex.RLock()
	defer node.rwmutex.RUnlock()

	ret := make([]*querypb.ChannelVersionInfo, 0)
	for collection, channels := range node.channels {
		for _, channel := range channels {
			ret = append(ret, &querypb.ChannelVersionInfo{
				Channel:    channel,
				Collection: collection,
				Version:    node.channelVersion[channel],
			})
		}
	}
	return ret
}

func (node *MockQueryNode) getAllSegments() []*querypb.SegmentVersionInfo {
	node.rwmutex.RLock()
	defer node.rwmutex.RUnlock()

	ret := make([]*querypb.SegmentVersionInfo, 0)
	for collection, shardSegments := range node.segments {
		for shard, segments := range shardSegments {
			for _, segment := range segments {
				ret = append(ret, &querypb.SegmentVersionInfo{
					ID:         segment,
					Collection: collection,
					Channel:    shard,
					Version:    node.segmentVersion[segment],
				})
			}
		}
	}
	return ret
}

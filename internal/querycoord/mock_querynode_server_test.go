// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.
package querycoord

import (
	"context"
	"errors"
	"net"
	"strconv"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type queryNodeServerMock struct {
	querypb.QueryNodeServer
	ctx         context.Context
	cancel      context.CancelFunc
	session     *sessionutil.Session
	grpcErrChan chan error
	grpcServer  *grpc.Server

	queryNodeIP   string
	queryNodePort int64
	queryNodeID   int64

	addQueryChannels    func() (*commonpb.Status, error)
	removeQueryChannels func() (*commonpb.Status, error)
	watchDmChannels     func() (*commonpb.Status, error)
	loadSegment         func() (*commonpb.Status, error)
	releaseCollection   func() (*commonpb.Status, error)
	releasePartition    func() (*commonpb.Status, error)
	releaseSegments     func() (*commonpb.Status, error)
}

func newQueryNodeServerMock(ctx context.Context) *queryNodeServerMock {
	ctx1, cancel := context.WithCancel(ctx)
	return &queryNodeServerMock{
		ctx:         ctx1,
		cancel:      cancel,
		grpcErrChan: make(chan error),

		addQueryChannels:    returnSuccessResult,
		removeQueryChannels: returnSuccessResult,
		watchDmChannels:     returnSuccessResult,
		loadSegment:         returnSuccessResult,
		releaseCollection:   returnSuccessResult,
		releasePartition:    returnSuccessResult,
		releaseSegments:     returnSuccessResult,
	}
}

func (qs *queryNodeServerMock) Register() error {
	log.Debug("query node session info", zap.String("metaPath", Params.MetaRootPath), zap.Strings("etcdEndPoints", Params.EtcdEndpoints))
	qs.session = sessionutil.NewSession(qs.ctx, Params.MetaRootPath, Params.EtcdEndpoints)
	qs.session.Init(typeutil.QueryNodeRole, qs.queryNodeIP+":"+strconv.FormatInt(qs.queryNodePort, 10), false)
	qs.queryNodeID = qs.session.ServerID
	log.Debug("query nodeID", zap.Int64("nodeID", qs.queryNodeID))
	log.Debug("query node address", zap.String("address", qs.session.Address))

	return nil
}

func (qs *queryNodeServerMock) init() error {
	qs.queryNodeIP = funcutil.GetLocalIP()
	grpcPort := Params.Port

	go func() {
		var lis net.Listener
		var err error
		err = retry.Do(qs.ctx, func() error {
			addr := ":" + strconv.Itoa(grpcPort)
			lis, err = net.Listen("tcp", addr)
			if err == nil {
				qs.queryNodePort = int64(lis.Addr().(*net.TCPAddr).Port)
			} else {
				// set port=0 to get next available port
				grpcPort = 0
			}
			return err
		}, retry.Attempts(2))
		if err != nil {
			qs.grpcErrChan <- err
		}

		qs.grpcServer = grpc.NewServer()
		querypb.RegisterQueryNodeServer(qs.grpcServer, qs)
		go funcutil.CheckGrpcReady(qs.ctx, qs.grpcErrChan)
		if err = qs.grpcServer.Serve(lis); err != nil {
			qs.grpcErrChan <- err
		}
	}()

	err := <-qs.grpcErrChan
	if err != nil {
		return err
	}

	if err := qs.Register(); err != nil {
		return err
	}

	return nil
}

func (qs *queryNodeServerMock) start() error {
	return nil
}

func (qs *queryNodeServerMock) stop() error {
	qs.cancel()
	if qs.grpcServer != nil {
		qs.grpcServer.GracefulStop()
	}

	return nil
}

func (qs *queryNodeServerMock) run() error {
	if err := qs.init(); err != nil {
		return err
	}

	if err := qs.start(); err != nil {
		return err
	}

	return nil
}

func (qs *queryNodeServerMock) GetComponentStates(ctx context.Context, req *internalpb.GetComponentStatesRequest) (*internalpb.ComponentStates, error) {
	return &internalpb.ComponentStates{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}, nil
}

func (qs *queryNodeServerMock) AddQueryChannel(ctx context.Context, req *querypb.AddQueryChannelRequest) (*commonpb.Status, error) {
	return qs.addQueryChannels()
}

func (qs *queryNodeServerMock) RemoveQueryChannel(ctx context.Context, req *querypb.RemoveQueryChannelRequest) (*commonpb.Status, error) {
	return qs.removeQueryChannels()
}

func (qs *queryNodeServerMock) WatchDmChannels(ctx context.Context, req *querypb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	return qs.watchDmChannels()
}

func (qs *queryNodeServerMock) LoadSegments(ctx context.Context, req *querypb.LoadSegmentsRequest) (*commonpb.Status, error) {
	return qs.loadSegment()
}

func (qs *queryNodeServerMock) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return qs.releaseCollection()
}

func (qs *queryNodeServerMock) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	return qs.releasePartition()
}

func (qs *queryNodeServerMock) ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest) (*commonpb.Status, error) {
	return qs.releaseSegments()
}

func (qs *queryNodeServerMock) GetSegmentInfo(context.Context, *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	return &querypb.GetSegmentInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}, nil
}

func (qs *queryNodeServerMock) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}, nil
}

func startQueryNodeServer(ctx context.Context) (*queryNodeServerMock, error) {
	node := newQueryNodeServerMock(ctx)
	err := node.run()
	if err != nil {
		return nil, err
	}

	return node, nil
}

func returnSuccessResult() (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

func returnFailedResult() (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}, errors.New("query node do task failed")
}

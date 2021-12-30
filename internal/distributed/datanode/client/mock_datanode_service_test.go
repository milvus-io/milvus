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

package grpcdatanodeclient

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/endpoints"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type server struct {
}

func (s *server) GetComponentStates(_ context.Context, _ *internalpb.GetComponentStatesRequest) (*internalpb.ComponentStates, error) {
	return &internalpb.ComponentStates{}, nil
}

func (s *server) GetStatisticsChannel(_ context.Context, _ *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{}, nil
}

func (s *server) WatchDmChannels(_ context.Context, _ *datapb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (s *server) FlushSegments(_ context.Context, _ *datapb.FlushSegmentsRequest) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

// https://wiki.lfaidata.foundation/display/MIL/MEP+8+--+Add+metrics+for+proxy
func (s *server) GetMetrics(_ context.Context, _ *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return &milvuspb.GetMetricsResponse{}, nil
}

func (s *server) Compaction(_ context.Context, _ *datapb.CompactionPlan) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func startMockDataNodeService(ctx context.Context, etcdCli *clientv3.Client) (*grpc.Server, int, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, -1, err
	}

	s := grpc.NewServer()
	datapb.RegisterDataNodeServer(s, &server{})
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Error("failed to serve mock datanode server", zap.Error(err))
		}
	}()

	nodeID := 1
	em, err := endpoints.NewManager(etcdCli, datanodePrefix)
	if err != nil {
		s.GracefulStop()
		return nil, -1, err
	}

	err = em.AddEndpoint(ctx, fmt.Sprintf("%s/%d", datanodePrefix, nodeID), endpoints.Endpoint{Addr: lis.Addr().String()})
	if err != nil {
		s.GracefulStop()
		return nil, -1, err
	}

	return s, nodeID, nil
}

func startEmbedEtcd(ctx context.Context, dir string) (*embed.Etcd, error) {
	lisURL, err := url.Parse("http://127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	cfg := embed.NewConfig()
	cfg.Dir = dir
	cfg.LPUrls = []url.URL{*lisURL}
	cfg.LCUrls = []url.URL{*lisURL}
	s, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, err
	}

	select {
	case <-s.Server.ReadyNotify():
		return s, nil
	case <-ctx.Done():
		return nil, errors.New("wait for embed etcd ready timeout")
	}
}

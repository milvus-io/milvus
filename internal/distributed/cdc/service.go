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

package cdc

import (
	"context"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/cdc"
	"github.com/milvus-io/milvus/internal/cdc/replication/replicatemanager"
	"github.com/milvus-io/milvus/internal/cdc/resource"
	"github.com/milvus-io/milvus/internal/util/componentutil"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Server is the server of cdc.
type Server struct {
	ctx    context.Context
	cancel context.CancelFunc

	cdcServer *cdc.CDCServer
	etcdCli   *clientv3.Client

	componentState *componentutil.ComponentStateService
	stopOnce       sync.Once
}

// NewServer create a new CDC server.
func NewServer(ctx context.Context) (*Server, error) {
	ctx1, cancel := context.WithCancel(ctx) //nolint:gosec // cancel is stored in Server and called on Stop()
	return &Server{
		ctx:            ctx1,
		cancel:         cancel,
		componentState: componentutil.NewComponentStateService(typeutil.CDCRole),
		stopOnce:       sync.Once{},
	}, nil
}

func (s *Server) Prepare() error {
	return nil
}

// Run runs the server.
func (s *Server) Run() error {
	if err := s.init(); err != nil {
		return err
	}
	mlog.Info(s.ctx, "cdc init done")

	if err := s.start(); err != nil {
		return err
	}
	mlog.Info(s.ctx, "cdc start done")
	return nil
}

// Stop stops the server, should be call after Run returned.
func (s *Server) Stop() (err error) {
	s.stopOnce.Do(s.stop)
	return nil
}

// stop stops the server.
func (s *Server) stop() {
	s.componentState.OnStopping()

	defer s.cancel()

	mlog.Info(s.ctx, "stopping cdc...")

	// Stop CDC service.
	s.cdcServer.Stop()

	// Don't close s.etcdCli here because it's a shared instance from kvfactory.
	// The kvfactory.CloseEtcdClient() will be called in roles.go to close it properly.

	mlog.Info(s.ctx, "cdc stop done")
}

// Health check the health status of cdc.
func (s *Server) Health(ctx context.Context) commonpb.StateCode {
	resp, _ := s.componentState.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	return resp.GetState().StateCode
}

func (s *Server) init() (err error) {
	defer func() {
		if err != nil {
			mlog.Error(s.ctx, "cdc init failed", mlog.Err(err))
			return
		}
		mlog.Info(s.ctx, "init cdc server finished")
	}()

	// Create etcd client.
	s.etcdCli, _ = kvfactory.GetEtcdAndPath()

	// Create CDC service.
	s.cdcServer = cdc.NewCDCServer(s.ctx)
	resource.Init(
		resource.OptETCD(s.etcdCli),
		resource.OptReplicateManagerClient(replicatemanager.NewReplicateManager()),
	)
	return nil
}

func (s *Server) start() (err error) {
	defer func() {
		if err != nil {
			mlog.Error(s.ctx, "CDC start failed", mlog.Err(err))
			return
		}
		mlog.Info(s.ctx, "start CDC server finished")
	}()

	s.cdcServer.Start()

	s.componentState.OnInitialized(0)
	return nil
}

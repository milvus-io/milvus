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
	"time"

	"github.com/tikv/client-go/v2/txnkv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/cdc"
	"github.com/milvus-io/milvus/internal/cdc/controller/controllerimpl"
	"github.com/milvus-io/milvus/internal/cdc/replication/replicatemanager"
	"github.com/milvus-io/milvus/internal/cdc/resource"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	tikvkv "github.com/milvus-io/milvus/internal/kv/tikv"
	"github.com/milvus-io/milvus/internal/util/componentutil"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tikv"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// Server is the server of cdc.
type Server struct {
	ctx    context.Context
	cancel context.CancelFunc

	metaKV    kv.MetaKv
	cdcServer *cdc.CDCServer

	etcdCli *clientv3.Client
	tikvCli *txnkv.Client

	componentState *componentutil.ComponentStateService
	stopOnce       sync.Once
}

// NewServer create a new CDC server.
func NewServer(ctx context.Context) (*Server, error) {
	ctx1, cancel := context.WithCancel(ctx)
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
	log.Ctx(s.ctx).Info("cdc init done")

	if err := s.start(); err != nil {
		return err
	}
	log.Ctx(s.ctx).Info("cdc start done")
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
	log := log.Ctx(s.ctx)

	log.Info("stopping cdc...")

	// Stop CDC service.
	s.cdcServer.Stop()

	// Stop etcd
	if s.etcdCli != nil {
		if err := s.etcdCli.Close(); err != nil {
			log.Warn("cdc stop etcd client failed", zap.Error(err))
		}
	}

	// Stop tikv
	if s.tikvCli != nil {
		if err := s.tikvCli.Close(); err != nil {
			log.Warn("cdc stop tikv client failed", zap.Error(err))
		}
	}

	log.Info("cdc stop done")
}

// Health check the health status of cdc.
func (s *Server) Health(ctx context.Context) commonpb.StateCode {
	resp, _ := s.componentState.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	return resp.GetState().StateCode
}

func (s *Server) init() (err error) {
	log := log.Ctx(s.ctx)
	defer func() {
		if err != nil {
			log.Error("cdc init failed", zap.Error(err))
			return
		}
		log.Info("init cdc server finished")
	}()

	// Create etcd client.
	s.etcdCli, _ = kvfactory.GetEtcdAndPath()

	if err := s.initMeta(); err != nil {
		return err
	}

	// Create CDC service.
	s.cdcServer = cdc.NewCDCServer(s.ctx)
	resource.Init(
		resource.OptMetaKV(s.metaKV),
		resource.OptReplicateManagerClient(replicatemanager.NewReplicateManager()),
		resource.OptController(controllerimpl.NewController()),
	)
	return nil
}

func (s *Server) start() (err error) {
	log := log.Ctx(s.ctx)
	defer func() {
		if err != nil {
			log.Error("CDC start failed", zap.Error(err))
			return
		}
		log.Info("start CDC server finished")
	}()

	s.cdcServer.Start()

	s.componentState.OnInitialized(0)
	return nil
}

func (s *Server) initMeta() error {
	params := paramtable.Get()
	metaType := params.MetaStoreCfg.MetaStoreType.GetValue()
	log := log.Ctx(s.ctx)
	log.Info("cdc connecting to metadata store", zap.String("metaType", metaType))
	metaRootPath := ""
	if metaType == util.MetaStoreTypeTiKV {
		var err error
		s.tikvCli, err = tikv.GetTiKVClient(&paramtable.Get().TiKVCfg)
		if err != nil {
			log.Warn("cdc init tikv client failed", zap.Error(err))
			return err
		}
		metaRootPath = params.TiKVCfg.MetaRootPath.GetValue()
		s.metaKV = tikvkv.NewTiKV(s.tikvCli, metaRootPath,
			tikvkv.WithRequestTimeout(paramtable.Get().ServiceParam.TiKVCfg.RequestTimeout.GetAsDuration(time.Millisecond)))
	} else if metaType == util.MetaStoreTypeEtcd {
		metaRootPath = params.EtcdCfg.MetaRootPath.GetValue()
		s.metaKV = etcdkv.NewEtcdKV(s.etcdCli, metaRootPath,
			etcdkv.WithRequestTimeout(paramtable.Get().ServiceParam.EtcdCfg.RequestTimeout.GetAsDuration(time.Millisecond)))
	}
	return nil
}

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

package indexnode

/*
#cgo pkg-config: milvus_common milvus_indexbuilder

#include <stdlib.h>
#include <stdint.h>
#include "common/init_c.h"
#include "indexbuilder/init_c.h"
*/
import "C"

import (
	"context"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/hardware"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// TODO add comments
// UniqueID is an alias of int64, is used as a unique identifier for the request.
type UniqueID = typeutil.UniqueID

// make sure IndexNode implements types.IndexNode
var _ types.IndexNode = (*IndexNode)(nil)

// make sure IndexNode implements types.IndexNodeComponent
var _ types.IndexNodeComponent = (*IndexNode)(nil)

// Params is a GlobalParamTable singleton of indexnode
var Params *paramtable.ComponentParam = paramtable.Get()

type taskKey struct {
	ClusterID string
	BuildID   UniqueID
}

// IndexNode is a component that executes the task of building indexes.
type IndexNode struct {
	lifetime lifetime.Lifetime[commonpb.StateCode]

	loopCtx    context.Context
	loopCancel func()

	sched *TaskScheduler

	once     sync.Once
	stopOnce sync.Once

	factory        dependency.Factory
	storageFactory StorageFactory
	session        *sessionutil.Session

	etcdCli *clientv3.Client
	address string

	initOnce  sync.Once
	stateLock sync.Mutex
	tasks     map[taskKey]*taskInfo
}

// NewIndexNode creates a new IndexNode component.
func NewIndexNode(ctx context.Context, factory dependency.Factory) *IndexNode {
	log.Debug("New IndexNode ...")
	rand.Seed(time.Now().UnixNano())
	ctx1, cancel := context.WithCancel(ctx)
	b := &IndexNode{
		loopCtx:        ctx1,
		loopCancel:     cancel,
		factory:        factory,
		storageFactory: &chunkMgr{},
		tasks:          map[taskKey]*taskInfo{},
		lifetime:       lifetime.NewLifetime(commonpb.StateCode_Abnormal),
	}
	sc := NewTaskScheduler(b.loopCtx)

	b.sched = sc
	return b
}

// Register register index node at etcd.
func (i *IndexNode) Register() error {
	i.session.Register()

	//start liveness check
	i.session.LivenessCheck(i.loopCtx, func() {
		log.Error("Index Node disconnected from etcd, process will exit", zap.Int64("Server Id", i.session.ServerID))
		if err := i.Stop(); err != nil {
			log.Fatal("failed to stop server", zap.Error(err))
		}
		// manually send signal to starter goroutine
		if i.session.TriggerKill {
			if p, err := os.FindProcess(os.Getpid()); err == nil {
				p.Signal(syscall.SIGINT)
			}
		}
	})
	return nil
}

func (i *IndexNode) initSegcore() {
	cEasyloggingYaml := C.CString(path.Join(Params.BaseTable.GetConfigDir(), paramtable.DefaultEasyloggingYaml))
	C.IndexBuilderInit(cEasyloggingYaml)
	C.free(unsafe.Pointer(cEasyloggingYaml))

	// override index builder SIMD type
	cSimdType := C.CString(Params.CommonCfg.SimdType.GetValue())
	C.IndexBuilderSetSimdType(cSimdType)
	C.free(unsafe.Pointer(cSimdType))

	// override segcore index slice size
	cIndexSliceSize := C.int64_t(Params.CommonCfg.IndexSliceSize.GetAsInt64())
	C.InitIndexSliceSize(cIndexSliceSize)

	cThreadCoreCoefficient := C.int64_t(Params.CommonCfg.ThreadCoreCoefficient.GetAsInt64())
	C.InitThreadCoreCoefficient(cThreadCoreCoefficient)

	cCPUNum := C.int(hardware.GetCPUNum())
	C.InitCpuNum(cCPUNum)

	localDataRootPath := filepath.Join(Params.LocalStorageCfg.Path.GetValue(), typeutil.IndexNodeRole)
	initcore.InitLocalChunkManager(localDataRootPath)
}

func (i *IndexNode) initSession() error {
	i.session = sessionutil.NewSession(i.loopCtx, Params.EtcdCfg.MetaRootPath.GetValue(), i.etcdCli)
	if i.session == nil {
		return errors.New("failed to initialize session")
	}
	i.session.Init(typeutil.IndexNodeRole, i.address, false, true)
	return nil
}

// Init initializes the IndexNode component.
func (i *IndexNode) Init() error {
	var initErr error
	i.initOnce.Do(func() {
		i.UpdateStateCode(commonpb.StateCode_Initializing)
		log.Info("IndexNode init", zap.String("state", i.lifetime.GetState().String()))
		err := i.initSession()
		if err != nil {
			log.Error(err.Error())
			initErr = err
			return
		}
		log.Info("IndexNode init session successful", zap.Int64("serverID", i.session.ServerID))

		if err != nil {
			log.Error("IndexNode NewMinIOKV failed", zap.Error(err))
			initErr = err
			return
		}

		log.Info("IndexNode NewMinIOKV succeeded")

		i.initSegcore()
	})

	log.Info("Init IndexNode finished", zap.Error(initErr))

	return initErr
}

// Start starts the IndexNode component.
func (i *IndexNode) Start() error {
	var startErr error
	i.once.Do(func() {
		startErr = i.sched.Start()

		i.UpdateStateCode(commonpb.StateCode_Healthy)
		log.Info("IndexNode", zap.Any("State", i.lifetime.GetState().String()))
	})

	log.Info("IndexNode start finished", zap.Error(startErr))
	return startErr
}

// Stop closes the server.
func (i *IndexNode) Stop() error {
	i.stopOnce.Do(func() {
		i.UpdateStateCode(commonpb.StateCode_Stopping)
		log.Info("Index node stopping")
		err := i.session.GoingStop()
		if err != nil {
			log.Warn("session fail to go stopping state", zap.Error(err))
		} else {
			i.waitTaskFinish()
		}

		// https://github.com/milvus-io/milvus/issues/12282
		i.UpdateStateCode(commonpb.StateCode_Abnormal)
		i.lifetime.Wait()
		log.Info("Index node abnormal")
		// cleanup all running tasks
		deletedTasks := i.deleteAllTasks()
		for _, task := range deletedTasks {
			if task.cancel != nil {
				task.cancel()
			}
		}
		i.loopCancel()
		if i.sched != nil {
			i.sched.Close()
		}
		if i.session != nil {
			i.session.Stop()
		}

		log.Info("Index node stopped.")
	})
	return nil
}

// UpdateStateCode updates the component state of IndexNode.
func (i *IndexNode) UpdateStateCode(code commonpb.StateCode) {
	i.lifetime.SetState(code)
}

// SetEtcdClient assigns parameter client to its member etcdCli
func (i *IndexNode) SetEtcdClient(client *clientv3.Client) {
	i.etcdCli = client
}

// GetComponentStates gets the component states of IndexNode.
func (i *IndexNode) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	log.RatedInfo(10, "get IndexNode components states ...")
	nodeID := common.NotRegisteredID
	if i.session != nil && i.session.Registered() {
		nodeID = i.session.ServerID
	}
	stateInfo := &milvuspb.ComponentInfo{
		// NodeID:    Params.NodeID, // will race with i.Register()
		NodeID:    nodeID,
		Role:      typeutil.IndexNodeRole,
		StateCode: i.lifetime.GetState(),
	}

	ret := &milvuspb.ComponentStates{
		State:              stateInfo,
		SubcomponentStates: nil, // todo add subcomponents states
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}

	log.RatedInfo(10, "IndexNode Component states",
		zap.String("State", ret.State.String()),
		zap.String("Status", ret.GetStatus().GetErrorCode().String()),
		zap.Any("SubcomponentStates", ret.SubcomponentStates))
	return ret, nil
}

// GetTimeTickChannel gets the time tick channel of IndexNode.
func (i *IndexNode) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	log.RatedInfo(10, "get IndexNode time tick channel ...")

	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}, nil
}

// GetStatisticsChannel gets the statistics channel of IndexNode.
func (i *IndexNode) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	log.RatedInfo(10, "get IndexNode statistics channel ...")
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}, nil
}

func (i *IndexNode) GetNodeID() int64 {
	return paramtable.GetNodeID()
}

// ShowConfigurations returns the configurations of indexNode matching req.Pattern
func (i *IndexNode) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	if !i.lifetime.Add(commonpbutil.IsHealthyOrStopping) {
		log.Warn("IndexNode.ShowConfigurations failed",
			zap.Int64("nodeId", paramtable.GetNodeID()),
			zap.String("req", req.Pattern),
			zap.Error(errIndexNodeIsUnhealthy(paramtable.GetNodeID())))

		return &internalpb.ShowConfigurationsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msgIndexNodeIsUnhealthy(paramtable.GetNodeID()),
			},
			Configuations: nil,
		}, nil
	}

	configList := make([]*commonpb.KeyValuePair, 0)
	for key, value := range Params.GetComponentConfigurations("indexnode", req.Pattern) {
		configList = append(configList,
			&commonpb.KeyValuePair{
				Key:   key,
				Value: value,
			})
	}

	return &internalpb.ShowConfigurationsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Configuations: configList,
	}, nil
}

func (i *IndexNode) SetAddress(address string) {
	i.address = address
}

func (i *IndexNode) GetAddress() string {
	return i.address
}

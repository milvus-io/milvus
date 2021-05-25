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

package indexnode

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"strconv"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	miniokv "github.com/milvus-io/milvus/internal/kv/minio"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	reqTimeoutInterval = time.Second * 10
)

type UniqueID = typeutil.UniqueID
type Timestamp = typeutil.Timestamp

type IndexNode struct {
	stateCode internalpb.StateCode

	loopCtx    context.Context
	loopCancel func()

	sched *TaskScheduler

	kv      kv.BaseKV
	session *sessionutil.Session

	serviceClient types.IndexService // method factory

	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()

	closer io.Closer
}

func NewIndexNode(ctx context.Context) (*IndexNode, error) {
	log.Debug("new index node ...")
	rand.Seed(time.Now().UnixNano())
	ctx1, cancel := context.WithCancel(ctx)
	b := &IndexNode{
		loopCtx:    ctx1,
		loopCancel: cancel,
	}
	var err error
	b.sched, err = NewTaskScheduler(b.loopCtx, b.kv)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// Register register index node at etcd
func (i *IndexNode) Register() error {
	i.session = sessionutil.NewSession(i.loopCtx, []string{Params.EtcdAddress})
	i.session.Init(typeutil.IndexNodeRole, Params.IP+":"+strconv.Itoa(Params.Port), false)
	Params.NodeID = i.session.ServerID
	return nil
}

func (i *IndexNode) Init() error {
	ctx := context.Background()

	err := funcutil.WaitForComponentHealthy(ctx, i.serviceClient, "IndexService", 1000000, time.Millisecond*200)
	if err != nil {
		return err
	}
	request := &indexpb.RegisterNodeRequest{
		Base: nil,
		Address: &commonpb.Address{
			Ip:   Params.IP,
			Port: int64(Params.Port),
		},
	}

	resp, err2 := i.serviceClient.RegisterNode(ctx, request)
	if err2 != nil {
		log.Debug("indexnode", zap.String("Index NodeImpl connect to IndexService failed", err.Error()))
		return err2
	}

	if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(resp.Status.Reason)
	}

	err = Params.LoadConfigFromInitParams(resp.InitParams)
	if err != nil {
		return err
	}

	option := &miniokv.Option{
		Address:           Params.MinIOAddress,
		AccessKeyID:       Params.MinIOAccessKeyID,
		SecretAccessKeyID: Params.MinIOSecretAccessKey,
		UseSSL:            Params.MinIOUseSSL,
		BucketName:        Params.MinioBucketName,
		CreateBucket:      true,
	}
	i.kv, err = miniokv.NewMinIOKV(i.loopCtx, option)
	if err != nil {
		return err
	}

	i.UpdateStateCode(internalpb.StateCode_Healthy)

	return nil
}

func (i *IndexNode) Start() error {
	i.sched.Start()

	// Start callbacks
	for _, cb := range i.startCallbacks {
		cb()
	}
	return nil
}

// Close closes the server.
func (i *IndexNode) Stop() error {
	i.loopCancel()
	if i.sched != nil {
		i.sched.Close()
	}
	for _, cb := range i.closeCallbacks {
		cb()
	}
	log.Debug("NodeImpl  closed.")
	return nil
}

func (i *IndexNode) UpdateStateCode(code internalpb.StateCode) {
	i.stateCode = code
}

func (i *IndexNode) SetIndexServiceClient(serviceClient types.IndexService) {
	i.serviceClient = serviceClient
}

func (i *IndexNode) BuildIndex(ctx context.Context, request *indexpb.BuildIndexRequest) (*commonpb.Status, error) {
	log.Debug("indexnode building index ...",
		zap.Int64("IndexBuildID", request.IndexBuildID),
		zap.String("Indexname", request.IndexName),
		zap.Int64("IndexID", request.IndexID),
		zap.Strings("DataPaths", request.DataPaths),
		zap.Any("TypeParams", request.TypeParams),
		zap.Any("IndexParams", request.IndexParams))

	t := &IndexBuildTask{
		BaseTask: BaseTask{
			ctx:  ctx,
			done: make(chan error),
		},
		req:           request,
		kv:            i.kv,
		serviceClient: i.serviceClient,
		nodeID:        Params.NodeID,
	}

	ret := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}

	err := i.sched.IndexBuildQueue.Enqueue(t)
	if err != nil {
		ret.ErrorCode = commonpb.ErrorCode_UnexpectedError
		ret.Reason = err.Error()
		return ret, nil
	}
	log.Debug("indexnode", zap.Int64("indexnode successfully schedule with indexBuildID", request.IndexBuildID))
	return ret, nil
}

func (i *IndexNode) DropIndex(ctx context.Context, request *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	log.Debug("IndexNode", zap.Int64("Drop index by id", request.IndexID))
	indexBuildIDs := i.sched.IndexBuildQueue.tryToRemoveUselessIndexBuildTask(request.IndexID)
	log.Debug("IndexNode", zap.Any("The index of the IndexBuildIDs to be deleted", indexBuildIDs))
	for _, indexBuildID := range indexBuildIDs {
		nty := &indexpb.NotifyBuildIndexRequest{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			IndexBuildID:   indexBuildID,
			NodeID:         Params.NodeID,
			IndexFilePaths: []string{},
		}
		resp, err := i.serviceClient.NotifyBuildIndex(ctx, nty)
		if err != nil {
			log.Warn("IndexNode", zap.String("DropIndex notify error", err.Error()))
		} else if resp.ErrorCode != commonpb.ErrorCode_Success {
			log.Warn("IndexNode", zap.String("DropIndex notify error reason", resp.Reason))
		}
	}
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

// AddStartCallback adds a callback in the startServer phase.
func (i *IndexNode) AddStartCallback(callbacks ...func()) {
	i.startCallbacks = append(i.startCallbacks, callbacks...)
}

// AddCloseCallback adds a callback in the Close phase.
func (i *IndexNode) AddCloseCallback(callbacks ...func()) {
	i.closeCallbacks = append(i.closeCallbacks, callbacks...)
}

func (i *IndexNode) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	log.Debug("get indexnode components states ...")
	stateInfo := &internalpb.ComponentInfo{
		NodeID:    Params.NodeID,
		Role:      "NodeImpl",
		StateCode: i.stateCode,
	}

	ret := &internalpb.ComponentStates{
		State:              stateInfo,
		SubcomponentStates: nil, // todo add subcomponents states
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}

	log.Debug("indexnode compoents states",
		zap.Any("State", ret.State),
		zap.Any("Status", ret.Status),
		zap.Any("SubcomponentStates", ret.SubcomponentStates))
	return ret, nil
}

func (i *IndexNode) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	log.Debug("get indexnode time tick channel ...")

	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}, nil
}

func (i *IndexNode) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	log.Debug("get indexnode statistics channel ...")
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}, nil
}

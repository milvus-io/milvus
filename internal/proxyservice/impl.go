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

package proxyservice

import (
	"context"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"strconv"
	"time"

	"github.com/milvus-io/milvus/internal/timesync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
)

const (
	timeoutInterval      = time.Second * 10
	StartParamsKey       = "START_PARAMS"
	ChannelYamlContent   = "advanced/channel.yaml"
	CommonYamlContent    = "advanced/common.yaml"
	DataNodeYamlContent  = "advanced/data_node.yaml"
	MasterYamlContent    = "advanced/master.yaml"
	ProxyNodeYamlContent = "advanced/proxy_node.yaml"
	QueryNodeYamlContent = "advanced/query_node.yaml"
	MilvusYamlContent    = "milvus.yaml"
)

func (s *ProxyService) fillNodeInitParams() error {
	s.nodeStartParams = make([]*commonpb.KeyValuePair, 0)

	getConfigContentByName := func(fileName string) []byte {
		_, fpath, _, _ := runtime.Caller(0)
		configFile := path.Dir(fpath) + "/../../configs/" + fileName
		_, err := os.Stat(configFile)
		log.Debug("proxyservice", zap.String("configFile = ", configFile))
		if os.IsNotExist(err) {
			runPath, err := os.Getwd()
			if err != nil {
				panic(err)
			}
			configFile = runPath + "/configs/" + fileName
		}
		data, err := ioutil.ReadFile(configFile)
		if err != nil {
			panic(err)
		}
		return append(data, []byte("\n")...)
	}

	channelYamlContent := getConfigContentByName(ChannelYamlContent)
	commonYamlContent := getConfigContentByName(CommonYamlContent)
	dataNodeYamlContent := getConfigContentByName(DataNodeYamlContent)
	masterYamlContent := getConfigContentByName(MasterYamlContent)
	proxyNodeYamlContent := getConfigContentByName(ProxyNodeYamlContent)
	queryNodeYamlContent := getConfigContentByName(QueryNodeYamlContent)
	milvusYamlContent := getConfigContentByName(MilvusYamlContent)

	appendContent := func(key string, content []byte) {
		s.nodeStartParams = append(s.nodeStartParams, &commonpb.KeyValuePair{
			Key:   StartParamsKey + "_" + key,
			Value: string(content),
		})
	}
	appendContent(ChannelYamlContent, channelYamlContent)
	appendContent(CommonYamlContent, commonYamlContent)
	appendContent(DataNodeYamlContent, dataNodeYamlContent)
	appendContent(MasterYamlContent, masterYamlContent)
	appendContent(ProxyNodeYamlContent, proxyNodeYamlContent)
	appendContent(QueryNodeYamlContent, queryNodeYamlContent)
	appendContent(MilvusYamlContent, milvusYamlContent)

	// var allContent []byte
	// allContent = append(allContent, channelYamlContent...)
	// allContent = append(allContent, commonYamlContent...)
	// allContent = append(allContent, dataNodeYamlContent...)
	// allContent = append(allContent, masterYamlContent...)
	// allContent = append(allContent, proxyNodeYamlContent...)
	// allContent = append(allContent, queryNodeYamlContent...)
	// allContent = append(allContent, writeNodeYamlContent...)
	// allContent = append(allContent, milvusYamlContent...)

	// s.nodeStartParams = append(s.nodeStartParams, &commonpb.KeyValuePair{
	// 	Key:   StartParamsKey,
	// 	Value: string(allContent),
	// })

	return nil
}

func (s *ProxyService) Init() error {
	err := s.fillNodeInitParams()
	if err != nil {
		return err
	}
	log.Debug("fill node init params ...")

	m := map[string]interface{}{
		"PulsarAddress":  Params.PulsarAddress,
		"ReceiveBufSize": 1024,
		"PulsarBufSize":  1024}
	err = s.msFactory.SetParams(m)
	if err != nil {
		return err
	}

	serviceTimeTickMsgStream, _ := s.msFactory.NewTtMsgStream(s.ctx)
	serviceTimeTickMsgStream.AsProducer([]string{Params.ServiceTimeTickChannel})
	log.Debug("proxyservice", zap.Strings("proxyservice AsProducer", []string{Params.ServiceTimeTickChannel}))
	log.Debug("proxyservice", zap.Strings("create service time tick producer channel", []string{Params.ServiceTimeTickChannel}))

	channels := make([]string, Params.InsertChannelNum)
	var i int64 = 0
	for ; i < Params.InsertChannelNum; i++ {
		channels[i] = Params.InsertChannelPrefixName + strconv.FormatInt(i, 10)
	}
	insertTickMsgStream, _ := s.msFactory.NewMsgStream(s.ctx)
	insertTickMsgStream.AsProducer(channels)
	log.Debug("proxyservice", zap.Strings("create insert time tick producer channels", channels))

	nodeTimeTickMsgStream, _ := s.msFactory.NewMsgStream(s.ctx)
	nodeTimeTickMsgStream.AsConsumer(Params.NodeTimeTickChannel,
		"proxyservicesub") // TODO: add config
	log.Debug("proxyservice", zap.Strings("create node time tick consumer channel", Params.NodeTimeTickChannel))

	ttBarrier := timesync.NewSoftTimeTickBarrier(s.ctx, nodeTimeTickMsgStream, []UniqueID{1}, 10)
	log.Debug("create soft time tick barrier ...")
	s.tick = newTimeTick(s.ctx, ttBarrier, serviceTimeTickMsgStream, insertTickMsgStream)
	log.Debug("create time tick ...")

	return nil
}

func (s *ProxyService) Start() error {
	s.sched.Start()
	log.Debug("start scheduler ...")
	defer func() {
		s.UpdateStateCode(internalpb.StateCode_Healthy)
		log.Debug("proxyservice", zap.Any("state of proxyservice", internalpb.StateCode_Healthy))
	}()
	return s.tick.Start()
}

func (s *ProxyService) Stop() error {
	s.sched.Close()
	log.Debug("close scheduler ...")
	s.tick.Close()
	log.Debug("close time tick")

	err := s.nodeInfos.ReleaseAllClients()
	if err != nil {
		panic(err)
	}
	log.Debug("stop all node ProxyNodes ...")

	s.cancel()

	return nil
}

func (s *ProxyService) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	stateInfo := &internalpb.ComponentInfo{
		NodeID:    UniqueID(0),
		Role:      "ProxyService",
		StateCode: s.stateCode,
	}

	ret := &internalpb.ComponentStates{
		State:              stateInfo,
		SubcomponentStates: nil, // todo add subcomponents states
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}
	return ret, nil
}

func (s *ProxyService) UpdateStateCode(code internalpb.StateCode) {
	s.stateCode = code
}

func (s *ProxyService) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Value: Params.ServiceTimeTickChannel,
	}, nil
}

func (s *ProxyService) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	panic("implement me")
}

func (s *ProxyService) RegisterLink(ctx context.Context) (*milvuspb.RegisterLinkResponse, error) {
	log.Debug("register link")

	t := &registerLinkTask{
		ctx:       ctx,
		Condition: newTaskCondition(ctx),
		nodeInfos: s.nodeInfos,
	}

	var err error

	err = s.sched.RegisterLinkTaskQueue.Enqueue(t)
	if err != nil {
		return &milvuspb.RegisterLinkResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Address: nil,
		}, nil
	}

	err = t.WaitToFinish()
	if err != nil {
		return &milvuspb.RegisterLinkResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Address: nil,
		}, nil
	}

	return t.response, nil
}

func (s *ProxyService) RegisterNode(ctx context.Context, request *proxypb.RegisterNodeRequest) (*proxypb.RegisterNodeResponse, error) {
	log.Debug("proxyservice receive RegisterNode request",
		zap.String("ip", request.Address.Ip),
		zap.Int64("port", request.Address.Port))

	t := &registerNodeTask{
		ctx:         ctx,
		request:     request,
		startParams: s.nodeStartParams,
		Condition:   newTaskCondition(ctx),
		allocator:   s.allocator,
		nodeInfos:   s.nodeInfos,
	}

	var err error

	err = s.sched.RegisterNodeTaskQueue.Enqueue(t)
	if err != nil {
		return &proxypb.RegisterNodeResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			InitParams: nil,
		}, nil
	}

	err = t.WaitToFinish()
	if err != nil {
		return &proxypb.RegisterNodeResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			InitParams: nil,
		}, nil
	}

	return t.response, nil
}

func (s *ProxyService) InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	log.Debug("proxyservice receive InvalidateCollectionMetaCache request",
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	t := &invalidateCollectionMetaCacheTask{
		ctx:       ctx,
		request:   request,
		Condition: newTaskCondition(ctx),
		nodeInfos: s.nodeInfos,
	}

	var err error

	err = s.sched.InvalidateCollectionMetaCacheTaskQueue.Enqueue(t)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	err = t.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	return t.response, nil
}

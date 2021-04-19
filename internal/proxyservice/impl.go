package proxyservice

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"path"
	"runtime"
	"strconv"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"

	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"

	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/proxypb"
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

func (s *ServiceImpl) fillNodeInitParams() error {
	s.nodeStartParams = make([]*commonpb.KeyValuePair, 0)

	getConfigContentByName := func(fileName string) []byte {
		_, fpath, _, _ := runtime.Caller(0)
		configFile := path.Dir(fpath) + "/../../configs/" + fileName
		_, err := os.Stat(configFile)
		log.Printf("configFile = %s", configFile)
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

func (s *ServiceImpl) Init() error {
	err := s.fillNodeInitParams()
	if err != nil {
		return err
	}
	log.Println("fill node init params ...")

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
	log.Println("create service time tick producer channel: ", []string{Params.ServiceTimeTickChannel})

	channels := make([]string, Params.InsertChannelNum)
	var i int64 = 0
	for ; i < Params.InsertChannelNum; i++ {
		channels[i] = Params.InsertChannelPrefixName + strconv.FormatInt(i, 10)
	}
	insertTickMsgStream, _ := s.msFactory.NewMsgStream(s.ctx)
	insertTickMsgStream.AsProducer(channels)
	log.Println("create insert time tick producer channel: ", channels)

	nodeTimeTickMsgStream, _ := s.msFactory.NewMsgStream(s.ctx)
	nodeTimeTickMsgStream.AsConsumer(Params.NodeTimeTickChannel,
		"proxyservicesub") // TODO: add config
	log.Println("create node time tick consumer channel: ", Params.NodeTimeTickChannel)

	ttBarrier := newSoftTimeTickBarrier(s.ctx, nodeTimeTickMsgStream, []UniqueID{1}, 10)
	log.Println("create soft time tick barrier ...")
	s.tick = newTimeTick(s.ctx, ttBarrier, serviceTimeTickMsgStream, insertTickMsgStream)
	log.Println("create time tick ...")

	return nil
}

func (s *ServiceImpl) Start() error {
	s.stateCode = internalpb2.StateCode_HEALTHY
	s.sched.Start()
	log.Println("start scheduler ...")
	return s.tick.Start()
}

func (s *ServiceImpl) Stop() error {
	s.sched.Close()
	log.Println("close scheduler ...")
	s.tick.Close()
	log.Println("close time tick")

	err := s.nodeInfos.ReleaseAllClients()
	if err != nil {
		panic(err)
	}
	log.Println("stop all node clients ...")

	s.cancel()

	return nil
}

func (s *ServiceImpl) GetComponentStates() (*internalpb2.ComponentStates, error) {
	stateInfo := &internalpb2.ComponentInfo{
		NodeID:    UniqueID(0),
		Role:      "ProxyService",
		StateCode: s.stateCode,
	}

	ret := &internalpb2.ComponentStates{
		State:              stateInfo,
		SubcomponentStates: nil, // todo add subcomponents states
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
	}
	return ret, nil
}

func (s *ServiceImpl) UpdateStateCode(code internalpb2.StateCode) {
	s.stateCode = code
}

func (s *ServiceImpl) GetTimeTickChannel() (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		Value: Params.ServiceTimeTickChannel,
	}, nil
}

func (s *ServiceImpl) GetStatisticsChannel() (*milvuspb.StringResponse, error) {
	panic("implement me")
}

func (s *ServiceImpl) RegisterLink() (*milvuspb.RegisterLinkResponse, error) {
	log.Println("register link")
	ctx, cancel := context.WithTimeout(s.ctx, timeoutInterval)
	defer cancel()

	t := &RegisterLinkTask{
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		nodeInfos: s.nodeInfos,
	}

	var err error

	err = s.sched.RegisterLinkTaskQueue.Enqueue(t)
	if err != nil {
		return &milvuspb.RegisterLinkResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
			Address: nil,
		}, nil
	}

	err = t.WaitToFinish()
	if err != nil {
		return &milvuspb.RegisterLinkResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
			Address: nil,
		}, nil
	}

	return t.response, nil
}

func (s *ServiceImpl) RegisterNode(request *proxypb.RegisterNodeRequest) (*proxypb.RegisterNodeResponse, error) {
	log.Println("RegisterNode: ", request)
	ctx, cancel := context.WithTimeout(s.ctx, timeoutInterval)
	defer cancel()

	t := &RegisterNodeTask{
		ctx:         ctx,
		request:     request,
		startParams: s.nodeStartParams,
		Condition:   NewTaskCondition(ctx),
		allocator:   s.allocator,
		nodeInfos:   s.nodeInfos,
	}

	var err error

	err = s.sched.RegisterNodeTaskQueue.Enqueue(t)
	if err != nil {
		return &proxypb.RegisterNodeResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
			InitParams: nil,
		}, nil
	}

	err = t.WaitToFinish()
	if err != nil {
		return &proxypb.RegisterNodeResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
			InitParams: nil,
		}, nil
	}

	return t.response, nil
}

func (s *ServiceImpl) InvalidateCollectionMetaCache(request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	log.Println("InvalidateCollectionMetaCache")
	ctx, cancel := context.WithTimeout(s.ctx, timeoutInterval)
	defer cancel()

	t := &InvalidateCollectionMetaCacheTask{
		ctx:       ctx,
		request:   request,
		Condition: NewTaskCondition(ctx),
		nodeInfos: s.nodeInfos,
	}

	var err error

	err = s.sched.InvalidateCollectionMetaCacheTaskQueue.Enqueue(t)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}, nil
	}

	err = t.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}, nil
	}

	return t.response, nil
}

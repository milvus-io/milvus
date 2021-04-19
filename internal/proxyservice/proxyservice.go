package proxyservice

import (
	"context"
	"math/rand"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/distributed/dataservice"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"

	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

type ServiceImpl struct {
	allocator NodeIDAllocator
	sched     *TaskScheduler
	tick      TimeTick
	nodeInfos *GlobalNodeInfoTable

	state *internalpb2.ComponentStates

	dataServiceClient *dataservice.Client
	nodeStartParams   []*commonpb.KeyValuePair

	ctx    context.Context
	cancel context.CancelFunc
}

func CreateProxyService(ctx context.Context) (ProxyService, error) {
	rand.Seed(time.Now().UnixNano())
	ctx1, cancel := context.WithCancel(ctx)
	s := &ServiceImpl{
		ctx:    ctx1,
		cancel: cancel,
	}

	s.allocator = NewNodeIDAllocator()
	s.sched = NewTaskScheduler(ctx1)
	s.nodeInfos = NewGlobalNodeInfoTable()

	s.state = &internalpb2.ComponentStates{
		State: &internalpb2.ComponentInfo{
			NodeID:    0,
			Role:      "proxyservice",
			StateCode: internalpb2.StateCode_INITIALIZING,
		},
		SubcomponentStates: nil,
		Status:             &commonpb.Status{},
	}

	return s, nil
}

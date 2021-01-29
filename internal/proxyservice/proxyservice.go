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
	stateCode internalpb2.StateCode

	//subStates *internalpb2.ComponentStates

	dataServiceClient *dataservice.Client
	nodeStartParams   []*commonpb.KeyValuePair

	ctx    context.Context
	cancel context.CancelFunc
}

func NewServiceImpl(ctx context.Context) (*ServiceImpl, error) {
	rand.Seed(time.Now().UnixNano())
	ctx1, cancel := context.WithCancel(ctx)
	s := &ServiceImpl{
		ctx:    ctx1,
		cancel: cancel,
	}

	s.allocator = NewNodeIDAllocator()
	s.sched = NewTaskScheduler(ctx1)
	s.nodeInfos = NewGlobalNodeInfoTable()
	s.stateCode = internalpb2.StateCode_ABNORMAL

	return s, nil
}

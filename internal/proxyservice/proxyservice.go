package proxyservice

import (
	"context"
	"math/rand"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

type ProxyService struct {
	allocator NodeIDAllocator
	sched     *TaskScheduler
	tick      *TimeTick
	nodeInfos *GlobalNodeInfoTable
	stateCode internalpb.StateCode

	//subStates *internalpb.ComponentStates

	nodeStartParams []*commonpb.KeyValuePair

	ctx    context.Context
	cancel context.CancelFunc

	msFactory msgstream.Factory
}

func NewProxyService(ctx context.Context, factory msgstream.Factory) (*ProxyService, error) {
	rand.Seed(time.Now().UnixNano())
	ctx1, cancel := context.WithCancel(ctx)
	s := &ProxyService{
		ctx:       ctx1,
		cancel:    cancel,
		msFactory: factory,
	}

	s.allocator = NewNodeIDAllocator()
	s.sched = NewTaskScheduler(ctx1)
	s.nodeInfos = NewGlobalNodeInfoTable()
	s.stateCode = internalpb.StateCode_Abnormal

	return s, nil
}

package proxyservice

import (
	"context"
	"math/rand"
	"time"
)

type ServiceImpl struct {
	allocator NodeIDAllocator
	sched     *TaskScheduler
	nodeInfos *GlobalNodeInfoTable

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

	return s, nil
}

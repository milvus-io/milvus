package proxyservice

import (
	"context"
	"fmt"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"

	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"

	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/proxypb"
)

const (
	timeoutInterval = time.Second * 10
)

func (s ServiceImpl) Init() error {
	return nil
}

func (s *ServiceImpl) Start() error {
	s.sched.Start()
	return nil
}

func (s *ServiceImpl) Stop() error {
	s.sched.Close()
	return nil
}

func (s *ServiceImpl) GetComponentStates() (*internalpb2.ComponentStates, error) {
	panic("implement me")
}

func (s *ServiceImpl) GetTimeTickChannel() (string, error) {
	panic("implement me")
}

func (s *ServiceImpl) GetStatisticsChannel() (string, error) {
	panic("implement me")
}

func (s *ServiceImpl) RegisterLink() (*milvuspb.RegisterLinkResponse, error) {
	fmt.Println("register link")
	ctx, cancel := context.WithTimeout(s.ctx, timeoutInterval)
	defer cancel()

	t := &RegisterLinkTask{
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
	fmt.Println("RegisterNode: ", request)
	ctx, cancel := context.WithTimeout(s.ctx, timeoutInterval)
	defer cancel()

	t := &RegisterNodeTask{
		request:   request,
		Condition: NewTaskCondition(ctx),
		allocator: s.allocator,
		nodeInfos: s.nodeInfos,
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

func (s *ServiceImpl) InvalidateCollectionMetaCache(request *proxypb.InvalidateCollMetaCacheRequest) error {
	fmt.Println("InvalidateCollectionMetaCache")
	ctx, cancel := context.WithTimeout(s.ctx, timeoutInterval)
	defer cancel()

	t := &InvalidateCollectionMetaCacheTask{
		request:   request,
		Condition: NewTaskCondition(ctx),
	}

	var err error

	err = s.sched.RegisterNodeTaskQueue.Enqueue(t)
	if err != nil {
		return err
	}

	err = t.WaitToFinish()
	if err != nil {
		return err
	}

	return nil
}

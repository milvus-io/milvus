package proxyservice

import (
	"context"
	"fmt"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"

	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"

	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"

	"github.com/zilliztech/milvus-distributed/internal/proto/proxypb"

	"github.com/zilliztech/milvus-distributed/internal/errors"
)

type TaskEnum = int

const (
	FromSDK    TaskEnum = 0
	FromMaster TaskEnum = 1
	FromNode   TaskEnum = 2
)

type task interface {
	PreExecute() error
	Execute() error
	PostExecute() error
	WaitToFinish() error
	Notify(err error)
}

type Condition interface {
	WaitToFinish() error
	Notify(err error)
}

type TaskCondition struct {
	done chan error
	ctx  context.Context
}

func (c *TaskCondition) WaitToFinish() error {
	select {
	case <-c.ctx.Done():
		return errors.New("timeout")
	case err := <-c.done:
		return err
	}
}

func (c *TaskCondition) Notify(err error) {
	c.done <- err
}

func NewTaskCondition(ctx context.Context) Condition {
	return &TaskCondition{
		done: make(chan error),
		ctx:  ctx,
	}
}

type RegisterLinkTask struct {
	Condition
	response  *milvuspb.RegisterLinkResponse
	nodeInfos *GlobalNodeInfoTable
}

func (t *RegisterLinkTask) PreExecute() error {
	return nil
}

func (t *RegisterLinkTask) Execute() error {
	info, err := t.nodeInfos.Pick()
	fmt.Println("info: ", info)
	if err != nil {
		return err
	}
	t.response = &milvuspb.RegisterLinkResponse{
		Address: &commonpb.Address{
			Ip:   info.ip,
			Port: info.port,
		},
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
			Reason:    "",
		},
	}
	return nil
}

func (t *RegisterLinkTask) PostExecute() error {
	return nil
}

type RegisterNodeTask struct {
	Condition
	request   *proxypb.RegisterNodeRequest
	response  *proxypb.RegisterNodeResponse
	allocator NodeIDAllocator
	nodeInfos *GlobalNodeInfoTable
}

func (t *RegisterNodeTask) PreExecute() error {
	return nil
}

func (t *RegisterNodeTask) Execute() error {
	nodeID := t.allocator.AllocOne()
	info := NodeInfo{
		ip:   t.request.Address.Ip,
		port: t.request.Address.Port,
	}
	err := t.nodeInfos.Register(nodeID, &info)
	// TODO: fill init params
	t.response = &proxypb.RegisterNodeResponse{
		InitParams: &internalpb2.InitParams{
			NodeID:      nodeID,
			StartParams: nil,
		},
	}
	return err
}

func (t *RegisterNodeTask) PostExecute() error {
	return nil
}

type InvalidateCollectionMetaCacheTask struct {
	Condition
	request   *proxypb.InvalidateCollMetaCacheRequest
	nodeInfos *GlobalNodeInfoTable
}

func (t *InvalidateCollectionMetaCacheTask) PreExecute() error {
	return nil
}

func (t *InvalidateCollectionMetaCacheTask) Execute() error {
	var err error
	clients, err := t.nodeInfos.ObtainAllClients()
	if err != nil {
		return err
	}
	for _, c := range clients {
		err = c.InvalidateCollectionMetaCache(t.request)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *InvalidateCollectionMetaCacheTask) PostExecute() error {
	return nil
}

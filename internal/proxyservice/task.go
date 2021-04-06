package proxyservice

import (
	"context"
	"errors"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/proxypb"
)

type TaskEnum = int

const (
	FromSDK    TaskEnum = 0
	FromMaster TaskEnum = 1
	FromNode   TaskEnum = 2
)

const (
	RegisterLinkTaskName                  = "RegisLinkTask"
	RegisterNodeTaskName                  = "RegisNodeTask"
	InvalidateCollectionMetaCacheTaskName = "InvalidateCollectionMetaCacheTask"
)

type task interface {
	Ctx() context.Context
	ID() UniqueID // return ReqID
	Name() string
	PreExecute(ctx context.Context) error
	Execute(ctx context.Context) error
	PostExecute(ctx context.Context) error
	WaitToFinish() error
	Notify(err error)
}

type Condition interface {
	WaitToFinish() error
	Notify(err error)
}

type taskCondition struct {
	done chan error
	ctx  context.Context
}

func (c *taskCondition) WaitToFinish() error {
	select {
	case <-c.ctx.Done():
		return errors.New("timeout")
	case err := <-c.done:
		return err
	}
}

func (c *taskCondition) Notify(err error) {
	c.done <- err
}

func newTaskCondition(ctx context.Context) Condition {
	return &taskCondition{
		done: make(chan error),
		ctx:  ctx,
	}
}

type registerLinkTask struct {
	Condition
	ctx       context.Context
	response  *milvuspb.RegisterLinkResponse
	nodeInfos *globalNodeInfoTable
}

func (t *registerLinkTask) Ctx() context.Context {
	return t.ctx
}

func (t *registerLinkTask) ID() UniqueID {
	return 0
}

func (t *registerLinkTask) Name() string {
	return RegisterLinkTaskName
}

func (t *registerLinkTask) PreExecute(ctx context.Context) error {
	return nil
}

func (t *registerLinkTask) Execute(ctx context.Context) error {
	info, err := t.nodeInfos.Pick()
	if err != nil {
		return err
	}
	t.response = &milvuspb.RegisterLinkResponse{
		Address: &commonpb.Address{
			Ip:   info.ip,
			Port: info.port,
		},
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}
	return nil
}

func (t *registerLinkTask) PostExecute(ctx context.Context) error {
	return nil
}

type registerNodeTask struct {
	Condition
	ctx         context.Context
	request     *proxypb.RegisterNodeRequest
	response    *proxypb.RegisterNodeResponse
	startParams []*commonpb.KeyValuePair
	allocator   nodeIDAllocator
	nodeInfos   *globalNodeInfoTable
}

func (t *registerNodeTask) Ctx() context.Context {
	return t.ctx
}

func (t *registerNodeTask) ID() UniqueID {
	return t.request.Base.MsgID
}

func (t *registerNodeTask) Name() string {
	return RegisterNodeTaskName
}

func (t *registerNodeTask) PreExecute(ctx context.Context) error {
	return nil
}

func (t *registerNodeTask) Execute(ctx context.Context) error {
	nodeID := t.allocator.AllocOne()
	info := nodeInfo{
		ip:   t.request.Address.Ip,
		port: t.request.Address.Port,
	}
	err := t.nodeInfos.Register(nodeID, &info)
	// TODO: fill init params
	t.response = &proxypb.RegisterNodeResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		InitParams: &internalpb.InitParams{
			NodeID:      nodeID,
			StartParams: t.startParams,
		},
	}
	return err
}

func (t *registerNodeTask) PostExecute(ctx context.Context) error {
	return nil
}

type invalidateCollectionMetaCacheTask struct {
	Condition
	ctx       context.Context
	request   *proxypb.InvalidateCollMetaCacheRequest
	response  *commonpb.Status
	nodeInfos *globalNodeInfoTable
}

func (t *invalidateCollectionMetaCacheTask) Ctx() context.Context {
	return t.ctx
}

func (t *invalidateCollectionMetaCacheTask) ID() UniqueID {
	return t.request.Base.MsgID
}

func (t *invalidateCollectionMetaCacheTask) Name() string {
	return InvalidateCollectionMetaCacheTaskName
}

func (t *invalidateCollectionMetaCacheTask) PreExecute(ctx context.Context) error {
	return nil
}

func (t *invalidateCollectionMetaCacheTask) Execute(ctx context.Context) error {
	var err error
	clients, err := t.nodeInfos.ObtainAllClients()
	if err != nil {
		return err
	}
	for _, c := range clients {
		status, _ := c.InvalidateCollectionMetaCache(ctx, t.request)
		if status == nil {
			return errors.New("invalidate collection meta cache error")
		}
		if status.ErrorCode != commonpb.ErrorCode_Success {
			return errors.New(status.Reason)
		}
	}
	t.response = &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	return nil
}

func (t *invalidateCollectionMetaCacheTask) PostExecute(ctx context.Context) error {
	return nil
}

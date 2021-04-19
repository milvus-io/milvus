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
	ctx       context.Context
	response  *milvuspb.RegisterLinkResponse
	nodeInfos *GlobalNodeInfoTable
}

func (t *RegisterLinkTask) Ctx() context.Context {
	return t.ctx
}

func (t *RegisterLinkTask) ID() UniqueID {
	return 0
}

func (t *RegisterLinkTask) Name() string {
	return RegisterLinkTaskName
}

func (t *RegisterLinkTask) PreExecute(ctx context.Context) error {
	return nil
}

func (t *RegisterLinkTask) Execute(ctx context.Context) error {
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

func (t *RegisterLinkTask) PostExecute(ctx context.Context) error {
	return nil
}

type RegisterNodeTask struct {
	Condition
	ctx         context.Context
	request     *proxypb.RegisterNodeRequest
	response    *proxypb.RegisterNodeResponse
	startParams []*commonpb.KeyValuePair
	allocator   NodeIDAllocator
	nodeInfos   *GlobalNodeInfoTable
}

func (t *RegisterNodeTask) Ctx() context.Context {
	return t.ctx
}

func (t *RegisterNodeTask) ID() UniqueID {
	return t.request.Base.MsgID
}

func (t *RegisterNodeTask) Name() string {
	return RegisterNodeTaskName
}

func (t *RegisterNodeTask) PreExecute(ctx context.Context) error {
	return nil
}

func (t *RegisterNodeTask) Execute(ctx context.Context) error {
	nodeID := t.allocator.AllocOne()
	info := NodeInfo{
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

func (t *RegisterNodeTask) PostExecute(ctx context.Context) error {
	return nil
}

type InvalidateCollectionMetaCacheTask struct {
	Condition
	ctx       context.Context
	request   *proxypb.InvalidateCollMetaCacheRequest
	response  *commonpb.Status
	nodeInfos *GlobalNodeInfoTable
}

func (t *InvalidateCollectionMetaCacheTask) Ctx() context.Context {
	return t.ctx
}

func (t *InvalidateCollectionMetaCacheTask) ID() UniqueID {
	return t.request.Base.MsgID
}

func (t *InvalidateCollectionMetaCacheTask) Name() string {
	return InvalidateCollectionMetaCacheTaskName
}

func (t *InvalidateCollectionMetaCacheTask) PreExecute(ctx context.Context) error {
	return nil
}

func (t *InvalidateCollectionMetaCacheTask) Execute(ctx context.Context) error {
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

func (t *InvalidateCollectionMetaCacheTask) PostExecute(ctx context.Context) error {
	return nil
}

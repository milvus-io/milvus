package masterservice

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/util/tsoutil"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
	"go.etcd.io/etcd/clientv3"
)

//  internalpb2 -> internalpb
//  proxypb(proxy_service)
//  querypb(query_service)
//  datapb(data_service)
//  indexpb(index_service)
//  milvuspb -> servicepb
//  masterpb2 -> masterpb (master_service)

type InitParams struct {
	ProxyTimeTickChannel string
}

type Service interface {
	Init(params *InitParams) error
	Start() error
	Stop() error
	GetServiceStates() (*internalpb2.ServiceStates, error)
	GetTimeTickChannel() (string, error)
	GetStatesChannel() (string, error)
}

type Interface interface {
	//service
	Init(params *InitParams) error
	Start() error
	Stop() error
	GetServiceStates(empty *commonpb.Empty) (*internalpb2.ServiceStates, error)

	//DDL request
	CreateCollection(in *milvuspb.CreateCollectionRequest) (*commonpb.Status, error)
	DropCollection(in *milvuspb.DropCollectionRequest) (*commonpb.Status, error)
	HasCollection(in *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error)
	DescribeCollection(in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error)
	ShowCollections(in *milvuspb.ShowCollectionRequest) (*milvuspb.ShowCollectionResponse, error)
	CreatePartition(in *milvuspb.CreatePartitionRequest) (*commonpb.Status, error)
	DropPartition(in *milvuspb.DropPartitionRequest) (*commonpb.Status, error)
	HasPartition(in *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error)
	ShowPartitions(in *milvuspb.ShowPartitionRequest) (*milvuspb.ShowPartitionResponse, error)

	//index builder service
	CreateIndex(in *milvuspb.CreateIndexRequest) (*commonpb.Status, error)
	DescribeIndex(in *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error)

	//global timestamp allocator
	AllocTimestamp(in *masterpb.TsoRequest) (*masterpb.TsoResponse, error)
	AllocID(in *masterpb.IDRequest) (*masterpb.IDResponse, error)

	//TODO, master load these channel form config file ?

	//receiver time tick from proxy service, and put it into this channel
	GetTimeTickChannel(empty *commonpb.Empty) (*milvuspb.StringResponse, error)

	//receive ddl from rpc and time tick from proxy service, and put them into this channel
	GetDdChannel(empty *commonpb.Empty) (*milvuspb.StringResponse, error)

	//just define a channel, not used currently
	GetStatisticsChannel(empty *commonpb.Empty) (*milvuspb.StringResponse, error)

	//segment
	DescribeSegment(in *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error)
	ShowSegments(in *milvuspb.ShowSegmentRequest) (*milvuspb.ShowSegmentResponse, error)

	//get system config from master, not used currently
	//GetSysConfigs(in *milvuspb.SysConfigRequest)

	//GetIndexState(ctx context.Context, request *milvuspb.IndexStateRequest) (*milvuspb.IndexStateResponse, error)
}

// ------------------ struct -----------------------

// master core
type Core struct {
	//TODO DataService Interface
	//TODO IndexService Interface
	//TODO ProxyServiceClient Interface, get proxy service time tick channel,InvalidateCollectionMetaCache

	//TODO Segment States Channel, from DataService, if create new segment, data service should put the segment id into this channel, and let the master add the segment id to the collection meta

	//TODO Segment Flush Watcher, monitor if segment has flushed into disk
	//TODO indexBuilder Sch, tell index service to build index

	MetaTable *metaTable
	//id allocator
	idAllocator *GlobalIDAllocator
	//tso allocator
	tsoAllocator *GlobalTSOAllocator

	//inner members
	ctx     context.Context
	cancel  context.CancelFunc
	etcdCli *clientv3.Client
	kvBase  *etcdkv.EtcdKV
	metaKV  *etcdkv.EtcdKV

	//TODO, receive time tick from proxy service time tick channel
	ProxyTimeTickChan <-chan typeutil.Timestamp

	//TODO, send time tick into dd channel and time tick channel
	SendTimeTick func(t typeutil.Timestamp) error

	//TODO, send create collection into dd channel
	DdCreateCollectionReq func(req *internalpb2.CreateCollectionRequest) error

	//TODO, send drop collection into dd channel, and notify the proxy to delete this collection
	DdDropCollectionReq func(req *internalpb2.DropCollectionRequest) error

	//TODO, send create partition into dd channel
	DdCreatePartitionReq func(req *internalpb2.CreatePartitionRequest) error

	//TODO, send drop partition into dd channel
	DdDropPartitionReq func(req *internalpb2.DropPartitionRequest) error

	//dd request scheduler
	ddReqQueue      chan reqTask //dd request will be push into this chan
	lastDdTimeStamp typeutil.Timestamp

	//time tick loop
	lastTimeTick typeutil.Timestamp

	//states code
	stateCode atomic.Value

	//call once
	initOnce  sync.Once
	startOnce sync.Once
	isInit    atomic.Value
}

// --------------------- function --------------------------

func NewCore(c context.Context) (*Core, error) {
	ctx, cancel := context.WithCancel(c)
	rand.Seed(time.Now().UnixNano())
	Params.Init()
	core := &Core{
		ctx:    ctx,
		cancel: cancel,
	}
	core.stateCode.Store(internalpb2.StateCode_INITIALIZING)
	core.isInit.Store(false)
	return core, nil
}

func (c *Core) checkInit() error {
	if c.MetaTable == nil {
		return errors.Errorf("MetaTable is nil")
	}
	if c.idAllocator == nil {
		return errors.Errorf("idAllocator is nil")
	}
	if c.tsoAllocator == nil {
		return errors.Errorf("tsoAllocator is nil")
	}
	if c.etcdCli == nil {
		return errors.Errorf("etcdCli is nil")
	}
	if c.metaKV == nil {
		return errors.Errorf("metaKV is nil")
	}
	if c.kvBase == nil {
		return errors.Errorf("kvBase is nil")
	}
	if c.ProxyTimeTickChan == nil {
		return errors.Errorf("ProxyTimeTickChan is nil")
	}
	if c.ddReqQueue == nil {
		return errors.Errorf("ddReqQueue is nil")
	}
	if c.DdCreateCollectionReq == nil {
		return errors.Errorf("DdCreateCollectionReq is nil")
	}
	if c.DdDropCollectionReq == nil {
		return errors.Errorf("DdDropCollectionReq is nil")
	}
	if c.DdCreatePartitionReq == nil {
		return errors.Errorf("DdCreatePartitionReq is nil")
	}
	if c.DdDropPartitionReq == nil {
		return errors.Errorf("DdDropPartitionReq is nil")
	}
	log.Printf("master node id = %d\n", Params.NodeID)
	return nil
}

func (c *Core) startDdScheduler() {
	for {
		select {
		case <-c.ctx.Done():
			log.Printf("close dd scheduler, exit task execution loop")
			return
		case task, ok := <-c.ddReqQueue:
			if !ok {
				log.Printf("dd chan is closed, exit task execution loopo")
				return
			}
			ts, err := task.Ts()
			if err != nil {
				task.Notify(err)
				break
			}
			if ts <= c.lastDdTimeStamp {
				task.Notify(errors.Errorf("input timestamp = %d, last dd time stamp = %d", ts, c.lastDdTimeStamp))
				break
			}
			err = task.Execute()
			task.Notify(err)
			c.lastDdTimeStamp = ts
		}
	}
}

func (c *Core) startTimeTickLoop() {
	for {
		select {
		case <-c.ctx.Done():
			log.Printf("close master time tick loop")
			return
		case tt, ok := <-c.ProxyTimeTickChan:
			if !ok {
				log.Printf("proxyTimeTickStream is closed, exit time tick loop")
				return
			}
			if tt <= c.lastTimeTick {
				log.Printf("master time tick go back, last time tick = %d, input time tick = %d", c.lastTimeTick, tt)
			}
			if err := c.SendTimeTick(tt); err != nil {
				log.Printf("master send time tick into dd and time_tick channel failed: %s", err.Error())
			}
			c.lastTimeTick = tt
		}
	}
}

func (c *Core) Init(params *InitParams) error {
	var initError error = nil
	c.initOnce.Do(func() {
		Params.ProxyTimeTickChannel = params.ProxyTimeTickChannel
		if c.etcdCli, initError = clientv3.New(clientv3.Config{Endpoints: []string{Params.EtcdAddress}, DialTimeout: 5 * time.Second}); initError != nil {
			return
		}
		c.metaKV = etcdkv.NewEtcdKV(c.etcdCli, Params.MetaRootPath)
		if c.MetaTable, initError = NewMetaTable(c.metaKV); initError != nil {
			return
		}

		c.kvBase = etcdkv.NewEtcdKV(c.etcdCli, Params.KvRootPath)

		c.idAllocator = NewGlobalIDAllocator("idTimestamp", tsoutil.NewTSOKVBase([]string{Params.EtcdAddress}, Params.KvRootPath, "gid"))
		if initError = c.idAllocator.Initialize(); initError != nil {
			return
		}
		c.tsoAllocator = NewGlobalTSOAllocator("timestamp", tsoutil.NewTSOKVBase([]string{Params.EtcdAddress}, Params.KvRootPath, "tso"))
		if initError = c.tsoAllocator.Initialize(); initError != nil {
			return
		}
		c.ddReqQueue = make(chan reqTask, 1024)
		c.isInit.Store(true)
	})
	return initError
}

func (c *Core) Start() error {
	isInit := c.isInit.Load().(bool)
	if !isInit {
		return errors.Errorf("call init before start")
	}
	if err := c.checkInit(); err != nil {
		return err
	}
	c.startOnce.Do(func() {
		go c.startDdScheduler()
		go c.startTimeTickLoop()
		c.stateCode.Store(internalpb2.StateCode_HEALTHY)
	})
	return nil
}

func (c *Core) Stop() error {
	c.cancel()
	c.stateCode.Store(internalpb2.StateCode_ABNORMAL)
	return nil
}

func (c *Core) GetServiceStates(empty *commonpb.Empty) (*internalpb2.ServiceStates, error) {
	code := c.stateCode.Load().(internalpb2.StateCode)
	return &internalpb2.ServiceStates{
		StateCode: code,
		NodeStates: []*internalpb2.NodeStates{
			{
				NodeID:    int64(Params.NodeID),
				Role:      "master",
				StateCode: code,
				ExtraInfo: nil,
			},
		},
		ExtraInfo: nil,
	}, nil
}

func (c *Core) GetTimeTickChannel(empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
			Reason:    "",
		},
		Value: Params.TimeTickChannel,
	}, nil
}

func (c *Core) GetDdChannel(empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
			Reason:    "",
		},
		Value: Params.DdChannel,
	}, nil
}

func (c *Core) GetStatisticsChannel(empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
			Reason:    "",
		},
		Value: Params.StatisticsChannel,
	}, nil
}

func (c *Core) CreateCollection(in *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	t := &CreateCollectionReqTask{
		baseReqTask: baseReqTask{
			cv:   make(chan error),
			core: c,
		},
		Req: in,
	}
	c.ddReqQueue <- t
	err := t.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    "Create collection failed: " + err.Error(),
		}, nil
	}
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
		Reason:    "",
	}, nil

}

func (c *Core) DropCollection(in *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	t := &DropCollectionReqTask{
		baseReqTask: baseReqTask{
			cv:   make(chan error),
			core: c,
		},
		Req: in,
	}
	c.ddReqQueue <- t
	err := t.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    "Create collection failed: " + err.Error(),
		}, nil
	}
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
		Reason:    "",
	}, nil
}

func (c *Core) HasCollection(in *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	t := &HasCollectionReqTask{
		baseReqTask: baseReqTask{
			cv:   make(chan error),
			core: c,
		},
		Req:           in,
		HasCollection: false,
	}
	c.ddReqQueue <- t
	err := t.WaitToFinish()
	if err != nil {
		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    "Has collection failed: " + err.Error(),
			},
			Value: false,
		}, nil
	}
	return &milvuspb.BoolResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
			Reason:    "",
		},
		Value: t.HasCollection,
	}, nil
}

func (c *Core) DescribeCollection(in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	t := &DescribeCollectionReqTask{
		baseReqTask: baseReqTask{
			cv:   make(chan error),
			core: c,
		},
		Req: in,
		Rsp: &milvuspb.DescribeCollectionResponse{},
	}
	c.ddReqQueue <- t
	err := t.WaitToFinish()
	if err != nil {
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    "describe collection failed: " + err.Error(),
			},
			Schema: nil,
		}, nil
	}
	t.Rsp.Status = &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
		Reason:    "",
	}
	return t.Rsp, nil
}

func (c *Core) ShowCollections(in *milvuspb.ShowCollectionRequest) (*milvuspb.ShowCollectionResponse, error) {
	t := &ShowCollectionReqTask{
		baseReqTask: baseReqTask{
			cv:   make(chan error),
			core: c,
		},
		Req: in,
		Rsp: &milvuspb.ShowCollectionResponse{
			CollectionNames: nil,
		},
	}
	c.ddReqQueue <- t
	err := t.WaitToFinish()
	if err != nil {
		return &milvuspb.ShowCollectionResponse{
			CollectionNames: nil,
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    "ShowCollections failed: " + err.Error(),
			},
		}, nil
	}
	t.Rsp.Status = &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
		Reason:    "",
	}
	return t.Rsp, nil
}

func (c *Core) CreatePartition(in *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	t := &CreatePartitionReqTask{
		baseReqTask: baseReqTask{
			cv:   make(chan error),
			core: c,
		},
		Req: in,
	}
	c.ddReqQueue <- t
	err := t.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    "create partition failed: " + err.Error(),
		}, nil
	}
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
		Reason:    "",
	}, nil
}

func (c *Core) DropPartition(in *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	t := &DropPartitionReqTask{
		baseReqTask: baseReqTask{
			cv:   make(chan error),
			core: c,
		},
		Req: in,
	}
	c.ddReqQueue <- t
	err := t.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    "DropPartition failed: " + err.Error(),
		}, nil
	}
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
		Reason:    "",
	}, nil
}

func (c *Core) HasPartition(in *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	t := &HasPartitionReqTask{
		baseReqTask: baseReqTask{
			cv:   make(chan error),
			core: c,
		},
		Req:          in,
		HasPartition: false,
	}
	c.ddReqQueue <- t
	err := t.WaitToFinish()
	if err != nil {
		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    "HasPartition failed: " + err.Error(),
			},
			Value: false,
		}, nil
	}
	return &milvuspb.BoolResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
			Reason:    "",
		},
		Value: t.HasPartition,
	}, nil
}

func (c *Core) ShowPartitions(in *milvuspb.ShowPartitionRequest) (*milvuspb.ShowPartitionResponse, error) {
	t := &ShowPartitionReqTask{
		baseReqTask: baseReqTask{
			cv:   make(chan error),
			core: c,
		},
		Req: in,
		Rsp: &milvuspb.ShowPartitionResponse{
			PartitionNames: nil,
			Status:         nil,
		},
	}
	c.ddReqQueue <- t
	err := t.WaitToFinish()
	if err != nil {
		return &milvuspb.ShowPartitionResponse{
			PartitionNames: nil,
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    "ShowPartitions failed: " + err.Error(),
			},
		}, nil
	}
	t.Rsp.Status = &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
		Reason:    "",
	}
	return t.Rsp, nil
}

//TODO
func (c *Core) CreateIndex(in *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	return nil, nil
}

//TODO
func (c *Core) DescribeIndex(in *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	return nil, nil
}

//TODO
func (c *Core) DescribeSegment(in *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error) {
	return nil, nil
}

//TODO
func (c *Core) ShowSegments(in *milvuspb.ShowSegmentRequest) (*milvuspb.ShowSegmentResponse, error) {
	return nil, nil
}

func (c *Core) AllocTimestamp(in *masterpb.TsoRequest) (*masterpb.TsoResponse, error) {
	ts, err := c.tsoAllocator.Alloc(in.Count)
	if err != nil {
		return &masterpb.TsoResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    "AllocTimestamp failed: " + err.Error(),
			},
			Timestamp: 0,
			Count:     0,
		}, nil
	}
	return &masterpb.TsoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
			Reason:    "",
		},
		Timestamp: ts,
		Count:     in.Count,
	}, nil
}

func (c *Core) AllocID(in *masterpb.IDRequest) (*masterpb.IDResponse, error) {
	start, _, err := c.idAllocator.Alloc(in.Count)
	if err != nil {
		return &masterpb.IDResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    "AllocID failed: " + err.Error(),
			},
			ID:    0,
			Count: in.Count,
		}, nil
	}
	return &masterpb.IDResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
			Reason:    "",
		},
		ID:    start,
		Count: in.Count,
	}, nil
}

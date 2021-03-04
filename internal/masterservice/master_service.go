package masterservice

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/allocator"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	"github.com/zilliztech/milvus-distributed/internal/log"
	ms "github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/proxypb"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
	"github.com/zilliztech/milvus-distributed/internal/tso"
	"github.com/zilliztech/milvus-distributed/internal/util/retry"
	"github.com/zilliztech/milvus-distributed/internal/util/tsoutil"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

//  internalpb2 -> internalpb
//  proxypb(proxy_service)
//  querypb(query_service)
//  datapb(data_service)
//  indexpb(index_service)
//  milvuspb -> milvuspb
//  masterpb2 -> masterpb (master_service)

type ProxyServiceInterface interface {
	GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error)
	InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error)
}

type DataServiceInterface interface {
	GetInsertBinlogPaths(ctx context.Context, req *datapb.InsertBinlogPathRequest) (*datapb.InsertBinlogPathsResponse, error)
	GetSegmentInfoChannel(ctx context.Context) (*milvuspb.StringResponse, error)
}

type IndexServiceInterface interface {
	BuildIndex(ctx context.Context, req *indexpb.BuildIndexRequest) (*indexpb.BuildIndexResponse, error)
	DropIndex(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error)
}

type QueryServiceInterface interface {
	ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error)
}

type Interface interface {
	//service
	Init() error
	Start() error
	Stop() error
	GetComponentStates(ctx context.Context) (*internalpb2.ComponentStates, error)

	//DDL request
	CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest) (*commonpb.Status, error)
	DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest) (*commonpb.Status, error)
	HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error)
	DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error)
	ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionRequest) (*milvuspb.ShowCollectionResponse, error)
	CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest) (*commonpb.Status, error)
	DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest) (*commonpb.Status, error)
	HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error)
	ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionRequest) (*milvuspb.ShowPartitionResponse, error)

	//index builder service
	CreateIndex(ctx context.Context, in *milvuspb.CreateIndexRequest) (*commonpb.Status, error)
	DescribeIndex(ctx context.Context, in *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error)
	DropIndex(ctx context.Context, in *milvuspb.DropIndexRequest) (*commonpb.Status, error)

	//global timestamp allocator
	AllocTimestamp(ctx context.Context, in *masterpb.TsoRequest) (*masterpb.TsoResponse, error)
	AllocID(ctx context.Context, in *masterpb.IDRequest) (*masterpb.IDResponse, error)

	//TODO, master load these channel form config file ?

	//receiver time tick from proxy service, and put it into this channel
	GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error)

	//receive ddl from rpc and time tick from proxy service, and put them into this channel
	GetDdChannel(ctx context.Context) (*milvuspb.StringResponse, error)

	//just define a channel, not used currently
	GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error)

	//segment
	DescribeSegment(ctx context.Context, in *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error)
	ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentRequest) (*milvuspb.ShowSegmentResponse, error)
}

// ------------------ struct -----------------------

// master core
type Core struct {
	/*
		ProxyServiceClient Interface:
		get proxy service time tick channel,InvalidateCollectionMetaCache

		DataService Interface:
		Segment States Channel, from DataService, if create new segment, data service should put the segment id into this channel, and let the master add the segment id to the collection meta
		Segment Flush Watcher, monitor if segment has flushed into disk

		IndexService Interface
		IndexService Sch, tell index service to build index
	*/

	MetaTable *metaTable
	//id allocator
	idAllocator *allocator.GlobalIDAllocator
	//tso allocator
	tsoAllocator *tso.GlobalTSOAllocator

	//inner members
	ctx     context.Context
	cancel  context.CancelFunc
	etcdCli *clientv3.Client
	kvBase  *etcdkv.EtcdKV
	metaKV  *etcdkv.EtcdKV

	//setMsgStreams, receive time tick from proxy service time tick channel
	ProxyTimeTickChan chan typeutil.Timestamp

	//setMsgStreams, send time tick into dd channel and time tick channel
	SendTimeTick func(t typeutil.Timestamp) error

	//setMsgStreams, send create collection into dd channel
	DdCreateCollectionReq func(req *internalpb2.CreateCollectionRequest) error

	//setMsgStreams, send drop collection into dd channel, and notify the proxy to delete this collection
	DdDropCollectionReq func(req *internalpb2.DropCollectionRequest) error

	//setMsgStreams, send create partition into dd channel
	DdCreatePartitionReq func(req *internalpb2.CreatePartitionRequest) error

	//setMsgStreams, send drop partition into dd channel
	DdDropPartitionReq func(req *internalpb2.DropPartitionRequest) error

	//setMsgStreams segment channel, receive segment info from data service, if master create segment
	DataServiceSegmentChan chan *datapb.SegmentInfo

	//setMsgStreams ,if segment flush completed, data node would put segment id into msg stream
	DataNodeSegmentFlushCompletedChan chan typeutil.UniqueID

	//get binlog file path from data service,
	GetBinlogFilePathsFromDataServiceReq func(segID typeutil.UniqueID, fieldID typeutil.UniqueID) ([]string, error)

	//call index builder's client to build index, return build id
	BuildIndexReq func(binlog []string, typeParams []*commonpb.KeyValuePair, indexParams []*commonpb.KeyValuePair, indexID typeutil.UniqueID, indexName string) (typeutil.UniqueID, error)
	DropIndexReq  func(indexID typeutil.UniqueID) error

	//proxy service interface, notify proxy service to drop collection
	InvalidateCollectionMetaCache func(ts typeutil.Timestamp, dbName string, collectionName string) error

	//query service interface, notify query service to release collection
	ReleaseCollection func(ts typeutil.Timestamp, dbID typeutil.UniqueID, collectionID typeutil.UniqueID) error

	// put create index task into this chan
	indexTaskQueue chan *CreateIndexTask

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
	//isInit    atomic.Value

	msFactory ms.Factory
}

// --------------------- function --------------------------

func NewCore(c context.Context, factory ms.Factory) (*Core, error) {
	ctx, cancel := context.WithCancel(c)
	rand.Seed(time.Now().UnixNano())
	core := &Core{
		ctx:       ctx,
		cancel:    cancel,
		msFactory: factory,
	}
	core.UpdateStateCode(internalpb2.StateCode_ABNORMAL)
	return core, nil
}

func (c *Core) UpdateStateCode(code internalpb2.StateCode) {
	c.stateCode.Store(code)
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
	if c.DataServiceSegmentChan == nil {
		return errors.Errorf("DataServiceSegmentChan is nil")
	}
	if c.GetBinlogFilePathsFromDataServiceReq == nil {
		return errors.Errorf("GetBinlogFilePathsFromDataServiceReq is nil")
	}
	if c.BuildIndexReq == nil {
		return errors.Errorf("BuildIndexReq is nil")
	}
	if c.DropIndexReq == nil {
		return errors.Errorf("DropIndexReq is nil")
	}
	if c.InvalidateCollectionMetaCache == nil {
		return errors.Errorf("InvalidateCollectionMetaCache is nil")
	}
	if c.indexTaskQueue == nil {
		return errors.Errorf("indexTaskQueue is nil")
	}
	if c.DataNodeSegmentFlushCompletedChan == nil {
		return errors.Errorf("DataNodeSegmentFlushCompletedChan is nil")
	}
	if c.ReleaseCollection == nil {
		return errors.Errorf("ReleaseCollection is nil")
	}

	log.Debug("master", zap.Int64("node id", int64(Params.NodeID)))
	log.Debug("master", zap.String("dd channel name", Params.DdChannel))
	log.Debug("master", zap.String("time tick channel name", Params.TimeTickChannel))
	return nil
}

func (c *Core) startDdScheduler() {
	for {
		select {
		case <-c.ctx.Done():
			log.Debug("close dd scheduler, exit task execution loop")
			return
		case task, ok := <-c.ddReqQueue:
			if !ok {
				log.Debug("dd chan is closed, exit task execution loop")
				return
			}
			ts, err := task.Ts()
			if err != nil {
				task.Notify(err)
				break
			}
			if !task.IgnoreTimeStamp() && ts <= c.lastDdTimeStamp {
				task.Notify(errors.Errorf("input timestamp = %d, last dd time stamp = %d", ts, c.lastDdTimeStamp))
				break
			}
			err = task.Execute()
			task.Notify(err)
			if ts > c.lastDdTimeStamp {
				c.lastDdTimeStamp = ts
			}
		}
	}
}

func (c *Core) startTimeTickLoop() {
	for {
		select {
		case <-c.ctx.Done():
			log.Debug("close master time tick loop")
			return
		case tt, ok := <-c.ProxyTimeTickChan:
			if !ok {
				log.Warn("proxyTimeTickStream is closed, exit time tick loop")
				return
			}
			if tt <= c.lastTimeTick {
				log.Warn("master time tick go back", zap.Uint64("last time tick", c.lastTimeTick), zap.Uint64("input time tick ", tt))
			}
			if err := c.SendTimeTick(tt); err != nil {
				log.Warn("master send time tick into dd and time_tick channel failed", zap.String("error", err.Error()))
			}
			c.lastTimeTick = tt
		}
	}
}

//data service send segment info to master when create segment
func (c *Core) startDataServiceSegmentLoop() {
	for {
		select {
		case <-c.ctx.Done():
			log.Debug("close data service segment loop")
			return
		case seg, ok := <-c.DataServiceSegmentChan:
			if !ok {
				log.Debug("data service segment is closed, exit loop")
				return
			}
			if seg == nil {
				log.Warn("segment from data service is nil")
			} else if err := c.MetaTable.AddSegment(seg); err != nil {
				//what if master add segment failed, but data service success?
				log.Warn("add segment info meta table failed ", zap.String("error", err.Error()))
			} else {
				log.Debug("add segment", zap.Int64("collection id", seg.CollectionID), zap.Int64("partition id", seg.PartitionID), zap.Int64("segment id", seg.SegmentID))
			}
		}
	}
}

//create index loop
func (c *Core) startCreateIndexLoop() {
	for {
		select {
		case <-c.ctx.Done():
			log.Debug("close create index loop")
			return
		case t, ok := <-c.indexTaskQueue:
			if !ok {
				log.Debug("index task chan has closed, exit loop")
				return
			}
			if err := t.BuildIndex(); err != nil {
				log.Warn("create index failed", zap.String("error", err.Error()))
			} else {
				log.Debug("create index", zap.String("index name", t.indexName), zap.String("field name", t.fieldSchema.Name), zap.Int64("segment id", t.segmentID))
			}
		}
	}
}

func (c *Core) startSegmentFlushCompletedLoop() {
	for {
		select {
		case <-c.ctx.Done():
			log.Debug("close segment flush completed loop")
			return
		case seg, ok := <-c.DataNodeSegmentFlushCompletedChan:
			if !ok {
				log.Debug("data node segment flush completed chan has colsed, exit loop")
			}
			coll, err := c.MetaTable.GetCollectionBySegmentID(seg)
			if err != nil {
				log.Warn("GetCollectionBySegmentID", zap.String("error", err.Error()))
				break
			}
			for _, f := range coll.FieldIndexes {
				idxInfo, err := c.MetaTable.GetIndexByID(f.IndexID)
				if err != nil {
					log.Warn("index not found", zap.Int64("index id", f.IndexID))
					continue
				}

				fieldSch, err := GetFieldSchemaByID(coll, f.FiledID)
				if err == nil {
					t := &CreateIndexTask{
						core:        c,
						segmentID:   seg,
						indexName:   idxInfo.IndexName,
						indexID:     idxInfo.IndexID,
						fieldSchema: fieldSch,
						indexParams: nil,
					}
					c.indexTaskQueue <- t
				}
			}
		}
	}
}

func (c *Core) tsLoop() {
	tsoTicker := time.NewTicker(tso.UpdateTimestampStep)
	defer tsoTicker.Stop()
	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	for {
		select {
		case <-tsoTicker.C:
			if err := c.tsoAllocator.UpdateTSO(); err != nil {
				log.Warn("failed to update timestamp", zap.String("error", err.Error()))
				return
			}
			if err := c.idAllocator.UpdateID(); err != nil {
				log.Warn("failed to update id", zap.String("error", err.Error()))
				return
			}
		case <-ctx.Done():
			// Server is closed and it should return nil.
			log.Debug("tsLoop is closed")
			return
		}
	}
}
func (c *Core) setMsgStreams() error {
	if Params.PulsarAddress == "" {
		return errors.Errorf("PulsarAddress is empty")
	}
	if Params.MsgChannelSubName == "" {
		return errors.Errorf("MsgChannelSubName is emptyr")
	}

	//proxy time tick stream,
	if Params.ProxyTimeTickChannel == "" {
		return errors.Errorf("ProxyTimeTickChannel is empty")
	}

	var err error
	m := map[string]interface{}{
		"PulsarAddress":  Params.PulsarAddress,
		"ReceiveBufSize": 1024,
		"PulsarBufSize":  1024}
	err = c.msFactory.SetParams(m)
	if err != nil {
		return err
	}

	proxyTimeTickStream, _ := c.msFactory.NewMsgStream(c.ctx)
	proxyTimeTickStream.AsConsumer([]string{Params.ProxyTimeTickChannel}, Params.MsgChannelSubName)
	proxyTimeTickStream.Start()

	// master time tick channel
	if Params.TimeTickChannel == "" {
		return errors.Errorf("TimeTickChannel is empty")
	}
	timeTickStream, _ := c.msFactory.NewMsgStream(c.ctx)
	timeTickStream.AsProducer([]string{Params.TimeTickChannel})

	// master dd channel
	if Params.DdChannel == "" {
		return errors.Errorf("DdChannel is empty")
	}
	ddStream, _ := c.msFactory.NewMsgStream(c.ctx)
	ddStream.AsProducer([]string{Params.DdChannel})

	c.SendTimeTick = func(t typeutil.Timestamp) error {
		msgPack := ms.MsgPack{}
		baseMsg := ms.BaseMsg{
			BeginTimestamp: t,
			EndTimestamp:   t,
			HashValues:     []uint32{0},
		}
		timeTickResult := internalpb2.TimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kTimeTick,
				MsgID:     0,
				Timestamp: t,
				SourceID:  int64(Params.NodeID),
			},
		}
		timeTickMsg := &ms.TimeTickMsg{
			BaseMsg:     baseMsg,
			TimeTickMsg: timeTickResult,
		}
		msgPack.Msgs = append(msgPack.Msgs, timeTickMsg)
		if err := timeTickStream.Broadcast(c.ctx, &msgPack); err != nil {
			return err
		}
		if err := ddStream.Broadcast(c.ctx, &msgPack); err != nil {
			return err
		}
		return nil
	}

	c.DdCreateCollectionReq = func(req *internalpb2.CreateCollectionRequest) error {
		msgPack := ms.MsgPack{}
		baseMsg := ms.BaseMsg{
			BeginTimestamp: req.Base.Timestamp,
			EndTimestamp:   req.Base.Timestamp,
			HashValues:     []uint32{0},
		}
		collMsg := &ms.CreateCollectionMsg{
			BaseMsg:                 baseMsg,
			CreateCollectionRequest: *req,
		}
		msgPack.Msgs = append(msgPack.Msgs, collMsg)
		if err := ddStream.Broadcast(c.ctx, &msgPack); err != nil {
			return err
		}
		return nil
	}

	c.DdDropCollectionReq = func(req *internalpb2.DropCollectionRequest) error {
		msgPack := ms.MsgPack{}
		baseMsg := ms.BaseMsg{
			BeginTimestamp: req.Base.Timestamp,
			EndTimestamp:   req.Base.Timestamp,
			HashValues:     []uint32{0},
		}
		collMsg := &ms.DropCollectionMsg{
			BaseMsg:               baseMsg,
			DropCollectionRequest: *req,
		}
		msgPack.Msgs = append(msgPack.Msgs, collMsg)
		if err := ddStream.Broadcast(c.ctx, &msgPack); err != nil {
			return err
		}
		return nil
	}

	c.DdCreatePartitionReq = func(req *internalpb2.CreatePartitionRequest) error {
		msgPack := ms.MsgPack{}
		baseMsg := ms.BaseMsg{
			BeginTimestamp: req.Base.Timestamp,
			EndTimestamp:   req.Base.Timestamp,
			HashValues:     []uint32{0},
		}
		collMsg := &ms.CreatePartitionMsg{
			BaseMsg:                baseMsg,
			CreatePartitionRequest: *req,
		}
		msgPack.Msgs = append(msgPack.Msgs, collMsg)
		if err := ddStream.Broadcast(c.ctx, &msgPack); err != nil {
			return err
		}
		return nil
	}

	c.DdDropPartitionReq = func(req *internalpb2.DropPartitionRequest) error {
		msgPack := ms.MsgPack{}
		baseMsg := ms.BaseMsg{
			BeginTimestamp: req.Base.Timestamp,
			EndTimestamp:   req.Base.Timestamp,
			HashValues:     []uint32{0},
		}
		collMsg := &ms.DropPartitionMsg{
			BaseMsg:              baseMsg,
			DropPartitionRequest: *req,
		}
		msgPack.Msgs = append(msgPack.Msgs, collMsg)
		if err := ddStream.Broadcast(c.ctx, &msgPack); err != nil {
			return err
		}
		return nil
	}

	// receive time tick from msg stream
	c.ProxyTimeTickChan = make(chan typeutil.Timestamp, 1024)
	go func() {
		for {
			select {
			case <-c.ctx.Done():
				return
			case ttmsgs, ok := <-proxyTimeTickStream.Chan():
				if !ok {
					log.Warn("proxy time tick msg stream closed")
					return
				}
				if len(ttmsgs.Msgs) > 0 {
					for _, ttm := range ttmsgs.Msgs {
						ttmsg, ok := ttm.(*ms.TimeTickMsg)
						if !ok {
							continue
						}
						c.ProxyTimeTickChan <- ttmsg.Base.Timestamp
					}
				}
			}
		}
	}()

	//segment channel, data service create segment,or data node flush segment will put msg in this channel
	if Params.DataServiceSegmentChannel == "" {
		return errors.Errorf("DataServiceSegmentChannel is empty")
	}
	dataServiceStream, _ := c.msFactory.NewMsgStream(c.ctx)
	dataServiceStream.AsConsumer([]string{Params.DataServiceSegmentChannel}, Params.MsgChannelSubName)
	dataServiceStream.Start()
	c.DataServiceSegmentChan = make(chan *datapb.SegmentInfo, 1024)
	c.DataNodeSegmentFlushCompletedChan = make(chan typeutil.UniqueID, 1024)

	// receive segment info from msg stream
	go func() {
		for {
			select {
			case <-c.ctx.Done():
				return
			case segMsg, ok := <-dataServiceStream.Chan():
				if !ok {
					log.Warn("data service segment msg closed")
				}
				if len(segMsg.Msgs) > 0 {
					for _, segm := range segMsg.Msgs {
						segInfoMsg, ok := segm.(*ms.SegmentInfoMsg)
						if ok {
							c.DataServiceSegmentChan <- segInfoMsg.Segment
						} else {
							flushMsg, ok := segm.(*ms.FlushCompletedMsg)
							if ok {
								c.DataNodeSegmentFlushCompletedChan <- flushMsg.SegmentFlushCompletedMsg.SegmentID
							} else {
								log.Debug("receive unexpected msg from data service stream", zap.Stringer("segment", segInfoMsg.SegmentMsg.Segment))
							}
						}
					}
				}
			}
		}
	}()

	return nil
}

func (c *Core) SetProxyService(ctx context.Context, s ProxyServiceInterface) error {
	rsp, err := s.GetTimeTickChannel(ctx)
	if err != nil {
		return err
	}
	Params.ProxyTimeTickChannel = rsp.Value
	log.Debug("proxy time tick", zap.String("channel name", Params.ProxyTimeTickChannel))

	c.InvalidateCollectionMetaCache = func(ts typeutil.Timestamp, dbName string, collectionName string) error {
		status, _ := s.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0, //TODO,MsgType
				MsgID:     0,
				Timestamp: ts,
				SourceID:  int64(Params.NodeID),
			},
			DbName:         dbName,
			CollectionName: collectionName,
		})
		if status == nil {
			return errors.New("invalidate collection metacache resp is nil")
		}
		if status.ErrorCode != commonpb.ErrorCode_SUCCESS {
			return errors.New(status.Reason)
		}
		return nil
	}
	return nil
}

func (c *Core) SetDataService(ctx context.Context, s DataServiceInterface) error {
	rsp, err := s.GetSegmentInfoChannel(ctx)
	if err != nil {
		return err
	}
	Params.DataServiceSegmentChannel = rsp.Value
	log.Debug("data service segment", zap.String("channel name", Params.DataServiceSegmentChannel))

	c.GetBinlogFilePathsFromDataServiceReq = func(segID typeutil.UniqueID, fieldID typeutil.UniqueID) ([]string, error) {
		ts, err := c.tsoAllocator.Alloc(1)
		if err != nil {
			return nil, err
		}
		binlog, err := s.GetInsertBinlogPaths(ctx, &datapb.InsertBinlogPathRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0, //TODO, msy type
				MsgID:     0,
				Timestamp: ts,
				SourceID:  int64(Params.NodeID),
			},
			SegmentID: segID,
		})
		if err != nil {
			return nil, err
		}
		if binlog.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
			return nil, errors.Errorf("GetInsertBinlogPaths from data service failed, error = %s", binlog.Status.Reason)
		}
		for i := range binlog.FieldIDs {
			if binlog.FieldIDs[i] == fieldID {
				return binlog.Paths[i].Values, nil
			}
		}
		return nil, errors.Errorf("binlog file not exist, segment id = %d, field id = %d", segID, fieldID)
	}
	return nil
}

func (c *Core) SetIndexService(ctx context.Context, s IndexServiceInterface) error {
	c.BuildIndexReq = func(binlog []string, typeParams []*commonpb.KeyValuePair, indexParams []*commonpb.KeyValuePair, indexID typeutil.UniqueID, indexName string) (typeutil.UniqueID, error) {
		rsp, err := s.BuildIndex(ctx, &indexpb.BuildIndexRequest{
			DataPaths:   binlog,
			TypeParams:  typeParams,
			IndexParams: indexParams,
			IndexID:     indexID,
			IndexName:   indexName,
		})
		if err != nil {
			return 0, err
		}
		if rsp.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
			return 0, errors.Errorf("BuildIndex from index service failed, error = %s", rsp.Status.Reason)
		}
		return rsp.IndexBuildID, nil
	}

	c.DropIndexReq = func(indexID typeutil.UniqueID) error {
		rsp, err := s.DropIndex(ctx, &indexpb.DropIndexRequest{
			IndexID: indexID,
		})
		if err != nil {
			return err
		}
		if rsp.ErrorCode != commonpb.ErrorCode_SUCCESS {
			return errors.Errorf("%s", rsp.Reason)
		}
		return nil
	}

	return nil
}

func (c *Core) SetQueryService(ctx context.Context, s QueryServiceInterface) error {
	c.ReleaseCollection = func(ts typeutil.Timestamp, dbID typeutil.UniqueID, collectionID typeutil.UniqueID) error {
		req := &querypb.ReleaseCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kReleaseCollection,
				MsgID:     0, //TODO, msg ID
				Timestamp: ts,
				SourceID:  int64(Params.NodeID),
			},
			DbID:         dbID,
			CollectionID: collectionID,
		}
		rsp, err := s.ReleaseCollection(ctx, req)
		if err != nil {
			return err
		}
		if rsp.ErrorCode != commonpb.ErrorCode_SUCCESS {
			return errors.Errorf("ReleaseCollection from query service failed, error = %s", rsp.Reason)
		}
		return nil
	}
	return nil
}

func (c *Core) Init() error {
	var initError error = nil
	c.initOnce.Do(func() {
		connectEtcdFn := func() error {
			if c.etcdCli, initError = clientv3.New(clientv3.Config{Endpoints: []string{Params.EtcdAddress}, DialTimeout: 5 * time.Second}); initError != nil {
				return initError
			}
			c.metaKV = etcdkv.NewEtcdKV(c.etcdCli, Params.MetaRootPath)
			if c.MetaTable, initError = NewMetaTable(c.metaKV); initError != nil {
				return initError
			}
			c.kvBase = etcdkv.NewEtcdKV(c.etcdCli, Params.KvRootPath)
			return nil
		}
		err := retry.Retry(200, time.Millisecond*200, connectEtcdFn)
		if err != nil {
			return
		}

		c.idAllocator = allocator.NewGlobalIDAllocator("idTimestamp", tsoutil.NewTSOKVBase([]string{Params.EtcdAddress}, Params.KvRootPath, "gid"))
		if initError = c.idAllocator.Initialize(); initError != nil {
			return
		}
		c.tsoAllocator = tso.NewGlobalTSOAllocator("timestamp", tsoutil.NewTSOKVBase([]string{Params.EtcdAddress}, Params.KvRootPath, "tso"))
		if initError = c.tsoAllocator.Initialize(); initError != nil {
			return
		}
		c.ddReqQueue = make(chan reqTask, 1024)
		c.indexTaskQueue = make(chan *CreateIndexTask, 1024)
		initError = c.setMsgStreams()
	})
	if initError == nil {
		log.Debug("Master service", zap.String("State Code", internalpb2.StateCode_name[int32(internalpb2.StateCode_INITIALIZING)]))
	}
	return initError
}

func (c *Core) Start() error {
	if err := c.checkInit(); err != nil {
		return err
	}
	c.startOnce.Do(func() {
		go c.startDdScheduler()
		go c.startTimeTickLoop()
		go c.startDataServiceSegmentLoop()
		go c.startCreateIndexLoop()
		go c.startSegmentFlushCompletedLoop()
		go c.tsLoop()
		c.stateCode.Store(internalpb2.StateCode_HEALTHY)
	})
	log.Debug("Master service", zap.String("State Code", internalpb2.StateCode_name[int32(internalpb2.StateCode_HEALTHY)]))
	return nil
}

func (c *Core) Stop() error {
	c.cancel()
	c.stateCode.Store(internalpb2.StateCode_ABNORMAL)
	return nil
}

func (c *Core) GetComponentStates(ctx context.Context) (*internalpb2.ComponentStates, error) {
	code := c.stateCode.Load().(internalpb2.StateCode)
	log.Debug("GetComponentStates", zap.String("State Code", internalpb2.StateCode_name[int32(code)]))

	return &internalpb2.ComponentStates{
		State: &internalpb2.ComponentInfo{
			NodeID:    int64(Params.NodeID),
			Role:      typeutil.MasterServiceRole,
			StateCode: code,
			ExtraInfo: nil,
		},
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
			Reason:    "",
		},
		SubcomponentStates: []*internalpb2.ComponentInfo{
			{
				NodeID:    int64(Params.NodeID),
				Role:      typeutil.MasterServiceRole,
				StateCode: code,
				ExtraInfo: nil,
			},
		},
	}, nil
}

func (c *Core) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
			Reason:    "",
		},
		Value: Params.TimeTickChannel,
	}, nil
}

func (c *Core) GetDdChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
			Reason:    "",
		},
		Value: Params.DdChannel,
	}, nil
}

func (c *Core) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
			Reason:    "",
		},
		Value: Params.StatisticsChannel,
	}, nil
}

func (c *Core) CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	code := c.stateCode.Load().(internalpb2.StateCode)
	if code != internalpb2.StateCode_HEALTHY {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    fmt.Sprintf("state code = %s", internalpb2.StateCode_name[int32(code)]),
		}, nil
	}
	log.Debug("CreateCollection ", zap.String("name", in.CollectionName))
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
		log.Debug("CreateCollection failed", zap.String("name", in.CollectionName))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    "Create collection failed: " + err.Error(),
		}, nil
	}
	log.Debug("CreateCollection Success", zap.String("name", in.CollectionName))
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
		Reason:    "",
	}, nil
}

func (c *Core) DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	code := c.stateCode.Load().(internalpb2.StateCode)
	if code != internalpb2.StateCode_HEALTHY {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    fmt.Sprintf("state code = %s", internalpb2.StateCode_name[int32(code)]),
		}, nil
	}
	log.Debug("DropCollection", zap.String("name", in.CollectionName))
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
		log.Debug("DropCollection Failed", zap.String("name", in.CollectionName))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    "Drop collection failed: " + err.Error(),
		}, nil
	}
	log.Debug("DropCollection Success", zap.String("name", in.CollectionName))
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
		Reason:    "",
	}, nil
}

func (c *Core) HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	code := c.stateCode.Load().(internalpb2.StateCode)
	if code != internalpb2.StateCode_HEALTHY {
		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    fmt.Sprintf("state code = %s", internalpb2.StateCode_name[int32(code)]),
			},
			Value: false,
		}, nil
	}
	log.Debug("HasCollection", zap.String("name", in.CollectionName))
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
		log.Debug("HasCollection Failed", zap.String("name", in.CollectionName))
		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    "Has collection failed: " + err.Error(),
			},
			Value: false,
		}, nil
	}
	log.Debug("HasCollection Success", zap.String("name", in.CollectionName))
	return &milvuspb.BoolResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
			Reason:    "",
		},
		Value: t.HasCollection,
	}, nil
}

func (c *Core) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	code := c.stateCode.Load().(internalpb2.StateCode)
	if code != internalpb2.StateCode_HEALTHY {
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    fmt.Sprintf("state code = %s", internalpb2.StateCode_name[int32(code)]),
			},
			Schema:       nil,
			CollectionID: 0,
		}, nil
	}
	log.Debug("DescribeCollection", zap.String("name", in.CollectionName))
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
		log.Debug("DescribeCollection Failed", zap.String("name", in.CollectionName))
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    "describe collection failed: " + err.Error(),
			},
			Schema: nil,
		}, nil
	}
	log.Debug("DescribeCollection Success", zap.String("name", in.CollectionName))
	t.Rsp.Status = &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
		Reason:    "",
	}
	return t.Rsp, nil
}

func (c *Core) ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionRequest) (*milvuspb.ShowCollectionResponse, error) {
	code := c.stateCode.Load().(internalpb2.StateCode)
	if code != internalpb2.StateCode_HEALTHY {
		return &milvuspb.ShowCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    fmt.Sprintf("state code = %s", internalpb2.StateCode_name[int32(code)]),
			},
			CollectionNames: nil,
		}, nil
	}
	log.Debug("ShowCollections", zap.String("dbname", in.DbName))
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
		log.Debug("ShowCollections failed", zap.String("dbname", in.DbName))
		return &milvuspb.ShowCollectionResponse{
			CollectionNames: nil,
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    "ShowCollections failed: " + err.Error(),
			},
		}, nil
	}
	log.Debug("ShowCollections Success", zap.String("dbname", in.DbName), zap.Strings("collection names", t.Rsp.CollectionNames))
	t.Rsp.Status = &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
		Reason:    "",
	}
	return t.Rsp, nil
}

func (c *Core) CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	code := c.stateCode.Load().(internalpb2.StateCode)
	if code != internalpb2.StateCode_HEALTHY {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    fmt.Sprintf("state code = %s", internalpb2.StateCode_name[int32(code)]),
		}, nil
	}
	log.Debug("CreatePartition", zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName))
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
		log.Debug("CreatePartition Failed", zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    "create partition failed: " + err.Error(),
		}, nil
	}
	log.Debug("CreatePartition Success", zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName))
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
		Reason:    "",
	}, nil
}

func (c *Core) DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	code := c.stateCode.Load().(internalpb2.StateCode)
	if code != internalpb2.StateCode_HEALTHY {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    fmt.Sprintf("state code = %s", internalpb2.StateCode_name[int32(code)]),
		}, nil
	}
	log.Debug("DropPartition", zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName))
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
		log.Debug("DropPartition Failed", zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    "DropPartition failed: " + err.Error(),
		}, nil
	}
	log.Debug("DropPartition Success", zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName))
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
		Reason:    "",
	}, nil
}

func (c *Core) HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	code := c.stateCode.Load().(internalpb2.StateCode)
	if code != internalpb2.StateCode_HEALTHY {
		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    fmt.Sprintf("state code = %s", internalpb2.StateCode_name[int32(code)]),
			},
			Value: false,
		}, nil
	}
	log.Debug("HasPartition", zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName))
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
		log.Debug("HasPartition Failed", zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName))
		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    "HasPartition failed: " + err.Error(),
			},
			Value: false,
		}, nil
	}
	log.Debug("HasPartition Success", zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName))
	return &milvuspb.BoolResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
			Reason:    "",
		},
		Value: t.HasPartition,
	}, nil
}

func (c *Core) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionRequest) (*milvuspb.ShowPartitionResponse, error) {
	code := c.stateCode.Load().(internalpb2.StateCode)
	if code != internalpb2.StateCode_HEALTHY {
		return &milvuspb.ShowPartitionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    fmt.Sprintf("state code = %s", internalpb2.StateCode_name[int32(code)]),
			},
			PartitionNames: nil,
			PartitionIDs:   nil,
		}, nil
	}
	log.Debug("ShowPartitions", zap.String("collection name", in.CollectionName))
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
	log.Debug("ShowPartitions Success", zap.String("collection name", in.CollectionName), zap.Strings("partition names", t.Rsp.PartitionNames), zap.Int64s("partition ids", t.Rsp.PartitionIDs))
	t.Rsp.Status = &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
		Reason:    "",
	}
	return t.Rsp, nil
}

func (c *Core) CreateIndex(ctx context.Context, in *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	code := c.stateCode.Load().(internalpb2.StateCode)
	if code != internalpb2.StateCode_HEALTHY {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    fmt.Sprintf("state code = %s", internalpb2.StateCode_name[int32(code)]),
		}, nil
	}
	log.Debug("CreateIndex", zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName))
	t := &CreateIndexReqTask{
		baseReqTask: baseReqTask{
			cv:   make(chan error),
			core: c,
		},
		Req: in,
	}
	c.ddReqQueue <- t
	err := t.WaitToFinish()
	if err != nil {
		log.Debug("CreateIndex Failed", zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    "CreateIndex failed, error = " + err.Error(),
		}, nil
	}
	log.Debug("CreateIndex Success", zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName))
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
		Reason:    "",
	}, nil
}

func (c *Core) DescribeIndex(ctx context.Context, in *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	code := c.stateCode.Load().(internalpb2.StateCode)
	if code != internalpb2.StateCode_HEALTHY {
		return &milvuspb.DescribeIndexResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    fmt.Sprintf("state code = %s", internalpb2.StateCode_name[int32(code)]),
			},
			IndexDescriptions: nil,
		}, nil
	}
	log.Debug("DescribeIndex", zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName))
	t := &DescribeIndexReqTask{
		baseReqTask: baseReqTask{
			cv:   make(chan error),
			core: c,
		},
		Req: in,
		Rsp: &milvuspb.DescribeIndexResponse{
			Status:            nil,
			IndexDescriptions: nil,
		},
	}
	c.ddReqQueue <- t
	err := t.WaitToFinish()
	if err != nil {
		return &milvuspb.DescribeIndexResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    "DescribeIndex failed, error = " + err.Error(),
			},
			IndexDescriptions: nil,
		}, nil
	}
	idxNames := make([]string, 0, len(t.Rsp.IndexDescriptions))
	for _, i := range t.Rsp.IndexDescriptions {
		idxNames = append(idxNames, i.IndexName)
	}
	log.Debug("DescribeIndex Success", zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName), zap.Strings("index names", idxNames))
	t.Rsp.Status = &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
		Reason:    "",
	}
	return t.Rsp, nil
}

func (c *Core) DropIndex(ctx context.Context, in *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	code := c.stateCode.Load().(internalpb2.StateCode)
	if code != internalpb2.StateCode_HEALTHY {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    fmt.Sprintf("state code = %s", internalpb2.StateCode_name[int32(code)]),
		}, nil
	}
	log.Debug("DropIndex", zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName), zap.String("index name", in.IndexName))
	t := &DropIndexReqTask{
		baseReqTask: baseReqTask{
			cv:   make(chan error),
			core: c,
		},
		Req: in,
	}
	c.ddReqQueue <- t
	err := t.WaitToFinish()
	if err != nil {
		log.Debug("DropIndex Failed", zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName), zap.String("index name", in.IndexName))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    "DropIndex failed, error = " + err.Error(),
		}, nil
	}
	log.Debug("DropIndex Success", zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName), zap.String("index name", in.IndexName))
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
		Reason:    "",
	}, nil
}

func (c *Core) DescribeSegment(ctx context.Context, in *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error) {
	code := c.stateCode.Load().(internalpb2.StateCode)
	if code != internalpb2.StateCode_HEALTHY {
		return &milvuspb.DescribeSegmentResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    fmt.Sprintf("state code = %s", internalpb2.StateCode_name[int32(code)]),
			},
			IndexID: 0,
		}, nil
	}
	log.Debug("DescribeSegment", zap.Int64("collection id", in.CollectionID), zap.Int64("segment id", in.SegmentID))
	t := &DescribeSegmentReqTask{
		baseReqTask: baseReqTask{
			cv:   make(chan error),
			core: c,
		},
		Req: in,
		Rsp: &milvuspb.DescribeSegmentResponse{
			Status:  nil,
			IndexID: 0,
		},
	}
	c.ddReqQueue <- t
	err := t.WaitToFinish()
	if err != nil {
		log.Debug("DescribeSegment Failed", zap.Int64("collection id", in.CollectionID), zap.Int64("segment id", in.SegmentID))
		return &milvuspb.DescribeSegmentResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    "DescribeSegment failed, error = " + err.Error(),
			},
			IndexID: 0,
		}, nil
	}
	log.Debug("DescribeSegment Success", zap.Int64("collection id", in.CollectionID), zap.Int64("segment id", in.SegmentID))
	t.Rsp.Status = &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
		Reason:    "",
	}
	return t.Rsp, nil
}

func (c *Core) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentRequest) (*milvuspb.ShowSegmentResponse, error) {
	code := c.stateCode.Load().(internalpb2.StateCode)
	if code != internalpb2.StateCode_HEALTHY {
		return &milvuspb.ShowSegmentResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    fmt.Sprintf("state code = %s", internalpb2.StateCode_name[int32(code)]),
			},
			SegmentIDs: nil,
		}, nil
	}
	log.Debug("ShowSegments", zap.Int64("collection id", in.CollectionID), zap.Int64("partition id", in.PartitionID))
	t := &ShowSegmentReqTask{
		baseReqTask: baseReqTask{
			cv:   make(chan error),
			core: c,
		},
		Req: in,
		Rsp: &milvuspb.ShowSegmentResponse{
			Status:     nil,
			SegmentIDs: nil,
		},
	}
	c.ddReqQueue <- t
	err := t.WaitToFinish()
	if err != nil {
		return &milvuspb.ShowSegmentResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    "ShowSegments failed, error: " + err.Error(),
			},
			SegmentIDs: nil,
		}, nil
	}
	log.Debug("ShowSegments Success", zap.Int64("collection id", in.CollectionID), zap.Int64("partition id", in.PartitionID), zap.Int64s("segments ids", t.Rsp.SegmentIDs))
	t.Rsp.Status = &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
		Reason:    "",
	}
	return t.Rsp, nil
}

func (c *Core) AllocTimestamp(ctx context.Context, in *masterpb.TsoRequest) (*masterpb.TsoResponse, error) {
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
	// log.Printf("AllocTimestamp : %d", ts)
	return &masterpb.TsoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
			Reason:    "",
		},
		Timestamp: ts,
		Count:     in.Count,
	}, nil
}

func (c *Core) AllocID(ctx context.Context, in *masterpb.IDRequest) (*masterpb.IDResponse, error) {
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
	log.Debug("AllocID", zap.Int64("id start", start), zap.Uint32("count", in.Count))
	return &masterpb.IDResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
			Reason:    "",
		},
		ID:    start,
		Count: in.Count,
	}, nil
}

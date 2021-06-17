// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package masterservice

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/allocator"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	ms "github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/masterpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/tso"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

//  internalpb -> internalpb
//  proxypb(proxy_service)
//  querypb(query_service)
//  datapb(data_service)
//  indexpb(index_service)
//  milvuspb -> milvuspb
//  masterpb2 -> masterpb (master_service)

// ------------------ struct -----------------------

// DdOperation used to save ddMsg into ETCD
type DdOperation struct {
	Body  string `json:"body"`
	Body1 string `json:"body1"` // used for CreateCollectionReq only
	Type  string `json:"type"`
}

const (
	// MetricRequestsTotal used to count the num of total requests
	MetricRequestsTotal = "total"

	// MetricRequestsSuccess used to count the num of successful requests
	MetricRequestsSuccess = "success"
)

func metricProxyNode(v int64) string {
	return fmt.Sprintf("client_%d", v)
}

// Core master core
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
	IDAllocator       func(count uint32) (typeutil.UniqueID, typeutil.UniqueID, error)
	IDAllocatorUpdate func() error

	//tso allocator
	TSOAllocator       func(count uint32) (typeutil.Timestamp, error)
	TSOAllocatorUpdate func() error

	//inner members
	ctx     context.Context
	cancel  context.CancelFunc
	etcdCli *clientv3.Client
	kvBase  *etcdkv.EtcdKV

	//setMsgStreams, send time tick into dd channel and time tick channel
	SendTimeTick func(t typeutil.Timestamp) error

	//setMsgStreams, send create collection into dd channel
	SendDdCreateCollectionReq func(ctx context.Context, req *internalpb.CreateCollectionRequest, channelNames []string) error

	//setMsgStreams, send drop collection into dd channel, and notify the proxy to delete this collection
	SendDdDropCollectionReq func(ctx context.Context, req *internalpb.DropCollectionRequest, channelNames []string) error

	//setMsgStreams, send create partition into dd channel
	SendDdCreatePartitionReq func(ctx context.Context, req *internalpb.CreatePartitionRequest, channelNames []string) error

	//setMsgStreams, send drop partition into dd channel
	SendDdDropPartitionReq func(ctx context.Context, req *internalpb.DropPartitionRequest, channelNames []string) error

	// if master create segment, data service will put segment msg into this channel
	DataServiceSegmentChan <-chan *ms.MsgPack

	// if segment flush completed, data node would put segment msg into this channel
	DataNodeFlushedSegmentChan <-chan *ms.MsgPack

	//get binlog file path from data service,
	CallGetBinlogFilePathsService func(segID typeutil.UniqueID, fieldID typeutil.UniqueID) ([]string, error)
	CallGetNumRowsService         func(segID typeutil.UniqueID, isFromFlushedChan bool) (int64, error)

	//call index builder's client to build index, return build id
	CallBuildIndexService func(ctx context.Context, binlog []string, field *schemapb.FieldSchema, idxInfo *etcdpb.IndexInfo) (typeutil.UniqueID, error)
	CallDropIndexService  func(ctx context.Context, indexID typeutil.UniqueID) error

	NewProxyClient func(sess *sessionutil.Session) (types.ProxyNode, error)

	//query service interface, notify query service to release collection
	CallReleaseCollectionService func(ctx context.Context, ts typeutil.Timestamp, dbID typeutil.UniqueID, collectionID typeutil.UniqueID) error

	//dd request scheduler
	ddReqQueue chan reqTask //dd request will be push into this chan

	//dml channels
	dmlChannels *dmlChannels

	//ProxyNode manager
	proxyNodeManager *proxyNodeManager

	// proxy clients
	proxyClientManager *proxyClientManager

	// channel timetick
	chanTimeTick *timetickSync

	//time tick loop
	lastTimeTick typeutil.Timestamp

	//states code
	stateCode atomic.Value

	//call once
	initOnce  sync.Once
	startOnce sync.Once
	//isInit    atomic.Value

	session     *sessionutil.Session
	sessCloseCh <-chan bool

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
	core.UpdateStateCode(internalpb.StateCode_Abnormal)
	return core, nil
}

func (c *Core) UpdateStateCode(code internalpb.StateCode) {
	c.stateCode.Store(code)
}

func (c *Core) checkInit() error {
	if c.MetaTable == nil {
		return fmt.Errorf("MetaTable is nil")
	}
	if c.IDAllocator == nil {
		return fmt.Errorf("idAllocator is nil")
	}
	if c.IDAllocatorUpdate == nil {
		return fmt.Errorf("idAllocatorUpdate is nil")
	}
	if c.TSOAllocator == nil {
		return fmt.Errorf("tsoAllocator is nil")
	}
	if c.TSOAllocatorUpdate == nil {
		return fmt.Errorf("tsoAllocatorUpdate is nil")
	}
	if c.etcdCli == nil {
		return fmt.Errorf("etcdCli is nil")
	}
	if c.kvBase == nil {
		return fmt.Errorf("kvBase is nil")
	}
	if c.ddReqQueue == nil {
		return fmt.Errorf("ddReqQueue is nil")
	}
	if c.SendDdCreateCollectionReq == nil {
		return fmt.Errorf("SendDdCreateCollectionReq is nil")
	}
	if c.SendDdDropCollectionReq == nil {
		return fmt.Errorf("SendDdDropCollectionReq is nil")
	}
	if c.SendDdCreatePartitionReq == nil {
		return fmt.Errorf("SendDdCreatePartitionReq is nil")
	}
	if c.SendDdDropPartitionReq == nil {
		return fmt.Errorf("SendDdDropPartitionReq is nil")
	}
	if c.CallGetBinlogFilePathsService == nil {
		return fmt.Errorf("CallGetBinlogFilePathsService is nil")
	}
	if c.CallGetNumRowsService == nil {
		return fmt.Errorf("CallGetNumRowsService is nil")
	}
	if c.CallBuildIndexService == nil {
		return fmt.Errorf("CallBuildIndexService is nil")
	}
	if c.CallDropIndexService == nil {
		return fmt.Errorf("CallDropIndexService is nil")
	}
	if c.NewProxyClient == nil {
		return fmt.Errorf("NewProxyNodeClient is nil")
	}
	if c.CallReleaseCollectionService == nil {
		return fmt.Errorf("CallReleaseCollectionService is nil")
	}
	if c.DataServiceSegmentChan == nil {
		return fmt.Errorf("DataServiceSegmentChan is nil")
	}
	if c.DataNodeFlushedSegmentChan == nil {
		return fmt.Errorf("DataNodeFlushedSegmentChan is nil")
	}

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
			err := task.Execute(task.Ctx())
			task.Notify(err)
		}
	}
}

func (c *Core) startTimeTickLoop() {
	ticker := time.NewTicker(time.Duration(Params.TimeTickInterval) * time.Millisecond)
	cnt := 0
	for {
		select {
		case <-c.ctx.Done():
			log.Debug("master context closed", zap.Error(c.ctx.Err()))
			return
		case <-ticker.C:
			if len(c.ddReqQueue) < 2 || cnt > 5 {
				tt := &TimetickTask{
					baseReqTask: baseReqTask{
						ctx:  c.ctx,
						cv:   make(chan error, 1),
						core: c,
					},
				}
				c.ddReqQueue <- tt
				cnt = 0
			} else {
				cnt++
			}

		}
	}
}

// data service send segment info msg to master when create segment
func (c *Core) startDataServiceSegmentLoop() {
	for {
		select {
		case <-c.ctx.Done():
			log.Debug("close data service segment loop")
			return
		case segMsg, ok := <-c.DataServiceSegmentChan:
			if !ok {
				log.Debug("data service segment channel is closed, exit loop")
				return
			}
			var segInfos []*datapb.SegmentInfo
			for _, msg := range segMsg.Msgs {
				if msg.Type() != commonpb.MsgType_SegmentInfo {
					continue
				}
				segInfoMsg, ok := msg.(*ms.SegmentInfoMsg)
				if !ok {
					log.Debug("input msg is not SegmentInfoMsg")
					continue
				}
				if segInfoMsg.Segment != nil {
					segInfos = append(segInfos, segInfoMsg.Segment)
					log.Debug("open segment", zap.Int64("segmentID", segInfoMsg.Segment.ID))
				}
			}
			if len(segInfos) > 0 {
				startPosStr, err := EncodeMsgPositions(segMsg.StartPositions)
				if err != nil {
					log.Error("encode msg start positions fail", zap.String("err", err.Error()))
					continue
				}
				endPosStr, err := EncodeMsgPositions(segMsg.EndPositions)
				if err != nil {
					log.Error("encode msg end positions fail", zap.String("err", err.Error()))
					continue
				}

				if _, err := c.MetaTable.AddSegment(segInfos, startPosStr, endPosStr); err != nil {
					//what if master add segment failed, but data service success?
					log.Debug("add segment info meta table failed ", zap.String("error", err.Error()))
					continue
				}
			}
		}
	}
}

// data node will put msg in this channel when flush segment
func (c *Core) startDataNodeFlushedSegmentLoop() {
	for {
		select {
		case <-c.ctx.Done():
			log.Debug("close segment flush completed loop")
			return
		case segMsg, ok := <-c.DataNodeFlushedSegmentChan:
			if !ok {
				log.Debug("data node segment flush completed chan has closed, exit loop")
				return
			}

			startPosStr, err := EncodeMsgPositions(segMsg.StartPositions)
			if err != nil {
				log.Error("encode msg start positions fail", zap.String("err", err.Error()))
				continue
			}
			endPosStr, err := EncodeMsgPositions(segMsg.EndPositions)
			if err != nil {
				log.Error("encode msg end positions fail", zap.String("err", err.Error()))
				continue
			}

			var segIdxInfos []*etcdpb.SegmentIndexInfo
			for _, msg := range segMsg.Msgs {
				// check msg type
				if msg.Type() != commonpb.MsgType_SegmentFlushDone {
					continue
				}
				flushMsg, ok := msg.(*ms.FlushCompletedMsg)
				if !ok {
					log.Debug("input msg is not FlushCompletedMsg")
					continue
				}
				segID := flushMsg.SegmentID
				log.Debug("flush segment", zap.Int64("id", segID))

				coll, err := c.MetaTable.GetCollectionBySegmentID(segID)
				if err != nil {
					log.Warn("GetCollectionBySegmentID error", zap.Error(err))
					continue
				}
				err = c.MetaTable.AddFlushedSegment(segID)
				if err != nil {
					log.Warn("AddFlushedSegment error", zap.Error(err))
					continue
				}

				if len(coll.FieldIndexes) == 0 {
					log.Debug("no index params on collection", zap.String("collection_name", coll.Schema.Name))
				}

				for _, f := range coll.FieldIndexes {
					fieldSch, err := GetFieldSchemaByID(coll, f.FiledID)
					if err != nil {
						log.Warn("field schema not found", zap.Int64("field id", f.FiledID))
						continue
					}

					idxInfo, err := c.MetaTable.GetIndexByID(f.IndexID)
					if err != nil {
						log.Warn("index not found", zap.Int64("index id", f.IndexID))
						continue
					}

					info := etcdpb.SegmentIndexInfo{
						SegmentID:   segID,
						FieldID:     fieldSch.FieldID,
						IndexID:     idxInfo.IndexID,
						EnableIndex: false,
					}
					info.BuildID, err = c.BuildIndex(segID, fieldSch, idxInfo, true)
					if err == nil && info.BuildID != 0 {
						info.EnableIndex = true
					} else {
						log.Error("build index fail", zap.String("error", err.Error()))
					}

					segIdxInfos = append(segIdxInfos, &info)
				}
			}

			if len(segIdxInfos) > 0 {
				_, err = c.MetaTable.AddIndex(segIdxInfos, startPosStr, endPosStr)
				if err != nil {
					log.Error("AddIndex fail", zap.String("err", err.Error()))
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
			if err := c.TSOAllocatorUpdate(); err != nil {
				log.Warn("failed to update timestamp: ", zap.Error(err))
				continue
			}
			if err := c.IDAllocatorUpdate(); err != nil {
				log.Warn("failed to update id: ", zap.Error(err))
				continue
			}
		case <-ctx.Done():
			// Server is closed and it should return nil.
			log.Debug("tsLoop is closed")
			return
		}
	}
}

func (c *Core) sessionLoop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case _, ok := <-c.sessCloseCh:
			if !ok {
				log.Error("master service disconnect with etcd, process will exit in 1 second")
				go func() {
					time.Sleep(time.Second)
					os.Exit(-1)
				}()
			}
		}
	}
}

func (c *Core) watchProxyNodeLoop() {

}

func (c *Core) setDdMsgSendFlag(b bool) error {
	flag, err := c.MetaTable.client.Load(DDMsgSendPrefix, 0)
	if err != nil {
		return err
	}

	if (b && flag == "true") || (!b && flag == "false") {
		log.Debug("DdMsg send flag need not change", zap.String("flag", flag))
		return nil
	}

	if b {
		_, err = c.MetaTable.client.Save(DDMsgSendPrefix, "true")
		return err
	}
	_, err = c.MetaTable.client.Save(DDMsgSendPrefix, "false")
	return err
}

func (c *Core) startMsgStreamAndSeek(chanName string, subName string, key string) (*ms.MsgStream, error) {
	stream, err := c.msFactory.NewMsgStream(c.ctx)
	if err != nil {
		return nil, err
	}
	stream.AsConsumer([]string{chanName}, subName)
	log.Debug("AsConsumer: " + chanName + ":" + subName)

	msgPosStr, err := c.MetaTable.client.Load(key, 0)
	if err == nil {
		msgPositions := make([]*ms.MsgPosition, 0)
		if err := DecodeMsgPositions(msgPosStr, &msgPositions); err != nil {
			return nil, fmt.Errorf("decode msg positions fail, err %s", err.Error())
		}
		if len(msgPositions) > 0 {
			if err := stream.Seek(msgPositions); err != nil {
				return nil, fmt.Errorf("msg stream seek fail, err %s", err.Error())
			}
			log.Debug("msg stream: " + chanName + ":" + subName + " seek to stored position")
		}
	}
	stream.Start()
	return &stream, nil
}

func (c *Core) setMsgStreams() error {
	if Params.PulsarAddress == "" {
		return fmt.Errorf("PulsarAddress is empty")
	}
	if Params.MsgChannelSubName == "" {
		return fmt.Errorf("MsgChannelSubName is emptyr")
	}

	// master time tick channel
	if Params.TimeTickChannel == "" {
		return fmt.Errorf("TimeTickChannel is empty")
	}
	timeTickStream, _ := c.msFactory.NewMsgStream(c.ctx)
	timeTickStream.AsProducer([]string{Params.TimeTickChannel})
	log.Debug("masterservice AsProducer: " + Params.TimeTickChannel)

	c.SendTimeTick = func(t typeutil.Timestamp) error {
		msgPack := ms.MsgPack{}
		baseMsg := ms.BaseMsg{
			BeginTimestamp: t,
			EndTimestamp:   t,
			HashValues:     []uint32{0},
		}
		timeTickResult := internalpb.TimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_TimeTick,
				MsgID:     0,
				Timestamp: t,
				SourceID:  c.session.ServerID,
			},
		}
		timeTickMsg := &ms.TimeTickMsg{
			BaseMsg:     baseMsg,
			TimeTickMsg: timeTickResult,
		}
		msgPack.Msgs = append(msgPack.Msgs, timeTickMsg)
		if err := timeTickStream.Broadcast(&msgPack); err != nil {
			return err
		}
		metrics.MasterDDChannelTimeTick.Set(float64(tsoutil.Mod24H(t)))

		//c.dmlChannels.BroadcastAll(&msgPack)
		pc := c.MetaTable.ListCollectionPhysicalChannels()
		pt := make([]uint64, len(pc))
		for i := 0; i < len(pt); i++ {
			pt[i] = t
		}
		ttMsg := internalpb.ChannelTimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_TimeTick,
				MsgID:     0, //TODO
				Timestamp: t,
				SourceID:  c.session.ServerID,
			},
			ChannelNames: pc,
			Timestamps:   pt,
		}
		return c.chanTimeTick.UpdateTimeTick(&ttMsg)
	}

	c.SendDdCreateCollectionReq = func(ctx context.Context, req *internalpb.CreateCollectionRequest, channelNames []string) error {
		msgPack := ms.MsgPack{}
		baseMsg := ms.BaseMsg{
			Ctx:            ctx,
			BeginTimestamp: req.Base.Timestamp,
			EndTimestamp:   req.Base.Timestamp,
			HashValues:     []uint32{0},
		}
		msg := &ms.CreateCollectionMsg{
			BaseMsg:                 baseMsg,
			CreateCollectionRequest: *req,
		}
		msgPack.Msgs = append(msgPack.Msgs, msg)
		return c.dmlChannels.BroadcastAll(channelNames, &msgPack)
	}

	c.SendDdDropCollectionReq = func(ctx context.Context, req *internalpb.DropCollectionRequest, channelNames []string) error {
		msgPack := ms.MsgPack{}
		baseMsg := ms.BaseMsg{
			Ctx:            ctx,
			BeginTimestamp: req.Base.Timestamp,
			EndTimestamp:   req.Base.Timestamp,
			HashValues:     []uint32{0},
		}
		msg := &ms.DropCollectionMsg{
			BaseMsg:               baseMsg,
			DropCollectionRequest: *req,
		}
		msgPack.Msgs = append(msgPack.Msgs, msg)
		return c.dmlChannels.BroadcastAll(channelNames, &msgPack)
	}

	c.SendDdCreatePartitionReq = func(ctx context.Context, req *internalpb.CreatePartitionRequest, channelNames []string) error {
		msgPack := ms.MsgPack{}
		baseMsg := ms.BaseMsg{
			Ctx:            ctx,
			BeginTimestamp: req.Base.Timestamp,
			EndTimestamp:   req.Base.Timestamp,
			HashValues:     []uint32{0},
		}
		msg := &ms.CreatePartitionMsg{
			BaseMsg:                baseMsg,
			CreatePartitionRequest: *req,
		}
		msgPack.Msgs = append(msgPack.Msgs, msg)
		return c.dmlChannels.BroadcastAll(channelNames, &msgPack)
	}

	c.SendDdDropPartitionReq = func(ctx context.Context, req *internalpb.DropPartitionRequest, channelNames []string) error {
		msgPack := ms.MsgPack{}
		baseMsg := ms.BaseMsg{
			Ctx:            ctx,
			BeginTimestamp: req.Base.Timestamp,
			EndTimestamp:   req.Base.Timestamp,
			HashValues:     []uint32{0},
		}
		msg := &ms.DropPartitionMsg{
			BaseMsg:              baseMsg,
			DropPartitionRequest: *req,
		}
		msgPack.Msgs = append(msgPack.Msgs, msg)
		return c.dmlChannels.BroadcastAll(channelNames, &msgPack)
	}

	if Params.DataServiceSegmentChannel == "" {
		return fmt.Errorf("DataServiceSegmentChannel is empty")
	}

	// data service will put msg into this channel when create segment
	dsChanName := Params.DataServiceSegmentChannel
	dsSubName := Params.MsgChannelSubName + "ds"
	dsStream, err := c.startMsgStreamAndSeek(dsChanName, dsSubName, SegInfoMsgEndPosPrefix)
	if err != nil {
		return err
	}
	c.DataServiceSegmentChan = (*dsStream).Chan()

	// data node will put msg into this channel when flush segment
	dnChanName := Params.DataServiceSegmentChannel
	dnSubName := Params.MsgChannelSubName + "dn"
	dnStream, err := c.startMsgStreamAndSeek(dnChanName, dnSubName, FlushedSegMsgEndPosPrefix)
	if err != nil {
		return err
	}
	c.DataNodeFlushedSegmentChan = (*dnStream).Chan()

	return nil
}

//SetNewProxyClient create proxy node by this func
func (c *Core) SetNewProxyClient(f func(sess *sessionutil.Session) (types.ProxyNode, error)) {
	if c.NewProxyClient == nil {
		c.NewProxyClient = f
	} else {
		log.Debug("NewProxyClient has already set")
	}
}

func (c *Core) SetDataService(ctx context.Context, s types.DataService) error {
	rsp, err := s.GetSegmentInfoChannel(ctx)
	if err != nil {
		return err
	}
	Params.DataServiceSegmentChannel = rsp.Value
	log.Debug("data service segment", zap.String("channel name", Params.DataServiceSegmentChannel))

	c.CallGetBinlogFilePathsService = func(segID typeutil.UniqueID, fieldID typeutil.UniqueID) (retFiles []string, retErr error) {
		defer func() {
			if err := recover(); err != nil {
				retFiles = nil
				retErr = fmt.Errorf("get bin log file paths panic, msg = %v", err)
			}
		}()
		ts, err := c.TSOAllocator(1)
		if err != nil {
			retFiles = nil
			retErr = err
			return
		}
		binlog, err := s.GetInsertBinlogPaths(ctx, &datapb.GetInsertBinlogPathsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0, //TODO, msg type
				MsgID:     0,
				Timestamp: ts,
				SourceID:  c.session.ServerID,
			},
			SegmentID: segID,
		})
		if err != nil {
			retFiles = nil
			retErr = err
			return
		}
		if binlog.Status.ErrorCode != commonpb.ErrorCode_Success {
			retFiles = nil
			retErr = fmt.Errorf("GetInsertBinlogPaths from data service failed, error = %s", binlog.Status.Reason)
			return
		}
		for i := range binlog.FieldIDs {
			if binlog.FieldIDs[i] == fieldID {
				retFiles = binlog.Paths[i].Values
				retErr = nil
				return
			}
		}
		retFiles = nil
		retErr = fmt.Errorf("binlog file not exist, segment id = %d, field id = %d", segID, fieldID)
		return
	}

	c.CallGetNumRowsService = func(segID typeutil.UniqueID, isFromFlushedChan bool) (retRows int64, retErr error) {
		defer func() {
			if err := recover(); err != nil {
				retRows = 0
				retErr = fmt.Errorf("get num rows panic, msg = %v", err)
				return
			}
		}()
		ts, err := c.TSOAllocator(1)
		if err != nil {
			retRows = 0
			retErr = err
			return
		}
		segInfo, err := s.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0, //TODO, msg type
				MsgID:     0,
				Timestamp: ts,
				SourceID:  c.session.ServerID,
			},
			SegmentIDs: []typeutil.UniqueID{segID},
		})
		if err != nil {
			retRows = 0
			retErr = err
			return
		}
		if segInfo.Status.ErrorCode != commonpb.ErrorCode_Success {
			return 0, fmt.Errorf("GetSegmentInfo from data service failed, error = %s", segInfo.Status.Reason)
		}
		if len(segInfo.Infos) != 1 {
			log.Debug("get segment info empty")
			retRows = 0
			retErr = nil
			return
		}
		if !isFromFlushedChan && segInfo.Infos[0].State != commonpb.SegmentState_Flushed {
			log.Debug("segment id not flushed", zap.Int64("segment id", segID))
			retRows = 0
			retErr = nil
			return
		}
		retRows = segInfo.Infos[0].NumOfRows
		retErr = nil
		return
	}
	return nil
}

func (c *Core) SetIndexService(s types.IndexService) error {
	c.CallBuildIndexService = func(ctx context.Context, binlog []string, field *schemapb.FieldSchema, idxInfo *etcdpb.IndexInfo) (retID typeutil.UniqueID, retErr error) {
		defer func() {
			if err := recover(); err != nil {
				retID = 0
				retErr = fmt.Errorf("build index panic, msg = %v", err)
				return
			}
		}()
		rsp, err := s.BuildIndex(ctx, &indexpb.BuildIndexRequest{
			DataPaths:   binlog,
			TypeParams:  field.TypeParams,
			IndexParams: idxInfo.IndexParams,
			IndexID:     idxInfo.IndexID,
			IndexName:   idxInfo.IndexName,
		})
		if err != nil {
			retID = 0
			retErr = err
			return
		}
		if rsp.Status.ErrorCode != commonpb.ErrorCode_Success {
			retID = 0
			retErr = fmt.Errorf("BuildIndex from index service failed, error = %s", rsp.Status.Reason)
			return
		}
		retID = rsp.IndexBuildID
		retErr = nil
		return
	}

	c.CallDropIndexService = func(ctx context.Context, indexID typeutil.UniqueID) (retErr error) {
		defer func() {
			if err := recover(); err != nil {
				retErr = fmt.Errorf("drop index from index service panic, msg = %v", err)
				return
			}
		}()
		rsp, err := s.DropIndex(ctx, &indexpb.DropIndexRequest{
			IndexID: indexID,
		})
		if err != nil {
			retErr = err
			return
		}
		if rsp.ErrorCode != commonpb.ErrorCode_Success {
			retErr = fmt.Errorf(rsp.Reason)
			return
		}
		retErr = nil
		return
	}

	return nil
}

func (c *Core) SetQueryService(s types.QueryService) error {
	c.CallReleaseCollectionService = func(ctx context.Context, ts typeutil.Timestamp, dbID typeutil.UniqueID, collectionID typeutil.UniqueID) (retErr error) {
		defer func() {
			if err := recover(); err != nil {
				retErr = fmt.Errorf("release collection from query service panic, msg = %v", err)
				return
			}
		}()
		req := &querypb.ReleaseCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ReleaseCollection,
				MsgID:     0, //TODO, msg ID
				Timestamp: ts,
				SourceID:  c.session.ServerID,
			},
			DbID:         dbID,
			CollectionID: collectionID,
		}
		rsp, err := s.ReleaseCollection(ctx, req)
		if err != nil {
			retErr = err
			return
		}
		if rsp.ErrorCode != commonpb.ErrorCode_Success {
			retErr = fmt.Errorf("ReleaseCollection from query service failed, error = %s", rsp.Reason)
			return
		}
		retErr = nil
		return
	}
	return nil
}

// BuildIndex will check row num and call build index service
func (c *Core) BuildIndex(segID typeutil.UniqueID, field *schemapb.FieldSchema, idxInfo *etcdpb.IndexInfo, isFlush bool) (typeutil.UniqueID, error) {
	if c.MetaTable.IsSegmentIndexed(segID, field, idxInfo.IndexParams) {
		return 0, nil
	}
	rows, err := c.CallGetNumRowsService(segID, isFlush)
	if err != nil {
		return 0, err
	}
	var bldID typeutil.UniqueID
	if rows < Params.MinSegmentSizeToEnableIndex {
		log.Debug("num of rows is less than MinSegmentSizeToEnableIndex", zap.Int64("num rows", rows))
	} else {
		binlogs, err := c.CallGetBinlogFilePathsService(segID, field.FieldID)
		if err != nil {
			return 0, err
		}
		bldID, err = c.CallBuildIndexService(c.ctx, binlogs, field, idxInfo)
		if err != nil {
			return 0, err
		}
	}
	log.Debug("build index", zap.String("index name", idxInfo.IndexName),
		zap.String("field name", field.Name),
		zap.Int64("segment id", segID))
	return bldID, nil
}

// Register register master service at etcd
func (c *Core) Register() error {
	c.session = sessionutil.NewSession(c.ctx, Params.MetaRootPath, Params.EtcdEndpoints)
	if c.session == nil {
		return fmt.Errorf("session is nil, maybe the etcd client connection fails")
	}
	c.sessCloseCh = c.session.Init(typeutil.MasterServiceRole, Params.Address, true)
	return nil
}

func (c *Core) Init() error {
	var initError error = nil
	c.initOnce.Do(func() {
		connectEtcdFn := func() error {
			if c.etcdCli, initError = clientv3.New(clientv3.Config{Endpoints: Params.EtcdEndpoints, DialTimeout: 5 * time.Second}); initError != nil {
				return initError
			}
			tsAlloc := func() typeutil.Timestamp {
				for {
					var ts typeutil.Timestamp
					var err error
					if ts, err = c.TSOAllocator(1); err == nil {
						return ts
					}
					time.Sleep(100 * time.Millisecond)
					log.Debug("alloc time stamp error", zap.Error(err))
				}
			}
			var ms *metaSnapshot
			ms, initError = newMetaSnapshot(c.etcdCli, Params.MetaRootPath, TimestampPrefix, 1024, tsAlloc)
			if initError != nil {
				return initError
			}
			if c.MetaTable, initError = NewMetaTable(ms); initError != nil {
				return initError
			}
			c.kvBase = etcdkv.NewEtcdKV(c.etcdCli, Params.KvRootPath)
			return nil
		}
		err := retry.Retry(100000, time.Millisecond*200, connectEtcdFn)
		if err != nil {
			return
		}

		idAllocator := allocator.NewGlobalIDAllocator("idTimestamp", tsoutil.NewTSOKVBase(Params.EtcdEndpoints, Params.KvRootPath, "gid"))
		if initError = idAllocator.Initialize(); initError != nil {
			return
		}
		c.IDAllocator = func(count uint32) (typeutil.UniqueID, typeutil.UniqueID, error) {
			return idAllocator.Alloc(count)
		}
		c.IDAllocatorUpdate = func() error {
			return idAllocator.UpdateID()
		}

		tsoAllocator := tso.NewGlobalTSOAllocator("timestamp", tsoutil.NewTSOKVBase(Params.EtcdEndpoints, Params.KvRootPath, "tso"))
		if initError = tsoAllocator.Initialize(); initError != nil {
			return
		}
		c.TSOAllocator = func(count uint32) (typeutil.Timestamp, error) {
			return tsoAllocator.Alloc(count)
		}
		c.TSOAllocatorUpdate = func() error {
			return tsoAllocator.UpdateTSO()
		}

		m := map[string]interface{}{
			"PulsarAddress":  Params.PulsarAddress,
			"ReceiveBufSize": 1024,
			"PulsarBufSize":  1024}
		if initError = c.msFactory.SetParams(m); initError != nil {
			return
		}

		c.dmlChannels = newDMLChannels(c)
		pc := c.MetaTable.ListCollectionPhysicalChannels()
		c.dmlChannels.AddProducerChannels(pc...)

		c.chanTimeTick = newTimeTickSync(c)
		c.chanTimeTick.AddProxyNode(c.session)
		c.proxyClientManager = newProxyClientManager(c)

		c.proxyNodeManager, initError = newProxyNodeManager(
			c.ctx,
			Params.EtcdEndpoints,
			c.chanTimeTick.GetProxyNodes,
			c.proxyClientManager.GetProxyClients,
		)
		c.proxyNodeManager.AddSession(c.chanTimeTick.AddProxyNode, c.proxyClientManager.AddProxyClient)
		c.proxyNodeManager.DelSession(c.chanTimeTick.DelProxyNode, c.proxyClientManager.DelProxyClient)

		c.ddReqQueue = make(chan reqTask, 1024)
		initError = c.setMsgStreams()
	})
	if initError == nil {
		log.Debug("Master service", zap.String("State Code", internalpb.StateCode_name[int32(internalpb.StateCode_Initializing)]))
	}
	return initError
}

func (c *Core) reSendDdMsg(ctx context.Context) error {
	flag, err := c.MetaTable.client.Load(DDMsgSendPrefix, 0)
	if err != nil || flag == "true" {
		log.Debug("No un-successful DdMsg")
		return nil
	}

	ddOpStr, err := c.MetaTable.client.Load(DDOperationPrefix, 0)
	if err != nil {
		log.Debug("DdOperation key does not exist")
		return nil
	}
	var ddOp DdOperation
	if err = json.Unmarshal([]byte(ddOpStr), &ddOp); err != nil {
		return err
	}

	switch ddOp.Type {
	case CreateCollectionDDType:
		var ddCollReq = internalpb.CreateCollectionRequest{}
		if err = proto.UnmarshalText(ddOp.Body, &ddCollReq); err != nil {
			return err
		}
		// TODO: can optimize
		var ddPartReq = internalpb.CreatePartitionRequest{}
		if err = proto.UnmarshalText(ddOp.Body1, &ddPartReq); err != nil {
			return err
		}
		collInfo, err := c.MetaTable.GetCollectionByName(ddCollReq.CollectionName, 0)
		if err != nil {
			return err
		}
		if err = c.SendDdCreateCollectionReq(ctx, &ddCollReq, collInfo.PhysicalChannelNames); err != nil {
			return err
		}
		if err = c.SendDdCreatePartitionReq(ctx, &ddPartReq, collInfo.PhysicalChannelNames); err != nil {
			return err
		}
	case DropCollectionDDType:
		var ddReq = internalpb.DropCollectionRequest{}
		if err = proto.UnmarshalText(ddOp.Body, &ddReq); err != nil {
			return err
		}
		collInfo, err := c.MetaTable.GetCollectionByName(ddReq.CollectionName, 0)
		if err != nil {
			return err
		}
		if err = c.SendDdDropCollectionReq(ctx, &ddReq, collInfo.PhysicalChannelNames); err != nil {
			return err
		}
		req := proxypb.InvalidateCollMetaCacheRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0, //TODO, msg type
				MsgID:     0, //TODO, msg id
				Timestamp: ddReq.Base.Timestamp,
				SourceID:  c.session.ServerID,
			},
			DbName:         ddReq.DbName,
			CollectionName: ddReq.CollectionName,
		}
		c.proxyClientManager.InvalidateCollectionMetaCache(c.ctx, &req)

	case CreatePartitionDDType:
		var ddReq = internalpb.CreatePartitionRequest{}
		if err = proto.UnmarshalText(ddOp.Body, &ddReq); err != nil {
			return err
		}
		collInfo, err := c.MetaTable.GetCollectionByName(ddReq.CollectionName, 0)
		if err != nil {
			return err
		}
		if err = c.SendDdCreatePartitionReq(ctx, &ddReq, collInfo.PhysicalChannelNames); err != nil {
			return err
		}
		req := proxypb.InvalidateCollMetaCacheRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0, //TODO, msg type
				MsgID:     0, //TODO, msg id
				Timestamp: ddReq.Base.Timestamp,
				SourceID:  c.session.ServerID,
			},
			DbName:         ddReq.DbName,
			CollectionName: ddReq.CollectionName,
		}
		c.proxyClientManager.InvalidateCollectionMetaCache(c.ctx, &req)
	case DropPartitionDDType:
		var ddReq = internalpb.DropPartitionRequest{}
		if err = proto.UnmarshalText(ddOp.Body, &ddReq); err != nil {
			return err
		}
		collInfo, err := c.MetaTable.GetCollectionByName(ddReq.CollectionName, 0)
		if err != nil {
			return err
		}
		if err = c.SendDdDropPartitionReq(ctx, &ddReq, collInfo.PhysicalChannelNames); err != nil {
			return err
		}
		req := proxypb.InvalidateCollMetaCacheRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0, //TODO, msg type
				MsgID:     0, //TODO, msg id
				Timestamp: ddReq.Base.Timestamp,
				SourceID:  c.session.ServerID,
			},
			DbName:         ddReq.DbName,
			CollectionName: ddReq.CollectionName,
		}
		c.proxyClientManager.InvalidateCollectionMetaCache(c.ctx, &req)
	default:
		return fmt.Errorf("Invalid DdOperation %s", ddOp.Type)
	}

	// Update DDOperation in etcd
	return c.setDdMsgSendFlag(true)
}

func (c *Core) Start() error {
	if err := c.checkInit(); err != nil {
		log.Debug("MasterService Start checkInit failed", zap.Error(err))
		return err
	}

	log.Debug("MasterService", zap.Int64("node id", c.session.ServerID))
	log.Debug("MasterService", zap.String("time tick channel name", Params.TimeTickChannel))

	c.startOnce.Do(func() {
		if err := c.proxyNodeManager.WatchProxyNode(); err != nil {
			log.Debug("MasterService Start WatchProxyNode failed", zap.Error(err))
			return
		}
		if err := c.reSendDdMsg(c.ctx); err != nil {
			log.Debug("MasterService Start reSendDdMsg failed", zap.Error(err))
			return
		}
		go c.startDdScheduler()
		go c.startTimeTickLoop()
		go c.startDataServiceSegmentLoop()
		go c.startDataNodeFlushedSegmentLoop()
		go c.tsLoop()
		go c.sessionLoop()
		go c.chanTimeTick.StartWatch()
		c.stateCode.Store(internalpb.StateCode_Healthy)
	})
	log.Debug("MasterService", zap.String("State Code", internalpb.StateCode_name[int32(internalpb.StateCode_Healthy)]))
	return nil
}

func (c *Core) Stop() error {
	c.cancel()
	c.stateCode.Store(internalpb.StateCode_Abnormal)
	return nil
}

func (c *Core) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	code := c.stateCode.Load().(internalpb.StateCode)
	log.Debug("GetComponentStates", zap.String("State Code", internalpb.StateCode_name[int32(code)]))

	return &internalpb.ComponentStates{
		State: &internalpb.ComponentInfo{
			NodeID:    c.session.ServerID,
			Role:      typeutil.MasterServiceRole,
			StateCode: code,
			ExtraInfo: nil,
		},
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		SubcomponentStates: []*internalpb.ComponentInfo{
			{
				NodeID:    c.session.ServerID,
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
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: Params.TimeTickChannel,
	}, nil
}

func (c *Core) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: Params.StatisticsChannel,
	}, nil
}

func (c *Core) CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	metrics.MasterCreateCollectionCounter.WithLabelValues(metricProxyNode(in.Base.SourceID), MetricRequestsTotal).Inc()
	code := c.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("state code = %s", internalpb.StateCode_name[int32(code)]),
		}, nil
	}
	log.Debug("CreateCollection ", zap.String("name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID))
	t := &CreateCollectionReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			cv:   make(chan error, 1),
			core: c,
		},
		Req: in,
	}
	c.ddReqQueue <- t
	err := t.WaitToFinish()
	if err != nil {
		log.Debug("CreateCollection failed", zap.String("name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "Create collection failed: " + err.Error(),
		}, nil
	}
	log.Debug("CreateCollection Success", zap.String("name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID))
	metrics.MasterCreateCollectionCounter.WithLabelValues(metricProxyNode(in.Base.SourceID), MetricRequestsSuccess).Inc()
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (c *Core) DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	metrics.MasterDropCollectionCounter.WithLabelValues(metricProxyNode(in.Base.SourceID), MetricRequestsTotal).Inc()
	code := c.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("state code = %s", internalpb.StateCode_name[int32(code)]),
		}, nil
	}
	log.Debug("DropCollection", zap.String("name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID))
	t := &DropCollectionReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			cv:   make(chan error, 1),
			core: c,
		},
		Req: in,
	}
	c.ddReqQueue <- t
	err := t.WaitToFinish()
	if err != nil {
		log.Debug("DropCollection Failed", zap.String("name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "Drop collection failed: " + err.Error(),
		}, nil
	}
	log.Debug("DropCollection Success", zap.String("name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID))
	metrics.MasterDropCollectionCounter.WithLabelValues(metricProxyNode(in.Base.SourceID), MetricRequestsSuccess).Inc()
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (c *Core) HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	metrics.MasterHasCollectionCounter.WithLabelValues(metricProxyNode(in.Base.SourceID), MetricRequestsTotal).Inc()
	code := c.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("state code = %s", internalpb.StateCode_name[int32(code)]),
			},
			Value: false,
		}, nil
	}
	log.Debug("HasCollection", zap.String("name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID))
	t := &HasCollectionReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			cv:   make(chan error, 1),
			core: c,
		},
		Req:           in,
		HasCollection: false,
	}
	c.ddReqQueue <- t
	err := t.WaitToFinish()
	if err != nil {
		log.Debug("HasCollection Failed", zap.String("name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID))
		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "Has collection failed: " + err.Error(),
			},
			Value: false,
		}, nil
	}
	log.Debug("HasCollection Success", zap.String("name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID))
	metrics.MasterHasCollectionCounter.WithLabelValues(metricProxyNode(in.Base.SourceID), MetricRequestsSuccess).Inc()
	return &milvuspb.BoolResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: t.HasCollection,
	}, nil
}

func (c *Core) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	metrics.MasterDescribeCollectionCounter.WithLabelValues(metricProxyNode(in.Base.SourceID), MetricRequestsTotal).Inc()
	code := c.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("state code = %s", internalpb.StateCode_name[int32(code)]),
			},
			Schema:       nil,
			CollectionID: 0,
		}, nil
	}
	log.Debug("DescribeCollection", zap.String("name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID))
	t := &DescribeCollectionReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			cv:   make(chan error, 1),
			core: c,
		},
		Req: in,
		Rsp: &milvuspb.DescribeCollectionResponse{},
	}
	c.ddReqQueue <- t
	err := t.WaitToFinish()
	if err != nil {
		log.Debug("DescribeCollection Failed", zap.String("name", in.CollectionName), zap.Error(err), zap.Int64("msgID", in.Base.MsgID))
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "describe collection failed: " + err.Error(),
			},
			Schema: nil,
		}, nil
	}
	log.Debug("DescribeCollection Success", zap.String("name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID))
	metrics.MasterDescribeCollectionCounter.WithLabelValues(metricProxyNode(in.Base.SourceID), MetricRequestsSuccess).Inc()
	t.Rsp.Status = &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}
	// log.Debug("describe collection", zap.Any("schema", t.Rsp.Schema))
	return t.Rsp, nil
}

func (c *Core) ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	metrics.MasterShowCollectionsCounter.WithLabelValues(metricProxyNode(in.Base.SourceID), MetricRequestsTotal).Inc()
	code := c.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		return &milvuspb.ShowCollectionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("state code = %s", internalpb.StateCode_name[int32(code)]),
			},
			CollectionNames: nil,
		}, nil
	}
	log.Debug("ShowCollections", zap.String("dbname", in.DbName), zap.Int64("msgID", in.Base.MsgID))
	t := &ShowCollectionReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			cv:   make(chan error, 1),
			core: c,
		},
		Req: in,
		Rsp: &milvuspb.ShowCollectionsResponse{
			CollectionNames: nil,
			CollectionIds:   nil,
		},
	}
	c.ddReqQueue <- t
	err := t.WaitToFinish()
	if err != nil {
		log.Debug("ShowCollections failed", zap.String("dbname", in.DbName), zap.Int64("msgID", in.Base.MsgID))
		return &milvuspb.ShowCollectionsResponse{
			CollectionNames: nil,
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "ShowCollections failed: " + err.Error(),
			},
		}, nil
	}
	log.Debug("ShowCollections Success", zap.String("dbname", in.DbName), zap.Strings("collection names", t.Rsp.CollectionNames), zap.Int64("msgID", in.Base.MsgID))
	metrics.MasterShowCollectionsCounter.WithLabelValues(metricProxyNode(in.Base.SourceID), MetricRequestsSuccess).Inc()
	t.Rsp.Status = &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}
	return t.Rsp, nil
}

func (c *Core) CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	metrics.MasterCreatePartitionCounter.WithLabelValues(metricProxyNode(in.Base.SourceID), MetricRequestsTotal).Inc()
	code := c.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("state code = %s", internalpb.StateCode_name[int32(code)]),
		}, nil
	}
	log.Debug("CreatePartition", zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName), zap.Int64("msgID", in.Base.MsgID))
	t := &CreatePartitionReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			cv:   make(chan error, 1),
			core: c,
		},
		Req: in,
	}
	c.ddReqQueue <- t
	err := t.WaitToFinish()
	if err != nil {
		log.Debug("CreatePartition Failed", zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName), zap.Int64("msgID", in.Base.MsgID))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "create partition failed: " + err.Error(),
		}, nil
	}
	log.Debug("CreatePartition Success", zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName), zap.Int64("msgID", in.Base.MsgID))
	metrics.MasterCreatePartitionCounter.WithLabelValues(metricProxyNode(in.Base.SourceID), MetricRequestsSuccess).Inc()
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (c *Core) DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	metrics.MasterDropPartitionCounter.WithLabelValues(metricProxyNode(in.Base.SourceID), MetricRequestsTotal).Inc()
	code := c.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("state code = %s", internalpb.StateCode_name[int32(code)]),
		}, nil
	}
	log.Debug("DropPartition", zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName), zap.Int64("msgID", in.Base.MsgID))
	t := &DropPartitionReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			cv:   make(chan error, 1),
			core: c,
		},
		Req: in,
	}
	c.ddReqQueue <- t
	err := t.WaitToFinish()
	if err != nil {
		log.Debug("DropPartition Failed", zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName), zap.Int64("msgID", in.Base.MsgID))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "DropPartition failed: " + err.Error(),
		}, nil
	}
	log.Debug("DropPartition Success", zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName), zap.Int64("msgID", in.Base.MsgID))
	metrics.MasterDropPartitionCounter.WithLabelValues(metricProxyNode(in.Base.SourceID), MetricRequestsSuccess).Inc()
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (c *Core) HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	metrics.MasterHasPartitionCounter.WithLabelValues(metricProxyNode(in.Base.SourceID), MetricRequestsTotal).Inc()
	code := c.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("state code = %s", internalpb.StateCode_name[int32(code)]),
			},
			Value: false,
		}, nil
	}
	log.Debug("HasPartition", zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName), zap.Int64("msgID", in.Base.MsgID))
	t := &HasPartitionReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			cv:   make(chan error, 1),
			core: c,
		},
		Req:          in,
		HasPartition: false,
	}
	c.ddReqQueue <- t
	err := t.WaitToFinish()
	if err != nil {
		log.Debug("HasPartition Failed", zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName), zap.Int64("msgID", in.Base.MsgID))
		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "HasPartition failed: " + err.Error(),
			},
			Value: false,
		}, nil
	}
	log.Debug("HasPartition Success", zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName), zap.Int64("msgID", in.Base.MsgID))
	metrics.MasterHasPartitionCounter.WithLabelValues(metricProxyNode(in.Base.SourceID), MetricRequestsSuccess).Inc()
	return &milvuspb.BoolResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: t.HasPartition,
	}, nil
}

func (c *Core) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	metrics.MasterShowPartitionsCounter.WithLabelValues(metricProxyNode(in.Base.SourceID), MetricRequestsTotal).Inc()
	log.Debug("ShowPartitionRequest received", zap.String("role", Params.RoleName), zap.Int64("msgID", in.Base.MsgID),
		zap.String("collection", in.CollectionName))
	code := c.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		log.Debug("ShowPartitionRequest failed: master is not healthy", zap.String("role", Params.RoleName),
			zap.Int64("msgID", in.Base.MsgID), zap.String("state", internalpb.StateCode_name[int32(code)]))
		return &milvuspb.ShowPartitionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("master is not healthy, state code = %s", internalpb.StateCode_name[int32(code)]),
			},
			PartitionNames: nil,
			PartitionIDs:   nil,
		}, nil
	}
	t := &ShowPartitionReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			cv:   make(chan error, 1),
			core: c,
		},
		Req: in,
		Rsp: &milvuspb.ShowPartitionsResponse{
			PartitionNames: nil,
			Status:         nil,
		},
	}
	c.ddReqQueue <- t
	err := t.WaitToFinish()
	if err != nil {
		log.Debug("ShowPartitionsRequest failed", zap.String("role", Params.RoleName), zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		return &milvuspb.ShowPartitionsResponse{
			PartitionNames: nil,
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}
	log.Debug("ShowPartitions succeed", zap.String("role", Params.RoleName), zap.Int64("msgID", t.Req.Base.MsgID),
		zap.String("collection name", in.CollectionName), zap.Strings("partition names", t.Rsp.PartitionNames),
		zap.Int64s("partition ids", t.Rsp.PartitionIDs))
	metrics.MasterShowPartitionsCounter.WithLabelValues(metricProxyNode(in.Base.SourceID), MetricRequestsSuccess).Inc()
	t.Rsp.Status = &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}
	return t.Rsp, nil
}

func (c *Core) CreateIndex(ctx context.Context, in *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	metrics.MasterCreateIndexCounter.WithLabelValues(metricProxyNode(in.Base.SourceID), MetricRequestsTotal).Inc()
	code := c.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("state code = %s", internalpb.StateCode_name[int32(code)]),
		}, nil
	}
	log.Debug("CreateIndex", zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName), zap.Int64("msgID", in.Base.MsgID))
	t := &CreateIndexReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			cv:   make(chan error, 1),
			core: c,
		},
		Req: in,
	}
	c.ddReqQueue <- t
	err := t.WaitToFinish()
	if err != nil {
		log.Debug("CreateIndex Failed", zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName), zap.Int64("msgID", in.Base.MsgID))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "CreateIndex failed, error = " + err.Error(),
		}, nil
	}
	log.Debug("CreateIndex Success", zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName), zap.Int64("msgID", in.Base.MsgID))
	metrics.MasterCreateIndexCounter.WithLabelValues(metricProxyNode(in.Base.SourceID), MetricRequestsSuccess).Inc()
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (c *Core) DescribeIndex(ctx context.Context, in *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	metrics.MasterDescribeIndexCounter.WithLabelValues(metricProxyNode(in.Base.SourceID), MetricRequestsTotal).Inc()
	code := c.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		return &milvuspb.DescribeIndexResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("state code = %s", internalpb.StateCode_name[int32(code)]),
			},
			IndexDescriptions: nil,
		}, nil
	}
	log.Debug("DescribeIndex", zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName), zap.Int64("msgID", in.Base.MsgID))
	t := &DescribeIndexReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			cv:   make(chan error, 1),
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
		log.Debug("DescribeIndex Failed", zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName), zap.Int64("msgID", in.Base.MsgID))
		return &milvuspb.DescribeIndexResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "DescribeIndex failed, error = " + err.Error(),
			},
			IndexDescriptions: nil,
		}, nil
	}
	idxNames := make([]string, 0, len(t.Rsp.IndexDescriptions))
	for _, i := range t.Rsp.IndexDescriptions {
		idxNames = append(idxNames, i.IndexName)
	}
	log.Debug("DescribeIndex Success", zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName), zap.Strings("index names", idxNames), zap.Int64("msgID", in.Base.MsgID))
	metrics.MasterDescribeIndexCounter.WithLabelValues(metricProxyNode(in.Base.SourceID), MetricRequestsSuccess).Inc()
	if len(t.Rsp.IndexDescriptions) == 0 {
		t.Rsp.Status = &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_IndexNotExist,
			Reason:    "index not exist",
		}
	} else {
		t.Rsp.Status = &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		}
	}
	return t.Rsp, nil
}

func (c *Core) DropIndex(ctx context.Context, in *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	metrics.MasterDropIndexCounter.WithLabelValues(metricProxyNode(in.Base.SourceID), MetricRequestsTotal).Inc()
	code := c.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("state code = %s", internalpb.StateCode_name[int32(code)]),
		}, nil
	}
	log.Debug("DropIndex", zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName), zap.String("index name", in.IndexName), zap.Int64("msgID", in.Base.MsgID))
	t := &DropIndexReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			cv:   make(chan error, 1),
			core: c,
		},
		Req: in,
	}
	c.ddReqQueue <- t
	err := t.WaitToFinish()
	if err != nil {
		log.Debug("DropIndex Failed", zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName), zap.String("index name", in.IndexName), zap.Int64("msgID", in.Base.MsgID))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "DropIndex failed, error = " + err.Error(),
		}, nil
	}
	log.Debug("DropIndex Success", zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName), zap.String("index name", in.IndexName), zap.Int64("msgID", in.Base.MsgID))
	metrics.MasterDropIndexCounter.WithLabelValues(metricProxyNode(in.Base.SourceID), MetricRequestsSuccess).Inc()
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (c *Core) DescribeSegment(ctx context.Context, in *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error) {
	metrics.MasterDescribeSegmentCounter.WithLabelValues(metricProxyNode(in.Base.SourceID), MetricRequestsTotal).Inc()
	code := c.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		return &milvuspb.DescribeSegmentResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("state code = %s", internalpb.StateCode_name[int32(code)]),
			},
			IndexID: 0,
		}, nil
	}
	log.Debug("DescribeSegment", zap.Int64("collection id", in.CollectionID), zap.Int64("segment id", in.SegmentID), zap.Int64("msgID", in.Base.MsgID))
	t := &DescribeSegmentReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			cv:   make(chan error, 1),
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
		log.Debug("DescribeSegment Failed", zap.Int64("collection id", in.CollectionID), zap.Int64("segment id", in.SegmentID), zap.Int64("msgID", in.Base.MsgID))
		return &milvuspb.DescribeSegmentResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "DescribeSegment failed, error = " + err.Error(),
			},
			IndexID: 0,
		}, nil
	}
	log.Debug("DescribeSegment Success", zap.Int64("collection id", in.CollectionID), zap.Int64("segment id", in.SegmentID), zap.Int64("msgID", in.Base.MsgID))
	metrics.MasterDescribeSegmentCounter.WithLabelValues(metricProxyNode(in.Base.SourceID), MetricRequestsSuccess).Inc()
	t.Rsp.Status = &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}
	return t.Rsp, nil
}

func (c *Core) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error) {
	metrics.MasterShowSegmentsCounter.WithLabelValues(metricProxyNode(in.Base.SourceID), MetricRequestsTotal).Inc()
	code := c.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		return &milvuspb.ShowSegmentsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("state code = %s", internalpb.StateCode_name[int32(code)]),
			},
			SegmentIDs: nil,
		}, nil
	}
	log.Debug("ShowSegments", zap.Int64("collection id", in.CollectionID), zap.Int64("partition id", in.PartitionID), zap.Int64("msgID", in.Base.MsgID))
	t := &ShowSegmentReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			cv:   make(chan error, 1),
			core: c,
		},
		Req: in,
		Rsp: &milvuspb.ShowSegmentsResponse{
			Status:     nil,
			SegmentIDs: nil,
		},
	}
	c.ddReqQueue <- t
	err := t.WaitToFinish()
	if err != nil {
		log.Debug("ShowSegments Failed", zap.Int64("collection id", in.CollectionID), zap.Int64("partition id", in.PartitionID), zap.Int64("msgID", in.Base.MsgID))
		return &milvuspb.ShowSegmentsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "ShowSegments failed, error: " + err.Error(),
			},
			SegmentIDs: nil,
		}, nil
	}
	log.Debug("ShowSegments Success", zap.Int64("collection id", in.CollectionID), zap.Int64("partition id", in.PartitionID), zap.Int64s("segments ids", t.Rsp.SegmentIDs), zap.Int64("msgID", in.Base.MsgID))
	metrics.MasterShowSegmentsCounter.WithLabelValues(metricProxyNode(in.Base.SourceID), MetricRequestsSuccess).Inc()
	t.Rsp.Status = &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}
	return t.Rsp, nil
}

func (c *Core) AllocTimestamp(ctx context.Context, in *masterpb.AllocTimestampRequest) (*masterpb.AllocTimestampResponse, error) {
	code := c.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		return &masterpb.AllocTimestampResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("state code = %s", internalpb.StateCode_name[int32(code)]),
			},
			Timestamp: 0,
			Count:     0,
		}, nil
	}
	ts, err := c.TSOAllocator(in.Count)
	if err != nil {
		log.Debug("AllocTimestamp failed", zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		return &masterpb.AllocTimestampResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "AllocTimestamp failed: " + err.Error(),
			},
			Timestamp: 0,
			Count:     0,
		}, nil
	}
	// log.Printf("AllocTimestamp : %d", ts)
	return &masterpb.AllocTimestampResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Timestamp: ts,
		Count:     in.Count,
	}, nil
}

func (c *Core) AllocID(ctx context.Context, in *masterpb.AllocIDRequest) (*masterpb.AllocIDResponse, error) {
	code := c.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		return &masterpb.AllocIDResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("state code = %s", internalpb.StateCode_name[int32(code)]),
			},
			ID:    0,
			Count: 0,
		}, nil
	}
	start, _, err := c.IDAllocator(in.Count)
	if err != nil {
		log.Debug("AllocID failed", zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		return &masterpb.AllocIDResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "AllocID failed: " + err.Error(),
			},
			ID:    0,
			Count: in.Count,
		}, nil
	}
	log.Debug("AllocID", zap.Int64("id start", start), zap.Uint32("count", in.Count))
	return &masterpb.AllocIDResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		ID:    start,
		Count: in.Count,
	}, nil
}

// UpdateChannelTimeTick used to handle ChannelTimeTickMsg
func (c *Core) UpdateChannelTimeTick(ctx context.Context, in *internalpb.ChannelTimeTickMsg) (*commonpb.Status, error) {
	code := c.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("state code = %s", internalpb.StateCode_name[int32(code)]),
		}, nil
	}
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}
	if in.Base.MsgType != commonpb.MsgType_TimeTick {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = fmt.Sprintf("UpdateChannelTimeTick receive invalid message %d", in.Base.GetMsgType())
		return status, nil
	}
	err := c.chanTimeTick.UpdateTimeTick(in)
	if err != nil {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
		return status, nil
	}
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

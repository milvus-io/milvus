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

package rootcoord

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"

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
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/tso"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// ------------------ struct -----------------------

// DdOperation used to save ddMsg into ETCD
type DdOperation struct {
	Body []byte `json:"body"`
	Type string `json:"type"`
}

const (
	// MetricRequestsTotal used to count the num of total requests
	MetricRequestsTotal = "total"

	// MetricRequestsSuccess used to count the num of successful requests
	MetricRequestsSuccess = "success"
)

func metricProxy(v int64) string {
	return fmt.Sprintf("client_%d", v)
}

// Core root coordinator core
type Core struct {
	MetaTable *MetaTable
	//id allocator
	IDAllocator       func(count uint32) (typeutil.UniqueID, typeutil.UniqueID, error)
	IDAllocatorUpdate func() error

	//tso allocator
	TSOAllocator       func(count uint32) (typeutil.Timestamp, error)
	TSOAllocatorUpdate func() error

	//inner members
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	etcdCli *clientv3.Client
	kvBase  kv.TxnKV //*etcdkv.EtcdKV

	//DDL lock
	ddlLock sync.Mutex

	kvBaseCreate func(root string) (kv.TxnKV, error)

	//setMsgStreams, send time tick into dd channel and time tick channel
	SendTimeTick func(t typeutil.Timestamp, reason string) error

	//setMsgStreams, send create collection into dd channel
	//returns corresponding message id for each channel
	SendDdCreateCollectionReq func(ctx context.Context, req *internalpb.CreateCollectionRequest, channelNames []string) (map[string][]byte, error)

	//setMsgStreams, send drop collection into dd channel, and notify the proxy to delete this collection
	SendDdDropCollectionReq func(ctx context.Context, req *internalpb.DropCollectionRequest, channelNames []string) error

	//setMsgStreams, send create partition into dd channel
	SendDdCreatePartitionReq func(ctx context.Context, req *internalpb.CreatePartitionRequest, channelNames []string) error

	//setMsgStreams, send drop partition into dd channel
	SendDdDropPartitionReq func(ctx context.Context, req *internalpb.DropPartitionRequest, channelNames []string) error

	//get binlog file path from data service,
	CallGetBinlogFilePathsService func(ctx context.Context, segID typeutil.UniqueID, fieldID typeutil.UniqueID) ([]string, error)
	CallGetNumRowsService         func(ctx context.Context, segID typeutil.UniqueID, isFromFlushedChan bool) (int64, error)
	CallGetFlushedSegmentsService func(ctx context.Context, collID, partID typeutil.UniqueID) ([]typeutil.UniqueID, error)

	//call index builder's client to build index, return build id
	CallBuildIndexService func(ctx context.Context, binlog []string, field *schemapb.FieldSchema, idxInfo *etcdpb.IndexInfo) (typeutil.UniqueID, error)
	CallDropIndexService  func(ctx context.Context, indexID typeutil.UniqueID) error

	NewProxyClient func(sess *sessionutil.Session) (types.Proxy, error)

	//query service interface, notify query service to release collection
	CallReleaseCollectionService func(ctx context.Context, ts typeutil.Timestamp, dbID, collectionID typeutil.UniqueID) error
	CallReleasePartitionService  func(ctx context.Context, ts typeutil.Timestamp, dbID, collectionID typeutil.UniqueID, partitionIDs []typeutil.UniqueID) error

	//dml channels
	dmlChannels *dmlChannels

	//Proxy manager
	proxyManager *proxyManager

	// proxy clients
	proxyClientManager *proxyClientManager

	// metrics cache manager
	metricsCacheManager *metricsinfo.MetricsCacheManager

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

// NewCore create rootcoord core
func NewCore(c context.Context, factory ms.Factory) (*Core, error) {
	ctx, cancel := context.WithCancel(c)
	rand.Seed(time.Now().UnixNano())
	core := &Core{
		ctx:       ctx,
		cancel:    cancel,
		ddlLock:   sync.Mutex{},
		msFactory: factory,
	}
	core.UpdateStateCode(internalpb.StateCode_Abnormal)
	return core, nil
}

// UpdateStateCode update state code
func (c *Core) UpdateStateCode(code internalpb.StateCode) {
	c.stateCode.Store(code)
}

func (c *Core) isHealthy() bool {
	code := c.stateCode.Load().(internalpb.StateCode)
	return code == internalpb.StateCode_Healthy
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
	if c.CallGetFlushedSegmentsService == nil {
		return fmt.Errorf("CallGetFlushedSegments is nil")
	}
	if c.NewProxyClient == nil {
		return fmt.Errorf("NewProxyClient is nil")
	}
	if c.CallReleaseCollectionService == nil {
		return fmt.Errorf("CallReleaseCollectionService is nil")
	}
	if c.CallReleasePartitionService == nil {
		return fmt.Errorf("CallReleasePartitionService is nil")
	}

	return nil
}

func (c *Core) startTimeTickLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(time.Duration(Params.TimeTickInterval) * time.Millisecond)
	for {
		select {
		case <-c.ctx.Done():
			log.Debug("rootcoord context closed", zap.Error(c.ctx.Err()))
			return
		case <-ticker.C:
			c.ddlLock.Lock()
			if ts, err := c.TSOAllocator(1); err == nil {
				c.SendTimeTick(ts, "timetick loop")
			}
			c.ddlLock.Unlock()
		}
	}
}

func (c *Core) tsLoop() {
	defer c.wg.Done()
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
	defer c.wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			return
		case _, ok := <-c.sessCloseCh:
			if !ok {
				log.Error("rootcoord disconnect with etcd, process will exit in 1 second")
				go func() {
					time.Sleep(time.Second)
					os.Exit(-1)
				}()
				return
			}
		}
	}
}

func (c *Core) checkFlushedSegmentsLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(10 * time.Minute)
	for {
		select {
		case <-c.ctx.Done():
			log.Debug("RootCoord context done,exit checkFlushedSegmentsLoop")
			return
		case <-ticker.C:
			log.Debug("check flushed segments")
			c.checkFlushedSegments(c.ctx)
		}
	}
}

func (c *Core) checkFlushedSegments(ctx context.Context) {
	collID2Meta, segID2IndexMeta, indexID2Meta := c.MetaTable.dupMeta()
	for _, collMeta := range collID2Meta {
		if len(collMeta.FieldIndexes) == 0 {
			continue
		}
		for _, partID := range collMeta.PartitionIDs {
			ctx2, cancel2 := context.WithTimeout(ctx, 3*time.Minute)
			segIDs, err := c.CallGetFlushedSegmentsService(ctx2, collMeta.ID, partID)
			if err != nil {
				log.Debug("get flushed segments from data coord failed",
					zap.Int64("collection id", collMeta.ID),
					zap.Int64("partition id", partID),
					zap.Error(err))
			} else {
				for _, segID := range segIDs {
					indexInfos := []*etcdpb.FieldIndexInfo{}
					indexMeta, ok := segID2IndexMeta[segID]
					if !ok {
						indexInfos = append(indexInfos, collMeta.FieldIndexes...)
					} else {
						for _, idx := range collMeta.FieldIndexes {
							if _, ok := indexMeta[idx.IndexID]; !ok {
								indexInfos = append(indexInfos, idx)
							}
						}
					}
					for _, idxInfo := range indexInfos {
						/* #nosec G601 */
						field, err := GetFieldSchemaByID(&collMeta, idxInfo.FiledID)
						if err != nil {
							log.Debug("GetFieldSchemaByID",
								zap.Any("collection_meta", collMeta),
								zap.Int64("field id", idxInfo.FiledID))
							continue
						}
						indexMeta, ok := indexID2Meta[idxInfo.IndexID]
						if !ok {
							log.Debug("index meta not exist", zap.Int64("index_id", idxInfo.IndexID))
							continue
						}
						info := etcdpb.SegmentIndexInfo{
							CollectionID: collMeta.ID,
							PartitionID:  partID,
							SegmentID:    segID,
							FieldID:      idxInfo.FiledID,
							IndexID:      idxInfo.IndexID,
							EnableIndex:  false,
						}
						log.Debug("build index by background checker",
							zap.Int64("segment_id", segID),
							zap.Int64("index_id", indexMeta.IndexID),
							zap.Int64("collection_id", collMeta.ID))
						info.BuildID, err = c.BuildIndex(ctx2, segID, field, &indexMeta, false)
						if err != nil {
							log.Debug("build index failed",
								zap.Int64("segment_id", segID),
								zap.Int64("field_id", field.FieldID),
								zap.Int64("index_id", indexMeta.IndexID))
							continue
						}
						if info.BuildID != 0 {
							info.EnableIndex = true
						}
						ts, _ := c.TSOAllocator(1)
						if err := c.MetaTable.AddIndex(&info, ts); err != nil {
							log.Debug("Add index into meta table failed",
								zap.Int64("collection_id", collMeta.ID),
								zap.Int64("index_id", info.IndexID),
								zap.Int64("build_id", info.BuildID),
								zap.Error(err))
						}
					}
				}
			}
			cancel2()
		}
	}
}

func (c *Core) getSegments(ctx context.Context, collID typeutil.UniqueID) (map[typeutil.UniqueID]typeutil.UniqueID, error) {
	collMeta, err := c.MetaTable.GetCollectionByID(collID, 0)
	if err != nil {
		return nil, err
	}
	segID2PartID := map[typeutil.UniqueID]typeutil.UniqueID{}
	for _, partID := range collMeta.PartitionIDs {
		if seg, err := c.CallGetFlushedSegmentsService(ctx, collID, partID); err == nil {
			for _, s := range seg {
				segID2PartID[s] = partID
			}
		} else {
			log.Debug("get flushed segments from data coord failed", zap.Int64("collection_id", collID), zap.Int64("partition_id", partID), zap.Error(err))
			return nil, err
		}
	}

	return segID2PartID, nil
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

	ts, _ := c.TSOAllocator(1)
	err = c.MetaTable.client.Save(DDMsgSendPrefix, strconv.FormatBool(b), ts)
	return err
}

func (c *Core) setMsgStreams() error {
	if Params.PulsarAddress == "" {
		return fmt.Errorf("PulsarAddress is empty")
	}
	if Params.MsgChannelSubName == "" {
		return fmt.Errorf("MsgChannelSubName is emptyr")
	}

	// rootcoord time tick channel
	if Params.TimeTickChannel == "" {
		return fmt.Errorf("TimeTickChannel is empty")
	}
	timeTickStream, _ := c.msFactory.NewMsgStream(c.ctx)
	timeTickStream.AsProducer([]string{Params.TimeTickChannel})
	log.Debug("rootcoord AsProducer: " + Params.TimeTickChannel)

	c.SendTimeTick = func(t typeutil.Timestamp, reason string) error {
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
		metrics.RootCoordDDChannelTimeTick.Set(float64(tsoutil.Mod24H(t)))

		//c.dmlChannels.BroadcastAll(&msgPack)
		pc := c.dmlChannels.ListChannels()
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
			ChannelNames:     pc,
			Timestamps:       pt,
			DefaultTimestamp: t,
		}
		//log.Debug("update timetick",
		//	zap.Any("DefaultTs", t),
		//	zap.Any("sourceID", c.session.ServerID),
		//	zap.Any("reason", reason))
		return c.chanTimeTick.UpdateTimeTick(&ttMsg, reason)
	}

	c.SendDdCreateCollectionReq = func(ctx context.Context, req *internalpb.CreateCollectionRequest, channelNames []string) (map[string][]byte, error) {
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
		return c.dmlChannels.BroadcastMark(channelNames, &msgPack)
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
		return c.dmlChannels.Broadcast(channelNames, &msgPack)
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
		return c.dmlChannels.Broadcast(channelNames, &msgPack)
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
		return c.dmlChannels.Broadcast(channelNames, &msgPack)
	}

	return nil
}

// SetNewProxyClient set client to create proxy
func (c *Core) SetNewProxyClient(f func(sess *sessionutil.Session) (types.Proxy, error)) {
	if c.NewProxyClient == nil {
		c.NewProxyClient = f
	} else {
		log.Debug("NewProxyClient has already set")
	}
}

// SetDataCoord set datacoord
func (c *Core) SetDataCoord(ctx context.Context, s types.DataCoord) error {
	initCh := make(chan struct{})
	go func() {
		for {
			if err := s.Init(); err == nil {
				if err := s.Start(); err == nil {
					close(initCh)
					log.Debug("RootCoord connect to DataCoord")
					return
				}
			}
			log.Debug("RootCoord connect to DataCoord, retry")
		}
	}()
	c.CallGetBinlogFilePathsService = func(ctx context.Context, segID typeutil.UniqueID, fieldID typeutil.UniqueID) (retFiles []string, retErr error) {
		defer func() {
			if err := recover(); err != nil {
				retErr = fmt.Errorf("get bin log file paths panic, msg = %v", err)
			}
		}()
		<-initCh //wait connect to data coord
		ts, err := c.TSOAllocator(1)
		if err != nil {
			return nil, err
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
			return nil, err
		}
		if binlog.Status.ErrorCode != commonpb.ErrorCode_Success {
			return nil, fmt.Errorf("GetInsertBinlogPaths from data service failed, error = %s", binlog.Status.Reason)
		}
		for i := range binlog.FieldIDs {
			if binlog.FieldIDs[i] == fieldID {
				return binlog.Paths[i].Values, nil
			}
		}
		return nil, fmt.Errorf("binlog file not exist, segment id = %d, field id = %d", segID, fieldID)
	}

	c.CallGetNumRowsService = func(ctx context.Context, segID typeutil.UniqueID, isFromFlushedChan bool) (retRows int64, retErr error) {
		defer func() {
			if err := recover(); err != nil {
				retErr = fmt.Errorf("get num rows panic, msg = %v", err)
			}
		}()
		<-initCh
		ts, err := c.TSOAllocator(1)
		if err != nil {
			return retRows, err
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
			return retRows, err
		}
		if segInfo.Status.ErrorCode != commonpb.ErrorCode_Success {
			return retRows, fmt.Errorf("GetSegmentInfo from data service failed, error = %s", segInfo.Status.Reason)
		}
		if len(segInfo.Infos) != 1 {
			log.Debug("get segment info empty")
			return retRows, nil
		}
		if !isFromFlushedChan && segInfo.Infos[0].State != commonpb.SegmentState_Flushed {
			log.Debug("segment id not flushed", zap.Int64("segment id", segID))
			return retRows, nil
		}
		return segInfo.Infos[0].NumOfRows, nil
	}

	c.CallGetFlushedSegmentsService = func(ctx context.Context, collID, partID typeutil.UniqueID) (retSegIDs []typeutil.UniqueID, retErr error) {
		defer func() {
			if err := recover(); err != nil {
				retErr = fmt.Errorf("get flushed segments from data coord panic, msg = %v", err)
			}
		}()
		<-initCh
		req := &datapb.GetFlushedSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0, //TODO,msg type
				MsgID:     0,
				Timestamp: 0,
				SourceID:  c.session.ServerID,
			},
			CollectionID: collID,
			PartitionID:  partID,
		}
		rsp, err := s.GetFlushedSegments(ctx, req)
		if err != nil {
			return retSegIDs, err
		}
		if rsp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return retSegIDs, fmt.Errorf("get flushed segments from data coord failed, reason = %s", rsp.Status.Reason)
		}
		return rsp.Segments, nil
	}

	return nil
}

// SetIndexCoord set indexcoord
func (c *Core) SetIndexCoord(s types.IndexCoord) error {
	initCh := make(chan struct{})
	go func() {
		for {
			if err := s.Init(); err == nil {
				if err := s.Start(); err == nil {
					close(initCh)
					log.Debug("RootCoord connect to IndexCoord")
					return
				}
			}
			log.Debug("RootCoord connect to IndexCoord, retry")
		}
	}()

	c.CallBuildIndexService = func(ctx context.Context, binlog []string, field *schemapb.FieldSchema, idxInfo *etcdpb.IndexInfo) (retID typeutil.UniqueID, retErr error) {
		defer func() {
			if err := recover(); err != nil {
				retErr = fmt.Errorf("build index panic, msg = %v", err)
			}
		}()
		<-initCh
		rsp, err := s.BuildIndex(ctx, &indexpb.BuildIndexRequest{
			DataPaths:   binlog,
			TypeParams:  field.TypeParams,
			IndexParams: idxInfo.IndexParams,
			IndexID:     idxInfo.IndexID,
			IndexName:   idxInfo.IndexName,
		})
		if err != nil {
			return retID, err
		}
		if rsp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return retID, fmt.Errorf("BuildIndex from index service failed, error = %s", rsp.Status.Reason)
		}
		return rsp.IndexBuildID, nil
	}

	c.CallDropIndexService = func(ctx context.Context, indexID typeutil.UniqueID) (retErr error) {
		defer func() {
			if err := recover(); err != nil {
				retErr = fmt.Errorf("drop index from index service panic, msg = %v", err)
			}
		}()
		<-initCh
		rsp, err := s.DropIndex(ctx, &indexpb.DropIndexRequest{
			IndexID: indexID,
		})
		if err != nil {
			return err
		}
		if rsp.ErrorCode != commonpb.ErrorCode_Success {
			return fmt.Errorf(rsp.Reason)
		}
		return nil
	}

	return nil
}

// SetQueryCoord set querycoord
func (c *Core) SetQueryCoord(s types.QueryCoord) error {
	initCh := make(chan struct{})
	go func() {
		for {
			if err := s.Init(); err == nil {
				if err := s.Start(); err == nil {
					close(initCh)
					log.Debug("RootCoord connect to QueryCoord")
					return
				}
			}
			log.Debug("RootCoord connect to QueryCoord, retry")
		}
	}()
	c.CallReleaseCollectionService = func(ctx context.Context, ts typeutil.Timestamp, dbID typeutil.UniqueID, collectionID typeutil.UniqueID) (retErr error) {
		defer func() {
			if err := recover(); err != nil {
				retErr = fmt.Errorf("release collection from query service panic, msg = %v", err)
			}
		}()
		<-initCh
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
			return err
		}
		if rsp.ErrorCode != commonpb.ErrorCode_Success {
			return fmt.Errorf("ReleaseCollection from query service failed, error = %s", rsp.Reason)
		}
		return nil
	}
	c.CallReleasePartitionService = func(ctx context.Context, ts typeutil.Timestamp, dbID, collectionID typeutil.UniqueID, partitionIDs []typeutil.UniqueID) (retErr error) {
		defer func() {
			if err := recover(); err != nil {
				retErr = fmt.Errorf("release partition from query service panic, msg = %v", err)
			}
		}()
		<-initCh
		req := &querypb.ReleasePartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ReleasePartitions,
				MsgID:     0, //TODO, msg ID
				Timestamp: ts,
				SourceID:  c.session.ServerID,
			},
			DbID:         dbID,
			CollectionID: collectionID,
			PartitionIDs: partitionIDs,
		}
		rsp, err := s.ReleasePartitions(ctx, req)
		if err != nil {
			return err
		}
		if rsp.ErrorCode != commonpb.ErrorCode_Success {
			return fmt.Errorf("ReleasePartitions from query service failed, error = %s", rsp.Reason)
		}
		return nil
	}
	return nil
}

// BuildIndex will check row num and call build index service
func (c *Core) BuildIndex(ctx context.Context, segID typeutil.UniqueID, field *schemapb.FieldSchema, idxInfo *etcdpb.IndexInfo, isFlush bool) (typeutil.UniqueID, error) {
	sp, ctx := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	if c.MetaTable.IsSegmentIndexed(segID, field, idxInfo.IndexParams) {
		return 0, nil
	}
	rows, err := c.CallGetNumRowsService(ctx, segID, isFlush)
	if err != nil {
		return 0, err
	}
	var bldID typeutil.UniqueID
	if rows < Params.MinSegmentSizeToEnableIndex {
		log.Debug("num of rows is less than MinSegmentSizeToEnableIndex", zap.Int64("num rows", rows))
	} else {
		binlogs, err := c.CallGetBinlogFilePathsService(ctx, segID, field.FieldID)
		if err != nil {
			return 0, err
		}
		bldID, err = c.CallBuildIndexService(ctx, binlogs, field, idxInfo)
		if err != nil {
			return 0, err
		}
	}
	log.Debug("build index", zap.String("index name", idxInfo.IndexName),
		zap.String("field name", field.Name), zap.Int64("segment id", segID), zap.Int64("num rows", rows))
	return bldID, nil
}

// Register register rootcoord at etcd
func (c *Core) Register() error {
	c.session = sessionutil.NewSession(c.ctx, Params.MetaRootPath, Params.EtcdEndpoints)
	if c.session == nil {
		return fmt.Errorf("session is nil, maybe the etcd client connection fails")
	}
	c.sessCloseCh = c.session.Init(typeutil.RootCoordRole, Params.Address, true)
	return nil
}

// Init initialize routine
func (c *Core) Init() error {
	var initError error = nil
	if c.kvBaseCreate == nil {
		c.kvBaseCreate = func(root string) (kv.TxnKV, error) {
			return etcdkv.NewEtcdKV(Params.EtcdEndpoints, root)
		}
	}
	c.initOnce.Do(func() {
		connectEtcdFn := func() error {
			if c.etcdCli, initError = clientv3.New(clientv3.Config{Endpoints: Params.EtcdEndpoints, DialTimeout: 5 * time.Second}); initError != nil {
				log.Error("RootCoord, Failed to new Etcd client", zap.Any("reason", initError))
				return initError
			}
			if c.kvBase, initError = c.kvBaseCreate(Params.KvRootPath); initError != nil {
				log.Error("RootCoord, Failed to new EtcdKV", zap.Any("reason", initError))
				return initError
			}
			var metaKV kv.TxnKV
			metaKV, initError = c.kvBaseCreate(Params.MetaRootPath)
			if initError != nil {
				log.Error("RootCoord, Failed to new EtcdKV", zap.Any("reason", initError))
				return initError
			}
			var ss *suffixSnapshot
			if ss, initError = newSuffixSnapshot(metaKV, "_ts", Params.MetaRootPath, "snapshots"); initError != nil {
				log.Error("RootCoord, Failed to new suffixSnapshot", zap.Error(initError))
				return initError
			}
			if c.MetaTable, initError = NewMetaTable(ss); initError != nil {
				log.Error("RootCoord, Failed to new MetaTable", zap.Any("reason", initError))
				return initError
			}

			return nil
		}
		log.Debug("RootCoord, Connect to Etcd")
		err := retry.Do(c.ctx, connectEtcdFn, retry.Attempts(300))
		if err != nil {
			return
		}

		log.Debug("RootCoord, Set TSO and ID Allocator")
		kv, initError := tsoutil.NewTSOKVBase(Params.EtcdEndpoints, Params.KvRootPath, "gid")
		if initError != nil {
			return
		}
		idAllocator := allocator.NewGlobalIDAllocator("idTimestamp", kv)
		if initError = idAllocator.Initialize(); initError != nil {
			return
		}
		c.IDAllocator = func(count uint32) (typeutil.UniqueID, typeutil.UniqueID, error) {
			return idAllocator.Alloc(count)
		}
		c.IDAllocatorUpdate = func() error {
			return idAllocator.UpdateID()
		}

		kv, initError = tsoutil.NewTSOKVBase(Params.EtcdEndpoints, Params.KvRootPath, "tso")
		if initError != nil {
			return
		}
		tsoAllocator := tso.NewGlobalTSOAllocator("timestamp", kv)
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

		c.dmlChannels = newDmlChannels(c, Params.DmlChannelName, Params.DmlChannelNum)

		// recover physical channels for all collections
		pc := c.MetaTable.ListCollectionPhysicalChannels()
		c.dmlChannels.AddProducerChannels(pc...)
		log.Debug("recover all physical channels", zap.Any("chanNames", pc))

		c.chanTimeTick = newTimeTickSync(c)
		c.chanTimeTick.AddProxy(c.session)
		c.proxyClientManager = newProxyClientManager(c)

		log.Debug("RootCoord, set proxy manager")
		c.proxyManager, initError = newProxyManager(
			c.ctx,
			Params.EtcdEndpoints,
			c.chanTimeTick.GetProxy,
			c.proxyClientManager.GetProxyClients,
		)
		if initError != nil {
			return
		}
		c.proxyManager.AddSession(c.chanTimeTick.AddProxy, c.proxyClientManager.AddProxyClient)
		c.proxyManager.DelSession(c.chanTimeTick.DelProxy, c.proxyClientManager.DelProxyClient)

		c.metricsCacheManager = metricsinfo.NewMetricsCacheManager()

		initError = c.setMsgStreams()
		if initError != nil {
			return
		}
	})
	if initError == nil {
		log.Debug(typeutil.RootCoordRole, zap.String("State Code", internalpb.StateCode_name[int32(internalpb.StateCode_Initializing)]))
	} else {
		log.Debug("RootCoord init error", zap.Error(initError))
	}
	return initError
}

func (c *Core) reSendDdMsg(ctx context.Context, force bool) error {
	if !force {
		flag, err := c.MetaTable.client.Load(DDMsgSendPrefix, 0)
		if err != nil || flag == "true" {
			log.Debug("No un-successful DdMsg")
			return nil
		}
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

	var invalidateCache bool
	var ts typeutil.Timestamp
	var dbName, collName string

	switch ddOp.Type {
	// TODO remove create collection resend
	// since create collection needs a start position to succeed
	case CreateCollectionDDType:
		var ddReq = internalpb.CreateCollectionRequest{}
		if err = proto.Unmarshal(ddOp.Body, &ddReq); err != nil {
			return err
		}
		collInfo, err := c.MetaTable.GetCollectionByName(ddReq.CollectionName, 0)
		if err != nil {
			return err
		}
		if _, err = c.SendDdCreateCollectionReq(ctx, &ddReq, collInfo.PhysicalChannelNames); err != nil {
			return err
		}
		invalidateCache = false
	case DropCollectionDDType:
		var ddReq = internalpb.DropCollectionRequest{}
		if err = proto.Unmarshal(ddOp.Body, &ddReq); err != nil {
			return err
		}
		ts = ddReq.Base.Timestamp
		dbName, collName = ddReq.DbName, ddReq.CollectionName
		collInfo, err := c.MetaTable.GetCollectionByName(ddReq.CollectionName, 0)
		if err != nil {
			return err
		}
		if err = c.SendDdDropCollectionReq(ctx, &ddReq, collInfo.PhysicalChannelNames); err != nil {
			return err
		}
		invalidateCache = true
	case CreatePartitionDDType:
		var ddReq = internalpb.CreatePartitionRequest{}
		if err = proto.Unmarshal(ddOp.Body, &ddReq); err != nil {
			return err
		}
		ts = ddReq.Base.Timestamp
		dbName, collName = ddReq.DbName, ddReq.CollectionName
		collInfo, err := c.MetaTable.GetCollectionByName(ddReq.CollectionName, 0)
		if err != nil {
			return err
		}
		if _, err = c.MetaTable.GetPartitionByName(collInfo.ID, ddReq.PartitionName, 0); err == nil {
			return fmt.Errorf("partition %s already created", ddReq.PartitionName)
		}
		if err = c.SendDdCreatePartitionReq(ctx, &ddReq, collInfo.PhysicalChannelNames); err != nil {
			return err
		}
		invalidateCache = true
	case DropPartitionDDType:
		var ddReq = internalpb.DropPartitionRequest{}
		if err = proto.Unmarshal(ddOp.Body, &ddReq); err != nil {
			return err
		}
		ts = ddReq.Base.Timestamp
		dbName, collName = ddReq.DbName, ddReq.CollectionName
		collInfo, err := c.MetaTable.GetCollectionByName(ddReq.CollectionName, 0)
		if err != nil {
			return err
		}
		if _, err = c.MetaTable.GetPartitionByName(collInfo.ID, ddReq.PartitionName, 0); err != nil {
			return err
		}
		if err = c.SendDdDropPartitionReq(ctx, &ddReq, collInfo.PhysicalChannelNames); err != nil {
			return err
		}
		invalidateCache = true
	default:
		return fmt.Errorf("Invalid DdOperation %s", ddOp.Type)
	}

	if invalidateCache {
		req := proxypb.InvalidateCollMetaCacheRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0, //TODO, msg type
				MsgID:     0, //TODO, msg id
				Timestamp: ts,
				SourceID:  c.session.ServerID,
			},
			DbName:         dbName,
			CollectionName: collName,
		}
		c.proxyClientManager.InvalidateCollectionMetaCache(c.ctx, &req)
	}

	// Update DDOperation in etcd
	return c.setDdMsgSendFlag(true)
}

// Start start rootcoord
func (c *Core) Start() error {
	if err := c.checkInit(); err != nil {
		log.Debug("RootCoord Start checkInit failed", zap.Error(err))
		return err
	}

	log.Debug(typeutil.RootCoordRole, zap.Int64("node id", c.session.ServerID))
	log.Debug(typeutil.RootCoordRole, zap.String("time tick channel name", Params.TimeTickChannel))

	c.startOnce.Do(func() {
		if err := c.proxyManager.WatchProxy(); err != nil {
			log.Debug("RootCoord Start WatchProxy failed", zap.Error(err))
			return
		}
		if err := c.reSendDdMsg(c.ctx, false); err != nil {
			log.Debug("RootCoord Start reSendDdMsg failed", zap.Error(err))
			return
		}
		c.wg.Add(5)
		go c.startTimeTickLoop()
		go c.tsLoop()
		go c.sessionLoop()
		go c.chanTimeTick.StartWatch(&c.wg)
		go c.checkFlushedSegmentsLoop()

		Params.CreatedTime = time.Now()
		Params.UpdatedTime = time.Now()

		c.stateCode.Store(internalpb.StateCode_Healthy)
	})

	log.Debug(typeutil.RootCoordRole, zap.String("State Code", internalpb.StateCode_name[int32(internalpb.StateCode_Healthy)]))

	return nil
}

// Stop stop rootcoord
func (c *Core) Stop() error {
	c.cancel()
	c.wg.Wait()
	c.stateCode.Store(internalpb.StateCode_Abnormal)
	return nil
}

// GetComponentStates get states of components
func (c *Core) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	code := c.stateCode.Load().(internalpb.StateCode)
	log.Debug("GetComponentStates", zap.String("State Code", internalpb.StateCode_name[int32(code)]))

	return &internalpb.ComponentStates{
		State: &internalpb.ComponentInfo{
			NodeID:    c.session.ServerID,
			Role:      typeutil.RootCoordRole,
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
				Role:      typeutil.RootCoordRole,
				StateCode: code,
				ExtraInfo: nil,
			},
		},
	}, nil
}

// GetTimeTickChannel get timetick channel name
func (c *Core) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: Params.TimeTickChannel,
	}, nil
}

// GetStatisticsChannel get statistics channel name
func (c *Core) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: Params.StatisticsChannel,
	}, nil
}

// CreateCollection create collection
func (c *Core) CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	metrics.RootCoordCreateCollectionCounter.WithLabelValues(metricProxy(in.Base.SourceID), MetricRequestsTotal).Inc()
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
			core: c,
		},
		Req: in,
	}
	err := executeTask(t)
	if err != nil {
		log.Debug("CreateCollection failed", zap.String("name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "Create collection failed: " + err.Error(),
		}, nil
	}
	log.Debug("CreateCollection Success", zap.String("name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID))
	metrics.RootCoordCreateCollectionCounter.WithLabelValues(metricProxy(in.Base.SourceID), MetricRequestsSuccess).Inc()
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

// DropCollection drop collection
func (c *Core) DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	metrics.RootCoordDropCollectionCounter.WithLabelValues(metricProxy(in.Base.SourceID), MetricRequestsTotal).Inc()
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
			core: c,
		},
		Req: in,
	}
	err := executeTask(t)
	if err != nil {
		log.Warn("DropCollection Failed", zap.String("name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "Drop collection failed: " + err.Error(),
		}, nil
	}
	log.Debug("DropCollection Success", zap.String("name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID))
	metrics.RootCoordDropCollectionCounter.WithLabelValues(metricProxy(in.Base.SourceID), MetricRequestsSuccess).Inc()
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

// HasCollection check collection existence
func (c *Core) HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	metrics.RootCoordHasCollectionCounter.WithLabelValues(metricProxy(in.Base.SourceID), MetricRequestsTotal).Inc()
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
			core: c,
		},
		Req:           in,
		HasCollection: false,
	}
	err := executeTask(t)
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
	metrics.RootCoordHasCollectionCounter.WithLabelValues(metricProxy(in.Base.SourceID), MetricRequestsSuccess).Inc()
	return &milvuspb.BoolResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: t.HasCollection,
	}, nil
}

// DescribeCollection return collection info
func (c *Core) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	metrics.RootCoordDescribeCollectionCounter.WithLabelValues(metricProxy(in.Base.SourceID), MetricRequestsTotal).Inc()
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
			core: c,
		},
		Req: in,
		Rsp: &milvuspb.DescribeCollectionResponse{},
	}
	err := executeTask(t)
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
	metrics.RootCoordDescribeCollectionCounter.WithLabelValues(metricProxy(in.Base.SourceID), MetricRequestsSuccess).Inc()
	t.Rsp.Status = &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}
	// log.Debug("describe collection", zap.Any("schema", t.Rsp.Schema))
	return t.Rsp, nil
}

// ShowCollections list all collection names
func (c *Core) ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	metrics.RootCoordShowCollectionsCounter.WithLabelValues(metricProxy(in.Base.SourceID), MetricRequestsTotal).Inc()
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
			core: c,
		},
		Req: in,
		Rsp: &milvuspb.ShowCollectionsResponse{
			CollectionNames: nil,
			CollectionIds:   nil,
		},
	}
	err := executeTask(t)
	if err != nil {
		log.Warn("ShowCollections failed", zap.String("dbname", in.DbName), zap.Int64("msgID", in.Base.MsgID),
			zap.Error(err))
		return &milvuspb.ShowCollectionsResponse{
			CollectionNames: nil,
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "ShowCollections failed: " + err.Error(),
			},
		}, nil
	}
	log.Debug("ShowCollections Success", zap.String("dbname", in.DbName), zap.Strings("collection names", t.Rsp.CollectionNames), zap.Int64("msgID", in.Base.MsgID))
	metrics.RootCoordShowCollectionsCounter.WithLabelValues(metricProxy(in.Base.SourceID), MetricRequestsSuccess).Inc()
	t.Rsp.Status = &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}
	return t.Rsp, nil
}

// CreatePartition create partition
func (c *Core) CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	metrics.RootCoordCreatePartitionCounter.WithLabelValues(metricProxy(in.Base.SourceID), MetricRequestsTotal).Inc()
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
			core: c,
		},
		Req: in,
	}
	err := executeTask(t)
	if err != nil {
		log.Warn("CreatePartition Failed", zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName), zap.Int64("msgID", in.Base.MsgID),
			zap.Error(err))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "create partition failed: " + err.Error(),
		}, nil
	}
	log.Debug("CreatePartition Success", zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName), zap.Int64("msgID", in.Base.MsgID))
	metrics.RootCoordCreatePartitionCounter.WithLabelValues(metricProxy(in.Base.SourceID), MetricRequestsSuccess).Inc()
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

// DropPartition drop partition
func (c *Core) DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	metrics.RootCoordDropPartitionCounter.WithLabelValues(metricProxy(in.Base.SourceID), MetricRequestsTotal).Inc()
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
			core: c,
		},
		Req: in,
	}
	err := executeTask(t)
	if err != nil {
		log.Warn("DropPartition Failed", zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName), zap.Int64("msgID", in.Base.MsgID),
			zap.Error(err))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "DropPartition failed: " + err.Error(),
		}, nil
	}
	log.Debug("DropPartition Success", zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName), zap.Int64("msgID", in.Base.MsgID))
	metrics.RootCoordDropPartitionCounter.WithLabelValues(metricProxy(in.Base.SourceID), MetricRequestsSuccess).Inc()
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

// HasPartition check partition existence
func (c *Core) HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	metrics.RootCoordHasPartitionCounter.WithLabelValues(metricProxy(in.Base.SourceID), MetricRequestsTotal).Inc()
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
			core: c,
		},
		Req:          in,
		HasPartition: false,
	}
	err := executeTask(t)
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
	metrics.RootCoordHasPartitionCounter.WithLabelValues(metricProxy(in.Base.SourceID), MetricRequestsSuccess).Inc()
	return &milvuspb.BoolResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: t.HasPartition,
	}, nil
}

// ShowPartitions list all partition names
func (c *Core) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	metrics.RootCoordShowPartitionsCounter.WithLabelValues(metricProxy(in.Base.SourceID), MetricRequestsTotal).Inc()
	log.Debug("ShowPartitionRequest received", zap.String("role", Params.RoleName), zap.Int64("msgID", in.Base.MsgID),
		zap.String("collection", in.CollectionName))
	code := c.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		log.Debug("ShowPartitionRequest failed: rootcoord is not healthy", zap.String("role", Params.RoleName),
			zap.Int64("msgID", in.Base.MsgID), zap.String("state", internalpb.StateCode_name[int32(code)]))
		return &milvuspb.ShowPartitionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("rootcoord is not healthy, state code = %s", internalpb.StateCode_name[int32(code)]),
			},
			PartitionNames: nil,
			PartitionIDs:   nil,
		}, nil
	}
	t := &ShowPartitionReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			core: c,
		},
		Req: in,
		Rsp: &milvuspb.ShowPartitionsResponse{
			PartitionNames: nil,
			Status:         nil,
		},
	}
	err := executeTask(t)
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
		zap.String("collection name", in.CollectionName), zap.Int("num of partitions", len(t.Rsp.PartitionNames)))
	metrics.RootCoordShowPartitionsCounter.WithLabelValues(metricProxy(in.Base.SourceID), MetricRequestsSuccess).Inc()
	t.Rsp.Status = &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}
	return t.Rsp, nil
}

// CreateIndex create index
func (c *Core) CreateIndex(ctx context.Context, in *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	metrics.RootCoordCreateIndexCounter.WithLabelValues(metricProxy(in.Base.SourceID), MetricRequestsTotal).Inc()
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
			core: c,
		},
		Req: in,
	}
	err := executeTask(t)
	if err != nil {
		log.Debug("CreateIndex Failed", zap.String("collection name", in.CollectionName),
			zap.String("field name", in.FieldName), zap.Int64("msgID", in.Base.MsgID),
			zap.Error(err))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "CreateIndex failed, error = " + err.Error(),
		}, nil
	}
	log.Debug("CreateIndex Success", zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName), zap.Int64("msgID", in.Base.MsgID))
	metrics.RootCoordCreateIndexCounter.WithLabelValues(metricProxy(in.Base.SourceID), MetricRequestsSuccess).Inc()
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

// DescribeIndex return index info
func (c *Core) DescribeIndex(ctx context.Context, in *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	metrics.RootCoordDescribeIndexCounter.WithLabelValues(metricProxy(in.Base.SourceID), MetricRequestsTotal).Inc()
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
			core: c,
		},
		Req: in,
		Rsp: &milvuspb.DescribeIndexResponse{
			Status:            nil,
			IndexDescriptions: nil,
		},
	}
	err := executeTask(t)
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
	metrics.RootCoordDescribeIndexCounter.WithLabelValues(metricProxy(in.Base.SourceID), MetricRequestsSuccess).Inc()
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

// DropIndex drop index
func (c *Core) DropIndex(ctx context.Context, in *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	metrics.RootCoordDropIndexCounter.WithLabelValues(metricProxy(in.Base.SourceID), MetricRequestsTotal).Inc()
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
			core: c,
		},
		Req: in,
	}
	err := executeTask(t)
	if err != nil {
		log.Debug("DropIndex Failed", zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName), zap.String("index name", in.IndexName), zap.Int64("msgID", in.Base.MsgID))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "DropIndex failed, error = " + err.Error(),
		}, nil
	}
	log.Debug("DropIndex Success", zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName), zap.String("index name", in.IndexName), zap.Int64("msgID", in.Base.MsgID))
	metrics.RootCoordDropIndexCounter.WithLabelValues(metricProxy(in.Base.SourceID), MetricRequestsSuccess).Inc()
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

// DescribeSegment return segment info
func (c *Core) DescribeSegment(ctx context.Context, in *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error) {
	metrics.RootCoordDescribeSegmentCounter.WithLabelValues(metricProxy(in.Base.SourceID), MetricRequestsTotal).Inc()
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
			core: c,
		},
		Req: in,
		Rsp: &milvuspb.DescribeSegmentResponse{
			Status:  nil,
			IndexID: 0,
		},
	}
	err := executeTask(t)
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
	metrics.RootCoordDescribeSegmentCounter.WithLabelValues(metricProxy(in.Base.SourceID), MetricRequestsSuccess).Inc()
	t.Rsp.Status = &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}
	return t.Rsp, nil
}

// ShowSegments list all segments
func (c *Core) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error) {
	metrics.RootCoordShowSegmentsCounter.WithLabelValues(metricProxy(in.Base.SourceID), MetricRequestsTotal).Inc()
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
			core: c,
		},
		Req: in,
		Rsp: &milvuspb.ShowSegmentsResponse{
			Status:     nil,
			SegmentIDs: nil,
		},
	}
	err := executeTask(t)
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
	metrics.RootCoordShowSegmentsCounter.WithLabelValues(metricProxy(in.Base.SourceID), MetricRequestsSuccess).Inc()
	t.Rsp.Status = &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}
	return t.Rsp, nil
}

// AllocTimestamp alloc timestamp
func (c *Core) AllocTimestamp(ctx context.Context, in *rootcoordpb.AllocTimestampRequest) (*rootcoordpb.AllocTimestampResponse, error) {
	code := c.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		return &rootcoordpb.AllocTimestampResponse{
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
		return &rootcoordpb.AllocTimestampResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "AllocTimestamp failed: " + err.Error(),
			},
			Timestamp: 0,
			Count:     0,
		}, nil
	}
	// log.Printf("AllocTimestamp : %d", ts)

	//return first available  time stamp
	ts = ts - uint64(in.Count) + 1
	return &rootcoordpb.AllocTimestampResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Timestamp: ts,
		Count:     in.Count,
	}, nil
}

// AllocID alloc ids
func (c *Core) AllocID(ctx context.Context, in *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error) {
	code := c.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		return &rootcoordpb.AllocIDResponse{
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
		return &rootcoordpb.AllocIDResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "AllocID failed: " + err.Error(),
			},
			ID:    0,
			Count: in.Count,
		}, nil
	}
	log.Debug("AllocID", zap.Int64("id start", start), zap.Uint32("count", in.Count))
	return &rootcoordpb.AllocIDResponse{
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
	err := c.chanTimeTick.UpdateTimeTick(in, "gRPC")
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

// ReleaseDQLMessageStream release DQL msgstream
func (c *Core) ReleaseDQLMessageStream(ctx context.Context, in *proxypb.ReleaseDQLMessageStreamRequest) (*commonpb.Status, error) {
	code := c.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("state code = %s", internalpb.StateCode_name[int32(code)]),
		}, nil
	}
	return c.proxyClientManager.ReleaseDQLMessageStream(ctx, in)
}

// SegmentFlushCompleted check whether segment flush has completed
func (c *Core) SegmentFlushCompleted(ctx context.Context, in *datapb.SegmentFlushCompletedMsg) (*commonpb.Status, error) {
	code := c.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("state code = %s", internalpb.StateCode_name[int32(code)]),
		}, nil
	}
	if in.Base.MsgType != commonpb.MsgType_SegmentFlushDone {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("SegmentFlushDone with incorrect msgtype = %s", commonpb.MsgType_name[int32(in.Base.MsgType)]),
		}, nil
	}
	segID := in.Segment.GetID()
	log.Debug("flush segment", zap.Int64("id", segID))

	coll, err := c.MetaTable.GetCollectionByID(in.Segment.CollectionID, 0)
	if err != nil {
		log.Warn("GetCollectionByID error", zap.Error(err))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("GetCollectionBySegmentID error = %v", err),
		}, nil
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
			CollectionID: in.Segment.CollectionID,
			PartitionID:  in.Segment.PartitionID,
			SegmentID:    segID,
			FieldID:      fieldSch.FieldID,
			IndexID:      idxInfo.IndexID,
			EnableIndex:  false,
		}
		info.BuildID, err = c.BuildIndex(ctx, segID, fieldSch, idxInfo, true)
		if err == nil && info.BuildID != 0 {
			info.EnableIndex = true
		} else {
			log.Error("build index fail", zap.Int64("buildid", info.BuildID), zap.Error(err))
			continue
		}
		ts, _ := c.TSOAllocator(1)
		err = c.MetaTable.AddIndex(&info, ts)
		if err != nil {
			log.Error("AddIndex fail", zap.String("err", err.Error()))
		}
	}

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

// GetMetrics get metrics
func (c *Core) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	log.Debug("RootCoord.GetMetrics",
		zap.Int64("node_id", c.session.ServerID),
		zap.String("req", req.Request))

	if !c.isHealthy() {
		log.Warn("RootCoord.GetMetrics failed",
			zap.Int64("node_id", c.session.ServerID),
			zap.String("req", req.Request),
			zap.Error(errRootCoordIsUnhealthy(c.session.ServerID)))

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msgRootCoordIsUnhealthy(c.session.ServerID),
			},
			Response: "",
		}, nil
	}

	metricType, err := metricsinfo.ParseMetricType(req.Request)
	if err != nil {
		log.Warn("RootCoord.GetMetrics failed to parse metric type",
			zap.Int64("node_id", c.session.ServerID),
			zap.String("req", req.Request),
			zap.Error(err))

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Response: "",
		}, nil
	}

	log.Debug("RootCoord.GetMetrics",
		zap.String("metric_type", metricType))

	if metricType == metricsinfo.SystemInfoMetrics {
		ret, err := c.metricsCacheManager.GetSystemInfoMetrics()
		if err == nil && ret != nil {
			return ret, nil
		}
		log.Debug("failed to get system info metrics from cache, recompute instead",
			zap.Error(err))

		systemInfoMetrics, err := c.getSystemInfoMetrics(ctx, req)

		log.Debug("RootCoord.GetMetrics",
			zap.Int64("node_id", c.session.ServerID),
			zap.String("req", req.Request),
			zap.String("metric_type", metricType),
			zap.Any("systemInfoMetrics", systemInfoMetrics), // TODO(dragondriver): necessary? may be very large
			zap.Error(err))

		c.metricsCacheManager.UpdateSystemInfoMetrics(systemInfoMetrics)

		return systemInfoMetrics, err
	}

	log.Debug("RootCoord.GetMetrics failed, request metric type is not implemented yet",
		zap.Int64("node_id", c.session.ServerID),
		zap.String("req", req.Request),
		zap.String("metric_type", metricType))

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    metricsinfo.MsgUnimplementedMetric,
		},
		Response: "",
	}, nil
}

// CreateAlias create collection alias
func (c *Core) CreateAlias(ctx context.Context, in *milvuspb.CreateAliasRequest) (*commonpb.Status, error) {
	code := c.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("state code = %s", internalpb.StateCode_name[int32(code)]),
		}, nil
	}
	log.Debug("CreateAlias ", zap.String("alias", in.Alias), zap.String("name", in.CollectionName))
	t := &CreateAliasReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			core: c,
		},
		Req: in,
	}
	err := executeTask(t)
	if err != nil {
		log.Debug("CreateAlias failed", zap.String("alias", in.Alias), zap.String("name", in.CollectionName), zap.Error(err))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "Create alias failed: " + err.Error(),
		}, nil
	}
	log.Debug("CreateAlias Success", zap.String("alias", in.Alias), zap.String("name", in.CollectionName))
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

// DropAlias drop collection alias
func (c *Core) DropAlias(ctx context.Context, in *milvuspb.DropAliasRequest) (*commonpb.Status, error) {
	code := c.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("state code = %s", internalpb.StateCode_name[int32(code)]),
		}, nil
	}
	log.Debug("DropAlias ", zap.String("alias", in.Alias))
	t := &DropAliasReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			core: c,
		},
		Req: in,
	}
	err := executeTask(t)
	if err != nil {
		log.Debug("DropAlias failed", zap.String("alias", in.Alias), zap.Error(err))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "Drop alias failed: " + err.Error(),
		}, nil
	}
	log.Debug("DropAlias Success", zap.String("alias", in.Alias))
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

// AlterAlias alter collection alias
func (c *Core) AlterAlias(ctx context.Context, in *milvuspb.AlterAliasRequest) (*commonpb.Status, error) {
	code := c.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("state code = %s", internalpb.StateCode_name[int32(code)]),
		}, nil
	}
	log.Debug("AlterAlias ", zap.String("alias", in.Alias), zap.String("name", in.CollectionName))
	t := &AlterAliasReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			core: c,
		},
		Req: in,
	}
	err := executeTask(t)
	if err != nil {
		log.Debug("AlterAlias failed", zap.String("alias", in.Alias), zap.String("name", in.CollectionName), zap.Error(err))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "Alter alias failed: " + err.Error(),
		}, nil
	}
	log.Debug("AlterAlias Success", zap.String("alias", in.Alias), zap.String("name", in.CollectionName))
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

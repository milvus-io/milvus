// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rootcoord

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/db/dao"
	"github.com/milvus-io/milvus/internal/metastore/db/dbcore"
	rootcoord2 "github.com/milvus-io/milvus/internal/metastore/db/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/metrics"
	ms "github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/tso"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/crypto"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/errorutil"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// UniqueID is an alias of typeutil.UniqueID.
type UniqueID = typeutil.UniqueID

const InvalidCollectionID = UniqueID(0)

// ------------------ struct -----------------------

// DdOperation used to save ddMsg into etcd
type DdOperation struct {
	Body []byte `json:"body"`
	Type string `json:"type"`
}

func metricProxy(v int64) string {
	return fmt.Sprintf("client_%d", v)
}

var Params paramtable.ComponentParam

// Core root coordinator core
type Core struct {
	MetaTable *MetaTable
	//id allocator
	IDAllocator       func(count uint32) (typeutil.UniqueID, typeutil.UniqueID, error)
	IDAllocatorUpdate func() error

	//tso allocator
	TSOAllocator        func(count uint32) (typeutil.Timestamp, error)
	TSOAllocatorUpdate  func() error
	TSOGetLastSavedTime func() time.Time

	//inner members
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	etcdCli   *clientv3.Client
	kvBase    kv.TxnKV //*etcdkv.EtcdKV
	impTaskKv kv.MetaKv

	//DDL lock
	ddlLock sync.Mutex

	kvBaseCreate func(root string) (kv.TxnKV, error)

	metaKVCreate func(root string) (kv.MetaKv, error)

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

	//get segment info from data service
	CallGetFlushedSegmentsService func(ctx context.Context, collID, partID typeutil.UniqueID) ([]typeutil.UniqueID, error)
	CallGetRecoveryInfoService    func(ctx context.Context, collID, partID UniqueID) ([]*datapb.SegmentBinlogs, error)

	//call index builder's client to build index, return build id or get index state.
	CallDropCollectionIndexService  func(ctx context.Context, collID UniqueID) error
	CallGetSegmentIndexStateService func(ctx context.Context, collID UniqueID, indexName string, segIDs []UniqueID) ([]*indexpb.SegmentIndexState, error)

	NewProxyClient func(sess *sessionutil.Session) (types.Proxy, error)

	//query service interface, notify query service to release collection
	CallReleaseCollectionService func(ctx context.Context, ts typeutil.Timestamp, dbID, collectionID typeutil.UniqueID) error
	CallReleasePartitionService  func(ctx context.Context, ts typeutil.Timestamp, dbID, collectionID typeutil.UniqueID, partitionIDs []typeutil.UniqueID) error

	// Communicates with queryCoord service for segments info.
	CallGetSegmentInfoService func(ctx context.Context, collectionID int64, segIDs []int64) (*querypb.GetSegmentInfoResponse, error)

	CallWatchChannels func(ctx context.Context, collectionID int64, channelNames []string, startPositions []*commonpb.KeyDataPair) error

	//assign import task to data service
	CallImportService func(ctx context.Context, req *datapb.ImportTaskRequest) *datapb.ImportTaskResponse

	// Seals segments in collection cID, so they can get flushed later.
	CallFlushOnCollection func(ctx context.Context, cID int64, segIDs []int64) error

	// CallAddSegRefLock triggers AcquireSegmentLock method on DataCoord.
	CallAddSegRefLock func(ctx context.Context, taskID int64, segIDs []int64) (retErr error)

	// CallReleaseSegRefLock triggers ReleaseSegmentLock method on DataCoord.
	CallReleaseSegRefLock func(ctx context.Context, taskID int64, segIDs []int64) (retErr error)

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

	session *sessionutil.Session

	factory dependency.Factory

	//import manager
	importManager *importManager
}

// --------------------- function --------------------------

// NewCore creates a new rootcoord core
func NewCore(c context.Context, factory dependency.Factory) (*Core, error) {
	ctx, cancel := context.WithCancel(c)
	rand.Seed(time.Now().UnixNano())
	core := &Core{
		ctx:     ctx,
		cancel:  cancel,
		ddlLock: sync.Mutex{},
		factory: factory,
	}
	core.UpdateStateCode(internalpb.StateCode_Abnormal)
	return core, nil
}

// UpdateStateCode update state code
func (c *Core) UpdateStateCode(code internalpb.StateCode) {
	c.stateCode.Store(code)
}

func (c *Core) checkHealthy() (internalpb.StateCode, bool) {
	code := c.stateCode.Load().(internalpb.StateCode)
	ok := code == internalpb.StateCode_Healthy
	return code, ok
}

func failStatus(code commonpb.ErrorCode, reason string) *commonpb.Status {
	return &commonpb.Status{
		ErrorCode: code,
		Reason:    reason,
	}
}

func succStatus() *commonpb.Status {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}
}

func (c *Core) checkInit() error {
	if c.MetaTable == nil {
		return fmt.Errorf("metaTable is nil")
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
	if c.impTaskKv == nil {
		return fmt.Errorf("impTaskKv is nil")
	}
	if c.SendDdCreateCollectionReq == nil {
		return fmt.Errorf("sendDdCreateCollectionReq is nil")
	}
	if c.SendDdDropCollectionReq == nil {
		return fmt.Errorf("sendDdDropCollectionReq is nil")
	}
	if c.SendDdCreatePartitionReq == nil {
		return fmt.Errorf("sendDdCreatePartitionReq is nil")
	}
	if c.SendDdDropPartitionReq == nil {
		return fmt.Errorf("sendDdDropPartitionReq is nil")
	}
	if c.CallGetFlushedSegmentsService == nil {
		return fmt.Errorf("callGetFlushedSegmentsService is nil")
	}
	if c.CallGetRecoveryInfoService == nil {
		return fmt.Errorf("CallGetRecoveryInfoService is nil")
	}
	if c.CallDropCollectionIndexService == nil {
		return fmt.Errorf("callDropIndexService is nil")
	}
	if c.CallGetSegmentIndexStateService == nil {
		return fmt.Errorf("callGetSegmentIndexStateService is nil")
	}
	if c.CallWatchChannels == nil {
		return fmt.Errorf("callWatchChannels is nil")
	}
	if c.NewProxyClient == nil {
		return fmt.Errorf("newProxyClient is nil")
	}
	if c.CallReleaseCollectionService == nil {
		return fmt.Errorf("callReleaseCollectionService is nil")
	}
	if c.CallReleasePartitionService == nil {
		return fmt.Errorf("callReleasePartitionService is nil")
	}
	if c.CallImportService == nil {
		return fmt.Errorf("callImportService is nil")
	}
	if c.CallAddSegRefLock == nil {
		return fmt.Errorf("callAddSegRefLock is nil")
	}
	if c.CallReleaseSegRefLock == nil {
		return fmt.Errorf("callReleaseSegRefLock is nil")
	}

	return nil
}

func (c *Core) startTimeTickLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(Params.ProxyCfg.TimeTickInterval)
	for {
		select {
		case <-c.ctx.Done():
			log.Debug("rootcoord context closed", zap.Error(c.ctx.Err()))
			return
		case <-ticker.C:
			c.ddlLock.Lock()
			if ts, err := c.TSOAllocator(1); err == nil {
				err := c.SendTimeTick(ts, "timetick loop")
				if err != nil {
					log.Warn("Failed to send timetick", zap.Error(err))
				}
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
			ts := c.TSOGetLastSavedTime()
			metrics.RootCoordTimestampSaved.Set(float64(ts.Unix()))
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

func (c *Core) getSegments(ctx context.Context, collID typeutil.UniqueID) (map[UniqueID]UniqueID, map[UniqueID]*datapb.SegmentBinlogs, error) {
	collMeta, err := c.MetaTable.GetCollectionByID(collID, 0)
	if err != nil {
		return nil, nil, err
	}
	segID2PartID := make(map[UniqueID]UniqueID)
	segID2Binlog := make(map[UniqueID]*datapb.SegmentBinlogs)
	for _, part := range collMeta.Partitions {
		if segs, err := c.CallGetRecoveryInfoService(ctx, collID, part.PartitionID); err == nil {
			for _, s := range segs {
				segID2PartID[s.SegmentID] = part.PartitionID
				segID2Binlog[s.SegmentID] = s
			}
		} else {
			log.Error("failed to get flushed segments info from dataCoord",
				zap.Int64("collection ID", collID),
				zap.Int64("partition ID", part.PartitionID),
				zap.Error(err))
			return nil, nil, err
		}
	}

	return segID2PartID, segID2Binlog, nil
}

func (c *Core) setMsgStreams() error {
	if Params.CommonCfg.RootCoordSubName == "" {
		return fmt.Errorf("RootCoordSubName is empty")
	}

	c.SendTimeTick = func(t typeutil.Timestamp, reason string) error {
		pc := c.chanTimeTick.listDmlChannels()
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
		return c.chanTimeTick.updateTimeTick(&ttMsg, reason)
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
		return c.chanTimeTick.broadcastMarkDmlChannels(channelNames, &msgPack)
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
		return c.chanTimeTick.broadcastDmlChannels(channelNames, &msgPack)
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
		return c.chanTimeTick.broadcastDmlChannels(channelNames, &msgPack)
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
		return c.chanTimeTick.broadcastDmlChannels(channelNames, &msgPack)
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

// SetDataCoord set dataCoord.
func (c *Core) SetDataCoord(ctx context.Context, s types.DataCoord) error {
	initCh := make(chan struct{})
	go func() {
		for {
			if err := s.Init(); err == nil {
				if err := s.Start(); err == nil {
					close(initCh)
					log.Debug("RootCoord connected to DataCoord")
					return
				}
			}
			log.Debug("Retrying RootCoord connection to DataCoord")
		}
	}()

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
			return nil, err
		}
		if rsp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return nil, fmt.Errorf("get flushed segments from data coord failed, reason = %s", rsp.Status.Reason)
		}
		return rsp.Segments, nil
	}

	c.CallGetRecoveryInfoService = func(ctx context.Context, collID, partID typeutil.UniqueID) ([]*datapb.SegmentBinlogs, error) {
		getSegmentInfoReq := &datapb.GetRecoveryInfoRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0, //TODO, msg type
				MsgID:     0,
				Timestamp: 0,
				SourceID:  c.session.ServerID,
			},
			CollectionID: collID,
			PartitionID:  partID,
		}
		resp, err := s.GetRecoveryInfo(ctx, getSegmentInfoReq)
		if err != nil {
			return nil, err
		}
		if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return nil, errors.New(resp.Status.Reason)
		}
		return resp.Binlogs, nil
	}

	c.CallWatchChannels = func(ctx context.Context, collectionID int64, channelNames []string, startPositions []*commonpb.KeyDataPair) (retErr error) {
		defer func() {
			if err := recover(); err != nil {
				retErr = fmt.Errorf("watch channels panic, msg = %v", err)
			}
		}()
		<-initCh
		req := &datapb.WatchChannelsRequest{
			CollectionID:   collectionID,
			ChannelNames:   channelNames,
			StartPositions: startPositions,
		}
		rsp, err := s.WatchChannels(ctx, req)
		if err != nil {
			return err
		}
		if rsp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return fmt.Errorf("data coord watch channels failed, reason = %s", rsp.Status.Reason)
		}
		return nil
	}

	c.CallImportService = func(ctx context.Context, req *datapb.ImportTaskRequest) *datapb.ImportTaskResponse {
		resp := &datapb.ImportTaskResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
		}
		defer func() {
			if err := recover(); err != nil {
				resp.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
				resp.Status.Reason = "assign import task to data coord panic"
			}
		}()
		resp, _ = s.Import(ctx, req)
		return resp
	}

	c.CallFlushOnCollection = func(ctx context.Context, cID int64, segIDs []int64) error {
		resp, err := s.Flush(ctx, &datapb.FlushRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Flush,
				SourceID: c.session.ServerID,
			},
			DbID:         0,
			SegmentIDs:   segIDs,
			CollectionID: cID,
		})
		if err != nil {
			return errors.New("failed to call flush to data coordinator: " + err.Error())
		}
		if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return errors.New(resp.Status.Reason)
		}
		log.Info("flush on collection succeed", zap.Int64("collection ID", cID))
		return nil
	}

	c.CallAddSegRefLock = func(ctx context.Context, taskID int64, segIDs []int64) (retErr error) {
		defer func() {
			if err := recover(); err != nil {
				retErr = fmt.Errorf("add seg ref lock panic, msg = %v", err)
			}
		}()
		<-initCh
		log.Info("acquiring seg lock",
			zap.Int64s("segment IDs", segIDs),
			zap.Int64("node ID", c.session.ServerID))
		resp, _ := s.AcquireSegmentLock(ctx, &datapb.AcquireSegmentLockRequest{
			SegmentIDs: segIDs,
			NodeID:     c.session.ServerID,
			TaskID:     taskID,
		})
		if resp.GetErrorCode() != commonpb.ErrorCode_Success {
			return fmt.Errorf("failed to acquire segment lock %s", resp.GetReason())
		}
		log.Info("acquire seg lock succeed",
			zap.Int64s("segment IDs", segIDs),
			zap.Int64("node ID", c.session.ServerID))
		return nil
	}

	c.CallReleaseSegRefLock = func(ctx context.Context, taskID int64, segIDs []int64) (retErr error) {
		defer func() {
			if err := recover(); err != nil {
				retErr = fmt.Errorf("release seg ref lock panic, msg = %v", err)
			}
		}()
		<-initCh
		log.Info("releasing seg lock",
			zap.Int64s("segment IDs", segIDs),
			zap.Int64("node ID", c.session.ServerID))
		resp, _ := s.ReleaseSegmentLock(ctx, &datapb.ReleaseSegmentLockRequest{
			SegmentIDs: segIDs,
			NodeID:     c.session.ServerID,
			TaskID:     taskID,
		})
		if resp.GetErrorCode() != commonpb.ErrorCode_Success {
			return fmt.Errorf("failed to release segment lock %s", resp.GetReason())
		}
		log.Info("release seg lock succeed",
			zap.Int64s("segment IDs", segIDs),
			zap.Int64("node ID", c.session.ServerID))
		return nil
	}

	return nil
}

// SetIndexCoord sets IndexCoord.
func (c *Core) SetIndexCoord(s types.IndexCoord) error {
	initCh := make(chan struct{})
	go func() {
		for {
			if err := s.Init(); err == nil {
				if err := s.Start(); err == nil {
					close(initCh)
					log.Debug("RootCoord connected to IndexCoord")
					return
				}
			}
			log.Debug("Retrying RootCoord connection to IndexCoord")
		}
	}()

	c.CallGetSegmentIndexStateService = func(ctx context.Context, collID UniqueID, indexName string, segIDs []UniqueID) (states []*indexpb.SegmentIndexState, retErr error) {
		defer func() {
			if err := recover(); err != nil {
				retErr = fmt.Errorf("get segment state panic, msg = %v", err)
			}
		}()
		<-initCh

		resp, err := s.GetSegmentIndexState(ctx, &indexpb.GetSegmentIndexStateRequest{
			CollectionID: collID,
			IndexName:    indexName,
			SegmentIDs:   segIDs,
		})
		if err != nil {
			return nil, err
		}
		if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return nil, errors.New(resp.Status.Reason)
		}

		return resp.GetStates(), nil
	}

	c.CallDropCollectionIndexService = func(ctx context.Context, collID UniqueID) (retErr error) {
		defer func() {
			if err := recover(); err != nil {
				retErr = fmt.Errorf("drop collection index panic, msg = %v", err)
			}
		}()
		<-initCh

		rsp, err := s.DropIndex(ctx, &indexpb.DropIndexRequest{
			CollectionID: collID,
			IndexName:    "",
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

// SetQueryCoord sets up queryCoord and queryCoord related function calls.
func (c *Core) SetQueryCoord(s types.QueryCoord) error {
	initCh := make(chan struct{})
	go func() {
		for {
			if err := s.Init(); err == nil {
				if err := s.Start(); err == nil {
					close(initCh)
					log.Debug("RootCoord connected to QueryCoord")
					return
				}
			}
			log.Debug("Retrying RootCoord connection to QueryCoord")
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
			return fmt.Errorf("releaseCollection from query service failed, error = %s", rsp.Reason)
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
			return fmt.Errorf("releasePartitions from query service failed, error = %s", rsp.Reason)
		}
		return nil
	}
	c.CallGetSegmentInfoService = func(ctx context.Context, collectionID int64, segIDs []int64) (retResp *querypb.GetSegmentInfoResponse, retErr error) {
		defer func() {
			if err := recover(); err != nil {
				retErr = fmt.Errorf("call segment info service panic, msg = %v", err)
			}
		}()
		<-initCh
		resp, err := s.GetSegmentInfo(ctx, &querypb.GetSegmentInfoRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_GetSegmentState,
				SourceID: c.session.ServerID,
			},
			CollectionID: collectionID,
			SegmentIDs:   segIDs,
		})
		return resp, err
	}
	return nil
}

// ExpireMetaCache will call invalidate collection meta cache
func (c *Core) ExpireMetaCache(ctx context.Context, collNames []string, collectionID UniqueID, ts typeutil.Timestamp) error {
	// if collectionID is specified, invalidate all the collection meta cache with the specified collectionID and return
	if collectionID != InvalidCollectionID {
		req := proxypb.InvalidateCollMetaCacheRequest{
			Base: &commonpb.MsgBase{
				Timestamp: ts,
				SourceID:  c.session.ServerID,
			},
			CollectionID: collectionID,
		}
		return c.proxyClientManager.InvalidateCollectionMetaCache(ctx, &req)
	}

	// if only collNames are specified, invalidate the collection meta cache with the specified collectionName
	for _, collName := range collNames {
		req := proxypb.InvalidateCollMetaCacheRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0, //TODO, msg type
				MsgID:     0, //TODO, msg id
				Timestamp: ts,
				SourceID:  c.session.ServerID,
			},
			CollectionName: collName,
		}
		err := c.proxyClientManager.InvalidateCollectionMetaCache(ctx, &req)
		if err != nil {
			// TODO: try to expire all or directly return err?
			return err
		}
	}
	return nil
}

// Register register rootcoord at etcd
func (c *Core) Register() error {
	c.session.Register()
	go c.session.LivenessCheck(c.ctx, func() {
		log.Error("Root Coord disconnected from etcd, process will exit", zap.Int64("Server Id", c.session.ServerID))
		if err := c.Stop(); err != nil {
			log.Fatal("failed to stop server", zap.Error(err))
		}
		// manually send signal to starter goroutine
		if c.session.TriggerKill {
			if p, err := os.FindProcess(os.Getpid()); err == nil {
				p.Signal(syscall.SIGINT)
			}
		}
	})

	c.UpdateStateCode(internalpb.StateCode_Healthy)
	log.Debug("RootCoord start successfully ", zap.String("State Code", internalpb.StateCode_Healthy.String()))
	return nil
}

// SetEtcdClient sets the etcdCli of Core
func (c *Core) SetEtcdClient(etcdClient *clientv3.Client) {
	c.etcdCli = etcdClient
}

func (c *Core) initSession() error {
	c.session = sessionutil.NewSession(c.ctx, Params.EtcdCfg.MetaRootPath, c.etcdCli)
	if c.session == nil {
		return fmt.Errorf("session is nil, the etcd client connection may have failed")
	}
	c.session.Init(typeutil.RootCoordRole, Params.RootCoordCfg.Address, true, true)
	Params.SetLogger(c.session.ServerID)
	return nil
}

// Init initialize routine
func (c *Core) Init() error {
	var initError error
	if c.kvBaseCreate == nil {
		c.kvBaseCreate = func(root string) (kv.TxnKV, error) {
			return etcdkv.NewEtcdKV(c.etcdCli, root), nil
		}
	}
	if c.metaKVCreate == nil {
		c.metaKVCreate = func(root string) (kv.MetaKv, error) {
			return etcdkv.NewEtcdKV(c.etcdCli, root), nil
		}
	}
	c.initOnce.Do(func() {
		if err := c.initSession(); err != nil {
			initError = err
			log.Error("RootCoord init session failed", zap.Error(err))
			return
		}
		connectEtcdFn := func() error {
			if c.kvBase, initError = c.kvBaseCreate(Params.EtcdCfg.KvRootPath); initError != nil {
				log.Error("RootCoord failed to new EtcdKV for kvBase", zap.Any("reason", initError))
				return initError
			}
			if c.impTaskKv, initError = c.metaKVCreate(Params.EtcdCfg.KvRootPath); initError != nil {
				log.Error("RootCoord failed to new EtcdKV for MetaKV", zap.Any("reason", initError))
				return initError
			}

			var catalog metastore.RootCoordCatalog
			switch Params.MetaStoreCfg.MetaStoreType {
			case util.MetaStoreTypeEtcd:
				var metaKV kv.TxnKV
				metaKV, initError = c.kvBaseCreate(Params.EtcdCfg.MetaRootPath)
				if initError != nil {
					log.Error("RootCoord failed to new EtcdKV", zap.Any("reason", initError))
					return initError
				}

				var ss *rootcoord.SuffixSnapshot
				if ss, initError = rootcoord.NewSuffixSnapshot(metaKV, "_ts", Params.EtcdCfg.MetaRootPath, "snapshots"); initError != nil {
					log.Error("RootCoord failed to new suffixSnapshot", zap.Error(initError))
					return initError
				}

				catalog = &rootcoord.Catalog{Txn: metaKV, Snapshot: ss}
			case util.MetaStoreTypeMysql:
				// connect to database
				err := dbcore.Connect(&Params.DBCfg)
				if err != nil {
					return err
				}

				catalog = rootcoord2.NewTableCatalog(dbcore.NewTxImpl(), dao.NewMetaDomain())
			default:
				return fmt.Errorf("not supported meta store: %s", Params.MetaStoreCfg.MetaStoreType)
			}

			if c.MetaTable, initError = NewMetaTable(c.ctx, catalog); initError != nil {
				log.Error("RootCoord failed to new MetaTable", zap.Any("reason", initError))
				return initError
			}

			return nil
		}
		log.Debug("RootCoord, Connecting to Etcd", zap.String("kv root", Params.EtcdCfg.KvRootPath), zap.String("meta root", Params.EtcdCfg.MetaRootPath))
		err := retry.Do(c.ctx, connectEtcdFn, retry.Attempts(100))
		if err != nil {
			return
		}

		log.Debug("RootCoord, Setting TSO and ID Allocator")
		kv := tsoutil.NewTSOKVBase(c.etcdCli, Params.EtcdCfg.KvRootPath, "gid")
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

		kv = tsoutil.NewTSOKVBase(c.etcdCli, Params.EtcdCfg.KvRootPath, "tso")
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
		c.TSOGetLastSavedTime = func() time.Time {
			return tsoAllocator.GetLastSavedTime()
		}

		c.factory.Init(&Params)

		chanMap := c.MetaTable.ListCollectionPhysicalChannels()
		c.chanTimeTick = newTimeTickSync(c.ctx, c.session.ServerID, c.factory, chanMap)
		c.chanTimeTick.addSession(c.session)
		c.proxyClientManager = newProxyClientManager(c)

		log.Debug("RootCoord, set proxy manager")
		c.proxyManager = newProxyManager(
			c.ctx,
			c.etcdCli,
			c.chanTimeTick.initSessions,
			c.proxyClientManager.GetProxyClients,
		)
		c.proxyManager.AddSessionFunc(c.chanTimeTick.addSession, c.proxyClientManager.AddProxyClient)
		c.proxyManager.DelSessionFunc(c.chanTimeTick.delSession, c.proxyClientManager.DelProxyClient)

		c.metricsCacheManager = metricsinfo.NewMetricsCacheManager()

		initError = c.setMsgStreams()
		if initError != nil {
			return
		}

		c.importManager = newImportManager(
			c.ctx,
			c.impTaskKv,
			c.IDAllocator,
			c.CallImportService,
			c.getCollectionName,
		)
		c.importManager.init(c.ctx)

		// init data
		initError = c.initData()
		if initError != nil {
			return
		}

		if initError = c.initRbac(); initError != nil {
			return
		}
		log.Debug("RootCoord init user root done")
	})
	if initError != nil {
		log.Debug("RootCoord init error", zap.Error(initError))
	}
	log.Debug("RootCoord init done")
	return initError
}

func (c *Core) initData() error {
	credInfo, _ := c.MetaTable.GetCredential(util.UserRoot)
	if credInfo == nil {
		log.Debug("RootCoord init user root")
		encryptedRootPassword, _ := crypto.PasswordEncrypt(util.DefaultRootPassword)
		err := c.MetaTable.AddCredential(&internalpb.CredentialInfo{Username: util.UserRoot, EncryptedPassword: encryptedRootPassword})
		return err
	}
	return nil
}

func (c *Core) initRbac() (initError error) {
	// create default roles, including admin, public
	for _, role := range util.DefaultRoles {
		if initError = c.MetaTable.CreateRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: role}); initError != nil {
			if common.IsIgnorableError(initError) {
				initError = nil
				continue
			}
			return
		}
	}

	// grant privileges for the public role
	globalPrivileges := []string{
		commonpb.ObjectPrivilege_PrivilegeDescribeCollection.String(),
		commonpb.ObjectPrivilege_PrivilegeShowCollections.String(),
	}
	collectionPrivileges := []string{
		commonpb.ObjectPrivilege_PrivilegeIndexDetail.String(),
	}

	for _, globalPrivilege := range globalPrivileges {
		if initError = c.MetaTable.OperatePrivilege(util.DefaultTenant, &milvuspb.GrantEntity{
			Role:       &milvuspb.RoleEntity{Name: util.RolePublic},
			Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Global.String()},
			ObjectName: util.AnyWord,
			Grantor: &milvuspb.GrantorEntity{
				User:      &milvuspb.UserEntity{Name: util.UserRoot},
				Privilege: &milvuspb.PrivilegeEntity{Name: globalPrivilege},
			},
		}, milvuspb.OperatePrivilegeType_Grant); initError != nil {
			if common.IsIgnorableError(initError) {
				initError = nil
				continue
			}
			return
		}
	}
	for _, collectionPrivilege := range collectionPrivileges {
		if initError = c.MetaTable.OperatePrivilege(util.DefaultTenant, &milvuspb.GrantEntity{
			Role:       &milvuspb.RoleEntity{Name: util.RolePublic},
			Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Collection.String()},
			ObjectName: util.AnyWord,
			Grantor: &milvuspb.GrantorEntity{
				User:      &milvuspb.UserEntity{Name: util.UserRoot},
				Privilege: &milvuspb.PrivilegeEntity{Name: collectionPrivilege},
			},
		}, milvuspb.OperatePrivilegeType_Grant); initError != nil {
			if common.IsIgnorableError(initError) {
				initError = nil
				continue
			}
			return
		}
	}
	return nil
}

func (c *Core) getCollectionName(collID, partitionID typeutil.UniqueID) (string, string, error) {
	colName, err := c.MetaTable.GetCollectionNameByID(collID)
	if err != nil {
		log.Error("RootCoord failed to get collection name by id", zap.Int64("ID", collID), zap.Error(err))
		return "", "", err
	}

	partName, err := c.MetaTable.GetPartitionNameByID(collID, partitionID, 0)
	if err != nil {
		log.Error("RootCoord failed to get partition name by id", zap.Int64("ID", partitionID), zap.Error(err))
		return colName, "", err
	}

	return colName, partName, nil
}

// Start starts RootCoord.
func (c *Core) Start() error {
	if err := c.checkInit(); err != nil {
		log.Debug("RootCoord Start checkInit failed", zap.Error(err))
		return err
	}

	log.Debug("starting service",
		zap.String("service role", typeutil.RootCoordRole),
		zap.Int64("node id", c.session.ServerID))

	c.startOnce.Do(func() {
		if err := c.proxyManager.WatchProxy(); err != nil {
			log.Fatal("RootCoord Start WatchProxy failed", zap.Error(err))
			// you can not just stuck here,
			panic(err)
		}

		c.wg.Add(5)
		go c.startTimeTickLoop()
		go c.tsLoop()
		go c.chanTimeTick.startWatch(&c.wg)
		go c.importManager.expireOldTasksLoop(&c.wg, c.CallReleaseSegRefLock)
		go c.importManager.sendOutTasksLoop(&c.wg)
		Params.RootCoordCfg.CreatedTime = time.Now()
		Params.RootCoordCfg.UpdatedTime = time.Now()
	})

	return nil
}

// Stop stops rootCoord.
func (c *Core) Stop() error {
	c.UpdateStateCode(internalpb.StateCode_Abnormal)

	c.cancel()
	c.wg.Wait()
	// wait at most one second to revoke
	c.session.Revoke(time.Second)
	return nil
}

// GetComponentStates get states of components
func (c *Core) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	code := c.stateCode.Load().(internalpb.StateCode)
	log.Debug("GetComponentStates", zap.String("State Code", internalpb.StateCode_name[int32(code)]))

	nodeID := common.NotRegisteredID
	if c.session != nil && c.session.Registered() {
		nodeID = c.session.ServerID
	}

	return &internalpb.ComponentStates{
		State: &internalpb.ComponentInfo{
			// NodeID:    c.session.ServerID, // will race with Core.Register()
			NodeID:    nodeID,
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
				NodeID:    nodeID,
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
		Value: Params.CommonCfg.RootCoordTimeTick,
	}, nil
}

// GetStatisticsChannel get statistics channel name
func (c *Core) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: Params.CommonCfg.RootCoordStatistics,
	}, nil
}

// CreateCollection create collection
func (c *Core) CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	metrics.RootCoordDDLReqCounter.WithLabelValues("CreateCollection", metrics.TotalLabel).Inc()
	if code, ok := c.checkHealthy(); !ok {
		return failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]), nil
	}

	tr := timerecord.NewTimeRecorder("CreateCollection")

	log.Debug("CreateCollection", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID))
	t := &CreateCollectionReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			core: c,
		},
		Req: in,
	}
	err := executeTask(t)
	if err != nil {
		log.Error("CreateCollection failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("collection name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("CreateCollection", metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, "CreateCollection failed: "+err.Error()), nil
	}
	log.Debug("CreateCollection success", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordDDLReqCounter.WithLabelValues("CreateCollection", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("CreateCollection").Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfCollections.Inc()
	return succStatus(), nil
}

// DropCollection drop collection
func (c *Core) DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	metrics.RootCoordDDLReqCounter.WithLabelValues("DropCollection", metrics.TotalLabel).Inc()
	if code, ok := c.checkHealthy(); !ok {
		return failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]), nil
	}
	tr := timerecord.NewTimeRecorder("DropCollection")
	log.Debug("DropCollection", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID))
	t := &DropCollectionReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			core: c,
		},
		Req: in,
	}
	err := executeTask(t)
	if err != nil {
		log.Error("DropCollection failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("collection name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("DropCollection", metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, "DropCollection failed: "+err.Error()), nil
	}
	log.Debug("DropCollection success", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordDDLReqCounter.WithLabelValues("DropCollection", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("DropCollection").Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfCollections.Dec()
	return succStatus(), nil
}

// HasCollection check collection existence
func (c *Core) HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	metrics.RootCoordDDLReqCounter.WithLabelValues("HasCollection", metrics.TotalLabel).Inc()
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.BoolResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
			Value:  false,
		}, nil
	}
	tr := timerecord.NewTimeRecorder("HasCollection")

	log.Debug("HasCollection", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID))
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
		log.Error("HasCollection failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("collection name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("HasCollection", metrics.FailLabel).Inc()
		return &milvuspb.BoolResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "HasCollection failed: "+err.Error()),
			Value:  false,
		}, nil
	}
	log.Debug("HasCollection success", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID), zap.Bool("hasCollection", t.HasCollection))

	metrics.RootCoordDDLReqCounter.WithLabelValues("HasCollection", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("HasCollection").Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &milvuspb.BoolResponse{
		Status: succStatus(),
		Value:  t.HasCollection,
	}, nil
}

// DescribeCollection return collection info
func (c *Core) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeCollection", metrics.TotalLabel).Inc()
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.DescribeCollectionResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode"+internalpb.StateCode_name[int32(code)]),
		}, nil
	}
	tr := timerecord.NewTimeRecorder("DescribeCollection")

	log.Ctx(ctx).Debug("DescribeCollection", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.Int64("id", in.CollectionID), zap.Int64("msgID", in.Base.MsgID))
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
		log.Ctx(ctx).Warn("DescribeCollection failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("collection name", in.CollectionName), zap.Int64("id", in.CollectionID), zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeCollection", metrics.FailLabel).Inc()
		return &milvuspb.DescribeCollectionResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "DescribeCollection failed: "+err.Error()),
		}, nil
	}
	log.Ctx(ctx).Debug("DescribeCollection success", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.Int64("id", in.CollectionID), zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeCollection", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("DescribeCollection").Observe(float64(tr.ElapseSpan().Milliseconds()))
	t.Rsp.Status = succStatus()
	return t.Rsp, nil
}

// ShowCollections list all collection names
func (c *Core) ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	metrics.RootCoordDDLReqCounter.WithLabelValues("ShowCollections", metrics.TotalLabel).Inc()
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.ShowCollectionsResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
		}, nil
	}
	tr := timerecord.NewTimeRecorder("ShowCollections")

	log.Debug("ShowCollections", zap.String("role", typeutil.RootCoordRole),
		zap.String("dbname", in.DbName), zap.Int64("msgID", in.Base.MsgID))
	t := &ShowCollectionReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			core: c,
		},
		Req: in,
		Rsp: &milvuspb.ShowCollectionsResponse{},
	}
	err := executeTask(t)
	if err != nil {
		log.Error("ShowCollections failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("dbname", in.DbName), zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("ShowCollections", metrics.FailLabel).Inc()
		return &milvuspb.ShowCollectionsResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "ShowCollections failed: "+err.Error()),
		}, nil
	}
	log.Debug("ShowCollections success", zap.String("role", typeutil.RootCoordRole),
		zap.String("dbname", in.DbName), zap.Int("num of collections", len(t.Rsp.CollectionNames)),
		zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordDDLReqCounter.WithLabelValues("ShowCollections", metrics.SuccessLabel).Inc()
	t.Rsp.Status = succStatus()
	metrics.RootCoordDDLReqLatency.WithLabelValues("ShowCollections").Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.Rsp, nil
}

// CreatePartition create partition
func (c *Core) CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	metrics.RootCoordDDLReqCounter.WithLabelValues("CreatePartition", metrics.TotalLabel).Inc()
	if code, ok := c.checkHealthy(); !ok {
		return failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]), nil
	}
	tr := timerecord.NewTimeRecorder("CreatePartition")
	log.Debug("CreatePartition", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName),
		zap.Int64("msgID", in.Base.MsgID))
	t := &CreatePartitionReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			core: c,
		},
		Req: in,
	}
	err := executeTask(t)
	if err != nil {
		log.Error("CreatePartition failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName),
			zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("CreatePartition", metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, "CreatePartition failed: "+err.Error()), nil
	}
	log.Debug("CreatePartition success", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName),
		zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordDDLReqCounter.WithLabelValues("CreatePartition", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("CreatePartition").Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfPartitions.WithLabelValues().Inc()
	return succStatus(), nil
}

// DropPartition drop partition
func (c *Core) DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	metrics.RootCoordDDLReqCounter.WithLabelValues("DropPartition", metrics.TotalLabel).Inc()
	if code, ok := c.checkHealthy(); !ok {
		return failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]), nil
	}
	tr := timerecord.NewTimeRecorder("DropPartition")
	log.Debug("DropPartition", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName),
		zap.Int64("msgID", in.Base.MsgID))
	t := &DropPartitionReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			core: c,
		},
		Req: in,
	}
	err := executeTask(t)
	if err != nil {
		log.Error("DropPartition failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName),
			zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("DropPartition", metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, "DropPartition failed: "+err.Error()), nil
	}
	log.Debug("DropPartition success", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName),
		zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordDDLReqCounter.WithLabelValues("DropPartition", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("DropPartition").Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfPartitions.WithLabelValues().Dec()
	return succStatus(), nil
}

// HasPartition check partition existence
func (c *Core) HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	metrics.RootCoordDDLReqCounter.WithLabelValues("HasPartition", metrics.TotalLabel).Inc()
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.BoolResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
			Value:  false,
		}, nil
	}
	tr := timerecord.NewTimeRecorder("HasPartition")

	log.Debug("HasPartition", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName),
		zap.Int64("msgID", in.Base.MsgID))
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
		log.Error("HasPartition failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName),
			zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("HasPartition", metrics.FailLabel).Inc()
		return &milvuspb.BoolResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "HasPartition failed: "+err.Error()),
			Value:  false,
		}, nil
	}
	log.Debug("HasPartition success", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName),
		zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordDDLReqCounter.WithLabelValues("HasPartition", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("HasPartition").Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &milvuspb.BoolResponse{
		Status: succStatus(),
		Value:  t.HasPartition,
	}, nil
}

// ShowPartitions list all partition names
func (c *Core) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	metrics.RootCoordDDLReqCounter.WithLabelValues("ShowPartitions", metrics.TotalLabel).Inc()
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.ShowPartitionsResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
		}, nil
	}

	tr := timerecord.NewTimeRecorder("ShowPartitions")
	log.Debug("ShowPartitions", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID))
	t := &ShowPartitionReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			core: c,
		},
		Req: in,
		Rsp: &milvuspb.ShowPartitionsResponse{},
	}
	err := executeTask(t)
	if err != nil {
		log.Error("ShowPartitions failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("collection name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("ShowPartitions", metrics.FailLabel).Inc()
		return &milvuspb.ShowPartitionsResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "ShowPartitions failed: "+err.Error()),
		}, nil
	}
	log.Debug("ShowPartitions success", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.Int("num of partitions", len(t.Rsp.PartitionNames)),
		zap.Int64("msgID", t.Req.Base.MsgID))

	metrics.RootCoordDDLReqCounter.WithLabelValues("ShowPartitions", metrics.SuccessLabel).Inc()
	t.Rsp.Status = succStatus()
	metrics.RootCoordDDLReqLatency.WithLabelValues("ShowPartitions").Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.Rsp, nil
}

//// DescribeSegment return segment info
//func (c *Core) DescribeSegment(ctx context.Context, in *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error) {
//	metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeSegment", metrics.TotalLabel).Inc()
//	if code, ok := c.checkHealthy(); !ok {
//		return &milvuspb.DescribeSegmentResponse{
//			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
//		}, nil
//	}
//	tr := timerecord.NewTimeRecorder("DescribeSegment")
//	log.Debug("DescribeSegment", zap.String("role", typeutil.RootCoordRole),
//		zap.Int64("collection id", in.CollectionID), zap.Int64("segment id", in.SegmentID),
//		zap.Int64("msgID", in.Base.MsgID))
//	t := &DescribeSegmentReqTask{
//		baseReqTask: baseReqTask{
//			ctx:  ctx,
//			core: c,
//		},
//		Req: in,
//		Rsp: &milvuspb.DescribeSegmentResponse{},
//	}
//	err := executeTask(t)
//	if err != nil {
//		log.Error("DescribeSegment failed", zap.String("role", typeutil.RootCoordRole),
//			zap.Int64("collection id", in.CollectionID), zap.Int64("segment id", in.SegmentID),
//			zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
//		metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeSegment", metrics.FailLabel).Inc()
//		return &milvuspb.DescribeSegmentResponse{
//			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "DescribeSegment failed: "+err.Error()),
//		}, nil
//	}
//	log.Debug("DescribeSegment success", zap.String("role", typeutil.RootCoordRole),
//		zap.Int64("collection id", in.CollectionID), zap.Int64("segment id", in.SegmentID),
//		zap.Int64("msgID", in.Base.MsgID))
//
//	metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeSegment", metrics.SuccessLabel).Inc()
//	metrics.RootCoordDDLReqLatency.WithLabelValues("DescribeSegment").Observe(float64(tr.ElapseSpan().Milliseconds()))
//	t.Rsp.Status = succStatus()
//	return t.Rsp, nil
//}
//
//func (c *Core) DescribeSegments(ctx context.Context, in *rootcoordpb.DescribeSegmentsRequest) (*rootcoordpb.DescribeSegmentsResponse, error) {
//	metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeSegments", metrics.TotalLabel).Inc()
//	if code, ok := c.checkHealthy(); !ok {
//		log.Error("failed to describe segments, rootcoord not healthy",
//			zap.String("role", typeutil.RootCoordRole),
//			zap.Int64("msgID", in.GetBase().GetMsgID()),
//			zap.Int64("collection", in.GetCollectionID()),
//			zap.Int64s("segments", in.GetSegmentIDs()))
//
//		return &rootcoordpb.DescribeSegmentsResponse{
//			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
//		}, nil
//	}
//
//	tr := timerecord.NewTimeRecorder("DescribeSegments")
//
//	log.Debug("received request to describe segments",
//		zap.String("role", typeutil.RootCoordRole),
//		zap.Int64("msgID", in.GetBase().GetMsgID()),
//		zap.Int64("collection", in.GetCollectionID()),
//		zap.Int64s("segments", in.GetSegmentIDs()))
//
//	t := &DescribeSegmentsReqTask{
//		baseReqTask: baseReqTask{
//			ctx:  ctx,
//			core: c,
//		},
//		Req: in,
//		Rsp: &rootcoordpb.DescribeSegmentsResponse{},
//	}
//
//	if err := executeTask(t); err != nil {
//		log.Error("failed to describe segments",
//			zap.Error(err),
//			zap.String("role", typeutil.RootCoordRole),
//			zap.Int64("msgID", in.GetBase().GetMsgID()),
//			zap.Int64("collection", in.GetCollectionID()),
//			zap.Int64s("segments", in.GetSegmentIDs()))
//
//		metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeSegments", metrics.FailLabel).Inc()
//		return &rootcoordpb.DescribeSegmentsResponse{
//			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "DescribeSegments failed: "+err.Error()),
//		}, nil
//	}
//
//	log.Debug("succeed to describe segments",
//		zap.String("role", typeutil.RootCoordRole),
//		zap.Int64("msgID", in.GetBase().GetMsgID()),
//		zap.Int64("collection", in.GetCollectionID()),
//		zap.Int64s("segments", in.GetSegmentIDs()))
//
//	metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeSegments", metrics.SuccessLabel).Inc()
//	metrics.RootCoordDDLReqLatency.WithLabelValues("DescribeSegments").Observe(float64(tr.ElapseSpan().Milliseconds()))
//
//	t.Rsp.Status = succStatus()
//	return t.Rsp, nil
//}

// ShowSegments list all segments
func (c *Core) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error) {
	metrics.RootCoordDDLReqCounter.WithLabelValues("ShowSegments", metrics.TotalLabel).Inc()
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.ShowSegmentsResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
		}, nil
	}
	tr := timerecord.NewTimeRecorder("ShowSegments")

	log.Debug("ShowSegments", zap.String("role", typeutil.RootCoordRole),
		zap.Int64("collection id", in.CollectionID), zap.Int64("partition id", in.PartitionID),
		zap.Int64("msgID", in.Base.MsgID))
	t := &ShowSegmentReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			core: c,
		},
		Req: in,
		Rsp: &milvuspb.ShowSegmentsResponse{},
	}
	err := executeTask(t)
	if err != nil {
		log.Debug("ShowSegments failed", zap.String("role", typeutil.RootCoordRole),
			zap.Int64("collection id", in.CollectionID), zap.Int64("partition id", in.PartitionID),
			zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("ShowSegments", metrics.FailLabel).Inc()
		return &milvuspb.ShowSegmentsResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "ShowSegments failed: "+err.Error()),
		}, nil
	}
	log.Debug("ShowSegments success", zap.String("role", typeutil.RootCoordRole),
		zap.Int64("collection id", in.CollectionID), zap.Int64("partition id", in.PartitionID),
		zap.Int64s("segments ids", t.Rsp.SegmentIDs),
		zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordDDLReqCounter.WithLabelValues("ShowSegments", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("ShowSegments").Observe(float64(tr.ElapseSpan().Milliseconds()))
	t.Rsp.Status = succStatus()
	return t.Rsp, nil
}

// AllocTimestamp alloc timestamp
func (c *Core) AllocTimestamp(ctx context.Context, in *rootcoordpb.AllocTimestampRequest) (*rootcoordpb.AllocTimestampResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &rootcoordpb.AllocTimestampResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
		}, nil
	}
	ts, err := c.TSOAllocator(in.Count)
	if err != nil {
		log.Error("AllocTimestamp failed", zap.String("role", typeutil.RootCoordRole),
			zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		return &rootcoordpb.AllocTimestampResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "AllocTimestamp failed: "+err.Error()),
		}, nil
	}

	//return first available  time stamp
	ts = ts - uint64(in.Count) + 1
	metrics.RootCoordTimestamp.Set(float64(ts))
	return &rootcoordpb.AllocTimestampResponse{
		Status:    succStatus(),
		Timestamp: ts,
		Count:     in.Count,
	}, nil
}

// AllocID alloc ids
func (c *Core) AllocID(ctx context.Context, in *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &rootcoordpb.AllocIDResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
		}, nil
	}
	start, _, err := c.IDAllocator(in.Count)
	if err != nil {
		log.Error("AllocID failed", zap.String("role", typeutil.RootCoordRole),
			zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		return &rootcoordpb.AllocIDResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "AllocID failed: "+err.Error()),
			Count:  in.Count,
		}, nil
	}
	metrics.RootCoordIDAllocCounter.Add(float64(in.Count))
	return &rootcoordpb.AllocIDResponse{
		Status: succStatus(),
		ID:     start,
		Count:  in.Count,
	}, nil
}

// UpdateChannelTimeTick used to handle ChannelTimeTickMsg
func (c *Core) UpdateChannelTimeTick(ctx context.Context, in *internalpb.ChannelTimeTickMsg) (*commonpb.Status, error) {
	if code, ok := c.checkHealthy(); !ok {
		log.Warn("failed to updateTimeTick because rootcoord is not healthy", zap.Any("state", code))
		return failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]), nil
	}
	if in.Base.MsgType != commonpb.MsgType_TimeTick {
		log.Warn("failed to updateTimeTick because base messasge is not timetick, state", zap.Any("base message type", in.Base.MsgType))
		msgTypeName := commonpb.MsgType_name[int32(in.Base.GetMsgType())]
		return failStatus(commonpb.ErrorCode_UnexpectedError, "invalid message type "+msgTypeName), nil
	}
	err := c.chanTimeTick.updateTimeTick(in, "gRPC")
	if err != nil {
		log.Warn("failed to updateTimeTick", zap.String("role", typeutil.RootCoordRole),
			zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		return failStatus(commonpb.ErrorCode_UnexpectedError, "UpdateTimeTick failed: "+err.Error()), nil
	}
	return succStatus(), nil
}

// ReleaseDQLMessageStream release DQL msgstream
func (c *Core) ReleaseDQLMessageStream(ctx context.Context, in *proxypb.ReleaseDQLMessageStreamRequest) (*commonpb.Status, error) {
	if code, ok := c.checkHealthy(); !ok {
		return failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]), nil
	}
	err := c.proxyClientManager.ReleaseDQLMessageStream(ctx, in)
	if err != nil {
		return failStatus(commonpb.ErrorCode_UnexpectedError, err.Error()), nil
	}
	return succStatus(), nil
}

// InvalidateCollectionMetaCache notifies RootCoord to release the collection cache in Proxies.
func (c *Core) InvalidateCollectionMetaCache(ctx context.Context, in *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	if code, ok := c.checkHealthy(); !ok {
		return failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]), nil
	}
	err := c.proxyClientManager.InvalidateCollectionMetaCache(ctx, in)
	if err != nil {
		return failStatus(commonpb.ErrorCode_UnexpectedError, err.Error()), nil
	}
	return succStatus(), nil
}

//// SegmentFlushCompleted check whether segment flush has completed
//func (c *Core) SegmentFlushCompleted(ctx context.Context, in *datapb.SegmentFlushCompletedMsg) (status *commonpb.Status, err error) {
//	if code, ok := c.checkHealthy(); !ok {
//		return failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]), nil
//	}
//	if in.Base.MsgType != commonpb.MsgType_SegmentFlushDone {
//		return failStatus(commonpb.ErrorCode_UnexpectedError, "invalid msg type "+commonpb.MsgType_name[int32(in.Base.MsgType)]), nil
//	}
//
//	log.Info("SegmentFlushCompleted received", zap.Int64("msgID", in.Base.MsgID), zap.Int64("collID", in.Segment.CollectionID),
//		zap.Int64("partID", in.Segment.PartitionID), zap.Int64("segID", in.Segment.ID), zap.Int64s("compactFrom", in.Segment.CompactionFrom))
//
//	err = c.createIndexForSegment(ctx, in.Segment.CollectionID, in.Segment.PartitionID, in.Segment.ID, in.Segment.NumOfRows, in.Segment.Binlogs)
//	if err != nil {
//		log.Error("createIndexForSegment", zap.Int64("msgID", in.Base.MsgID), zap.Int64("collID", in.Segment.CollectionID),
//			zap.Int64("partID", in.Segment.PartitionID), zap.Int64("segID", in.Segment.ID), zap.Error(err))
//		return failStatus(commonpb.ErrorCode_UnexpectedError, err.Error()), nil
//	}
//
//	buildIDs := c.MetaTable.GetBuildIDsBySegIDs(in.Segment.CompactionFrom)
//	if len(buildIDs) != 0 {
//		if err = c.CallRemoveIndexService(ctx, buildIDs); err != nil {
//			log.Error("CallRemoveIndexService failed", zap.Int64("msgID", in.Base.MsgID), zap.Int64("collID", in.Segment.CollectionID),
//				zap.Int64("partID", in.Segment.PartitionID), zap.Int64("segID", in.Segment.ID),
//				zap.Int64s("compactFrom", in.Segment.CompactionFrom), zap.Int64s("buildIDs", buildIDs), zap.Error(err))
//			return failStatus(commonpb.ErrorCode_UnexpectedError, err.Error()), nil
//		}
//	}
//
//	if err = c.MetaTable.RemoveSegments(in.Segment.CollectionID, in.Segment.PartitionID, in.Segment.CompactionFrom); err != nil {
//		log.Error("RemoveSegments failed", zap.Int64("msgID", in.Base.MsgID), zap.Int64("collID", in.Segment.CollectionID),
//			zap.Int64("partID", in.Segment.PartitionID), zap.Int64("segID", in.Segment.ID),
//			zap.Int64s("compactFrom", in.Segment.CompactionFrom), zap.Error(err))
//		return failStatus(commonpb.ErrorCode_UnexpectedError, err.Error()), nil
//	}
//
//	log.Debug("SegmentFlushCompleted success", zap.String("role", typeutil.RootCoordRole),
//		zap.Int64("collection id", in.Segment.CollectionID), zap.Int64("partition id", in.Segment.PartitionID),
//		zap.Int64("segment id", in.Segment.ID), zap.Int64("msgID", in.Base.MsgID))
//	return succStatus(), nil
//}

//ShowConfigurations returns the configurations of RootCoord matching req.Pattern
func (c *Core) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &internalpb.ShowConfigurationsResponse{
			Status:        failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
			Configuations: nil,
		}, nil
	}

	return getComponentConfigurations(ctx, req), nil
}

// GetMetrics get metrics
func (c *Core) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.GetMetricsResponse{
			Status:   failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
			Response: "",
		}, nil
	}

	metricType, err := metricsinfo.ParseMetricType(in.Request)
	if err != nil {
		log.Warn("ParseMetricType failed", zap.String("role", typeutil.RootCoordRole),
			zap.Int64("node_id", c.session.ServerID), zap.String("req", in.Request), zap.Error(err))
		return &milvuspb.GetMetricsResponse{
			Status:   failStatus(commonpb.ErrorCode_UnexpectedError, "ParseMetricType failed: "+err.Error()),
			Response: "",
		}, nil
	}

	log.Debug("GetMetrics success", zap.String("role", typeutil.RootCoordRole),
		zap.String("metric_type", metricType), zap.Int64("msgID", in.GetBase().GetMsgID()))

	if metricType == metricsinfo.SystemInfoMetrics {
		ret, err := c.metricsCacheManager.GetSystemInfoMetrics()
		if err == nil && ret != nil {
			return ret, nil
		}

		log.Warn("GetSystemInfoMetrics from cache failed", zap.String("role", typeutil.RootCoordRole),
			zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Error(err))

		systemInfoMetrics, err := c.getSystemInfoMetrics(ctx, in)
		if err != nil {
			log.Warn("GetSystemInfoMetrics failed", zap.String("role", typeutil.RootCoordRole),
				zap.String("metric_type", metricType), zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Error(err))
			return &milvuspb.GetMetricsResponse{
				Status:   failStatus(commonpb.ErrorCode_UnexpectedError, fmt.Sprintf("getSystemInfoMetrics failed: %s", err.Error())),
				Response: "",
			}, nil
		}

		c.metricsCacheManager.UpdateSystemInfoMetrics(systemInfoMetrics)
		return systemInfoMetrics, err
	}

	log.Warn("GetMetrics failed, metric type not implemented", zap.String("role", typeutil.RootCoordRole),
		zap.String("metric_type", metricType), zap.Int64("msgID", in.GetBase().GetMsgID()))

	return &milvuspb.GetMetricsResponse{
		Status:   failStatus(commonpb.ErrorCode_UnexpectedError, metricsinfo.MsgUnimplementedMetric),
		Response: "",
	}, nil
}

// CreateAlias create collection alias
func (c *Core) CreateAlias(ctx context.Context, in *milvuspb.CreateAliasRequest) (*commonpb.Status, error) {
	metrics.RootCoordDDLReqCounter.WithLabelValues("CreateAlias", metrics.TotalLabel).Inc()
	if code, ok := c.checkHealthy(); !ok {
		return failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]), nil
	}
	tr := timerecord.NewTimeRecorder("CreateAlias")
	log.Debug("CreateAlias", zap.String("role", typeutil.RootCoordRole),
		zap.String("alias", in.Alias), zap.String("collection name", in.CollectionName),
		zap.Int64("msgID", in.Base.MsgID))
	t := &CreateAliasReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			core: c,
		},
		Req: in,
	}
	err := executeTask(t)
	if err != nil {
		log.Error("CreateAlias failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("alias", in.Alias), zap.String("collection name", in.CollectionName),
			zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("CreateAlias", metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, "CreateAlias failed: "+err.Error()), nil
	}
	log.Debug("CreateAlias success", zap.String("role", typeutil.RootCoordRole),
		zap.String("alias", in.Alias), zap.String("collection name", in.CollectionName),
		zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordDDLReqCounter.WithLabelValues("CreateAlias", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("CreateAlias").Observe(float64(tr.ElapseSpan().Milliseconds()))
	return succStatus(), nil
}

// DropAlias drop collection alias
func (c *Core) DropAlias(ctx context.Context, in *milvuspb.DropAliasRequest) (*commonpb.Status, error) {
	metrics.RootCoordDDLReqCounter.WithLabelValues("DropAlias", metrics.TotalLabel).Inc()
	if code, ok := c.checkHealthy(); !ok {
		return failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]), nil
	}
	tr := timerecord.NewTimeRecorder("DropAlias")
	log.Debug("DropAlias", zap.String("role", typeutil.RootCoordRole),
		zap.String("alias", in.Alias), zap.Int64("msgID", in.Base.MsgID))
	t := &DropAliasReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			core: c,
		},
		Req: in,
	}
	err := executeTask(t)
	if err != nil {
		log.Error("DropAlias failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("alias", in.Alias), zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("DropAlias", metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, "DropAlias failed: "+err.Error()), nil
	}
	log.Debug("DropAlias success", zap.String("role", typeutil.RootCoordRole),
		zap.String("alias", in.Alias), zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordDDLReqCounter.WithLabelValues("DropAlias", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("DropAlias").Observe(float64(tr.ElapseSpan().Milliseconds()))
	return succStatus(), nil
}

// AlterAlias alter collection alias
func (c *Core) AlterAlias(ctx context.Context, in *milvuspb.AlterAliasRequest) (*commonpb.Status, error) {
	metrics.RootCoordDDLReqCounter.WithLabelValues("DropAlias", metrics.TotalLabel).Inc()
	if code, ok := c.checkHealthy(); !ok {
		return failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]), nil
	}
	tr := timerecord.NewTimeRecorder("AlterAlias")
	log.Debug("AlterAlias", zap.String("role", typeutil.RootCoordRole),
		zap.String("alias", in.Alias), zap.String("collection name", in.CollectionName),
		zap.Int64("msgID", in.Base.MsgID))
	t := &AlterAliasReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			core: c,
		},
		Req: in,
	}
	err := executeTask(t)
	if err != nil {
		log.Error("AlterAlias failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("alias", in.Alias), zap.String("collection name", in.CollectionName),
			zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("AlterAlias", metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, "AlterAlias failed: "+err.Error()), nil
	}
	log.Debug("AlterAlias success", zap.String("role", typeutil.RootCoordRole),
		zap.String("alias", in.Alias), zap.String("collection name", in.CollectionName),
		zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordDDLReqCounter.WithLabelValues("AlterAlias", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("AlterAlias").Observe(float64(tr.ElapseSpan().Milliseconds()))
	return succStatus(), nil
}

// Import imports large files (json, numpy, etc.) on MinIO/S3 storage into Milvus storage.
func (c *Core) Import(ctx context.Context, req *milvuspb.ImportRequest) (*milvuspb.ImportResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.ImportResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
		}, nil
	}

	// Get collection/partition ID from collection/partition name.
	var cID UniqueID
	var err error
	if cID, err = c.MetaTable.GetCollectionIDByName(req.GetCollectionName()); err != nil {
		log.Error("failed to find collection ID from its name",
			zap.String("collection name", req.GetCollectionName()),
			zap.Error(err))
		return nil, err
	}
	var pID UniqueID
	if pID, err = c.MetaTable.getPartitionByName(cID, req.GetPartitionName(), 0); err != nil {
		log.Error("failed to get partition ID from its name",
			zap.String("partition name", req.GetPartitionName()),
			zap.Error(err))
		return nil, err
	}
	log.Info("RootCoord receive import request",
		zap.String("collection name", req.GetCollectionName()),
		zap.Int64("collection ID", cID),
		zap.String("partition name", req.GetPartitionName()),
		zap.Int64("partition ID", pID),
		zap.Int("# of files = ", len(req.GetFiles())),
		zap.Bool("row-based", req.GetRowBased()),
	)
	resp := c.importManager.importJob(ctx, req, cID, pID)
	return resp, nil
}

// GetImportState returns the current state of an import task.
func (c *Core) GetImportState(ctx context.Context, req *milvuspb.GetImportStateRequest) (*milvuspb.GetImportStateResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.GetImportStateResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
		}, nil
	}
	return c.importManager.getTaskState(req.GetTask()), nil
}

// ListImportTasks returns id array of all import tasks.
func (c *Core) ListImportTasks(ctx context.Context, req *milvuspb.ListImportTasksRequest) (*milvuspb.ListImportTasksResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.ListImportTasksResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
		}, nil
	}

	resp := &milvuspb.ListImportTasksResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Tasks: c.importManager.listAllTasks(),
	}
	return resp, nil
}

// ReportImport reports import task state to RootCoord.
func (c *Core) ReportImport(ctx context.Context, ir *rootcoordpb.ImportResult) (*commonpb.Status, error) {
	log.Info("RootCoord receive import state report",
		zap.Int64("task ID", ir.GetTaskId()),
		zap.Any("import state", ir.GetState()))
	if code, ok := c.checkHealthy(); !ok {
		return failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]), nil
	}
	// Special case for ImportState_ImportAllocSegment state, where we shall only add segment ref lock and do no other
	// operations.
	// TODO: This is inelegant and must get re-structured.
	if ir.GetState() == commonpb.ImportState_ImportAllocSegment {
		// Lock the segments, so we don't lose track of them when compaction happens.
		// Note that these locks will be unlocked in c.postImportPersistLoop() -> checkSegmentLoadedLoop().
		if err := c.CallAddSegRefLock(ctx, ir.GetTaskId(), ir.GetSegments()); err != nil {
			log.Error("failed to acquire segment ref lock", zap.Error(err))
			return &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("failed to acquire segment ref lock %s", err.Error()),
			}, nil
		}
		// Update task store with new segments.
		c.importManager.appendTaskSegments(ir.GetTaskId(), ir.GetSegments())
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		}, nil
	}
	// Upon receiving ReportImport request, update the related task's state in task store.
	ti, err := c.importManager.updateTaskState(ir)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UpdateImportTaskFailure,
			Reason:    err.Error(),
		}, nil
	}

	// This method update a busy node to idle node, and send import task to idle node
	resendTaskFunc := func() {
		func() {
			c.importManager.busyNodesLock.Lock()
			defer c.importManager.busyNodesLock.Unlock()
			delete(c.importManager.busyNodes, ir.GetDatanodeId())
			log.Info("DataNode is no longer busy",
				zap.Int64("dataNode ID", ir.GetDatanodeId()),
				zap.Int64("task ID", ir.GetTaskId()))

		}()
		c.importManager.sendOutTasks(c.importManager.ctx)
	}

	// If task failed, send task to idle datanode
	if ir.GetState() == commonpb.ImportState_ImportFailed {
		// Release segments when task fails.
		log.Info("task failed, release segment ref locks")
		err := retry.Do(ctx, func() error {
			return c.CallReleaseSegRefLock(ctx, ir.GetTaskId(), ir.GetSegments())
		}, retry.Attempts(100))
		if err != nil {
			log.Error("failed to release lock, about to panic!")
			panic(err)
		}
		resendTaskFunc()
	}

	// So much for reporting, unless the task just reached `ImportPersisted` state.
	if ir.GetState() != commonpb.ImportState_ImportPersisted {
		log.Debug("non import-persisted state received, return immediately",
			zap.Any("task ID", ir.GetTaskId()),
			zap.Any("import state", ir.GetState()))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		}, nil
	}

	// Look up collection name on collection ID.
	var colName string
	var colMeta *model.Collection
	if colMeta, err = c.MetaTable.GetCollectionByID(ti.GetCollectionId(), 0); err != nil {
		log.Error("failed to get collection name",
			zap.Int64("collection ID", ti.GetCollectionId()),
			zap.Error(err))
		// In some unexpected cases, user drop collection when bulkload task still in pending list, the datanode become idle.
		// If we directly return, the pending tasks will remain in pending list. So we call resendTaskFunc() to push next pending task to idle datanode.
		resendTaskFunc()
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_CollectionNameNotFound,
			Reason:    "failed to get collection name for collection ID" + strconv.FormatInt(ti.GetCollectionId(), 10),
		}, nil
	}
	colName = colMeta.Name

	// When DataNode has done its thing, remove it from the busy node list. And send import task again
	resendTaskFunc()

	// Flush all import data segments.
	c.CallFlushOnCollection(ctx, ti.GetCollectionId(), ir.GetSegments())
	// Check if data are "queryable" and if indices are built on all segments.
	go c.postImportPersistLoop(c.ctx, ir.GetTaskId(), ti.GetCollectionId(), colName, ir.GetSegments())

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

// CountCompleteIndex checks indexing status of the given segments.
// It returns an error if error occurs. It also returns a boolean indicating whether indexing is done (or if no index
// is needed).
func (c *Core) CountCompleteIndex(ctx context.Context, collectionName string, collectionID UniqueID,
	allSegmentIDs []UniqueID) (bool, error) {
	// Note: Index name is always Params.CommonCfg.DefaultIndexName in current Milvus designs as of today.
	indexName := Params.CommonCfg.DefaultIndexName

	states, err := c.CallGetSegmentIndexStateService(ctx, collectionID, indexName, allSegmentIDs)
	if err != nil {
		log.Error("failed to get index state in checkSegmentIndexStates", zap.Error(err))
		return false, err
	}

	// Count the # of segments with finished index.
	ct := 0
	for _, s := range states {
		if s.State == commonpb.IndexState_Finished {
			ct++
		}
	}
	log.Info("segment indexing state checked",
		//zap.Int64s("segments checked", seg2Check),
		//zap.Int("# of checked segment", len(seg2Check)),
		zap.Int("# of segments with complete index", ct),
		zap.String("collection name", collectionName),
		zap.Int64("collection ID", collectionID),
	)
	return len(allSegmentIDs) == ct, nil
}

func (c *Core) postImportPersistLoop(ctx context.Context, taskID int64, colID int64, colName string, segIDs []UniqueID) {
	// Loop and check if segments are loaded in queryNodes.
	c.wg.Add(1)
	go c.checkSegmentLoadedLoop(ctx, taskID, colID, segIDs)
	// Check if collection has any indexed fields. If so, start a loop to check segments' index states.
	if _, err := c.MetaTable.GetCollectionByID(colID, 0); err != nil {
		log.Error("failed to find meta for collection",
			zap.Int64("collection ID", colID),
			zap.Error(err))
	} else {
		log.Info("start checking index state", zap.Int64("collection ID", colID))
		c.wg.Add(1)
		go c.checkCompleteIndexLoop(ctx, taskID, colID, colName, segIDs)
	}
}

// checkSegmentLoadedLoop loops and checks if all segments in `segIDs` are loaded in queryNodes.
func (c *Core) checkSegmentLoadedLoop(ctx context.Context, taskID int64, colID int64, segIDs []UniqueID) {
	defer c.wg.Done()
	ticker := time.NewTicker(time.Duration(Params.RootCoordCfg.ImportSegmentStateCheckInterval*1000) * time.Millisecond)
	defer ticker.Stop()
	expireTicker := time.NewTicker(time.Duration(Params.RootCoordCfg.ImportSegmentStateWaitLimit*1000) * time.Millisecond)
	defer expireTicker.Stop()
	defer func() {
		log.Info("we are done checking segment loading state, release segment ref locks")
		err := retry.Do(ctx, func() error {
			return c.CallReleaseSegRefLock(ctx, taskID, segIDs)
		}, retry.Attempts(100))
		if err != nil {
			log.Error("failed to release lock, about to panic!")
			panic(err)
		}
	}()
	for {
		select {
		case <-c.ctx.Done():
			log.Info("(in check segment loaded loop) context done, exiting checkSegmentLoadedLoop")
			return
		case <-ticker.C:
			resp, err := c.CallGetSegmentInfoService(ctx, colID, segIDs)
			log.Debug("(in check segment loaded loop)",
				zap.Int64("task ID", taskID),
				zap.Int64("collection ID", colID),
				zap.Int64s("segment IDs expected", segIDs),
				zap.Int("# of segments found", len(resp.GetInfos())))
			if err != nil {
				log.Warn("(in check segment loaded loop) failed to call get segment info on queryCoord",
					zap.Int64("task ID", taskID),
					zap.Int64("collection ID", colID),
					zap.Int64s("segment IDs", segIDs))
			} else if len(resp.GetInfos()) == len(segIDs) {
				// Check if all segment info are loaded in queryNodes.
				log.Info("(in check segment loaded loop) all import data segments loaded in queryNodes",
					zap.Int64("task ID", taskID),
					zap.Int64("collection ID", colID),
					zap.Int64s("segment IDs", segIDs))
				c.importManager.setTaskDataQueryable(taskID)
				return
			}
		case <-expireTicker.C:
			log.Warn("(in check segment loaded loop) segments still not loaded after max wait time",
				zap.Int64("task ID", taskID),
				zap.Int64("collection ID", colID),
				zap.Int64s("segment IDs", segIDs))
			return
		}
	}
}

// checkCompleteIndexLoop loops and checks if all indices are built for an import task's segments.
func (c *Core) checkCompleteIndexLoop(ctx context.Context, taskID int64, colID int64, colName string, segIDs []UniqueID) {
	defer c.wg.Done()
	ticker := time.NewTicker(time.Duration(Params.RootCoordCfg.ImportIndexCheckInterval*1000) * time.Millisecond)
	defer ticker.Stop()
	expireTicker := time.NewTicker(time.Duration(Params.RootCoordCfg.ImportIndexWaitLimit*1000) * time.Millisecond)
	defer expireTicker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			log.Info("(in check complete index loop) context done, exiting checkCompleteIndexLoop")
			return
		case <-ticker.C:
			if done, err := c.CountCompleteIndex(ctx, colName, colID, segIDs); err == nil && done {
				log.Info("(in check complete index loop) indices are built or no index needed",
					zap.Int64("task ID", taskID))
				c.importManager.setTaskDataIndexed(taskID)
				return
			} else if err != nil {
				log.Error("(in check complete index loop) an error occurs",
					zap.Error(err))
			}
		case <-expireTicker.C:
			log.Warn("(in check complete index loop) indexing is taken too long",
				zap.Int64("task ID", taskID),
				zap.Int64("collection ID", colID),
				zap.Int64s("segment IDs", segIDs))
			return
		}
	}
}

// ExpireCredCache will call invalidate credential cache
func (c *Core) ExpireCredCache(ctx context.Context, username string) error {
	req := proxypb.InvalidateCredCacheRequest{
		Base: &commonpb.MsgBase{
			MsgType:  0, //TODO, msg type
			MsgID:    0, //TODO, msg id
			SourceID: c.session.ServerID,
		},
		Username: username,
	}
	return c.proxyClientManager.InvalidateCredentialCache(ctx, &req)
}

// UpdateCredCache will call update credential cache
func (c *Core) UpdateCredCache(ctx context.Context, credInfo *internalpb.CredentialInfo) error {
	req := proxypb.UpdateCredCacheRequest{
		Base: &commonpb.MsgBase{
			MsgType:  0, //TODO, msg type
			MsgID:    0, //TODO, msg id
			SourceID: c.session.ServerID,
		},
		Username: credInfo.Username,
		Password: credInfo.Sha256Password,
	}
	return c.proxyClientManager.UpdateCredentialCache(ctx, &req)
}

// CreateCredential create new user and password
// 	1. decode ciphertext password to raw password
// 	2. encrypt raw password
// 	3. save in to etcd
func (c *Core) CreateCredential(ctx context.Context, credInfo *internalpb.CredentialInfo) (*commonpb.Status, error) {
	method := "CreateCredential"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	log.Debug("CreateCredential", zap.String("role", typeutil.RootCoordRole),
		zap.String("username", credInfo.Username))

	// insert to db
	err := c.MetaTable.AddCredential(credInfo)
	if err != nil {
		log.Error("CreateCredential save credential failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("username", credInfo.Username), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_CreateCredentialFailure, "CreateCredential failed: "+err.Error()), nil
	}
	// update proxy's local cache
	err = c.UpdateCredCache(ctx, credInfo)
	if err != nil {
		log.Warn("CreateCredential add cache failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("username", credInfo.Username), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
	}
	log.Debug("CreateCredential success", zap.String("role", typeutil.RootCoordRole),
		zap.String("username", credInfo.Username))

	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfCredentials.Inc()
	return succStatus(), nil
}

// GetCredential get credential by username
func (c *Core) GetCredential(ctx context.Context, in *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error) {
	method := "GetCredential"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	log.Debug("GetCredential", zap.String("role", typeutil.RootCoordRole),
		zap.String("username", in.Username))

	credInfo, err := c.MetaTable.GetCredential(in.Username)
	if err != nil {
		log.Error("GetCredential query credential failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("username", in.Username), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return &rootcoordpb.GetCredentialResponse{
			Status: failStatus(commonpb.ErrorCode_GetCredentialFailure, "GetCredential failed: "+err.Error()),
		}, err
	}
	log.Debug("GetCredential success", zap.String("role", typeutil.RootCoordRole),
		zap.String("username", in.Username))

	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &rootcoordpb.GetCredentialResponse{
		Status:   succStatus(),
		Username: credInfo.Username,
		Password: credInfo.EncryptedPassword,
	}, nil
}

// UpdateCredential update password for a user
func (c *Core) UpdateCredential(ctx context.Context, credInfo *internalpb.CredentialInfo) (*commonpb.Status, error) {
	method := "UpdateCredential"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	log.Debug("UpdateCredential", zap.String("role", typeutil.RootCoordRole),
		zap.String("username", credInfo.Username))
	// update data on storage
	err := c.MetaTable.AlterCredential(credInfo)
	if err != nil {
		log.Error("UpdateCredential save credential failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("username", credInfo.Username), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UpdateCredentialFailure, "UpdateCredential failed: "+err.Error()), nil
	}
	// update proxy's local cache
	err = c.UpdateCredCache(ctx, credInfo)
	if err != nil {
		log.Error("UpdateCredential update cache failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("username", credInfo.Username), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UpdateCredentialFailure, "UpdateCredential failed: "+err.Error()), nil
	}
	log.Debug("UpdateCredential success", zap.String("role", typeutil.RootCoordRole),
		zap.String("username", credInfo.Username))

	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return succStatus(), nil
}

// DeleteCredential delete a user
func (c *Core) DeleteCredential(ctx context.Context, in *milvuspb.DeleteCredentialRequest) (*commonpb.Status, error) {
	method := "DeleteCredential"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)

	// delete data on storage
	err := c.MetaTable.DeleteCredential(in.Username)
	if err != nil {
		log.Error("DeleteCredential remove credential failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("username", in.Username), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_DeleteCredentialFailure, "DeleteCredential failed: "+err.Error()), err
	}
	// invalidate proxy's local cache
	err = c.ExpireCredCache(ctx, in.Username)
	if err != nil {
		log.Error("DeleteCredential expire credential cache failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("username", in.Username), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_DeleteCredentialFailure, "DeleteCredential failed: "+err.Error()), nil
	}
	log.Debug("DeleteCredential success", zap.String("role", typeutil.RootCoordRole),
		zap.String("username", in.Username))

	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfCredentials.Dec()
	return succStatus(), nil
}

// ListCredUsers list all usernames
func (c *Core) ListCredUsers(ctx context.Context, in *milvuspb.ListCredUsersRequest) (*milvuspb.ListCredUsersResponse, error) {
	method := "ListCredUsers"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)

	credInfo, err := c.MetaTable.ListCredentialUsernames()
	if err != nil {
		log.Error("ListCredUsers query usernames failed", zap.String("role", typeutil.RootCoordRole),
			zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return &milvuspb.ListCredUsersResponse{
			Status: failStatus(commonpb.ErrorCode_ListCredUsersFailure, "ListCredUsers failed: "+err.Error()),
		}, err
	}
	log.Debug("ListCredUsers success", zap.String("role", typeutil.RootCoordRole))

	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &milvuspb.ListCredUsersResponse{
		Status:    succStatus(),
		Usernames: credInfo.Usernames,
	}, nil
}

// CreateRole create role
// - check the node health
// - check if the role is existed
// - check if the role num has reached the limit
// - create the role by the metatable api
func (c *Core) CreateRole(ctx context.Context, in *milvuspb.CreateRoleRequest) (*commonpb.Status, error) {
	method := "CreateRole"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	logger.Debug(method, zap.Any("in", in))

	if code, ok := c.checkHealthy(); !ok {
		return errorutil.UnhealthyStatus(code), errorutil.UnhealthyError()
	}
	entity := in.Entity

	err := c.MetaTable.CreateRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: entity.Name})
	if err != nil {
		errMsg := "fail to create role"
		log.Error(errMsg, zap.Any("in", in), zap.Error(err))
		return failStatus(commonpb.ErrorCode_CreateRoleFailure, errMsg), nil
	}

	logger.Debug(method+" success", zap.String("role_name", entity.Name))
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfRoles.Inc()

	return succStatus(), nil
}

// DropRole drop role
// - check the node health
// - check if the role name is existed
// - check if the role has some grant info
// - get all role mapping of this role
// - drop these role mappings
// - drop the role by the metatable api
func (c *Core) DropRole(ctx context.Context, in *milvuspb.DropRoleRequest) (*commonpb.Status, error) {
	method := "DropRole"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	logger.Debug(method, zap.Any("in", in))

	if code, ok := c.checkHealthy(); !ok {
		return errorutil.UnhealthyStatus(code), errorutil.UnhealthyError()
	}
	if _, err := c.MetaTable.SelectRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: in.RoleName}, false); err != nil {
		errMsg := "the role isn't existed"
		log.Error(errMsg, zap.Any("in", in), zap.Error(err))
		return failStatus(commonpb.ErrorCode_DropRoleFailure, errMsg), nil
	}

	grantEntities, err := c.MetaTable.SelectGrant(util.DefaultTenant, &milvuspb.GrantEntity{
		Role: &milvuspb.RoleEntity{Name: in.RoleName},
	})
	if len(grantEntities) != 0 {
		errMsg := "fail to drop the role that it has privileges. Use REVOKE API to revoke privileges"
		log.Error(errMsg, zap.Any("in", in), zap.Error(err))
		return failStatus(commonpb.ErrorCode_DropRoleFailure, errMsg), nil
	}
	roleResults, err := c.MetaTable.SelectRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: in.RoleName}, true)
	if err != nil {
		errMsg := "fail to select a role by role name"
		log.Error(errMsg, zap.Any("in", in), zap.Error(err))
		return failStatus(commonpb.ErrorCode_DropRoleFailure, errMsg), nil
	}
	logger.Debug("role to user info", zap.Int("counter", len(roleResults)))
	for _, roleResult := range roleResults {
		for index, userEntity := range roleResult.Users {
			if err = c.MetaTable.OperateUserRole(util.DefaultTenant,
				&milvuspb.UserEntity{Name: userEntity.Name},
				&milvuspb.RoleEntity{Name: roleResult.Role.Name}, milvuspb.OperateUserRoleType_RemoveUserFromRole); err != nil {
				if common.IsIgnorableError(err) {
					continue
				}
				errMsg := "fail to remove user from role"
				log.Error(errMsg, zap.Any("in", in), zap.String("role_name", roleResult.Role.Name), zap.String("username", userEntity.Name), zap.Int("current_index", index), zap.Error(err))
				return failStatus(commonpb.ErrorCode_OperateUserRoleFailure, errMsg), nil
			}
		}
	}
	if err = c.MetaTable.DropGrant(util.DefaultTenant, &milvuspb.RoleEntity{Name: in.RoleName}); err != nil {
		errMsg := "fail to drop the grant"
		log.Error(errMsg, zap.Any("in", in), zap.Error(err))
		return failStatus(commonpb.ErrorCode_DropRoleFailure, errMsg), nil
	}
	if err = c.MetaTable.DropRole(util.DefaultTenant, in.RoleName); err != nil {
		errMsg := "fail to drop the role"
		log.Error(errMsg, zap.Any("in", in), zap.Error(err))
		return failStatus(commonpb.ErrorCode_DropRoleFailure, errMsg), nil
	}

	logger.Debug(method+" success", zap.String("role_name", in.RoleName))
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfRoles.Dec()
	return succStatus(), nil
}

// OperateUserRole operate the relationship between a user and a role
// - check the node health
// - check if the role is valid
// - check if the user is valid
// - operate the user-role by the metatable api
// - update the policy cache
func (c *Core) OperateUserRole(ctx context.Context, in *milvuspb.OperateUserRoleRequest) (*commonpb.Status, error) {
	method := "OperateUserRole-" + in.Type.String()
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	logger.Debug(method, zap.Any("in", in))

	if code, ok := c.checkHealthy(); !ok {
		return errorutil.UnhealthyStatus(code), errorutil.UnhealthyError()
	}

	if _, err := c.MetaTable.SelectRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: in.RoleName}, false); err != nil {
		errMsg := "fail to check the role name"
		log.Error(errMsg, zap.Any("in", in), zap.Error(err))
		return failStatus(commonpb.ErrorCode_OperateUserRoleFailure, errMsg), nil
	}
	if _, err := c.MetaTable.SelectUser(util.DefaultTenant, &milvuspb.UserEntity{Name: in.Username}, false); err != nil {
		errMsg := "fail to check the username"
		log.Error(errMsg, zap.Any("in", in), zap.Error(err))
		return failStatus(commonpb.ErrorCode_OperateUserRoleFailure, errMsg), nil
	}
	updateCache := true
	if err := c.MetaTable.OperateUserRole(util.DefaultTenant, &milvuspb.UserEntity{Name: in.Username}, &milvuspb.RoleEntity{Name: in.RoleName}, in.Type); err != nil {
		if !common.IsIgnorableError(err) {
			errMsg := "fail to operate user to role"
			log.Error(errMsg, zap.Any("in", in), zap.Error(err))
			return failStatus(commonpb.ErrorCode_OperateUserRoleFailure, errMsg), nil
		}
		updateCache = false
	}

	if updateCache {
		var opType int32
		switch in.Type {
		case milvuspb.OperateUserRoleType_AddUserToRole:
			opType = int32(typeutil.CacheAddUserToRole)
		case milvuspb.OperateUserRoleType_RemoveUserFromRole:
			opType = int32(typeutil.CacheRemoveUserFromRole)
		default:
			errMsg := "invalid operate type for the OperateUserRole api"
			log.Error(errMsg, zap.Any("in", in))
			return failStatus(commonpb.ErrorCode_OperateUserRoleFailure, errMsg), nil
		}
		if err := c.proxyClientManager.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
			OpType: opType,
			OpKey:  funcutil.EncodeUserRoleCache(in.Username, in.RoleName),
		}); err != nil {
			errMsg := "fail to refresh policy info cache"
			log.Error(errMsg, zap.Any("in", in), zap.Error(err))
			return failStatus(commonpb.ErrorCode_OperateUserRoleFailure, errMsg), nil
		}
	}

	logger.Debug(method + " success")
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return succStatus(), nil
}

// SelectRole select role
// - check the node health
// - check if the role is valid when this param is provided
// - select role by the metatable api
func (c *Core) SelectRole(ctx context.Context, in *milvuspb.SelectRoleRequest) (*milvuspb.SelectRoleResponse, error) {
	method := "SelectRole"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	logger.Debug(method, zap.Any("in", in))

	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.SelectRoleResponse{Status: errorutil.UnhealthyStatus(code)}, errorutil.UnhealthyError()
	}

	if in.Role != nil {
		if _, err := c.MetaTable.SelectRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: in.Role.Name}, false); err != nil {
			if common.IsKeyNotExistError(err) {
				return &milvuspb.SelectRoleResponse{
					Status: succStatus(),
				}, nil
			}
			errMsg := "fail to select the role to check the role name"
			log.Error(errMsg, zap.Any("in", in), zap.Error(err))
			return &milvuspb.SelectRoleResponse{
				Status: failStatus(commonpb.ErrorCode_SelectRoleFailure, errMsg),
			}, nil
		}
	}
	roleResults, err := c.MetaTable.SelectRole(util.DefaultTenant, in.Role, in.IncludeUserInfo)
	if err != nil {
		errMsg := "fail to select the role"
		log.Error(errMsg, zap.Any("in", in), zap.Error(err))
		return &milvuspb.SelectRoleResponse{
			Status: failStatus(commonpb.ErrorCode_SelectRoleFailure, errMsg),
		}, nil
	}

	logger.Debug(method + " success")
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &milvuspb.SelectRoleResponse{
		Status:  succStatus(),
		Results: roleResults,
	}, nil
}

// SelectUser select user
// - check the node health
// - check if the user is valid when this param is provided
// - select user by the metatable api
func (c *Core) SelectUser(ctx context.Context, in *milvuspb.SelectUserRequest) (*milvuspb.SelectUserResponse, error) {
	method := "SelectUser"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	logger.Debug(method, zap.Any("in", in))

	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.SelectUserResponse{Status: errorutil.UnhealthyStatus(code)}, errorutil.UnhealthyError()
	}

	if in.User != nil {
		if _, err := c.MetaTable.SelectUser(util.DefaultTenant, &milvuspb.UserEntity{Name: in.User.Name}, false); err != nil {
			if common.IsKeyNotExistError(err) {
				return &milvuspb.SelectUserResponse{
					Status: succStatus(),
				}, nil
			}
			errMsg := "fail to select the user to check the username"
			log.Error(errMsg, zap.Any("in", in), zap.Error(err))
			return &milvuspb.SelectUserResponse{
				Status: failStatus(commonpb.ErrorCode_SelectUserFailure, errMsg),
			}, nil
		}
	}
	userResults, err := c.MetaTable.SelectUser(util.DefaultTenant, in.User, in.IncludeRoleInfo)
	if err != nil {
		errMsg := "fail to select the user"
		log.Error(errMsg, zap.Any("in", in), zap.Error(err))
		return &milvuspb.SelectUserResponse{
			Status: failStatus(commonpb.ErrorCode_SelectUserFailure, errMsg),
		}, nil
	}

	logger.Debug(method + " success")
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &milvuspb.SelectUserResponse{
		Status:  succStatus(),
		Results: userResults,
	}, nil
}

func (c *Core) isValidRole(entity *milvuspb.RoleEntity) error {
	if entity == nil {
		return errors.New("the role entity is nil")
	}
	if entity.Name == "" {
		return errors.New("the name in the role entity is empty")
	}
	if _, err := c.MetaTable.SelectRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: entity.Name}, false); err != nil {
		return err
	}
	return nil
}

func (c *Core) isValidObject(entity *milvuspb.ObjectEntity) error {
	if entity == nil {
		return errors.New("the object entity is nil")
	}
	if _, ok := commonpb.ObjectType_value[entity.Name]; !ok {
		return fmt.Errorf("the object type in the object entity[name: %s] is invalid", entity.Name)
	}
	return nil
}

func (c *Core) isValidGrantor(entity *milvuspb.GrantorEntity, object string) error {
	if entity == nil {
		return errors.New("the grantor entity is nil")
	}
	if entity.User == nil {
		return errors.New("the user entity in the grantor entity is nil")
	}
	if entity.User.Name == "" {
		return errors.New("the name in the user entity of the grantor entity is empty")
	}
	if _, err := c.MetaTable.SelectUser(util.DefaultTenant, &milvuspb.UserEntity{Name: entity.User.Name}, false); err != nil {
		return err
	}
	if entity.Privilege == nil {
		return errors.New("the privilege entity in the grantor entity is nil")
	}
	if util.IsAnyWord(entity.Privilege.Name) {
		return nil
	}
	if privilegeName := util.PrivilegeNameForMetastore(entity.Privilege.Name); privilegeName == "" {
		return fmt.Errorf("the privilege name[%s] in the privilege entity is invalid", entity.Privilege.Name)
	}
	privileges, ok := util.ObjectPrivileges[object]
	if !ok {
		return fmt.Errorf("the object type[%s] is invalid", object)
	}
	for _, privilege := range privileges {
		if privilege == entity.Privilege.Name {
			return nil
		}
	}
	return fmt.Errorf("the privilege name[%s] is invalid", entity.Privilege.Name)
}

// OperatePrivilege operate the privilege, including grant and revoke
// - check the node health
// - check if the operating type is valid
// - check if the entity is nil
// - check if the params, including the resource entity, the principal entity, the grantor entity, is valid
// - operate the privilege by the metatable api
// - update the policy cache
func (c *Core) OperatePrivilege(ctx context.Context, in *milvuspb.OperatePrivilegeRequest) (*commonpb.Status, error) {
	method := "OperatePrivilege"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	logger.Debug(method, zap.Any("in", in))

	if code, ok := c.checkHealthy(); !ok {
		return errorutil.UnhealthyStatus(code), errorutil.UnhealthyError()
	}
	if in.Type != milvuspb.OperatePrivilegeType_Grant && in.Type != milvuspb.OperatePrivilegeType_Revoke {
		errMsg := fmt.Sprintf("invalid operate privilege type, current type: %s, valid value: [%s, %s]", in.Type, milvuspb.OperatePrivilegeType_Grant, milvuspb.OperatePrivilegeType_Revoke)
		log.Error(errMsg, zap.Any("in", in))
		return failStatus(commonpb.ErrorCode_OperatePrivilegeFailure, errMsg), nil
	}
	if in.Entity == nil {
		errMsg := "the grant entity in the request is nil"
		log.Error(errMsg, zap.Any("in", in))
		return failStatus(commonpb.ErrorCode_OperatePrivilegeFailure, errMsg), nil
	}
	if err := c.isValidObject(in.Entity.Object); err != nil {
		log.Error("", zap.Error(err))
		return failStatus(commonpb.ErrorCode_OperatePrivilegeFailure, err.Error()), nil
	}
	if err := c.isValidRole(in.Entity.Role); err != nil {
		log.Error("", zap.Error(err))
		return failStatus(commonpb.ErrorCode_OperatePrivilegeFailure, err.Error()), nil
	}
	if err := c.isValidGrantor(in.Entity.Grantor, in.Entity.Object.Name); err != nil {
		log.Error("", zap.Error(err))
		return failStatus(commonpb.ErrorCode_OperatePrivilegeFailure, err.Error()), nil
	}

	logger.Debug("before PrivilegeNameForMetastore", zap.String("privilege", in.Entity.Grantor.Privilege.Name))
	if !util.IsAnyWord(in.Entity.Grantor.Privilege.Name) {
		in.Entity.Grantor.Privilege.Name = util.PrivilegeNameForMetastore(in.Entity.Grantor.Privilege.Name)
	}
	logger.Debug("after PrivilegeNameForMetastore", zap.String("privilege", in.Entity.Grantor.Privilege.Name))
	if in.Entity.Object.Name == commonpb.ObjectType_Global.String() {
		in.Entity.ObjectName = util.AnyWord
	}
	updateCache := true
	if err := c.MetaTable.OperatePrivilege(util.DefaultTenant, in.Entity, in.Type); err != nil {
		if !common.IsIgnorableError(err) {
			errMsg := "fail to operate the privilege"
			log.Error(errMsg, zap.Any("in", in), zap.Error(err))
			return failStatus(commonpb.ErrorCode_OperatePrivilegeFailure, errMsg), nil
		}
		updateCache = false
	}

	if updateCache {
		var opType int32
		switch in.Type {
		case milvuspb.OperatePrivilegeType_Grant:
			opType = int32(typeutil.CacheGrantPrivilege)
		case milvuspb.OperatePrivilegeType_Revoke:
			opType = int32(typeutil.CacheRevokePrivilege)
		default:
			errMsg := "invalid operate type for the OperatePrivilege api"
			log.Error(errMsg, zap.Any("in", in))
			return failStatus(commonpb.ErrorCode_OperatePrivilegeFailure, errMsg), nil
		}
		if err := c.proxyClientManager.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
			OpType: opType,
			OpKey:  funcutil.PolicyForPrivilege(in.Entity.Role.Name, in.Entity.Object.Name, in.Entity.ObjectName, in.Entity.Grantor.Privilege.Name),
		}); err != nil {
			errMsg := "fail to refresh policy info cache"
			log.Error(errMsg, zap.Any("in", in), zap.Error(err))
			return failStatus(commonpb.ErrorCode_OperatePrivilegeFailure, errMsg), nil
		}
	}

	logger.Debug(method + " success")
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return succStatus(), nil
}

// SelectGrant select grant
// - check the node health
// - check if the principal entity is valid
// - check if the resource entity which is provided by the user is valid
// - select grant by the metatable api
func (c *Core) SelectGrant(ctx context.Context, in *milvuspb.SelectGrantRequest) (*milvuspb.SelectGrantResponse, error) {
	method := "SelectGrant"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	logger.Debug(method, zap.Any("in", in))

	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.SelectGrantResponse{
			Status: errorutil.UnhealthyStatus(code),
		}, errorutil.UnhealthyError()
	}
	if in.Entity == nil {
		errMsg := "the grant entity in the request is nil"
		log.Error(errMsg, zap.Any("in", in))
		return &milvuspb.SelectGrantResponse{
			Status: failStatus(commonpb.ErrorCode_SelectGrantFailure, errMsg),
		}, nil
	}
	if err := c.isValidRole(in.Entity.Role); err != nil {
		log.Error("", zap.Any("in", in), zap.Error(err))
		return &milvuspb.SelectGrantResponse{
			Status: failStatus(commonpb.ErrorCode_SelectGrantFailure, err.Error()),
		}, nil
	}
	if in.Entity.Object != nil {
		if err := c.isValidObject(in.Entity.Object); err != nil {
			log.Error("", zap.Any("in", in), zap.Error(err))
			return &milvuspb.SelectGrantResponse{
				Status: failStatus(commonpb.ErrorCode_SelectGrantFailure, err.Error()),
			}, nil
		}
	}

	grantEntities, err := c.MetaTable.SelectGrant(util.DefaultTenant, in.Entity)
	if common.IsKeyNotExistError(err) {
		return &milvuspb.SelectGrantResponse{
			Status: succStatus(),
		}, nil
	}
	if err != nil {
		errMsg := "fail to select the grant"
		log.Error(errMsg, zap.Any("in", in), zap.Error(err))
		return &milvuspb.SelectGrantResponse{
			Status: failStatus(commonpb.ErrorCode_SelectGrantFailure, errMsg),
		}, nil
	}

	logger.Debug(method + " success")
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &milvuspb.SelectGrantResponse{
		Status:   succStatus(),
		Entities: grantEntities,
	}, nil
}

func (c *Core) ListPolicy(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
	method := "PolicyList"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	logger.Debug(method, zap.Any("in", in))

	if code, ok := c.checkHealthy(); !ok {
		return &internalpb.ListPolicyResponse{
			Status: errorutil.UnhealthyStatus(code),
		}, errorutil.UnhealthyError()
	}

	policies, err := c.MetaTable.ListPolicy(util.DefaultTenant)
	if err != nil {
		errMsg := "fail to list policy"
		log.Error(errMsg, zap.Any("in", in), zap.Error(err))
		return &internalpb.ListPolicyResponse{
			Status: failStatus(commonpb.ErrorCode_ListPolicyFailure, errMsg),
		}, nil
	}
	userRoles, err := c.MetaTable.ListUserRole(util.DefaultTenant)
	if err != nil {
		errMsg := "fail to list user-role"
		log.Error(errMsg, zap.Any("in", in), zap.Error(err))
		return &internalpb.ListPolicyResponse{
			Status: failStatus(commonpb.ErrorCode_ListPolicyFailure, "fail to list user-role"),
		}, nil
	}

	logger.Debug(method + " success")
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &internalpb.ListPolicyResponse{
		Status:      succStatus(),
		PolicyInfos: policies,
		UserRoles:   userRoles,
	}, nil
}

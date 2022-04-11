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
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/milvus-io/milvus/internal/util/dependency"

	"github.com/golang/protobuf/proto"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	ms "github.com/milvus-io/milvus/internal/mq/msgstream"
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
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// UniqueID is an alias of typeutil.UniqueID.
type UniqueID = typeutil.UniqueID

// ------------------ struct -----------------------

// DdOperation used to save ddMsg into etcd
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

	//get binlog file path from data service,
	CallGetBinlogFilePathsService func(ctx context.Context, segID typeutil.UniqueID, fieldID typeutil.UniqueID) ([]string, error)
	CallGetNumRowsService         func(ctx context.Context, segID typeutil.UniqueID, isFromFlushedChan bool) (int64, error)
	CallGetFlushedSegmentsService func(ctx context.Context, collID, partID typeutil.UniqueID) ([]typeutil.UniqueID, error)

	//call index builder's client to build index, return build id or get index state.
	CallBuildIndexService     func(ctx context.Context, binlog []string, field *schemapb.FieldSchema, idxInfo *etcdpb.IndexInfo, numRows int64) (typeutil.UniqueID, error)
	CallDropIndexService      func(ctx context.Context, indexID typeutil.UniqueID) error
	CallGetIndexStatesService func(ctx context.Context, IndexBuildIDs []int64) ([]*indexpb.IndexInfo, error)

	NewProxyClient func(sess *sessionutil.Session) (types.Proxy, error)

	//query service interface, notify query service to release collection
	CallReleaseCollectionService func(ctx context.Context, ts typeutil.Timestamp, dbID, collectionID typeutil.UniqueID) error
	CallReleasePartitionService  func(ctx context.Context, ts typeutil.Timestamp, dbID, collectionID typeutil.UniqueID, partitionIDs []typeutil.UniqueID) error

	CallWatchChannels func(ctx context.Context, collectionID int64, channelNames []string) error

	// Update segment state.
	CallUpdateSegmentStateService func(ctx context.Context, segID typeutil.UniqueID, ss commonpb.SegmentState) error

	//assign import task to data service
	CallImportService func(ctx context.Context, req *datapb.ImportTaskRequest) *datapb.ImportTaskResponse

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
	if c.CallGetBinlogFilePathsService == nil {
		return fmt.Errorf("callGetBinlogFilePathsService is nil")
	}
	if c.CallGetNumRowsService == nil {
		return fmt.Errorf("callGetNumRowsService is nil")
	}
	if c.CallBuildIndexService == nil {
		return fmt.Errorf("callBuildIndexService is nil")
	}
	if c.CallDropIndexService == nil {
		return fmt.Errorf("callDropIndexService is nil")
	}
	if c.CallGetFlushedSegmentsService == nil {
		return fmt.Errorf("callGetFlushedSegmentsService is nil")
	}
	if c.CallUpdateSegmentStateService == nil {
		return fmt.Errorf("CallUpdateSegmentStateService is nil")
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
			metrics.RootCoordETCDTimestampAllocCounter.Set(float64(ts.Unix()))
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

func (c *Core) checkFlushedSegmentsLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(10 * time.Minute)
	for {
		select {
		case <-c.ctx.Done():
			log.Debug("RootCoord context done, exit check FlushedSegmentsLoop")
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
				log.Debug("failed to get flushed segments from data coord",
					zap.Int64("collection id", collMeta.ID),
					zap.Int64("partition id", partID),
					zap.Error(err))
				cancel2()
				continue
			}
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
						log.Debug("index meta does not exist", zap.Int64("index_id", idxInfo.IndexID))
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
					log.Debug("building index by background checker",
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
					if err := c.MetaTable.AddIndex(&info); err != nil {
						log.Debug("Add index into meta table failed",
							zap.Int64("collection_id", collMeta.ID),
							zap.Int64("index_id", info.IndexID),
							zap.Int64("build_id", info.BuildID),
							zap.Error(err))
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
			log.Debug("failed to get flushed segments from data coord", zap.Int64("collection_id", collID), zap.Int64("partition_id", partID), zap.Error(err))
			return nil, err
		}
	}

	metrics.RootCoordNumOfSegments.WithLabelValues(strconv.FormatInt(collID, 10)).Set(float64(len(segID2PartID)))
	return segID2PartID, nil
}

func (c *Core) setMsgStreams() error {
	if Params.PulsarCfg.Address == "" {
		return fmt.Errorf("pulsar address is empty")
	}
	if Params.CommonCfg.RootCoordSubName == "" {
		return fmt.Errorf("RootCoordSubName is empty")
	}

	// rootcoord time tick channel
	if Params.CommonCfg.RootCoordTimeTick == "" {
		return fmt.Errorf("timeTickChannel is empty")
	}
	timeTickStream, _ := c.factory.NewMsgStream(c.ctx)
	metrics.RootCoordNumOfMsgStream.Inc()
	timeTickStream.AsProducer([]string{Params.CommonCfg.RootCoordTimeTick})
	log.Debug("RootCoord register timetick producer success", zap.String("channel name", Params.CommonCfg.RootCoordTimeTick))

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
		//log.Debug("update timetick",
		//	zap.Any("DefaultTs", t),
		//	zap.Any("sourceID", c.session.ServerID),
		//	zap.Any("reason", reason))
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
	c.CallGetBinlogFilePathsService = func(ctx context.Context, segID typeutil.UniqueID, fieldID typeutil.UniqueID) (retFiles []string, retErr error) {
		defer func() {
			if err := recover(); err != nil {
				retErr = fmt.Errorf("get bin log file paths panic, msg = %v", err)
			}
		}()
		<-initCh //wait connect to DataCoord
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
			return nil, fmt.Errorf("getInsertBinlogPaths from data service failed, error = %s", binlog.Status.Reason)
		}
		binlogPaths := make([]string, 0)
		for i := range binlog.FieldIDs {
			if binlog.FieldIDs[i] == fieldID {
				binlogPaths = append(binlogPaths, binlog.Paths[i].Values...)
			}
		}
		if len(binlogPaths) == 0 {
			return nil, fmt.Errorf("binlog file does not exist, segment id = %d, field id = %d", segID, fieldID)
		}
		return binlogPaths, nil
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
			return retRows, fmt.Errorf("getSegmentInfo from data service failed, error = %s", segInfo.Status.Reason)
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
			return nil, err
		}
		if rsp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return nil, fmt.Errorf("get flushed segments from data coord failed, reason = %s", rsp.Status.Reason)
		}
		return rsp.Segments, nil
	}

	c.CallUpdateSegmentStateService = func(ctx context.Context, segID typeutil.UniqueID, ss commonpb.SegmentState) (retErr error) {
		defer func() {
			if err := recover(); err != nil {
				retErr = fmt.Errorf("update segment state from data coord panic, msg = %v", err)
			}
		}()
		<-initCh
		req := &datapb.SetSegmentStateRequest{
			SegmentId: segID,
			NewState:  ss,
		}
		resp, err := s.SetSegmentState(ctx, req)
		if err != nil || resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			log.Error("failed to update segment state",
				zap.Any("request", req), zap.Any("response", resp), zap.Error(err))
			return err
		}
		log.Info("successfully set segment state",
			zap.Int64("segment ID", req.GetSegmentId()),
			zap.String("new segment state", req.GetNewState().String()))
		return nil
	}

	c.CallWatchChannels = func(ctx context.Context, collectionID int64, channelNames []string) (retErr error) {
		defer func() {
			if err := recover(); err != nil {
				retErr = fmt.Errorf("watch channels panic, msg = %v", err)
			}
		}()
		<-initCh
		req := &datapb.WatchChannelsRequest{
			CollectionID: collectionID,
			ChannelNames: channelNames,
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
		if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return resp
		}

		return resp
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
					log.Debug("RootCoord connected to IndexCoord")
					return
				}
			}
			log.Debug("Retrying RootCoord connection to IndexCoord")
		}
	}()

	c.CallBuildIndexService = func(ctx context.Context, binlog []string, field *schemapb.FieldSchema, idxInfo *etcdpb.IndexInfo, numRows int64) (retID typeutil.UniqueID, retErr error) {
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
			NumRows:     numRows,
			FieldSchema: field,
		})
		if err != nil {
			return retID, err
		}
		if rsp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return retID, fmt.Errorf("buildIndex from index service failed, error = %s", rsp.Status.Reason)
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

	c.CallGetIndexStatesService = func(ctx context.Context, IndexBuildIDs []int64) (idxInfo []*indexpb.IndexInfo, retErr error) {
		defer func() {
			if err := recover(); err != nil {
				retErr = fmt.Errorf("get index state from index service panic, msg = %v", err)
			}
		}()
		<-initCh
		res, err := s.GetIndexStates(ctx, &indexpb.GetIndexStatesRequest{
			IndexBuildIDs: IndexBuildIDs,
		})
		if err != nil {
			log.Error("RootCoord failed to get index states from IndexCoord.", zap.Error(err))
			return nil, err
		}
		log.Debug("got index states", zap.String("get index state result", res.String()))
		if res.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			log.Error("Get index states failed.",
				zap.String("error_code", res.GetStatus().GetErrorCode().String()),
				zap.String("reason", res.GetStatus().GetReason()))
			return nil, fmt.Errorf(res.GetStatus().GetErrorCode().String())
		}
		return res.GetStates(), nil
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
	return nil
}

// BuildIndex will check row num and call build index service
func (c *Core) BuildIndex(ctx context.Context, segID typeutil.UniqueID, field *schemapb.FieldSchema, idxInfo *etcdpb.IndexInfo, isFlush bool) (typeutil.UniqueID, error) {
	log.Debug("start build index", zap.String("index name", idxInfo.IndexName),
		zap.String("field name", field.Name), zap.Int64("segment id", segID))
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
	if rows < Params.RootCoordCfg.MinSegmentSizeToEnableIndex {
		log.Debug("num of rows is less than MinSegmentSizeToEnableIndex", zap.Int64("num rows", rows))
	} else {
		binlogs, err := c.CallGetBinlogFilePathsService(ctx, segID, field.FieldID)
		if err != nil {
			return 0, err
		}
		bldID, err = c.CallBuildIndexService(ctx, binlogs, field, idxInfo, rows)
		if err != nil {
			return 0, err
		}

		log.Debug("CallBuildIndex finished", zap.String("index name", idxInfo.IndexName),
			zap.String("field name", field.Name), zap.Int64("segment id", segID), zap.Int64("num rows", rows))
	}
	return bldID, nil
}

// RemoveIndex will call drop index service
func (c *Core) RemoveIndex(ctx context.Context, collName string, indexName string) error {
	_, indexInfos, err := c.MetaTable.GetIndexByName(collName, indexName)
	if err != nil {
		log.Error("GetIndexByName failed,", zap.String("collection name", collName),
			zap.String("index name", indexName), zap.Error(err))
		return err
	}
	for _, indexInfo := range indexInfos {
		if err = c.CallDropIndexService(ctx, indexInfo.IndexID); err != nil {
			log.Error("CallDropIndexService failed,", zap.String("collection name", collName), zap.Error(err))
			return err
		}
	}
	return nil
}

// ExpireMetaCache will call invalidate collection meta cache
func (c *Core) ExpireMetaCache(ctx context.Context, collNames []string, ts typeutil.Timestamp) {
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
		c.proxyClientManager.InvalidateCollectionMetaCache(ctx, &req)
	}
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
			var metaKV kv.TxnKV
			metaKV, initError = c.kvBaseCreate(Params.EtcdCfg.MetaRootPath)
			if initError != nil {
				log.Error("RootCoord failed to new EtcdKV", zap.Any("reason", initError))
				return initError
			}
			var ss *suffixSnapshot
			if ss, initError = newSuffixSnapshot(metaKV, "_ts", Params.EtcdCfg.MetaRootPath, "snapshots"); initError != nil {
				log.Error("RootCoord failed to new suffixSnapshot", zap.Error(initError))
				return initError
			}
			if c.MetaTable, initError = NewMetaTable(metaKV, ss); initError != nil {
				log.Error("RootCoord failed to new MetaTable", zap.Any("reason", initError))
				return initError
			}

			return nil
		}
		log.Debug("RootCoord, Connecting to Etcd", zap.String("kv root", Params.EtcdCfg.KvRootPath), zap.String("meta root", Params.EtcdCfg.MetaRootPath))
		err := retry.Do(c.ctx, connectEtcdFn, retry.Attempts(300))
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
			c.CallImportService,
		)
	})
	if initError != nil {
		log.Debug("RootCoord init error", zap.Error(initError))
	}
	log.Debug("RootCoord init done")
	return initError
}

func (c *Core) reSendDdMsg(ctx context.Context, force bool) error {
	if !force {
		flag, err := c.MetaTable.txn.Load(DDMsgSendPrefix)
		if err != nil {
			// TODO, this is super ugly hack but our kv interface does not support loadWithExist
			// leave it for later
			if strings.Contains(err.Error(), "there is no value on key") {
				log.Debug("skip reSendDdMsg with no dd-msg-send key")
				return nil
			}
			return err
		}
		value, err := strconv.ParseBool(flag)
		if err != nil {
			return err
		}
		if value {
			log.Debug("skip reSendDdMsg with dd-msg-send set to true")
			return nil
		}
	}

	ddOpStr, err := c.MetaTable.txn.Load(DDOperationPrefix)
	if err != nil {
		log.Debug("DdOperation key does not exist")
		return nil
	}
	var ddOp DdOperation
	if err = json.Unmarshal([]byte(ddOpStr), &ddOp); err != nil {
		return err
	}

	invalidateCache := false
	var ts typeutil.Timestamp
	var collName string

	switch ddOp.Type {
	// TODO remove create collection resend
	// since create collection needs a start position to succeed
	case CreateCollectionDDType:
		var ddReq = internalpb.CreateCollectionRequest{}
		if err = proto.Unmarshal(ddOp.Body, &ddReq); err != nil {
			return err
		}
		if _, err := c.MetaTable.GetCollectionByName(ddReq.CollectionName, 0); err != nil {
			if _, err = c.SendDdCreateCollectionReq(ctx, &ddReq, ddReq.PhysicalChannelNames); err != nil {
				return err
			}
		} else {
			log.Debug("collection has been created, skip re-send CreateCollection",
				zap.String("collection name", collName))
		}
	case DropCollectionDDType:
		var ddReq = internalpb.DropCollectionRequest{}
		if err = proto.Unmarshal(ddOp.Body, &ddReq); err != nil {
			return err
		}
		ts = ddReq.Base.Timestamp
		collName = ddReq.CollectionName
		if collInfo, err := c.MetaTable.GetCollectionByName(ddReq.CollectionName, 0); err == nil {
			if err = c.SendDdDropCollectionReq(ctx, &ddReq, collInfo.PhysicalChannelNames); err != nil {
				return err
			}
			invalidateCache = true
		} else {
			log.Debug("collection has been removed, skip re-send DropCollection",
				zap.String("collection name", collName))
		}
	case CreatePartitionDDType:
		var ddReq = internalpb.CreatePartitionRequest{}
		if err = proto.Unmarshal(ddOp.Body, &ddReq); err != nil {
			return err
		}
		ts = ddReq.Base.Timestamp
		collName = ddReq.CollectionName
		collInfo, err := c.MetaTable.GetCollectionByName(ddReq.CollectionName, 0)
		if err != nil {
			return err
		}
		if _, err = c.MetaTable.GetPartitionByName(collInfo.ID, ddReq.PartitionName, 0); err != nil {
			if err = c.SendDdCreatePartitionReq(ctx, &ddReq, collInfo.PhysicalChannelNames); err != nil {
				return err
			}
			invalidateCache = true
		} else {
			log.Debug("partition has been created, skip re-send CreatePartition",
				zap.String("collection name", collName), zap.String("partition name", ddReq.PartitionName))
		}
	case DropPartitionDDType:
		var ddReq = internalpb.DropPartitionRequest{}
		if err = proto.Unmarshal(ddOp.Body, &ddReq); err != nil {
			return err
		}
		ts = ddReq.Base.Timestamp
		collName = ddReq.CollectionName
		collInfo, err := c.MetaTable.GetCollectionByName(ddReq.CollectionName, 0)
		if err != nil {
			return err
		}
		if _, err = c.MetaTable.GetPartitionByName(collInfo.ID, ddReq.PartitionName, 0); err == nil {
			if err = c.SendDdDropPartitionReq(ctx, &ddReq, collInfo.PhysicalChannelNames); err != nil {
				return err
			}
			invalidateCache = true
		} else {
			log.Debug("partition has been removed, skip re-send DropPartition",
				zap.String("collection name", collName), zap.String("partition name", ddReq.PartitionName))
		}
	default:
		return fmt.Errorf("invalid DdOperation %s", ddOp.Type)
	}

	if invalidateCache {
		c.ExpireMetaCache(ctx, []string{collName}, ts)
	}

	// Update DDOperation in etcd
	return c.MetaTable.txn.Save(DDMsgSendPrefix, strconv.FormatBool(true))
}

// Start starts RootCoord.
func (c *Core) Start() error {
	if err := c.checkInit(); err != nil {
		log.Debug("RootCoord Start checkInit failed", zap.Error(err))
		return err
	}

	log.Debug(typeutil.RootCoordRole, zap.Int64("node id", c.session.ServerID))
	log.Debug(typeutil.RootCoordRole, zap.String("time tick channel name", Params.CommonCfg.RootCoordTimeTick))

	c.startOnce.Do(func() {
		if err := c.proxyManager.WatchProxy(); err != nil {
			log.Fatal("RootCoord Start WatchProxy failed", zap.Error(err))
			// you can not just stuck here,
			panic(err)
		}
		if err := c.reSendDdMsg(c.ctx, false); err != nil {
			log.Fatal("RootCoord Start reSendDdMsg failed", zap.Error(err))
			panic(err)
		}
		c.wg.Add(5)
		go c.startTimeTickLoop()
		go c.tsLoop()
		go c.chanTimeTick.startWatch(&c.wg)
		go c.checkFlushedSegmentsLoop()
		go c.importManager.expireOldTasksLoop(&c.wg)
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
	metrics.RootCoordCreateCollectionCounter.WithLabelValues(metrics.TotalLabel).Inc()
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
		return failStatus(commonpb.ErrorCode_UnexpectedError, "CreateCollection failed: "+err.Error()), nil
	}
	log.Debug("CreateCollection success", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordCreateCollectionCounter.WithLabelValues(metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLWriteTypeLatency.WithLabelValues("CreateCollection").Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfCollections.Inc()
	return succStatus(), nil
}

// DropCollection drop collection
func (c *Core) DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	metrics.RootCoordDropCollectionCounter.WithLabelValues(metrics.TotalLabel).Inc()
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
		return failStatus(commonpb.ErrorCode_UnexpectedError, "DropCollection failed: "+err.Error()), nil
	}
	log.Debug("DropCollection success", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordDropCollectionCounter.WithLabelValues(metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLWriteTypeLatency.WithLabelValues("DropCollection").Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfCollections.Dec()
	return succStatus(), nil
}

// HasCollection check collection existence
func (c *Core) HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	metrics.RootCoordHasCollectionCounter.WithLabelValues(metrics.TotalLabel).Inc()
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
		return &milvuspb.BoolResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "HasCollection failed: "+err.Error()),
			Value:  false,
		}, nil
	}
	log.Debug("HasCollection success", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordHasCollectionCounter.WithLabelValues(metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReadTypeLatency.WithLabelValues("HasCollection").Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &milvuspb.BoolResponse{
		Status: succStatus(),
		Value:  t.HasCollection,
	}, nil
}

// DescribeCollection return collection info
func (c *Core) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	metrics.RootCoordDescribeCollectionCounter.WithLabelValues(metrics.TotalLabel).Inc()
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.DescribeCollectionResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode"+internalpb.StateCode_name[int32(code)]),
		}, nil
	}
	tr := timerecord.NewTimeRecorder("DescribeCollection")

	log.Debug("DescribeCollection", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID))
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
		log.Error("DescribeCollection failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("collection name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		return &milvuspb.DescribeCollectionResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "DescribeCollection failed: "+err.Error()),
		}, nil
	}
	log.Debug("DescribeCollection success", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordDescribeCollectionCounter.WithLabelValues(metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReadTypeLatency.WithLabelValues("DescribeCollection").Observe(float64(tr.ElapseSpan().Milliseconds()))
	t.Rsp.Status = succStatus()
	return t.Rsp, nil
}

// ShowCollections list all collection names
func (c *Core) ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	metrics.RootCoordShowCollectionsCounter.WithLabelValues(MetricRequestsTotal).Inc()
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
		return &milvuspb.ShowCollectionsResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "ShowCollections failed: "+err.Error()),
		}, nil
	}
	log.Debug("ShowCollections success", zap.String("role", typeutil.RootCoordRole),
		zap.String("dbname", in.DbName), zap.Int("num of collections", len(t.Rsp.CollectionNames)),
		zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordShowCollectionsCounter.WithLabelValues(MetricRequestsSuccess).Inc()
	t.Rsp.Status = succStatus()
	metrics.RootCoordDDLReadTypeLatency.WithLabelValues("ShowCollections").Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.Rsp, nil
}

// CreatePartition create partition
func (c *Core) CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	metrics.RootCoordCreatePartitionCounter.WithLabelValues(metrics.TotalLabel).Inc()
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
		return failStatus(commonpb.ErrorCode_UnexpectedError, "CreatePartition failed: "+err.Error()), nil
	}
	log.Debug("CreatePartition success", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName),
		zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordCreatePartitionCounter.WithLabelValues(metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLWriteTypeLatency.WithLabelValues("CreatePartition").Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfPartitions.WithLabelValues(in.CollectionName).Inc()
	return succStatus(), nil
}

// DropPartition drop partition
func (c *Core) DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	metrics.RootCoordDropPartitionCounter.WithLabelValues(metrics.TotalLabel).Inc()
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
		return failStatus(commonpb.ErrorCode_UnexpectedError, "DropPartition failed: "+err.Error()), nil
	}
	log.Debug("DropPartition success", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName),
		zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordDropPartitionCounter.WithLabelValues(metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLWriteTypeLatency.WithLabelValues("DropPartition").Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfPartitions.WithLabelValues(in.CollectionName).Dec()
	return succStatus(), nil
}

// HasPartition check partition existence
func (c *Core) HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	metrics.RootCoordHasPartitionCounter.WithLabelValues(metrics.TotalLabel).Inc()
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
		return &milvuspb.BoolResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "HasPartition failed: "+err.Error()),
			Value:  false,
		}, nil
	}
	log.Debug("HasPartition success", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName),
		zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordHasPartitionCounter.WithLabelValues(metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReadTypeLatency.WithLabelValues("HasPartition").Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &milvuspb.BoolResponse{
		Status: succStatus(),
		Value:  t.HasPartition,
	}, nil
}

// ShowPartitions list all partition names
func (c *Core) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	metrics.RootCoordShowPartitionsCounter.WithLabelValues(metrics.TotalLabel).Inc()
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
		return &milvuspb.ShowPartitionsResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "ShowPartitions failed: "+err.Error()),
		}, nil
	}
	log.Debug("ShowPartitions success", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.Int("num of partitions", len(t.Rsp.PartitionNames)),
		zap.Int64("msgID", t.Req.Base.MsgID))

	metrics.RootCoordShowPartitionsCounter.WithLabelValues(metrics.SuccessLabel).Inc()
	t.Rsp.Status = succStatus()
	metrics.RootCoordDDLReadTypeLatency.WithLabelValues("ShowPartitions").Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.Rsp, nil
}

// CreateIndex create index
func (c *Core) CreateIndex(ctx context.Context, in *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	metrics.RootCoordCreateIndexCounter.WithLabelValues(metrics.TotalLabel).Inc()
	if code, ok := c.checkHealthy(); !ok {
		return failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]), nil
	}
	tr := timerecord.NewTimeRecorder("CreateIndex")
	log.Debug("CreateIndex", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName),
		zap.Int64("msgID", in.Base.MsgID))
	t := &CreateIndexReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			core: c,
		},
		Req: in,
	}
	err := executeTask(t)
	if err != nil {
		log.Error("CreateIndex failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName),
			zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		return failStatus(commonpb.ErrorCode_UnexpectedError, "CreateIndex failed: "+err.Error()), nil
	}
	log.Debug("CreateIndex success", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName),
		zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordCreateIndexCounter.WithLabelValues(metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLWriteTypeLatency.WithLabelValues("CreateIndex").Observe(float64(tr.ElapseSpan().Milliseconds()))
	return succStatus(), nil
}

// DescribeIndex return index info
func (c *Core) DescribeIndex(ctx context.Context, in *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	metrics.RootCoordDescribeIndexCounter.WithLabelValues(metrics.TotalLabel).Inc()
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.DescribeIndexResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
		}, nil
	}
	tr := timerecord.NewTimeRecorder("DescribeIndex")
	log.Debug("DescribeIndex", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName),
		zap.Int64("msgID", in.Base.MsgID))
	t := &DescribeIndexReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			core: c,
		},
		Req: in,
		Rsp: &milvuspb.DescribeIndexResponse{},
	}
	err := executeTask(t)
	if err != nil {
		log.Error("DescribeIndex failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName),
			zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		return &milvuspb.DescribeIndexResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "DescribeIndex failed: "+err.Error()),
		}, nil
	}
	idxNames := make([]string, 0, len(t.Rsp.IndexDescriptions))
	for _, i := range t.Rsp.IndexDescriptions {
		idxNames = append(idxNames, i.IndexName)
	}
	log.Debug("DescribeIndex success", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName),
		zap.Strings("index names", idxNames), zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordDescribeIndexCounter.WithLabelValues(metrics.SuccessLabel).Inc()
	if len(t.Rsp.IndexDescriptions) == 0 {
		t.Rsp.Status = failStatus(commonpb.ErrorCode_IndexNotExist, "index not exist")
	} else {
		t.Rsp.Status = succStatus()
	}
	metrics.RootCoordDDLWriteTypeLatency.WithLabelValues("DescribeIndex").Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.Rsp, nil
}

// DropIndex drop index
func (c *Core) DropIndex(ctx context.Context, in *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	metrics.RootCoordDropIndexCounter.WithLabelValues(metrics.TotalLabel).Inc()
	if code, ok := c.checkHealthy(); !ok {
		return failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]), nil
	}
	tr := timerecord.NewTimeRecorder("DropIndex")
	log.Debug("DropIndex", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName),
		zap.String("index name", in.IndexName), zap.Int64("msgID", in.Base.MsgID))
	t := &DropIndexReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			core: c,
		},
		Req: in,
	}
	err := executeTask(t)
	if err != nil {
		log.Error("DropIndex failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName),
			zap.String("index name", in.IndexName), zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		return failStatus(commonpb.ErrorCode_UnexpectedError, "DropIndex failed: "+err.Error()), nil
	}
	log.Debug("DropIndex success", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName),
		zap.String("index name", in.IndexName), zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordDropIndexCounter.WithLabelValues(metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLWriteTypeLatency.WithLabelValues("DropIndex").Observe(float64(tr.ElapseSpan().Milliseconds()))
	return succStatus(), nil
}

// DescribeSegment return segment info
func (c *Core) DescribeSegment(ctx context.Context, in *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error) {
	metrics.RootCoordDescribeSegmentCounter.WithLabelValues(metrics.TotalLabel).Inc()
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.DescribeSegmentResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
		}, nil
	}
	tr := timerecord.NewTimeRecorder("DescribeSegment")
	log.Debug("DescribeSegment", zap.String("role", typeutil.RootCoordRole),
		zap.Int64("collection id", in.CollectionID), zap.Int64("segment id", in.SegmentID),
		zap.Int64("msgID", in.Base.MsgID))
	t := &DescribeSegmentReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			core: c,
		},
		Req: in,
		Rsp: &milvuspb.DescribeSegmentResponse{},
	}
	err := executeTask(t)
	if err != nil {
		log.Error("DescribeSegment failed", zap.String("role", typeutil.RootCoordRole),
			zap.Int64("collection id", in.CollectionID), zap.Int64("segment id", in.SegmentID),
			zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		return &milvuspb.DescribeSegmentResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "DescribeSegment failed: "+err.Error()),
		}, nil
	}
	log.Debug("DescribeSegment success", zap.String("role", typeutil.RootCoordRole),
		zap.Int64("collection id", in.CollectionID), zap.Int64("segment id", in.SegmentID),
		zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordDescribeSegmentCounter.WithLabelValues(metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReadTypeLatency.WithLabelValues("DescribeSegment").Observe(float64(tr.ElapseSpan().Milliseconds()))
	t.Rsp.Status = succStatus()
	return t.Rsp, nil
}

func (c *Core) DescribeSegments(ctx context.Context, in *rootcoordpb.DescribeSegmentsRequest) (*rootcoordpb.DescribeSegmentsResponse, error) {
	metrics.RootCoordDescribeSegmentsCounter.WithLabelValues(metrics.TotalLabel).Inc()

	if code, ok := c.checkHealthy(); !ok {
		log.Error("failed to describe segments, rootcoord not healthy",
			zap.String("role", typeutil.RootCoordRole),
			zap.Int64("msgID", in.GetBase().GetMsgID()),
			zap.Int64("collection", in.GetCollectionID()),
			zap.Int64s("segments", in.GetSegmentIDs()))

		return &rootcoordpb.DescribeSegmentsResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
		}, nil
	}

	tr := timerecord.NewTimeRecorder("DescribeSegments")

	log.Debug("received request to describe segments",
		zap.String("role", typeutil.RootCoordRole),
		zap.Int64("msgID", in.GetBase().GetMsgID()),
		zap.Int64("collection", in.GetCollectionID()),
		zap.Int64s("segments", in.GetSegmentIDs()))

	t := &DescribeSegmentsReqTask{
		baseReqTask: baseReqTask{
			ctx:  ctx,
			core: c,
		},
		Req: in,
		Rsp: &rootcoordpb.DescribeSegmentsResponse{},
	}

	if err := executeTask(t); err != nil {
		log.Error("failed to describe segments",
			zap.Error(err),
			zap.String("role", typeutil.RootCoordRole),
			zap.Int64("msgID", in.GetBase().GetMsgID()),
			zap.Int64("collection", in.GetCollectionID()),
			zap.Int64s("segments", in.GetSegmentIDs()))

		return &rootcoordpb.DescribeSegmentsResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "DescribeSegments failed: "+err.Error()),
		}, nil
	}

	log.Debug("succeed to describe segments",
		zap.String("role", typeutil.RootCoordRole),
		zap.Int64("msgID", in.GetBase().GetMsgID()),
		zap.Int64("collection", in.GetCollectionID()),
		zap.Int64s("segments", in.GetSegmentIDs()))

	metrics.RootCoordDescribeSegmentsCounter.WithLabelValues(metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReadTypeLatency.WithLabelValues("DescribeSegments").Observe(float64(tr.ElapseSpan().Milliseconds()))

	t.Rsp.Status = succStatus()
	return t.Rsp, nil
}

// ShowSegments list all segments
func (c *Core) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error) {
	metrics.RootCoordShowSegmentsCounter.WithLabelValues(metrics.TotalLabel).Inc()
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
		return &milvuspb.ShowSegmentsResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "ShowSegments failed: "+err.Error()),
		}, nil
	}
	log.Debug("ShowSegments success", zap.String("role", typeutil.RootCoordRole),
		zap.Int64("collection id", in.CollectionID), zap.Int64("partition id", in.PartitionID),
		zap.Int64s("segments ids", t.Rsp.SegmentIDs),
		zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordShowSegmentsCounter.WithLabelValues(metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReadTypeLatency.WithLabelValues("ShowSegments").Observe(float64(tr.ElapseSpan().Milliseconds()))
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
	metrics.RootCoordTimestampAllocCounter.Set(float64(ts))
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
	return c.proxyClientManager.ReleaseDQLMessageStream(ctx, in)
}

// SegmentFlushCompleted check whether segment flush has completed
func (c *Core) SegmentFlushCompleted(ctx context.Context, in *datapb.SegmentFlushCompletedMsg) (*commonpb.Status, error) {
	if code, ok := c.checkHealthy(); !ok {
		return failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]), nil
	}
	if in.Base.MsgType != commonpb.MsgType_SegmentFlushDone {
		return failStatus(commonpb.ErrorCode_UnexpectedError, "invalid msg type "+commonpb.MsgType_name[int32(in.Base.MsgType)]), nil
	}
	segID := in.Segment.GetID()
	log.Debug("SegmentFlushCompleted", zap.String("role", typeutil.RootCoordRole),
		zap.Int64("collection id", in.Segment.CollectionID), zap.Int64("partition id", in.Segment.PartitionID),
		zap.Int64("segment id", segID), zap.Int64("msgID", in.Base.MsgID))

	coll, err := c.MetaTable.GetCollectionByID(in.Segment.CollectionID, 0)
	if err != nil {
		log.Error("GetCollectionByID failed", zap.String("role", typeutil.RootCoordRole),
			zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		return failStatus(commonpb.ErrorCode_UnexpectedError, "GetCollectionByID failed: "+err.Error()), nil
	}

	if len(coll.FieldIndexes) == 0 {
		log.Debug("no index params on collection", zap.String("role", typeutil.RootCoordRole),
			zap.String("collection_name", coll.Schema.Name), zap.Int64("msgID", in.Base.MsgID))
	}

	for _, f := range coll.FieldIndexes {
		fieldSch, err := GetFieldSchemaByID(coll, f.FiledID)
		if err != nil {
			log.Warn("field schema not found", zap.String("role", typeutil.RootCoordRole),
				zap.String("collection_name", coll.Schema.Name), zap.Int64("field id", f.FiledID),
				zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
			continue
		}

		idxInfo, err := c.MetaTable.GetIndexByID(f.IndexID)
		if err != nil {
			log.Warn("index not found", zap.String("role", typeutil.RootCoordRole),
				zap.String("collection_name", coll.Schema.Name), zap.Int64("field id", f.FiledID),
				zap.Int64("index id", f.IndexID), zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
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
			log.Error("BuildIndex failed", zap.String("role", typeutil.RootCoordRole),
				zap.String("collection_name", coll.Schema.Name), zap.Int64("field id", f.FiledID),
				zap.Int64("index id", f.IndexID), zap.Int64("build id", info.BuildID),
				zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
			continue
		}
		err = c.MetaTable.AddIndex(&info)
		if err != nil {
			log.Error("AddIndex failed", zap.String("role", typeutil.RootCoordRole),
				zap.String("collection_name", coll.Schema.Name), zap.Int64("field id", f.FiledID),
				zap.Int64("index id", f.IndexID), zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
			continue
		}
	}

	log.Debug("SegmentFlushCompleted success", zap.String("role", typeutil.RootCoordRole),
		zap.Int64("collection id", in.Segment.CollectionID), zap.Int64("partition id", in.Segment.PartitionID),
		zap.Int64("segment id", segID), zap.Int64("msgID", in.Base.MsgID))
	return succStatus(), nil
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
		log.Error("ParseMetricType failed", zap.String("role", typeutil.RootCoordRole),
			zap.Int64("node_id", c.session.ServerID), zap.String("req", in.Request), zap.Error(err))
		return &milvuspb.GetMetricsResponse{
			Status:   failStatus(commonpb.ErrorCode_UnexpectedError, "ParseMetricType failed: "+err.Error()),
			Response: "",
		}, nil
	}

	log.Debug("GetMetrics success", zap.String("role", typeutil.RootCoordRole),
		zap.String("metric_type", metricType), zap.Int64("msgID", in.Base.MsgID))

	if metricType == metricsinfo.SystemInfoMetrics {
		ret, err := c.metricsCacheManager.GetSystemInfoMetrics()
		if err == nil && ret != nil {
			return ret, nil
		}

		log.Warn("GetSystemInfoMetrics from cache failed", zap.String("role", typeutil.RootCoordRole),
			zap.Int64("msgID", in.Base.MsgID), zap.Error(err))

		systemInfoMetrics, err := c.getSystemInfoMetrics(ctx, in)
		if err != nil {
			log.Error("GetSystemInfoMetrics failed", zap.String("role", typeutil.RootCoordRole),
				zap.String("metric_type", metricType), zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
			return nil, err
		}

		c.metricsCacheManager.UpdateSystemInfoMetrics(systemInfoMetrics)
		return systemInfoMetrics, err
	}

	log.Error("GetMetrics failed, metric type not implemented", zap.String("role", typeutil.RootCoordRole),
		zap.String("metric_type", metricType), zap.Int64("msgID", in.Base.MsgID))

	return &milvuspb.GetMetricsResponse{
		Status:   failStatus(commonpb.ErrorCode_UnexpectedError, metricsinfo.MsgUnimplementedMetric),
		Response: "",
	}, nil
}

// CreateAlias create collection alias
func (c *Core) CreateAlias(ctx context.Context, in *milvuspb.CreateAliasRequest) (*commonpb.Status, error) {
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
		return failStatus(commonpb.ErrorCode_UnexpectedError, "CreateAlias failed: "+err.Error()), nil
	}
	log.Debug("CreateAlias success", zap.String("role", typeutil.RootCoordRole),
		zap.String("alias", in.Alias), zap.String("collection name", in.CollectionName),
		zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordDDLWriteTypeLatency.WithLabelValues("CreateAlias").Observe(float64(tr.ElapseSpan().Milliseconds()))
	return succStatus(), nil
}

// DropAlias drop collection alias
func (c *Core) DropAlias(ctx context.Context, in *milvuspb.DropAliasRequest) (*commonpb.Status, error) {
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
		return failStatus(commonpb.ErrorCode_UnexpectedError, "DropAlias failed: "+err.Error()), nil
	}
	log.Debug("DropAlias success", zap.String("role", typeutil.RootCoordRole),
		zap.String("alias", in.Alias), zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordDDLWriteTypeLatency.WithLabelValues("DropAlias").Observe(float64(tr.ElapseSpan().Milliseconds()))
	return succStatus(), nil
}

// AlterAlias alter collection alias
func (c *Core) AlterAlias(ctx context.Context, in *milvuspb.AlterAliasRequest) (*commonpb.Status, error) {
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
		return failStatus(commonpb.ErrorCode_UnexpectedError, "AlterAlias failed: "+err.Error()), nil
	}
	log.Debug("AlterAlias success", zap.String("role", typeutil.RootCoordRole),
		zap.String("alias", in.Alias), zap.String("collection name", in.CollectionName),
		zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordDDLWriteTypeLatency.WithLabelValues("AlterAlias").Observe(float64(tr.ElapseSpan().Milliseconds()))
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
	var cID int64
	var ok bool
	if cID, ok = c.MetaTable.collName2ID[req.GetCollectionName()]; !ok {
		log.Error("failed to find collection ID for collection name",
			zap.String("collection name", req.GetCollectionName()))
		return nil, fmt.Errorf("collection ID not found for collection name %s", req.GetCollectionName())
	}
	log.Info("receive import request",
		zap.String("collection name", req.GetCollectionName()),
		zap.Int64("collection ID", cID),
		zap.String("partition name", req.GetPartitionName()),
		zap.Int("# of files = ", len(req.GetFiles())),
	)
	resp := c.importManager.importJob(ctx, req, cID)
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

// ReportImport reports import task state to RootCoord.
func (c *Core) ReportImport(ctx context.Context, ir *rootcoordpb.ImportResult) (*commonpb.Status, error) {
	log.Info("receive import state report",
		zap.Int64("task ID", ir.GetTaskId()),
		zap.Any("import state", ir.GetState()))
	if code, ok := c.checkHealthy(); !ok {
		return failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]), nil
	}
	// Upon receiving ReportImport request, update the related task's state in task store.
	ti, err := c.importManager.updateTaskState(ir)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UpdateImportTaskFailure,
			Reason:    err.Error(),
		}, nil
	}

	// That's all for reporting, if task hasn't reached persisted or completed status yet.
	if ti.GetState().GetStateCode() != commonpb.ImportState_ImportPersisted &&
		ti.GetState().GetStateCode() != commonpb.ImportState_ImportCompleted {
		log.Debug("transitional import state received, return immediately", zap.Any("import result", ir))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		}, nil
	}

	// Reverse look up collection name on collection ID.
	var colName string
	for k, v := range c.MetaTable.collName2ID {
		if v == ti.GetCollectionId() {
			colName = k
		}
	}
	if colName == "" {
		log.Error("Collection name not found for collection ID", zap.Int64("collection ID", ti.GetCollectionId()))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_CollectionNameNotFound,
			Reason:    "Collection name not found for collection ID" + strconv.FormatInt(ti.GetCollectionId(), 10),
		}, nil
	}

	// When DataNode has done its thing, remove it from the busy node list.
	func() {
		c.importManager.busyNodesLock.Lock()
		defer c.importManager.busyNodesLock.Unlock()
		delete(c.importManager.busyNodes, ir.GetDatanodeId())
		log.Info("dataNode is no longer busy",
			zap.Int64("dataNode ID", ir.GetDatanodeId()),
			zap.Int64("task ID", ir.GetTaskId()))
	}()

	// Start a loop to check segments' index states periodically.
	c.wg.Add(1)
	go c.checkCompleteIndexLoop(ctx, ti, colName, ir.Segments)

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

// CountCompleteIndex checks indexing status of the given segments, and returns the # of segments that has complete index.
func (c *Core) CountCompleteIndex(ctx context.Context, collectionName string, collectionID UniqueID,
	allSegmentIDs []UniqueID) (int, error) {
	// Note: Index name is always Params.CommonCfg.DefaultIndexName in current Milvus design as of today.
	indexName := Params.CommonCfg.DefaultIndexName

	// Retrieve index status and detailed index information.
	describeIndexReq := &milvuspb.DescribeIndexRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_DescribeIndex,
		},
		CollectionName: collectionName,
		IndexName:      indexName,
	}
	indexDescriptionResp, err := c.DescribeIndex(ctx, describeIndexReq)
	if err != nil {
		return 0, err
	}
	log.Debug("got index description", zap.String("index_description", indexDescriptionResp.String()))

	// Check if the target index name exists.
	matchIndexID := int64(-1)
	foundIndexID := false
	for _, desc := range indexDescriptionResp.IndexDescriptions {
		if desc.IndexName == indexName {
			matchIndexID = desc.IndexID
			foundIndexID = true
			break
		}
	}
	if !foundIndexID {
		return 0, fmt.Errorf("no index is created")
	}
	log.Debug("found match index ID", zap.Int64("match index ID", matchIndexID))

	getIndexStatesRequest := &indexpb.GetIndexStatesRequest{
		IndexBuildIDs: make([]UniqueID, 0),
	}

	// Fetch index build IDs from segments.
	for _, segmentID := range allSegmentIDs {
		describeSegmentRequest := &milvuspb.DescribeSegmentRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_DescribeSegment,
			},
			CollectionID: collectionID,
			SegmentID:    segmentID,
		}
		segmentDesc, err := c.DescribeSegment(ctx, describeSegmentRequest)
		if err != nil {
			log.Error("Failed to describe segment",
				zap.Int64("collection ID", collectionID),
				zap.Int64("segment ID", segmentID))
			return 0, err
		}
		if segmentDesc.IndexID == matchIndexID {
			if segmentDesc.EnableIndex {
				getIndexStatesRequest.IndexBuildIDs = append(getIndexStatesRequest.IndexBuildIDs, segmentDesc.BuildID)
			}
		}
	}
	log.Debug("proxy GetIndexState", zap.Int("# of IndexBuildIDs", len(getIndexStatesRequest.IndexBuildIDs)), zap.Error(err))

	if len(getIndexStatesRequest.IndexBuildIDs) == 0 {
		log.Info("empty index build IDs returned",
			zap.String("collection name", collectionName),
			zap.Int64("collection ID", collectionID))
		return 0, nil
	}
	states, err := c.CallGetIndexStatesService(ctx, getIndexStatesRequest.IndexBuildIDs)
	if err != nil {
		log.Error("failed to get index state in checkSegmentIndexStates", zap.Error(err))
		return 0, err
	}

	// Count the # of segments with finished index.
	ct := 0
	for _, s := range states {
		if s.State == commonpb.IndexState_Finished {
			ct++
		}
	}
	log.Info("segment indexing state checked",
		zap.Int("# of checked segment", len(states)),
		zap.Int("# of segments with complete index", ct),
		zap.String("collection name", collectionName),
		zap.Int64("collection ID", collectionID),
	)
	return ct, nil
}

// checkCompleteIndexLoop checks index build states for an import task's segments and bring these segments online when
// the criteria are met. checkCompleteIndexLoop does the check every CheckCompleteIndexInterval and exits if:
// (1) a certain percent of indices are built, (2) when context is done or (3) when `ImportIndexWaitLimit` has passed.
func (c *Core) checkCompleteIndexLoop(ctx context.Context, ti *datapb.ImportTaskInfo, colName string, segIDs []UniqueID) {
	defer c.wg.Done()
	ticker := time.NewTicker(time.Duration(Params.RootCoordCfg.ImportIndexCheckInterval*1000) * time.Millisecond)
	defer ticker.Stop()
	expireTicker := time.NewTicker(time.Duration(Params.RootCoordCfg.ImportIndexWaitLimit*1000) * time.Millisecond)
	defer expireTicker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			log.Info("(in check complete index loop) context done, exiting checkCompleteIndexLoop",
				zap.Int64("task ID", ti.GetId()))
			return
		case <-ticker.C:
			log.Info("(in check complete index loop) check segments' index states", zap.Int64("task ID", ti.GetId()))
			if ct, err := c.CountCompleteIndex(ctx, colName, ti.GetCollectionId(), segIDs); err == nil &&
				segmentsOnlineReady(ct, len(segIDs)) {
				log.Info("segment indices are ready",
					zap.Int64("task ID", ti.GetId()),
					zap.Int("total # of segments", len(segIDs)),
					zap.Int("# of segments with index ready", ct))
				c.bringSegmentsOnline(ctx, segIDs)
				return
			}
		case <-expireTicker.C:
			log.Info("(in check complete index loop) waited for sufficiently long time, bring segments online",
				zap.Int64("task ID", ti.GetId()))
			c.bringSegmentsOnline(ctx, segIDs)
			return
		}
	}
}

// bringSegmentsOnline brings the segments online so that data in these segments become searchable
// it is done by changing segments' states from `importing` to `flushed`.
func (c *Core) bringSegmentsOnline(ctx context.Context, segIDs []UniqueID) {
	log.Info("bringing import task's segments online!", zap.Any("segment IDs", segIDs))
	// TODO: Make update on segment states atomic.
	for _, id := range segIDs {
		// Explicitly mark segment states `flushed`.
		c.CallUpdateSegmentStateService(ctx, id, commonpb.SegmentState_Flushed)
	}
}

// segmentsOnlineReady returns true if segments are ready to go up online (a.k.a. become searchable).
func segmentsOnlineReady(idxBuilt, segCount int) bool {
	// Consider segments are ready when:
	// (1) all but up to 2 segments have indices ready, or
	// (2) over 85% of segments have indices ready.
	if segCount-idxBuilt <= 2 || float64(idxBuilt)/float64(segCount) > 0.85 {
		return true
	}
	return false
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
		Password: credInfo.EncryptedPassword,
	}
	return c.proxyClientManager.UpdateCredentialCache(ctx, &req)
}

// ClearCredUsersCache will call clear credential usernames cache
func (c *Core) ClearCredUsersCache(ctx context.Context) error {
	req := internalpb.ClearCredUsersCacheRequest{}
	return c.proxyClientManager.ClearCredUsersCache(ctx, &req)
}

// CreateCredential create new user and password
// 	1. decode ciphertext password to raw password
// 	2. encrypt raw password
// 	3. save in to etcd
func (c *Core) CreateCredential(ctx context.Context, credInfo *internalpb.CredentialInfo) (*commonpb.Status, error) {
	metrics.RootCoordCreateCredentialCounter.WithLabelValues(metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("CreateCredential")
	log.Debug("CreateCredential", zap.String("role", typeutil.RootCoordRole),
		zap.String("username", credInfo.Username))

	if cred, _ := c.MetaTable.getCredential(credInfo.Username); cred != nil {
		return failStatus(commonpb.ErrorCode_CreateCredentialFailure, "user already exists:"+credInfo.Username), nil
	}
	// update proxy's local cache
	err := c.ClearCredUsersCache(ctx)
	if err != nil {
		log.Error("CreateCredential clear credential username list cache failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("username", credInfo.Username), zap.Error(err))
		metrics.RootCoordCreateCredentialCounter.WithLabelValues(metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_CreateCredentialFailure, "CreateCredential failed: "+err.Error()), nil
	}
	// insert to db
	err = c.MetaTable.AddCredential(credInfo)
	if err != nil {
		log.Error("CreateCredential save credential failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("username", credInfo.Username), zap.Error(err))
		metrics.RootCoordCreateCredentialCounter.WithLabelValues(metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_CreateCredentialFailure, "CreateCredential failed: "+err.Error()), nil
	}
	log.Debug("CreateCredential success", zap.String("role", typeutil.RootCoordRole),
		zap.String("username", credInfo.Username))

	metrics.RootCoordCreateCredentialCounter.WithLabelValues(metrics.SuccessLabel).Inc()
	metrics.RootCoordCredentialWriteTypeLatency.WithLabelValues("CreateCredential").Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfCredentials.Inc()
	return succStatus(), nil
}

// GetCredential get credential by username
func (c *Core) GetCredential(ctx context.Context, in *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error) {
	metrics.RootCoordGetCredentialCounter.WithLabelValues(metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("GetCredential")
	log.Debug("GetCredential", zap.String("role", typeutil.RootCoordRole),
		zap.String("username", in.Username))

	credInfo, err := c.MetaTable.getCredential(in.Username)
	if err != nil {
		log.Error("GetCredential query credential failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("username", in.Username), zap.Error(err))
		metrics.RootCoordGetCredentialCounter.WithLabelValues(metrics.FailLabel).Inc()
		return &rootcoordpb.GetCredentialResponse{
			Status: failStatus(commonpb.ErrorCode_GetCredentialFailure, "GetCredential failed: "+err.Error()),
		}, err
	}
	log.Debug("GetCredential success", zap.String("role", typeutil.RootCoordRole),
		zap.String("username", in.Username))

	metrics.RootCoordGetCredentialCounter.WithLabelValues(metrics.SuccessLabel).Inc()
	metrics.RootCoordCredentialReadTypeLatency.WithLabelValues("GetCredential", in.Username).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &rootcoordpb.GetCredentialResponse{
		Status:   succStatus(),
		Username: credInfo.Username,
		Password: credInfo.EncryptedPassword,
	}, nil
}

// UpdateCredential update password for a user
func (c *Core) UpdateCredential(ctx context.Context, credInfo *internalpb.CredentialInfo) (*commonpb.Status, error) {
	metrics.RootCoordUpdateCredentialCounter.WithLabelValues(metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("UpdateCredential")
	log.Debug("UpdateCredential", zap.String("role", typeutil.RootCoordRole),
		zap.String("username", credInfo.Username))
	// update proxy's local cache
	err := c.UpdateCredCache(ctx, credInfo)
	if err != nil {
		log.Error("UpdateCredential update credential cache failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("username", credInfo.Username), zap.Error(err))
		metrics.RootCoordUpdateCredentialCounter.WithLabelValues(metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UpdateCredentialFailure, "UpdateCredential failed: "+err.Error()), nil
	}
	// update data on storage
	err = c.MetaTable.AddCredential(credInfo)
	if err != nil {
		log.Error("UpdateCredential save credential failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("username", credInfo.Username), zap.Error(err))
		metrics.RootCoordUpdateCredentialCounter.WithLabelValues(metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UpdateCredentialFailure, "UpdateCredential failed: "+err.Error()), nil
	}
	log.Debug("UpdateCredential success", zap.String("role", typeutil.RootCoordRole),
		zap.String("username", credInfo.Username))

	metrics.RootCoordUpdateCredentialCounter.WithLabelValues(metrics.SuccessLabel).Inc()
	metrics.RootCoordCredentialWriteTypeLatency.WithLabelValues("UpdateCredential").Observe(float64(tr.ElapseSpan().Milliseconds()))
	return succStatus(), nil
}

// DeleteCredential delete a user
func (c *Core) DeleteCredential(ctx context.Context, in *milvuspb.DeleteCredentialRequest) (*commonpb.Status, error) {
	metrics.RootCoordDeleteCredentialCounter.WithLabelValues(metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("DeleteCredential")

	log.Debug("DeleteCredential", zap.String("role", typeutil.RootCoordRole),
		zap.String("username", in.Username))
	// invalidate proxy's local cache
	err := c.ExpireCredCache(ctx, in.Username)
	if err != nil {
		log.Error("DeleteCredential expire credential cache failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("username", in.Username), zap.Error(err))
		metrics.RootCoordDeleteCredentialCounter.WithLabelValues(metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_DeleteCredentialFailure, "DeleteCredential failed: "+err.Error()), nil
	}
	// delete data on storage
	err = c.MetaTable.DeleteCredential(in.Username)
	if err != nil {
		log.Error("DeleteCredential remove credential failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("username", in.Username), zap.Error(err))
		metrics.RootCoordDeleteCredentialCounter.WithLabelValues(metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_DeleteCredentialFailure, "DeleteCredential failed: "+err.Error()), err
	}
	log.Debug("DeleteCredential success", zap.String("role", typeutil.RootCoordRole),
		zap.String("username", in.Username))

	metrics.RootCoordDeleteCredentialCounter.WithLabelValues(metrics.SuccessLabel).Inc()
	metrics.RootCoordCredentialWriteTypeLatency.WithLabelValues("DeleteCredential").Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfCredentials.Dec()
	return succStatus(), nil
}

// ListCredUsers list all usernames
func (c *Core) ListCredUsers(ctx context.Context, in *milvuspb.ListCredUsersRequest) (*milvuspb.ListCredUsersResponse, error) {
	metrics.RootCoordListCredUsersCounter.WithLabelValues(metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("ListCredUsers")

	credInfo, err := c.MetaTable.ListCredentialUsernames()
	if err != nil {
		log.Error("ListCredUsers query usernames failed", zap.String("role", typeutil.RootCoordRole),
			zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		return &milvuspb.ListCredUsersResponse{
			Status: failStatus(commonpb.ErrorCode_ListCredUsersFailure, "ListCredUsers failed: "+err.Error()),
		}, err
	}
	log.Debug("ListCredUsers success", zap.String("role", typeutil.RootCoordRole))

	metrics.RootCoordListCredUsersCounter.WithLabelValues(metrics.SuccessLabel).Inc()
	metrics.RootCoordCredentialReadTypeLatency.WithLabelValues("ListCredUsers", "ALL.API").Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &milvuspb.ListCredUsersResponse{
		Status:    succStatus(),
		Usernames: credInfo.Usernames,
	}, nil
}

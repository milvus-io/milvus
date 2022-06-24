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
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

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
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/crypto"
	"github.com/milvus-io/milvus/internal/util/dependency"
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
	CallBuildIndexService     func(ctx context.Context, segID UniqueID, binlog []string, field *schemapb.FieldSchema, idxInfo *etcdpb.IndexInfo, numRows int64) (typeutil.UniqueID, error)
	CallDropIndexService      func(ctx context.Context, indexID typeutil.UniqueID) error
	CallRemoveIndexService    func(ctx context.Context, buildIDs []UniqueID) error
	CallGetIndexStatesService func(ctx context.Context, IndexBuildIDs []int64) ([]*indexpb.IndexInfo, error)

	NewProxyClient func(sess *sessionutil.Session) (types.Proxy, error)

	//query service interface, notify query service to release collection
	CallReleaseCollectionService func(ctx context.Context, ts typeutil.Timestamp, dbID, collectionID typeutil.UniqueID) error
	CallReleasePartitionService  func(ctx context.Context, ts typeutil.Timestamp, dbID, collectionID typeutil.UniqueID, partitionIDs []typeutil.UniqueID) error

	// Communicates with queryCoord service for segments info.
	CallGetSegmentInfoService func(ctx context.Context, collectionID int64, segIDs []int64) (*querypb.GetSegmentInfoResponse, error)

	CallWatchChannels func(ctx context.Context, collectionID int64, channelNames []string) error

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
	if c.CallBuildIndexService == nil {
		return fmt.Errorf("callBuildIndexService is nil")
	}
	if c.CallDropIndexService == nil {
		return fmt.Errorf("callDropIndexService is nil")
	}
	if c.CallRemoveIndexService == nil {
		return fmt.Errorf("callDropIndexService is nil")
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

func (c *Core) recycleDroppedIndex() {
	defer c.wg.Done()
	ticker := time.NewTicker(3 * time.Second)

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			droppedIndex := c.MetaTable.GetDroppedIndex()
			for collID, fieldIndexes := range droppedIndex {
				for _, fieldIndex := range fieldIndexes {
					indexID := fieldIndex.GetIndexID()
					fieldID := fieldIndex.GetFiledID()
					if err := c.CallDropIndexService(c.ctx, indexID); err != nil {
						log.Warn("Notify IndexCoord to drop index failed, wait to retry", zap.Int64("collID", collID),
							zap.Int64("fieldID", fieldID), zap.Int64("indexID", indexID))
					}
				}
			}
			err := c.MetaTable.RecycleDroppedIndex()
			if err != nil {
				log.Warn("Remove index meta failed, wait to retry", zap.Error(err))
			}
		}
	}
}

func (c *Core) createIndexForSegment(ctx context.Context, collID, partID, segID UniqueID, numRows int64, binlogs []*datapb.FieldBinlog) error {
	collID2Meta, _, indexID2Meta := c.MetaTable.dupMeta()
	collMeta, ok := collID2Meta[collID]
	if !ok {
		log.Error("collection meta is not exist", zap.Int64("collID", collID))
		return fmt.Errorf("collection meta is not exist with ID = %d", collID)
	}
	if len(collMeta.FieldIndexes) == 0 {
		log.Info("collection has no index, no need to build index on segment", zap.Int64("collID", collID),
			zap.Int64("segID", segID))
		return nil
	}
	for _, fieldIndex := range collMeta.FieldIndexes {
		indexMeta, ok := indexID2Meta[fieldIndex.IndexID]
		if !ok {
			log.Warn("index has no meta", zap.Int64("collID", collID), zap.Int64("indexID", fieldIndex.IndexID))
			return fmt.Errorf("index has no meta with ID = %d in collection %d", fieldIndex.IndexID, collID)
		}
		if indexMeta.Deleted {
			log.Info("index has been deleted, no need to build index on segment")
			continue
		}

		field, err := GetFieldSchemaByID(&collMeta, fieldIndex.FiledID)
		if err != nil {
			log.Error("GetFieldSchemaByID failed",
				zap.Int64("collectionID", collID),
				zap.Int64("fieldID", fieldIndex.FiledID), zap.Error(err))
			return err
		}
		if c.MetaTable.IsSegmentIndexed(segID, field, indexMeta.IndexParams) {
			continue
		}
		createTS, err := c.TSOAllocator(1)
		if err != nil {
			log.Error("RootCoord alloc timestamp failed", zap.Int64("collectionID", collID), zap.Error(err))
			return err
		}

		segIndexInfo := etcdpb.SegmentIndexInfo{
			CollectionID: collMeta.ID,
			PartitionID:  partID,
			SegmentID:    segID,
			FieldID:      fieldIndex.FiledID,
			IndexID:      fieldIndex.IndexID,
			EnableIndex:  false,
			CreateTime:   createTS,
		}
		buildID, err := c.BuildIndex(ctx, segID, numRows, binlogs, field, &indexMeta, false)
		if err != nil {
			log.Debug("build index failed",
				zap.Int64("segmentID", segID),
				zap.Int64("fieldID", field.FieldID),
				zap.Int64("indexID", indexMeta.IndexID))
			return err
		}
		// if buildID == 0, means it's no need to build index.
		if buildID != 0 {
			segIndexInfo.BuildID = buildID
			segIndexInfo.EnableIndex = true
		}

		if err := c.MetaTable.AddIndex(&segIndexInfo); err != nil {
			log.Error("Add index into meta table failed, need remove index with buildID",
				zap.Int64("collectionID", collID), zap.Int64("indexID", fieldIndex.IndexID),
				zap.Int64("buildID", buildID), zap.Error(err))
			if err = retry.Do(ctx, func() error {
				return c.CallRemoveIndexService(ctx, []UniqueID{buildID})
			}); err != nil {
				log.Error("remove index failed, need to be resolved manually", zap.Int64("collectionID", collID),
					zap.Int64("indexID", fieldIndex.IndexID), zap.Int64("buildID", buildID), zap.Error(err))
				return err
			}
			return err
		}
	}
	return nil
}

func (c *Core) checkFlushedSegments(ctx context.Context) {
	collID2Meta := c.MetaTable.dupCollectionMeta()
	for collID, collMeta := range collID2Meta {
		if len(collMeta.FieldIndexes) == 0 {
			continue
		}
		for _, partID := range collMeta.PartitionIDs {
			segBinlogs, err := c.CallGetRecoveryInfoService(ctx, collMeta.ID, partID)
			if err != nil {
				log.Ctx(ctx).Debug("failed to get flushed segments from dataCoord",
					zap.Int64("collection ID", collMeta.GetID()),
					zap.Int64("partition ID", partID),
					zap.Error(err))
				continue
			}
			segIDs := make(map[UniqueID]struct{})
			for _, segBinlog := range segBinlogs {
				segIDs[segBinlog.GetSegmentID()] = struct{}{}
				err = c.createIndexForSegment(ctx, collID, partID, segBinlog.GetSegmentID(), segBinlog.GetNumOfRows(), segBinlog.GetFieldBinlogs())
				if err != nil {
					log.Error("createIndexForSegment failed, wait to retry", zap.Int64("collID", collID),
						zap.Int64("partID", partID), zap.Int64("segID", segBinlog.GetSegmentID()), zap.Error(err))
					continue
				}
			}
			recycledSegIDs, recycledBuildIDs := c.MetaTable.AlignSegmentsMeta(collID, partID, segIDs)
			log.Ctx(ctx).Info("there buildIDs should be remove index", zap.Int64s("buildIDs", recycledBuildIDs))
			if len(recycledBuildIDs) > 0 {
				if err := c.CallRemoveIndexService(ctx, recycledBuildIDs); err != nil {
					log.Ctx(ctx).Error("CallRemoveIndexService remove indexes on segments failed",
						zap.Int64s("need dropped buildIDs", recycledBuildIDs), zap.Error(err))
					continue
				}
			}

			if err := c.MetaTable.RemoveSegments(collID, partID, recycledSegIDs); err != nil {
				log.Ctx(ctx).Warn("remove segments failed, wait to retry", zap.Int64("collID", collID), zap.Int64("partID", partID),
					zap.Int64s("segIDs", recycledSegIDs), zap.Error(err))
				continue
			}
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
	for _, partID := range collMeta.PartitionIDs {
		if segs, err := c.CallGetRecoveryInfoService(ctx, collID, partID); err == nil {
			for _, s := range segs {
				segID2PartID[s.SegmentID] = partID
				segID2Binlog[s.SegmentID] = s
			}
		} else {
			log.Ctx(ctx).Error("failed to get flushed segments info from dataCoord",
				zap.Int64("collection ID", collID),
				zap.Int64("partition ID", partID),
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
					log.Ctx(ctx).Debug("RootCoord connected to DataCoord")
					return
				}
			}
			log.Ctx(ctx).Debug("Retrying RootCoord connection to DataCoord")
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
		log.Ctx(ctx).Info("flush on collection succeed", zap.Int64("collection ID", cID))
		return nil
	}

	c.CallAddSegRefLock = func(ctx context.Context, taskID int64, segIDs []int64) (retErr error) {
		defer func() {
			if err := recover(); err != nil {
				retErr = fmt.Errorf("add seg ref lock panic, msg = %v", err)
			}
		}()
		<-initCh
		log.Ctx(ctx).Info("acquiring seg lock",
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
		log.Ctx(ctx).Info("acquire seg lock succeed",
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
		log.Ctx(ctx).Info("releasing seg lock",
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
		log.Ctx(ctx).Info("release seg lock succeed",
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

	c.CallBuildIndexService = func(ctx context.Context, segID UniqueID, binlog []string, field *schemapb.FieldSchema, idxInfo *etcdpb.IndexInfo, numRows int64) (retID typeutil.UniqueID, retErr error) {
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
			SegmentID:   segID,
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
			log.Ctx(ctx).Error("RootCoord failed to get index states from IndexCoord.", zap.Error(err))
			return nil, err
		}
		log.Ctx(ctx).Debug("got index states", zap.String("get index state result", res.String()))
		if res.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			log.Ctx(ctx).Error("Get index states failed.",
				zap.String("error_code", res.GetStatus().GetErrorCode().String()),
				zap.String("reason", res.GetStatus().GetReason()))
			return nil, fmt.Errorf(res.GetStatus().GetErrorCode().String())
		}
		return res.GetStates(), nil
	}

	c.CallRemoveIndexService = func(ctx context.Context, buildIDs []UniqueID) (retErr error) {
		defer func() {
			if err := recover(); err != nil {
				retErr = fmt.Errorf("remove index from index service panic, msg = %v", err)
			}
		}()
		<-initCh
		status, err := s.RemoveIndex(ctx, &indexpb.RemoveIndexRequest{
			BuildIDs: buildIDs,
		})
		if err != nil {
			return err
		}

		if status.GetErrorCode() != commonpb.ErrorCode_Success {
			return fmt.Errorf(status.Reason)
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

// BuildIndex will check row num and call build index service
func (c *Core) BuildIndex(ctx context.Context, segID UniqueID, numRows int64, binlogs []*datapb.FieldBinlog, field *schemapb.FieldSchema, idxInfo *etcdpb.IndexInfo, isFlush bool) (typeutil.UniqueID, error) {
	log.Ctx(ctx).Debug("start build index", zap.String("index name", idxInfo.IndexName),
		zap.String("field name", field.Name), zap.Int64("segment id", segID))
	sp, ctx := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	if c.MetaTable.IsSegmentIndexed(segID, field, idxInfo.IndexParams) {
		info, err := c.MetaTable.GetSegmentIndexInfoByID(segID, field.FieldID, idxInfo.GetIndexName())
		return info.BuildID, err
	}
	var bldID UniqueID
	var err error
	if numRows < Params.RootCoordCfg.MinSegmentSizeToEnableIndex {
		log.Ctx(ctx).Debug("num of rows is less than MinSegmentSizeToEnableIndex", zap.Int64("num rows", numRows))
	} else {
		binLogs := make([]string, 0)
		for _, fieldBinLog := range binlogs {
			if fieldBinLog.GetFieldID() == field.GetFieldID() {
				for _, binLog := range fieldBinLog.GetBinlogs() {
					binLogs = append(binLogs, binLog.LogPath)
				}
				break
			}
		}
		bldID, err = c.CallBuildIndexService(ctx, segID, binLogs, field, idxInfo, numRows)
	}

	return bldID, err
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
	})
	if initError != nil {
		log.Debug("RootCoord init error", zap.Error(initError))
	}
	log.Debug("RootCoord init done")
	return initError
}

func (c *Core) initData() error {
	credInfo, _ := c.MetaTable.getCredential(util.UserRoot)
	if credInfo == nil {
		log.Debug("RootCoord init user root")
		encryptedRootPassword, _ := crypto.PasswordEncrypt(util.DefaultRootPassword)
		err := c.MetaTable.AddCredential(&internalpb.CredentialInfo{Username: util.UserRoot, EncryptedPassword: encryptedRootPassword})
		return err
	}
	return nil
}

func (c *Core) reSendDdMsg(ctx context.Context, force bool) error {
	if !force {
		flag, err := c.MetaTable.txn.Load(DDMsgSendPrefix)
		if err != nil {
			// TODO, this is super ugly hack but our kv interface does not support loadWithExist
			// leave it for later
			if strings.Contains(err.Error(), "there is no value on key") {
				log.Ctx(ctx).Debug("skip reSendDdMsg with no dd-msg-send key")
				return nil
			}
			return err
		}
		value, err := strconv.ParseBool(flag)
		if err != nil {
			return err
		}
		if value {
			log.Ctx(ctx).Debug("skip reSendDdMsg with dd-msg-send set to true")
			return nil
		}
	}

	ddOpStr, err := c.MetaTable.txn.Load(DDOperationPrefix)
	if err != nil {
		log.Ctx(ctx).Debug("DdOperation key does not exist")
		return nil
	}
	var ddOp DdOperation
	if err = json.Unmarshal([]byte(ddOpStr), &ddOp); err != nil {
		return err
	}

	invalidateCache := false
	var ts typeutil.Timestamp
	var collName string
	var collectionID UniqueID

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
			log.Ctx(ctx).Debug("collection has been created, skip re-send CreateCollection",
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
			collectionID = ddReq.CollectionID
		} else {
			log.Ctx(ctx).Debug("collection has been removed, skip re-send DropCollection",
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
			collectionID = ddReq.CollectionID
		} else {
			log.Ctx(ctx).Debug("partition has been created, skip re-send CreatePartition",
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
			collectionID = ddReq.CollectionID
		} else {
			log.Debug("partition has been removed, skip re-send DropPartition",
				zap.String("collection name", collName), zap.String("partition name", ddReq.PartitionName))
		}
	default:
		return fmt.Errorf("invalid DdOperation %s", ddOp.Type)
	}

	if invalidateCache {
		if err = c.ExpireMetaCache(ctx, nil, collectionID, ts); err != nil {
			return err
		}
	}

	// Update DDOperation in etcd
	return c.MetaTable.txn.Save(DDMsgSendPrefix, strconv.FormatBool(true))
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
		if err := c.reSendDdMsg(c.ctx, false); err != nil {
			log.Fatal("RootCoord Start reSendDdMsg failed", zap.Error(err))
			panic(err)
		}
		c.wg.Add(7)
		go c.startTimeTickLoop()
		go c.tsLoop()
		go c.chanTimeTick.startWatch(&c.wg)
		go c.checkFlushedSegmentsLoop()
		go c.importManager.expireOldTasksLoop(&c.wg, c.CallReleaseSegRefLock)
		go c.importManager.sendOutTasksLoop(&c.wg)
		go c.recycleDroppedIndex()
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
	log.Ctx(ctx).Debug("GetComponentStates", zap.String("State Code", internalpb.StateCode_name[int32(code)]))

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

	log.Ctx(ctx).Debug("CreateCollection", zap.String("role", typeutil.RootCoordRole),
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
		log.Ctx(ctx).Error("CreateCollection failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("collection name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("CreateCollection", metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, "CreateCollection failed: "+err.Error()), nil
	}
	log.Ctx(ctx).Debug("CreateCollection success", zap.String("role", typeutil.RootCoordRole),
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
	log.Ctx(ctx).Debug("DropCollection", zap.String("role", typeutil.RootCoordRole),
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
		log.Ctx(ctx).Error("DropCollection failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("collection name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("DropCollection", metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, "DropCollection failed: "+err.Error()), nil
	}
	log.Ctx(ctx).Debug("DropCollection success", zap.String("role", typeutil.RootCoordRole),
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

	log.Ctx(ctx).Debug("HasCollection", zap.String("role", typeutil.RootCoordRole),
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
		log.Ctx(ctx).Error("HasCollection failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("collection name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("HasCollection", metrics.FailLabel).Inc()
		return &milvuspb.BoolResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "HasCollection failed: "+err.Error()),
			Value:  false,
		}, nil
	}
	log.Ctx(ctx).Debug("HasCollection success", zap.String("role", typeutil.RootCoordRole),
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
		log.Ctx(ctx).Error("DescribeCollection failed", zap.String("role", typeutil.RootCoordRole),
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

	log.Ctx(ctx).Debug("ShowCollections", zap.String("role", typeutil.RootCoordRole),
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
		log.Ctx(ctx).Error("ShowCollections failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("dbname", in.DbName), zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("ShowCollections", metrics.FailLabel).Inc()
		return &milvuspb.ShowCollectionsResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "ShowCollections failed: "+err.Error()),
		}, nil
	}
	log.Ctx(ctx).Debug("ShowCollections success", zap.String("role", typeutil.RootCoordRole),
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
	log.Ctx(ctx).Debug("CreatePartition", zap.String("role", typeutil.RootCoordRole),
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
		log.Ctx(ctx).Error("CreatePartition failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName),
			zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("CreatePartition", metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, "CreatePartition failed: "+err.Error()), nil
	}
	log.Ctx(ctx).Debug("CreatePartition success", zap.String("role", typeutil.RootCoordRole),
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
	log.Ctx(ctx).Debug("DropPartition", zap.String("role", typeutil.RootCoordRole),
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
		log.Ctx(ctx).Error("DropPartition failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName),
			zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("DropPartition", metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, "DropPartition failed: "+err.Error()), nil
	}
	log.Ctx(ctx).Debug("DropPartition success", zap.String("role", typeutil.RootCoordRole),
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

	log.Ctx(ctx).Debug("HasPartition", zap.String("role", typeutil.RootCoordRole),
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
		log.Ctx(ctx).Error("HasPartition failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("collection name", in.CollectionName), zap.String("partition name", in.PartitionName),
			zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("HasPartition", metrics.FailLabel).Inc()
		return &milvuspb.BoolResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "HasPartition failed: "+err.Error()),
			Value:  false,
		}, nil
	}
	log.Ctx(ctx).Debug("HasPartition success", zap.String("role", typeutil.RootCoordRole),
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
	log.Ctx(ctx).Debug("ShowPartitions", zap.String("role", typeutil.RootCoordRole),
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
		log.Ctx(ctx).Error("ShowPartitions failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("collection name", in.CollectionName), zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("ShowPartitions", metrics.FailLabel).Inc()
		return &milvuspb.ShowPartitionsResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "ShowPartitions failed: "+err.Error()),
		}, nil
	}
	log.Ctx(ctx).Debug("ShowPartitions success", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.Int("num of partitions", len(t.Rsp.PartitionNames)),
		zap.Int64("msgID", t.Req.Base.MsgID))

	metrics.RootCoordDDLReqCounter.WithLabelValues("ShowPartitions", metrics.SuccessLabel).Inc()
	t.Rsp.Status = succStatus()
	metrics.RootCoordDDLReqLatency.WithLabelValues("ShowPartitions").Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.Rsp, nil
}

// CreateIndex create index
func (c *Core) CreateIndex(ctx context.Context, in *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	metrics.RootCoordDDLReqCounter.WithLabelValues("CreateIndex", metrics.TotalLabel).Inc()
	if code, ok := c.checkHealthy(); !ok {
		return failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]), nil
	}
	tr := timerecord.NewTimeRecorder("CreateIndex")
	log.Ctx(ctx).Debug("CreateIndex", zap.String("role", typeutil.RootCoordRole),
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
		log.Ctx(ctx).Error("CreateIndex failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName),
			zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("CreateIndex", metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, "CreateIndex failed: "+err.Error()), nil
	}
	log.Ctx(ctx).Debug("CreateIndex success", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName),
		zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordDDLReqCounter.WithLabelValues("CreateIndex", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("CreateIndex").Observe(float64(tr.ElapseSpan().Milliseconds()))
	return succStatus(), nil
}

// DescribeIndex return index info
func (c *Core) DescribeIndex(ctx context.Context, in *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeIndex", metrics.TotalLabel).Inc()
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.DescribeIndexResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
		}, nil
	}
	tr := timerecord.NewTimeRecorder("DescribeIndex")
	log.Ctx(ctx).Debug("DescribeIndex", zap.String("role", typeutil.RootCoordRole),
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
		log.Ctx(ctx).Error("DescribeIndex failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName),
			zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeIndex", metrics.FailLabel).Inc()
		return &milvuspb.DescribeIndexResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "DescribeIndex failed: "+err.Error()),
		}, nil
	}
	idxNames := make([]string, 0, len(t.Rsp.IndexDescriptions))
	for _, i := range t.Rsp.IndexDescriptions {
		idxNames = append(idxNames, i.IndexName)
	}
	log.Ctx(ctx).Debug("DescribeIndex success", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName),
		zap.Strings("index names", idxNames), zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeIndex", metrics.SuccessLabel).Inc()
	if len(t.Rsp.IndexDescriptions) == 0 {
		t.Rsp.Status = failStatus(commonpb.ErrorCode_IndexNotExist, "index not exist")
	} else {
		t.Rsp.Status = succStatus()
	}
	metrics.RootCoordDDLReqLatency.WithLabelValues("DescribeIndex").Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.Rsp, nil
}

func (c *Core) GetIndexState(ctx context.Context, in *milvuspb.GetIndexStateRequest) (*indexpb.GetIndexStatesResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		log.Ctx(ctx).Error("RootCoord GetIndexState failed, RootCoord is not healthy")
		return &indexpb.GetIndexStatesResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
		}, nil
	}
	log.Ctx(ctx).Info("RootCoord GetIndexState", zap.String("collName", in.GetCollectionName()),
		zap.String("fieldName", in.GetFieldName()), zap.String("indexName", in.GetIndexName()))

	// initBuildIDs are the buildIDs generated when CreateIndex is called.
	initBuildIDs, err := c.MetaTable.GetInitBuildIDs(in.GetCollectionName(), in.GetIndexName())
	if err != nil {
		log.Ctx(ctx).Error("RootCoord GetIndexState failed", zap.String("collName", in.GetCollectionName()),
			zap.String("fieldName", in.GetFieldName()), zap.String("indexName", in.GetIndexName()), zap.Error(err))
		return &indexpb.GetIndexStatesResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, err.Error()),
		}, nil
	}

	ret := &indexpb.GetIndexStatesResponse{
		Status: succStatus(),
	}
	if len(initBuildIDs) == 0 {
		log.Ctx(ctx).Warn("RootCoord GetIndexState successful, all segments generated when CreateIndex is called have been compacted")
		return ret, nil
	}

	states, err := c.CallGetIndexStatesService(ctx, initBuildIDs)
	if err != nil {
		log.Ctx(ctx).Error("RootCoord GetIndexState CallGetIndexStatesService failed", zap.String("collName", in.GetCollectionName()),
			zap.String("fieldName", in.GetFieldName()), zap.String("indexName", in.GetIndexName()), zap.Error(err))
		ret.Status = failStatus(commonpb.ErrorCode_UnexpectedError, err.Error())
		return ret, err
	}

	log.Ctx(ctx).Info("RootCoord GetIndexState successful", zap.String("collName", in.GetCollectionName()),
		zap.String("fieldName", in.GetFieldName()), zap.String("indexName", in.GetIndexName()))
	return &indexpb.GetIndexStatesResponse{
		Status: succStatus(),
		States: states,
	}, nil
}

// DropIndex drop index
func (c *Core) DropIndex(ctx context.Context, in *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	metrics.RootCoordDDLReqCounter.WithLabelValues("DropIndex", metrics.TotalLabel).Inc()
	if code, ok := c.checkHealthy(); !ok {
		return failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]), nil
	}
	tr := timerecord.NewTimeRecorder("DropIndex")
	log.Ctx(ctx).Debug("DropIndex", zap.String("role", typeutil.RootCoordRole),
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
		log.Ctx(ctx).Error("DropIndex failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName),
			zap.String("index name", in.IndexName), zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("DropIndex", metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, "DropIndex failed: "+err.Error()), nil
	}
	log.Ctx(ctx).Debug("DropIndex success", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.CollectionName), zap.String("field name", in.FieldName),
		zap.String("index name", in.IndexName), zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordDDLReqCounter.WithLabelValues("DropIndex", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("DropIndex").Observe(float64(tr.ElapseSpan().Milliseconds()))
	return succStatus(), nil
}

// DescribeSegment return segment info
func (c *Core) DescribeSegment(ctx context.Context, in *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error) {
	metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeSegment", metrics.TotalLabel).Inc()
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.DescribeSegmentResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
		}, nil
	}
	tr := timerecord.NewTimeRecorder("DescribeSegment")
	log.Ctx(ctx).Debug("DescribeSegment", zap.String("role", typeutil.RootCoordRole),
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
		log.Ctx(ctx).Error("DescribeSegment failed", zap.String("role", typeutil.RootCoordRole),
			zap.Int64("collection id", in.CollectionID), zap.Int64("segment id", in.SegmentID),
			zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeSegment", metrics.FailLabel).Inc()
		return &milvuspb.DescribeSegmentResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "DescribeSegment failed: "+err.Error()),
		}, nil
	}
	log.Ctx(ctx).Debug("DescribeSegment success", zap.String("role", typeutil.RootCoordRole),
		zap.Int64("collection id", in.CollectionID), zap.Int64("segment id", in.SegmentID),
		zap.Int64("msgID", in.Base.MsgID))

	metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeSegment", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("DescribeSegment").Observe(float64(tr.ElapseSpan().Milliseconds()))
	t.Rsp.Status = succStatus()
	return t.Rsp, nil
}

func (c *Core) DescribeSegments(ctx context.Context, in *rootcoordpb.DescribeSegmentsRequest) (*rootcoordpb.DescribeSegmentsResponse, error) {
	metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeSegments", metrics.TotalLabel).Inc()
	if code, ok := c.checkHealthy(); !ok {
		log.Ctx(ctx).Error("failed to describe segments, rootcoord not healthy",
			zap.String("role", typeutil.RootCoordRole),
			zap.Int64("msgID", in.GetBase().GetMsgID()),
			zap.Int64("collection", in.GetCollectionID()),
			zap.Int64s("segments", in.GetSegmentIDs()))

		return &rootcoordpb.DescribeSegmentsResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
		}, nil
	}

	tr := timerecord.NewTimeRecorder("DescribeSegments")

	log.Ctx(ctx).Debug("received request to describe segments",
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
		log.Ctx(ctx).Error("failed to describe segments",
			zap.Error(err),
			zap.String("role", typeutil.RootCoordRole),
			zap.Int64("msgID", in.GetBase().GetMsgID()),
			zap.Int64("collection", in.GetCollectionID()),
			zap.Int64s("segments", in.GetSegmentIDs()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeSegments", metrics.FailLabel).Inc()
		return &rootcoordpb.DescribeSegmentsResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "DescribeSegments failed: "+err.Error()),
		}, nil
	}

	log.Ctx(ctx).Debug("succeed to describe segments",
		zap.String("role", typeutil.RootCoordRole),
		zap.Int64("msgID", in.GetBase().GetMsgID()),
		zap.Int64("collection", in.GetCollectionID()),
		zap.Int64s("segments", in.GetSegmentIDs()))

	metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeSegments", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("DescribeSegments").Observe(float64(tr.ElapseSpan().Milliseconds()))

	t.Rsp.Status = succStatus()
	return t.Rsp, nil
}

// ShowSegments list all segments
func (c *Core) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error) {
	metrics.RootCoordDDLReqCounter.WithLabelValues("ShowSegments", metrics.TotalLabel).Inc()
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.ShowSegmentsResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
		}, nil
	}
	tr := timerecord.NewTimeRecorder("ShowSegments")

	log.Ctx(ctx).Debug("ShowSegments", zap.String("role", typeutil.RootCoordRole),
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
		log.Ctx(ctx).Debug("ShowSegments failed", zap.String("role", typeutil.RootCoordRole),
			zap.Int64("collection id", in.CollectionID), zap.Int64("partition id", in.PartitionID),
			zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("ShowSegments", metrics.FailLabel).Inc()
		return &milvuspb.ShowSegmentsResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "ShowSegments failed: "+err.Error()),
		}, nil
	}
	log.Ctx(ctx).Debug("ShowSegments success", zap.String("role", typeutil.RootCoordRole),
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
		log.Ctx(ctx).Error("AllocTimestamp failed", zap.String("role", typeutil.RootCoordRole),
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
		log.Ctx(ctx).Error("AllocID failed", zap.String("role", typeutil.RootCoordRole),
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
		log.Ctx(ctx).Warn("failed to updateTimeTick because rootcoord is not healthy", zap.Any("state", code))
		return failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]), nil
	}
	if in.Base.MsgType != commonpb.MsgType_TimeTick {
		log.Ctx(ctx).Warn("failed to updateTimeTick because base messasge is not timetick, state", zap.Any("base message type", in.Base.MsgType))
		msgTypeName := commonpb.MsgType_name[int32(in.Base.GetMsgType())]
		return failStatus(commonpb.ErrorCode_UnexpectedError, "invalid message type "+msgTypeName), nil
	}
	err := c.chanTimeTick.updateTimeTick(in, "gRPC")
	if err != nil {
		log.Ctx(ctx).Warn("failed to updateTimeTick", zap.String("role", typeutil.RootCoordRole),
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

// SegmentFlushCompleted check whether segment flush has completed
func (c *Core) SegmentFlushCompleted(ctx context.Context, in *datapb.SegmentFlushCompletedMsg) (status *commonpb.Status, err error) {
	if code, ok := c.checkHealthy(); !ok {
		return failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]), nil
	}
	if in.Base.MsgType != commonpb.MsgType_SegmentFlushDone {
		return failStatus(commonpb.ErrorCode_UnexpectedError, "invalid msg type "+commonpb.MsgType_name[int32(in.Base.MsgType)]), nil
	}

	log.Ctx(ctx).Info("SegmentFlushCompleted received", zap.Int64("msgID", in.Base.MsgID), zap.Int64("collID", in.Segment.CollectionID),
		zap.Int64("partID", in.Segment.PartitionID), zap.Int64("segID", in.Segment.ID), zap.Int64s("compactFrom", in.Segment.CompactionFrom))

	err = c.createIndexForSegment(ctx, in.Segment.CollectionID, in.Segment.PartitionID, in.Segment.ID, in.Segment.NumOfRows, in.Segment.Binlogs)
	if err != nil {
		log.Ctx(ctx).Error("createIndexForSegment", zap.Int64("msgID", in.Base.MsgID), zap.Int64("collID", in.Segment.CollectionID),
			zap.Int64("partID", in.Segment.PartitionID), zap.Int64("segID", in.Segment.ID), zap.Error(err))
		return failStatus(commonpb.ErrorCode_UnexpectedError, err.Error()), nil
	}

	buildIDs := c.MetaTable.GetBuildIDsBySegIDs(in.Segment.CompactionFrom)
	if len(buildIDs) != 0 {
		if err = c.CallRemoveIndexService(ctx, buildIDs); err != nil {
			log.Error("CallRemoveIndexService failed", zap.Int64("msgID", in.Base.MsgID), zap.Int64("collID", in.Segment.CollectionID),
				zap.Int64("partID", in.Segment.PartitionID), zap.Int64("segID", in.Segment.ID),
				zap.Int64s("compactFrom", in.Segment.CompactionFrom), zap.Int64s("buildIDs", buildIDs), zap.Error(err))
			return failStatus(commonpb.ErrorCode_UnexpectedError, err.Error()), nil
		}
	}

	if err = c.MetaTable.RemoveSegments(in.Segment.CollectionID, in.Segment.PartitionID, in.Segment.CompactionFrom); err != nil {
		log.Ctx(ctx).Error("RemoveSegments failed", zap.Int64("msgID", in.Base.MsgID), zap.Int64("collID", in.Segment.CollectionID),
			zap.Int64("partID", in.Segment.PartitionID), zap.Int64("segID", in.Segment.ID),
			zap.Int64s("compactFrom", in.Segment.CompactionFrom), zap.Error(err))
		return failStatus(commonpb.ErrorCode_UnexpectedError, err.Error()), nil
	}

	log.Ctx(ctx).Debug("SegmentFlushCompleted success", zap.String("role", typeutil.RootCoordRole),
		zap.Int64("collection id", in.Segment.CollectionID), zap.Int64("partition id", in.Segment.PartitionID),
		zap.Int64("segment id", in.Segment.ID), zap.Int64("msgID", in.Base.MsgID))
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
		log.Ctx(ctx).Error("ParseMetricType failed", zap.String("role", typeutil.RootCoordRole),
			zap.Int64("node_id", c.session.ServerID), zap.String("req", in.Request), zap.Error(err))
		return &milvuspb.GetMetricsResponse{
			Status:   failStatus(commonpb.ErrorCode_UnexpectedError, "ParseMetricType failed: "+err.Error()),
			Response: "",
		}, nil
	}

	log.Ctx(ctx).Debug("GetMetrics success", zap.String("role", typeutil.RootCoordRole),
		zap.String("metric_type", metricType), zap.Int64("msgID", in.Base.MsgID))

	if metricType == metricsinfo.SystemInfoMetrics {
		ret, err := c.metricsCacheManager.GetSystemInfoMetrics()
		if err == nil && ret != nil {
			return ret, nil
		}

		log.Ctx(ctx).Warn("GetSystemInfoMetrics from cache failed", zap.String("role", typeutil.RootCoordRole),
			zap.Int64("msgID", in.Base.MsgID), zap.Error(err))

		systemInfoMetrics, err := c.getSystemInfoMetrics(ctx, in)
		if err != nil {
			log.Ctx(ctx).Error("GetSystemInfoMetrics failed", zap.String("role", typeutil.RootCoordRole),
				zap.String("metric_type", metricType), zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
			return nil, err
		}

		c.metricsCacheManager.UpdateSystemInfoMetrics(systemInfoMetrics)
		return systemInfoMetrics, err
	}

	log.Ctx(ctx).Error("GetMetrics failed, metric type not implemented", zap.String("role", typeutil.RootCoordRole),
		zap.String("metric_type", metricType), zap.Int64("msgID", in.Base.MsgID))

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
	log.Ctx(ctx).Debug("CreateAlias", zap.String("role", typeutil.RootCoordRole),
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
		log.Ctx(ctx).Error("CreateAlias failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("alias", in.Alias), zap.String("collection name", in.CollectionName),
			zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("CreateAlias", metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, "CreateAlias failed: "+err.Error()), nil
	}
	log.Ctx(ctx).Debug("CreateAlias success", zap.String("role", typeutil.RootCoordRole),
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
	log.Ctx(ctx).Debug("DropAlias", zap.String("role", typeutil.RootCoordRole),
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
		log.Ctx(ctx).Error("DropAlias failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("alias", in.Alias), zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("DropAlias", metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, "DropAlias failed: "+err.Error()), nil
	}
	log.Ctx(ctx).Debug("DropAlias success", zap.String("role", typeutil.RootCoordRole),
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
	log.Ctx(ctx).Debug("AlterAlias", zap.String("role", typeutil.RootCoordRole),
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
		log.Ctx(ctx).Error("AlterAlias failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("alias", in.Alias), zap.String("collection name", in.CollectionName),
			zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("AlterAlias", metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, "AlterAlias failed: "+err.Error()), nil
	}
	log.Ctx(ctx).Debug("AlterAlias success", zap.String("role", typeutil.RootCoordRole),
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
		log.Ctx(ctx).Error("failed to find collection ID from its name",
			zap.String("collection name", req.GetCollectionName()),
			zap.Error(err))
		return nil, err
	}
	var pID UniqueID
	if pID, err = c.MetaTable.getPartitionByName(cID, req.GetPartitionName(), 0); err != nil {
		log.Ctx(ctx).Error("failed to get partition ID from its name",
			zap.String("partition name", req.GetPartitionName()),
			zap.Error(err))
		return nil, err
	}
	log.Ctx(ctx).Info("RootCoord receive import request",
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
	log.Ctx(ctx).Info("RootCoord receive import state report",
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
			log.Ctx(ctx).Error("failed to acquire segment ref lock", zap.Error(err))
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
			log.Ctx(ctx).Info("DataNode is no longer busy",
				zap.Int64("dataNode ID", ir.GetDatanodeId()),
				zap.Int64("task ID", ir.GetTaskId()))

		}()
		c.importManager.sendOutTasks(c.importManager.ctx)
	}

	// If task failed, send task to idle datanode
	if ir.GetState() == commonpb.ImportState_ImportFailed {
		// Release segments when task fails.
		log.Ctx(ctx).Info("task failed, release segment ref locks")
		err := retry.Do(ctx, func() error {
			return c.CallReleaseSegRefLock(ctx, ir.GetTaskId(), ir.GetSegments())
		}, retry.Attempts(100))
		if err != nil {
			log.Ctx(ctx).Error("failed to release lock, about to panic!")
			panic(err)
		}
		resendTaskFunc()
	}

	// So much for reporting, unless the task just reached `ImportPersisted` state.
	if ir.GetState() != commonpb.ImportState_ImportPersisted {
		log.Ctx(ctx).Debug("non import-persisted state received, return immediately",
			zap.Any("task ID", ir.GetTaskId()),
			zap.Any("import state", ir.GetState()))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		}, nil
	}

	// Look up collection name on collection ID.
	var colName string
	var colMeta *etcdpb.CollectionInfo
	if colMeta, err = c.MetaTable.GetCollectionByID(ti.GetCollectionId(), 0); err != nil {
		log.Ctx(ctx).Error("failed to get collection name",
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
	colName = colMeta.GetSchema().GetName()

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
		return false, err
	}
	if len(indexDescriptionResp.GetIndexDescriptions()) == 0 {
		log.Ctx(ctx).Info("no index needed for collection, consider indexing done",
			zap.Int64("collection ID", collectionID))
		return true, nil
	}
	log.Ctx(ctx).Debug("got index description",
		zap.Any("index description", indexDescriptionResp))

	// Check if the target index name exists.
	matchIndexID := int64(-1)
	foundIndexID := false
	for _, desc := range indexDescriptionResp.GetIndexDescriptions() {
		if desc.GetIndexName() == indexName {
			matchIndexID = desc.GetIndexID()
			foundIndexID = true
			break
		}
	}
	if !foundIndexID {
		return false, fmt.Errorf("no index is created")
	}
	log.Ctx(ctx).Debug("found match index ID",
		zap.Int64("match index ID", matchIndexID))

	getIndexStatesRequest := &indexpb.GetIndexStatesRequest{
		IndexBuildIDs: make([]UniqueID, 0),
	}

	// Fetch index build IDs from segments.
	var seg2Check []UniqueID
	for _, segmentID := range allSegmentIDs {
		describeSegmentRequest := &milvuspb.DescribeSegmentRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_DescribeSegment,
			},
			CollectionID: collectionID,
			SegmentID:    segmentID,
		}
		segmentDesc, _ := c.DescribeSegment(ctx, describeSegmentRequest)
		if segmentDesc.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			// Describe failed, since the segment could get compacted, simply log and ignore the error.
			log.Ctx(ctx).Error("failed to describe segment",
				zap.Int64("collection ID", collectionID),
				zap.Int64("segment ID", segmentID),
				zap.String("error", segmentDesc.GetStatus().GetReason()))
		}
		if segmentDesc.GetIndexID() == matchIndexID {
			if segmentDesc.GetEnableIndex() {
				seg2Check = append(seg2Check, segmentID)
				getIndexStatesRequest.IndexBuildIDs = append(getIndexStatesRequest.GetIndexBuildIDs(), segmentDesc.GetBuildID())
			}
		}
	}
	if len(getIndexStatesRequest.GetIndexBuildIDs()) == 0 {
		log.Ctx(ctx).Info("none index build IDs returned, perhaps no index is needed",
			zap.String("collection name", collectionName),
			zap.Int64("collection ID", collectionID))
		return true, nil
	}

	log.Ctx(ctx).Debug("working on GetIndexState",
		zap.Int("# of IndexBuildIDs", len(getIndexStatesRequest.GetIndexBuildIDs())))

	states, err := c.CallGetIndexStatesService(ctx, getIndexStatesRequest.GetIndexBuildIDs())
	if err != nil {
		log.Ctx(ctx).Error("failed to get index state in checkSegmentIndexStates", zap.Error(err))
		return false, err
	}

	// Count the # of segments with finished index.
	ct := 0
	for _, s := range states {
		if s.State == commonpb.IndexState_Finished {
			ct++
		}
	}
	log.Ctx(ctx).Info("segment indexing state checked",
		zap.Int64s("segments checked", seg2Check),
		zap.Int("# of checked segment", len(seg2Check)),
		zap.Int("# of segments with complete index", ct),
		zap.String("collection name", collectionName),
		zap.Int64("collection ID", collectionID),
	)
	return len(seg2Check) == ct, nil
}

func (c *Core) postImportPersistLoop(ctx context.Context, taskID int64, colID int64, colName string, segIDs []UniqueID) {
	// Loop and check if segments are loaded in queryNodes.
	c.wg.Add(1)
	go c.checkSegmentLoadedLoop(ctx, taskID, colID, segIDs)
	// Check if collection has any indexed fields. If so, start a loop to check segments' index states.
	if colMeta, err := c.MetaTable.GetCollectionByID(colID, 0); err != nil {
		log.Ctx(ctx).Error("failed to find meta for collection",
			zap.Int64("collection ID", colID),
			zap.Error(err))
	} else if len(colMeta.GetFieldIndexes()) == 0 {
		log.Ctx(ctx).Info("no index field found for collection", zap.Int64("collection ID", colID))
	} else {
		log.Ctx(ctx).Info("start checking index state", zap.Int64("collection ID", colID))
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
		log.Ctx(ctx).Info("we are done checking segment loading state, release segment ref locks")
		err := retry.Do(ctx, func() error {
			return c.CallReleaseSegRefLock(ctx, taskID, segIDs)
		}, retry.Attempts(100))
		if err != nil {
			log.Ctx(ctx).Error("failed to release lock, about to panic!")
			panic(err)
		}
	}()
	for {
		select {
		case <-c.ctx.Done():
			log.Ctx(ctx).Info("(in check segment loaded loop) context done, exiting checkSegmentLoadedLoop")
			return
		case <-ticker.C:
			resp, err := c.CallGetSegmentInfoService(ctx, colID, segIDs)
			log.Ctx(ctx).Debug("(in check segment loaded loop)",
				zap.Int64("task ID", taskID),
				zap.Int64("collection ID", colID),
				zap.Int64s("segment IDs expected", segIDs),
				zap.Int("# of segments found", len(resp.GetInfos())))
			if err != nil {
				log.Ctx(ctx).Warn("(in check segment loaded loop) failed to call get segment info on queryCoord",
					zap.Int64("task ID", taskID),
					zap.Int64("collection ID", colID),
					zap.Int64s("segment IDs", segIDs))
			} else if len(resp.GetInfos()) == len(segIDs) {
				// Check if all segment info are loaded in queryNodes.
				log.Ctx(ctx).Info("(in check segment loaded loop) all import data segments loaded in queryNodes",
					zap.Int64("task ID", taskID),
					zap.Int64("collection ID", colID),
					zap.Int64s("segment IDs", segIDs))
				c.importManager.setTaskDataQueryable(taskID)
				return
			}
		case <-expireTicker.C:
			log.Ctx(ctx).Warn("(in check segment loaded loop) segments still not loaded after max wait time",
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
			log.Ctx(ctx).Info("(in check complete index loop) context done, exiting checkCompleteIndexLoop")
			return
		case <-ticker.C:
			if done, err := c.CountCompleteIndex(ctx, colName, colID, segIDs); err == nil && done {
				log.Ctx(ctx).Info("(in check complete index loop) indices are built or no index needed",
					zap.Int64("task ID", taskID))
				c.importManager.setTaskDataIndexed(taskID)
				return
			} else if err != nil {
				log.Ctx(ctx).Error("(in check complete index loop) an error occurs",
					zap.Error(err))
			}
		case <-expireTicker.C:
			log.Ctx(ctx).Warn("(in check complete index loop) indexing is taken too long",
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
	log.Ctx(ctx).Debug("CreateCredential", zap.String("role", typeutil.RootCoordRole),
		zap.String("username", credInfo.Username))

	if cred, _ := c.MetaTable.getCredential(credInfo.Username); cred != nil {
		return failStatus(commonpb.ErrorCode_CreateCredentialFailure, "user already exists:"+credInfo.Username), nil
	}
	// insert to db
	err := c.MetaTable.AddCredential(credInfo)
	if err != nil {
		log.Ctx(ctx).Error("CreateCredential save credential failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("username", credInfo.Username), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_CreateCredentialFailure, "CreateCredential failed: "+err.Error()), nil
	}
	// update proxy's local cache
	err = c.UpdateCredCache(ctx, credInfo)
	if err != nil {
		log.Ctx(ctx).Warn("CreateCredential add cache failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("username", credInfo.Username), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
	}
	log.Ctx(ctx).Debug("CreateCredential success", zap.String("role", typeutil.RootCoordRole),
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
	log.Ctx(ctx).Debug("GetCredential", zap.String("role", typeutil.RootCoordRole),
		zap.String("username", in.Username))

	credInfo, err := c.MetaTable.getCredential(in.Username)
	if err != nil {
		log.Ctx(ctx).Error("GetCredential query credential failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("username", in.Username), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return &rootcoordpb.GetCredentialResponse{
			Status: failStatus(commonpb.ErrorCode_GetCredentialFailure, "GetCredential failed: "+err.Error()),
		}, err
	}
	log.Ctx(ctx).Debug("GetCredential success", zap.String("role", typeutil.RootCoordRole),
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
	log.Ctx(ctx).Debug("UpdateCredential", zap.String("role", typeutil.RootCoordRole),
		zap.String("username", credInfo.Username))
	// update data on storage
	err := c.MetaTable.AddCredential(credInfo)
	if err != nil {
		log.Ctx(ctx).Error("UpdateCredential save credential failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("username", credInfo.Username), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UpdateCredentialFailure, "UpdateCredential failed: "+err.Error()), nil
	}
	// update proxy's local cache
	err = c.UpdateCredCache(ctx, credInfo)
	if err != nil {
		log.Ctx(ctx).Error("UpdateCredential update cache failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("username", credInfo.Username), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UpdateCredentialFailure, "UpdateCredential failed: "+err.Error()), nil
	}
	log.Ctx(ctx).Debug("UpdateCredential success", zap.String("role", typeutil.RootCoordRole),
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
		log.Ctx(ctx).Error("DeleteCredential remove credential failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("username", in.Username), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_DeleteCredentialFailure, "DeleteCredential failed: "+err.Error()), err
	}
	// invalidate proxy's local cache
	err = c.ExpireCredCache(ctx, in.Username)
	if err != nil {
		log.Ctx(ctx).Error("DeleteCredential expire credential cache failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("username", in.Username), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_DeleteCredentialFailure, "DeleteCredential failed: "+err.Error()), nil
	}
	log.Ctx(ctx).Debug("DeleteCredential success", zap.String("role", typeutil.RootCoordRole),
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
		log.Ctx(ctx).Error("ListCredUsers query usernames failed", zap.String("role", typeutil.RootCoordRole),
			zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return &milvuspb.ListCredUsersResponse{
			Status: failStatus(commonpb.ErrorCode_ListCredUsersFailure, "ListCredUsers failed: "+err.Error()),
		}, err
	}
	log.Ctx(ctx).Debug("ListCredUsers success", zap.String("role", typeutil.RootCoordRole))

	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &milvuspb.ListCredUsersResponse{
		Status:    succStatus(),
		Usernames: credInfo.Usernames,
	}, nil
}

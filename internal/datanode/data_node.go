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

// Package datanode implements data persistence logic.
//
// Data node persists insert logs into persistent storage like minIO/S3.
package datanode

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
	"github.com/milvus-io/milvus/api/schemapb"
	allocator2 "github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/importutil"
	"github.com/milvus-io/milvus/internal/util/logutil"
	"github.com/milvus-io/milvus/internal/util/metautil"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	// RPCConnectionTimeout is used to set the timeout for rpc request
	RPCConnectionTimeout = 30 * time.Second

	// MetricRequestsTotal is used to count the num of total requests
	MetricRequestsTotal = "total"

	// MetricRequestsSuccess is used to count the num of successful requests
	MetricRequestsSuccess = "success"

	// ConnectEtcdMaxRetryTime is used to limit the max retry time for connection etcd
	ConnectEtcdMaxRetryTime = 100
)

var getFlowGraphServiceAttempts = uint(50)

// makes sure DataNode implements types.DataNode
var _ types.DataNode = (*DataNode)(nil)

// Params from config.yaml
var Params paramtable.ComponentParam

// rateCol is global rateCollector in DataNode.
var rateCol *rateCollector

// DataNode communicates with outside services and unioun all
// services in datanode package.
//
// DataNode implements `types.Component`, `types.DataNode` interfaces.
//  `etcdCli`   is a connection of etcd
//  `rootCoord` is a grpc client of root coordinator.
//  `dataCoord` is a grpc client of data service.
//  `NodeID` is unique to each datanode.
//  `State` is current statement of this data node, indicating whether it's healthy.
//
//  `clearSignal` is a signal channel for releasing the flowgraph resources.
//  `segmentCache` stores all flushing and flushed segments.
type DataNode struct {
	ctx    context.Context
	cancel context.CancelFunc
	Role   string
	State  atomic.Value // internalpb.StateCode_Initializing

	flowgraphManager *flowgraphManager
	eventManagerMap  sync.Map // vchannel name -> channelEventManager

	clearSignal        chan string // vchannel name
	segmentCache       *Cache
	compactionExecutor *compactionExecutor

	etcdCli   *clientv3.Client
	rootCoord types.RootCoord
	dataCoord types.DataCoord

	session        *sessionutil.Session
	watchKv        kv.MetaKv
	chunkManager   storage.ChunkManager
	rowIDAllocator *allocator2.IDAllocator

	closer io.Closer

	factory dependency.Factory
}

// NewDataNode will return a DataNode with abnormal state.
func NewDataNode(ctx context.Context, factory dependency.Factory) *DataNode {
	rand.Seed(time.Now().UnixNano())
	ctx2, cancel2 := context.WithCancel(ctx)
	node := &DataNode{
		ctx:    ctx2,
		cancel: cancel2,
		Role:   typeutil.DataNodeRole,

		rootCoord:          nil,
		dataCoord:          nil,
		factory:            factory,
		segmentCache:       newCache(),
		compactionExecutor: newCompactionExecutor(),

		flowgraphManager: newFlowgraphManager(),
		clearSignal:      make(chan string, 100),
	}
	node.UpdateStateCode(internalpb.StateCode_Abnormal)
	return node
}

// SetEtcdClient sets etcd client for DataNode
func (node *DataNode) SetEtcdClient(etcdCli *clientv3.Client) {
	node.etcdCli = etcdCli
}

// SetRootCoord sets RootCoord's grpc client, error is returned if repeatedly set.
func (node *DataNode) SetRootCoord(rc types.RootCoord) error {
	switch {
	case rc == nil, node.rootCoord != nil:
		return errors.New("Nil parameter or repeatly set")
	default:
		node.rootCoord = rc
		return nil
	}
}

// SetDataCoord sets data service's grpc client, error is returned if repeatedly set.
func (node *DataNode) SetDataCoord(ds types.DataCoord) error {
	switch {
	case ds == nil, node.dataCoord != nil:
		return errors.New("Nil parameter or repeatly set")
	default:
		node.dataCoord = ds
		return nil
	}
}

// Register register datanode to etcd
func (node *DataNode) Register() error {
	node.session.Register()

	// Start liveness check
	go node.session.LivenessCheck(node.ctx, func() {
		log.Error("Data Node disconnected from etcd, process will exit", zap.Int64("Server Id", node.session.ServerID))
		if err := node.Stop(); err != nil {
			log.Fatal("failed to stop server", zap.Error(err))
		}
		// manually send signal to starter goroutine
		if node.session.TriggerKill {
			if p, err := os.FindProcess(os.Getpid()); err == nil {
				p.Signal(syscall.SIGINT)
			}
		}
	})

	return nil
}

func (node *DataNode) initSession() error {
	node.session = sessionutil.NewSession(node.ctx, Params.EtcdCfg.MetaRootPath, node.etcdCli)
	if node.session == nil {
		return errors.New("failed to initialize session")
	}
	node.session.Init(typeutil.DataNodeRole, Params.DataNodeCfg.IP+":"+strconv.Itoa(Params.DataNodeCfg.Port), false, true)
	Params.DataNodeCfg.SetNodeID(node.session.ServerID)
	Params.SetLogger(Params.DataNodeCfg.GetNodeID())
	return nil
}

// initRateCollector creates and starts rateCollector in QueryNode.
func (node *DataNode) initRateCollector() error {
	var err error
	rateCol, err = newRateCollector()
	if err != nil {
		return err
	}
	rateCol.Register(metricsinfo.InsertConsumeThroughput)
	rateCol.Register(metricsinfo.DeleteConsumeThroughput)
	return nil
}

// Init function does nothing now.
func (node *DataNode) Init() error {
	log.Info("DataNode Init",
		zap.String("TimeTickChannelName", Params.CommonCfg.DataCoordTimeTick),
	)
	if err := node.initSession(); err != nil {
		log.Error("DataNode init session failed", zap.Error(err))
		return err
	}

	err := node.initRateCollector()
	if err != nil {
		log.Error("DataNode init rateCollector failed", zap.Int64("nodeID", Params.QueryNodeCfg.GetNodeID()), zap.Error(err))
		return err
	}
	log.Info("DataNode init rateCollector done", zap.Int64("nodeID", Params.QueryNodeCfg.GetNodeID()))

	idAllocator, err := allocator2.NewIDAllocator(node.ctx, node.rootCoord, Params.DataNodeCfg.GetNodeID())
	if err != nil {
		log.Error("failed to create id allocator",
			zap.Error(err),
			zap.String("role", typeutil.DataNodeRole), zap.Int64("DataNodeID", Params.DataNodeCfg.GetNodeID()))
		return err
	}
	node.rowIDAllocator = idAllocator

	node.factory.Init(&Params)
	log.Info("DataNode Init successfully",
		zap.String("MsgChannelSubName", Params.CommonCfg.DataNodeSubName))

	return nil
}

// StartWatchChannels start loop to watch channel allocation status via kv(etcd for now)
func (node *DataNode) StartWatchChannels(ctx context.Context) {
	defer logutil.LogPanic()
	// REF MEP#7 watch path should be [prefix]/channel/{node_id}/{channel_name}
	// TODO, this is risky, we'd better watch etcd with revision rather simply a path
	watchPrefix := path.Join(Params.DataNodeCfg.ChannelWatchSubPath, fmt.Sprintf("%d", Params.DataNodeCfg.GetNodeID()))
	evtChan := node.watchKv.WatchWithPrefix(watchPrefix)
	// after watch, first check all exists nodes first
	err := node.checkWatchedList()
	if err != nil {
		log.Warn("StartWatchChannels failed", zap.Error(err))
		return
	}
	for {
		select {
		case <-ctx.Done():
			log.Info("watch etcd loop quit")
			return
		case event, ok := <-evtChan:
			if !ok {
				log.Warn("datanode failed to watch channel, return")
				return
			}

			if err := event.Err(); err != nil {
				log.Warn("datanode watch channel canceled", zap.Error(event.Err()))
				// https://github.com/etcd-io/etcd/issues/8980
				if event.Err() == v3rpc.ErrCompacted {
					go node.StartWatchChannels(ctx)
					return
				}
				// if watch loop return due to event canceled, the datanode is not functional anymore
				log.Panic("datanode is not functional for event canceled", zap.Error(err))
				return
			}
			for _, evt := range event.Events {
				// We need to stay in order until events enqueued
				node.handleChannelEvt(evt)
			}
		}
	}
}

// checkWatchedList list all nodes under [prefix]/channel/{node_id} and make sure all nodeds are watched
// serves the corner case for etcd connection lost and missing some events
func (node *DataNode) checkWatchedList() error {
	// REF MEP#7 watch path should be [prefix]/channel/{node_id}/{channel_name}
	prefix := path.Join(Params.DataNodeCfg.ChannelWatchSubPath, fmt.Sprintf("%d", Params.DataNodeCfg.GetNodeID()))
	keys, values, err := node.watchKv.LoadWithPrefix(prefix)
	if err != nil {
		return err
	}
	for i, val := range values {
		node.handleWatchInfo(&event{eventType: putEventType}, keys[i], []byte(val))
	}
	return nil
}

// handleChannelEvt handles event from kv watch event
func (node *DataNode) handleChannelEvt(evt *clientv3.Event) {
	var e *event
	switch evt.Type {
	case clientv3.EventTypePut: // datacoord shall put channels needs to be watched here
		e = &event{
			eventType: putEventType,
			version:   evt.Kv.Version,
		}

	case clientv3.EventTypeDelete:
		e = &event{
			eventType: deleteEventType,
			version:   evt.Kv.Version,
		}
	}
	node.handleWatchInfo(e, string(evt.Kv.Key), evt.Kv.Value)
}

func (node *DataNode) handleWatchInfo(e *event, key string, data []byte) {
	switch e.eventType {
	case putEventType:
		watchInfo, err := parsePutEventData(data)
		if err != nil {
			log.Warn("fail to handle watchInfo", zap.Int("event type", e.eventType), zap.String("key", key), zap.Error(err))
			return
		}

		if isEndWatchState(watchInfo.State) {
			log.Debug("DataNode received a PUT event with an end State", zap.String("state", watchInfo.State.String()))
			return
		}

		e.info = watchInfo
		e.vChanName = watchInfo.GetVchan().GetChannelName()
		log.Info("DataNode is handling watchInfo PUT event", zap.String("key", key), zap.Any("watch state", watchInfo.GetState().String()))
	case deleteEventType:
		e.vChanName = parseDeleteEventKey(key)
		log.Info("DataNode is handling watchInfo DELETE event", zap.String("key", key))
	}

	actualManager, loaded := node.eventManagerMap.LoadOrStore(e.vChanName, newChannelEventManager(
		node.handlePutEvent, node.handleDeleteEvent, retryWatchInterval,
	))
	if !loaded {
		actualManager.(*channelEventManager).Run()
	}

	actualManager.(*channelEventManager).handleEvent(*e)

	// Whenever a delete event comes, this eventManager will be removed from map
	if e.eventType == deleteEventType {
		if m, loaded := node.eventManagerMap.LoadAndDelete(e.vChanName); loaded {
			m.(*channelEventManager).Close()
		}
	}
}

func parsePutEventData(data []byte) (*datapb.ChannelWatchInfo, error) {
	watchInfo := datapb.ChannelWatchInfo{}
	err := proto.Unmarshal(data, &watchInfo)
	if err != nil {
		return nil, fmt.Errorf("invalid event data: fail to parse ChannelWatchInfo, err: %v", err)
	}

	if watchInfo.Vchan == nil {
		return nil, fmt.Errorf("invalid event: ChannelWatchInfo with nil VChannelInfo")
	}
	reviseVChannelInfo(watchInfo.GetVchan())
	return &watchInfo, nil
}

func parseDeleteEventKey(key string) string {
	parts := strings.Split(key, "/")
	vChanName := parts[len(parts)-1]
	return vChanName
}

func (node *DataNode) handlePutEvent(watchInfo *datapb.ChannelWatchInfo, version int64) (err error) {
	vChanName := watchInfo.GetVchan().GetChannelName()
	switch watchInfo.State {
	case datapb.ChannelWatchState_Uncomplete, datapb.ChannelWatchState_ToWatch:
		if err := node.flowgraphManager.addAndStart(node, watchInfo.GetVchan()); err != nil {
			return fmt.Errorf("fail to add and start flowgraph for vChanName: %s, err: %v", vChanName, err)
		}
		log.Info("handle put event: new data sync service success", zap.String("vChanName", vChanName))
		watchInfo.State = datapb.ChannelWatchState_WatchSuccess

	case datapb.ChannelWatchState_ToRelease:
		// there is no reason why we release fail
		node.tryToReleaseFlowgraph(vChanName)
		watchInfo.State = datapb.ChannelWatchState_ReleaseSuccess
	}

	v, err := proto.Marshal(watchInfo)
	if err != nil {
		return fmt.Errorf("fail to marshal watchInfo with state, vChanName: %s, state: %s ,err: %w", vChanName, watchInfo.State.String(), err)
	}

	key := path.Join(Params.DataNodeCfg.ChannelWatchSubPath, fmt.Sprintf("%d", Params.DataNodeCfg.GetNodeID()), vChanName)

	success, err := node.watchKv.CompareVersionAndSwap(key, version, string(v))
	// etcd error, retrying
	if err != nil {
		// flow graph will leak if not release, causing new datanode failed to subscribe
		node.tryToReleaseFlowgraph(vChanName)
		log.Warn("fail to update watch state to etcd", zap.String("vChanName", vChanName),
			zap.String("state", watchInfo.State.String()), zap.Error(err))
		return err
	}
	// etcd valid but the states updated.
	if !success {
		log.Info("handle put event: failed to compare version and swap, release flowgraph",
			zap.String("key", key), zap.String("state", watchInfo.State.String()),
			zap.String("vChanName", vChanName))
		// flow graph will leak if not release, causing new datanode failed to subscribe
		node.tryToReleaseFlowgraph(vChanName)
		return nil
	}
	log.Info("handle put event success", zap.String("key", key),
		zap.String("state", watchInfo.State.String()), zap.String("vChanName", vChanName))
	return nil
}

func (node *DataNode) handleDeleteEvent(vChanName string) {
	node.tryToReleaseFlowgraph(vChanName)
}

// tryToReleaseFlowgraph tries to release a flowgraph
func (node *DataNode) tryToReleaseFlowgraph(vChanName string) {
	log.Info("try to release flowgraph", zap.String("vChanName", vChanName))
	node.flowgraphManager.release(vChanName)
}

// BackGroundGC runs in background to release datanode resources
// GOOSE TODO: remove background GC, using ToRelease for drop-collection after #15846
func (node *DataNode) BackGroundGC(vChannelCh <-chan string) {
	log.Info("DataNode Background GC Start")
	for {
		select {
		case vchanName := <-vChannelCh:
			node.tryToReleaseFlowgraph(vchanName)
		case <-node.ctx.Done():
			log.Warn("DataNode context done, exiting background GC")
			return
		}
	}
}

// Start will update DataNode state to HEALTHY
func (node *DataNode) Start() error {
	if err := node.rowIDAllocator.Start(); err != nil {
		log.Error("failed to start id allocator", zap.Error(err), zap.String("role", typeutil.DataNodeRole))
		return err
	}
	log.Debug("start id allocator done", zap.String("role", typeutil.DataNodeRole))

	rep, err := node.rootCoord.AllocTimestamp(node.ctx, &rootcoordpb.AllocTimestampRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_RequestTSO,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  Params.DataNodeCfg.GetNodeID(),
		},
		Count: 1,
	})
	if err != nil || rep.Status.ErrorCode != commonpb.ErrorCode_Success {
		log.Warn("fail to alloc timestamp", zap.Any("rep", rep), zap.Error(err))
		return errors.New("DataNode fail to alloc timestamp")
	}

	connectEtcdFn := func() error {
		etcdKV := etcdkv.NewEtcdKV(node.etcdCli, Params.EtcdCfg.MetaRootPath)
		node.watchKv = etcdKV
		return nil
	}
	err = retry.Do(node.ctx, connectEtcdFn, retry.Attempts(ConnectEtcdMaxRetryTime))
	if err != nil {
		return errors.New("DataNode fail to connect etcd")
	}

	chunkManager, err := node.factory.NewPersistentStorageChunkManager(node.ctx)

	if err != nil {
		return err
	}

	node.chunkManager = chunkManager

	go node.BackGroundGC(node.clearSignal)

	go node.compactionExecutor.start(node.ctx)

	// Start node watch node
	go node.StartWatchChannels(node.ctx)

	Params.DataNodeCfg.CreatedTime = time.Now()
	Params.DataNodeCfg.UpdatedTime = time.Now()

	node.UpdateStateCode(internalpb.StateCode_Healthy)
	return nil
}

// UpdateStateCode updates datanode's state code
func (node *DataNode) UpdateStateCode(code internalpb.StateCode) {
	node.State.Store(code)
}

// GetStateCode return datanode's state code
func (node *DataNode) GetStateCode() internalpb.StateCode {
	return node.State.Load().(internalpb.StateCode)
}

func (node *DataNode) isHealthy() bool {
	return node.GetStateCode() == internalpb.StateCode_Healthy
}

// WatchDmChannels is not in use
func (node *DataNode) WatchDmChannels(ctx context.Context, in *datapb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	log.Warn("DataNode WatchDmChannels is not in use")

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "watchDmChannels do nothing",
	}, nil
}

// GetComponentStates will return current state of DataNode
func (node *DataNode) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	log.Debug("DataNode current state", zap.Any("State", node.State.Load()))
	nodeID := common.NotRegisteredID
	if node.session != nil && node.session.Registered() {
		nodeID = node.session.ServerID
	}
	states := &internalpb.ComponentStates{
		State: &internalpb.ComponentInfo{
			// NodeID:    Params.NodeID, // will race with DataNode.Register()
			NodeID:    nodeID,
			Role:      node.Role,
			StateCode: node.State.Load().(internalpb.StateCode),
		},
		SubcomponentStates: make([]*internalpb.ComponentInfo, 0),
		Status:             &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
	}
	return states, nil
}

// ReadyToFlush tells whether DataNode is ready for flushing
func (node *DataNode) ReadyToFlush() error {
	if !node.isHealthy() {
		return errors.New("DataNode not in HEALTHY state")
	}
	return nil
}

// FlushSegments packs flush messages into flowGraph through flushChan.
//   If DataNode receives a valid segment to flush, new flush message for the segment should be ignored.
//   So if receiving calls to flush segment A, DataNode should guarantee the segment to be flushed.
//
//   One precondition: The segmentID in req is in ascending order.
func (node *DataNode) FlushSegments(ctx context.Context, req *datapb.FlushSegmentsRequest) (*commonpb.Status, error) {
	metrics.DataNodeFlushReqCounter.WithLabelValues(
		fmt.Sprint(Params.DataNodeCfg.GetNodeID()),
		MetricRequestsTotal).Inc()

	errStatus := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}

	if !node.isHealthy() {
		errStatus.Reason = "dataNode not in HEALTHY state"
		return errStatus, nil
	}

	log.Info("receiving FlushSegments request",
		zap.Int64("collection ID", req.GetCollectionID()),
		zap.Int64s("segments", req.GetSegmentIDs()),
		zap.Int64s("stale segments", req.GetMarkSegmentIDs()),
	)

	// TODO: Here and in other places, replace `flushed` param with a more meaningful name.
	processSegments := func(segmentIDs []UniqueID, flushed bool) ([]UniqueID, bool) {
		noErr := true
		var flushedSeg []UniqueID
		for _, segID := range segmentIDs {
			// if the segment in already being flushed, skip it.
			if node.segmentCache.checkIfCached(segID) {
				logDupFlush(req.GetCollectionID(), segID)
				continue
			}
			// Get the flush channel for the given segment ID.
			// If no flush channel is found, report an error.
			flushCh, err := node.flowgraphManager.getFlushCh(segID)
			if err != nil {
				errStatus.Reason = "no flush channel found for the segment, unable to flush"
				log.Error(errStatus.Reason, zap.Int64("segment ID", segID), zap.Error(err))
				noErr = false
				continue
			}

			// Double check that the segment is still not cached.
			// Skip this flush if segment ID is cached, otherwise cache the segment ID and proceed.
			exist := node.segmentCache.checkOrCache(segID)
			if exist {
				logDupFlush(req.GetCollectionID(), segID)
				continue
			}
			// flushedSeg is only for logging purpose.
			flushedSeg = append(flushedSeg, segID)
			// Send the segment to its flush channel.
			flushCh <- flushMsg{
				msgID:        req.GetBase().GetMsgID(),
				timestamp:    req.GetBase().GetTimestamp(),
				segmentID:    segID,
				collectionID: req.GetCollectionID(),
				flushed:      flushed,
			}
		}
		log.Info("flow graph flushSegment tasks triggered",
			zap.Bool("flushed", flushed),
			zap.Int64("collection ID", req.GetCollectionID()),
			zap.Int64s("segments sending to flush channel", flushedSeg))
		return flushedSeg, noErr
	}

	seg, noErr1 := processSegments(req.GetSegmentIDs(), true)
	staleSeg, noErr2 := processSegments(req.GetMarkSegmentIDs(), false)
	// Log success flushed segments.
	if len(seg)+len(staleSeg) > 0 {
		log.Info("sending segments to flush channel",
			zap.Any("newly sealed segment IDs", seg),
			zap.Any("stale segment IDs", staleSeg))
	}
	// Fail FlushSegments call if at least one segment (no matter stale or not) fails to get flushed.
	if !noErr1 || !noErr2 {
		return errStatus, nil
	}

	metrics.DataNodeFlushReqCounter.WithLabelValues(
		fmt.Sprint(Params.DataNodeCfg.GetNodeID()),
		MetricRequestsSuccess).Inc()
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

// ResendSegmentStats resend un-flushed segment stats back upstream to DataCoord by resending DataNode time tick message.
// It returns a list of segments to be sent.
func (node *DataNode) ResendSegmentStats(ctx context.Context, req *datapb.ResendSegmentStatsRequest) (*datapb.ResendSegmentStatsResponse, error) {
	log.Info("start resending segment stats, if any",
		zap.Int64("DataNode ID", Params.DataNodeCfg.GetNodeID()))
	segResent := node.flowgraphManager.resendTT()
	log.Info("found segment(s) with stats to resend",
		zap.Int64s("segment IDs", segResent))
	return &datapb.ResendSegmentStatsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		SegResent: segResent,
	}, nil
}

// Stop will release DataNode resources and shutdown datanode
func (node *DataNode) Stop() error {
	// https://github.com/milvus-io/milvus/issues/12282
	node.UpdateStateCode(internalpb.StateCode_Abnormal)

	node.cancel()
	node.flowgraphManager.dropAll()

	if node.rowIDAllocator != nil {
		log.Info("close id allocator", zap.String("role", typeutil.DataNodeRole))
		node.rowIDAllocator.Close()
	}

	if node.closer != nil {
		err := node.closer.Close()
		if err != nil {
			return err
		}
	}

	node.session.Revoke(time.Second)

	return nil
}

// GetTimeTickChannel currently do nothing
func (node *DataNode) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}, nil
}

// GetStatisticsChannel currently do nothing
func (node *DataNode) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}, nil
}

//ShowConfigurations returns the configurations of DataNode matching req.Pattern
func (node *DataNode) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	log.Debug("DataNode.ShowConfigurations", zap.String("pattern", req.Pattern))
	if !node.isHealthy() {
		log.Warn("DataNode.ShowConfigurations failed",
			zap.Int64("nodeId", Params.QueryNodeCfg.GetNodeID()),
			zap.String("req", req.Pattern),
			zap.Error(errDataNodeIsUnhealthy(Params.QueryNodeCfg.GetNodeID())))

		return &internalpb.ShowConfigurationsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msgDataNodeIsUnhealthy(Params.QueryNodeCfg.GetNodeID()),
			},
			Configuations: nil,
		}, nil
	}

	return getComponentConfigurations(ctx, req), nil
}

// GetMetrics return datanode metrics
// TODO(dragondriver): cache the Metrics and set a retention to the cache
func (node *DataNode) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	log.Debug("DataNode.GetMetrics",
		zap.Int64("node_id", Params.DataNodeCfg.GetNodeID()),
		zap.String("req", req.Request))

	if !node.isHealthy() {
		log.Warn("DataNode.GetMetrics failed",
			zap.Int64("node_id", Params.DataNodeCfg.GetNodeID()),
			zap.String("req", req.Request),
			zap.Error(errDataNodeIsUnhealthy(Params.DataNodeCfg.GetNodeID())))

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msgDataNodeIsUnhealthy(Params.DataNodeCfg.GetNodeID()),
			},
		}, nil
	}

	metricType, err := metricsinfo.ParseMetricType(req.Request)
	if err != nil {
		log.Warn("DataNode.GetMetrics failed to parse metric type",
			zap.Int64("node_id", Params.DataNodeCfg.GetNodeID()),
			zap.String("req", req.Request),
			zap.Error(err))

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug("DataNode.GetMetrics",
		zap.String("metric_type", metricType))

	if metricType == metricsinfo.SystemInfoMetrics {
		systemInfoMetrics, err := node.getSystemInfoMetrics(ctx, req)

		log.Debug("DataNode.GetMetrics",
			zap.Int64("node_id", Params.DataNodeCfg.GetNodeID()),
			zap.String("req", req.Request),
			zap.String("metric_type", metricType),
			zap.Any("systemInfoMetrics", systemInfoMetrics), // TODO(dragondriver): necessary? may be very large
			zap.Error(err))

		return systemInfoMetrics, nil
	}

	log.Debug("DataNode.GetMetrics failed, request metric type is not implemented yet",
		zap.Int64("node_id", Params.DataNodeCfg.GetNodeID()),
		zap.String("req", req.Request),
		zap.String("metric_type", metricType))

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    metricsinfo.MsgUnimplementedMetric,
		},
	}, nil
}

// Compaction handles compaction request from DataCoord
// returns status as long as compaction task enqueued or invalid
func (node *DataNode) Compaction(ctx context.Context, req *datapb.CompactionPlan) (*commonpb.Status, error) {
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}

	ds, ok := node.flowgraphManager.getFlowgraphService(req.GetChannel())
	if !ok {
		log.Warn("illegel compaction plan, channel not in this DataNode", zap.String("channel name", req.GetChannel()))
		status.Reason = errIllegalCompactionPlan.Error()
		return status, nil
	}

	if !node.compactionExecutor.channelValidateForCompaction(req.GetChannel()) {
		log.Warn("channel of compaction is marked invalid in compaction executor", zap.String("channel name", req.GetChannel()))
		status.Reason = "channel marked invalid"
		return status, nil
	}

	binlogIO := &binlogIO{node.chunkManager, ds.idAllocator}
	task := newCompactionTask(
		node.ctx,
		binlogIO, binlogIO,
		ds.replica,
		ds.flushManager,
		ds.idAllocator,
		req,
	)

	node.compactionExecutor.execute(task)

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

// GetCompactionState called by DataCoord
// return status of all compaction plans
func (node *DataNode) GetCompactionState(ctx context.Context, req *datapb.CompactionStateRequest) (*datapb.CompactionStateResponse, error) {
	log.Info("DataNode.GetCompactionState")
	if !node.isHealthy() {
		return &datapb.CompactionStateResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "DataNode is unhealthy",
			},
		}, nil
	}
	results := make([]*datapb.CompactionStateResult, 0)
	node.compactionExecutor.executing.Range(func(k, v any) bool {
		results = append(results, &datapb.CompactionStateResult{
			State:  commonpb.CompactionState_Executing,
			PlanID: k.(UniqueID),
		})
		return true
	})
	node.compactionExecutor.completed.Range(func(k, v any) bool {
		results = append(results, &datapb.CompactionStateResult{
			State:  commonpb.CompactionState_Completed,
			PlanID: k.(UniqueID),
			Result: v.(*datapb.CompactionResult),
		})
		node.compactionExecutor.completed.Delete(k)
		return true
	})
	log.Debug("Compaction results", zap.Any("results", results))
	return &datapb.CompactionStateResponse{
		Status:  &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		Results: results,
	}, nil
}

// SyncSegments called by DataCoord, sync the compacted segments' meta between DC and DN
func (node *DataNode) SyncSegments(ctx context.Context, req *datapb.SyncSegmentsRequest) (*commonpb.Status, error) {
	log.Ctx(ctx).Info("DataNode receives SyncSegments",
		zap.Int64("planID", req.GetPlanID()),
		zap.Int64("target segmentID", req.GetCompactedTo()),
		zap.Int64s("compacted from", req.GetCompactedFrom()),
		zap.Int64("numOfRows", req.GetNumOfRows()),
	)
	status := &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}

	if !node.isHealthy() {
		status.Reason = "DataNode is unhealthy"
		return status, nil
	}

	if len(req.GetCompactedFrom()) <= 0 {
		status.Reason = "invalid request, compacted from segments shouldn't be empty"
		return status, nil
	}

	oneSegment := req.GetCompactedFrom()[0]
	replica, err := node.flowgraphManager.getReplica(oneSegment)
	if err != nil {
		status.Reason = fmt.Sprintf("invalid request, err=%s", err.Error())
		return status, nil
	}

	// check if all compactedFrom segments are valid
	var invalidSegIDs []UniqueID
	for _, segID := range req.GetCompactedFrom() {
		if !replica.hasSegment(segID, true) {
			invalidSegIDs = append(invalidSegIDs, segID)
		}
	}
	if len(invalidSegIDs) > 0 {
		status.Reason = fmt.Sprintf("invalid request, some segments are not in the same replica: %v", invalidSegIDs)
		return status, nil
	}

	// oneSegment is definitely in the replica, guaranteed by the check before.
	collID, partID, _ := replica.getCollectionAndPartitionID(oneSegment)
	chanName, _ := replica.getChannelName(oneSegment)
	targetSeg := &Segment{
		collectionID: collID,
		partitionID:  partID,
		channelName:  chanName,
		segmentID:    req.GetCompactedTo(),
		numRows:      req.GetNumOfRows(),
	}

	replica.(*SegmentReplica).initPKBloomFilter(ctx, targetSeg, req.GetStatsLogs(), tsoutil.GetCurrentTime())

	if err := replica.mergeFlushedSegments(targetSeg, req.GetPlanID(), req.GetCompactedFrom()); err != nil {
		status.Reason = err.Error()
		return status, nil
	}

	status.ErrorCode = commonpb.ErrorCode_Success
	return status, nil
}

// Import data files(json, numpy, etc.) on MinIO/S3 storage, read and parse them into sealed segments
func (node *DataNode) Import(ctx context.Context, req *datapb.ImportTaskRequest) (*commonpb.Status, error) {
	log.Info("DataNode receive import request",
		zap.Int64("task ID", req.GetImportTask().GetTaskId()),
		zap.Int64("collection ID", req.GetImportTask().GetCollectionId()),
		zap.Int64("partition ID", req.GetImportTask().GetPartitionId()),
		zap.Strings("channel names", req.GetImportTask().GetChannelNames()),
		zap.Int64s("working dataNodes", req.WorkingNodes))
	defer func() {
		log.Info("DataNode finish import request", zap.Int64("task ID", req.GetImportTask().GetTaskId()))
	}()

	importResult := &rootcoordpb.ImportResult{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		TaskId:     req.GetImportTask().TaskId,
		DatanodeId: Params.DataNodeCfg.GetNodeID(),
		State:      commonpb.ImportState_ImportStarted,
		Segments:   make([]int64, 0),
		AutoIds:    make([]int64, 0),
		RowCount:   0,
	}
	// func to report import state to rootcoord
	reportFunc := func(res *rootcoordpb.ImportResult) error {
		status, err := node.rootCoord.ReportImport(ctx, res)
		if err != nil {
			log.Error("fail to report import state to root coord", zap.Error(err))
			return err
		}
		if status != nil && status.ErrorCode != commonpb.ErrorCode_Success {
			return errors.New(status.GetReason())
		}
		return nil
	}

	if !node.isHealthy() {
		log.Warn("DataNode import failed",
			zap.Int64("collection ID", req.GetImportTask().GetCollectionId()),
			zap.Int64("partition ID", req.GetImportTask().GetPartitionId()),
			zap.Int64("taskID", req.GetImportTask().GetTaskId()),
			zap.Error(errDataNodeIsUnhealthy(Params.DataNodeCfg.GetNodeID())))

		msg := msgDataNodeIsUnhealthy(Params.DataNodeCfg.GetNodeID())
		importResult.State = commonpb.ImportState_ImportFailed
		importResult.Infos = append(importResult.Infos, &commonpb.KeyValuePair{Key: "failed_reason", Value: msg})
		reportErr := reportFunc(importResult)
		if reportErr != nil {
			log.Warn("fail to report import state to root coord", zap.Error(reportErr))
		}
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    msg,
		}, nil
	}

	// get a timestamp for all the rows
	rep, err := node.rootCoord.AllocTimestamp(ctx, &rootcoordpb.AllocTimestampRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_RequestTSO,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  Params.DataNodeCfg.GetNodeID(),
		},
		Count: 1,
	})

	if rep.Status.ErrorCode != commonpb.ErrorCode_Success || err != nil {
		msg := "DataNode alloc ts failed"
		log.Warn(msg)
		importResult.State = commonpb.ImportState_ImportFailed
		importResult.Infos = append(importResult.Infos, &commonpb.KeyValuePair{Key: "failed_reason", Value: msg})
		if reportErr := reportFunc(importResult); reportErr != nil {
			log.Warn("fail to report import state to root coord", zap.Error(reportErr))
		}
		if err != nil {
			return &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msg,
			}, nil
		}
	}

	ts := rep.GetTimestamp()

	// get collection schema and shard number
	metaService := newMetaService(node.rootCoord, req.GetImportTask().GetCollectionId())
	colInfo, err := metaService.getCollectionInfo(ctx, req.GetImportTask().GetCollectionId(), 0)
	if err != nil {
		importResult.State = commonpb.ImportState_ImportFailed
		importResult.Infos = append(importResult.Infos, &commonpb.KeyValuePair{Key: "failed_reason", Value: err.Error()})
		reportErr := reportFunc(importResult)
		if reportErr != nil {
			log.Warn("fail to report import state to root coord", zap.Error(err))
		}
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	// parse files and generate segments
	segmentSize := int64(Params.DataCoordCfg.SegmentMaxSize) * 1024 * 1024
	importWrapper := importutil.NewImportWrapper(ctx, colInfo.GetSchema(), colInfo.GetShardsNum(), segmentSize, node.rowIDAllocator, node.chunkManager,
		importFlushReqFunc(node, req, importResult, colInfo.GetSchema(), ts), importResult, reportFunc)
	err = importWrapper.Import(req.GetImportTask().GetFiles(), req.GetImportTask().GetRowBased(), false)
	if err != nil {
		importResult.State = commonpb.ImportState_ImportFailed
		importResult.Infos = append(importResult.Infos, &commonpb.KeyValuePair{Key: "failed_reason", Value: err.Error()})
		reportErr := reportFunc(importResult)
		if reportErr != nil {
			log.Warn("fail to report import state to root coord", zap.Error(err))
		}
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	resp := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	return resp, nil
}

// AddImportSegment adds the import segment to the current DataNode.
func (node *DataNode) AddImportSegment(ctx context.Context, req *datapb.AddImportSegmentRequest) (*datapb.AddImportSegmentResponse, error) {
	log.Info("adding segment to DataNode flow graph",
		zap.Int64("segment ID", req.GetSegmentId()),
		zap.Int64("collection ID", req.GetCollectionId()),
		zap.Int64("partition ID", req.GetPartitionId()),
		zap.String("channel name", req.GetChannelName()),
		zap.Int64("# of rows", req.GetRowNum()))
	// Fetch the flow graph on the given v-channel.
	var ds *dataSyncService
	// Retry in case the channel hasn't been watched yet.
	err := retry.Do(ctx, func() error {
		var ok bool
		ds, ok = node.flowgraphManager.getFlowgraphService(req.GetChannelName())
		if !ok {
			return errors.New("channel not found")
		}
		return nil
	}, retry.Attempts(getFlowGraphServiceAttempts))
	if err != nil {
		log.Error("channel not found in current DataNode",
			zap.String("channel name", req.GetChannelName()),
			zap.Int64("node ID", Params.DataNodeCfg.GetNodeID()))
		return &datapb.AddImportSegmentResponse{
			Status: &commonpb.Status{
				// TODO: Add specific error code.
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "channel not found in current DataNode",
			},
		}, nil
	}
	// Get the current dml channel position ID, that will be used in segments start positions and end positions.
	posID, err := ds.getChannelLatestMsgID(context.Background(), req.GetChannelName())
	if err != nil {
		return &datapb.AddImportSegmentResponse{
			Status: &commonpb.Status{
				// TODO: Add specific error code.
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "failed to get channel position",
			},
		}, nil
	}
	// Add the new segment to the replica.
	if !ds.replica.hasSegment(req.GetSegmentId(), true) {
		log.Info("adding a new segment to replica",
			zap.Int64("segment ID", req.GetSegmentId()))
		// Add segment as a flushed segment, but set `importing` to true to add extra information of the segment.
		// By 'extra information' we mean segment info while adding a `SegmentType_Flushed` typed segment.
		if err := ds.replica.addSegment(
			addSegmentReq{
				segType:      datapb.SegmentType_Flushed,
				segID:        req.GetSegmentId(),
				collID:       req.GetCollectionId(),
				partitionID:  req.GetPartitionId(),
				channelName:  req.GetChannelName(),
				numOfRows:    req.GetRowNum(),
				statsBinLogs: req.GetStatsLog(),
				startPos: &internalpb.MsgPosition{
					ChannelName: req.GetChannelName(),
					MsgID:       posID,
					Timestamp:   req.GetBase().GetTimestamp(),
				},
				endPos: &internalpb.MsgPosition{
					ChannelName: req.GetChannelName(),
					MsgID:       posID,
					Timestamp:   req.GetBase().GetTimestamp(),
				},
				recoverTs: req.GetBase().GetTimestamp(),
				importing: true,
			}); err != nil {
			log.Error("failed to add segment to flow graph",
				zap.Error(err))
			return &datapb.AddImportSegmentResponse{
				Status: &commonpb.Status{
					// TODO: Add specific error code.
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}
	}
	ds.flushingSegCache.Remove(req.GetSegmentId())
	return &datapb.AddImportSegmentResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		ChannelPos: posID,
	}, nil
}

func importFlushReqFunc(node *DataNode, req *datapb.ImportTaskRequest, res *rootcoordpb.ImportResult, schema *schemapb.CollectionSchema, ts Timestamp) importutil.ImportFlushFunc {
	return func(fields map[storage.FieldID]storage.FieldData, shardID int) error {
		chNames := req.GetImportTask().GetChannelNames()
		importTaskID := req.GetImportTask().GetTaskId()
		if shardID >= len(chNames) {
			log.Error("import task returns invalid shard ID",
				zap.Int64("task ID", importTaskID),
				zap.Int("shard ID", shardID),
				zap.Int("# of channels", len(chNames)),
				zap.Strings("channel names", chNames),
			)
			return fmt.Errorf("syncSegmentID Failed: invalid shard ID %d", shardID)
		}

		tr := timerecord.NewTimeRecorder("import callback function")
		defer tr.Elapse("finished")

		var rowNum int
		for _, field := range fields {
			rowNum = field.RowNum()
			break
		}
		colID := req.GetImportTask().GetCollectionId()
		partID := req.GetImportTask().GetPartitionId()
		segmentIDReq := composeAssignSegmentIDRequest(rowNum, shardID, chNames, colID, partID)
		targetChName := segmentIDReq.GetSegmentIDRequests()[0].GetChannelName()
		log.Info("target channel for the import task",
			zap.Int64("task ID", importTaskID),
			zap.String("target channel name", targetChName))
		resp, err := node.dataCoord.AssignSegmentID(context.Background(), segmentIDReq)
		if err != nil {
			return fmt.Errorf("syncSegmentID Failed:%w", err)
		}
		if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return fmt.Errorf("syncSegmentID Failed:%s", resp.Status.Reason)
		}
		segmentID := resp.SegIDAssignments[0].SegID

		fieldInsert, fieldStats, err := createBinLogs(rowNum, schema, ts, fields, node, segmentID, colID, partID)
		if err != nil {
			return err
		}

		log.Info("adding segment to the correct DataNode flow graph and saving binlog paths",
			zap.Int64("segment ID", segmentID),
			zap.Uint64("ts", ts))
		err = retry.Do(context.Background(), func() error {
			// Ask DataCoord to save binlog path and add segment to the corresponding DataNode flow graph.
			resp, err := node.dataCoord.SaveImportSegment(context.Background(), &datapb.SaveImportSegmentRequest{
				Base: &commonpb.MsgBase{
					SourceID: Params.DataNodeCfg.GetNodeID(),
					// Pass current timestamp downstream.
					Timestamp: ts,
				},
				SegmentId:    segmentID,
				ChannelName:  targetChName,
				CollectionId: req.GetImportTask().GetCollectionId(),
				PartitionId:  req.GetImportTask().GetPartitionId(),
				RowNum:       int64(rowNum),
				SaveBinlogPathReq: &datapb.SaveBinlogPathsRequest{
					Base: &commonpb.MsgBase{
						MsgType:   0,
						MsgID:     0,
						Timestamp: ts,
						SourceID:  Params.DataNodeCfg.GetNodeID(),
					},
					SegmentID:           segmentID,
					CollectionID:        req.GetImportTask().GetCollectionId(),
					Field2BinlogPaths:   fieldInsert,
					Field2StatslogPaths: fieldStats,
					// Set start positions of a SaveBinlogPathRequest explicitly.
					StartPositions: []*datapb.SegmentStartPosition{
						{
							StartPosition: &internalpb.MsgPosition{
								ChannelName: targetChName,
								Timestamp:   ts,
							},
							SegmentID: segmentID,
						},
					},
					Importing: true,
				},
			})
			// Only retrying when DataCoord is unhealthy or err != nil, otherwise return immediately.
			if err != nil {
				return fmt.Errorf(err.Error())
			}
			if resp.ErrorCode != commonpb.ErrorCode_Success && resp.ErrorCode != commonpb.ErrorCode_DataCoordNA {
				return retry.Unrecoverable(fmt.Errorf("failed to save import segment, reason = %s", resp.Reason))
			} else if resp.ErrorCode == commonpb.ErrorCode_DataCoordNA {
				return fmt.Errorf("failed to save import segment: %s", resp.GetReason())
			}
			return nil
		})
		if err != nil {
			log.Warn("failed to save import segment", zap.Error(err))
			return err
		}
		log.Info("segment imported and persisted", zap.Int64("segmentID", segmentID))
		res.Segments = append(res.Segments, segmentID)
		res.RowCount += int64(rowNum)
		return nil
	}
}

func composeAssignSegmentIDRequest(rowNum int, shardID int, chNames []string,
	collID int64, partID int64) *datapb.AssignSegmentIDRequest {
	// use the first field's row count as segment row count
	// all the fields row count are same, checked by ImportWrapper
	// ask DataCoord to alloc a new segment
	log.Info("import task flush segment",
		zap.Any("channel names", chNames),
		zap.Int("shard ID", shardID))
	segReqs := []*datapb.SegmentIDRequest{
		{
			ChannelName:  chNames[shardID],
			Count:        uint32(rowNum),
			CollectionID: collID,
			PartitionID:  partID,
			IsImport:     true,
		},
	}
	segmentIDReq := &datapb.AssignSegmentIDRequest{
		NodeID:            0,
		PeerRole:          typeutil.ProxyRole,
		SegmentIDRequests: segReqs,
	}
	return segmentIDReq
}

func createBinLogs(rowNum int, schema *schemapb.CollectionSchema, ts Timestamp,
	fields map[storage.FieldID]storage.FieldData, node *DataNode, segmentID, colID, partID UniqueID) ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, error) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tsFieldData := make([]int64, rowNum)
	for i := range tsFieldData {
		tsFieldData[i] = int64(ts)
	}
	fields[common.TimeStampField] = &storage.Int64FieldData{
		Data:    tsFieldData,
		NumRows: []int64{int64(rowNum)},
	}

	if status, _ := node.dataCoord.UpdateSegmentStatistics(context.TODO(), &datapb.UpdateSegmentStatisticsRequest{
		Stats: []*datapb.SegmentStats{{
			SegmentID: segmentID,
			NumRows:   int64(rowNum),
		}},
	}); status.GetErrorCode() != commonpb.ErrorCode_Success {
		return nil, nil, fmt.Errorf(status.GetReason())
	}

	data := BufferData{buffer: &InsertData{
		Data: fields,
	}}
	data.updateSize(int64(rowNum))
	meta := &etcdpb.CollectionMeta{
		ID:     colID,
		Schema: schema,
	}
	binLogs, statsBinLogs, err := storage.NewInsertCodec(meta).Serialize(partID, segmentID, data.buffer)
	if err != nil {
		return nil, nil, err
	}

	var alloc allocatorInterface = newAllocator(node.rootCoord)
	start, _, err := alloc.allocIDBatch(uint32(len(binLogs)))
	if err != nil {
		return nil, nil, err
	}

	field2Insert := make(map[UniqueID]*datapb.Binlog, len(binLogs))
	kvs := make(map[string][]byte, len(binLogs))
	field2Logidx := make(map[UniqueID]UniqueID, len(binLogs))
	for idx, blob := range binLogs {
		fieldID, err := strconv.ParseInt(blob.GetKey(), 10, 64)
		if err != nil {
			log.Error("Flush failed ... cannot parse string to fieldID ..", zap.Error(err))
			return nil, nil, err
		}

		logidx := start + int64(idx)

		// no error raise if alloc=false
		k := metautil.JoinIDPath(colID, partID, segmentID, fieldID, logidx)

		key := path.Join(node.chunkManager.RootPath(), common.SegmentInsertLogPath, k)
		kvs[key] = blob.Value[:]
		field2Insert[fieldID] = &datapb.Binlog{
			EntriesNum:    data.size,
			TimestampFrom: ts,
			TimestampTo:   ts,
			LogPath:       key,
			LogSize:       int64(len(blob.Value)),
		}
		field2Logidx[fieldID] = logidx
	}

	field2Stats := make(map[UniqueID]*datapb.Binlog)
	// write stats binlog
	for _, blob := range statsBinLogs {
		fieldID, err := strconv.ParseInt(blob.GetKey(), 10, 64)
		if err != nil {
			log.Error("Flush failed ... cannot parse string to fieldID ..", zap.Error(err))
			return nil, nil, err
		}

		logidx := field2Logidx[fieldID]

		// no error raise if alloc=false
		k := metautil.JoinIDPath(colID, partID, segmentID, fieldID, logidx)

		key := path.Join(node.chunkManager.RootPath(), common.SegmentStatslogPath, k)
		kvs[key] = blob.Value
		field2Stats[fieldID] = &datapb.Binlog{
			EntriesNum:    data.size,
			TimestampFrom: ts,
			TimestampTo:   ts,
			LogPath:       key,
			LogSize:       int64(len(blob.Value)),
		}
	}

	err = node.chunkManager.MultiWrite(ctx, kvs)
	if err != nil {
		return nil, nil, err
	}
	var (
		fieldInsert []*datapb.FieldBinlog
		fieldStats  []*datapb.FieldBinlog
	)
	for k, v := range field2Insert {
		fieldInsert = append(fieldInsert, &datapb.FieldBinlog{FieldID: k, Binlogs: []*datapb.Binlog{v}})
	}
	for k, v := range field2Stats {
		fieldStats = append(fieldStats, &datapb.FieldBinlog{FieldID: k, Binlogs: []*datapb.Binlog{v}})
	}
	return fieldInsert, fieldStats, nil
}

func logDupFlush(cID, segID int64) {
	log.Info("segment is already being flushed, ignoring flush request",
		zap.Int64("collection ID", cID),
		zap.Int64("segment ID", segID))
}

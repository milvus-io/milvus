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
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/samber/lo"
	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

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
	"github.com/milvus-io/milvus/internal/util/commonpbutil"
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
)

const (
	// ConnectEtcdMaxRetryTime is used to limit the max retry time for connection etcd
	ConnectEtcdMaxRetryTime = 100
)

var getFlowGraphServiceAttempts = uint(50)

// makes sure DataNode implements types.DataNode
var _ types.DataNode = (*DataNode)(nil)

// Params from config.yaml
var Params paramtable.ComponentParam

// DataNode communicates with outside services and unioun all
// services in datanode package.
//
// DataNode implements `types.Component`, `types.DataNode` interfaces.
//
//	`etcdCli`   is a connection of etcd
//	`rootCoord` is a grpc client of root coordinator.
//	`dataCoord` is a grpc client of data service.
//	`NodeID` is unique to each datanode.
//	`State` is current statement of this data node, indicating whether it's healthy.
//
//	`clearSignal` is a signal channel for releasing the flowgraph resources.
//	`segmentCache` stores all flushing and flushed segments.
type DataNode struct {
	ctx              context.Context
	cancel           context.CancelFunc
	Role             string
	State            atomic.Value // commonpb.StateCode_Initializing
	stateCode        atomic.Value // commonpb.StateCode_Initializing
	flowgraphManager *flowgraphManager
	eventManagerMap  sync.Map // vchannel name -> channelEventManager

	clearSignal        chan string // vchannel name
	segmentCache       *Cache
	compactionExecutor *compactionExecutor
	timeTickSender     *timeTickSender

	etcdCli   *clientv3.Client
	rootCoord types.RootCoord
	dataCoord types.DataCoord

	session        *sessionutil.Session
	watchKv        kv.MetaKv
	chunkManager   storage.ChunkManager
	rowIDAllocator *allocator2.IDAllocator

	closer io.Closer

	factory dependency.Factory

	reportImportRetryTimes uint // unitest set this value to 1 to save time, default is 10
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

		reportImportRetryTimes: 10,
	}
	node.UpdateStateCode(commonpb.StateCode_Abnormal)
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
		return errors.New("nil parameter or repeatedly set")
	default:
		node.rootCoord = rc
		return nil
	}
}

// SetDataCoord sets data service's grpc client, error is returned if repeatedly set.
func (node *DataNode) SetDataCoord(ds types.DataCoord) error {
	switch {
	case ds == nil, node.dataCoord != nil:
		return errors.New("nil parameter or repeatedly set")
	default:
		node.dataCoord = ds
		return nil
	}
}

// Register register datanode to etcd
func (node *DataNode) Register() error {
	node.session.Register()
	metrics.NumNodes.WithLabelValues(strconv.FormatInt(node.session.ServerID, 10), typeutil.DataNodeRole).Inc()
	log.Info("DataNode Register Finished")
	// Start liveness check
	node.session.LivenessCheck(node.ctx, func() {
		log.Error("Data Node disconnected from etcd, process will exit", zap.Int64("Server Id", node.session.ServerID))
		if err := node.Stop(); err != nil {
			log.Fatal("failed to stop server", zap.Error(err))
		}
		metrics.NumNodes.WithLabelValues(strconv.FormatInt(node.session.ServerID, 10), typeutil.DataNodeRole).Dec()
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
	sessionutil.SaveServerInfo(typeutil.DataNodeRole, node.session.ServerID)
	Params.SetLogger(Params.DataNodeCfg.GetNodeID())
	return nil
}

// initRateCollector creates and starts rateCollector in QueryNode.
func (node *DataNode) initRateCollector() error {
	err := initGlobalRateCollector()
	if err != nil {
		return err
	}
	rateCol.Register(metricsinfo.InsertConsumeThroughput)
	rateCol.Register(metricsinfo.DeleteConsumeThroughput)
	return nil
}

// Init function does nothing now.
func (node *DataNode) Init() error {
	log.Info("DataNode server initializing",
		zap.String("TimeTickChannelName", Params.CommonCfg.DataCoordTimeTick),
	)
	if err := node.initSession(); err != nil {
		log.Error("DataNode server init session failed", zap.Error(err))
		return err
	}

	err := node.initRateCollector()
	if err != nil {
		log.Error("DataNode server init rateCollector failed", zap.Int64("node ID", Params.QueryNodeCfg.GetNodeID()), zap.Error(err))
		return err
	}
	log.Info("DataNode server init rateCollector done", zap.Int64("node ID", Params.QueryNodeCfg.GetNodeID()))

	idAllocator, err := allocator2.NewIDAllocator(node.ctx, node.rootCoord, Params.DataNodeCfg.GetNodeID())
	if err != nil {
		log.Error("failed to create id allocator",
			zap.Error(err),
			zap.String("role", typeutil.DataNodeRole), zap.Int64("DataNode ID", Params.DataNodeCfg.GetNodeID()))
		return err
	}
	node.rowIDAllocator = idAllocator

	node.factory.Init(&Params)
	log.Info("DataNode server init succeeded",
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
				go node.StartWatchChannels(ctx)
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
			log.Info("DataNode received a PUT event with an end State", zap.String("state", watchInfo.State.String()))
			return
		}

		if watchInfo.Progress != 0 {
			log.Info("DataNode received a PUT event with tickler update progress", zap.String("channel", watchInfo.Vchan.ChannelName), zap.Int64("version", e.version))
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
	key := path.Join(Params.DataNodeCfg.ChannelWatchSubPath, fmt.Sprintf("%d", Params.DataNodeCfg.GetNodeID()), vChanName)
	tickler := newTickler(version, key, watchInfo, node.watchKv, Params.DataNodeCfg.WatchEventTicklerInterval)

	switch watchInfo.State {
	case datapb.ChannelWatchState_Uncomplete, datapb.ChannelWatchState_ToWatch:
		if err := node.flowgraphManager.addAndStart(node, watchInfo.GetVchan(), watchInfo.GetSchema(), tickler); err != nil {
			log.Warn("handle put event: new data sync service failed", zap.String("vChanName", vChanName), zap.Error(err))
			watchInfo.State = datapb.ChannelWatchState_WatchFailure
		} else {
			log.Info("handle put event: new data sync service success", zap.String("vChanName", vChanName))
			watchInfo.State = datapb.ChannelWatchState_WatchSuccess
		}
	case datapb.ChannelWatchState_ToRelease:
		// there is no reason why we release fail
		node.tryToReleaseFlowgraph(vChanName)
		watchInfo.State = datapb.ChannelWatchState_ReleaseSuccess
	}

	v, err := proto.Marshal(watchInfo)
	if err != nil {
		return fmt.Errorf("fail to marshal watchInfo with state, vChanName: %s, state: %s ,err: %w", vChanName, watchInfo.State.String(), err)
	}

	success, err := node.watchKv.CompareVersionAndSwap(key, tickler.version, string(v))
	// etcd error
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
	log.Info("release flowgraph success", zap.String("vChanName", vChanName))
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
	log.Info("start id allocator done", zap.String("role", typeutil.DataNodeRole))

	rep, err := node.rootCoord.AllocTimestamp(node.ctx, &rootcoordpb.AllocTimestampRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_RequestTSO),
			commonpbutil.WithMsgID(0),
			commonpbutil.WithTimeStamp(0),
			commonpbutil.WithSourceID(Params.DataNodeCfg.GetNodeID()),
		),
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

	if Params.DataNodeCfg.DataNodeTimeTickByRPC {
		node.timeTickSender = newTimeTickSender(node.dataCoord, node.session.ServerID)
		go node.timeTickSender.start(node.ctx)
	}

	// Start node watch node
	go node.StartWatchChannels(node.ctx)

	go node.flowgraphManager.start()

	Params.DataNodeCfg.CreatedTime = time.Now()
	Params.DataNodeCfg.UpdatedTime = time.Now()

	node.UpdateStateCode(commonpb.StateCode_Healthy)
	return nil
}

// UpdateStateCode updates datanode's state code
func (node *DataNode) UpdateStateCode(code commonpb.StateCode) {
	node.stateCode.Store(code)
}

// GetStateCode return datanode's state code
func (node *DataNode) GetStateCode() commonpb.StateCode {
	return node.stateCode.Load().(commonpb.StateCode)
}

func (node *DataNode) isHealthy() bool {
	return node.GetStateCode() == commonpb.StateCode_Healthy
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
func (node *DataNode) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	log.Debug("DataNode current state", zap.Any("State", node.stateCode.Load()))
	nodeID := common.NotRegisteredID
	if node.session != nil && node.session.Registered() {
		nodeID = node.session.ServerID
	}
	states := &milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			// NodeID:    Params.NodeID, // will race with DataNode.Register()
			NodeID:    nodeID,
			Role:      node.Role,
			StateCode: node.stateCode.Load().(commonpb.StateCode),
		},
		SubcomponentStates: make([]*milvuspb.ComponentInfo, 0),
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
//
//	If DataNode receives a valid segment to flush, new flush message for the segment should be ignored.
//	So if receiving calls to flush segment A, DataNode should guarantee the segment to be flushed.
//
//	One precondition: The segmentID in req is in ascending order.
func (node *DataNode) FlushSegments(ctx context.Context, req *datapb.FlushSegmentsRequest) (*commonpb.Status, error) {
	metrics.DataNodeFlushReqCounter.WithLabelValues(
		fmt.Sprint(Params.DataNodeCfg.GetNodeID()),
		metrics.TotalLabel).Inc()

	errStatus := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}

	if !node.isHealthy() {
		setNotServingStatus(errStatus, node.GetStateCode())
		return errStatus, nil
	}

	if req.GetBase().GetTargetID() != node.session.ServerID {
		log.Warn("flush segment target id not matched",
			zap.Int64("targetID", req.GetBase().GetTargetID()),
			zap.Int64("serverID", node.session.ServerID),
		)
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NodeIDNotMatch,
			Reason:    common.WrapNodeIDNotMatchMsg(req.GetBase().GetTargetID(), node.session.ServerID),
		}
		return status, nil
	}

	log.Info("receiving FlushSegments request",
		zap.Int64("collection ID", req.GetCollectionID()),
		zap.Int64s("segments", req.GetSegmentIDs()),
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
	// Log success flushed segments.
	if len(seg) > 0 {
		log.Info("sending segments to flush channel",
			zap.Any("newly sealed segment IDs", seg))
	}
	// Fail FlushSegments call if at least one segment fails to get flushed.
	if !noErr1 {
		return errStatus, nil
	}

	metrics.DataNodeFlushReqCounter.WithLabelValues(
		fmt.Sprint(Params.DataNodeCfg.GetNodeID()),
		metrics.SuccessLabel).Inc()
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

// ResendSegmentStats resend un-flushed segment stats back upstream to DataCoord by resending DataNode time tick message.
// It returns a list of segments to be sent.
// Deprecated in 2.2.15, reversed it just for compatibility during rolling back
func (node *DataNode) ResendSegmentStats(ctx context.Context, req *datapb.ResendSegmentStatsRequest) (*datapb.ResendSegmentStatsResponse, error) {
	return &datapb.ResendSegmentStatsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		SegResent: make([]int64, 0),
	}, nil
}

// Stop will release DataNode resources and shutdown datanode
func (node *DataNode) Stop() error {
	// https://github.com/milvus-io/milvus/issues/12282
	node.UpdateStateCode(commonpb.StateCode_Abnormal)

	node.flowgraphManager.dropAll()
	node.flowgraphManager.stop()
	// Delay the cancellation of ctx to ensure that the session is automatically recycled after closed flow graph
	node.cancel()

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

	if node.session != nil {
		node.session.Stop()
	}

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

// ShowConfigurations returns the configurations of DataNode matching req.Pattern
func (node *DataNode) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	log.Debug("DataNode.ShowConfigurations", zap.String("pattern", req.Pattern))
	if !node.isHealthy() {
		log.Warn("DataNode.ShowConfigurations failed",
			zap.Int64("nodeId", Params.QueryNodeCfg.GetNodeID()),
			zap.String("req", req.Pattern),
			zap.Error(errDataNodeIsUnhealthy(Params.QueryNodeCfg.GetNodeID())))

		resp := &internalpb.ShowConfigurationsResponse{Status: &commonpb.Status{}}
		setNotServingStatus(resp.Status, node.GetStateCode())
		return resp, nil
	}

	return getComponentConfigurations(ctx, req), nil
}

// GetMetrics return datanode metrics
func (node *DataNode) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("nodeID", Params.DataNodeCfg.GetNodeID()),
		zap.String("req", req.GetRequest()))

	if !node.isHealthy() {
		log.Warn("DataNode.GetMetrics failed",
			zap.Error(errDataNodeIsUnhealthy(Params.DataNodeCfg.GetNodeID())))

		resp := &milvuspb.GetMetricsResponse{Status: &commonpb.Status{}}
		setNotServingStatus(resp.Status, node.GetStateCode())
		return resp, nil
	}

	metricType, err := metricsinfo.ParseMetricType(req.Request)
	if err != nil {
		log.Warn("DataNode.GetMetrics failed to parse metric type",
			zap.Error(err))

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("datanode GetMetrics failed, nodeID=%d, err=%s", node.session.ServerID, err.Error()),
			},
		}, nil
	}

	if metricType == metricsinfo.SystemInfoMetrics {
		systemInfoMetrics, err := node.getSystemInfoMetrics(ctx, req)
		if err != nil {
			log.Warn("DataNode GetMetrics failed", zap.Error(err))
			return &milvuspb.GetMetricsResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    fmt.Sprintf("datanode GetMetrics failed, nodeID=%d, err=%s", node.session.ServerID, err.Error()),
				},
			}, nil
		}

		return systemInfoMetrics, nil
	}

	log.RatedWarn(60, "DataNode.GetMetrics failed, request metric type is not implemented yet",
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

	for _, segment := range req.GetSegmentBinlogs() {
		segmentInfo := ds.channel.getSegment(segment.GetSegmentID())
		if segmentInfo == nil || segmentInfo.getType() != datapb.SegmentType_Flushed {
			log.Warn("compaction plan contains segment which is not flushed or missing",
				zap.Int64("segmentID", segment.GetSegmentID()),
			)
			status.Reason = fmt.Sprintf("Segment %d is not flushed or missing", segment.GetSegmentID())
			return status, nil
		}
	}

	binlogIO := &binlogIO{node.chunkManager, ds.idAllocator}
	task := newCompactionTask(
		node.ctx,
		binlogIO, binlogIO,
		ds.channel,
		ds.flushManager,
		ds.idAllocator,
		req,
		node.chunkManager,
	)

	node.compactionExecutor.execute(task)

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

// GetCompactionState called by DataCoord
// return status of all compaction plans
func (node *DataNode) GetCompactionState(ctx context.Context, req *datapb.CompactionStateRequest) (*datapb.CompactionStateResponse, error) {
	if !node.isHealthy() {
		resp := &datapb.CompactionStateResponse{Status: &commonpb.Status{}}
		setNotServingStatus(resp.Status, node.GetStateCode())
		return resp, nil
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
		return true
	})

	if len(results) > 0 {
		planIDs := lo.Map(results, func(result *datapb.CompactionStateResult, i int) UniqueID {
			return result.GetPlanID()
		})
		log.Info("Compaction results", zap.Int64s("results", planIDs))
	}
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
		setNotServingStatus(status, node.GetStateCode())
		return status, nil
	}

	if len(req.GetCompactedFrom()) <= 0 {
		status.Reason = "invalid request, compacted from segments shouldn't be empty"
		return status, nil
	}

	var (
		oneSegment int64
		channel    Channel
		err        error
		ds         *dataSyncService
		ok         bool
	)

	for _, fromSegment := range req.GetCompactedFrom() {
		channel, err = node.flowgraphManager.getChannel(fromSegment)
		if err != nil {
			log.Ctx(ctx).Warn("fail to get the channel", zap.Int64("segment", fromSegment), zap.Error(err))
			continue
		}
		ds, ok = node.flowgraphManager.getFlowgraphService(channel.getChannelName())
		if !ok {
			log.Ctx(ctx).Warn("fail to find flow graph service", zap.Int64("segment", fromSegment))
			continue
		}
		oneSegment = fromSegment
		break
	}
	if oneSegment == 0 {
		log.Ctx(ctx).Warn("no valid segment, maybe the request is a retry")
		status.ErrorCode = commonpb.ErrorCode_Success
		return status, nil
	}

	// oneSegment is definitely in the channel, guaranteed by the check before.
	collID, partID, _ := channel.getCollectionAndPartitionID(oneSegment)
	targetSeg := &Segment{
		collectionID: collID,
		partitionID:  partID,
		segmentID:    req.GetCompactedTo(),
		numRows:      req.GetNumOfRows(),
		lastSyncTs:   tsoutil.GetCurrentTime(),
	}

	err = channel.InitPKstats(ctx, targetSeg, req.GetStatsLogs(), tsoutil.GetCurrentTime())
	if err != nil {
		log.Ctx(ctx).Warn("init pk stats fail", zap.Error(err))
		status.Reason = fmt.Sprintf("init pk stats fail, err=%s", err.Error())
		return status, nil
	}

	ds.fg.Blockall()
	defer ds.fg.Unblock()
	channel.mergeFlushedSegments(ctx, targetSeg, req.GetPlanID(), req.GetCompactedFrom())
	log.Ctx(ctx).Info("DataNode SyncSegments success", zap.Int64("planID", req.GetPlanID()),
		zap.Int64("target segmentID", req.GetCompactedTo()),
		zap.Int64s("compacted from", req.GetCompactedFrom()),
		zap.Int64("numOfRows", req.GetNumOfRows()))
	node.compactionExecutor.injectDone(req.GetPlanID(), true)
	status.ErrorCode = commonpb.ErrorCode_Success
	return status, nil
}

// Import data files(json, numpy, etc.) on MinIO/S3 storage, read and parse them into sealed segments
func (node *DataNode) Import(ctx context.Context, req *datapb.ImportTaskRequest) (*commonpb.Status, error) {
	logFields := []zap.Field{
		zap.Int64("task ID", req.GetImportTask().GetTaskId()),
		zap.Int64("collection ID", req.GetImportTask().GetCollectionId()),
		zap.Int64("partition ID", req.GetImportTask().GetPartitionId()),
		zap.String("database name", req.GetImportTask().GetDatabaseName()),
		zap.Strings("channel names", req.GetImportTask().GetChannelNames()),
		zap.Int64s("working dataNodes", req.WorkingNodes),
		zap.Int64("node ID", Params.DataNodeCfg.GetNodeID()),
	}
	log.Info("DataNode receive import request", logFields...)
	defer func() {
		log.Info("DataNode finish import request", logFields...)
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
	importResult.Infos = append(importResult.Infos, &commonpb.KeyValuePair{Key: importutil.ProgressPercent, Value: "0"})

	// Spawn a new context to ignore cancellation from parental context.
	newCtx, cancel := context.WithTimeout(context.TODO(), Params.DataNodeCfg.BulkInsertTimeoutSeconds)
	defer cancel()

	// function to report import state to RootCoord.
	// retry 10 times, if the rootcoord is down, the report function will cost 20+ seconds
	reportFunc := reportImportFunc(node)
	returnFailFunc := func(msg string, inputErr error) (*commonpb.Status, error) {
		logFields = append(logFields, zap.Error(inputErr))
		log.Warn(msg, logFields...)
		importResult.State = commonpb.ImportState_ImportFailed
		importResult.Infos = append(importResult.Infos, &commonpb.KeyValuePair{Key: importutil.FailedReason, Value: inputErr.Error()})

		reportFunc(importResult)

		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    inputErr.Error(),
		}, nil
	}

	if !node.isHealthy() {
		err := errDataNodeIsUnhealthy(Params.DataNodeCfg.GetNodeID())
		logFields = append(logFields, zap.Error(err))
		log.Warn("DataNode import failed, node is not healthy", logFields...)

		resp := &commonpb.Status{}
		setNotServingStatus(resp, node.GetStateCode())
		return resp, nil
	}

	// get a timestamp for all the rows
	// Ignore cancellation from parent context.
	rep, err := node.rootCoord.AllocTimestamp(newCtx, &rootcoordpb.AllocTimestampRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_RequestTSO),
			commonpbutil.WithMsgID(0),
			commonpbutil.WithTimeStamp(0),
			commonpbutil.WithSourceID(Params.DataNodeCfg.GetNodeID()),
		),
		Count: 1,
	})

	if rep.Status.ErrorCode != commonpb.ErrorCode_Success || err != nil {
		return returnFailFunc("DataNode alloc ts failed", err)
	}

	ts := rep.GetTimestamp()

	// get collection schema and shard number
	metaService := newMetaService(node.rootCoord, req.GetImportTask().GetCollectionId())
	colInfo, err := metaService.getCollectionInfo(newCtx, req.GetImportTask().GetCollectionId(), 0)
	if err != nil {
		return returnFailFunc("failed to get collection info for collection ID", err)
	}

	var partitionIDs []int64
	if req.GetImportTask().GetPartitionId() == 0 {
		if !typeutil.HasPartitionKey(colInfo.GetSchema()) {
			err = errors.New("try auto-distribute data but the collection has no partition key")
			return returnFailFunc(err.Error(), err)
		}
		// TODO: prefer to set partitionIDs in coord instead of get here.
		// the colInfo doesn't have a correct database name(it is empty). use the database name passed from rootcoord.
		partitions, err := node.getPartitions(ctx, req.GetImportTask().GetDatabaseName(), colInfo.GetCollectionName())
		if err != nil {
			return returnFailFunc("failed to get partition id list", err)
		}
		if len(partitions) == 0 {
			return returnFailFunc("failed to get partition id list", errors.New("partition list of partitionKey collection is empty"))
		}
		_, partitionIDs, err = typeutil.RearrangePartitionsForPartitionKey(partitions)
		if err != nil {
			return returnFailFunc("failed to rearrange target partitions", err)
		}
	} else {
		partitionIDs = []int64{req.GetImportTask().GetPartitionId()}
	}

	collectionInfo, err := importutil.NewCollectionInfo(colInfo.GetSchema(), colInfo.GetShardsNum(), partitionIDs)
	if err != nil {
		return returnFailFunc("invalid collection info to import", err)
	}

	// parse files and generate segments
	segmentSize := int64(Params.DataCoordCfg.SegmentMaxSize) * 1024 * 1024
	importWrapper := importutil.NewImportWrapper(newCtx, collectionInfo, segmentSize, Params.DataNodeCfg.BinLogMaxSize,
		node.rowIDAllocator, node.chunkManager, importResult, reportFunc)
	importWrapper.SetCallbackFunctions(assignSegmentFunc(node, req),
		createBinLogsFunc(node, req, colInfo.GetSchema(), ts),
		saveSegmentFunc(node, req, importResult, ts))
	// todo: pass tsStart and tsStart after import_wrapper support
	tsStart, tsEnd, err := importutil.ParseTSFromOptions(req.GetImportTask().GetInfos())
	isBackup := importutil.IsBackup(req.GetImportTask().GetInfos())
	if err != nil {
		return returnFailFunc("failed to parse timestamp from import options", err)
	}
	logFields = append(logFields, zap.Uint64("start_ts", tsStart), zap.Uint64("end_ts", tsEnd))
	log.Info("import time range", logFields...)
	err = importWrapper.Import(req.GetImportTask().GetFiles(),
		importutil.ImportOptions{OnlyValidate: false, TsStartPoint: tsStart, TsEndPoint: tsEnd, IsBackup: isBackup})
	if err != nil {
		return returnFailFunc("failed to import files", err)
	}

	resp := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	return resp, nil
}

func (node *DataNode) getPartitions(ctx context.Context, dbName string, collectionName string) (map[string]int64, error) {
	req := &milvuspb.ShowPartitionsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_ShowPartitions),
		),
		DbName:         dbName,
		CollectionName: collectionName,
	}

	logFields := []zap.Field{
		zap.String("dbName", dbName),
		zap.String("collection name", collectionName),
	}
	resp, err := node.rootCoord.ShowPartitions(ctx, req)
	if err != nil {
		logFields = append(logFields, zap.Error(err))
		log.Warn("failed to get partitions of collection", logFields...)
		return nil, err
	}
	if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
		log.Warn("failed to get partitions of collection", logFields...)
		return nil, errors.New(resp.Status.Reason)
	}

	partitionNames := resp.GetPartitionNames()
	partitionIDs := resp.GetPartitionIDs()
	if len(partitionNames) != len(partitionIDs) {
		logFields = append(logFields, zap.Int("number of names", len(partitionNames)), zap.Int("number of ids", len(partitionIDs)))
		log.Warn("partition names and ids are unequal", logFields...)
		return nil, fmt.Errorf("partition names and ids are unequal, number of names: %d, number of ids: %d",
			len(partitionNames), len(partitionIDs))
	}

	partitions := make(map[string]int64)
	for i := 0; i < len(partitionNames); i++ {
		partitions[partitionNames[i]] = partitionIDs[i]
	}

	return partitions, nil
}

// AddImportSegment adds the import segment to the current DataNode.
func (node *DataNode) AddImportSegment(ctx context.Context, req *datapb.AddImportSegmentRequest) (*datapb.AddImportSegmentResponse, error) {
	logFields := []zap.Field{
		zap.Int64("segment ID", req.GetSegmentId()),
		zap.Int64("collection ID", req.GetCollectionId()),
		zap.Int64("partition ID", req.GetPartitionId()),
		zap.String("channel name", req.GetChannelName()),
		zap.Int64("# of rows", req.GetRowNum()),
	}
	log.Info("adding segment to DataNode flow graph", logFields...)
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
		logFields = append(logFields, zap.Int64("node ID", Params.DataNodeCfg.GetNodeID()))
		log.Error("channel not found in current DataNode", logFields...)
		return &datapb.AddImportSegmentResponse{
			Status: &commonpb.Status{
				// TODO: Add specific error code.
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "channel not found in current DataNode",
			},
		}, nil
	}
	// Get the current dml channel position ID, that will be used in segments start positions and end positions.
	var posID []byte
	err = retry.Do(ctx, func() error {
		id, innerError := ds.getChannelLatestMsgID(context.Background(), req.GetChannelName(), req.GetSegmentId())
		posID = id
		return innerError
	}, retry.Attempts(30))

	if err != nil {
		return &datapb.AddImportSegmentResponse{
			Status: &commonpb.Status{
				// TODO: Add specific error code.
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "failed to get channel position",
			},
		}, nil
	}
	// Add the new segment to the channel.
	if !ds.channel.hasSegment(req.GetSegmentId(), true) {
		log.Info("adding a new segment to channel", logFields...)
		// Add segment as a flushed segment, but set `importing` to true to add extra information of the segment.
		// By 'extra information' we mean segment info while adding a `SegmentType_Flushed` typed segment.
		if err := ds.channel.addSegment(
			addSegmentReq{
				segType:      datapb.SegmentType_Flushed,
				segID:        req.GetSegmentId(),
				collID:       req.GetCollectionId(),
				partitionID:  req.GetPartitionId(),
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
			logFields = append(logFields, zap.Error(err))
			log.Error("failed to add segment to flow graph", logFields...)
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

func assignSegmentFunc(node *DataNode, req *datapb.ImportTaskRequest) importutil.AssignSegmentFunc {
	return func(shardID int, partID int64) (int64, string, error) {
		chNames := req.GetImportTask().GetChannelNames()
		importTaskID := req.GetImportTask().GetTaskId()
		logFields := []zap.Field{
			zap.Int64("task ID", importTaskID),
			zap.Int("shard ID", shardID),
			zap.Int64("partition ID", partID),
			zap.Int("# of channels", len(chNames)),
			zap.Strings("channel names", chNames),
		}
		if shardID >= len(chNames) {
			log.Error("import task returns invalid shard ID", logFields...)
			return 0, "", fmt.Errorf("syncSegmentID Failed: invalid shard ID %d", shardID)
		}

		tr := timerecord.NewTimeRecorder("assign segment function")
		defer tr.Elapse("finished")

		colID := req.GetImportTask().GetCollectionId()
		segmentIDReq := composeAssignSegmentIDRequest(1, shardID, chNames, colID, partID)
		targetChName := segmentIDReq.GetSegmentIDRequests()[0].GetChannelName()
		logFields = append(logFields, zap.Int64("collection ID", colID))
		logFields = append(logFields, zap.String("target channel name", targetChName))
		log.Info("assign segment for the import task", logFields...)
		resp, err := node.dataCoord.AssignSegmentID(context.Background(), segmentIDReq)
		if err != nil {
			return 0, "", fmt.Errorf("syncSegmentID Failed:%w", err)
		}
		if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return 0, "", fmt.Errorf("syncSegmentID Failed:%s", resp.Status.Reason)
		}
		if len(resp.SegIDAssignments) == 0 || resp.SegIDAssignments[0] == nil {
			return 0, "", fmt.Errorf("syncSegmentID Failed: the collection was dropped")
		}

		segmentID := resp.SegIDAssignments[0].SegID
		logFields = append(logFields, zap.Int64("segment ID", segmentID))
		log.Info("new segment assigned", logFields...)

		// call report to notify the rootcoord update the segment id list for this task
		// ignore the returned error, since even report failed the segments still can be cleaned
		// retry 10 times, if the rootcoord is down, the report function will cost 20+ seconds
		importResult := &rootcoordpb.ImportResult{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			TaskId:     req.GetImportTask().TaskId,
			DatanodeId: Params.DataNodeCfg.GetNodeID(),
			State:      commonpb.ImportState_ImportStarted,
			Segments:   []int64{segmentID},
			AutoIds:    make([]int64, 0),
			RowCount:   0,
		}
		reportFunc := reportImportFunc(node)
		reportFunc(importResult)

		return segmentID, targetChName, nil
	}
}

func createBinLogsFunc(node *DataNode, req *datapb.ImportTaskRequest, schema *schemapb.CollectionSchema, ts Timestamp) importutil.CreateBinlogsFunc {
	return func(fields importutil.BlockData, segmentID int64, partID int64) ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, error) {
		var rowNum int
		for _, field := range fields {
			rowNum = field.RowNum()
			break
		}

		chNames := req.GetImportTask().GetChannelNames()
		importTaskID := req.GetImportTask().GetTaskId()
		logFields := []zap.Field{
			zap.Int64("task ID", importTaskID),
			zap.Int64("partition ID", partID),
			zap.Int64("segment ID", segmentID),
			zap.Int("# of channels", len(chNames)),
			zap.Strings("channel names", chNames),
		}

		if rowNum <= 0 {
			log.Info("fields data is empty, no need to generate binlog", logFields...)
			return nil, nil, nil
		}
		logFields = append(logFields, zap.Int("row count", rowNum))

		colID := req.GetImportTask().GetCollectionId()
		fieldInsert, fieldStats, err := createBinLogs(rowNum, schema, ts, fields, node, segmentID, colID, partID)
		if err != nil {
			logFields = append(logFields, zap.Any("err", err))
			log.Error("failed to create binlogs", logFields...)
			return nil, nil, err
		}

		logFields = append(logFields, zap.Int("insert log count", len(fieldInsert)), zap.Int("stats log count", len(fieldStats)))
		log.Info("new binlog created", logFields...)

		return fieldInsert, fieldStats, err
	}
}

func saveSegmentFunc(node *DataNode, req *datapb.ImportTaskRequest, res *rootcoordpb.ImportResult, ts Timestamp) importutil.SaveSegmentFunc {
	importTaskID := req.GetImportTask().GetTaskId()
	return func(fieldsInsert []*datapb.FieldBinlog, fieldsStats []*datapb.FieldBinlog, segmentID int64,
		targetChName string, rowCount int64, partID int64,
	) error {
		logFields := []zap.Field{
			zap.Int64("task ID", importTaskID),
			zap.Int64("partition ID", partID),
			zap.Int64("segment ID", segmentID),
			zap.String("target channel name", targetChName),
			zap.Int64("row count", rowCount),
			zap.Uint64("ts", ts),
		}
		log.Info("adding segment to the correct DataNode flow graph and saving binlog paths", logFields...)

		err := retry.Do(context.Background(), func() error {
			// Ask DataCoord to save binlog path and add segment to the corresponding DataNode flow graph.
			resp, err := node.dataCoord.SaveImportSegment(context.Background(), &datapb.SaveImportSegmentRequest{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithTimeStamp(ts), // Pass current timestamp downstream.
					commonpbutil.WithSourceID(Params.DataNodeCfg.GetNodeID()),
				),
				SegmentId:    segmentID,
				ChannelName:  targetChName,
				CollectionId: req.GetImportTask().GetCollectionId(),
				PartitionId:  partID,
				RowNum:       rowCount,
				SaveBinlogPathReq: &datapb.SaveBinlogPathsRequest{
					Base: commonpbutil.NewMsgBase(
						commonpbutil.WithMsgType(0),
						commonpbutil.WithMsgID(0),
						commonpbutil.WithTimeStamp(ts),
						commonpbutil.WithSourceID(Params.DataNodeCfg.GetNodeID()),
					),
					SegmentID:           segmentID,
					CollectionID:        req.GetImportTask().GetCollectionId(),
					Field2BinlogPaths:   fieldsInsert,
					Field2StatslogPaths: fieldsStats,
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
			if resp.ErrorCode != commonpb.ErrorCode_Success && resp.ErrorCode != commonpb.ErrorCode_NotReadyServe {
				return retry.Unrecoverable(fmt.Errorf("failed to save import segment, reason = %s", resp.Reason))
			} else if resp.ErrorCode == commonpb.ErrorCode_NotReadyServe {
				return fmt.Errorf("failed to save import segment: %s", resp.GetReason())
			}
			return nil
		})
		if err != nil {
			log.Warn("failed to save import segment", zap.Error(err))
			return err
		}
		log.Info("segment imported and persisted", logFields...)
		res.Segments = append(res.Segments, segmentID)
		res.RowCount += rowCount
		return nil
	}
}

func composeAssignSegmentIDRequest(rowNum int, shardID int, chNames []string,
	collID int64, partID int64,
) *datapb.AssignSegmentIDRequest {
	// use the first field's row count as segment row count
	// all the fields row count are same, checked by ImportWrapper
	// ask DataCoord to alloc a new segment
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
	fields map[storage.FieldID]storage.FieldData, node *DataNode, segmentID, colID, partID UniqueID,
) ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tsFieldData := make([]int64, rowNum)
	for i := range tsFieldData {
		tsFieldData[i] = int64(ts)
	}
	fields[common.TimeStampField] = &storage.Int64FieldData{
		Data: tsFieldData,
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

func reportImportFunc(node *DataNode) importutil.ReportFunc {
	return func(importResult *rootcoordpb.ImportResult) error {
		err := retry.Do(context.Background(), func() error {
			status, err := node.rootCoord.ReportImport(context.Background(), importResult)
			if err != nil {
				log.Error("fail to report import state to RootCoord", zap.Error(err))
				return err
			}
			if status != nil && status.ErrorCode != commonpb.ErrorCode_Success {
				return errors.New(status.GetReason())
			}
			return nil
		}, retry.Attempts(node.reportImportRetryTimes))

		return err
	}
}

func logDupFlush(cID, segID int64) {
	log.Info("segment is already being flushed, ignoring flush request",
		zap.Int64("collection ID", cID),
		zap.Int64("segment ID", segID))
}

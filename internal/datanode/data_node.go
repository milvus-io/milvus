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
	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	allocator2 "github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/importutil"
	"github.com/milvus-io/milvus/internal/util/logutil"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	// RPCConnectionTimeout is used to set the timeout for rpc request
	RPCConnectionTimeout = 30 * time.Second

	// MetricRequestsTotal is used to count the num of total requests
	MetricRequestsTotal = "total"

	// MetricRequestsSuccess is used to count the num of successful requests
	MetricRequestsSuccess = "success"

	// ConnectEtcdMaxRetryTime is used to limit the max retry time for connection etcd
	ConnectEtcdMaxRetryTime = 1000
)

const illegalRequestErrStr = "Illegal request"

// makes sure DataNode implements types.DataNode
var _ types.DataNode = (*DataNode)(nil)

// Params from config.yaml
var Params paramtable.ComponentParam

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
	NodeID UniqueID
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

	session      *sessionutil.Session
	watchKv      kv.MetaKv
	chunkManager storage.ChunkManager

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

// SetNodeID set node id for DataNode
func (node *DataNode) SetNodeID(id UniqueID) {
	node.NodeID = id
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
	Params.DataNodeCfg.NodeID = node.session.ServerID
	node.NodeID = node.session.ServerID
	Params.SetLogger(Params.DataNodeCfg.NodeID)
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

	node.factory.Init(&Params)
	log.Info("DataNode Init successfully",
		zap.String("MsgChannelSubName", Params.CommonCfg.DataNodeSubName))

	return nil
}

// StartWatchChannels start loop to watch channel allocation status via kv(etcd for now)
func (node *DataNode) StartWatchChannels(ctx context.Context) {
	defer logutil.LogPanic()
	// REF MEP#7 watch path should be [prefix]/channel/{node_id}/{channel_name}
	watchPrefix := path.Join(Params.DataNodeCfg.ChannelWatchSubPath, fmt.Sprintf("%d", node.NodeID))
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
		case event := <-evtChan:
			if event.Canceled { // event canceled
				log.Warn("watch channel canceled", zap.Error(event.Err()))
				// https://github.com/etcd-io/etcd/issues/8980
				if event.Err() == v3rpc.ErrCompacted {
					go node.StartWatchChannels(ctx)
					return
				}
				// if watch loop return due to event canceled, the datanode is not functional anymore
				// stop the datanode and wait for restart
				err := node.Stop()
				if err != nil {
					log.Warn("node stop failed", zap.Error(err))
				}
				return
			}
			for _, evt := range event.Events {
				go node.handleChannelEvt(evt)
			}
		}
	}
}

// checkWatchedList list all nodes under [prefix]/channel/{node_id} and make sure all nodeds are watched
// serves the corner case for etcd connection lost and missing some events
func (node *DataNode) checkWatchedList() error {
	// REF MEP#7 watch path should be [prefix]/channel/{node_id}/{channel_name}
	prefix := path.Join(Params.DataNodeCfg.ChannelWatchSubPath, fmt.Sprintf("%d", node.NodeID))
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
	return &watchInfo, nil
}

func parseDeleteEventKey(key string) string {
	parts := strings.Split(key, "/")
	vChanName := parts[len(parts)-1]
	return vChanName
}

func (node *DataNode) handlePutEvent(watchInfo *datapb.ChannelWatchInfo, version int64) (err error) {
	vChanName := watchInfo.GetVchan().GetChannelName()
	log.Info("handle put event", zap.String("watch state", watchInfo.State.String()), zap.String("vChanName", vChanName))

	switch watchInfo.State {
	case datapb.ChannelWatchState_Uncomplete, datapb.ChannelWatchState_ToWatch:
		if err := node.flowgraphManager.addAndStart(node, watchInfo.GetVchan()); err != nil {
			return fmt.Errorf("fail to add and start flowgraph for vChanName: %s, err: %v", vChanName, err)
		}

		log.Debug("handle put event: new data sync service success", zap.String("vChanName", vChanName))
		watchInfo.State = datapb.ChannelWatchState_WatchSuccess

	case datapb.ChannelWatchState_ToRelease:
		if node.tryToReleaseFlowgraph(vChanName) {
			watchInfo.State = datapb.ChannelWatchState_ReleaseSuccess
		} else {
			watchInfo.State = datapb.ChannelWatchState_ReleaseFailure
		}
	}

	v, err := proto.Marshal(watchInfo)
	if err != nil {
		return fmt.Errorf("fail to marshal watchInfo with state, vChanName: %s, state: %s ,err: %w", vChanName, watchInfo.State.String(), err)
	}

	k := path.Join(Params.DataNodeCfg.ChannelWatchSubPath, fmt.Sprintf("%d", node.NodeID), vChanName)

	log.Debug("handle put event: try to save result state", zap.String("key", k), zap.String("state", watchInfo.State.String()))
	err = node.watchKv.CompareVersionAndSwap(k, version, string(v))
	if err != nil {
		return fmt.Errorf("fail to update watch state to etcd, vChanName: %s, state: %s, err: %w", vChanName, watchInfo.State.String(), err)
	}
	return nil
}

func (node *DataNode) handleDeleteEvent(vChanName string) bool {
	return node.tryToReleaseFlowgraph(vChanName)
}

// tryToReleaseFlowgraph tries to release a flowgraph, returns false if failed
func (node *DataNode) tryToReleaseFlowgraph(vChanName string) bool {
	success := true
	defer func() {
		if x := recover(); x != nil {
			log.Error("release flowgraph panic", zap.String("vChanName", vChanName), zap.Any("recovered", x))
			success = false
		}
	}()
	node.flowgraphManager.release(vChanName)
	log.Info("try to release flowgraph success", zap.String("vChanName", vChanName))
	return success
}

// BackGroundGC runs in background to release datanode resources
// GOOSE TODO: remove background GC, using ToRelease for drop-collection after #15846
func (node *DataNode) BackGroundGC(vChannelCh <-chan string) {
	log.Info("DataNode Background GC Start")
	for {
		select {
		case vchanName := <-vChannelCh:
			log.Info("GC flowgraph", zap.String("vChanName", vchanName))
			node.tryToReleaseFlowgraph(vchanName)
		case <-node.ctx.Done():
			log.Warn("DataNode context done, exiting background GC")
			return
		}
	}
}

// FilterThreshold is the start time ouf DataNode
var FilterThreshold Timestamp

// Start will update DataNode state to HEALTHY
func (node *DataNode) Start() error {

	rep, err := node.rootCoord.AllocTimestamp(node.ctx, &rootcoordpb.AllocTimestampRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_RequestTSO,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  node.NodeID,
		},
		Count: 1,
	})
	if err != nil {
		log.Warn("fail to alloc timestamp", zap.Error(err))
		return err
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

	chunkManager, err := node.factory.NewVectorStorageChunkManager(node.ctx)

	if err != nil {
		return err
	}

	node.chunkManager = chunkManager

	if rep.Status.ErrorCode != commonpb.ErrorCode_Success || err != nil {
		return errors.New("DataNode fail to start")
	}

	FilterThreshold = rep.GetTimestamp()

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
	code := node.State.Load().(internalpb.StateCode)
	return code == internalpb.StateCode_Healthy
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

// ReadyToFlush tells wether DataNode is ready for flushing
func (node *DataNode) ReadyToFlush() error {
	if node.State.Load().(internalpb.StateCode) != internalpb.StateCode_Healthy {
		return errors.New("DataNode not in HEALTHY state")
	}
	return nil
}

// FlushSegments packs flush messages into flowgraph through flushChan.
//   If DataNode receives a valid segment to flush, new flush message for the segment should be ignored.
//   So if receiving calls to flush segment A, DataNode should guarantee the segment to be flushed.
//
//   One precondition: The segmentID in req is in ascending order.
func (node *DataNode) FlushSegments(ctx context.Context, req *datapb.FlushSegmentsRequest) (*commonpb.Status, error) {
	metrics.DataNodeFlushSegmentsReqCounter.WithLabelValues(
		fmt.Sprint(Params.DataNodeCfg.NodeID),
		MetricRequestsTotal).Inc()

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}

	if node.State.Load().(internalpb.StateCode) != internalpb.StateCode_Healthy {
		status.Reason = "DataNode not in HEALTHY state"
		return status, nil
	}

	log.Info("Receive FlushSegments req",
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int64s("segments", req.GetSegmentIDs()),
		zap.Int64s("stale segments", req.GetMarkSegmentIDs()),
	)

	processSegments := func(segmentIDs []UniqueID, flushed bool) bool {
		noErr := true
		for _, id := range segmentIDs {
			if node.segmentCache.checkIfCached(id) {
				// Segment in flushing, ignore
				log.Info("segment flushing, ignore the flush request until flush is done.",
					zap.Int64("collectionID", req.GetCollectionID()), zap.Int64("segmentID", id))
				status.Reason = "segment is flushing, nothing is done"
				noErr = false
				continue
			}

			node.segmentCache.Cache(id)

			flushCh, err := node.flowgraphManager.getFlushCh(id)
			if err != nil {
				status.Reason = "no flush channel found for v-channel"
				log.Error("no flush channel found for v-channel", zap.Error(err))
				noErr = false
				continue
			}

			flushCh <- flushMsg{
				msgID:        req.Base.MsgID,
				timestamp:    req.Base.Timestamp,
				segmentID:    id,
				collectionID: req.CollectionID,
				flushed:      flushed,
			}
		}
		log.Info("flow graph flushSegment tasks triggered",
			zap.Bool("flushed", flushed),
			zap.Int64("collection ID", req.GetCollectionID()),
			zap.Int64s("segments", segmentIDs),
			zap.Int64s("mark segments", req.GetMarkSegmentIDs()))
		return noErr
	}

	ok := processSegments(req.GetSegmentIDs(), true)
	if !ok {
		return status, nil
	}
	ok = processSegments(req.GetMarkSegmentIDs(), false)
	if !ok {
		return status, nil
	}

	status.ErrorCode = commonpb.ErrorCode_Success
	metrics.DataNodeFlushSegmentsReqCounter.WithLabelValues(
		fmt.Sprint(Params.DataNodeCfg.NodeID),
		MetricRequestsSuccess).Inc()

	return status, nil
}

// Stop will release DataNode resources and shutdown datanode
func (node *DataNode) Stop() error {
	// https://github.com/milvus-io/milvus/issues/12282
	node.UpdateStateCode(internalpb.StateCode_Abnormal)

	node.cancel()
	node.flowgraphManager.dropAll()

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
			Reason:    "",
		},
		Value: "",
	}, nil
}

// GetStatisticsChannel currently do nothing
func (node *DataNode) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: "",
	}, nil
}

// GetMetrics return datanode metrics
// TODO(dragondriver): cache the Metrics and set a retention to the cache
func (node *DataNode) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	log.Debug("DataNode.GetMetrics",
		zap.Int64("node_id", Params.DataNodeCfg.NodeID),
		zap.String("req", req.Request))

	if !node.isHealthy() {
		log.Warn("DataNode.GetMetrics failed",
			zap.Int64("node_id", Params.DataNodeCfg.NodeID),
			zap.String("req", req.Request),
			zap.Error(errDataNodeIsUnhealthy(Params.DataNodeCfg.NodeID)))

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msgDataNodeIsUnhealthy(Params.DataNodeCfg.NodeID),
			},
			Response: "",
		}, nil
	}

	metricType, err := metricsinfo.ParseMetricType(req.Request)
	if err != nil {
		log.Warn("DataNode.GetMetrics failed to parse metric type",
			zap.Int64("node_id", Params.DataNodeCfg.NodeID),
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

	log.Debug("DataNode.GetMetrics",
		zap.String("metric_type", metricType))

	if metricType == metricsinfo.SystemInfoMetrics {
		systemInfoMetrics, err := node.getSystemInfoMetrics(ctx, req)

		log.Debug("DataNode.GetMetrics",
			zap.Int64("node_id", Params.DataNodeCfg.NodeID),
			zap.String("req", req.Request),
			zap.String("metric_type", metricType),
			zap.Any("systemInfoMetrics", systemInfoMetrics), // TODO(dragondriver): necessary? may be very large
			zap.Error(err))

		return systemInfoMetrics, nil
	}

	log.Debug("DataNode.GetMetrics failed, request metric type is not implemented yet",
		zap.Int64("node_id", Params.DataNodeCfg.NodeID),
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
		node.dataCoord,
		req,
	)

	node.compactionExecutor.execute(task)

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

// Import data files(json, numpy, etc.) on MinIO/S3 storage, read and parse them into sealed segments
func (node *DataNode) Import(ctx context.Context, req *datapb.ImportTaskRequest) (*commonpb.Status, error) {
	log.Info("receive import request",
		zap.Int64("task ID", req.GetImportTask().GetTaskId()),
		zap.Int64("collection ID", req.GetImportTask().GetCollectionId()),
		zap.Int64("partition ID", req.GetImportTask().GetPartitionId()),
		zap.Any("channel names", req.GetImportTask().GetChannelNames()),
		zap.Any("working dataNodes", req.WorkingNodes))

	importResult := &rootcoordpb.ImportResult{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		TaskId:     req.GetImportTask().TaskId,
		DatanodeId: node.NodeID,
		State:      commonpb.ImportState_ImportStarted,
		Segments:   make([]int64, 0),
		AutoIds:    make([]int64, 0),
		RowCount:   0,
	}
	reportFunc := func(res *rootcoordpb.ImportResult) error {
		_, err := node.rootCoord.ReportImport(ctx, res)
		return err
	}

	if !node.isHealthy() {
		log.Warn("DataNode import failed",
			zap.Int64("collection ID", req.GetImportTask().GetCollectionId()),
			zap.Int64("partition ID", req.GetImportTask().GetPartitionId()),
			zap.Int64("taskID", req.GetImportTask().GetTaskId()),
			zap.Error(errDataNodeIsUnhealthy(Params.DataNodeCfg.NodeID)))

		msg := msgDataNodeIsUnhealthy(Params.DataNodeCfg.NodeID)
		importResult.State = commonpb.ImportState_ImportFailed
		importResult.Infos = append(importResult.Infos, &commonpb.KeyValuePair{Key: "failed_reason", Value: msg})
		reportFunc(importResult)
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    msg,
		}, nil
	}
	rep, err := node.rootCoord.AllocTimestamp(ctx, &rootcoordpb.AllocTimestampRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_RequestTSO,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  node.NodeID,
		},
		Count: 1,
	})

	if rep.Status.ErrorCode != commonpb.ErrorCode_Success || err != nil {
		msg := "DataNode alloc ts failed"
		log.Warn(msg)
		importResult.State = commonpb.ImportState_ImportFailed
		importResult.Infos = append(importResult.Infos, &commonpb.KeyValuePair{Key: "failed_reason", Value: msg})
		reportFunc(importResult)
		if err != nil {
			return &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msg,
			}, nil
		}
	}

	ts := rep.GetTimestamp()

	metaService := newMetaService(node.rootCoord, req.GetImportTask().GetCollectionId())
	schema, err := metaService.getCollectionSchema(ctx, req.GetImportTask().GetCollectionId(), 0)
	if err != nil {
		importResult.State = commonpb.ImportState_ImportFailed
		importResult.Infos = append(importResult.Infos, &commonpb.KeyValuePair{Key: "failed_reason", Value: err.Error()})
		reportFunc(importResult)
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	// temp id allocator service
	idAllocator, err := allocator2.NewIDAllocator(node.ctx, node.rootCoord, Params.DataNodeCfg.NodeID)
	_ = idAllocator.Start()
	defer idAllocator.Close()

	segmentSize := int64(Params.DataCoordCfg.SegmentMaxSize) * 1024 * 1024
	importWrapper := importutil.NewImportWrapper(ctx, schema, 2, segmentSize, idAllocator, node.chunkManager,
		importFlushReqFunc(node, req, importResult, schema, ts), importResult, reportFunc)
	err = importWrapper.Import(req.GetImportTask().GetFiles(), req.GetImportTask().GetRowBased(), false)
	if err != nil {
		importResult.State = commonpb.ImportState_ImportFailed
		importResult.Infos = append(importResult.Infos, &commonpb.KeyValuePair{Key: "failed_reason", Value: err.Error()})
		reportFunc(importResult)
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

func importFlushReqFunc(node *DataNode, req *datapb.ImportTaskRequest, res *rootcoordpb.ImportResult, schema *schemapb.CollectionSchema, ts Timestamp) importutil.ImportFlushFunc {
	return func(fields map[storage.FieldID]storage.FieldData, shardNum int) error {
		if shardNum >= len(req.GetImportTask().GetChannelNames()) {
			log.Error("import task returns invalid shard number",
				zap.Int("shard num", shardNum),
				zap.Int("# of channels", len(req.GetImportTask().GetChannelNames())),
				zap.Any("channel names", req.GetImportTask().GetChannelNames()),
			)
			return fmt.Errorf("syncSegmentID Failed: invalid shard number %d", shardNum)
		}
		log.Info("import task flush segment",
			zap.Any("channel names", req.ImportTask.ChannelNames),
			zap.Int("shard num", shardNum))
		segReqs := []*datapb.SegmentIDRequest{
			{
				ChannelName:  req.ImportTask.ChannelNames[shardNum],
				Count:        1,
				CollectionID: req.GetImportTask().GetCollectionId(),
				PartitionID:  req.GetImportTask().GetPartitionId(),
			},
		}
		segmentIDReq := &datapb.AssignSegmentIDRequest{
			NodeID:            0,
			PeerRole:          typeutil.ProxyRole,
			SegmentIDRequests: segReqs,
		}

		resp, err := node.dataCoord.AssignSegmentID(context.Background(), segmentIDReq)
		if err != nil {
			return fmt.Errorf("syncSegmentID Failed:%w", err)
		}

		if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return fmt.Errorf("syncSegmentID Failed:%s", resp.Status.Reason)
		}
		segmentID := resp.SegIDAssignments[0].SegID

		// TODO: this code block is long and tedious, maybe split it into separate functions.
		var rowNum int
		for _, field := range fields {
			rowNum = field.RowNum()
			break
		}
		tsFieldData := make([]int64, rowNum)
		for i := range tsFieldData {
			tsFieldData[i] = int64(ts)
		}
		fields[common.TimeStampField] = &storage.Int64FieldData{
			Data:    tsFieldData,
			NumRows: []int64{int64(rowNum)},
		}
		var pkFieldID int64
		for _, field := range schema.Fields {
			if field.IsPrimaryKey {
				pkFieldID = field.GetFieldID()
				break
			}
		}
		fields[common.RowIDField] = fields[pkFieldID]
		if status, _ := node.dataCoord.UpdateSegmentStatistics(context.TODO(), &datapb.UpdateSegmentStatisticsRequest{
			Stats: []*datapb.SegmentStats{{
				SegmentID: segmentID,
				NumRows:   int64(rowNum),
			}},
		}); status.GetErrorCode() != commonpb.ErrorCode_Success {
			// TODO: reportImport the failure.
			return fmt.Errorf(status.GetReason())
		}

		data := BufferData{buffer: &InsertData{
			Data: fields,
		}}
		meta := &etcdpb.CollectionMeta{
			ID:     req.GetImportTask().GetCollectionId(),
			Schema: schema,
		}
		inCodec := storage.NewInsertCodec(meta)

		binLogs, statsBinlogs, err := inCodec.Serialize(req.GetImportTask().GetPartitionId(), segmentID, data.buffer)
		if err != nil {
			return err
		}

		var alloc allocatorInterface = newAllocator(node.rootCoord)
		start, _, err := alloc.allocIDBatch(uint32(len(binLogs)))
		if err != nil {
			return err
		}

		field2Insert := make(map[UniqueID]*datapb.Binlog, len(binLogs))
		kvs := make(map[string][]byte, len(binLogs))
		field2Logidx := make(map[UniqueID]UniqueID, len(binLogs))
		for idx, blob := range binLogs {
			fieldID, err := strconv.ParseInt(blob.GetKey(), 10, 64)
			if err != nil {
				log.Error("Flush failed ... cannot parse string to fieldID ..", zap.Error(err))
				return err
			}

			logidx := start + int64(idx)

			// no error raise if alloc=false
			k := JoinIDPath(req.GetImportTask().GetCollectionId(), req.GetImportTask().GetPartitionId(), segmentID, fieldID, logidx)

			key := path.Join(Params.DataNodeCfg.InsertBinlogRootPath, k)
			kvs[key] = blob.Value[:]
			field2Insert[fieldID] = &datapb.Binlog{
				EntriesNum:    data.size,
				TimestampFrom: 0, //TODO
				TimestampTo:   0, //TODO,
				LogPath:       key,
				LogSize:       int64(len(blob.Value)),
			}
			field2Logidx[fieldID] = logidx
		}

		field2Stats := make(map[UniqueID]*datapb.Binlog)
		// write stats binlog
		for _, blob := range statsBinlogs {
			fieldID, err := strconv.ParseInt(blob.GetKey(), 10, 64)
			if err != nil {
				log.Error("Flush failed ... cannot parse string to fieldID ..", zap.Error(err))
				return err
			}

			logidx := field2Logidx[fieldID]

			// no error raise if alloc=false
			k := JoinIDPath(req.GetImportTask().GetCollectionId(), req.GetImportTask().GetPartitionId(), segmentID, fieldID, logidx)

			key := path.Join(Params.DataNodeCfg.StatsBinlogRootPath, k)
			kvs[key] = blob.Value
			field2Stats[fieldID] = &datapb.Binlog{
				EntriesNum:    0,
				TimestampFrom: 0, //TODO
				TimestampTo:   0, //TODO,
				LogPath:       key,
				LogSize:       int64(len(blob.Value)),
			}
		}

		err = node.chunkManager.MultiWrite(kvs)
		if err != nil {
			return err
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

		ds, ok := node.flowgraphManager.getFlowgraphService(segReqs[0].GetChannelName())
		if !ok {
			log.Warn("channel not found in current dataNode",
				zap.String("channel name", segReqs[0].GetChannelName()),
				zap.Int64("node ID", node.NodeID))
			return errors.New("channel " + segReqs[0].GetChannelName() + " not found in current dataNode")
		}

		// Update flow graph replica segment info.
		// TODO: Duplicate code. Add wrapper function.
		if !ds.replica.hasSegment(segmentID, true) {
			err = ds.replica.addNewSegment(segmentID,
				req.GetImportTask().GetCollectionId(),
				req.GetImportTask().GetPartitionId(),
				segReqs[0].GetChannelName(),
				&internalpb.MsgPosition{
					ChannelName: segReqs[0].GetChannelName(),
				},
				&internalpb.MsgPosition{
					ChannelName: segReqs[0].GetChannelName(),
				})
			if err != nil {
				log.Error("failed to add segment",
					zap.Int64("segment ID", segmentID),
					zap.Int64("collection ID", req.GetImportTask().GetCollectionId()),
					zap.Int64("partition ID", req.GetImportTask().GetPartitionId()),
					zap.String("channel mame", segReqs[0].GetChannelName()),
					zap.Error(err))
			}
		}
		ds.replica.updateStatistics(segmentID, int64(rowNum))

		req := &datapb.SaveBinlogPathsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0, //TODO msg type
				MsgID:     0, //TODO msg id
				Timestamp: 0, //TODO time stamp
				SourceID:  Params.DataNodeCfg.NodeID,
			},
			SegmentID:           segmentID,
			CollectionID:        req.ImportTask.GetCollectionId(),
			Field2BinlogPaths:   fieldInsert,
			Field2StatslogPaths: fieldStats,
			Importing:           true,
		}

		err = retry.Do(context.Background(), func() error {
			rsp, err := node.dataCoord.SaveBinlogPaths(context.Background(), req)
			// should be network issue, return error and retry
			if err != nil {
				return fmt.Errorf(err.Error())
			}

			// TODO should retry only when datacoord status is unhealthy
			if rsp.ErrorCode != commonpb.ErrorCode_Success {
				return fmt.Errorf("data service save bin log path failed, reason = %s", rsp.Reason)
			}
			return nil
		})
		if err != nil {
			log.Warn("failed to SaveBinlogPaths", zap.Error(err))
			return err
		}

		log.Info("segment imported and persisted", zap.Int64("segmentID", segmentID))
		res.Segments = append(res.Segments, segmentID)
		res.RowCount += int64(rowNum)
		return nil
	}
}

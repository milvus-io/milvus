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
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	miniokv "github.com/milvus-io/milvus/internal/kv/minio"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/logutil"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
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
	ConnectEtcdMaxRetryTime = 1000
)

const illegalRequestErrStr = "Illegal request"

// makes sure DataNode implements types.DataNode
var _ types.DataNode = (*DataNode)(nil)

var Params paramtable.GlobalParamTable

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
//  `vchan2SyncService` is a map of vchannlName to dataSyncService, so that datanode
//  has ability to scale flowgraph.
//  `vchan2FlushCh` holds flush-signal channels for every flowgraph.
//  `clearSignal` is a signal channel for releasing the flowgraph resources.
//  `segmentCache` stores all flushing and flushed segments.
type DataNode struct {
	ctx    context.Context
	cancel context.CancelFunc
	NodeID UniqueID
	Role   string
	State  atomic.Value // internalpb.StateCode_Initializing

	// TODO struct
	chanMut           sync.RWMutex
	vchan2SyncService map[string]*dataSyncService // vchannel name
	vchan2FlushChs    map[string]chan flushMsg    // vchannel name to flush channels

	clearSignal        chan string // vchannel name
	segmentCache       *Cache
	compactionExecutor *compactionExecutor

	etcdCli   *clientv3.Client
	rootCoord types.RootCoord
	dataCoord types.DataCoord

	session *sessionutil.Session
	watchKv kv.MetaKv
	blobKv  kv.BaseKV

	closer io.Closer

	msFactory msgstream.Factory
}

// NewDataNode will return a DataNode with abnormal state.
func NewDataNode(ctx context.Context, factory msgstream.Factory) *DataNode {
	rand.Seed(time.Now().UnixNano())
	ctx2, cancel2 := context.WithCancel(ctx)
	node := &DataNode{
		ctx:    ctx2,
		cancel: cancel2,
		Role:   typeutil.DataNodeRole,

		rootCoord:          nil,
		dataCoord:          nil,
		msFactory:          factory,
		segmentCache:       newCache(),
		compactionExecutor: newCompactionExecutor(),

		vchan2SyncService: make(map[string]*dataSyncService),
		vchan2FlushChs:    make(map[string]chan flushMsg),
		clearSignal:       make(chan string, 100),
	}
	node.UpdateStateCode(internalpb.StateCode_Abnormal)
	return node
}

// Set etcd client
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
			syscall.Kill(syscall.Getpid(), syscall.SIGINT)
		}
	})

	return nil
}

func (node *DataNode) initSession() error {
	node.session = sessionutil.NewSession(node.ctx, Params.BaseParams.MetaRootPath, node.etcdCli)
	if node.session == nil {
		return errors.New("failed to initialize session")
	}
	node.session.Init(typeutil.DataNodeRole, Params.DataNodeCfg.IP+":"+strconv.Itoa(Params.DataNodeCfg.Port), false, true)
	Params.DataNodeCfg.NodeID = node.session.ServerID
	node.NodeID = node.session.ServerID
	Params.BaseParams.SetLogger(Params.DataNodeCfg.NodeID)
	return nil
}

// Init function does nothing now.
func (node *DataNode) Init() error {
	log.Debug("DataNode Init",
		zap.String("TimeTickChannelName", Params.DataNodeCfg.TimeTickChannelName),
	)
	if err := node.initSession(); err != nil {
		log.Error("DataNode init session failed", zap.Error(err))
		return err
	}
	Params.DataNodeCfg.Refresh()

	m := map[string]interface{}{
		"PulsarAddress":  Params.PulsarCfg.Address,
		"ReceiveBufSize": 1024,
		"PulsarBufSize":  1024,
	}

	if err := node.msFactory.SetParams(m); err != nil {
		log.Warn("DataNode Init msFactory SetParams failed, use default",
			zap.Error(err))
		return err
	}
	log.Debug("DataNode Init",
		zap.String("MsgChannelSubName", Params.DataNodeCfg.MsgChannelSubName))

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
			log.Debug("watch etcd loop quit")
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
		node.handleWatchInfo(keys[i], []byte(val))
	}
	return nil
}

// handleChannelEvt handles event from kv watch event
func (node *DataNode) handleChannelEvt(evt *clientv3.Event) {
	switch evt.Type {
	case clientv3.EventTypePut: // datacoord shall put channels needs to be watched here
		log.Debug("DataNode handleChannelEvt EventTypePut", zap.String("key", string(evt.Kv.Key)))
		node.handleWatchInfo(string(evt.Kv.Key), evt.Kv.Value)
	case clientv3.EventTypeDelete:
		// guaranteed there is no "/" in channel name
		parts := strings.Split(string(evt.Kv.Key), "/")
		vchanName := parts[len(parts)-1]
		log.Debug("DataNode handleChannelEvt EventTypeDelete",
			zap.String("key", string(evt.Kv.Key)),
			zap.String("vChanName", vchanName),
			zap.Int64("node id", Params.DataNodeCfg.NodeID))
		node.ReleaseDataSyncService(vchanName)
	}
}

func (node *DataNode) handleWatchInfo(key string, data []byte) {
	watchInfo := datapb.ChannelWatchInfo{}
	err := proto.Unmarshal(data, &watchInfo)
	if err != nil {
		log.Warn("fail to parse ChannelWatchInfo", zap.String("key", key), zap.Error(err))
		return
	}
	log.Debug("DataNode handleWatchInfo Unmarshal success")
	if watchInfo.State == datapb.ChannelWatchState_Complete {
		log.Warn("DataNode handleWatchInfo State is already ChannelWatchState_Complete")
		return
	}
	if watchInfo.Vchan == nil {
		log.Warn("found ChannelWatchInfo with nil VChannelInfo", zap.String("key", key))
		return
	}
	log.Warn("DataNode handleWatchInfo try to NewDataSyncService", zap.String("key", key))
	err = node.NewDataSyncService(watchInfo.Vchan)
	if err != nil {
		log.Warn("fail to create DataSyncService", zap.String("key", key), zap.Error(err))
		return
	}
	log.Warn("DataNode handleWatchInfo NewDataSyncService success", zap.String("key", key))

	watchInfo.State = datapb.ChannelWatchState_Complete
	v, err := proto.Marshal(&watchInfo)
	if err != nil {
		log.Warn("DataNode handleWatchInfo fail to Marshal watchInfo", zap.String("key", key), zap.Error(err))
		return
	}
	k := path.Join(Params.DataNodeCfg.ChannelWatchSubPath, fmt.Sprintf("%d", node.NodeID), watchInfo.GetVchan().GetChannelName())
	log.Warn("DataNode handleWatchInfo try to Save", zap.String("key", key),
		zap.String("k", k),
		zap.String("v", string(v)))

	err = node.watchKv.Save(k, string(v))
	if err != nil {
		log.Warn("DataNode handleWatchInfo fail to change WatchState to complete", zap.String("key", key), zap.Error(err))
		node.ReleaseDataSyncService(key)
	}
}

// NewDataSyncService adds a new dataSyncService for new dmlVchannel and starts dataSyncService.
func (node *DataNode) NewDataSyncService(vchan *datapb.VchannelInfo) error {
	node.chanMut.RLock()
	if _, ok := node.vchan2SyncService[vchan.GetChannelName()]; ok {
		node.chanMut.RUnlock()
		return nil
	}
	node.chanMut.RUnlock()

	replica, err := newReplica(node.ctx, node.rootCoord, vchan.CollectionID)
	if err != nil {
		return err
	}

	var alloc allocatorInterface = newAllocator(node.rootCoord)

	log.Debug("DataNode NewDataSyncService received Vchannel Info",
		zap.Int64("collectionID", vchan.CollectionID),
		zap.Int("Unflushed Segment Number", len(vchan.GetUnflushedSegments())),
		zap.Int("Flushed Segment Number", len(vchan.GetFlushedSegments())),
	)

	flushCh := make(chan flushMsg, 100)

	dataSyncService, err := newDataSyncService(node.ctx, flushCh, replica, alloc, node.msFactory, vchan, node.clearSignal, node.dataCoord, node.segmentCache, node.blobKv, node.compactionExecutor)
	if err != nil {
		log.Error("DataNode NewDataSyncService newDataSyncService failed",
			zap.Error(err),
		)
		return err
	}

	node.chanMut.Lock()
	node.vchan2SyncService[vchan.GetChannelName()] = dataSyncService
	node.vchan2FlushChs[vchan.GetChannelName()] = flushCh
	node.chanMut.Unlock()

	log.Info("DataNode NewDataSyncService success",
		zap.Int64("Collection ID", vchan.GetCollectionID()),
		zap.String("Vchannel name", vchan.GetChannelName()),
	)
	dataSyncService.start()

	return nil
}

// BackGroundGC runs in background to release datanode resources
func (node *DataNode) BackGroundGC(vChannelCh <-chan string) {
	log.Info("DataNode Background GC Start")
	for {
		select {
		case vChan := <-vChannelCh:
			log.Info("GC flowgraph", zap.String("vChan", vChan))
			node.ReleaseDataSyncService(vChan)
		case <-node.ctx.Done():
			log.Info("DataNode ctx done")
			return
		}
	}
}

// ReleaseDataSyncService release flowgraph resources for a vchanName
func (node *DataNode) ReleaseDataSyncService(vchanName string) {
	log.Info("Release flowgraph resources begin", zap.String("Vchannel", vchanName))

	node.chanMut.Lock()
	dss, ok := node.vchan2SyncService[vchanName]
	delete(node.vchan2SyncService, vchanName)
	delete(node.vchan2FlushChs, vchanName)
	node.chanMut.Unlock()

	if ok {
		// This is a time-consuming process, better to put outside of the lock
		dss.close()
	}
	log.Debug("Release flowgraph resources end", zap.String("Vchannel", vchanName))
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
		etcdKV := etcdkv.NewEtcdKV(node.etcdCli, Params.BaseParams.MetaRootPath)
		node.watchKv = etcdKV
		return nil
	}
	err = retry.Do(node.ctx, connectEtcdFn, retry.Attempts(ConnectEtcdMaxRetryTime))
	if err != nil {
		return errors.New("DataNode fail to connect etcd")
	}

	option := &miniokv.Option{
		Address:           Params.MinioCfg.Address,
		AccessKeyID:       Params.MinioCfg.AccessKeyID,
		SecretAccessKeyID: Params.MinioCfg.SecretAccessKey,
		UseSSL:            Params.MinioCfg.UseSSL,
		BucketName:        Params.MinioCfg.BucketName,
		CreateBucket:      true,
	}

	kv, err := miniokv.NewMinIOKV(node.ctx, option)
	if err != nil {
		return err
	}

	node.blobKv = kv

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

func (node *DataNode) getChannelNamebySegmentID(segID UniqueID) string {
	node.chanMut.RLock()
	defer node.chanMut.RUnlock()
	for name, dataSync := range node.vchan2SyncService {
		if dataSync.replica.hasSegment(segID, true) {
			return name
		}
	}
	return ""
}

func (node *DataNode) getChannelNamesbyCollectionID(collID UniqueID) []string {
	node.chanMut.RLock()
	defer node.chanMut.RUnlock()

	channels := make([]string, 0, len(node.vchan2SyncService))
	for name, dataSync := range node.vchan2SyncService {
		if dataSync.collectionID == collID {
			channels = append(channels, name)
		}
	}
	return channels
}

// ReadyToFlush tells wether DataNode is ready for flushing
func (node *DataNode) ReadyToFlush() error {
	if node.State.Load().(internalpb.StateCode) != internalpb.StateCode_Healthy {
		return errors.New("DataNode not in HEALTHY state")
	}

	node.chanMut.RLock()
	defer node.chanMut.RUnlock()
	if len(node.vchan2SyncService) == 0 && len(node.vchan2FlushChs) == 0 {
		// Healthy but Idle
		msg := "DataNode HEALTHY but IDLE, please try WatchDmChannels to make it work"
		log.Warn(msg)
		return errors.New(msg)
	}

	if len(node.vchan2SyncService) != len(node.vchan2FlushChs) {
		// TODO restart
		msg := "DataNode HEALTHY but abnormal inside, restarting..."
		log.Warn(msg)
		return errors.New(msg)
	}
	return nil
}

// FlushSegments packs flush messages into flowgraph through flushChan.
//   If DataNode receives a valid segment to flush, new flush message for the segment should be ignored.
//   So if receiving calls to flush segment A, DataNode should guarantee the segment to be flushed.
//
//   One precondition: The segmentID in req is in ascending order.
func (node *DataNode) FlushSegments(ctx context.Context, req *datapb.FlushSegmentsRequest) (*commonpb.Status, error) {
	metrics.DataNodeFlushSegmentsCounter.WithLabelValues(MetricRequestsTotal).Inc()
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}

	if err := node.ReadyToFlush(); err != nil {
		status.Reason = err.Error()
		return status, nil
	}

	log.Debug("Receive FlushSegments req",
		zap.Int64("collectionID", req.GetCollectionID()), zap.Int("num", len(req.SegmentIDs)),
		zap.Int64s("segments", req.SegmentIDs),
	)

	processSegments := func(segmentIDs []UniqueID, flushed bool) bool {
		noErr := true
		for _, id := range segmentIDs {
			chanName := node.getChannelNamebySegmentID(id)
			if len(chanName) == 0 {
				log.Warn("FlushSegments failed, cannot find segment in DataNode replica",
					zap.Int64("collectionID", req.GetCollectionID()), zap.Int64("segmentID", id))

				status.Reason = fmt.Sprintf("DataNode replica not find segment %d!", id)
				noErr = false
				continue
			}

			if node.segmentCache.checkIfCached(id) {
				// Segment in flushing, ignore
				log.Info("Segment flushing, ignore the flush request until flush is done.",
					zap.Int64("collectionID", req.GetCollectionID()), zap.Int64("segmentID", id))

				continue
			}

			node.segmentCache.Cache(id)

			node.chanMut.RLock()
			flushChs, ok := node.vchan2FlushChs[chanName]
			node.chanMut.RUnlock()
			if !ok {
				status.Reason = "DataNode abnormal, restarting"
				log.Error("DataNode abnormal, no flushCh for a vchannel")
				noErr = false
				continue
			}

			flushChs <- flushMsg{
				msgID:        req.Base.MsgID,
				timestamp:    req.Base.Timestamp,
				segmentID:    id,
				collectionID: req.CollectionID,
				flushed:      flushed,
			}
		}
		log.Debug("Flowgraph flushSegment tasks triggered", zap.Bool("flushed", flushed),
			zap.Int64("collectionID", req.GetCollectionID()), zap.Int64s("segments", segmentIDs))

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
	metrics.DataNodeFlushSegmentsCounter.WithLabelValues(MetricRequestsSuccess).Inc()
	return status, nil
}

// Stop will release DataNode resources and shutdown datanode
func (node *DataNode) Stop() error {
	// https://github.com/milvus-io/milvus/issues/12282
	node.UpdateStateCode(internalpb.StateCode_Abnormal)

	node.cancel()

	node.chanMut.RLock()
	defer node.chanMut.RUnlock()
	// close services
	for _, syncService := range node.vchan2SyncService {
		if syncService != nil {
			(*syncService).close()
		}
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

	ds, ok := node.vchan2SyncService[req.GetChannel()]
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

	binlogIO := &binlogIO{node.blobKv, ds.idAllocator}
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

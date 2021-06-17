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
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/masterpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
)

const (
	RPCConnectionTimeout = 30 * time.Second
)

// DataNode struct communicates with outside services and unioun all
// services of data node.
//
// DataNode struct implements `types.Component`, `types.DataNode` interfaces.
//  `masterService` holds a grpc client of master service.
//  `dataService` holds a grpc client of data service.
//  `NodeID` is unique to each data node.
//  `State` is current statement of this data node, indicating whether it's healthy.
//
//  `vchan2SyncService` holds map of vchannlName and dataSyncService, so that datanode
//  has ability to scale flowgraph
//  `vchan2FlushCh` holds flush-signal channels for every flowgraph
type DataNode struct {
	ctx     context.Context
	cancel  context.CancelFunc
	NodeID  UniqueID
	Role    string
	State   atomic.Value // internalpb.StateCode_Initializing
	watchDm chan struct{}

	chanMut           sync.RWMutex
	vchan2SyncService map[string]*dataSyncService // vchannel name
	vchan2FlushCh     map[string]chan<- *flushMsg // vchannel name
	clearSignal       chan UniqueID               // collection ID
	segmentCache      *Cache

	masterService types.MasterService
	dataService   types.DataService

	session *sessionutil.Session

	closer io.Closer

	msFactory msgstream.Factory
}

// NewDataNode will return a DataNode with abnormal state.
func NewDataNode(ctx context.Context, factory msgstream.Factory) *DataNode {
	rand.Seed(time.Now().UnixNano())
	ctx2, cancel2 := context.WithCancel(ctx)
	node := &DataNode{
		ctx:     ctx2,
		cancel:  cancel2,
		Role:    typeutil.DataNodeRole,
		watchDm: make(chan struct{}, 1),

		masterService: nil,
		dataService:   nil,
		msFactory:     factory,
		segmentCache:  newCache(),

		vchan2SyncService: make(map[string]*dataSyncService),
		vchan2FlushCh:     make(map[string]chan<- *flushMsg),
		clearSignal:       make(chan UniqueID, 100),
	}
	node.UpdateStateCode(internalpb.StateCode_Abnormal)
	return node
}

// SetMasterServiceInterface sets master service's grpc client, error is returned if repeatedly set.
func (node *DataNode) SetMasterServiceInterface(ms types.MasterService) error {
	switch {
	case ms == nil, node.masterService != nil:
		return errors.New("Nil parameter or repeatly set")
	default:
		node.masterService = ms
		return nil
	}
}

// SetDataServiceInterface sets data service's grpc client, error is returned if repeatedly set.
func (node *DataNode) SetDataServiceInterface(ds types.DataService) error {
	switch {
	case ds == nil, node.dataService != nil:
		return errors.New("Nil parameter or repeatly set")
	default:
		node.dataService = ds
		return nil
	}
}

// Register register data node at etcd
func (node *DataNode) Register() error {
	node.session = sessionutil.NewSession(node.ctx, Params.MetaRootPath, Params.EtcdEndpoints)
	node.session.Init(typeutil.DataNodeRole, Params.IP+":"+strconv.Itoa(Params.Port), false)
	Params.NodeID = node.session.ServerID

	Params.initMsgChannelSubName()
	log.Debug("DataNode Init",
		zap.String("MsgChannelSubName", Params.MsgChannelSubName),
	)
	return nil
}

// Init function supposes data service is in INITIALIZING state.
func (node *DataNode) Init() error {
	log.Debug("DataNode Init",
		zap.Any("SegmentStatisticsChannelName", Params.SegmentStatisticsChannelName),
		zap.Any("TimeTickChannelName", Params.TimeTickChannelName),
	)

	return nil
}

// NewDataSyncService adds a new dataSyncService for new dmlVchannel and starts dataSyncService.
func (node *DataNode) NewDataSyncService(vchan *datapb.VchannelInfo) error {
	node.chanMut.Lock()
	defer node.chanMut.Unlock()
	if _, ok := node.vchan2SyncService[vchan.GetChannelName()]; ok {
		return nil
	}

	replica := newReplica(node.masterService, vchan.CollectionID)

	var alloc allocatorInterface = newAllocator(node.masterService)

	log.Debug("Received Vchannel Info",
		zap.Int("Unflushed Segment Number", len(vchan.GetUnflushedSegments())),
		zap.Int("Flushed Segment Number", len(vchan.GetFlushedSegments())),
	)

	flushChan := make(chan *flushMsg, 100)
	dataSyncService := newDataSyncService(node.ctx, flushChan, replica, alloc, node.msFactory, vchan, node.clearSignal, node.dataService)
	node.vchan2SyncService[vchan.GetChannelName()] = dataSyncService
	node.vchan2FlushCh[vchan.GetChannelName()] = flushChan

	go dataSyncService.start()

	log.Info("New dataSyncService started!")

	return nil
}

// BackGroundGC runs in background to release datanode resources
func (node *DataNode) BackGroundGC(collIDCh <-chan UniqueID) {
	log.Info("DataNode Background GC Start")
	for {
		select {
		case collID := <-collIDCh:
			log.Info("GC collection", zap.Int64("ID", collID))
			for _, vchanName := range node.getChannelNamesbyCollectionID(collID) {
				node.ReleaseDataSyncService(vchanName)
			}
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
	defer node.chanMut.Unlock()
	if dss, ok := node.vchan2SyncService[vchanName]; ok {
		dss.close()
	}

	delete(node.vchan2SyncService, vchanName)
	delete(node.vchan2FlushCh, vchanName)

	log.Debug("Release flowgraph resources end", zap.String("Vchannel", vchanName))
}

var FilterThreshold Timestamp

// Start will update DataNode state to HEALTHY
func (node *DataNode) Start() error {

	rep, err := node.masterService.AllocTimestamp(node.ctx, &masterpb.AllocTimestampRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_RequestTSO,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  node.NodeID,
		},
		Count: 1,
	})

	if rep.Status.ErrorCode != commonpb.ErrorCode_Success || err != nil {
		return errors.New("DataNode fail to start")
	}

	FilterThreshold = rep.GetTimestamp()

	go node.BackGroundGC(node.clearSignal)
	node.UpdateStateCode(internalpb.StateCode_Healthy)
	return nil
}

// UpdateStateCode updates datanode's state code
func (node *DataNode) UpdateStateCode(code internalpb.StateCode) {
	node.State.Store(code)
}

// WatchDmChannels create a new dataSyncService for every unique dmlVchannel name, ignore if dmlVchannel existed.
func (node *DataNode) WatchDmChannels(ctx context.Context, in *datapb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}

	switch {
	case node.State.Load() != internalpb.StateCode_Healthy:
		status.Reason = fmt.Sprintf("DataNode %d not healthy, please re-send message", node.NodeID)
		return status, errors.New(status.GetReason())

	case len(in.GetVchannels()) == 0:
		status.Reason = "Illegal request"
		return status, errors.New(status.GetReason())

	default:
		for _, chanInfo := range in.GetVchannels() {
			log.Info("DataNode new dataSyncService",
				zap.String("channel name", chanInfo.ChannelName),
				zap.Any("channal Info", chanInfo),
			)
			node.NewDataSyncService(chanInfo)
		}

		status.ErrorCode = commonpb.ErrorCode_Success
		log.Debug("DataNode WatchDmChannels Done")
		return status, nil
	}
}

// GetComponentStates will return current state of DataNode
func (node *DataNode) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	log.Debug("DataNode current state", zap.Any("State", node.State.Load()))
	states := &internalpb.ComponentStates{
		State: &internalpb.ComponentInfo{
			NodeID:    Params.NodeID,
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
		if dataSync.replica.hasSegment(segID) {
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
	if len(node.vchan2SyncService) == 0 && len(node.vchan2FlushCh) == 0 {
		// Healthy but Idle
		msg := "DataNode HEALTHY but IDLE, please try WatchDmChannels to make it work"
		log.Warn(msg)
		return errors.New(msg)
	}

	if len(node.vchan2SyncService) != len(node.vchan2FlushCh) {
		// TODO restart
		msg := "DataNode HEALTHY but abnormal inside, restarting..."
		log.Warn(msg)
		return errors.New(msg)
	}
	return nil
}

func (node *DataNode) getSegmentPositionPair(segmentID UniqueID, chanName string) ([]*internalpb.MsgPosition, []*internalpb.MsgPosition) {
	node.chanMut.Lock()
	defer node.chanMut.Unlock()
	sync, ok := node.vchan2SyncService[chanName]
	if !ok {
		return nil, nil
	}

	starts, ends := sync.replica.getSegmentPositions(segmentID)
	return starts, ends
}

// FlushSegments packs flush messages into flowgraph through flushChan.
//   If DataNode receives a valid segment to flush, new flush message for the segment should be ignored.
//   So if receiving calls to flush segment A, DataNode should guarantee the segment to be flushed.
//
//   There are 1 precondition: The segmentID in req is in ascending order.
func (node *DataNode) FlushSegments(ctx context.Context, req *datapb.FlushSegmentsRequest) (*commonpb.Status, error) {
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}

	if err := node.ReadyToFlush(); err != nil {
		status.Reason = err.Error()
		return status, nil
	}

	numOfFlushingSeg := len(req.SegmentIDs)
	log.Debug("FlushSegments ...", zap.Int("num", len(req.SegmentIDs)), zap.Int64s("segments", req.SegmentIDs))
	dmlFlushedCh := make(chan []*datapb.ID2PathList, len(req.SegmentIDs))
	for _, id := range req.SegmentIDs {
		chanName := node.getChannelNamebySegmentID(id)
		log.Info("vchannel", zap.String("name", chanName))
		if len(chanName) == 0 {
			status.Reason = fmt.Sprintf("DataNode not find segment %d!", id)
			return status, errors.New(status.GetReason())
		}

		if node.segmentCache.checkIfCached(id) {
			// Segment in flushing or flushed, ignore
			log.Info("Segment in flushing, ignore it", zap.Int64("ID", id))
			numOfFlushingSeg--
			continue
		}

		node.segmentCache.Cache(id)

		node.chanMut.RLock()
		flushCh, ok := node.vchan2FlushCh[chanName]
		node.chanMut.RUnlock()
		if !ok {
			// TODO restart DataNode or reshape vchan2FlushCh and vchan2SyncService
			status.Reason = "DataNode abnormal, restarting"
			return status, nil
		}

		flushmsg := &flushMsg{
			msgID:        req.Base.MsgID,
			timestamp:    req.Base.Timestamp,
			segmentID:    id,
			collectionID: req.CollectionID,
			dmlFlushedCh: dmlFlushedCh,
		}
		flushCh <- flushmsg

	}

	failedSegments := ""
	for i := 0; i < numOfFlushingSeg; i++ {
		msg := <-dmlFlushedCh
		if len(msg) != 1 {
			panic("flush size expect to 1")
		}
		if msg[0].Paths == nil {
			failedSegments += fmt.Sprintf(" %d", msg[0].ID)
		}
	}
	if len(failedSegments) != 0 {
		status.Reason = fmt.Sprintf("flush failed segment list = %s", failedSegments)
		return status, nil
	}
	log.Debug("FlushSegments Done")

	status.ErrorCode = commonpb.ErrorCode_Success
	return status, nil
}

func (node *DataNode) Stop() error {
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
		node.closer.Close()
	}
	return nil
}

func (node *DataNode) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: "",
	}, nil
}

func (node *DataNode) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: "",
	}, nil
}

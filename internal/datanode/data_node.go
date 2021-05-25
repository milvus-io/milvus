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
// Data node persists definition language (ddl) strings and insert logs into persistent storage like minIO/S3.
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

	vchan2SyncService map[string]*dataSyncService
	vchan2FlushCh     map[string]chan<- *flushMsg

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

		vchan2SyncService: make(map[string]*dataSyncService),
		vchan2FlushCh:     make(map[string]chan<- *flushMsg),
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
	node.session = sessionutil.NewSession(node.ctx, []string{Params.EtcdAddress})
	node.session.Init(typeutil.DataNodeRole, Params.IP+":"+strconv.Itoa(Params.Port), false)
	Params.NodeID = node.session.ServerID
	return nil
}

// Init function supposes data service is in INITIALIZING state.
//
// In Init process, data node will register itself to data service with its node id
// and address. Therefore, `SetDataServiceInterface()` must be called before this func.
// Registering return several channel names data node need.
//
// At last, data node initializes its `dataSyncService` and `metaService`.
func (node *DataNode) Init() error {
	ctx := context.Background()

	node.session = sessionutil.NewSession(ctx, []string{Params.EtcdAddress})
	node.session.Init(typeutil.DataNodeRole, Params.IP+":"+strconv.Itoa(Params.Port), false)

	// TODO find DataService & MasterService
	req := &datapb.RegisterNodeRequest{
		Base: &commonpb.MsgBase{
			SourceID: node.NodeID,
		},
		Address: &commonpb.Address{
			Ip:   Params.IP,
			Port: int64(Params.Port),
		},
	}

	resp, err := node.dataService.RegisterNode(ctx, req)
	if err != nil {
		return fmt.Errorf("Register node failed: %v", err)
	}
	if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
		return fmt.Errorf("Receive error when registering data node, msg: %s", resp.Status.Reason)
	}

	for _, kv := range resp.InitParams.StartParams {
		switch kv.Key {
		case "DDChannelName":
			Params.DDChannelNames = []string{kv.Value}
		case "SegmentStatisticsChannelName":
			Params.SegmentStatisticsChannelName = kv.Value
		case "TimeTickChannelName":
			Params.TimeTickChannelName = kv.Value
		case "CompleteFlushChannelName":
			Params.CompleteFlushChannelName = kv.Value
		default:
			return fmt.Errorf("Invalid key: %v", kv.Key)
		}
	}

	return nil
}

// NewDataSyncService adds a new dataSyncService to DataNode
func (node *DataNode) NewDataSyncService(vchanPair *datapb.VchannelPair) error {
	if _, ok := node.vchan2SyncService[vchanPair.GetDmlVchannelName()]; ok {
		return nil
	}

	replica := newReplica()
	var alloc allocatorInterface = newAllocator(node.masterService)

	flushChan := make(chan *flushMsg, 100)
	dataSyncService := newDataSyncService(node.ctx, flushChan, replica, alloc, node.msFactory, vchanPair)
	metaService := newMetaService(node.ctx, replica, node.masterService)
	node.vchan2SyncService[vchanPair.GetDmlVchannelName()] = dataSyncService
	node.vchan2FlushCh[vchanPair.GetDmlVchannelName()] = flushChan

	metaService.init()
	go dataSyncService.start()

	return nil
}

// Start will update state to HEALTHY
func (node *DataNode) Start() error {
	node.UpdateStateCode(internalpb.StateCode_Healthy)
	return nil
}

// UpdateStateCode update datanode's state code
func (node *DataNode) UpdateStateCode(code internalpb.StateCode) {
	node.State.Store(code)
}

// WatchDmChannels set insert channel names data node subscribs to.
func (node *DataNode) WatchDmChannels(ctx context.Context, in *datapb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}

	switch {
	case node.State.Load() != internalpb.StateCode_Healthy:
		status.Reason = fmt.Sprintf("DataNode %d not initializing!", node.NodeID)
		return status, errors.New(status.GetReason())

	case len(in.GetVchannels()) == 0:
		status.Reason = "Illegal request"
		return status, errors.New(status.GetReason())

	default:
		for _, chanPair := range in.GetVchannels() {
			node.NewDataSyncService(chanPair)
		}

		status.ErrorCode = commonpb.ErrorCode_Success
		return status, nil
	}
}

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

func (node *DataNode) getChannelName(segID UniqueID) string {
	for name, dataSync := range node.vchan2SyncService {
		if dataSync.replica.hasSegment(segID) {
			return name
		}
	}
	return ""
}

// FlushSegments packs flush messages into flowgraph through flushChan.
func (node *DataNode) FlushSegments(ctx context.Context, req *datapb.FlushSegmentsRequest) (*commonpb.Status, error) {
	log.Debug("FlushSegments ...", zap.Int("num", len(req.SegmentIDs)))
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}

	for _, id := range req.SegmentIDs {
		chanName := node.getChannelName(id)
		if chanName == "" {
			status.Reason = fmt.Sprintf("DataNode not find segment %d!", id)
			return status, errors.New(status.GetReason())
		}
		flushCh, ok := node.vchan2FlushCh[chanName]
		if !ok {
			status.Reason = "DataNode abnormal!"
			return status, errors.New(status.GetReason())
		}

		ddlFlushedCh := make(chan bool)
		dmlFlushedCh := make(chan bool)

		flushmsg := &flushMsg{
			msgID:        req.Base.MsgID,
			timestamp:    req.Base.Timestamp,
			segmentID:    id,
			collectionID: req.CollectionID,
			ddlFlushedCh: ddlFlushedCh,
			dmlFlushedCh: dmlFlushedCh,
		}

		flushCh <- flushmsg

		// GOOSE TODO get binlog paths.
		waitReceive := func(wg *sync.WaitGroup, flushedCh <-chan bool, req *datapb.SaveBinlogPathsRequest) {
			defer wg.Done()
			select {
			case <-time.After(300 * time.Second):
				return
			case isFlushed := <-flushedCh:
				if isFlushed {
					log.Debug("Yeah! It's safe to notify dataservice")
				}
			}
		}

		// TODO make a queue for this func
		currentSegID := id
		go func() {
			log.Info("Waiting for flush completed", zap.Int64("segmentID", currentSegID))
			req := &datapb.SaveBinlogPathsRequest{
				Base:         &commonpb.MsgBase{},
				SegmentID:    currentSegID,
				CollectionID: req.CollectionID,
			}

			var wg sync.WaitGroup
			wg.Add(1)
			go waitReceive(&wg, ddlFlushedCh, req)
			wg.Add(1)
			go waitReceive(&wg, dmlFlushedCh, req)
			wg.Wait()

			// TODO
			//status := node.dataService.SaveBinlogPaths(req)
			log.Info("Flush Completed", zap.Int64("segmentID", currentSegID))
		}()

	}

	status.ErrorCode = commonpb.ErrorCode_Success
	return status, nil
}

func (node *DataNode) Stop() error {
	node.cancel()

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

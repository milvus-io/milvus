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

package datanode

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/types"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
)

const (
	RPCConnectionTimeout = 30 * time.Second
)

type DataNode struct {
	ctx     context.Context
	cancel  context.CancelFunc
	NodeID  UniqueID
	Role    string
	State   atomic.Value // internalpb.StateCode_Initializing
	watchDm chan struct{}

	dataSyncService *dataSyncService
	metaService     *metaService

	masterService types.MasterService
	dataService   types.DataService

	flushChan chan<- *flushMsg
	replica   Replica

	closer io.Closer

	msFactory msgstream.Factory
}

func NewDataNode(ctx context.Context, factory msgstream.Factory) *DataNode {
	rand.Seed(time.Now().UnixNano())
	ctx2, cancel2 := context.WithCancel(ctx)
	node := &DataNode{
		ctx:     ctx2,
		cancel:  cancel2,
		Role:    typeutil.DataNodeRole,
		watchDm: make(chan struct{}, 1),

		dataSyncService: nil,
		metaService:     nil,
		masterService:   nil,
		dataService:     nil,
		replica:         nil,
		msFactory:       factory,
	}
	node.UpdateStateCode(internalpb.StateCode_Abnormal)
	return node
}

func (node *DataNode) SetMasterServiceInterface(ms types.MasterService) error {
	switch {
	case ms == nil, node.masterService != nil:
		return errors.New("Nil parameter or repeatly set")
	default:
		node.masterService = ms
		return nil
	}
}

func (node *DataNode) SetDataServiceInterface(ds types.DataService) error {
	switch {
	case ds == nil, node.dataService != nil:
		return errors.New("Nil parameter or repeatly set")
	default:
		node.dataService = ds
		return nil
	}
}

// Suppose dataservice is in INITIALIZING
func (node *DataNode) Init() error {
	ctx := context.Background()

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

	select {
	case <-time.After(RPCConnectionTimeout):
		return errors.New("Get DmChannels failed in 30 seconds")
	case <-node.watchDm:
		log.Debug("insert channel names set")
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

	replica := newReplica()

	var alloc allocatorInterface = newAllocator(node.masterService)

	chanSize := 100
	flushChan := make(chan *flushMsg, chanSize)
	node.flushChan = flushChan
	node.dataSyncService = newDataSyncService(node.ctx, flushChan, replica, alloc, node.msFactory)
	node.dataSyncService.init()
	node.metaService = newMetaService(node.ctx, replica, node.masterService)
	node.replica = replica

	return nil
}

func (node *DataNode) Start() error {
	node.metaService.init()
	go node.dataSyncService.start()
	node.UpdateStateCode(internalpb.StateCode_Healthy)
	return nil
}

func (node *DataNode) UpdateStateCode(code internalpb.StateCode) {
	node.State.Store(code)
}

func (node *DataNode) WatchDmChannels(ctx context.Context, in *datapb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}

	switch {
	case node.State.Load() != internalpb.StateCode_Initializing:
		status.Reason = fmt.Sprintf("DataNode %d not initializing!", node.NodeID)
		return status, errors.New(status.GetReason())

	case len(Params.InsertChannelNames) != 0:
		status.Reason = fmt.Sprintf("DataNode has %d already set insert channels!", node.NodeID)
		return status, errors.New(status.GetReason())

	default:
		Params.InsertChannelNames = in.GetChannelNames()
		status.ErrorCode = commonpb.ErrorCode_Success
		node.watchDm <- struct{}{}
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

func (node *DataNode) FlushSegments(ctx context.Context, req *datapb.FlushSegmentsRequest) (*commonpb.Status, error) {
	log.Debug("FlushSegments ...", zap.Int("num", len(req.SegmentIDs)))
	ids := make([]UniqueID, 0)
	ids = append(ids, req.SegmentIDs...)

	flushmsg := &flushMsg{
		msgID:        req.Base.MsgID,
		timestamp:    req.Base.Timestamp,
		segmentIDs:   ids,
		collectionID: req.CollectionID,
	}

	node.flushChan <- flushmsg
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (node *DataNode) Stop() error {
	node.cancel()

	// close services
	if node.dataSyncService != nil {
		(*node.dataSyncService).close()
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

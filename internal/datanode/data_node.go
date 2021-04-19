package datanode

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"errors"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/types"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
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
	State   atomic.Value // internalpb2.StateCode_INITIALIZING
	watchDm chan struct{}

	dataSyncService *dataSyncService
	metaService     *metaService

	masterService types.MasterService
	dataService   types.DataService

	flushChan chan *flushMsg
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
		watchDm: make(chan struct{}),

		dataSyncService: nil,
		metaService:     nil,
		masterService:   nil,
		dataService:     nil,
		replica:         nil,
		msFactory:       factory,
	}
	node.UpdateStateCode(internalpb2.StateCode_ABNORMAL)
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
			MsgType:  commonpb.MsgType_kNone,
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
	node.flushChan = make(chan *flushMsg, chanSize)

	node.dataSyncService = newDataSyncService(node.ctx, node.flushChan, replica, alloc, node.msFactory)
	node.dataSyncService.init()
	node.metaService = newMetaService(node.ctx, replica, node.masterService)

	node.replica = replica

	return nil
}

func (node *DataNode) Start() error {
	node.metaService.init()
	go node.dataSyncService.start()
	node.UpdateStateCode(internalpb2.StateCode_HEALTHY)
	return nil
}

func (node *DataNode) UpdateStateCode(code internalpb2.StateCode) {
	node.State.Store(code)
}

func (node *DataNode) WatchDmChannels(ctx context.Context, in *datapb.WatchDmChannelRequest) (*commonpb.Status, error) {
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_ERROR_CODE_UNEXPECTED_ERROR,
	}

	switch {

	case node.State.Load() != internalpb2.StateCode_INITIALIZING:
		status.Reason = fmt.Sprintf("DataNode %d not initializing!", node.NodeID)
		return status, errors.New(status.GetReason())

	case len(Params.InsertChannelNames) != 0:
		status.Reason = fmt.Sprintf("DataNode has %d already set insert channels!", node.NodeID)
		return status, errors.New(status.GetReason())

	default:
		Params.InsertChannelNames = in.GetChannelNames()
		status.ErrorCode = commonpb.ErrorCode_ERROR_CODE_SUCCESS
		node.watchDm <- struct{}{}
		return status, nil
	}
}

func (node *DataNode) GetComponentStates(ctx context.Context) (*internalpb2.ComponentStates, error) {
	log.Debug("DataNode current state", zap.Any("State", node.State.Load()))
	states := &internalpb2.ComponentStates{
		State: &internalpb2.ComponentInfo{
			NodeID:    Params.NodeID,
			Role:      node.Role,
			StateCode: node.State.Load().(internalpb2.StateCode),
		},
		SubcomponentStates: make([]*internalpb2.ComponentInfo, 0),
		Status:             &commonpb.Status{ErrorCode: commonpb.ErrorCode_ERROR_CODE_SUCCESS},
	}
	return states, nil
}

func (node *DataNode) FlushSegments(ctx context.Context, in *datapb.FlushSegRequest) error {
	ids := make([]UniqueID, 0)
	ids = append(ids, in.SegmentIDs...)

	flushmsg := &flushMsg{
		msgID:        in.Base.MsgID,
		timestamp:    in.Base.Timestamp,
		segmentIDs:   ids,
		collectionID: in.CollectionID,
	}

	node.flushChan <- flushmsg
	return nil
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
			ErrorCode: commonpb.ErrorCode_ERROR_CODE_SUCCESS,
			Reason:    "",
		},
		Value: "",
	}, nil
}

func (node *DataNode) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_ERROR_CODE_SUCCESS,
			Reason:    "",
		},
		Value: "",
	}, nil
}

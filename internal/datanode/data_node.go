package datanode

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync/atomic"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

const (
	RPCConnectionTimeout = 30 * time.Second
)

type (
	Inteface interface {
		// Service
		Init() error
		Start() error
		Stop() error

		// Component
		GetComponentStates() (*internalpb2.ComponentStates, error)
		GetTimeTickChannel() (*milvuspb.StringResponse, error)   // This function has no effect
		GetStatisticsChannel() (*milvuspb.StringResponse, error) // This function has no effect

		WatchDmChannels(in *datapb.WatchDmChannelRequest) (*commonpb.Status, error)
		FlushSegments(in *datapb.FlushSegRequest) (*commonpb.Status, error)

		SetMasterServiceInterface(ms MasterServiceInterface) error
		SetDataServiceInterface(ds DataServiceInterface) error
	}

	DataServiceInterface interface {
		GetComponentStates() (*internalpb2.ComponentStates, error)
		RegisterNode(req *datapb.RegisterNodeRequest) (*datapb.RegisterNodeResponse, error)
	}

	MasterServiceInterface interface {
		GetComponentStates() (*internalpb2.ComponentStates, error)
		AllocID(in *masterpb.IDRequest) (*masterpb.IDResponse, error)
		ShowCollections(in *milvuspb.ShowCollectionRequest) (*milvuspb.ShowCollectionResponse, error)
		DescribeCollection(in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error)
	}

	DataNode struct {
		ctx     context.Context
		cancel  context.CancelFunc
		NodeID  UniqueID
		Role    string
		State   atomic.Value // internalpb2.StateCode_INITIALIZING
		watchDm chan struct{}

		dataSyncService *dataSyncService
		metaService     *metaService

		masterService MasterServiceInterface
		dataService   DataServiceInterface

		flushChan chan *flushMsg
		replica   Replica

		closer io.Closer
	}
)

func NewDataNode(ctx context.Context) *DataNode {

	Params.Init()
	ctx2, cancel2 := context.WithCancel(ctx)
	node := &DataNode{
		ctx:     ctx2,
		cancel:  cancel2,
		NodeID:  Params.NodeID, // GOOSE TODO: How to init
		Role:    typeutil.DataNodeRole,
		watchDm: make(chan struct{}),

		dataSyncService: nil,
		metaService:     nil,
		masterService:   nil,
		dataService:     nil,
		replica:         nil,
	}

	node.State.Store(internalpb2.StateCode_INITIALIZING)

	return node
}

func (node *DataNode) SetMasterServiceInterface(ms MasterServiceInterface) error {
	switch {
	case ms == nil, node.masterService != nil:
		return errors.New("Nil parameter or repeatly set")
	default:
		node.masterService = ms
		return nil
	}
}

func (node *DataNode) SetDataServiceInterface(ds DataServiceInterface) error {
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

	req := &datapb.RegisterNodeRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_kNone,
			SourceID: node.NodeID,
		},
		Address: &commonpb.Address{
			Ip:   Params.IP,
			Port: Params.Port,
		},
	}

	resp, err := node.dataService.RegisterNode(req)
	if err != nil {
		return errors.Errorf("Register node failed: %v", err)
	}

	select {
	case <-time.After(RPCConnectionTimeout):
		return errors.New("Get DmChannels failed in 30 seconds")
	case <-node.watchDm:
		log.Println("insert channel names set")
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
			return errors.Errorf("Invalid key: %v", kv.Key)
		}

	}

	replica := newReplica()

	var alloc allocator = newAllocatorImpl(node.masterService)

	chanSize := 100
	node.flushChan = make(chan *flushMsg, chanSize)

	node.dataSyncService = newDataSyncService(node.ctx, node.flushChan, replica, alloc)
	node.dataSyncService.init()
	node.metaService = newMetaService(node.ctx, replica, node.masterService)

	node.replica = replica

	return nil
}

func (node *DataNode) Start() error {
	node.metaService.init()
	go node.dataSyncService.start()
	node.State.Store(internalpb2.StateCode_HEALTHY)
	return nil
}

func (node *DataNode) WatchDmChannels(in *datapb.WatchDmChannelRequest) (*commonpb.Status, error) {
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
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
		status.ErrorCode = commonpb.ErrorCode_SUCCESS
		node.watchDm <- struct{}{}
		return status, nil
	}
}

func (node *DataNode) GetComponentStates() (*internalpb2.ComponentStates, error) {
	log.Println("DataNode current state:", node.State.Load())
	states := &internalpb2.ComponentStates{
		State: &internalpb2.ComponentInfo{
			NodeID:    Params.NodeID,
			Role:      node.Role,
			StateCode: node.State.Load().(internalpb2.StateCode),
		},
		SubcomponentStates: make([]*internalpb2.ComponentInfo, 0),
		Status:             &commonpb.Status{ErrorCode: commonpb.ErrorCode_SUCCESS},
	}
	return states, nil
}

func (node *DataNode) FlushSegments(in *datapb.FlushSegRequest) error {
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

func (node *DataNode) GetTimeTickChannel() (string, error) {
	return "Nothing happened", nil
}

func (node *DataNode) GetStatisticsChannel() (string, error) {
	return "Nothing happened", nil
}

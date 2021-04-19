package datanode

import (
	"context"
	"io"
	"log"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"

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
		typeutil.Service
		typeutil.Component

		WatchDmChannels(in *datapb.WatchDmChannelRequest) (*commonpb.Status, error)
		FlushSegments(in *datapb.FlushSegRequest) (*commonpb.Status, error)
	}

	DataServiceInterface interface {
		RegisterNode(req *datapb.RegisterNodeRequest) (*datapb.RegisterNodeResponse, error)
	}

	MasterServiceInterface interface {
		AllocID(in *masterpb.IDRequest) (*masterpb.IDResponse, error)
		ShowCollections(in *milvuspb.ShowCollectionRequest) (*milvuspb.ShowCollectionResponse, error)
		DescribeCollection(in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error)
	}

	DataNode struct {
		// GOOSE TODO: complete interface with component
		ctx    context.Context
		NodeID UniqueID
		Role   string
		State  internalpb2.StateCode

		dataSyncService *dataSyncService
		metaService     *metaService

		masterService MasterServiceInterface
		dataService   DataServiceInterface

		replica collectionReplica

		tracer opentracing.Tracer
		closer io.Closer
	}
)

func NewDataNode(ctx context.Context, nodeID UniqueID, masterService MasterServiceInterface,
	dataService DataServiceInterface) *DataNode {

	Params.Init()
	node := &DataNode{
		ctx:             ctx,
		NodeID:          nodeID,     // GOOSE TODO
		Role:            "DataNode", // GOOSE TODO
		State:           internalpb2.StateCode_INITIALIZING,
		dataSyncService: nil,
		metaService:     nil,
		masterService:   masterService,
		dataService:     dataService,
		replica:         nil,
	}

	return node
}

func (node *DataNode) Init() error {

	req := &datapb.RegisterNodeRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_kNone, //GOOSE TODO
			SourceID: node.NodeID,
		},
		Address: &commonpb.Address{
			Ip:   Params.IP,
			Port: Params.Port,
		},
	}

	resp, err := node.dataService.RegisterNode(req)
	if err != nil {
		return errors.Errorf("Init failed: %v", err)
	}

	for _, kv := range resp.InitParams.StartParams {
		log.Println(kv)
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

	var replica collectionReplica = &collectionReplicaImpl{
		collections: make([]*Collection, 0),
		segments:    make([]*Segment, 0),
	}

	var alloc allocator = newAllocatorImpl(node.masterService)
	chanSize := 100
	flushChan := make(chan *flushMsg, chanSize)
	node.dataSyncService = newDataSyncService(node.ctx, flushChan, replica, alloc)
	node.metaService = newMetaService(node.ctx, replica, node.masterService)
	node.replica = replica

	// Opentracing
	cfg := &config.Configuration{
		ServiceName: "data_node",
		Sampler: &config.SamplerConfig{
			Type:  "const",
			Param: 1,
		},
		Reporter: &config.ReporterConfig{
			LogSpans: true,
		},
	}
	tracer, closer, err := cfg.NewTracer(config.Logger(jaeger.StdLogger))
	if err != nil {
		return errors.Errorf("ERROR: cannot init Jaeger: %v\n", err)
	}
	node.tracer = tracer
	node.closer = closer

	opentracing.SetGlobalTracer(node.tracer)

	node.State = internalpb2.StateCode_HEALTHY
	return nil
}

func (node *DataNode) Start() error {

	go node.dataSyncService.start()
	node.metaService.init()

	return nil
}

func (node *DataNode) WatchDmChannels(in *datapb.WatchDmChannelRequest) error {
	// GOOSE TODO: Implement me
	return nil
}

func (node *DataNode) GetComponentStates() (*internalpb2.ComponentStates, error) {
	states := &internalpb2.ComponentStates{
		State: &internalpb2.ComponentInfo{
			NodeID:    Params.NodeID,
			Role:      node.Role,
			StateCode: node.State,
		},
		SubcomponentStates: make([]*internalpb2.ComponentInfo, 0),
		Status:             &commonpb.Status{},
	}
	return states, nil
}

func (node *DataNode) FlushSegments(in *datapb.FlushSegRequest) error {
	// GOOSE TODO: Implement me
	return nil
}

func (node *DataNode) Stop() error {
	<-node.ctx.Done()

	// close services
	if node.dataSyncService != nil {
		(*node.dataSyncService).close()
	}

	if node.closer != nil {
		node.closer.Close()
	}
	return nil

}

package datanode

import (
	"context"
	"io"
	"log"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

type DataNode struct {
	ctx             context.Context
	DataNodeID      uint64
	dataSyncService *dataSyncService
	metaService     *metaService

	replica collectionReplica

	tracer opentracing.Tracer
	closer io.Closer
}

func NewDataNode(ctx context.Context, dataNodeID uint64) *DataNode {

	collections := make([]*Collection, 0)

	var replica collectionReplica = &collectionReplicaImpl{
		collections: collections,
	}

	node := &DataNode{
		ctx:             ctx,
		DataNodeID:      dataNodeID,
		dataSyncService: nil,
		// metaService:     nil,
		replica: replica,
	}

	return node
}

func (node *DataNode) Init() error {
	Params.Init()
	return nil
}

func (node *DataNode) Start() error {
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

	var err error
	node.tracer, node.closer, err = cfg.NewTracer(config.Logger(jaeger.StdLogger))
	if err != nil {
		log.Printf("ERROR: cannot init Jaeger: %v\n", err)
	} else {
		opentracing.SetGlobalTracer(node.tracer)
	}

	// TODO GOOSE Init Size??
	chanSize := 100
	flushChan := make(chan *flushMsg, chanSize)

	node.dataSyncService = newDataSyncService(node.ctx, flushChan, node.replica)
	node.metaService = newMetaService(node.ctx, node.replica)

	go node.dataSyncService.start()
	// go node.flushSyncService.start()
	node.metaService.start()

	return nil
}

func (node *DataNode) WatchDmChannels(in *datapb.WatchDmChannelRequest) error {
	// GOOSE TODO: Implement me
	return nil
}

func (node *DataNode) GetComponentStates() (*internalpb2.ComponentStates, error) {
	// GOOSE TODO: Implement me
	return nil, nil
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

package datanode

import (
	"context"
	"io"
	"log"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
)

type DataNode struct {
	ctx              context.Context
	DataNodeID       uint64
	dataSyncService  *dataSyncService
	flushSyncService *flushSyncService
	metaService      *metaService
	replica          collectionReplica

	tracer opentracing.Tracer
	closer io.Closer
}

func NewDataNode(ctx context.Context, dataNodeID uint64) *DataNode {

	collections := make([]*Collection, 0)

	var replica collectionReplica = &collectionReplicaImpl{
		collections: collections,
	}

	node := &DataNode{
		ctx:              ctx,
		DataNodeID:       dataNodeID,
		dataSyncService:  nil,
		flushSyncService: nil,
		metaService:      nil,
		replica:          replica,
	}

	return node
}

func Init() {
	Params.Init()
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
	ddChan := make(chan *ddlFlushSyncMsg, chanSize)
	insertChan := make(chan *insertFlushSyncMsg, chanSize)

	node.flushSyncService = newFlushSyncService(node.ctx, ddChan, insertChan)
	node.dataSyncService = newDataSyncService(node.ctx, ddChan, insertChan, node.replica)
	node.metaService = newMetaService(node.ctx, node.replica)

	go node.dataSyncService.start()
	go node.flushSyncService.start()
	node.metaService.start()

	return nil
}

func (node *DataNode) Close() {
	<-node.ctx.Done()

	// close services
	if node.dataSyncService != nil {
		(*node.dataSyncService).close()
	}

	if node.closer != nil {
		node.closer.Close()
	}

}

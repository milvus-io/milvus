package writenode

import (
	"context"
	"fmt"
	"io"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
)

type WriteNode struct {
	ctx              context.Context
	WriteNodeID      uint64
	dataSyncService  *dataSyncService
	flushSyncService *flushSyncService
	metaService      *metaService
	replica          collectionReplica
	tracer           opentracing.Tracer
	closer           io.Closer
}

func NewWriteNode(ctx context.Context, writeNodeID uint64) *WriteNode {

	collections := make([]*Collection, 0)

	var replica collectionReplica = &collectionReplicaImpl{
		collections: collections,
	}

	node := &WriteNode{
		ctx:              ctx,
		WriteNodeID:      writeNodeID,
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

func (node *WriteNode) Start() error {
	cfg := &config.Configuration{
		ServiceName: "write_node",
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
		panic(fmt.Sprintf("ERROR: cannot init Jaeger: %v\n", err))
	}
	opentracing.SetGlobalTracer(node.tracer)

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

func (node *WriteNode) Close() {
	<-node.ctx.Done()

	// close services
	if node.dataSyncService != nil {
		(*node.dataSyncService).close()
	}

	if node.closer != nil {
		node.closer.Close()
	}

}

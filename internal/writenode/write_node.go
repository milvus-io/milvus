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

	tracer opentracing.Tracer
	closer io.Closer
}

func NewWriteNode(ctx context.Context, writeNodeID uint64) *WriteNode {

	node := &WriteNode{
		ctx:              ctx,
		WriteNodeID:      writeNodeID,
		dataSyncService:  nil,
		flushSyncService: nil,
	}

	return node
}

func Init() {
	Params.Init()
}

func (node *WriteNode) Start() error {
	cfg := &config.Configuration{
		ServiceName: "tracing",
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

	node.dataSyncService = newDataSyncService(node.ctx, ddChan, insertChan)

	go node.dataSyncService.start()
	go node.flushSyncService.start()

	return nil
}

func (node *WriteNode) Close() {
	<-node.ctx.Done()

	// close services
	if node.dataSyncService != nil {
		(*node.dataSyncService).close()
	}
}

package querynode

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"

*/
import "C"

import (
	"context"
	"fmt"
	"io"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
)

type QueryNode struct {
	queryNodeLoopCtx    context.Context
	queryNodeLoopCancel context.CancelFunc

	QueryNodeID uint64

	replica collectionReplica

	// services
	dataSyncService  *dataSyncService
	metaService      *metaService
	searchService    *searchService
	loadIndexService *loadIndexService
	statsService     *statsService

	//opentracing
	tracer opentracing.Tracer
	closer io.Closer
}

func Init() {
	Params.Init()
}

func NewQueryNode(ctx context.Context, queryNodeID uint64) *QueryNode {

	ctx1, cancel := context.WithCancel(ctx)
	q := &QueryNode{
		queryNodeLoopCtx:    ctx1,
		queryNodeLoopCancel: cancel,
		QueryNodeID:         queryNodeID,

		dataSyncService: nil,
		metaService:     nil,
		searchService:   nil,
		statsService:    nil,
	}

	var err error
	cfg := &config.Configuration{
		ServiceName: "query_node",
		Sampler: &config.SamplerConfig{
			Type:  "const",
			Param: 1,
		},
		Reporter: &config.ReporterConfig{
			LogSpans: true,
		},
	}
	q.tracer, q.closer, err = cfg.NewTracer(config.Logger(jaeger.StdLogger))
	if err != nil {
		panic(fmt.Sprintf("ERROR: cannot init Jaeger: %v\n", err))
	}
	opentracing.SetGlobalTracer(q.tracer)

	segmentsMap := make(map[int64]*Segment)
	collections := make([]*Collection, 0)

	tSafe := newTSafe()

	q.replica = &collectionReplicaImpl{
		collections: collections,
		segments:    segmentsMap,

		tSafe: tSafe,
	}

	return q
}

func (node *QueryNode) Start() error {
	// todo add connectMaster logic
	node.dataSyncService = newDataSyncService(node.queryNodeLoopCtx, node.replica)
	node.searchService = newSearchService(node.queryNodeLoopCtx, node.replica)
	node.metaService = newMetaService(node.queryNodeLoopCtx, node.replica)
	node.loadIndexService = newLoadIndexService(node.queryNodeLoopCtx, node.replica)
	node.statsService = newStatsService(node.queryNodeLoopCtx, node.replica, node.loadIndexService.fieldStatsChan)

	go node.dataSyncService.start()
	go node.searchService.start()
	go node.metaService.start()
	go node.loadIndexService.start()
	go node.statsService.start()

	<-node.queryNodeLoopCtx.Done()
	return nil
}

func (node *QueryNode) Close() {
	node.queryNodeLoopCancel()

	// free collectionReplica
	node.replica.freeAll()

	// close services
	if node.dataSyncService != nil {
		node.dataSyncService.close()
	}
	if node.searchService != nil {
		node.searchService.close()
	}
	if node.statsService != nil {
		node.statsService.close()
	}
	if node.closer != nil {
		node.closer.Close()
	}

}

package reader

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "collection_c.h"
#include "segment_c.h"

*/
import "C"

import (
	"context"
)

type QueryNode struct {
	ctx context.Context

	QueryNodeID uint64

	replica *collectionReplica

	dataSyncService *dataSyncService
	metaService     *metaService
	searchService   *searchService
	statsService    *statsService
}

func NewQueryNode(ctx context.Context, queryNodeID uint64) *QueryNode {
	segmentsMap := make(map[int64]*Segment)
	collections := make([]*Collection, 0)

	tSafe := newTSafe()

	var replica collectionReplica = &collectionReplicaImpl{
		collections: collections,
		segments:    segmentsMap,

		tSafe: tSafe,
	}

	return &QueryNode{
		ctx: ctx,

		QueryNodeID: queryNodeID,

		replica: &replica,

		dataSyncService: nil,
		metaService:     nil,
		searchService:   nil,
		statsService:    nil,
	}
}

func (node *QueryNode) Start() {
	node.dataSyncService = newDataSyncService(node.ctx, node.replica)
	node.searchService = newSearchService(node.ctx, node.replica)
	node.metaService = newMetaService(node.ctx, node.replica)
	node.statsService = newStatsService(node.ctx, node.replica)

	go node.dataSyncService.start()
	go node.searchService.start()
	go node.metaService.start()
	node.statsService.start()
}

func (node *QueryNode) Close() {
	<-node.ctx.Done()
	// free collectionReplica
	(*node.replica).freeAll()

	// close services
	if node.dataSyncService != nil {
		(*node.dataSyncService).close()
	}
	if node.searchService != nil {
		(*node.searchService).close()
	}
	if node.statsService != nil {
		(*node.statsService).close()
	}
}

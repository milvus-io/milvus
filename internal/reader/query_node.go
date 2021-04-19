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
	pulsarURL   string

	tSafe Timestamp

	container *container

	dataSyncService *dataSyncService
	metaService     *metaService
	searchService   *searchService
	statsService    *statsService
}

func NewQueryNode(ctx context.Context, queryNodeID uint64, pulsarURL string) *QueryNode {
	segmentsMap := make(map[int64]*Segment)
	collections := make([]*Collection, 0)

	var container container = &colSegContainer{
		collections: collections,
		segments:    segmentsMap,
	}

	return &QueryNode{
		ctx: ctx,

		QueryNodeID: queryNodeID,
		pulsarURL:   pulsarURL,

		tSafe: 0,

		container: &container,

		dataSyncService: nil,
		metaService:     nil,
		searchService:   nil,
		statsService:    nil,
	}
}

func (node *QueryNode) Start() {
	node.dataSyncService = newDataSyncService(node.ctx, node, node.pulsarURL)
	node.searchService = newSearchService(node.ctx, node, node.pulsarURL)
	node.metaService = newMetaService(node.ctx, node.container)
	node.statsService = newStatsService(node.ctx, node.container, node.pulsarURL)

	go node.dataSyncService.start()
	go node.searchService.start()
	go node.metaService.start()
	node.statsService.start()
}

func (node *QueryNode) Close() {
	// TODO: close services
}

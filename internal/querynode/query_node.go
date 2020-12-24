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
)

type QueryNode struct {
	queryNodeLoopCtx    context.Context
	queryNodeLoopCancel context.CancelFunc

	QueryNodeID uint64

	replica collectionReplica

	dataSyncService *dataSyncService
	metaService     *metaService
	searchService   *searchService
	statsService    *statsService
}

func Init() {
	Params.Init()
}

func NewQueryNode(ctx context.Context, queryNodeID uint64) *QueryNode {

	ctx1, cancel := context.WithCancel(ctx)

	segmentsMap := make(map[int64]*Segment)
	collections := make([]*Collection, 0)

	tSafe := newTSafe()

	var replica collectionReplica = &collectionReplicaImpl{
		collections: collections,
		segments:    segmentsMap,

		tSafe: tSafe,
	}

	return &QueryNode{
		queryNodeLoopCtx:    ctx1,
		queryNodeLoopCancel: cancel,
		QueryNodeID:         queryNodeID,

		replica: replica,

		dataSyncService: nil,
		metaService:     nil,
		searchService:   nil,
		statsService:    nil,
	}
}

func (node *QueryNode) Start() error {
	// todo add connectMaster logic
	node.dataSyncService = newDataSyncService(node.queryNodeLoopCtx, node.replica)
	node.searchService = newSearchService(node.queryNodeLoopCtx, node.replica)
	node.metaService = newMetaService(node.queryNodeLoopCtx, node.replica)
	node.statsService = newStatsService(node.queryNodeLoopCtx, node.replica)

	go node.dataSyncService.start()
	go node.searchService.start()
	go node.metaService.start()
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
}

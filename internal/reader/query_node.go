package reader

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "collection_c.h"
#include "partition_c.h"
#include "segment_c.h"

*/
import "C"

import (
	"context"
	"errors"
	"strconv"

	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type Timestamp = typeutil.Timestamp

type QueryNode struct {
	ctx context.Context

	QueryNodeID uint64
	pulsarURL   string

	Collections []*Collection
	SegmentsMap map[int64]*Segment

	queryNodeTime *QueryNodeTime

	manipulationService *manipulationService
	metaService         *metaService
	searchService       *searchService
	segmentService      *segmentService
}

func NewQueryNode(ctx context.Context, queryNodeID uint64, pulsarURL string) *QueryNode {
	queryNodeTimeSync := &QueryNodeTime{}

	segmentsMap := make(map[int64]*Segment)

	return &QueryNode{
		ctx: ctx,

		QueryNodeID: queryNodeID,
		pulsarURL:   pulsarURL,

		Collections: nil,
		SegmentsMap: segmentsMap,

		queryNodeTime: queryNodeTimeSync,

		manipulationService: nil,
		metaService:         nil,
		searchService:       nil,
		segmentService:      nil,
	}
}

func (node *QueryNode) Start() {
	node.manipulationService = newManipulationService(node.ctx, node, node.pulsarURL)
	node.searchService = newSearchService(node.ctx, node, node.pulsarURL)
	node.metaService = newMetaService(node.ctx, node)
	node.segmentService = newSegmentService(node.ctx, node, node.pulsarURL)

	go node.manipulationService.start()
	go node.searchService.start()
	go node.metaService.start()
	node.segmentService.start()
}

func (node *QueryNode) Close() {
	// TODO: close services
}

func (node *QueryNode) newCollection(collectionID int64, collectionName string, schemaConfig string) *Collection {
	/*
		CCollection
		newCollection(const char* collection_name, const char* schema_conf);
	*/
	cName := C.CString(collectionName)
	cSchema := C.CString(schemaConfig)
	collection := C.NewCollection(cName, cSchema)

	var newCollection = &Collection{CollectionPtr: collection, CollectionName: collectionName, CollectionID: collectionID}
	node.Collections = append(node.Collections, newCollection)

	return newCollection
}

func (node *QueryNode) deleteCollection(collection *Collection) {
	/*
		void
		deleteCollection(CCollection collection);
	*/
	cPtr := collection.CollectionPtr
	C.DeleteCollection(cPtr)

	collectionID := collection.CollectionID
	tmpCollections := make([]*Collection, 0)
	for _, col := range node.Collections {
		if col.CollectionID == collectionID {
			for _, p := range collection.Partitions {
				for _, s := range p.Segments {
					delete(node.SegmentsMap, s.SegmentID)
				}
			}
		} else {
			tmpCollections = append(tmpCollections, col)
		}
	}

	node.Collections = tmpCollections
}

/************************************** util functions ***************************************/
// Function `GetSegmentByEntityId` should return entityIDs, timestamps and segmentIDs
//func (node *QueryNode) GetKey2Segments() (*[]int64, *[]uint64, *[]int64) {
//	var entityIDs = make([]int64, 0)
//	var timestamps = make([]uint64, 0)
//	var segmentIDs = make([]int64, 0)
//
//	var key2SegMsg = node.messageClient.Key2SegMsg
//	for _, msg := range key2SegMsg {
//		if msg.SegmentId == nil {
//			segmentIDs = append(segmentIDs, -1)
//			entityIDs = append(entityIDs, msg.Uid)
//			timestamps = append(timestamps, msg.Timestamp)
//		} else {
//			for _, segmentID := range msg.SegmentId {
//				segmentIDs = append(segmentIDs, segmentID)
//				entityIDs = append(entityIDs, msg.Uid)
//				timestamps = append(timestamps, msg.Timestamp)
//			}
//		}
//	}
//
//	return &entityIDs, &timestamps, &segmentIDs
//}

func (node *QueryNode) getCollectionByID(collectionID int64) *Collection {
	for _, collection := range node.Collections {
		if collection.CollectionID == collectionID {
			return collection
		}
	}

	return nil
}

func (node *QueryNode) getCollectionByCollectionName(collectionName string) (*Collection, error) {
	for _, collection := range node.Collections {
		if collection.CollectionName == collectionName {
			return collection, nil
		}
	}

	return nil, errors.New("Cannot found collection: " + collectionName)
}

func (node *QueryNode) getSegmentBySegmentID(segmentID int64) (*Segment, error) {
	targetSegment, ok := node.SegmentsMap[segmentID]

	if !ok {
		return nil, errors.New("cannot found segment with id = " + strconv.FormatInt(segmentID, 10))
	}

	return targetSegment, nil
}

func (node *QueryNode) foundSegmentBySegmentID(segmentID int64) bool {
	_, ok := node.SegmentsMap[segmentID]

	return ok
}

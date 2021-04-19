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
	"time"

	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"

	"github.com/zilliztech/milvus-distributed/internal/kv"
	"github.com/zilliztech/milvus-distributed/internal/msgclient"
	msgPb "github.com/zilliztech/milvus-distributed/internal/proto/message"
)

type Timestamp = typeutil.Timestamp

type QueryNodeDataBuffer struct {
	InsertDeleteBuffer      []*msgPb.InsertOrDeleteMsg
	SearchBuffer            []*msgPb.SearchMsg
	validInsertDeleteBuffer []bool
	validSearchBuffer       []bool
}

type QueryInfo struct {
	NumQueries int64  `json:"num_queries"`
	TopK       int    `json:"topK"`
	FieldName  string `json:"field_name"`
}

type MsgCounter struct {
	InsertCounter int64
	InsertTime    time.Time

	DeleteCounter int64
	DeleteTime    time.Time

	SearchCounter int64
	SearchTime    time.Time
}

type InsertLog struct {
	MsgLength              int
	DurationInMilliseconds int64
	InsertTime             time.Time
	NumSince               int64
	Speed                  float64
}

type QueryNode struct {
	// context
	ctx context.Context

	QueryNodeID          uint64
	Collections          []*Collection
	SegmentsMap          map[int64]*Segment
	messageClient        *msgclient.ReaderMessageClient
	queryNodeTimeSync    *QueryNodeTime
	buffer               QueryNodeDataBuffer
	deletePreprocessData DeletePreprocessData
	deleteData           DeleteData
	insertData           InsertData
	kvBase               *kv.EtcdKV
	msgCounter           *MsgCounter
	InsertLogs           []InsertLog
}

func NewQueryNode(ctx context.Context, queryNodeID uint64, timeSync uint64) *QueryNode {
	mc := msgclient.ReaderMessageClient{}

	queryNodeTimeSync := &QueryNodeTime{
		ReadTimeSyncMin: timeSync,
		ReadTimeSyncMax: timeSync,
		WriteTimeSync:   timeSync,
		ServiceTimeSync: timeSync,
		TSOTimeSync:     timeSync,
	}

	segmentsMap := make(map[int64]*Segment)

	buffer := QueryNodeDataBuffer{
		InsertDeleteBuffer:      make([]*msgPb.InsertOrDeleteMsg, 0),
		SearchBuffer:            make([]*msgPb.SearchMsg, 0),
		validInsertDeleteBuffer: make([]bool, 0),
		validSearchBuffer:       make([]bool, 0),
	}

	msgCounter := MsgCounter{
		InsertCounter: 0,
		DeleteCounter: 0,
		SearchCounter: 0,
	}

	return &QueryNode{
		ctx:               ctx,
		QueryNodeID:       queryNodeID,
		Collections:       nil,
		SegmentsMap:       segmentsMap,
		messageClient:     &mc,
		queryNodeTimeSync: queryNodeTimeSync,
		buffer:            buffer,
		msgCounter:        &msgCounter,
	}
}

func (node *QueryNode) Close() {
	if node.messageClient != nil {
		node.messageClient.Close()
	}
	if node.kvBase != nil {
		node.kvBase.Close()
	}
}

func CreateQueryNode(ctx context.Context, queryNodeID uint64, timeSync uint64, mc *msgclient.ReaderMessageClient) *QueryNode {
	queryNodeTimeSync := &QueryNodeTime{
		ReadTimeSyncMin: timeSync,
		ReadTimeSyncMax: timeSync,
		WriteTimeSync:   timeSync,
		ServiceTimeSync: timeSync,
		TSOTimeSync:     timeSync,
	}

	segmentsMap := make(map[int64]*Segment)

	buffer := QueryNodeDataBuffer{
		InsertDeleteBuffer:      make([]*msgPb.InsertOrDeleteMsg, 0),
		SearchBuffer:            make([]*msgPb.SearchMsg, 0),
		validInsertDeleteBuffer: make([]bool, 0),
		validSearchBuffer:       make([]bool, 0),
	}

	msgCounter := MsgCounter{
		InsertCounter: 0,
		InsertTime:    time.Now(),
		DeleteCounter: 0,
		DeleteTime:    time.Now(),
		SearchCounter: 0,
		SearchTime:    time.Now(),
	}

	return &QueryNode{
		ctx:               ctx,
		QueryNodeID:       queryNodeID,
		Collections:       nil,
		SegmentsMap:       segmentsMap,
		messageClient:     mc,
		queryNodeTimeSync: queryNodeTimeSync,
		buffer:            buffer,
		msgCounter:        &msgCounter,
		InsertLogs:        make([]InsertLog, 0),
	}
}

func (node *QueryNode) QueryNodeDataInit() {
	deletePreprocessData := DeletePreprocessData{
		deleteRecords: make([]*DeleteRecord, 0),
		count:         0,
	}

	deleteData := DeleteData{
		deleteIDs:        make(map[int64][]int64),
		deleteTimestamps: make(map[int64][]uint64),
		deleteOffset:     make(map[int64]int64),
	}

	insertData := InsertData{
		insertIDs:        make(map[int64][]int64),
		insertTimestamps: make(map[int64][]uint64),
		// insertRecords:    make(map[int64][][]byte),
		insertOffset: make(map[int64]int64),
	}

	node.deletePreprocessData = deletePreprocessData
	node.deleteData = deleteData
	node.insertData = insertData
}

func (node *QueryNode) NewCollection(collectionID int64, collectionName string, schemaConfig string) *Collection {
	cName := C.CString(collectionName)
	cSchema := C.CString(schemaConfig)
	collection := C.NewCollection(cName, cSchema)

	var newCollection = &Collection{CollectionPtr: collection, CollectionName: collectionName, CollectionID: collectionID}
	node.Collections = append(node.Collections, newCollection)

	return newCollection
}

func (node *QueryNode) DeleteCollection(collection *Collection) {
	/*
		void
		DeleteCollection(CCollection collection);
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

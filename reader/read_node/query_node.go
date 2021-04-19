package reader

/*

#cgo CFLAGS: -I${SRCDIR}/../../core/include

#cgo LDFLAGS: -L${SRCDIR}/../../core/lib -lmilvus_dog_segment -Wl,-rpath=${SRCDIR}/../../core/lib

#include "collection_c.h"
#include "partition_c.h"
#include "segment_c.h"

*/
import "C"

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/czs007/suvlim/conf"
	msgPb "github.com/czs007/suvlim/pkg/master/grpc/message"
	"github.com/czs007/suvlim/pkg/master/kv"
	"github.com/czs007/suvlim/reader/message_client"
	"github.com/stretchr/testify/assert"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//"github.com/stretchr/testify/assert"
)

type InsertData struct {
	insertIDs        map[int64][]int64
	insertTimestamps map[int64][]uint64
	insertRecords    map[int64][][]byte
	insertOffset     map[int64]int64
}

type DeleteData struct {
	deleteIDs        map[int64][]int64
	deleteTimestamps map[int64][]uint64
	deleteOffset     map[int64]int64
}

type DeleteRecord struct {
	entityID  int64
	timestamp uint64
	segmentID int64
}

type DeletePreprocessData struct {
	deleteRecords []*DeleteRecord
	count         int32
}

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

	QueryNodeId   uint64
	Collections   []*Collection
	SegmentsMap   map[int64]*Segment
	messageClient *message_client.MessageClient
	//mc                   *message_client.MessageClient
	queryNodeTimeSync    *QueryNodeTime
	buffer               QueryNodeDataBuffer
	deletePreprocessData DeletePreprocessData
	deleteData           DeleteData
	insertData           InsertData
	kvBase               *kv.EtcdKVBase
	msgCounter           *MsgCounter
	InsertLogs           []InsertLog
}

func NewQueryNode(ctx context.Context, queryNodeId uint64, timeSync uint64) *QueryNode {
	mc := message_client.MessageClient{}

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
		QueryNodeId:       queryNodeId,
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

func CreateQueryNode(ctx context.Context, queryNodeId uint64, timeSync uint64, mc *message_client.MessageClient) *QueryNode {
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
		QueryNodeId:       queryNodeId,
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
		insertRecords:    make(map[int64][][]byte),
		insertOffset:     make(map[int64]int64),
	}

	node.deletePreprocessData = deletePreprocessData
	node.deleteData = deleteData
	node.insertData = insertData
}

func (node *QueryNode) NewCollection(collectionID uint64, collectionName string, schemaConfig string) *Collection {
	/*
		void
		UpdateIndexes(CCollection c_collection, const char *index_string);
	*/
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

	// TODO: remove from node.Collections
}

func (node *QueryNode) UpdateIndexes(collection *Collection, indexConfig *string) {
	/*
		void
		UpdateIndexes(CCollection c_collection, const char *index_string);
	*/
	cCollectionPtr := collection.CollectionPtr
	cIndexConfig := C.CString(*indexConfig)
	C.UpdateIndexes(cCollectionPtr, cIndexConfig)
}

////////////////////////////////////////////////////////////////////////////////////////////////////

func (node *QueryNode) PrepareBatchMsg() []int {
	var msgLen = node.messageClient.PrepareBatchMsg()
	return msgLen
}

func (node *QueryNode) InitQueryNodeCollection() {
	// TODO: remove hard code, add collection creation request
	// TODO: error handle
	var newCollection = node.NewCollection(0, "collection1", "fakeSchema")
	var newPartition = newCollection.NewPartition("partition1")
	// TODO: add segment id
	var segment = newPartition.NewSegment(0)
	node.SegmentsMap[0] = segment
}

////////////////////////////////////////////////////////////////////////////////////////////////////

func (node *QueryNode) RunInsertDelete(wg *sync.WaitGroup) {
	const Debug = true
	const CountInsertMsgBaseline = 1000 * 1000
	var BaselineCounter int64 = 0

	if Debug {
		for {
			select {
			case <-node.ctx.Done():
				wg.Done()
				return
			default:
				var msgLen = node.PrepareBatchMsg()
				var timeRange = TimeRange{node.messageClient.TimeSyncStart(), node.messageClient.TimeSyncEnd()}
				assert.NotEqual(nil, 0, timeRange.timestampMin)
				assert.NotEqual(nil, 0, timeRange.timestampMax)

				if node.msgCounter.InsertCounter/CountInsertMsgBaseline != BaselineCounter {
					node.WriteQueryLog()
					BaselineCounter = node.msgCounter.InsertCounter / CountInsertMsgBaseline
				}

				if msgLen[0] == 0 && len(node.buffer.InsertDeleteBuffer) <= 0 {
					node.queryNodeTimeSync.UpdateSearchTimeSync(timeRange)
					continue
				}

				node.QueryNodeDataInit()
				node.MessagesPreprocess(node.messageClient.InsertOrDeleteMsg, timeRange)
				//fmt.Println("MessagesPreprocess Done")
				node.WriterDelete()
				node.PreInsertAndDelete()
				//fmt.Println("PreInsertAndDelete Done")
				node.DoInsertAndDelete()
				//fmt.Println("DoInsertAndDelete Done")
				node.queryNodeTimeSync.UpdateSearchTimeSync(timeRange)
			}
		}
	} else {
		for {
			select {
			case <-node.ctx.Done():
				wg.Done()
				return
			default:
				var msgLen = node.PrepareBatchMsg()
				var timeRange = TimeRange{node.messageClient.TimeSyncStart(), node.messageClient.TimeSyncEnd()}
				assert.NotEqual(nil, 0, timeRange.timestampMin)
				assert.NotEqual(nil, 0, timeRange.timestampMax)

				if msgLen[0] == 0 && len(node.buffer.InsertDeleteBuffer) <= 0 {
					node.queryNodeTimeSync.UpdateSearchTimeSync(timeRange)
					continue
				}

				node.QueryNodeDataInit()
				node.MessagesPreprocess(node.messageClient.InsertOrDeleteMsg, timeRange)
				//fmt.Println("MessagesPreprocess Done")
				node.WriterDelete()
				node.PreInsertAndDelete()
				//fmt.Println("PreInsertAndDelete Done")
				node.DoInsertAndDelete()
				//fmt.Println("DoInsertAndDelete Done")
				node.queryNodeTimeSync.UpdateSearchTimeSync(timeRange)
			}
		}
	}
	wg.Done()
}

func (node *QueryNode) TestInsertDelete(timeRange TimeRange) {
	node.QueryNodeDataInit()
	node.MessagesPreprocess(node.messageClient.InsertOrDeleteMsg, timeRange)
	fmt.Println("MessagesPreprocess Done")
	node.WriterDelete()
	node.PreInsertAndDelete()
	fmt.Println("PreInsertAndDelete Done")
	node.DoInsertAndDelete()
	fmt.Println("DoInsertAndDelete Done")
	node.queryNodeTimeSync.UpdateSearchTimeSync(timeRange)
	fmt.Print("UpdateSearchTimeSync Done\n\n\n")
}

func (node *QueryNode) RunSearch(wg *sync.WaitGroup) {
	for {
		select {
		case <-node.ctx.Done():
			wg.Done()
			return
		case msg := <-node.messageClient.GetSearchChan():
			node.messageClient.SearchMsg = node.messageClient.SearchMsg[:0]
			node.messageClient.SearchMsg = append(node.messageClient.SearchMsg, msg)
			//for  {
			//if node.messageClient.SearchMsg[0].Timestamp < node.queryNodeTimeSync.ServiceTimeSync {
			var status = node.Search(node.messageClient.SearchMsg)
			fmt.Println("Do Search done")
			if status.ErrorCode != 0 {
				fmt.Println("Search Failed")
				node.PublishFailedSearchResult()
			}
			//break
			//}
			//}
		default:
		}
	}
	wg.Done()
}

////////////////////////////////////////////////////////////////////////////////////////////////////

func (node *QueryNode) MessagesPreprocess(insertDeleteMessages []*msgPb.InsertOrDeleteMsg, timeRange TimeRange) msgPb.Status {
	var tMax = timeRange.timestampMax

	// 1. Extract messages before readTimeSync from QueryNodeDataBuffer.
	//    Set valid bitmap to false.
	for i, msg := range node.buffer.InsertDeleteBuffer {
		if msg.Timestamp < tMax {
			if msg.Op == msgPb.OpType_INSERT {
				if msg.RowsData == nil {
					continue
				}
				node.insertData.insertIDs[msg.SegmentId] = append(node.insertData.insertIDs[msg.SegmentId], msg.Uid)
				node.insertData.insertTimestamps[msg.SegmentId] = append(node.insertData.insertTimestamps[msg.SegmentId], msg.Timestamp)
				node.insertData.insertRecords[msg.SegmentId] = append(node.insertData.insertRecords[msg.SegmentId], msg.RowsData.Blob)
			} else if msg.Op == msgPb.OpType_DELETE {
				var r = DeleteRecord{
					entityID:  msg.Uid,
					timestamp: msg.Timestamp,
				}
				node.deletePreprocessData.deleteRecords = append(node.deletePreprocessData.deleteRecords, &r)
				atomic.AddInt32(&node.deletePreprocessData.count, 1)
			}
			node.buffer.validInsertDeleteBuffer[i] = false
		}
	}

	// 2. Remove invalid messages from buffer.
	tmpInsertOrDeleteBuffer := make([]*msgPb.InsertOrDeleteMsg, 0)
	for i, isValid := range node.buffer.validInsertDeleteBuffer {
		if isValid {
			tmpInsertOrDeleteBuffer = append(tmpInsertOrDeleteBuffer, node.buffer.InsertDeleteBuffer[i])
		}
	}
	node.buffer.InsertDeleteBuffer = tmpInsertOrDeleteBuffer

	// 3. Resize the valid bitmap and set all bits to true.
	node.buffer.validInsertDeleteBuffer = node.buffer.validInsertDeleteBuffer[:len(node.buffer.InsertDeleteBuffer)]
	for i := range node.buffer.validInsertDeleteBuffer {
		node.buffer.validInsertDeleteBuffer[i] = true
	}

	// 4. Extract messages before readTimeSync from current messageClient.
	//    Move massages after readTimeSync to QueryNodeDataBuffer.
	//    Set valid bitmap to true.
	for _, msg := range insertDeleteMessages {
		if msg.Timestamp < tMax {
			if msg.Op == msgPb.OpType_INSERT {
				if msg.RowsData == nil {
					continue
				}
				node.insertData.insertIDs[msg.SegmentId] = append(node.insertData.insertIDs[msg.SegmentId], msg.Uid)
				node.insertData.insertTimestamps[msg.SegmentId] = append(node.insertData.insertTimestamps[msg.SegmentId], msg.Timestamp)
				node.insertData.insertRecords[msg.SegmentId] = append(node.insertData.insertRecords[msg.SegmentId], msg.RowsData.Blob)
			} else if msg.Op == msgPb.OpType_DELETE {
				var r = DeleteRecord{
					entityID:  msg.Uid,
					timestamp: msg.Timestamp,
				}
				node.deletePreprocessData.deleteRecords = append(node.deletePreprocessData.deleteRecords, &r)
				atomic.AddInt32(&node.deletePreprocessData.count, 1)
			}
		} else {
			node.buffer.InsertDeleteBuffer = append(node.buffer.InsertDeleteBuffer, msg)
			node.buffer.validInsertDeleteBuffer = append(node.buffer.validInsertDeleteBuffer, true)
		}
	}

	return msgPb.Status{ErrorCode: msgPb.ErrorCode_SUCCESS}
}

func (node *QueryNode) WriterDelete() msgPb.Status {
	// TODO: set timeout
	for {
		if node.deletePreprocessData.count == 0 {
			return msgPb.Status{ErrorCode: msgPb.ErrorCode_SUCCESS}
		}
		node.messageClient.PrepareKey2SegmentMsg()
		var ids, timestamps, segmentIDs = node.GetKey2Segments()
		for i := 0; i < len(*ids); i++ {
			id := (*ids)[i]
			timestamp := (*timestamps)[i]
			segmentID := (*segmentIDs)[i]
			for _, r := range node.deletePreprocessData.deleteRecords {
				if r.timestamp == timestamp && r.entityID == id {
					r.segmentID = segmentID
					atomic.AddInt32(&node.deletePreprocessData.count, -1)
				}
			}
		}
	}
}

func (node *QueryNode) PreInsertAndDelete() msgPb.Status {
	// 1. Do PreInsert
	for segmentID := range node.insertData.insertRecords {
		var targetSegment, err = node.GetSegmentBySegmentID(segmentID)
		if err != nil {
			fmt.Println(err.Error())
			return msgPb.Status{ErrorCode: 1}
		}

		var numOfRecords = len(node.insertData.insertRecords[segmentID])
		var offset = targetSegment.SegmentPreInsert(numOfRecords)
		node.insertData.insertOffset[segmentID] = offset
	}

	// 2. Sort delete preprocess data by segment id
	for _, r := range node.deletePreprocessData.deleteRecords {
		node.deleteData.deleteIDs[r.segmentID] = append(node.deleteData.deleteIDs[r.segmentID], r.entityID)
		node.deleteData.deleteTimestamps[r.segmentID] = append(node.deleteData.deleteTimestamps[r.segmentID], r.timestamp)
	}

	// 3. Do PreDelete
	for segmentID := range node.deleteData.deleteIDs {
		if segmentID < 0 {
			continue
		}
		var targetSegment, err = node.GetSegmentBySegmentID(segmentID)
		if err != nil {
			fmt.Println(err.Error())
			return msgPb.Status{ErrorCode: 1}
		}

		var numOfRecords = len(node.deleteData.deleteIDs[segmentID])
		var offset = targetSegment.SegmentPreDelete(numOfRecords)
		node.deleteData.deleteOffset[segmentID] = offset
	}

	return msgPb.Status{ErrorCode: msgPb.ErrorCode_SUCCESS}
}

func (node *QueryNode) DoInsertAndDelete() msgPb.Status {
	var wg sync.WaitGroup
	// Do insert
	for segmentID := range node.insertData.insertRecords {
		wg.Add(1)
		go node.DoInsert(segmentID, &wg)
	}

	// Do delete
	for segmentID, deleteIDs := range node.deleteData.deleteIDs {
		if segmentID < 0 {
			continue
		}
		wg.Add(1)
		var deleteTimestamps = node.deleteData.deleteTimestamps[segmentID]
		go node.DoDelete(segmentID, &deleteIDs, &deleteTimestamps, &wg)
		fmt.Println("Do delete done")
	}

	wg.Wait()
	return msgPb.Status{ErrorCode: msgPb.ErrorCode_SUCCESS}
}

func (node *QueryNode) DoInsert(segmentID int64, wg *sync.WaitGroup) msgPb.Status {
	var targetSegment, err = node.GetSegmentBySegmentID(segmentID)
	if err != nil {
		fmt.Println(err.Error())
		return msgPb.Status{ErrorCode: 1}
	}

	ids := node.insertData.insertIDs[segmentID]
	timestamps := node.insertData.insertTimestamps[segmentID]
	records := node.insertData.insertRecords[segmentID]
	offsets := node.insertData.insertOffset[segmentID]

	err = targetSegment.SegmentInsert(offsets, &ids, &timestamps, &records)
	fmt.Println("Do insert done, len = ", len(node.insertData.insertIDs[segmentID]))

	node.QueryLog(len(ids))

	if err != nil {
		fmt.Println(err.Error())
		return msgPb.Status{ErrorCode: 1}
	}

	wg.Done()
	return msgPb.Status{ErrorCode: msgPb.ErrorCode_SUCCESS}
}

func (node *QueryNode) DoDelete(segmentID int64, deleteIDs *[]int64, deleteTimestamps *[]uint64, wg *sync.WaitGroup) msgPb.Status {
	var segment, err = node.GetSegmentBySegmentID(segmentID)
	if err != nil {
		fmt.Println(err.Error())
		return msgPb.Status{ErrorCode: 1}
	}

	offset := node.deleteData.deleteOffset[segmentID]

	node.msgCounter.DeleteCounter += int64(len(*deleteIDs))
	err = segment.SegmentDelete(offset, deleteIDs, deleteTimestamps)
	if err != nil {
		fmt.Println(err.Error())
		return msgPb.Status{ErrorCode: 1}
	}

	wg.Done()
	return msgPb.Status{ErrorCode: msgPb.ErrorCode_SUCCESS}
}

func (node *QueryNode) QueryJson2Info(queryJson *string) *QueryInfo {
	var query QueryInfo
	var err = json.Unmarshal([]byte(*queryJson), &query)

	if err != nil {
		log.Printf("Unmarshal query json failed")
		return nil
	}

	//fmt.Println(query)
	return &query
}

func (node *QueryNode) Search(searchMessages []*msgPb.SearchMsg) msgPb.Status {

	type SearchResultTmp struct {
		ResultId       int64
		ResultDistance float32
	}

	node.msgCounter.SearchCounter += int64(len(searchMessages))

	// Traverse all messages in the current messageClient.
	// TODO: Do not receive batched search requests
	for _, msg := range searchMessages {
		var clientId = msg.ClientId
		var searchTimestamp = msg.Timestamp

		// ServiceTimeSync update by readerTimeSync, which is get from proxy.
		// Proxy send this timestamp per `conf.Config.Timesync.Interval` milliseconds.
		// However, timestamp of search request (searchTimestamp) is precision time.
		// So the ServiceTimeSync is always less than searchTimestamp.
		// Here, we manually make searchTimestamp's logic time minus `conf.Config.Timesync.Interval` milliseconds.
		// Which means `searchTimestamp.logicTime = searchTimestamp.logicTime - conf.Config.Timesync.Interval`.
		var logicTimestamp = searchTimestamp << 46 >> 46
		searchTimestamp = (searchTimestamp>>18-uint64(conf.Config.Timesync.Interval+600))<<18 + logicTimestamp

		var vector = msg.Records
		// We now only the first Json is valid.
		var queryJson = msg.Json[0]

		// 1. Timestamp check
		// TODO: return or wait? Or adding graceful time
		if searchTimestamp > node.queryNodeTimeSync.ServiceTimeSync {
			fmt.Println("Invalid query time, timestamp = ", searchTimestamp>>18, ", SearchTimeSync = ", node.queryNodeTimeSync.ServiceTimeSync>>18)
			return msgPb.Status{ErrorCode: 1}
		}

		// 2. Get query information from query json
		query := node.QueryJson2Info(&queryJson)
		// 2d slice for receiving multiple queries's results
		var resultsTmp = make([][]SearchResultTmp, query.NumQueries)
		for i := 0; i < int(query.NumQueries); i++ {
			resultsTmp[i] = make([]SearchResultTmp, 0)
		}

		// 3. Do search in all segments
		for _, segment := range node.SegmentsMap {
			if segment.GetRowCount() <= 0 {
				// Skip empty segment
				continue
			}

			//fmt.Println("Search in segment:", segment.SegmentId, ",segment rows:", segment.GetRowCount())
			var res, err = segment.SegmentSearch(query, searchTimestamp, vector)
			if err != nil {
				fmt.Println(err.Error())
				return msgPb.Status{ErrorCode: 1}
			}

			for i := 0; i < int(query.NumQueries); i++ {
				for j := i * query.TopK; j < (i+1)*query.TopK; j++ {
					resultsTmp[i] = append(resultsTmp[i], SearchResultTmp{
						ResultId:       res.ResultIds[j],
						ResultDistance: res.ResultDistances[j],
					})
				}
			}
		}

		// 4. Reduce results
		for _, rTmp := range resultsTmp {
			sort.Slice(rTmp, func(i, j int) bool {
				return rTmp[i].ResultDistance < rTmp[j].ResultDistance
			})
		}

		for _, rTmp := range resultsTmp {
			if len(rTmp) > query.TopK {
				rTmp = rTmp[:query.TopK]
			}
		}

		var entities = msgPb.Entities{
			Ids: make([]int64, 0),
		}
		var results = msgPb.QueryResult{
			Status: &msgPb.Status{
				ErrorCode: 0,
			},
			Entities:  &entities,
			Distances: make([]float32, 0),
			QueryId:   msg.Uid,
			ClientId:  clientId,
		}
		for _, rTmp := range resultsTmp {
			for _, res := range rTmp {
				results.Entities.Ids = append(results.Entities.Ids, res.ResultId)
				results.Distances = append(results.Distances, res.ResultDistance)
				results.Scores = append(results.Distances, float32(0))
			}
		}
		// Send numQueries to RowNum.
		results.RowNum = query.NumQueries

		// 5. publish result to pulsar
		//fmt.Println(results.Entities.Ids)
		//fmt.Println(results.Distances)
		node.PublishSearchResult(&results)
	}

	return msgPb.Status{ErrorCode: msgPb.ErrorCode_SUCCESS}
}
